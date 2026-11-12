from __future__ import annotations
import sys
import traceback
import json
import urllib.parse
import urllib.request
from pathlib import Path
from importlib import import_module

from fastapi import FastAPI, BackgroundTasks, HTTPException
from typing import Dict, List
from pydantic import BaseModel

app = FastAPI()

APPS_DIR = Path("/app/apps")
DATA_DIR = Path("/app/data")
OUTPUTS_DIR = Path("/app/outputs")
RESULTS_DIR = Path("/app/results")

#the map output is to separate by key into r map output files for each mapper
class MapTask(BaseModel):
    job_id: str
    app_name: str
    data_path: str
    map_partition: int
    reduce_partitions: int

class ReduceTask(BaseModel):
    job_id: str
    app_name: str
    reduce_partition: int
    reduce_partitions: int
    map_partitions: int
    workers: list[str]

@app.get("/healthcheck")
def healthcheck():
    return {"status": "up"}


def load_app(app_name: str):
    apps_path = str(APPS_DIR)
    if apps_path not in sys.path:
        sys.path.append(apps_path)

    module = import_module(app_name)

    map_fn = getattr(module, "map")
    reduce_fn = getattr(module, "reduce")
    partitioner_fn = getattr(module, "partitioner")
    return map_fn, reduce_fn, partitioner_fn


def resolve_data_path(data_path: str) -> Path:
    p = data_path.strip()

    if p.startswith("./data/"):
        return DATA_DIR / p[len("./data/") :]
    if p == "./data":
        return DATA_DIR

    if p.startswith("data/"):
        return DATA_DIR / p[len("data/") :]
    if p == "data":
        return DATA_DIR

    if p.startswith("/data/") or p == "/data":
        return Path(p)

    return Path(p)


def pick_partition_file(folder: Path, map_partition: int) -> Path:
    direct = folder / str(map_partition)
    if direct.is_file():
        return direct

    files = sorted([f for f in folder.iterdir() if f.is_file()], key=lambda x: x.name)
    if not files:
        raise FileNotFoundError(f"No files found in data folder: {folder}")

    if map_partition < 0 or map_partition >= len(files):
        raise FileNotFoundError(
            f"Partition {map_partition} out of range for folder {folder}. "
            f"Found {len(files)} files: {[f.name for f in files]}"
        )
    return files[map_partition]


def run_map(task: MapTask) -> None:
    try:
        # Load the user application - we need the map function, the partitioner, and the number of reduce partitions
        # - map function → transforms input into (key, value) pairs
        # - partitioner → decides which reducer each key goes to
        map_fn, _reduce_fn, partitioner_fn = load_app(task.app_name)

        # Convert the given path (like "./data/...") into the real path inside the container
        data_folder = resolve_data_path(task.data_path)
        # Each map task only processes ONE partition (one file)
        # Example: if map_partition = 2 → read file "2"
        part_file = pick_partition_file(data_folder, task.map_partition)

        # Read the entire file as text
        content = part_file.read_text(encoding="utf-8", errors="ignore")
        
        # Call the user-defined map function
        # It returns pairs like: ("word", 1)
        pairs = list(map_fn(part_file.name, content))

        # Create folder to store intermediate results
        # Structure: /outputs/job_id/map_partition/
        base_out = OUTPUTS_DIR / task.job_id / str(task.map_partition)
        base_out.mkdir(parents=True, exist_ok=True)

        # Number of reduce partitions (must be > 0)
        rcount = int(task.reduce_partitions)
        if rcount <= 0:
            raise ValueError("reduce_partitions must be > 0")

        for k, v in pairs:
            rp = partitioner_fn(k, rcount) # Decide which reduce partition this key belongs to

            # Make sure rp is an integer
            # If not, convert it using hash
            if not isinstance(rp, int):
                rp = hash(rp)
            rp = int(rp) % rcount

            out_file = base_out / str(rp)# Each reduce partition has its own file
            # Append the key-value pair to the file
            with out_file.open("a", encoding="utf-8") as f:
                f.write(f"{k} {v}\n")

        print(
            f"[MAP DONE] job_id={task.job_id} map_partition={task.map_partition} "
            f"pairs={len(pairs)} out_dir={base_out}"
        )
        
        # 2.2.4: notify master that map task is completed
        try:
            import urllib.request
            # When the map task is finished, notify the master
            master_url = f"http://master:8000/jobs/{task.job_id}/map/{task.map_partition}/completed"
            request = urllib.request.Request(master_url, method='POST')
            with urllib.request.urlopen(request) as resp:
                print(f"[MAP NOTIFY] Notified master of completion for partition {task.map_partition}")
        except Exception as notify_error: # If notification fails, just log the error
            print(f"[MAP NOTIFY ERROR] Failed to notify master: {notify_error}")

    except Exception as e:
        print(f"[MAP ERROR] job_id={task.job_id} map_partition={task.map_partition} error={e}")
        traceback.print_exc()

@app.get("/map-output")
# This endpoint returns the intermediate output of one map task
# It is used later by a reduce task
def get_map_output(job_id: str, map_partition: int, reduce_partition: int) -> Dict[str, List[str]]:
    # Build the path to the intermediate file (Structure: /outputs/job_id/map_partition/reduce_partition)
    path = OUTPUTS_DIR / job_id / str(map_partition) / str(reduce_partition)
    # This means there is no output for this partition, return 404
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="map output not found")
    
    # Create an empty dictionary
    # It will store data like:
    # {"cat": ["1", "1"], "dog": ["1"]}
    result: Dict[str, List[str]] = {}
    # Read the file line by line
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()# Remove extra spaces
        if not line:
            continue # Skip empty lines
        # Split the line into key and value (Example: "cat 1" -> key="cat", value="1")
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue  # If the line does not have the correct format, skip it
        
        # Add the value to the list for this key
        # If the key does not exist yet, create an empty list first
        k, v = parts[0], parts[1]
        result.setdefault(k, []).append(v)

    return result

def fetch_map_output(worker_base: str, job_id: str, map_partition: int, reduce_partition: int) -> dict[str, list[str]]:
    # This helper function asks another worker for its map output
    qs = urllib.parse.urlencode( 
        {"job_id": job_id, "map_partition": map_partition, "reduce_partition": reduce_partition}
    )
    # Ensure the worker_base has the http:// protocol
    if not worker_base.startswith("http://"):
        worker_base = f"http://{worker_base}"# If the URL has no http:// prefix, add it
    url = f"{worker_base}/map-output?{qs}" # Build the full URL
    try:
        # Send GET request to the worker
        with urllib.request.urlopen(url) as resp:
            # Read JSON response and convert it to a Python dictionary
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {}   #we treat 404 as empty: map writes only partitions that received keys
        raise
    
def run_reduce(task: ReduceTask) -> None:
    try:
        _map_fn, reduce_fn, _partitioner_fn = load_app(task.app_name)

        rcount = int(task.reduce_partitions) 
        rp = int(task.reduce_partition)  
        mp_count = int(task.map_partitions)
        if rcount <= 0:
            raise ValueError("reduce_partitions must be > 0")  # Check that reduce_partitions is valid
        if rp < 0 or rp >= rcount:
            raise ValueError("reduce_partition out of range") # Check that this reduce partition number is valid
        if mp_count <= 0: # Check that map_partitions is valid
            raise ValueError("map_partitions must be > 0")
        if not task.workers:
            raise ValueError("workers list cannot be empty") # There must be at least one worker in the list

        grouped: dict[str, list[str]] = {} # This dictionary will collect all values for each key

        for mp in range(mp_count):
            # Choose the worker that holds this map partition
            worker_base = task.workers[mp % len(task.workers)]
            
            # Ask that worker for its intermediate output
            data = fetch_map_output(worker_base, task.job_id, mp, rp)
            # Merge returned data into the grouped dictionary
            for k, vals in data.items():
                grouped.setdefault(k, []).extend(vals)

        out_dir = RESULTS_DIR / task.job_id # Create output folder for final reduce results
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / str(rp) # Output file for this reduce partition

        with out_file.open("w", encoding="utf-8") as f:
            # Process keys in sorted order 
            for k in sorted(grouped.keys()):
                # Convert values from strings to integers
                # Example: ["1", "1", "1"] -> [1, 1, 1]
                int_vals = [int(x) for x in grouped[k]]
                res = reduce_fn(k, int_vals)# REDUCE example: reduce("cat", [1, 1, 1]) -> "3"
                
                # Some reduce functions may return only the value
                # Others may return (key, value)
                if isinstance(res, tuple) and len(res) == 2:
                    out_k, out_v = res
                else:
                    out_k, out_v = k, res
                f.write(f"{out_k} {out_v}\n") # Write final result line

        print(f"[REDUCE DONE] job_id={task.job_id} reduce_partition={rp} keys={len(grouped)}")
        

        # 2.2.5: notify master that reduce task is completed
        try:
            import urllib.request
            master_url = f"http://master:8000/jobs/{task.job_id}/reduce/{rp}/completed"
            request = urllib.request.Request(master_url, method='POST')
            with urllib.request.urlopen(request) as resp:
                print(f"[REDUCE NOTIFY] Notified master of completion for partition {rp}")
        except Exception as notify_error:
            print(f"[REDUCE NOTIFY ERROR] Failed to notify master: {notify_error}")

    except Exception as e:
        print(f"[REDUCE ERROR] job_id={task.job_id} reduce_partition={task.reduce_partition} error={e}")
        traceback.print_exc()



@app.post("/map")
# This endpoint is called by the master to start a map task
# We do NOT run the map immediately
# Instead, we send it to the background so the API can respond fast
def post_map(task: MapTask, background: BackgroundTasks):
    background.add_task(run_map, task)
    return {"status": "in-progress"}# Tell the master: "I received the task and I'm working on it"

@app.post("/reduce")
# This endpoint starts a reduce task
# Do the reduce work in the background
# This lets the API respond immediately
def post_reduce(task: ReduceTask, background: BackgroundTasks):
    background.add_task(run_reduce, task)
    return {"status": "in-progress"} # Tell the master: "I received the reduce task"
