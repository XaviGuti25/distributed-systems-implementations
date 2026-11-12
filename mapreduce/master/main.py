from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List
from uuid import uuid4
import httpx
import threading
import time

app = FastAPI()

jobs: Dict[str, dict] = {}

WORKERS = ["worker1", "worker2", "worker3", "worker4", "worker5"]
WORKER_PORT = 8000

# 2.2.7 – Worker health tracking
worker_health: Dict[str, bool] = {worker: True for worker in WORKERS}

# 2.2.8 – Worker concurrency control
worker_busy: Dict[str, bool] = {worker: False for worker in WORKERS}

HEALTH_CHECK_INTERVAL = 30

class NewJobRequest(BaseModel):
    app_name: str
    data_path: str
    map_partitions: int
    reduce_partitions: int


class JobResponse(BaseModel):
    job_id: str
    app_name: str
    data_path: str
    map_partitions: int
    reduce_partitions: int
    mappers: List[dict]
    reducers: List[dict]
    status: str

# 2.2.1 – MASTER HEALTHCHECK
@app.get("/healthcheck")
def healthcheck():
    return {"status": "up"}

# 2.2.7 – WORKER HEALTH MONITORING
<<<<<<< HEAD
=======
# ==========================================================

#monitor worker hearbeat works infinitely every 30 seconds, if no response no tasks will be assigned to said worker
>>>>>>> de29a68 (viva el celta)
def check_worker_health(worker_name: str) -> bool:
    try:
        response = httpx.get(
            f"http://{worker_name}:{WORKER_PORT}/healthcheck",
            timeout=5.0
        )
        return response.status_code == 200
    except Exception:
        return False

def monitor_worker_health():
    while True:
        for worker in WORKERS:
            worker_health[worker] = check_worker_health(worker)
        time.sleep(HEALTH_CHECK_INTERVAL)
        
health_thread = threading.Thread(target=monitor_worker_health, daemon=True)
health_thread.start()

# 2.2.2 – CREATE JOB
@app.post("/jobs")
def post_jobs(request: NewJobRequest) -> JobResponse:
    job_id = str(uuid4()) #create a unique job ID
    
    #Create one mapper task per input partition
    # Each mapper starts as pending and has no worker assigned yet
    mappers = [{
        "worker": None,
        "status": "pending",
        "partition": i
    } for i in range(request.map_partitions)]
    
    # Create one reducer task per reduce partition
    # Each reducer starts as pending and has no worker assigned yet
    reducers = [{
        "worker": None,
        "status": "pending",
        "partition": i
    } for i in range(request.reduce_partitions)]

<<<<<<< HEAD
    job = {
        "job_id": job_id,
        "app_name": request.app_name,
        "data_path": request.data_path,
        "map_partitions": request.map_partitions,
        "reduce_partitions": request.reduce_partitions,
        "mappers": mappers,
        "reducers": reducers,
        "status": "in-progress"
    }

    jobs[job_id] = job # Save the job in memory

    assign_available_maps(job_id) # Try to assign map tasks to available workers

    return job

# 2.2.3 – GET JOB
@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> JobResponse:
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]

# 2.2.4 – MAP COMPLETION
@app.post("/jobs/{job_id}/map/{partition}/completed")
def post_map_completed(job_id: str, partition: int):
    # Check that the job exists
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

=======
# ==========================================================
# 2.2.8 – SCHEDULING LOGIC (CONCURRENCY CONTROL)
# ==========================================================
#assign maps to workers that are not buisy or not alive
def assign_available_maps(job_id: str):
>>>>>>> de29a68 (viva el celta)
    job = jobs[job_id]

    # Find the map task with this partition number 
    # This tells the master which map task is now done 
    mapper = next((m for m in job["mappers"] if m["partition"] == partition), None)
    if not mapper:
        raise HTTPException(status_code=404, detail="Map task not found")

    # Prevent marking the same task as completed twice
    if mapper["status"] == "completed":
        raise HTTPException(status_code=409, detail="Already completed")

    mapper["status"] = "completed"

    worker = mapper["worker"].split(":")[0]
    worker_busy[worker] = False # Free the worker so it can receive a new task

    assign_available_maps(job_id) # Try to assign more pending map tasks

    if all(m["status"] == "completed" for m in job["mappers"]): # If all map tasks are finished, start reduce tasks
        assign_available_reduces(job_id)

    return {"message": "Status updated"}

# 2.2.5 – REDUCE COMPLETION
@app.post("/jobs/{job_id}/reduce/{partition}/completed")

#same approach as map completion, but for reduce tasks.

def post_reduce_completed(job_id: str, partition: int):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]
    # Find the reduce task that matches the partition number sent by the worker, 
    # so the master knows exactly which reduce task has finished.
    reducer = next((r for r in job["reducers"] if r["partition"] == partition), None)
    if not reducer:
        raise HTTPException(status_code=404, detail="Reduce task not found")

    if reducer["status"] == "completed":
        raise HTTPException(status_code=409, detail="Already completed")

    reducer["status"] = "completed"

    worker = reducer["worker"].split(":")[0]
    worker_busy[worker] = False

    assign_available_reduces(job_id)

    if all(r["status"] == "completed" for r in job["reducers"]):
        job["status"] = "completed"

    return {"message": "Status updated"}

# 2.2.8 – SCHEDULING LOGIC (CONCURRENCY CONTROL)
def assign_available_maps(job_id: str):
    job = jobs[job_id]
    # Find all map tasks that are still pending
    pending_maps = [m for m in job["mappers"] if m["status"] == "pending"]

    if not pending_maps:
        return #if no pending maps, return

    for worker in WORKERS:
        # Skip unhealthy workers & busy workers
        if not worker_health.get(worker, True):
            continue
        if worker_busy.get(worker, False):
            continue

        if not pending_maps:
            break #stop if no more pending maps
        
        # Take the next pending map task, mark woker as busy, assign the task to the worker and mark the task as in-progress
        mapper = pending_maps.pop(0)
        worker_busy[worker] = True
        mapper["worker"] = f"{worker}:{WORKER_PORT}"
        mapper["status"] = "in-progress"
        
        # Build request with all the info the worker needs to execute the map task
        map_task = {
            "job_id": job_id,
            "app_name": job["app_name"],
            "data_path": job["data_path"],
            "map_partition": mapper["partition"],
            "reduce_partitions": job["reduce_partitions"]
        }

        try:
            # Finally send the request to the worker to start executing the map task
            httpx.post(
                f"http://{worker}:{WORKER_PORT}/map",
                json=map_task,
                timeout=30.0
            )
            # case it fails, mark the worker as not busy and reset the task to pending
        except Exception as e:
            print(f"[SCHEDULER] Map assign failed for {worker}: {e}")
            worker_busy[worker] = False
            mapper["worker"] = None
            mapper["status"] = "pending"


#assign reduce tasks call from the list of aviable reducers the reduce task inside the worker main
def assign_available_reduces(job_id: str):
    # Exactly the same approach as map scheduling, but for reduce tasks. 
    # the main difference is that we also send the list of all workers to the worker, 
    # so it can fetch intermediate data from all mappers when executing the reduce task.
    job = jobs[job_id]

    pending_reduces = [r for r in job["reducers"] if r["status"] == "pending"]

    if not pending_reduces:
        return

    for worker in WORKERS:

        if not worker_health.get(worker, True):
            continue
        if worker_busy.get(worker, False):
            continue

        if not pending_reduces:
            break

        reducer = pending_reduces.pop(0)

        worker_busy[worker] = True
        reducer["worker"] = f"{worker}:{WORKER_PORT}"
        reducer["status"] = "in-progress"

        reduce_task = {
            "job_id": job_id,
            "app_name": job["app_name"],
            "reduce_partition": reducer["partition"],
            "reduce_partitions": job["reduce_partitions"],
            "map_partitions": job["map_partitions"],
            "workers": [f"{w}:{WORKER_PORT}" for w in WORKERS]
        }

        try:
            httpx.post(
                f"http://{worker}:{WORKER_PORT}/reduce",
                json=reduce_task,
                timeout=30.0
            )
        except Exception as e:
            print(f"[SCHEDULER] Reduce assign failed for {worker}: {e}")
            worker_busy[worker] = False
            reducer["worker"] = None
<<<<<<< HEAD
            reducer["status"] = "pending"
=======
            reducer["status"] = "pending"


# ==========================================================
# 2.2.2 – CREATE JOB
# ==========================================================
#called by client generate id create map tasks create reduce tasks and start assignign map tasks
@app.post("/jobs")
def post_jobs(request: NewJobRequest) -> JobResponse:
    job_id = str(uuid4())

    mappers = [{
        "worker": None,
        "status": "pending",
        "partition": i
    } for i in range(request.map_partitions)]

    reducers = [{
        "worker": None,
        "status": "pending",
        "partition": i
    } for i in range(request.reduce_partitions)]

    job = {
        "job_id": job_id,
        "app_name": request.app_name,
        "data_path": request.data_path,
        "map_partitions": request.map_partitions,
        "reduce_partitions": request.reduce_partitions,
        "mappers": mappers,
        "reducers": reducers,
        "status": "in-progress"
    }

    jobs[job_id] = job

    assign_available_maps(job_id)

    return job


# ==========================================================
# 2.2.3 – GET JOB
# ==========================================================
#returns job in progress or job not found or job finsihed basically the status from jobid if not found pop error
@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> JobResponse:
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]


# ==========================================================
# 2.2.4 – MAP COMPLETION
# ==========================================================
#caller by worker we marked that map as completer free the worker and if all maps are finish start reduce
@app.post("/jobs/{job_id}/map/{partition}/completed")
def post_map_completed(job_id: str, partition: int):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]

    mapper = next((m for m in job["mappers"] if m["partition"] == partition), None)
    if not mapper:
        raise HTTPException(status_code=404, detail="Map task not found")

    if mapper["status"] == "completed":
        raise HTTPException(status_code=409, detail="Already completed")

    mapper["status"] = "completed"

    worker = mapper["worker"].split(":")[0]
    worker_busy[worker] = False

    assign_available_maps(job_id)

    if all(m["status"] == "completed" for m in job["mappers"]):
        assign_available_reduces(job_id)

    return {"message": "Status updated"}


# ==========================================================
# 2.2.5 – REDUCE COMPLETION
# ==========================================================
#mark reduce as completed free worker if all reduce tasks finished mark job completed
@app.post("/jobs/{job_id}/reduce/{partition}/completed")
def post_reduce_completed(job_id: str, partition: int):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]

    reducer = next((r for r in job["reducers"] if r["partition"] == partition), None)
    if not reducer:
        raise HTTPException(status_code=404, detail="Reduce task not found")

    if reducer["status"] == "completed":
        raise HTTPException(status_code=409, detail="Already completed")

    reducer["status"] = "completed"

    worker = reducer["worker"].split(":")[0]
    worker_busy[worker] = False

    assign_available_reduces(job_id)

    if all(r["status"] == "completed" for r in job["reducers"]):
        job["status"] = "completed"

    return {"message": "Status updated"}
>>>>>>> de29a68 (viva el celta)
