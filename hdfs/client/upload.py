import sys
import os
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import httpx

NAMENODE_URL = "http://localhost:8000"

def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)

def load_block_size_bytes() -> int: #looks for block size in namenode->settings.json
    candidates = [
        Path("settings.json"),
        Path("namenode") / "settings.json",
        Path(__file__).resolve().parent.parent / "namenode" / "settings.json",
    ]
    for p in candidates:
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if "block_size_bytes" in data:
                return int(data["block_size_bytes"])
    return 1_000_000

def request_json( #General error handler for requests (function calls that expect to return a json)
    method: str,
    url: str,
    *,
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
) -> Any:
    try:
        r = httpx.request(method, url, json=json_body, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json() if r.content else None
    except httpx.RequestError as e:
        die(f"Can't reach {url}: {e}")
    except httpx.HTTPStatusError as e:
        die(f"{url} returned {e.response.status_code}: {e.response.text}")
    except ValueError as e:
        die(f"{url} didn't return valid JSON: {e}")

def get_datanode_map() -> Dict[str, Tuple[str, int]]: #get list of datanodes from namenode and make dictionary with datanode_id, (host,port)
    #so client knows where are the replicas (so where to send a block)
    payload = request_json("GET", f"{NAMENODE_URL}/datanodes")
    dns = payload.get("datanodes") if isinstance(payload, dict) else None
    if not isinstance(dns, list):
        die(f"Unexpected /datanodes response: {payload}")
    out: Dict[str, Tuple[str, int]] = {}
    for dn in dns:
        out[str(dn["id"])] = (dn["host"], int(dn["port"]))
    return out

def create_file_on_namenode(file_name: str) -> None: #registers file in namenode before any block is uploaded
    url = f"{NAMENODE_URL}/files"
    body = {"file_name": file_name}
    try:
        r = httpx.post(url, json=body, timeout=10.0)
        r.raise_for_status()
    except httpx.RequestError as e:
        die(f"Cannot reach {url}: {e}")
    except httpx.HTTPStatusError as e:
        die(f"{url} returned {e.response.status_code}: {e.response.text}")

def add_block_on_namenode(filename: str) -> Dict[str, Any]: #returns a dict containing the block number and the list of replica ids
    return request_json("POST", f"{NAMENODE_URL}/files/{filename}/blocks")

def extract_block_info(block_resp: Dict[str, Any]) -> Tuple[int, List[str]]: #returns a (block_number, replica_ids) tuple (for the block on that iteration)
    if "number" in block_resp:
        block_number = int(block_resp["number"])
    elif "block_number" in block_resp:
        block_number = int(block_resp["block_number"])
    else:
        die(f"Block response missing block number: {block_resp}")

    replica_ids = block_resp.get("replicas")
    if not isinstance(replica_ids, list) or not replica_ids:
        if "pipeline" in block_resp and isinstance(block_resp["pipeline"], list):
            replica_ids = block_resp["pipeline"]
        else:
            die(f"Block response missing replicas: {block_resp}")

    return block_number, [str(x) for x in replica_ids]

def put_block_to_datanode( #client to datanode: here's the block, store it, and also pass it along to 2 and 3
    filename: str,
    block_number: int,
    host: str,
    port: int,
    data: bytes,
    pipeline_q: Optional[str],
) -> None:
    url = f"http://{host}:{port}/files/{filename}/blocks/{block_number}"
    params = {"pipeline": pipeline_q} if pipeline_q else None
    files = {"file": ("block", data, "application/octet-stream")}
    r = httpx.put(url, params=params, files=files, timeout=20.0)
    r.raise_for_status()

def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: upload.py <source_path> <sshdfs_file_name>", file=sys.stderr)
        return 1

    source_path = Path(sys.argv[1])
    sshdfs_name = sys.argv[2]

    if not source_path.exists() or not source_path.is_file():
        die(f"Source path does not exist or is not a file: {source_path}")

    size_bytes = source_path.stat().st_size
    block_size = load_block_size_bytes()

    # 1 discover datanodes
    datanode_map = get_datanode_map()

    # 2 create file on namenode
    create_file_on_namenode(sshdfs_name)

    # 3 read local file in blocks and upload
    with source_path.open("rb") as f:
        block_index = 0
        while True:
            chunk = f.read(block_size)
            if not chunk:
                break

            # ask namenode to add a new block and get placement
            block_resp = add_block_on_namenode(sshdfs_name)

            # extract: block_number + full replicas list (from namenode)
            block_number, replica_ids = extract_block_info(block_resp)

            # failover: try each replica as the entry datanode
            last_err = None

            # fast path: pipeline from first replica to the rest
            try:
                entry_id = replica_ids[0]
                if entry_id not in datanode_map:
                    raise RuntimeError(f"Unknown datanode id {entry_id}")

                host, port = datanode_map[entry_id]
                pipeline_q = ",".join(replica_ids[1:]) if len(replica_ids) > 1 else None

                put_block_to_datanode(sshdfs_name, block_number, host, port, chunk, pipeline_q)

            except Exception as e:
                last_err = str(e)

                # fallback: direct PUT to each replica (no pipeline)
                success = 0

                for rid in replica_ids:
                    rid = str(rid)
                    if rid not in datanode_map:
                        last_err = f"Unknown datanode id {rid}"
                        continue

                    host, port = datanode_map[rid]

                    try:
                        put_block_to_datanode(sshdfs_name, block_number, host, port, chunk, pipeline_q=None)
                        success += 1
                    except Exception as e2:
                        last_err = str(e2)

                if success == 0:
                    die(
                        f"Upload failed for block {block_number}. "
                        f"Pipeline failed and all replicas failed. Last error: {last_err}"
                    )
                    
    print(f"Uploaded '{source_path}' as '{sshdfs_name}' in blocks of {block_size} bytes.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
