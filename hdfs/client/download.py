import sys
from pathlib import Path
from typing import Dict, Any, List, Tuple

import httpx

NAMENODE_URL = "http://localhost:8000"

def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)

def get_json(url: str) -> Any: #general error handling for GET requests
    try:
        r = httpx.get(url, timeout=10.0)
        r.raise_for_status()
        return r.json()
    except httpx.RequestError as e:
        die(f"Can't reach {url}: {e}")
    except httpx.HTTPStatusError as e:
        die(f"{url} returned {e.response.status_code}: {e.response.text}")
    except ValueError as e:
        die(f"{url} didn't return valid JSON: {e}")

def get_datanode_map() -> Dict[str, Tuple[str, int]]: #get list of datanodes from namenode and make dictionary with datanode_id, (host,port)
    #so client knows where are the replicas (so where to send a block)
    payload = get_json(f"{NAMENODE_URL}/datanodes")
    dns = payload.get("datanodes")
    if not isinstance(dns, list):
        die(f"Unexpected /datanodes response: {payload}")

    out: Dict[str, Tuple[str, int]] = {}
    for dn in dns:
        out[str(dn["id"])] = (dn["host"], int(dn["port"]))
    return out

def fetch_block_from_any_replica( #failover logic on download: for each replica, makes GET request to datanode
        #and return content of the block if success, otherwise goes to next replica.
    file_name: str,
    block_number: int,
    replica_ids: List[str],
    datanode_map: Dict[str, Tuple[str, int]],
) -> bytes:
    last_err = None
    for rid in replica_ids:
        rid = str(rid)
        if rid not in datanode_map:
            last_err = f"Unknown datanode id: {rid}"
            continue
        host, port = datanode_map[rid]
        url = f"http://{host}:{port}/files/{file_name}/blocks/{block_number}"
        try:
            r = httpx.get(url, timeout=15.0)
            r.raise_for_status()
            return r.content
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            last_err = f"{url} failed: {e}"
            continue
    die(f"All replicas failed for block {block_number}. Last error: {last_err}")

def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: download.py <hdfs_file_name> <destination_path>", file=sys.stderr)
        return 1
    file_name = sys.argv[1]
    dest_path = Path(sys.argv[2])
    datanode_map = get_datanode_map()

    # 1 get file metadata from namenode
    meta = get_json(f"{NAMENODE_URL}/files/{file_name}")
    blocks = None
    if isinstance(meta, dict):
        if "blocks" in meta:
            blocks = meta["blocks"]
        elif "file" in meta and isinstance(meta["file"], dict) and "blocks" in meta["file"]:
            blocks = meta["file"]["blocks"]
    if not isinstance(blocks, list):
        die(f"Unexpected file metadata format (missing blocks list): {meta}")

    # 2 download each block (in order) from any replica and write final file
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with dest_path.open("wb") as out:
        for b in blocks:
            if not isinstance(b, dict):
                die(f"Unexpected block entry: {b}")
            if "number" in b:
                block_number = int(b["number"])
            elif "block_number" in b:
                block_number = int(b["block_number"])
            else:
                die(f"Block entry missing number: {b}")
            replica_ids = b.get("replicas")
            if not isinstance(replica_ids, list) or not replica_ids:
                die(f"Block entry missing replicas: {b}")
            data = fetch_block_from_any_replica(file_name, block_number, replica_ids, datanode_map)
            out.write(data)
    print(f"Downloaded '{file_name}' to '{dest_path}'.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
