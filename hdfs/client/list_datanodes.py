import sys
import httpx

NAMENODE_URL = "http://localhost:8000"

def main() -> int: #Makes one GET request to the namenode, iterates the list, and prints each one as id@host:port
    url = f"{NAMENODE_URL}/datanodes"
    try:
        r = httpx.get(url, timeout=10.0)
        r.raise_for_status()
    except httpx.RequestError as e:
        print(f"ERROR: cannot reach namenode at {url}: {e}", file=sys.stderr)
        return 1
    except httpx.HTTPStatusError as e:
        print(f"ERROR: namenode returned {e.response.status_code}: {e.response.text}", file=sys.stderr)
        return 1
    payload = r.json()
    datanodes = payload.get("datanodes")

    if not isinstance(datanodes, list):
        print(f"ERROR: unexpected response (expected {{'datanodes': [...]}}): {payload}", file=sys.stderr)
        return 1

    for i, dn in enumerate(datanodes):
        try:
            print(f'{dn["id"]}@{dn["host"]}:{dn["port"]}')
        except Exception as e:
            print(f"ERROR on element {i}: {dn} -> {e}", file=sys.stderr)
            return 1

if __name__ == "__main__":
    raise SystemExit(main())
