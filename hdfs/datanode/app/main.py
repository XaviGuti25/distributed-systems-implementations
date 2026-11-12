from fastapi import FastAPI, UploadFile, File, HTTPException
from pathlib import Path
from fastapi.responses import FileResponse
from typing import Optional, List
import httpx

app = FastAPI(title="datanode")

@app.get("/healthcheck")
def healthcheck():
    return {"status": "up"}

BASE_STORAGE = Path("/code/datanode/storage") #container adress for block storage

#converts string into list just for format issues
def parse_pipeline(pipeline: Optional[str]) -> List[int]: #helper function for 1.3.4
    """
    pipeline is expected like "2,3" (datanode ids).
    Returns [2,3]. Empty/None -> [].
    """
    if not pipeline:
        return []
    parts = [p.strip() for p in pipeline.split(",") if p.strip()]
    out: List[int] = []
    for p in parts:
        if not p.isdigit():
            raise HTTPException(status_code=400, detail="Invalid pipeline")
        out.append(int(p))
    return out

def pipeline_to_str(p: List[int]) -> str: #helper function for 1.3.4
    return ",".join(str(x) for x in p)

def safe_filename(name: str) -> str:
    if name.startswith(("/", "\\")) or ".." in name or "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return name

@app.put("/files/{filename}/blocks/{block_number}") #recieve block from client store locally and if needed replicate on other datanodes
async def put_block(filename: str, block_number: int, file: UploadFile = File(...), pipeline: Optional[str] = None,):
    
    # 1 stores locally
    filename = safe_filename(filename)
    block_dir = BASE_STORAGE / filename # build path: datanode/storage/<filename>/<block_number>
    block_dir.mkdir(parents=True, exist_ok=True)
    block_path = block_dir / str(block_number)

    data = await file.read()
    if data is None:
        raise HTTPException(status_code=400, detail="Empty upload")

    # to write bytes
    with open(block_path, "wb") as f:
        f.write(data)

    # 2 writes pipeline
    #call put_block but on next datanode in pipeline
    p = parse_pipeline(pipeline)
    if p:
        next_id = p[0]
        rest = p[1:]

        # In Compose, use service name as hostname
        next_url = f"http://datanode{next_id}:80/files/{filename}/blocks/{block_number}"
        params = {}
        if rest:
            params["pipeline"] = pipeline_to_str(rest)

        # forward same bytes as multipart/form-data ("file" field)
        files = {"file": (file.filename or "block", data, file.content_type or "application/octet-stream")}

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.put(next_url, params=params, files=files)
                r.raise_for_status()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Pipeline forward failed: {e}")

    return {"status": "stored", "filename": filename, "block_number": block_number, "bytes": len(data)}

@app.get("/files/{filename}/blocks/{block_number}")
def get_block(filename: str, block_number: int): #reading a block from the datanode file system

    filename = safe_filename(filename)  # reuse existing validator

    block_path = BASE_STORAGE / filename / str(block_number)

    if not block_path.exists() or not block_path.is_file():
        raise HTTPException(status_code=404, detail="Block not found")

    return FileResponse( # it returns the raw bytes as a downloadable file
    
        path=str(block_path),
        media_type="application/octet-stream",
        filename=f"{filename}.block.{block_number}",
    )
