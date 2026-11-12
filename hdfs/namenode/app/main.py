import json
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

SETTINGS_PATH = "settings.json"
CHECKPOINT_PATH = "checkpoint.json"
JOURNAL_PATH = "journal.log"

image = {}

def load_settings():
    with open(SETTINGS_PATH, "r") as f:
        return json.load(f)


def save_checkpoint(data): #save checkpoint
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump(data, f)


def append_journal(entry: dict):
    with open(JOURNAL_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")


def load_journal():
    if not os.path.exists(JOURNAL_PATH):
        return []

    with open(JOURNAL_PATH, "r") as f:
        return [json.loads(line) for line in f if line.strip()]


def truncate_journal(): #we delete all actions already persisted in checkpoint
    open(JOURNAL_PATH, "w").close()


def apply_journal_entry(entry: dict): #apply change from journal to image
    action = entry["action"]

    if action == "create-file":
        file = entry["file"]
        image[file["file_name"]] = file

    elif action == "add-block":
        image[entry["file_name"]]["blocks"].append(entry["block"])

    elif action == "delete-file":
        image.pop(entry["file_name"], None)


def initialize_image(): #we load the last stored image from checkpoint and apply recent changes from journal
    global image

    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH, "r") as f:
            image = json.load(f)
    else:
        image = {}

    for entry in load_journal():
        apply_journal_entry(entry)

    save_checkpoint(image)
    truncate_journal()


@app.on_event("startup")
def startup_event():
    initialize_image()



class CreateFileRequest(BaseModel):
    file_name: str



@app.get("/healthcheck") #heartbeat
def healthcheck():
    return {"status": "up"}


@app.get("/datanodes")
def list_datanodes():
    settings = load_settings()
    return {"datanodes": settings["datanodes"]}


@app.post("/files")
def create_file(req: CreateFileRequest): #creates metadata for new file
    file_name = req.file_name.strip()

    if not file_name:
        raise HTTPException(status_code=400, detail="File name cannot be empty")

    if file_name in image:
        raise HTTPException(status_code=400, detail="File already exists")

    settings = load_settings()

    file_metadata = {
        "file_name": file_name,
        "block_size_bytes": settings["block_size_bytes"],
        "blocks": []
    }

    append_journal({
        "action": "create-file",
        "file": file_metadata
    })

    image[file_name] = file_metadata
    save_checkpoint(image)

    return file_metadata


@app.get("/files/{filename}")
def get_file(filename: str): #gets metadata of a file
    if filename not in image:
        raise HTTPException(status_code=404, detail="File not found")

    return image[filename]


@app.post("/files/{filename}/blocks")
def add_block(filename: str): #adds block to file and decides where to store
    if filename not in image:
        raise HTTPException(status_code=404, detail="File not found")

    settings = load_settings()
    datanodes = settings["datanodes"]
    replication_factor = settings["replication_factor"]

    blocks = image[filename]["blocks"]
    block_number = len(blocks)

    replicas = [dn["id"] for dn in datanodes[:replication_factor]]

    new_block = {
        "number": block_number,
        "replicas": replicas
    }

    append_journal({
        "action": "add-block",
        "file_name": filename,
        "block": new_block
    })

    image[filename]["blocks"].append(new_block)
    save_checkpoint(image)

    return new_block


@app.delete("/files/{filename}")
def delete_file(filename: str): #removes file from system
    if filename not in image:
        raise HTTPException(status_code=404, detail="File not found")

    append_journal({
        "action": "delete-file",
        "file_name": filename
    })

    deleted_file = image.pop(filename)
    save_checkpoint(image)

    return deleted_file

