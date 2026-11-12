import os
import asyncio
import json
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.kraft import KRaft

app = FastAPI()

BROKER_ID = int(os.getenv("BROKER_ID", "1"))
LEADER_BROKER_ID = int(os.getenv("LEADER_BROKER_ID", "1"))
ALL_BROKER_IDS = os.getenv("ALL_BROKER_IDS", "1,2,3,4,5").split(",")
DATA_DIR = os.getenv("DATA_DIR", "/data")
BROKERS = os.getenv("BROKERS", "1@broker-1:80,2@broker-2:80,3@broker-3:80,4@broker-4:80,5@broker-5:80")

kraft = KRaft()

# 4.3.8 - Leader failure detection: Track consecutive replication failures
# If leader appears dead (N consecutive failures), trigger new election
replication_failure_count = 0
REPLICATION_FAILURE_THRESHOLD = 2  # 2 failures = ~2 seconds (loop runs every 1s) - more aggressive


# Helper function to get current leader from quorum state, fallback to env var
def get_current_leader():
    """Get the leader from quorum state, or fallback to LEADER_BROKER_ID from env"""
    leader = kraft.get_leader()
    return leader if leader != -1 else LEADER_BROKER_ID


class CreateTopicRequest(BaseModel):
    topic_name: str
    partition_count: int


class ProduceRequest(BaseModel):
    topic_partition: str
    key: str
    payload: str
    acks: str | None = None


class ConsumeRequest(BaseModel):
    topic_partition: str
    last_offset: int
    max_batch_size: int
    follower_broker_id: str | None = None


# NEW MODEL FOR VOTING
class VoteRequest(BaseModel):
    candidate_epoch: int
    last_offset: int
    last_offset_epoch: int
    candidate_id: str


# NEW MODEL FOR BEGINNING QUORUM EPOCH
class BeginQuorumEpochRequest(BaseModel):
    leader_epoch: int
    leader_id: str


# NEW MODEL FOR FETCHING METADATA
class FetchMetadataRequest(BaseModel):
    last_offset: int
    last_offset_epoch: int
    max_batch_size: int
    follower_broker_id: str


@app.get("/healthcheck")
def healthcheck():
    leader_id = kraft.get_leader()
    return {
        "status": "up",
        "broker_id": BROKER_ID,
        "leader_broker_id": leader_id if leader_id != -1 else LEADER_BROKER_ID,
    }


# ---------------------------
# KRaft Control Plane
# ---------------------------

# Handles incoming vote requests from a candidate broker.
# Decides whether to grant a vote based on epoch, prior votes, and log freshness.
# Ensures only one valid leader can be elected per term.

@app.post("/kraft/v1/voteRequest")
def vote_request(request: VoteRequest):

    granted = kraft.vote_for_candidate(
        request.candidate_epoch,
        request.last_offset,
        request.last_offset_epoch,
        request.candidate_id
    )

    if granted:
        return {
            "granted": True
        }

    return {
        "granted": False,
        "leader_epoch": kraft.quorum_state["leader_epoch"],
        "leader_id": kraft.quorum_state["leader_id"]
    }

# ==========================
# 4.3.5 REPLICATING METADATA
# ==========================
# Purpose: Allow followers to fetch new topics from the leader
# The leader keeps all metadata changes in __cluster_metadata.log
# Followers periodically ask for new changes and store them locally

@app.post("/kraft/v1/fetchMetadata")
def fetch_metadata(request: FetchMetadataRequest):
    """
    LEADER ONLY: Return metadata log entries to followers.
    
    Followers call this every second asking:
    "Leader, I have offset X, what's new since then?"
    
    Leader responds with all new entries so followers can catch up.
    """
    leader = get_current_leader()
    if BROKER_ID != leader:
        raise HTTPException(
            status_code=421,
            detail=f"leader is {leader}, can't accept"
        )
    
    records = kraft.read_metadata_log(request.last_offset, request.max_batch_size)
    
    return {
        "records": records
    }


# ---------------------------
# Admin API
# ---------------------------

#append new topic in metadata log file (only leader can do this) with n partitions
@app.post("/admin/v1/topics", status_code=201)
def create_topic(request: CreateTopicRequest):
    leader = get_current_leader()
    if BROKER_ID != leader:
        raise HTTPException(
            status_code=421,
            detail=f"leader is {leader}, can't accept"
        )

    if "-" in request.topic_name:
        raise HTTPException(
            status_code=400,
            detail="topic name can't contain dashes"
        )

    if kraft.topic_exists(request.topic_name):
        raise HTTPException(
            status_code=409,
            detail=f"topic {request.topic_name} already exists"
        )

    partitions = []
    for i in range(1, request.partition_count + 1):
        partitions.append({
            "id": f"{request.topic_name}-{i}",
            "replica_brokers": ALL_BROKER_IDS
        })

    topic = {
        "topic_name": request.topic_name,
        "partitions": partitions
    }

    kraft.append_to_metadata_log("create_topic", topic)

    return topic

#if i am leader build current state by replaying all actions in metadata log and return all topics
@app.get("/admin/v1/topics")
def list_topics():
    leader = get_current_leader()
    if BROKER_ID != leader:
        raise HTTPException(
            status_code=421,
            detail=f"leader is {leader}, can't accept"
        )

    return {
        "topics": kraft.list_topics()
    }

#if i am leader append action to delete topic with topic name in metadata log file
@app.delete("/admin/v1/topics/{topic_name}", status_code=204)
def delete_topic(topic_name: str):
    leader = get_current_leader()
    if BROKER_ID != leader:
        raise HTTPException(
            status_code=421,
            detail=f"leader is {leader}, can't accept"
        )

    if not kraft.topic_exists(topic_name):
        raise HTTPException(
            status_code=404,
            detail=f"topic {topic_name} does not exist"
        )

    kraft.append_to_metadata_log("delete_topic", {"topic_name": topic_name})
    return


# ---------------------------
# Data Plane Helpers
# ---------------------------

def get_partition_log_path(topic_partition: str) -> str:
    partition_dir = os.path.join(DATA_DIR, topic_partition)
    os.makedirs(partition_dir, exist_ok=True)
    return os.path.join(partition_dir, "00000000000000000000.log")


def get_next_offset(log_path: str) -> int:
    if not os.path.exists(log_path):
        return 0

    last_non_empty_line = None
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                last_non_empty_line = line

    if last_non_empty_line is None:
        return 0

    first_space = last_non_empty_line.find(" ")
    if first_space == -1:
        raise ValueError("invalid partition log line: missing offset separator")

    last_offset = int(last_non_empty_line[:first_space])
    return last_offset + 1


def topic_partition_exists(topic_partition: str) -> bool:
    if "-" not in topic_partition:
        return False

    topic_name, partition_number_str = topic_partition.rsplit("-", 1)
    if not partition_number_str.isdigit():
        return False

    topic = kraft.get_topic(topic_name)
    if topic is None:
        return False

    return any(p["id"] == topic_partition for p in topic["partitions"])


# ---------------------------
# Partition Log Reading
# ---------------------------

def read_partition_records(log_path: str):
    records = []

    if not os.path.exists(log_path):
        return records

    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue

            parts = line.split(" ", 2)
            if len(parts) != 3:
                continue

            offset_str, key, payload = parts
            records.append({
                "offset": int(offset_str),
                "key": key,
                "payload": payload,
            })

    return records


# ---------------------------
# Broker Utilities
# ---------------------------

def parse_brokers(brokers_str: str):
    brokers = []
    for item in brokers_str.split(","):
        broker_id, address = item.split("@", 1)
        brokers.append({
            "broker_id": broker_id,
            "address": address,
        })
    return brokers


def get_broker_address(broker_id: int | str):
    broker_id = str(broker_id)
    for broker in parse_brokers(BROKERS):
        if broker["broker_id"] == broker_id:
            return broker["address"]
    return None


def get_local_last_offset(topic_partition: str) -> int:
    log_path = get_partition_log_path(topic_partition)
    records = read_partition_records(log_path)
    if not records:
        return -1
    return records[-1]["offset"]


def append_record_if_missing(topic_partition: str, offset: int, key: str, payload: str):
    log_path = get_partition_log_path(topic_partition)
    local_last_offset = get_local_last_offset(topic_partition)

    if offset <= local_last_offset:
        return

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{offset} {key} {payload}\n")


# ---------------------------
# Produce API
# ---------------------------
#if i am leadeer and topic+partition exists get the log file  where to store the data if folder does not exist we create it
#we look at the last message offset and append the new one in the next one (append only)
@app.post("/data/v1/produce", status_code=204)
def produce(request: ProduceRequest):
    leader = get_current_leader()
    if BROKER_ID != leader:
        raise HTTPException(
            status_code=421,
            detail=f"leader is {leader}, can't accept"
        )

    if not topic_partition_exists(request.topic_partition):
        raise HTTPException(
            status_code=404,
            detail=f"topic-partition {request.topic_partition} does not exist"
        )

    log_path = get_partition_log_path(request.topic_partition)
    next_offset = get_next_offset(log_path)

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{next_offset} {request.key} {request.payload}\n")

    return


# ---------------------------
# Consume API
# ---------------------------

#A client sends a consume request with a topic-partition, an offset (last_offset), and how many messages it wants. The request must go to the leader broker, which validates the partition and reads the corresponding log file. It then returns the messages that come after the given offset, up to the requested batch size, along with a new offset. This new offset acts as a bookmark that the client uses in the next request to continue reading from where it left off.
@app.post("/data/v1/consume")
def consume(request: ConsumeRequest):

    leader = get_current_leader()
    if BROKER_ID != leader:
        raise HTTPException(
            status_code=421,
            detail=f"leader is {leader}, can't accept"
        )

    if not topic_partition_exists(request.topic_partition):
        raise HTTPException(
            status_code=404,
            detail=f"topic-partition {request.topic_partition} does not exist"
        )

    log_path = get_partition_log_path(request.topic_partition)
    records = read_partition_records(log_path)

    if request.last_offset == -1:
        selected_records = records[:request.max_batch_size]
    else:
        selected_records = []
        reached_last_offset = False

        for record in records:
            if reached_last_offset:
                selected_records.append(record)
                if len(selected_records) >= request.max_batch_size:
                    break

            if record["offset"] == request.last_offset:
                reached_last_offset = True

    if not selected_records:
        return {
            "last_offset": -1,
            "records": []
        }

    return {
        "last_offset": selected_records[-1]["offset"],
        "records": selected_records
    }


# ---------------------------
# Replication
# ---------------------------

# Data replication is performed by followers pulling new records from the leader.
# Each follower periodically checks the leader for all topics and partitions,
# determines the last offset it has locally, and requests any new messages after that offset.
# The leader returns the missing records, and the follower appends them to its own log.
# This ensures all brokers maintain the same data, providing durability and fault tolerance.

async def replicate_from_leader_once():
    """
    4.2.3 - DATA PARTITION REPLICATION
    4.3.8 - DETECT LEADER FAILURE & TRIGGER ELECTION
    
    FOLLOWERS ONLY: Replicate data partitions from the leader.
    If leader is unreachable multiple times, assume it's dead and trigger election.
    """
    global replication_failure_count

    current_leader = get_current_leader()
    if BROKER_ID == current_leader:
        return  # Leaders don't replicate from themselves

    leader_address = get_broker_address(current_leader)

    if leader_address is None:
        # Can't find leader address
        replication_failure_count += 1
        if replication_failure_count >= REPLICATION_FAILURE_THRESHOLD:
            kraft.quorum_state["leader_id"] = -1
            kraft._persist_quorum_state(kraft.quorum_state)
            replication_failure_count = 0
        return

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:

            topics_response = await client.get(
                f"http://{leader_address}/admin/v1/topics"
            )

            if topics_response.status_code != 200:
                # Leader didn't respond
                replication_failure_count += 1
                if replication_failure_count >= REPLICATION_FAILURE_THRESHOLD:
                    kraft.quorum_state["leader_id"] = -1
                    kraft._persist_quorum_state(kraft.quorum_state)
                    replication_failure_count = 0
                return

            # SUCCESS! Reset failure counter
            replication_failure_count = 0

            topics_payload = topics_response.json()
            topics = topics_payload.get("topics", [])

            for topic in topics:
                for partition in topic.get("partitions", []):

                    topic_partition = partition["id"]
                    last_offset = get_local_last_offset(topic_partition)

                    consume_payload = {
                        "topic_partition": topic_partition,
                        "last_offset": last_offset,
                        "max_batch_size": 100,
                        "follower_broker_id": None
                    }

                    consume_response = await client.post(
                        f"http://{leader_address}/data/v1/consume",
                        json=consume_payload
                    )

                    if consume_response.status_code != 200:
                        continue

                    consume_data = consume_response.json()
                    records = consume_data.get("records", [])

                    for record in records:
                        append_record_if_missing(
                            topic_partition=topic_partition,
                            offset=record["offset"],
                            key=record["key"],
                            payload=record["payload"]
                        )

    except Exception:
        # Connection error, network timeout, etc - leader is likely unreachable
        replication_failure_count += 1
        if replication_failure_count >= REPLICATION_FAILURE_THRESHOLD:
            kraft.quorum_state["leader_id"] = -1
            kraft._persist_quorum_state(kraft.quorum_state)
            replication_failure_count = 0
        return

#Followers periodically fetch new metadata log entries from the leader using offsets and append them locally to maintain a consistent view of topics.

async def replicate_metadata_from_leader_once():
    """
    4.3.5 - FETCH & APPLY NEW METADATA FROM LEADER
    4.3.8 - DETECT LEADER FAILURE & TRIGGER ELECTION
    
    FOLLOWERS ONLY: Called every second to ask leader for new topic metadata.
    
    What happens:
    1. Check if this broker is the leader (if yes, skip - leader doesn't replicate)
    2. Find the leader's address
    3. Count how many entries are in our local metadata log
    4. Call leader's /fetchMetadata endpoint: "I have offset X, what's new?"
    5. Leader returns new entries since offset X
    6. Apply each new entry to our local metadata log
    
    FAILURE HANDLING (4.3.8):
    - If replication succeeds: reset failure counter (leader is alive!)
    - If replication fails: increment failure counter
    - If failures exceed threshold: assume leader is dead, trigger new election
    
    Result: All brokers stay in sync, and dead leaders are automatically replaced!
    """
    global replication_failure_count
    
    if BROKER_ID == get_current_leader():
        return  # Leader doesn't need to replicate from itself
    
    leader_address = get_broker_address(get_current_leader())
    
    if leader_address is None:
        # Can't find leader address - record failure
        replication_failure_count += 1
        if replication_failure_count >= REPLICATION_FAILURE_THRESHOLD:
            # Leader hasn't responded in 5 seconds - assume it's dead!
            kraft.quorum_state["leader_id"] = -1
            kraft._persist_quorum_state(kraft.quorum_state)
            replication_failure_count = 0
        return
    
    try:
        # Get last offset from our metadata log
        last_local_offset = -1
        if os.path.exists(kraft.metadata_log_path):
            with open(kraft.metadata_log_path, "r", encoding="utf-8") as f:
                line_count = sum(1 for line in f if line.strip())
                if line_count > 0:
                    last_local_offset = line_count - 1
        
        async with httpx.AsyncClient(timeout=5.0) as client:
            metadata_response = await client.post(
                f"http://{leader_address}/kraft/v1/fetchMetadata",
                json={
                    "last_offset": last_local_offset,
                    "last_offset_epoch": -1,
                    "max_batch_size": 100,
                    "follower_broker_id": str(BROKER_ID)
                }
            )
            
            if metadata_response.status_code != 200:
                # Leader didn't respond properly
                replication_failure_count += 1
                if replication_failure_count >= REPLICATION_FAILURE_THRESHOLD:
                    kraft.quorum_state["leader_id"] = -1
                    kraft._persist_quorum_state(kraft.quorum_state)
                    replication_failure_count = 0
                return
            
            # SUCCESS! Reset failure counter (leader is alive)
            replication_failure_count = 0
            
            response_data = metadata_response.json()
            records = response_data.get("records", [])
            
            # Apply each metadata record to our log
            for record in records:
                action = record.get("action")
                payload = record.get("payload", {})
                
                # Append to metadata log
                kraft.append_to_metadata_log(action, payload)
    
    except Exception:
        return


async def background_replication_loop():
    while True:
        await replicate_from_leader_once()
        await asyncio.sleep(1)


async def background_metadata_replication_loop():
    """Fetch metadata from leader every second"""
    while True:
        await replicate_metadata_from_leader_once()
        await asyncio.sleep(1)


# ==========================
# 4.3.4 AUTOMATIC ELECTIONS
# ==========================
# Purpose: When brokers start with no leader (leader_id == -1), they automatically elect one
# 
# How it works:
# 1. Each broker waits 1-3 random seconds (so they don't all vote at same time)
# 2. Each broker becomes a candidate and asks all others for votes
# 3. First broker to get MAJORITY of votes becomes the leader
# 4. Winner notifies everyone via beginQuorumEpoch
# 5. All brokers are now in sync with same leader
#
# This makes the system self-healing: if one broker crashes, others will elect a new leader!

# ---------------------------
# Election Logic
# ---------------------------
# Sends vote requests to all other brokers in the cluster.
# Collects and counts how many brokers grant their vote to the candidate.
# Returns total votes to determine if majority is achieved.
async def collect_votes(candidate_broker_id: str, candidate_epoch: int, last_offset: int, last_offset_epoch: int):
    """
    4.3.4 - COLLECT VOTES
    A candidate broker asks all other brokers to vote for it.
    
    Returns (votes_count, total_brokers) so we can check if we won.
    """
    votes = 1  # Vote for yourself (you always vote for yourself!)
    
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            for broker_id_str in ALL_BROKER_IDS:
                broker_id_str = broker_id_str.strip()
                if broker_id_str == candidate_broker_id:
                    continue  # Skip yourself
                
                broker_address = get_broker_address(broker_id_str)
                if broker_address is None:
                    continue
                
                try:
                    response = await client.post(
                        f"http://{broker_address}/kraft/v1/voteRequest",
                        json={
                            "candidate_epoch": candidate_epoch,
                            "last_offset": last_offset,
                            "last_offset_epoch": last_offset_epoch,
                            "candidate_id": candidate_broker_id
                        },
                        timeout=3.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get("granted"):
                            votes += 1
                except Exception:
                    continue
    except Exception:
        pass
    
    return votes, len(ALL_BROKER_IDS)

# Attempts to become the leader of the cluster.
# Becomes a candidate, collects votes, and checks for majority.
# If successful, announces itself as leader to all brokers.
async def try_become_leader(candidate_broker_id: str):
    """
    4.3.4 - TRY TO WIN AN ELECTION
    
    Steps:
    1. Become a candidate (increment epoch, vote for yourself)
    2. Collect votes from all other brokers
    3. If we get majority (F+1 out of 2F+1), we won!
    4. Notify everyone else that we're the leader
    """
    # Step 1: Declare candidacy
    candidate_epoch = kraft.become_candidate(candidate_broker_id)
    
    # Get my metadata log info (to prove we're up-to-date)
    log_path = kraft.metadata_log_path
    last_offset = -1
    
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            if lines:
                last_offset = len(lines) - 1
    
    last_offset_epoch = 0  # For simplicity
    
    # Step 2: Ask everyone for votes
    votes, total_brokers = await collect_votes(candidate_broker_id, candidate_epoch, last_offset, last_offset_epoch)
    majority = (total_brokers // 2) + 1
    
    # Step 3: Did we win? (Check if we have majority votes)
    if votes >= majority:
        # Step 4: Tell everyone we're the leader!
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                for broker_id_str in ALL_BROKER_IDS:
                    broker_id_str = broker_id_str.strip()
                    if broker_id_str == candidate_broker_id:
                        continue
                    
                    broker_address = get_broker_address(broker_id_str)
                    if broker_address is None:
                        continue
                    
                    try:
                        # Use beginQuorumEpoch to announce the new leader
                        await client.post(
                            f"http://{broker_address}/kraft/v1/beginQuorumEpoch",
                            json={
                                "leader_epoch": candidate_epoch,
                                "leader_id": candidate_broker_id
                            },
                            timeout=3.0
                        )
                    except Exception:
                        pass
        except Exception:
            pass
        
        return True  # We won!
    
    return False  # We lost

# Continuously monitors if a leader exists in the system.
# If no leader is detected, waits briefly and triggers a new election attempt.
# Ensures automatic recovery from leader failures.
async def election_loop():
    """
    4.3.4 - AUTOMATIC ELECTION LOOP
    
    Runs on EVERY broker in the background. Continuously checks:
    - Is there a leader right now?
    - If NO leader:
      - Wait 1-3 random seconds
      - Try to become a candidate
    
    This way, if the leader dies, followers automatically elect a new one!
    """
    import random
    
    while True:
        try:
            # Check if there's a leader
            leader = kraft.get_leader()
            
            if leader == -1:
                # No leader, wait random time and try to become one
                wait_time = random.uniform(1, 3)
                await asyncio.sleep(wait_time)
                
                # Check again if someone else became leader
                leader = kraft.get_leader()
                if leader == -1:
                    # Try to become leader
                    won = await try_become_leader(str(BROKER_ID))
                    
                    if won:
                        # Update local LEADER_BROKER_ID
                        global LEADER_BROKER_ID
                        LEADER_BROKER_ID = BROKER_ID
            
            await asyncio.sleep(1)
        except Exception:
            await asyncio.sleep(1)

# ==========================
# 4.3.3 BEGINNING A QUORUM
# ==========================
# Purpose: A newly elected leader announces itself to all brokers
# When a broker wins an election, it calls this endpoint to tell everyone:
# "Hey! I'm the new leader!"
# 
# Announces that a broker has won the election and is now the leader.
# Updates the local quorum state with the new leader_id and leader_epoch.
# Makes all brokers agree on the same leader.
@app.post("/kraft/v1/beginQuorumEpoch")
def begin_quorum_epoch(request: BeginQuorumEpochRequest):
    """
    Called when a broker wins the election.
    Stores the new leader info in the quorum state file.
    """
    global LEADER_BROKER_ID
    
    kraft.new_leader(request.leader_id, request.leader_epoch)
    LEADER_BROKER_ID = int(request.leader_id)
    
    return {
        "leader_epoch": request.leader_epoch,
        "leader_id": request.leader_id
    }


# ==========================
# 4.3.9 FOLLOWER RECOVERY
# ==========================
# Purpose: When a follower recovers from being disconnected, the leader keeps retrying
# to announce itself until the follower responds
#
# How it works:
# 1. If this broker is the leader, continuously try to announce itself to all followers
# 2. For each follower that hasn't responded yet, keep retrying every 2 seconds
# 3. Stop retrying a follower once it responds successfully
# 4. Stop the loop if this broker is no longer the leader
#
# This ensures recovered followers rejoin without causing elections!

async def leader_follower_recovery_loop():
    """
    4.3.9 - RETRY SENDING LEADER ANNOUNCEMENT TO RECONNECTING FOLLOWERS
    
    The leader keeps track of which followers have acknowledged the leadership announcement.
    For any follower that hasn't responded yet, keep retrying every 2 seconds.
    Once a follower responds, stop retrying for that follower.
    Stop the entire loop if this broker stops being the leader.
    """
    acknowledged_followers = set()  # Track which followers have acknowledged
    
    while True:
        try:
            # Check if I'm the leader
            current_leader = kraft.get_leader()
            
            if current_leader == BROKER_ID:
                # I'm the leader! Retry announcing to all followers
                
                for broker_id_str in ALL_BROKER_IDS:
                    broker_id_str = broker_id_str.strip()
                    if broker_id_str == str(BROKER_ID):
                        continue  # Don't announce to myself
                    
                    if broker_id_str in acknowledged_followers:
                        continue  # Already acknowledged, skip
                    
                    # Get the follower's address
                    broker_address = get_broker_address(broker_id_str)
                    if broker_address is None:
                        continue
                    
                    # Try to announce leadership to this follower
                    try:
                        async with httpx.AsyncClient(timeout=2.0) as client:
                            response = await client.post(
                                f"http://{broker_address}/kraft/v1/beginQuorumEpoch",
                                json={
                                    "leader_epoch": kraft.get_leader_epoch(),
                                    "leader_id": str(BROKER_ID)
                                },
                                timeout=2.0
                            )
                            
                            if response.status_code == 200:
                                # Follower responded successfully!
                                acknowledged_followers.add(broker_id_str)
                    except Exception:
                        # Follower didn't respond yet, will retry in next iteration
                        pass
            else:
                # I'm not the leader anymore, reset and wait
                acknowledged_followers.clear()
            
            await asyncio.sleep(2)  # Retry every 2 seconds
        except Exception:
            await asyncio.sleep(2)


@app.on_event("startup")
async def schedule_periodic_replication():
    loop = asyncio.get_event_loop()
    loop.create_task(background_replication_loop())
    loop.create_task(background_metadata_replication_loop())
    loop.create_task(election_loop())
    loop.create_task(leader_follower_recovery_loop())  # 4.3.9 - Retry announcing to reconnecting followers
