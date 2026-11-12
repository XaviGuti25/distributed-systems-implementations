import json
import os

class KRaft:
    def __init__(self):
        self.metadata_dir = os.getenv("METADATA_DIR", "/metadata")
        os.makedirs(self.metadata_dir, exist_ok=True)

        self.metadata_log_path = os.path.join(self.metadata_dir, "__cluster_metadata.log")
        self.quorum_state_path = os.path.join(self.metadata_dir, "quorum-state")

        # load quorum state
        self.quorum_state = self._load_quorum_state()

        # load topics
        self.topics = self._load_metadata_log()


    def _persist_quorum_state(self, quorum_state):
        with open(self.quorum_state_path, "w", encoding="utf-8") as f:
            json.dump(quorum_state, f)


    def _load_quorum_state(self):
        if not os.path.exists(self.quorum_state_path):
            default_state = {
                "leader_id": -1,
                "leader_epoch": 0,
                "voted_id": -1
            }

            with open(self.quorum_state_path, "w", encoding="utf-8") as f:
                json.dump(default_state, f)

            return default_state

        with open(self.quorum_state_path, "r", encoding="utf-8") as f:
            return json.load(f)


    def _load_metadata_log(self):
        topics = {}

        if not os.path.exists(self.metadata_log_path):
            return topics

        with open(self.metadata_log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                entry = json.loads(line)
                action = entry.get("action")
                body = entry.get("body", {})

                if action == "create_topic":
                    topic_name = body["topic_name"]
                    topics[topic_name] = body

                elif action == "delete_topic":
                    topic_name = body["topic_name"]
                    if topic_name in topics:
                        del topics[topic_name]

        return topics


    def append_to_metadata_log(self, action, body):
        entry = {
            "action": action,
            "body": body
        }

        with open(self.metadata_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        if action == "create_topic":
            self.topics[body["topic_name"]] = body

        elif action == "delete_topic":
            topic_name = body["topic_name"]
            if topic_name in self.topics:
                del self.topics[topic_name]


    def topic_exists(self, topic_name):
        return topic_name in self.topics


    def get_topic(self, topic_name):
        return self.topics.get(topic_name)


    def list_topics(self):
        return list(self.topics.values())


    def read_metadata_log(self, from_offset, max_batch_size):
        """
        Read metadata log entries starting from from_offset.
        Returns at most max_batch_size records.
        """
        records = []
        
        if not os.path.exists(self.metadata_log_path):
            return records
        
        with open(self.metadata_log_path, "r", encoding="utf-8") as f:
            for offset, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                
                # Skip records before from_offset
                if offset <= from_offset:
                    continue
                
                # Stop if we have enough records
                if len(records) >= max_batch_size:
                    break
                
                try:
                    entry = json.loads(line)
                    records.append({
                        "offset": offset,
                        "epoch": 0,  # Simplified: all entries have epoch 0 for now
                        "action": entry.get("action"),
                        "payload": entry.get("body", {})
                    })
                except Exception:
                    continue
        
        return records


    def get_voted(self):
        return self.quorum_state.get("voted_id", -1)


    def get_leader(self):
        return self.quorum_state.get("leader_id", -1)

    def get_leader_epoch(self):
        return self.quorum_state.get("leader_epoch", 0)

    def vote_for_candidate(
        self,
        candidate_epoch: int,
        candidate_last_offset: int,
        candidate_last_offset_epoch: int,
        candidate_id: str
    ):

        current_epoch = self.quorum_state["leader_epoch"]
        voted_id = self.quorum_state["voted_id"]

        if candidate_epoch < current_epoch:
            return False

        if candidate_epoch > current_epoch:
            self.quorum_state["leader_epoch"] = candidate_epoch
            self.quorum_state["voted_id"] = -1
            self._persist_quorum_state(self.quorum_state)
            voted_id = -1

        if voted_id != -1 and voted_id != candidate_id:
            return False

      
        self.quorum_state["voted_id"] = candidate_id
        self._persist_quorum_state(self.quorum_state)

        return True


    def new_leader(self, leader_id: str, leader_epoch: int) -> bool:
        """
        4.3.3 - NEW LEADER
        Called when a broker wins the election and becomes the new leader.
        
        Updates quorum state with:
        - leader_id: Who's the leader now
        - leader_epoch: Which election round this is (so we know it's newer info)
        - voted_id: -1 (reset because we found a leader)
        """
        self.quorum_state["leader_id"] = leader_id
        self.quorum_state["leader_epoch"] = leader_epoch
        self.quorum_state["voted_id"] = -1
        self._persist_quorum_state(self.quorum_state)  # Save to disk so it survives restart
        return True


    def become_candidate(self, broker_id: str) -> int:
        """
        4.3.4 - BECOME A CANDIDATE
        Called when this broker wants to run for leader.
        
        Steps:
        1. Increment the epoch (election round)
        2. Vote for yourself
        3. Save to disk
        
        Returns the new epoch so others know you're running.
        """
        self.quorum_state["leader_epoch"] += 1
        self.quorum_state["voted_id"] = broker_id  # Vote for yourself!
        self._persist_quorum_state(self.quorum_state)
        return self.quorum_state["leader_epoch"]


    def read_metadata_log(self, from_offset, max_batch_size):
        """
        4.3.5 - READ METADATA LOG
        Followers call this to ask the leader for new topic metadata.
        
        Leader returns records starting from from_offset:
        - up to max_batch_size records
        - Each record has: offset, epoch, action (create/delete topic), payload
        
        Followers then write these to their own metadata log to stay in sync.
        """
