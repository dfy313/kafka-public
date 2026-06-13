import os, hashlib, json, threading, requests
from pathlib import Path
from kafkaesque.structs import Color

# =============== ENV CONFIG ===============
BROKER_NAME                 = os.getenv("BROKER_NAME", "default_broker") 
LEADER_ADDR                 = os.getenv("LEADER_ADDR", "localhost:19092")
BASE_DIR                    = Path(f".var/kafkaesque/{BROKER_NAME}").resolve()
SESSION_TIMEOUT_SECONDS     = 25

# =============== DATA DIRECTORY HELPERS ===============
def ensure_data_dirs():
    BASE_DIR.mkdir(parents=True, exist_ok=True)

def ensure_partition_files(topic: str, partitions: int) -> None:
    topic_directory = BASE_DIR / topic
    topic_directory.mkdir(parents=True, exist_ok=True)
    for pid in range(partitions):
        partition_file(topic, pid).touch(exist_ok=True)

def partition_file(topic: str, pid: int) -> Path:
    return BASE_DIR / topic / f"{pid}.log"

def choose_partition(topic_cfg: dict, key) -> int:
    parts = int(topic_cfg["partitions"])
    return int.from_bytes(hashlib.md5(key.encode()).digest()[:4], "big") % parts

def choose_coordinator_partition(consumer_group_id: str, num_partitions: int) -> int:
    hash_bytes = hashlib.md5(consumer_group_id.encode()).digest()
    return int.from_bytes(hash_bytes, byteorder='big') % num_partitions

# =============== LOG READING UTILITIES  ===============
def get_committed_offset_from_log(consumer_group_id, topic, partition_id, offset_file):
    """Simple linear scan. Can use mmap + bucketing to optimize lookups"""
    target_key = f"{consumer_group_id}|{topic}|{partition_id}"
    if (not offset_file.exists()) or offset_file.stat().st_size == 0: return -1
    with offset_file.open("r", encoding="utf-8") as f:
        lines = f.readlines()
    for line in reversed(lines):
        record = json.loads(line)
        if record.get("key") == target_key:
            return int(record.get("offset"))
    return -1

def read_records_from(partition_file_path, start, limit = None):
    """Can use mmap + bucketing for more efficient reading"""
    output = []
    with partition_file_path.open("r", encoding="utf-8") as f:
        for _ in range(start): # Skip first `start` lines
            if not f.readline():
                return []  # File shorter than `start`
        while True: # Read until EOF or `limit`
            if limit is not None and len(output) >= limit: break
            line = f.readline()
            if not line: break
            record = json.loads(line)
            output.append(record)
    return output

# =============== CONSUMER GROUP HELPERS  ===============
def assign_partitions(topic_registry_cache, clients, topic_subscriptions):
    assignment = {t: {} for t in topic_subscriptions}
    num_clients = len(clients)
    for topic in topic_subscriptions:
        num_partitions = topic_registry_cache.get(topic).get("partitions")
        for partition in range(num_partitions):
            owner = clients[partition % num_clients] # Round-robin by join order
            assignment[topic][partition] = owner
    return assignment

def finalize_group_assignment(group, topic_registry_cache):
    previous_assignment = group.get("assignment") or {}
    new_assignment = assign_partitions(topic_registry_cache,  sorted(list(group["clients"].keys())), group["subscriptions"])
    if new_assignment != previous_assignment:
        group["assignment"] = new_assignment
        group["generation"] += 1

def evict_stale_members_and_reassign(group, topic_registry_cache, current_time):
    clients = group.get("clients", {})
    stale_clients = []
    for client_id, last_seen in clients.items():
        if current_time - last_seen > SESSION_TIMEOUT_SECONDS:
            stale_clients.append(client_id)
    if stale_clients:
        for client_id in stale_clients:
            clients.pop(client_id, None)
        finalize_group_assignment(group, topic_registry_cache)

# =============== REPLICATION THREAD HELPER ===============
def start_replication(log_end_offsets):
    stop = threading.Event()
    def _loop():
        _cycle = 0
        while not stop.is_set():
            #################################################
            ### 🧪 TESTING ONLY — DELAYED REPLICATION (5 MIN)
            #################################################
            # skip_replication_delay = Path(".var/skip_replication_delay")
            # if _cycle % 10 == 0: # Delay every 10th cycle
            #     for t in range(300, 0, -15):
            #         print(f"[REPLICATION] ⏳ {t}s remaining")
            #         if stop.wait(15): return
            #         if skip_replication_delay.exists():
            #             skip_replication_delay.unlink()
            #             break
            # _cycle += 1
            #################################################
            stop.wait(5) # Avoid busy loop
            print(f"{Color.BLUE}[⟳ REPLICATION] {BROKER_NAME}{Color.WHITE}")
            payload = {"follower_offsets": {f"{topic}:{pid}": leo for (topic, pid), leo in log_end_offsets.items()}}
            try:
                response = requests.post(f"http://{LEADER_ADDR}/replica_fetch", json=payload, timeout=5)
                records_output = response.json().get("records", {})
                for partition_key, records in records_output.items():
                    topic, pid_str = partition_key.split(":")
                    partition_tuple = (topic, int(pid_str))
                    pf = partition_file(*partition_tuple)
                    pf.parent.mkdir(parents=True, exist_ok=True)
                    with pf.open("a", encoding="utf-8") as f:
                        for rec in records:
                            f.write(json.dumps(rec) + "\n")
                    log_end_offsets[partition_tuple] = log_end_offsets.setdefault(partition_tuple, 0) + len(records)
            except Exception as e:
                print("Replica fetch failed:", e)

    replication_thread = threading.Thread(target=_loop, daemon=True, name="replication-loop")
    replication_thread.start()
    return stop
