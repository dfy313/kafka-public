import os, hashlib, json
from pathlib import Path

# =============== ENV CONFIG ===============
BROKER_NAME                 = os.getenv("BROKER_NAME", "default_broker") 
BASE_DIR                    = Path(f".var/kafkaesque/{BROKER_NAME}").resolve()

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
