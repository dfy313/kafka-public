import os
from pathlib import Path

# =============== ENV CONFIG ===============
BROKER_NAME  = os.getenv("BROKER_NAME", "default_broker") 
BASE_DIR     = Path(f".var/kafkaesque/{BROKER_NAME}").resolve()

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
