import json
from kafkaesque.zookeeper.znodes import CONTROLLER_ZNODE, TOPIC_REGISTRY_ZNODE
from kafkaesque.broker._util import ensure_partition_files, partition_file

def install_controller_watch(zk, controller_view):
    @zk.DataWatch(CONTROLLER_ZNODE)
    def _controller_watch(data, _stat, _event=None):
        if not data:
            controller_view["broker_id"] = None
            controller_view["address"] = None
            return
        current_controller = json.loads(data.decode("utf-8"))
        controller_view["broker_id"] = current_controller.get("broker_id")
        controller_view["address"]   = current_controller.get("address")

def install_topics_watch(zk, topic_registry_cache, log_end_offsets):
    @zk.DataWatch(TOPIC_REGISTRY_ZNODE)
    def _topics_watch(data, _stat, _event=None):
        if not data: return
        topic_cfg = json.loads(data.decode("utf-8"))
        topic_registry_cache.clear()
        for topic_name, cfg in topic_cfg.items():
            partitions, replication_factor, minISR = cfg["partitions"], cfg["replication_factor"], cfg.get("minISR")
            topic_registry_cache[topic_name] = {
                "partitions": partitions,
                "replication_factor": replication_factor,
                **({"minISR": int(minISR)} if minISR is not None else {}),
            }
            ensure_partition_files(topic_name, partitions)
            for pid in range(partitions):
                if (topic_name, pid) not in log_end_offsets:
                    pf = partition_file(topic_name, pid)
                    with pf.open("r", encoding="utf-8") as f:
                        line_count = sum(1 for _ in f)
                    log_end_offsets[(topic_name, pid)] = line_count
