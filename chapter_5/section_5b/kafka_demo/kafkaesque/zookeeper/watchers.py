import json
from kafkaesque.zookeeper.znodes import CONTROLLER_ZNODE, TOPIC_REGISTRY_ZNODE, PARTITION_ASSIGNMENTS_ZNODE, ELECTION_ZNODE
from kafkaesque.broker._util import ensure_partition_files, partition_file, BROKER_NAME

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

def install_election_children_watch(zk, peer_broker_view):
    @zk.ChildrenWatch(ELECTION_ZNODE)
    def _election_children_watch(children):
        peer_broker_view["broker_id"], peer_broker_view["address"] = None, None
        for child in children:
            data = json.loads(zk.get(f"{ELECTION_ZNODE}/{child}")[0].decode("utf-8"))
            if data["broker_id"] != BROKER_NAME:
                peer_broker_view["broker_id"] = data["broker_id"]
                peer_broker_view["address"] = data["address"]

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

def install_partition_assignments_watch(zk, partition_assignments_cache):
    @zk.DataWatch(PARTITION_ASSIGNMENTS_ZNODE)
    def _partition_assignments_watch(data, _stat, _event=None):
        if not data: return
        partition_assignments_cache.clear()
        topics = zk.get_children(PARTITION_ASSIGNMENTS_ZNODE)
        for topic in topics:
            topic_subpath_znode = f"{PARTITION_ASSIGNMENTS_ZNODE}/{topic}"
            partition_ids = zk.get_children(topic_subpath_znode)
            for pid_str in partition_ids:
                data, _stat = zk.get(f"{topic_subpath_znode}/{pid_str}")
                partition_assignments_cache[(topic, int(pid_str))] = json.loads(data.decode("utf-8"))
