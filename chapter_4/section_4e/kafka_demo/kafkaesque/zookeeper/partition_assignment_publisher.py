import json, time
from kafkaesque.zookeeper.znodes import PARTITION_ASSIGNMENTS_ZNODE, ELECTION_ZNODE, TOPIC_REGISTRY_ZNODE

class PartitionAssignmentPublisher:
    def __init__(self, zk):
        self.zk = zk

    # =============== LIFECYCLE ===============
    def start(self):
        self.zk.ensure_path(PARTITION_ASSIGNMENTS_ZNODE)
        self.zk.ensure_path(TOPIC_REGISTRY_ZNODE)

        @self.zk.DataWatch(TOPIC_REGISTRY_ZNODE)
        def _topics_watch(_data, _stat, _event=None):
            if not _data: return
            self.compute_and_publish_round_robin()

        @self.zk.ChildrenWatch(ELECTION_ZNODE)
        def _election_children_watch(_children):
            self.compute_and_publish_round_robin()

    # =============== ROUND-ROBIN ASSIGNMENT ===============
    def compute_and_publish_round_robin(self):
        topic_cfg  = self._read_topics()
        live_brokers = self._live_brokers()
        num_brokers = len(live_brokers)
        for topic_name, cfg in topic_cfg.items():
            partitions = cfg["partitions"]
            replication_factor = min(cfg["replication_factor"], num_brokers)
            for pid in range(partitions):
                leader_idx = pid % num_brokers
                leader = live_brokers[leader_idx]
                replicas = [] # Build replicas ring-style starting from leader
                for i in range(replication_factor):
                    follower_broker = live_brokers[(leader_idx + i) % num_brokers]
                    replicas.append(follower_broker["broker_id"])
                payload = {"leader_alias": leader["broker_id"], "leader_address": leader["address"], "replicas": replicas}
                self.zk.ensure_path(f"{PARTITION_ASSIGNMENTS_ZNODE}/{topic_name}/{pid}")
                self.zk.set(f"{PARTITION_ASSIGNMENTS_ZNODE}/{topic_name}/{pid}", json.dumps(payload).encode("utf-8"))
        self.zk.set(PARTITION_ASSIGNMENTS_ZNODE, json.dumps({"epoch": time.time()}).encode("utf-8")) # Trigger broker watches

    # =============== INTERNAL HELPERS ===============
    def _read_topics(self):
        data, _stat = self.zk.get(TOPIC_REGISTRY_ZNODE)
        return json.loads(data.decode("utf-8")) if data else {}

    def _live_brokers(self):
        children = self.zk.get_children(ELECTION_ZNODE)
        return sorted(
            [
                json.loads(self.zk.get(f"{ELECTION_ZNODE}/{child}")[0].decode("utf-8"))
                for child in children
            ],
            key=lambda e: e["broker_id"]
        )
