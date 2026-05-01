import threading, json, time
from pprint import pprint
from flask import Flask, request, jsonify
from kafkaesque.broker import _util as util
from kafkaesque.structs import Color
from kafkaesque.zookeeper import watchers as watchers
from kafkaesque.zookeeper.znodes import TOPIC_REGISTRY_ZNODE
from kafkaesque.broker import router as router

def create_app(zk) -> Flask:
    util.ensure_data_dirs()
    topic_registry_cache: dict[str, dict] = {}
    log_end_offsets: dict[tuple[str, int], int] = {}
    consumer_groups_cache: dict[str, dict] = {}
    partition_locks: dict[tuple[str, int], threading.Lock] = {}
    consumer_groups_lock = threading.Lock()
    isr_cache: dict[tuple[str, int], list[str]] = {}
    high_watermarks: dict[tuple[str, int], int] = {}
    controller_view = {"broker_id": None, "address": None}
    peer_broker_view = {"broker_id": None, "address": None}
    partition_assignments_cache: dict[tuple[str, int], dict] = {}

    watchers.install_controller_watch(zk, controller_view)
    watchers.install_topics_watch(zk, topic_registry_cache, log_end_offsets)
    watchers.install_partition_assignments_watch(zk, partition_assignments_cache)
    watchers.install_election_children_watch(zk, peer_broker_view)

    app = Flask(__name__)
    app.log_end_offsets = log_end_offsets
    app.peer_broker_view = peer_broker_view

    @app.get("/debug")
    def debug_info():
        print(Color.CYAN + "##################################################")
        print("#              KAFKAESQUE BROKER DEBUG           #")
        print("##################################################")
        print(Color.YELLOW + "=== TOPIC_REGISTRY_CACHE ===" + Color.MAGENTA); pprint(topic_registry_cache)
        print(Color.YELLOW + "\n=== CONSUMER_GROUPS_CACHE ===" + Color.MAGENTA); pprint(consumer_groups_cache)
        print(Color.YELLOW + "\n=== LOG_END_OFFSETS ===" + Color.MAGENTA); pprint(log_end_offsets, width=1)
        print(Color.YELLOW + "\n=== ISR_CACHE ===" + Color.MAGENTA); pprint(isr_cache, width=1)
        print(Color.YELLOW + "\n=== HIGH_WATERMARKS ===" + Color.MAGENTA); pprint(high_watermarks, width=1)
        print(Color.YELLOW + "\n=== CONTROLLER_VIEW ===" + Color.MAGENTA); pprint(controller_view)
        print(Color.YELLOW + "\n=== PEER_BROKER_VIEW ===" + Color.MAGENTA); pprint(peer_broker_view)
        print(Color.YELLOW + "\n=== PARTITION_ASSIGNMENTS_CACHE ===" + Color.MAGENTA); pprint(partition_assignments_cache)
        print(Color.CYAN + "##################################################")
        print("#              END DEBUG OUTPUT                  #")
        print("##################################################" + Color.WHITE)
        return jsonify({"status": "ok"}), 200

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok"}), 200

    @app.post("/topics")
    @router.controller(controller_view)
    def create_topic():
        body = request.get_json(force=True) or {}
        name = body.get("name")
        partitions = int(body.get("partitions", 1))
        replication_factor = int(body.get("replication_factor", 1))
        minISR = body.get("minISR")

        data, _stat = zk.get(TOPIC_REGISTRY_ZNODE)
        topic_cfg = json.loads(data.decode("utf-8")) if data else {}
        topic_cfg[name] = {
            "partitions": partitions,
            "replication_factor": replication_factor,
            **({"minISR": int(minISR)} if minISR is not None else {}),
        }
        zk.set(TOPIC_REGISTRY_ZNODE, json.dumps(topic_cfg).encode("utf-8"))

        return jsonify({"created": name, "partitions": partitions, "replication_factor": replication_factor}), 201

    @app.get("/topics/<name>")
    def describe_topic(name: str):
        topic_cfg = topic_registry_cache.get(name)
        return jsonify({"name": name, **topic_cfg})

    @app.post("/produce")
    @router.partition_leader(topic_registry_cache, partition_assignments_cache, body_topic_key=True)
    def produce():
        body = request.get_json(force=True) or {}
        topic, key, value = body.get("topic"), body.get("key"), body.get("value")
        topic_cfg = topic_registry_cache.get(topic)
        pid = util.choose_partition(topic_cfg, key)
        partition_tuple = (topic, pid)
        pf = util.partition_file(*partition_tuple)

        if len(partition_assignments_cache[partition_tuple]["replicas"]) == 1:
            isr_cache[partition_tuple] = [util.BROKER_NAME]
        current_isr = isr_cache.get(partition_tuple, [util.BROKER_NAME])
        if len(current_isr) < topic_cfg.get("minISR"):
            raise RuntimeError("NotEnoughInSyncReplicas")

        partition_lock = partition_locks.setdefault(partition_tuple, threading.Lock())
        with partition_lock:
            current_offset = log_end_offsets.get(partition_tuple, 0)
            with pf.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"offset": current_offset, "key": key, "value": value}) + "\n")
            log_end_offsets[partition_tuple] = current_offset + 1

        return jsonify({"topic": topic, "partition": pid, "offset": current_offset}), 200

    @app.post("/groups/<group_id>/join")
    @router.consumer_group_coordinator(topic_registry_cache, partition_assignments_cache)
    def join_consumer_group(group_id):
        body = request.get_json(force=True) or {}
        client_id = body.get("client_id", "anon")
        subscriptions = set(body.get("subscriptions", []))

        with consumer_groups_lock:
            group = consumer_groups_cache.setdefault(
                group_id,
                {
                    "clients": {},
                    "subscriptions": subscriptions,
                    "assignment": {},
                    "generation": 0,
                },
            )
            group["clients"][client_id] = time.time()
            util.finalize_group_assignment(group, topic_registry_cache)

        return jsonify({
            "group_id": group_id,
            "client_id": client_id,
            "assignment": group["assignment"],
            "generation": group["generation"]
        })

    @app.post("/groups/<group_id>/heartbeat")
    @router.consumer_group_coordinator(topic_registry_cache, partition_assignments_cache, on_proxy=router.prune_local_state(consumer_groups_cache, consumer_groups_lock))
    def heartbeat(group_id):
        body = request.get_json(force=True) or {}
        client_id = body.get("client_id", "anon")
        generation = body.get("generation")

        with consumer_groups_lock:
            consumer_group = consumer_groups_cache.get(group_id)
            if consumer_group is None: return jsonify({"action": "rejoin", "reason": "group-unknown"}), 200
            current_time = time.time()
            consumer_group["clients"][client_id] = current_time
            util.evict_stale_members_and_reassign(consumer_group, topic_registry_cache, current_time)
            if generation != consumer_group["generation"]: return jsonify({"action": "rejoin", "reason": "stale-generation"}), 200

        return jsonify({"status": "ok"}), 200

    @app.get("/groups/<group_id>/offsets/<topic>/<int:partition_id>")
    @router.consumer_group_coordinator(topic_registry_cache, partition_assignments_cache)
    def get_committed_offset(group_id: str, topic: str, partition_id: int):
        coordinator_pid = util.choose_coordinator_partition(group_id, topic_registry_cache["__consumer_offsets"]["partitions"])
        offsets_tuple = ("__consumer_offsets", coordinator_pid)
        offset_file = util.partition_file(*offsets_tuple)

        partition_lock = partition_locks.setdefault(offsets_tuple, threading.Lock())
        with partition_lock:
            committed_offset = util.get_committed_offset_from_log(group_id, topic, partition_id, offset_file)

        return jsonify({"group_id": group_id, "topic": topic, "partition": partition_id, "offset": committed_offset})

    @app.post("/groups/<group_id>/offsets/<topic>/<int:pid>")
    @router.consumer_group_coordinator(topic_registry_cache, partition_assignments_cache)
    def commit_offset(group_id: str, topic: str, pid: int):
        body = request.get_json(force=True) or {}
        new_offset = body.get("offset")
        coordinator_pid = util.choose_coordinator_partition(group_id, topic_registry_cache["__consumer_offsets"]["partitions"])
        offsets_tuple = ("__consumer_offsets", coordinator_pid)
        offset_file = util.partition_file(*offsets_tuple)

        partition_lock = partition_locks.setdefault(offsets_tuple, threading.Lock())
        with partition_lock:
            with offset_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"key": f"{group_id}|{topic}|{pid}","offset": new_offset}) + "\n")
            log_end_offsets[offsets_tuple] = log_end_offsets.setdefault(offsets_tuple, 0) + 1

        return jsonify({"group_id": group_id, "topic": topic, "partition": pid, "offset": new_offset}), 200

    @app.get("/fetch/<topic>/<int:partition_id>")
    @router.partition_leader(topic_registry_cache, partition_assignments_cache, topic_arg="topic", pid_arg="partition_id")
    def fetch_record(topic: str, partition_id: int):
        offset = int(request.args.get("offset", 0))
        partition_tuple = (topic, partition_id)
        pf = util.partition_file(*partition_tuple)

        if len(partition_assignments_cache[partition_tuple]["replicas"]) == 1:
            high_watermarks[partition_tuple] = log_end_offsets.get(partition_tuple, 0)
        high_watermark = high_watermarks.get(partition_tuple, 0)
        if offset >= high_watermark: # There's nothing *committed* to read
            return jsonify({"record": None, "next_offset": offset}), 200

        partition_lock = partition_locks.setdefault(partition_tuple, threading.Lock())
        with partition_lock:
            records = util.read_records_from(pf, offset, limit=1)

        return jsonify({"record": records[0] if records else None, "next_offset": offset + len(records)}), 200

    @app.post("/replica_fetch")
    def replica_fetch():
        body = request.get_json(force=True) or {}
        bootstrap = body.get("bootstrap", False)
        replica_id = body.get("replica_id", "unknown")
        follower_offsets = body.get("follower_offsets", {})

        records_output = {}
        for partition_tuple, leader_leo in list(log_end_offsets.items()):
            if topic_registry_cache[partition_tuple[0]]["replication_factor"] < 2: continue
            if not bootstrap and partition_assignments_cache[partition_tuple]["leader_alias"] != util.BROKER_NAME:
                isr_cache.pop(partition_tuple, None)
                high_watermarks.pop(partition_tuple, None)
                continue

            partition_key = f"{partition_tuple[0]}:{partition_tuple[1]}"
            follower_leo = int(follower_offsets.get(partition_key, 0))
            partition_records = []

            if follower_leo < leader_leo:
                pf = util.partition_file(*partition_tuple)
                partition_lock = partition_locks.setdefault(partition_tuple, threading.Lock())
                with partition_lock:
                    partition_records = util.read_records_from(pf, follower_leo)
                records_output[partition_key] = partition_records

            if bootstrap or partition_tuple[0] == "__consumer_offsets": continue # Only handle ISR / HW for data topics
            if follower_leo + len(partition_records) >= leader_leo: # Follower fully caught up
                isr_cache[partition_tuple] = [util.BROKER_NAME, replica_id]
                high_watermarks[partition_tuple] = leader_leo
            else: # Follower lagging
                isr_cache[partition_tuple] = [util.BROKER_NAME]

        return jsonify({"records": records_output}), 200

    return app
