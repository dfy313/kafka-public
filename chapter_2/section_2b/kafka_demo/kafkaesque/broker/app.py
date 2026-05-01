import threading, json
from pprint import pprint
from flask import Flask, request, jsonify
from kafkaesque.broker import _util as util
from kafkaesque.structs import Color

def create_app() -> Flask:
    util.ensure_data_dirs()
    app = Flask(__name__)
    topic_registry_cache: dict[str, dict] = {}
    log_end_offsets: dict[tuple[str, int], int] = {}
    partition_locks:   dict[tuple[str, int], threading.Lock] = {}

    @app.get("/debug")
    def debug_info():
        print(Color.CYAN + "##################################################")
        print("#              KAFKAESQUE BROKER DEBUG           #")
        print("##################################################")
        print(Color.YELLOW + "=== TOPIC_REGISTRY_CACHE ===" + Color.MAGENTA); pprint(topic_registry_cache)
        print(Color.YELLOW + "\n=== LOG_END_OFFSETS ===" + Color.MAGENTA); pprint(log_end_offsets, width=1)
        print(Color.CYAN + "##################################################")
        print("#              END DEBUG OUTPUT                  #")
        print("##################################################\n" + Color.WHITE)
        return jsonify({"status": "ok"}), 200

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok"}), 200

    @app.post("/topics")
    def create_topic():
        body = request.get_json(force=True) or {}
        name = body.get("name")
        partitions = int(body.get("partitions", 1))
        replication_factor = int(body.get("replication_factor", 1))

        topic_registry_cache[name] = {
            "partitions": partitions,
            "replication_factor": replication_factor,
        }
        util.ensure_partition_files(name, partitions)

        return jsonify({"created": name, "partitions": partitions, "replication_factor": replication_factor}), 201

    @app.get("/topics/<name>")
    def describe_topic(name: str):
        topic_cfg = topic_registry_cache.get(name)
        return jsonify({"name": name, **topic_cfg})

    @app.post("/produce")
    def produce():
        body = request.get_json(force=True) or {}
        topic, key, value = body.get("topic"), body.get("key"), body.get("value")
        topic_cfg = topic_registry_cache.get(topic)
        pid = util.choose_partition(topic_cfg, key)
        partition_tuple = (topic, pid)
        pf = util.partition_file(*partition_tuple)

        partition_lock = partition_locks.setdefault(partition_tuple, threading.Lock())
        with partition_lock:
            current_offset = log_end_offsets.get(partition_tuple, 0)
            with pf.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"offset": current_offset, "key": key, "value": value}) + "\n")
            log_end_offsets[partition_tuple] = current_offset + 1

        return jsonify({"topic": topic, "partition": pid, "offset": current_offset}), 200

    return app
