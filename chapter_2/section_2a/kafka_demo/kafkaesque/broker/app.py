from pprint import pprint
from flask import Flask, request, jsonify
from kafkaesque.broker import _util as util

def create_app() -> Flask:
    util.ensure_data_dirs()
    app = Flask(__name__)
    topic_registry_cache: dict[str, dict] = {}

    @app.get("/debug")
    def debug_info():
        CYAN, YELLOW, MAGENTA, WHITE = "\033[96m", "\033[93m", "\033[95m", "\033[0m"
        print(CYAN + "##################################################")
        print("#              KAFKAESQUE BROKER DEBUG           #")
        print("##################################################")
        print(YELLOW + "=== TOPIC_REGISTRY_CACHE ===" + MAGENTA); pprint(topic_registry_cache)
        print(CYAN + "##################################################")
        print("#              END DEBUG OUTPUT                  #")
        print("##################################################\n" + WHITE)
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

    return app
