import os, logging
from .broker.app import create_app
from kafkaesque.broker._util import start_replication
from .zookeeper.controller_election import ControllerElection
from kazoo.client import KazooClient

BROKER_HOST = os.getenv("BROKER_HOST", "0.0.0.0")
BROKER_PORT = os.getenv("BROKER_PORT", "19092")
BROKER_NAME = os.getenv("BROKER_NAME", "default_broker")
ZK_CONNECT  = os.getenv("ZK_CONNECT", "localhost:2181")

logging.getLogger("werkzeug").setLevel(logging.WARNING)

def main():
    zk = KazooClient(hosts=ZK_CONNECT)
    zk.start(timeout=5)
    controller_election = ControllerElection(zk=zk, broker_id=BROKER_NAME, address= f"{BROKER_HOST}:{BROKER_PORT}")
    controller_election.start()

    app = create_app(zk)
    replica_stop = None
    if BROKER_NAME != "broker_a" and BROKER_NAME != "default_broker":
        replica_stop = start_replication(app.log_end_offsets)

    try:
        app.run(host=BROKER_HOST, port=BROKER_PORT, debug=False, use_reloader=False, threaded=True)
    finally:
        if replica_stop: replica_stop.set()
        controller_election.stop()
        zk.stop(); zk.close()

if __name__ == "__main__":
    main()
