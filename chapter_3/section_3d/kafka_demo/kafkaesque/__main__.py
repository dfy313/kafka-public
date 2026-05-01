import os, logging
from .broker.app import create_app
from kafkaesque.broker._util import start_replication

BROKER_HOST = os.getenv("BROKER_HOST", "0.0.0.0")
BROKER_PORT = os.getenv("BROKER_PORT", "19092")
BROKER_NAME = os.getenv("BROKER_NAME", "default_broker") 

logging.getLogger("werkzeug").setLevel(logging.WARNING)

def main():
    app = create_app()

    replica_stop = None
    if BROKER_NAME != "broker_a" and BROKER_NAME != "default_broker":
        replica_stop = start_replication(app.log_end_offsets)

    try:
        app.run(host=BROKER_HOST, port=BROKER_PORT, debug=False, use_reloader=False, threaded=True)
    finally:
        if replica_stop: replica_stop.set()

if __name__ == "__main__":
    main()
