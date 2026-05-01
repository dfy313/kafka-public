import os, logging
from .broker.app import create_app

BROKER_HOST = os.getenv("BROKER_HOST", "0.0.0.0")
BROKER_PORT = os.getenv("BROKER_PORT", "19092")

logging.getLogger("werkzeug").setLevel(logging.WARNING)

def main():
    app = create_app()
    app.run(host=BROKER_HOST, port=BROKER_PORT, debug=False, use_reloader=False, threaded=True)

if __name__ == "__main__":
    main()
