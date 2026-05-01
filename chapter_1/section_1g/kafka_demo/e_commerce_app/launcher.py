import subprocess, sys, os, time, signal
from urllib.request import urlopen
from pathlib import Path
from e_commerce_app.service_base import KAFKA_BOOTSTRAP

PYTHON = sys.executable
HERE = Path(__file__).resolve().parent          # .../e_commerce_app
REPO_ROOT = HERE.parent                         # Parent dir that contains e_commerce_app package

services = [
    {"name": "order_service",           "module": "e_commerce_app.services.order_service",        "PORT": "5001", "SUBSCRIPTIONS": "",       "GROUP_ID": "order_service"},
    {"name": "payment_service_1",       "module": "e_commerce_app.services.payment_service",      "PORT": "5002", "SUBSCRIPTIONS": "order",  "GROUP_ID": "payment_service",      "CLIENT_ID": "payment-A"},
    # {"name": "payment_service_2",       "module": "e_commerce_app.services.payment_service",      "PORT": "5102", "SUBSCRIPTIONS": "order",  "GROUP_ID": "payment_service",      "CLIENT_ID": "payment-B"},
    {"name": "notification_service_1",  "module": "e_commerce_app.services.notification_service", "PORT": "5003", "SUBSCRIPTIONS": "payment", "GROUP_ID": "notification_service", "CLIENT_ID": "notification-A"},
    # {"name": "notification_service_2",  "module": "e_commerce_app.services.notification_service", "PORT": "5103", "SUBSCRIPTIONS": "payment", "GROUP_ID": "notification_service", "CLIENT_ID": "notification-B"}
]

def wait_until_up(port: str, retries: int = 40, delay: float = 0.25) -> bool:
    url = f"http://localhost:{port}/healthz"
    for _ in range(retries):
        try:
            with urlopen(url, timeout=0.5):
                return True
        except Exception:
            time.sleep(delay)
    return False

def print_service_map(rows):
    print("\nService map:")
    print("--------------------------------------------------------------------------")
    print(f"{'SERVICE':24} {'GROUP_ID':24} {'SUBSCRIPTIONS':24} {'URL'}")
    print("--------------------------------------------------------------------------")
    for r in rows:
        url = f"http://localhost:{r['PORT']}/"
        subs = r["SUBSCRIPTIONS"] if r["SUBSCRIPTIONS"] else "—"
        print(f"{r['name']:24} {r['GROUP_ID']:24} {subs:24} {url}")
    print("--------------------------------------------------------------------------\n")

def killpg(pid: int, sig: int):
    try: os.killpg(pid, sig)
    except ProcessLookupError: pass
    except Exception: pass

procs = []

try:
    for row in services:
        env = os.environ.copy()
        env.update({
            "GROUP_ID": row["GROUP_ID"],
            "SUBSCRIPTIONS": row["SUBSCRIPTIONS"],
            "PORT": row["PORT"],
            "KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
        })

        if "CLIENT_ID" in row: # Optional per-process client id (only set if present in the row)
            env["CLIENT_ID"] = row["CLIENT_ID"]

        p = subprocess.Popen(
            [PYTHON, "-u", "-m", row["module"]],
            env=env,
            cwd=str(REPO_ROOT),
            start_new_session=True,
        )
        procs.append(p)
        print(f"Started {row['name']} on port {row['PORT']} (pid={p.pid})")
    # Health checks
    for row in services:
        status = "UP" if wait_until_up(row["PORT"]) else "timed out"
        print(f"  -> {row['name']} @ {row['PORT']}: {status}")
    print_service_map(services)

    # Wait until any child exits or until Ctrl-C
    while any(p.poll() is None for p in procs):
        time.sleep(0.3)

except KeyboardInterrupt:
    print("\nStopping all services...")

finally:
    # Ignore extra Ctrl-C during cleanup so we finish shutting down cleanly
    try: signal.signal(signal.SIGINT, signal.SIG_IGN)
    except Exception: pass

    for p in procs:
        if p.poll() is None:
            killpg(p.pid, signal.SIGTERM)
