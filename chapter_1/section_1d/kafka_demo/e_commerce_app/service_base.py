# ── Standard library ─────────────────────────────────────────────────────────
import os, json
# ── Third-party ──────────────────────────────────────────────────────────────
from flask import Flask, jsonify
from kafka import KafkaProducer
import pymysql

# =============== ENV CONFIG & UTILITIES ===============
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
if not KAFKA_BOOTSTRAP:
    raise SystemExit(
        "KAFKA_BOOTSTRAP is not set. Set it to <broker-host>:9092 "
        "(e.g., host.docker.internal:9092, kafka:9092, or your EC2 private DNS)."
    )
DB_HOST = os.getenv("DB_HOST")
if not DB_HOST:
    raise SystemExit("DB_HOST is not set.")

CLIENT_ID = os.getenv("CLIENT_ID")
PORT = os.getenv("PORT")
GROUP_ID = os.getenv("GROUP_ID")
SUBSCRIPTIONS = os.getenv("SUBSCRIPTIONS")

# =============== KAFKA CLIENT FACTORIES ===============
def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
        linger_ms=5,
        retries=5,
    )

# =============== DB HELPERS (MySQL) ===============
def get_db_connection_cursor():
    conn = pymysql.connect(
        host=DB_HOST,
        user="admin",
        password="Password100!",
        database="services_db",
    )
    return conn, conn.cursor()

def insert_order(order_event: dict):
    """
    Plain INSERT into Orders.
    Fails with IntegrityError if order_id already exists.
    """
    order_id = order_event.get("order_id")
    user_id = order_event.get("user_id")
    total_amount = order_event.get("total_amount")
    iso_ts = order_event.get("timestamp")
    items_json = json.dumps(order_event.get("items", []))
    sql = """
        INSERT INTO Orders (order_id, user_id, total_amount, created_timestamp, items)
        VALUES (%s, %s, %s,
                STR_TO_DATE(%s, '%%Y-%%m-%%dT%%H:%%i:%%sZ'),
                CAST(%s AS JSON))
    """
    conn, cur = get_db_connection_cursor()
    try:
        cur.execute(sql, (order_id, user_id, total_amount, iso_ts, items_json))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def delete_order(order_id):
    conn, cur = get_db_connection_cursor()
    sql = "DELETE FROM Orders WHERE order_id = %s"
    try:
        cur.execute(sql, (order_id,))
        conn.commit()
    finally:
        cur.close()
        conn.close()

# =============== FLASK APP FACTORY ===============
def create_base_app(group_id: str, subscriptions: list, kafka_bootstrap: str | None = None):
    app = Flask(__name__)

    @app.route("/")
    def root():
        payload = {"group_id": group_id, "subscriptions": subscriptions}
        if kafka_bootstrap:
            payload["kafka_bootstrap"] = kafka_bootstrap
        return jsonify(payload)
    
    @app.route("/healthz")
    def healthz():
        return "ok", 200

    return app
