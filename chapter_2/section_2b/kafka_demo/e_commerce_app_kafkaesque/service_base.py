# ── Standard library ─────────────────────────────────────────────────────────
import os, json, signal, threading
# ── Third-party ──────────────────────────────────────────────────────────────
from flask import Flask, jsonify
from kafkaesque import KafkaesqueProducer
from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata
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
def create_kafkaesque_producer():
    return KafkaesqueProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
        linger_ms=5,
        retries=5,
    )

def create_kafka_consumer(group_id, subscriptions, service_name):
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        client_id=CLIENT_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        max_poll_records=200,
        session_timeout_ms=45000,
        heartbeat_interval_ms=5000,
    )
    consumer.subscribe(subscriptions)
    print(f"[{service_name}] subscribed to {subscriptions} as group '{group_id}'")
    return consumer

# =============== DB HELPERS (MySQL) ===============
def get_db_connection_cursor():
    conn = pymysql.connect(
        host=DB_HOST,
        user="admin",
        password="Password100!",
        database="services_db",
    )
    return conn, conn.cursor()

def order_exists(order_id: str) -> bool:
    sql = "SELECT 1 FROM Orders WHERE order_id = %s LIMIT 1"
    conn, cursor = get_db_connection_cursor()
    try:
        cursor.execute(sql, (order_id,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        conn.close()

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

# =============== CONSUMER RUNTIME ===============
def install_graceful_shutdown():
    """
    Use Ctrl+C (SIGINT) to stop
    Do not use Ctrl+Z (SIGTSTP) as this suspends the process without 
      running any cleanup, so flask servers keep their sockets open
    """
    stop = threading.Event()
    def _graceful_exit(*_):
        stop.set()
        try: os.kill(os.getpid(), signal.SIGINT)
        except Exception: pass
    signal.signal(signal.SIGTERM, _graceful_exit)
    return stop

def run_consumer_loop(
    consumer,
    stop_event,
    process_event,
    producer = None
):
    try:
        while not stop_event.is_set(): # Loop until we're asked to stop (e.g., on shutdown)

            # Step (1): poll kafka topic(s) for new messages
            batches = consumer.poll(timeout_ms=1000, max_records=500)
            if not batches: continue
            
            to_commit: dict[TopicPartition, OffsetAndMetadata] = {}

            # Step (2): iterate topic-partitions and their fetched messages
            for tp, messages in batches.items():
                # Per-partition AT-LEAST-ONCE semantics
                # - Messages are processed in order within each TopicPartition.
                # - If process_event(evt) returns False OR raises, we break out of THIS PARTITION's batch,
                #   do not advance last_ok_offset, and do not add a commit for this tp.
                #   The failed record will be delivered again on the next poll (retry), preserving order.
                #
                # Implications:
                # - Handlers must be IDEMPOTENT (DB writes, emits) because a record may be reprocessed.
                # - A permanent "poison pill" will stall this partition; consider retry counters + backoff,
                #   DLQ/parking, or pause/resume to avoid starvation.

                if not messages: continue

                last_ok_offset = None

                for m in messages:
                    evt = m.value

                    # Step (3): process event (your business logic)
                    try:
                        if not process_event(evt):
                            print(f"[{CLIENT_ID}] ❌ {tp.topic}-{tp.partition}@{m.offset}")
                            break  # stop processing further messages in this partition's batch
                    except Exception as e:
                        print(f"[{CLIENT_ID}] error at {tp.topic}-{tp.partition}@{m.offset}: {e}")
                        break

                    print(f"[{CLIENT_ID}] ✅ {tp.topic}-{tp.partition}@{m.offset}")
                    last_ok_offset = m.offset

                # Step (4): commit offset to Kafka
                if last_ok_offset is not None:
                    to_commit[tp] = OffsetAndMetadata(last_ok_offset + 1, None, -1)

            if to_commit:
                consumer.commit(offsets=to_commit)
                summary = ", ".join(f"{tp.topic}-{tp.partition}@{om.offset}" for tp, om in to_commit.items())
                print(f"[{CLIENT_ID}] committed: {summary}")

    except Exception as e:
        print(f"[{CLIENT_ID}] consumer error: {e}")

    finally:
        try: # Always close the consumer
            consumer.close()
        except Exception:
            pass
        if producer:
            try: # close the producer if we used one for follow-up events
                producer.close()
            except Exception:
                pass
        print(f"[{CLIENT_ID}] consumer closed")

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
