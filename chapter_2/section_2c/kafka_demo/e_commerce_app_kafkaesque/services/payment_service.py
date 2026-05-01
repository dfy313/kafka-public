import threading, time, datetime
import pymysql
from e_commerce_app_kafkaesque.service_base import (
    KAFKA_BOOTSTRAP, PORT, GROUP_ID, SUBSCRIPTIONS,                           # env/config
    order_exists,                                                             # db helpers
    create_base_app, create_kafkaesque_producer, create_kafkaesque_consumer,  # factories
    run_consumer_loop, install_graceful_shutdown,                             # consumer runtime
)

app = create_base_app(GROUP_ID, [SUBSCRIPTIONS], KAFKA_BOOTSTRAP)
producer = create_kafkaesque_producer()
_stop = install_graceful_shutdown()

def process_payment(event: dict):
    oid = (event or {}).get("order_id")
    if not oid:
        print("PAYMENT: missing order_id in event")
        return False
    try:
        if not order_exists(oid):
            print(f"PAYMENT: order_id {oid} NOT found in DB — will retry")
            return False
    except pymysql.MySQLError as e:
        print(f"PAYMENT: DB check failed for {oid}: {e}")
        return False
    
    time.sleep(1) # Simulate payment processing 

    if not emit_payment_success(event):
        return False
    
    print(f"PAYMENT PROCESSED SUCCESSFULLY {event}")
    return True

def emit_payment_success(order_event: dict):
    try:
        payload = {
            "topic": "payment",
            "event_type": "PaymentSuccess",
            "order_id": order_event.get("order_id"),
            "user_id": order_event.get("user_id"),
            "total_amount": order_event.get("total_amount"),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        key = order_event.get("order_id")
        md = producer.send("payment", key=key, value=payload).get(timeout=10)
        print(f"[PAYMENT] → emitted PaymentSuccess to {md.topic}-{md.partition}@{md.offset}")
        return True
    except Exception as e:
        print(f"[PAYMENT] emit failed: {e}")
        return False

def consume_loop():
    consumer = create_kafkaesque_consumer(GROUP_ID, [SUBSCRIPTIONS], "PAYMENT")
    run_consumer_loop(
        consumer=consumer,
        stop_event=_stop,
        process_event=process_payment,
        producer=producer
    )


if __name__ == "__main__":
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    print(f"Starting payment_service: GROUP_ID={GROUP_ID} SUBS={SUBSCRIPTIONS} PORT={PORT}")
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)
