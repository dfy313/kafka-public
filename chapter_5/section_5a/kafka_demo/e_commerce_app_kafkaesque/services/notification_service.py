import threading, time
import pymysql
from e_commerce_app_kafkaesque.service_base import (
    PORT, KAFKA_BOOTSTRAP, GROUP_ID, SUBSCRIPTIONS,  # env/config
    order_exists,                                    # db helpers
    create_base_app, create_kafkaesque_consumer,     # factories
    run_consumer_loop, install_graceful_shutdown     # consumer runtime
)

app = create_base_app(GROUP_ID, [SUBSCRIPTIONS], KAFKA_BOOTSTRAP)
_stop = install_graceful_shutdown()

def process_notification(event: dict):
    oid = (event or {}).get("order_id")
    if not oid:
        print("NOTIFICATION: missing order_id in event")
        return False
    try:
        if not order_exists(oid):
            print(f"NOTIFICATION: order_id {oid} NOT found in DB — will retry")
            return False
    except pymysql.MySQLError as e:
        print(f"NOTIFICATION: DB check failed for {oid}: {e}")
        return False

    time.sleep(1) # Simulate notification processing 

    print(f"NOTIFICATION PROCESSED SUCCESSFULLY {event}")
    return True

def consume_loop():
    consumer = create_kafkaesque_consumer(GROUP_ID, [SUBSCRIPTIONS], "NOTIFICATION")
    run_consumer_loop(
        consumer=consumer,
        stop_event=_stop,
        process_event=process_notification,
        producer = None
    )


if __name__ == "__main__":
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    print(f"Starting notification_service: GROUP_ID={GROUP_ID} SUBS={SUBSCRIPTIONS} PORT={PORT}")
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)
