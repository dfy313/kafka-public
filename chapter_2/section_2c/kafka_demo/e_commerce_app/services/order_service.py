import pymysql
from flask import request, jsonify
from e_commerce_app.service_base import (
    KAFKA_BOOTSTRAP, PORT, GROUP_ID, SUBSCRIPTIONS,  # env/config
    insert_order, delete_order,                      # db helpers
    create_base_app, create_kafka_producer,          # factories
)

app = create_base_app(GROUP_ID, [SUBSCRIPTIONS], KAFKA_BOOTSTRAP)
producer = create_kafka_producer()

@app.route("/produce", methods=["POST"])
def produce():
    data = request.get_json(force=True)
    topic = data.get("topic")
    event = data.get("event")
    key = data.get("key") or (event.get("order_id") if isinstance(event, dict) else None)
    if not topic or event is None:
        return jsonify({"error": "topic and event are required"}), 400
    try:
        insert_order(event)
    except pymysql.MySQLError as e:
        return jsonify({"error": f"db insert failed: {str(e)}"}), 500

    try:
        md = producer.send(topic, key=key, value=event).get(timeout=10)
    except Exception as e:
        delete_order(key)
        return jsonify({"error": f"failed to publish event; order not created: {e}"}), 500

    return jsonify({"status": "produced", "topic": md.topic, "partition": md.partition, "offset": md.offset}), 200

if __name__ == "__main__":
    print(f"Starting order_service: GROUP_ID={GROUP_ID} PORT={PORT} BOOTSTRAP={KAFKA_BOOTSTRAP}")
    try:
        app.run(host="0.0.0.0", port=PORT, use_reloader=False)
    finally:
        producer.close()
        print(f"[{GROUP_ID}] producer closed")
