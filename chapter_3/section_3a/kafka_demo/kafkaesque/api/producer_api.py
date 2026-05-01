import requests
from kafkaesque.api._util import http_get_json, http_post_json, pick_live_broker
from kafkaesque.structs import RecordMetadata, SendFuture

class KafkaesqueProducer:
    def __init__(self, bootstrap_servers, **_kwargs):
        self.bootstrap_servers = bootstrap_servers.split(',')
        # Persistent HTTP session (connection pooling + keep-alive)
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        # Runtime state
        self.base_url = pick_live_broker(self._session, self.bootstrap_servers)
        self._closed = False
        self._topic_cache = set()

    # =============== PUBLIC API ===============
    def send(self, topic: str, key=None, value=None):
        """
        Returns:
            SendFuture

        This mirrors the return contract of the real Kafka Python client's
        `Producer.send()` method, allowing existing producer code to work
        unchanged after migrating to Kafkaesque.
        """
        if self._closed:
            raise RuntimeError("KafkaesqueProducer is closed")
        self._ensure_topic_exists(topic)
        payload = {"topic": topic, "key": key, "value": value}
        response_body = http_post_json(self._session, self.base_url, "/produce", payload)
        md = RecordMetadata(
            topic=response_body["topic"],
            partition=int(response_body["partition"]),
            offset=int(response_body["offset"]),
            key=key,
            value=value,
        )
        return SendFuture(md)
    
    def close(self, timeout=None):
        self._closed = True
        self._topic_cache.clear()
        self._session.close()  # Release pooled connections

    # =============== INTERNAL HELPERS ===============
    def _ensure_topic_exists(self, topic: str):
        if topic in self._topic_cache: return
        response = http_get_json(self._session, self.base_url, f"/topics/{topic}")
        if response.get("name") == topic:
            self._topic_cache.add(topic)
