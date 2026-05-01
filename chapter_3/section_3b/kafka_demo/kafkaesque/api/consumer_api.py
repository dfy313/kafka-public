import time, requests, threading
from kafkaesque.structs import TopicPartition, OffsetAndMetadata, RecordMetadata, Color
from kafkaesque.api._util import http_get_json, http_post_json, pick_live_broker

HEARTBEAT_INTERVAL = 10

class KafkaesqueConsumer:
    def __init__(self, bootstrap_servers, group_id, client_id, **_kwargs):
        self.bootstrap_servers = bootstrap_servers.split(',')
        self.group_id = group_id
        self.client_id = client_id
        # Persistent HTTP session (connection pooling + keep-alive)
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        # Runtime state
        self.base_url = pick_live_broker(self._session, self.bootstrap_servers)
        self._closed = False
        self._needs_join = False
        self._generation = -1
        self._subscription = set()
        self._assigned = []
        self._positions = {}
        self._heartbeat_thread = None
        # Idle backoff to handle EADDRNOTAVAIL
        self._idle_backoff_seconds = 0.1
        self._idle_backoff_cap = 10.0

    # =============== PUBLIC API ===============
    def subscribe(self, topics):
        """
        Record the desired topics and mark that we must (re)join on next poll().
        No network I/O here and no files are created.
        """
        self._subscription = set(topics)
        self._needs_join = True

    def poll(self, timeout_ms=1000, max_records=500):
        """
        Returns:
            dict[TopicPartition, list[RecordMetadata]]

        This mirrors the structure returned by the real Kafka Python client.
        We keep this contract so that existing client-side code continues to
        work unchanged after migrating to Kafkaesque.
        """
        if self._closed: return {}
        self._join_if_needed()
        if not self._assigned: return {}

        results = {}
        idle_backoff_needed = True
    
        for tp in self._assigned:
            topic, partition = tp.topic, tp.partition
            start = self._positions.get((topic, partition), 0)
            fetch_records_response = http_get_json(self._session, self.base_url, f"/fetch/{topic}/{partition}?offset={start}")
            record = fetch_records_response.get("record", None)
            if record:
                idle_backoff_needed = False
                key, value, offset = record.get("key"), record.get("value"), int(record.get("offset"))
                self._positions[(topic, partition)] = fetch_records_response.get("next_offset")
                results[tp] = [RecordMetadata(topic, partition, offset, key, value)]

        if idle_backoff_needed:
            time.sleep(self._idle_backoff_seconds)
            self._idle_backoff_seconds = min(self._idle_backoff_seconds * 2, self._idle_backoff_cap)
        else:
            self._idle_backoff_seconds = 0.1

        return results

    def commit(self, offsets: dict[TopicPartition, OffsetAndMetadata]):
        """Commit offsets to the broker. Offsets should be the 'next' offset to read."""
        if self._closed: return
        items = list(offsets.items())
        for tp, metadata in items:
            payload = {"offset": int(metadata.offset)}
            http_post_json(self._session, self.base_url, f"/groups/{self.group_id}/offsets/{tp.topic}/{tp.partition}", payload)

    def close(self):
        self._closed = True
        self._subscription.clear()
        self._assigned.clear()
        self._positions.clear()
        self._session.close() # Release pooled connections

    # =============== INTERNAL HELPERS ===============
    def _join_if_needed(self):
        if not self._needs_join: return
        payload = {"client_id": self.client_id, "subscriptions": sorted(self._subscription)}
        response = http_post_json(self._session, self.base_url, f"/groups/{self.group_id}/join", payload)
        full_assignment = response.get("assignment") or {}
        assigned, positions = [], {}

        for topic, partition_assignments in sorted(full_assignment.items()):
            for pid, owner in partition_assignments.items():
                if owner != self.client_id: continue
                assigned.append(TopicPartition(topic, pid))
                get_offset_response = http_get_json(self._session, self.base_url, f"/groups/{self.group_id}/offsets/{topic}/{pid}")
                committed_offset = get_offset_response["offset"]
                positions[(topic, pid)] = 0 if committed_offset < 0 else committed_offset

        self._generation = response.get("generation")
        self._assigned = assigned
        self._positions = positions
        self._needs_join = False

        if self._heartbeat_thread is None:
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._heartbeat_thread.start()

    def _heartbeat_loop(self):
        while not self._closed:
            if self._needs_join: continue
            payload = {"client_id": self.client_id, "generation": self._generation}
            response = http_post_json(self._session, self.base_url, f"/groups/{self.group_id}/heartbeat", payload)
            if response.get("action") == "rejoin":
                self._needs_join = True
                continue
            print(
                f"{Color.PINK}[♥︎ HEARTBEAT] {self.client_id} (gen {self._generation})\n"
                f"   positions={self._positions}\n{Color.WHITE}"
            )
            time.sleep(HEARTBEAT_INTERVAL)
