import functools, requests
from flask import request
from kafkaesque.broker._util import BROKER_NAME, choose_partition, choose_coordinator_partition
from kafkaesque.structs import Color

# =============== ROUTER DECORATORS ===============
def controller(controller_view):
    """Proxy if this broker is not the controller."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            if controller_view["broker_id"] != BROKER_NAME:
                body = request.get_json(force=True) if request.data else None
                return proxy(request.method, controller_view["address"], request.full_path.rstrip("?"), body)
            return fn(*args, **kwargs)
        return wrapped
    return decorator

def partition_leader(topic_registry_cache, partition_assignments_cache, *, topic_arg=None, pid_arg=None, body_topic_key=False):
    """Proxy if this broker is not the leader for the (topic, partition_id)."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            body = request.get_json(force=True) if request.data else None
            topic =  body.get("topic") if body_topic_key else kwargs[topic_arg]
            partition_id = choose_partition(topic_registry_cache.get(topic, {}), body.get("key")) if body_topic_key else kwargs[pid_arg]
            leader_meta = partition_assignments_cache[(topic, int(partition_id))]
            if leader_meta["leader_alias"] != BROKER_NAME:
                return proxy(request.method, leader_meta["leader_address"], request.full_path.rstrip("?"), body)
            return fn(*args, **kwargs)
        return wrapped
    return decorator

def consumer_group_coordinator(topic_registry_cache, partition_assignments_cache, on_proxy=None):
    """Proxy if this broker is not the leader for the group's __consumer_offsets coordinator partition."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            group_id = kwargs["group_id"]
            coordinator_pid = choose_coordinator_partition(group_id, topic_registry_cache["__consumer_offsets"]["partitions"])
            offsets_tuple = ("__consumer_offsets", coordinator_pid)
            leader_meta = partition_assignments_cache[offsets_tuple]
            if leader_meta["leader_alias"] != BROKER_NAME:
                if on_proxy: on_proxy(group_id)  # e.g., prune consumer_groups_cache
                body = request.get_json(force=True) if request.data else None
                return proxy(request.method, leader_meta["leader_address"], request.full_path.rstrip("?"), body)
            return fn(*args, **kwargs)
        return wrapped
    return decorator

# =============== HELPER FUNCTIONS ===============
def proxy(method, host_addr, path, body = None):
    """Proxies the request to the given host address and path."""
    url = f"http://{host_addr}{path}"
    print(f"{Color.GREEN}[✧ PROXY] {method} {path} -> {host_addr}{Color.WHITE}")
    r = requests.request(method, url, json=body, timeout=5)
    headers = {
        "Content-Type": r.headers.get("Content-Type", "application/json"),
        "Via": f"kafkaesque-proxy {BROKER_NAME}",
        "X-Kafkaesque-Proxied": "true",
    }            
    return (r.text, r.status_code, headers)

def prune_local_state(cache, lock):
    """Returns an on_proxy callback that removes a key from cache."""
    def prune(key):
        with lock: cache.pop(key, None)
    return prune
