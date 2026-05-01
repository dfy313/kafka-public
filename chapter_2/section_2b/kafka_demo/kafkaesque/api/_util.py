import requests

def normalize_url(bootstrap: str) -> str:
    return f"http://{bootstrap}"

def http_get_json(session, base_url, path, timeout_s = 5):
    r = session.get(f"{base_url}{path}", timeout=timeout_s)
    r.raise_for_status()
    return r.json()

def http_post_json(session, base_url, path, payload, timeout_s = 5):
    r = session.post(f"{base_url}{path}", json=payload, timeout=timeout_s)
    r.raise_for_status()
    return r.json()

def pick_live_broker(session, bootstrap_servers):
    """Return a reachable bootstrap server URL."""
    for bootstrap_server in bootstrap_servers:
        try:
            bootstrap_server_url = normalize_url(bootstrap_server)
            _response = http_get_json(session, bootstrap_server_url, "/healthz")
            print(f"Broker OK at {bootstrap_server_url}/healthz")
            return bootstrap_server_url
        except requests.RequestException:
            print(f"Broker not reachable at {bootstrap_server_url}/healthz")
    raise RuntimeError("No bootstrap servers reachable")
