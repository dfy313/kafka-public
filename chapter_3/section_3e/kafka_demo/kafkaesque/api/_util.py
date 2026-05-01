import requests, time

def normalize_url(bootstrap: str) -> str:
    return f"http://{bootstrap}"

def http_request(method, session, base_url, path, payload = None):
    r = session.request(method, f"{base_url}{path}", json=payload, timeout=5)
    r.raise_for_status()
    return r.json()

def pick_live_broker(session, bootstrap_servers):
    """Return a reachable bootstrap server URL."""
    for bootstrap_server in bootstrap_servers:
        try:
            bootstrap_server_url = normalize_url(bootstrap_server)
            _response = http_request("GET", session, bootstrap_server_url, "/healthz")
            print(f"Broker OK at {bootstrap_server_url}/healthz")
            return bootstrap_server_url
        except requests.RequestException:
            print(f"Broker not reachable at {bootstrap_server_url}/healthz")
    raise RuntimeError("No bootstrap servers reachable")

def http_with_retry(method, session, base_url, path, bootstrap_servers, attempts, payload = None):
    """Returns: (result, updated_base_url)"""
    last_error = None
    for _ in range(attempts):
        try:
            result = http_request(method, session, base_url, path, payload=payload)
            return result, base_url  # Success: keep current base
        except requests.RequestException as err:
            last_error = err
            try: # Repick a healthy broker
                base_url = pick_live_broker(session, bootstrap_servers)
            except Exception as pick_err:
                last_error = pick_err
        time.sleep(5) # Cooldown period
    raise RuntimeError(f"Request failed after {attempts} attempts: {last_error}") from last_error
