import json
import sys
import time
from pathlib import Path

import requests

CONNECT_URL = "http://localhost:8083/connectors"
CONFIG_PATH = Path(__file__).with_name("connector.json")


def wait_for_connect(timeout_seconds: int = 120) -> None:
    start = time.time()
    while True:
        try:
            response = requests.get(CONNECT_URL, timeout=5)
            if response.ok:
                return
        except requests.RequestException:
            pass
        if time.time() - start > timeout_seconds:
            raise RuntimeError("Kafka Connect did not become ready in time.")
        print("Waiting for Kafka Connect on http://localhost:8083 ...")
        time.sleep(3)


def main() -> int:
    if not CONFIG_PATH.exists():
        print(f"Missing connector config: {CONFIG_PATH}")
        return 1

    try:
        wait_for_connect()
    except RuntimeError as exc:
        print(str(exc))
        return 1

    payload = json.loads(CONFIG_PATH.read_text())
    name = payload["name"]

    try:
        existing = requests.get(CONNECT_URL, timeout=10)
        existing.raise_for_status()
        connectors = existing.json()

        if name in connectors:
            print(f"Connector '{name}' already exists. Updating it...")
            response = requests.put(f"{CONNECT_URL}/{name}/config", json=payload["config"], timeout=30)
        else:
            print(f"Creating connector '{name}'...")
            response = requests.post(CONNECT_URL, json=payload, timeout=30)

        response.raise_for_status()
    except requests.RequestException as exc:
        print("Connector registration failed.")
        print(str(exc))
        if hasattr(exc, 'response') and exc.response is not None:
            print(exc.response.text)
        return 1

    print("Connector is ready")
    print(response.text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
