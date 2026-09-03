from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib import request


def register_connector(*, connect_url: str, config_path: Path) -> dict[str, object]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    name = payload["name"]
    config = payload["config"]
    body = json.dumps(config).encode("utf-8")
    req = request.Request(
        f"{connect_url.rstrip('/')}/connectors/{name}/config",
        data=body,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    with request.urlopen(req, timeout=30) as response:
        response_body = response.read().decode("utf-8")
    return {"name": name, "status_code": response.status, "response": json.loads(response_body)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Register or update the Debezium source connector.")
    parser.add_argument("--connect-url", default=os.getenv("CONNECT_URL", "http://localhost:8083"))
    parser.add_argument("--config", default=os.getenv("CONNECTOR_CONFIG", "connect/debezium/postgres_customer_sources.json"))
    args = parser.parse_args()
    result = register_connector(connect_url=args.connect_url, config_path=Path(args.config))
    print(f"connector={result['name']} status_code={result['status_code']}")


if __name__ == "__main__":
    main()
