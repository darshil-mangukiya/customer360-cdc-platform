from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion.debezium_mapper import debezium_to_normalized_envelope

CONNECTOR_PATH = ROOT / "connect/debezium/postgres_customer_sources.json"
DEMO_SQL_PATH = ROOT / "source_systems/postgres/init/01_cdc_demo_changes.sql"
DEMO_TABLE = "source_customers_cdc_demo"
DEMO_TOPIC = "cdc.source_customers_cdc_demo"
DEMO_KAFKA_METADATA = [(0, 1100), (0, 1101), (0, 1102)]


def _customer_payload(status: str, updated_at: str) -> dict[str, Any]:
    return {
        "customer_id": "cdc_demo_customer_001",
        "external_account_id": "acct_ext_cdc_demo_001",
        "email": "cdc.demo@example.com",
        "phone": "+14155551010",
        "first_name": "Cdc",
        "last_name": "Demo",
        "tenant_id": "tenant_us",
        "business_unit": "self_serve",
        "customer_status": status,
        "created_at": "2026-06-01T00:00:00Z",
        "updated_at": updated_at,
        "source_updated_at": updated_at,
    }


def sample_debezium_messages() -> list[dict[str, Any]]:
    source_base = {
        "connector": "postgresql",
        "name": "customer_ops",
        "db": "customer_ops",
        "schema": "public",
        "table": DEMO_TABLE,
    }
    inserted = _customer_payload("lead", "2026-06-01T00:00:00Z")
    updated = _customer_payload("active", "2026-06-01T00:05:00Z")
    return [
        {
            "before": None,
            "after": inserted,
            "source": {**source_base, "lsn": 100001, "txId": 9001, "ts_ms": 1780272000000},
            "op": "c",
            "ts_ms": 1780272001000,
        },
        {
            "before": inserted,
            "after": updated,
            "source": {**source_base, "lsn": 100002, "txId": 9002, "ts_ms": 1780272300000},
            "op": "u",
            "ts_ms": 1780272301000,
        },
        {
            "before": updated,
            "after": None,
            "source": {**source_base, "lsn": 100003, "txId": 9003, "ts_ms": 1780272600000},
            "op": "d",
            "ts_ms": 1780272601000,
        },
    ]


def validate_connector_config(path: Path = CONNECTOR_PATH) -> dict[str, Any]:
    connector = json.loads(path.read_text(encoding="utf-8"))
    config = connector.get("config", {})
    required = [
        "connector.class",
        "plugin.name",
        "database.hostname",
        "database.dbname",
        "database.server.name",
        "topic.prefix",
        "slot.name",
        "publication.name",
        "table.include.list",
        "transforms.route.regex",
        "transforms.route.replacement",
    ]
    missing = [field for field in required if not config.get(field)]
    if missing:
        raise ValueError(f"Debezium connector config missing required fields: {missing}")
    include_list = {table.strip() for table in str(config["table.include.list"]).split(",")}
    if f"public.{DEMO_TABLE}" not in include_list:
        raise ValueError(f"Debezium connector config does not include public.{DEMO_TABLE}")
    if config.get("topic.prefix") != "customer_ops":
        raise ValueError("Debezium connector config topic.prefix must be customer_ops for the local proof")
    if config.get("transforms.route.replacement") != "cdc.$1":
        raise ValueError("Debezium connector route transform must map source tables into cdc.* topics")
    return connector


def validate_demo_sql(path: Path = DEMO_SQL_PATH) -> None:
    sql = path.read_text(encoding="utf-8").lower()
    required_snippets = [
        f"create table if not exists public.{DEMO_TABLE}",
        "replica identity full",
        "insert into public.source_customers_cdc_demo",
        "update public.source_customers_cdc_demo",
        "delete from public.source_customers_cdc_demo",
    ]
    missing = [snippet for snippet in required_snippets if snippet not in sql]
    if missing:
        raise ValueError(f"CDC demo SQL missing expected snippets: {missing}")


def build_proof_markdown() -> str:
    connector = validate_connector_config()
    validate_demo_sql()
    source_messages = sample_debezium_messages()
    envelopes = [
        debezium_to_normalized_envelope(
            message,
            topic=DEMO_TOPIC,
            kafka_partition=partition,
            kafka_offset=offset,
        ).as_dict()
        for message, (partition, offset) in zip(source_messages, DEMO_KAFKA_METADATA, strict=True)
    ]
    connector_config = connector["config"]
    sample_rows = "\n".join(
        [
            "| `{event_id}` | `{operation_type}` | `{source_table}` | `{record_primary_key}` | `{topic_name}` | `{source_lsn}` | `{source_transaction_id}` | `{kafka_partition}` | `{kafka_offset}` |".format(
                **envelope
            )
            for envelope in envelopes
        ]
    )
    sample_event = {
        key: envelopes[0][key]
        for key in [
            "event_id",
            "tenant_id",
            "source_system",
            "source_table",
            "operation_type",
            "event_timestamp",
            "record_primary_key",
            "topic_name",
            "source_lsn",
            "source_transaction_id",
            "source_commit_timestamp",
            "kafka_partition",
            "kafka_offset",
            "event_hash",
        ]
    }
    source_sample = {
        "operation": source_messages[1]["op"],
        "before": source_messages[1]["before"],
        "after": source_messages[1]["after"],
        "source": source_messages[1]["source"],
        "topic": DEMO_TOPIC,
        "kafka_partition": DEMO_KAFKA_METADATA[1][0],
        "kafka_offset": DEMO_KAFKA_METADATA[1][1],
    }
    return f"""# Sample Debezium CDC Demo

Local synthetic proof output.

Related command:

```bash
make cdc-demo
```

## Proof Type

Debezium-compatible local CDC proof. This validates the Kafka Connect/Debezium connector configuration, demo source-table SQL, and conversion of Debezium-style insert/update/delete messages into the repository's normalized CDC envelope.

This proof does not claim a running production Debezium pipeline. Full connector execution requires the Docker Compose Kafka Connect stack.

## Connector Compatibility

- Connector class: `{connector_config["connector.class"]}`
- Plugin: `{connector_config["plugin.name"]}`
- Source database: `{connector_config["database.dbname"]}`
- Source server name: `{connector_config["database.server.name"]}`
- Topic prefix: `{connector_config["topic.prefix"]}`
- Publication: `{connector_config["publication.name"]}`
- Slot: `{connector_config["slot.name"]}`
- Demo source table included: `public.{DEMO_TABLE}`
- Routed topic: `{DEMO_TOPIC}`
- Route transform: `{connector_config["transforms.route.regex"]}` -> `{connector_config["transforms.route.replacement"]}`

## Demo Source Changes

Source SQL: `source_systems/postgres/init/01_cdc_demo_changes.sql`

The demo SQL creates `public.{DEMO_TABLE}`, enables `replica identity full`, and defines insert/update/delete changes for one synthetic customer record.

## Normalized CDC Output

| Event ID | Operation | Source Table | Primary Key | Topic | Source LSN | Transaction ID | Partition | Offset |
| --- | --- | --- | --- | --- | --- | --- | ---: | ---: |
{sample_rows}

Sample Debezium-compatible update message with before/after payload and source metadata:

```json
{json.dumps(source_sample, indent=2, sort_keys=True)}
```

Sample normalized insert event:

```json
{json.dumps(sample_event, indent=2, sort_keys=True)}
```
"""


def write_proof(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(build_proof_markdown(), encoding="utf-8")
    print(f"cdc_demo_proof={output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Debezium-compatible local CDC proof artifact.")
    parser.add_argument("--output", default="reports/sample_debezium_cdc_demo.md")
    args = parser.parse_args()
    write_proof(ROOT / args.output)


if __name__ == "__main__":
    main()
