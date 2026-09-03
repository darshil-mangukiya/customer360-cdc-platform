from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from ingestion.cdc_envelope import NormalizedEnvelope
from ingestion.debezium_mapper import debezium_to_normalized_envelope, is_debezium_message
from ingestion.loader import build_rejection_record, load_to_postgres


def consume_to_postgres(
    *,
    dsn: str,
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
    max_messages: int | None = None,
    resolve_identity_batch: bool = False,
) -> int:
    try:
        from kafka import KafkaConsumer
        from kafka.serializer import DeserializeWrapper
    except ImportError as exc:
        raise RuntimeError("Install kafka-python to consume CDC events from Kafka.") from exc

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        key_deserializer=DeserializeWrapper(lambda key: key.decode("utf-8") if key else None),
        value_deserializer=DeserializeWrapper(lambda value: json.loads(value.decode("utf-8"))),
        auto_offset_reset="earliest",
    )

    count = 0
    rejected_count = 0
    batch: list[NormalizedEnvelope] = []
    rejected: list[dict] = []
    started = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        for message in consumer:
            value = message.value
            try:
                if is_debezium_message(value):
                    batch.append(
                        debezium_to_normalized_envelope(
                            value,
                            topic=message.topic,
                            kafka_partition=message.partition,
                            kafka_offset=message.offset,
                        )
                    )
                else:
                    batch.append(NormalizedEnvelope(**value))
            except Exception as exc:
                payload = value.get("payload", value) if isinstance(value, dict) else {}
                source = payload.get("source", {}) if isinstance(payload, dict) else {}
                forensic = {
                    "source_system": source.get("name") or source.get("db") or "debezium_postgres",
                    "source_table": source.get("table") or message.topic.rsplit(".", 1)[-1],
                    "batch_id": "debezium_runtime",
                    "kafka_topic": message.topic,
                    "kafka_partition": message.partition,
                    "kafka_offset": message.offset,
                    "debezium_message": value,
                }
                rejected.append(build_rejection_record(forensic, exc, failure_stage="debezium_normalization"))
                rejected_count += 1
            count += 1
            if len(batch) + len(rejected) >= 500 or (max_messages and count >= max_messages):
                load_to_postgres(dsn=dsn, landed=batch, rejected=rejected, summaries=[])
                if resolve_identity_batch and batch:
                    from identity_resolution.resolver import load_identity_to_postgres, resolve_identity

                    canonical, mappings, audit = resolve_identity(batch)
                    load_identity_to_postgres(dsn=dsn, canonical=canonical, mappings=mappings, audit=audit)
                consumer.commit()
                batch = []
                rejected = []
            if max_messages and count >= max_messages:
                break

        if batch or rejected:
            load_to_postgres(dsn=dsn, landed=batch, rejected=rejected, summaries=[])
            if resolve_identity_batch and batch:
                from identity_resolution.resolver import load_identity_to_postgres, resolve_identity

                canonical, mappings, audit = resolve_identity(batch)
                load_identity_to_postgres(dsn=dsn, canonical=canonical, mappings=mappings, audit=audit)
            consumer.commit()
    finally:
        consumer.close()
    ended = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    print(f"consumed={count} rejected={rejected_count} started={started} ended={ended}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume CDC envelopes from Kafka into Postgres.")
    parser.add_argument("--dsn", default=os.getenv("WAREHOUSE_DSN", "postgresql://c360:c360@localhost:5432/c360"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--group-id", default="customer360-cdc-loader")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument(
        "--resolve-identity",
        action="store_true",
        help="Resolve each landed batch and persist its canonical identity records before committing Kafka offsets.",
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        default=[
            "cdc.customers",
            "cdc.subscriptions",
            "cdc.orders",
            "cdc.engagement_events",
            "cdc.support_interactions",
            "cdc.marketing_engagement",
        ],
    )
    args = parser.parse_args()
    consume_to_postgres(
        dsn=args.dsn,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        topics=args.topics,
        max_messages=args.max_messages,
        resolve_identity_batch=args.resolve_identity,
    )


if __name__ == "__main__":
    main()
