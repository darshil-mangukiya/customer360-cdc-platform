from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Protocol

from ingestion.cdc_envelope import load_jsonl, normalize_event


class Publisher(Protocol):
    def send(self, topic: str, key: str, value: dict[str, Any]) -> None: ...

    def flush(self) -> None: ...


class KafkaEnvelopePublisher:
    def __init__(self, bootstrap_servers: str) -> None:
        try:
            from kafka import KafkaProducer
            from kafka.serializer import SerializeWrapper
        except ImportError as exc:
            raise RuntimeError("Install kafka-python to publish CDC events to Kafka.") from exc

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=SerializeWrapper(lambda key: key.encode("utf-8")),
            value_serializer=SerializeWrapper(lambda value: json.dumps(value, sort_keys=True).encode("utf-8")),
            acks="all",
            retries=5,
            linger_ms=50,
        )

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self._producer.send(topic, key=key, value=value)

    def flush(self) -> None:
        self._producer.flush()


class DryRunPublisher:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.messages.append((topic, key))

    def flush(self) -> None:
        topic_counts: dict[str, int] = {}
        for topic, _ in self.messages:
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
        print(f"dry_run_topic_counts={topic_counts}")


def publish_file(path: Path, publisher: Publisher) -> tuple[int, int]:
    published = 0
    rejected = 0
    for raw in load_jsonl(str(path)):
        try:
            envelope = normalize_event(raw)
        except ValueError as exc:
            rejected += 1
            print(f"producer_rejected_event event_id={raw.get('event_id')} reason={exc}")
            continue
        publisher.send(
            envelope.topic_name,
            key=f"{envelope.source_system}:{envelope.record_primary_key}",
            value=envelope.as_dict(),
        )
        published += 1
    publisher.flush()
    return published, rejected


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish normalized CDC envelopes to Kafka.")
    parser.add_argument("--input", default="data_generation/output/cdc_events.jsonl")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    publisher: Publisher = DryRunPublisher() if args.dry_run else KafkaEnvelopePublisher(args.bootstrap_servers)
    published, rejected = publish_file(Path(args.input), publisher)
    print(f"published={published} rejected={rejected}")


if __name__ == "__main__":
    main()
