from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

from ingestion.cdc_envelope import NormalizedEnvelope


@dataclass(frozen=True)
class DeduplicationLogRow:
    event_id: str
    tenant_id: str
    source_system: str
    source_table: str
    record_primary_key: str
    event_hash: str
    dedupe_status: str
    duplicate_key: str | None
    failure_category: str | None
    duplicate_of_event_id: str | None
    is_replay: bool
    replay_batch_id: str | None


@dataclass(frozen=True)
class OrderingAnomalyRow:
    anomaly_id: str
    tenant_id: str
    source_system: str
    source_table: str
    record_primary_key: str
    event_id: str
    previous_event_id: str
    previous_sequence_number: int
    current_sequence_number: int
    previous_commit_timestamp: str
    current_commit_timestamp: str
    anomaly_type: str


@dataclass(frozen=True)
class WatermarkCheckpointRow:
    tenant_id: str
    source_system: str
    source_table: str
    last_event_timestamp: str
    last_event_id: str
    last_sequence_number: int
    source_lsn: str | None
    high_watermark: str
    last_successful_run_id: str
    replay_start: str | None
    updated_at: str


@dataclass(frozen=True)
class TopicOffsetCheckpointRow:
    kafka_topic: str
    kafka_partition: int
    max_kafka_offset: int
    last_event_id: str
    tenant_id: str
    source_table: str


def build_deduplication_log(events: list[NormalizedEnvelope]) -> list[DeduplicationLogRow]:
    seen_ids: dict[str, str] = {}
    seen_hashes: dict[str, str] = {}
    seen_offsets: dict[tuple[str, int, int], str] = {}
    rows: list[DeduplicationLogRow] = []
    for event in events:
        event_hash = event.event_hash or event.envelope_hash
        offset_key = None
        if event.kafka_partition is not None and event.kafka_offset is not None:
            offset_key = (event.kafka_topic or event.topic_name, event.kafka_partition, event.kafka_offset)
        duplicate_of = seen_ids.get(event.event_id)
        duplicate_key = "event_id" if duplicate_of else None
        if not duplicate_of:
            duplicate_of = seen_hashes.get(event_hash)
            duplicate_key = "event_hash" if duplicate_of else None
        if not duplicate_of and offset_key:
            duplicate_of = seen_offsets.get(offset_key)
            duplicate_key = "kafka_offset" if duplicate_of else None
        status = "duplicate" if duplicate_of else "accepted"
        if not duplicate_of:
            seen_ids[event.event_id] = event.event_id
            seen_hashes[event_hash] = event.event_id
            if offset_key:
                seen_offsets[offset_key] = event.event_id
        rows.append(
            DeduplicationLogRow(
                event_id=event.event_id,
                tenant_id=event.tenant_id,
                source_system=event.source_system,
                source_table=event.source_table,
                record_primary_key=event.record_primary_key,
                event_hash=event_hash,
                dedupe_status=status,
                duplicate_key=duplicate_key,
                failure_category="DUPLICATE_EVENT" if duplicate_of else None,
                duplicate_of_event_id=duplicate_of,
                is_replay=event.is_replay,
                replay_batch_id=event.replay_batch_id,
            )
        )
    return rows


def build_ordering_anomalies(events: list[NormalizedEnvelope]) -> list[OrderingAnomalyRow]:
    anomalies: list[OrderingAnomalyRow] = []
    latest_by_record: dict[tuple[str, str, str], NormalizedEnvelope] = {}
    for event in events:
        key = (event.tenant_id, event.source_system, event.source_table, event.record_primary_key)
        previous = latest_by_record.get(key)
        if previous:
            out_of_sequence = event.event_sequence_number and previous.event_sequence_number > event.event_sequence_number
            late_commit = previous.source_commit_timestamp and event.source_commit_timestamp
            late_commit = bool(late_commit and previous.source_commit_timestamp > event.source_commit_timestamp)
            if out_of_sequence or late_commit:
                anomaly_type = "out_of_order_sequence" if out_of_sequence else "late_arriving_commit"
                anomalies.append(
                    OrderingAnomalyRow(
                        anomaly_id=f"{event.event_id}:{anomaly_type}",
                        tenant_id=event.tenant_id,
                        source_system=event.source_system,
                        source_table=event.source_table,
                        record_primary_key=event.record_primary_key,
                        event_id=event.event_id,
                        previous_event_id=previous.event_id,
                        previous_sequence_number=previous.event_sequence_number,
                        current_sequence_number=event.event_sequence_number,
                        previous_commit_timestamp=previous.source_commit_timestamp or previous.event_timestamp,
                        current_commit_timestamp=event.source_commit_timestamp or event.event_timestamp,
                        anomaly_type=anomaly_type,
                    )
                )
                continue
        latest_by_record[key] = event
    return anomalies


def build_watermark_checkpoints(
    events: list[NormalizedEnvelope], *, run_id: str = "local_ingestion", replay_start: str | None = None
) -> list[WatermarkCheckpointRow]:
    latest: dict[tuple[str, str, str], NormalizedEnvelope] = {}
    for event in events:
        key = (event.tenant_id, event.source_system, event.source_table)
        previous = latest.get(key)
        if not previous or (event.event_timestamp, event.event_id) >= (previous.event_timestamp, previous.event_id):
            latest[key] = event
    return [
        WatermarkCheckpointRow(
            tenant_id=event.tenant_id,
            source_system=event.source_system,
            source_table=event.source_table,
            last_event_timestamp=event.event_timestamp,
            last_event_id=event.event_id,
            last_sequence_number=event.event_sequence_number,
            source_lsn=event.source_lsn,
            high_watermark=event.source_commit_timestamp or event.event_timestamp,
            last_successful_run_id=run_id,
            replay_start=replay_start,
            updated_at=event.ingested_at,
        )
        for event in sorted(latest.values(), key=lambda row: (row.tenant_id, row.source_system, row.source_table))
    ]


def build_topic_offset_checkpoints(events: list[NormalizedEnvelope]) -> list[TopicOffsetCheckpointRow]:
    latest: dict[tuple[str, int], NormalizedEnvelope] = {}
    for event in events:
        if event.kafka_partition is None or event.kafka_offset is None:
            continue
        key = (event.kafka_topic or event.topic_name, event.kafka_partition)
        previous = latest.get(key)
        if not previous or (event.kafka_offset, event.event_id) >= ((previous.kafka_offset or -1), previous.event_id):
            latest[key] = event
    return [
        TopicOffsetCheckpointRow(
            kafka_topic=event.kafka_topic or event.topic_name,
            kafka_partition=int(event.kafka_partition or 0),
            max_kafka_offset=int(event.kafka_offset or 0),
            last_event_id=event.event_id,
            tenant_id=event.tenant_id,
            source_table=event.source_table,
        )
        for event in sorted(latest.values(), key=lambda row: (row.kafka_topic or row.topic_name, row.kafka_partition or 0))
    ]


def filter_idempotent_events(events: list[NormalizedEnvelope]) -> list[NormalizedEnvelope]:
    audit = build_deduplication_log(events)
    return [event for event, row in zip(events, audit) if row.dedupe_status == "accepted"]


def materialize_latest_state(events: list[NormalizedEnvelope]) -> dict[tuple[str, str, str, str], NormalizedEnvelope]:
    """Apply accepted CDC deterministically without letting old events replace newer state."""
    state: dict[tuple[str, str, str, str], NormalizedEnvelope] = {}
    for event in filter_idempotent_events(events):
        key = (event.tenant_id, event.source_system, event.source_table, event.record_primary_key)
        previous = state.get(key)
        event_order = (event.event_sequence_number, event.source_commit_timestamp or event.event_timestamp, event.event_id)
        previous_order = (
            previous.event_sequence_number,
            previous.source_commit_timestamp or previous.event_timestamp,
            previous.event_id,
        ) if previous else None
        if previous_order is not None and event_order <= previous_order:
            continue
        state[key] = event
    return state


def write_cdc_audit_outputs(events: list[NormalizedEnvelope], output_dir: Path) -> dict[str, int]:
    rows_by_file = {
        "cdc_event_deduplication_log.csv": [asdict(row) for row in build_deduplication_log(events)],
        "cdc_ordering_anomalies.csv": [asdict(row) for row in build_ordering_anomalies(events)],
        "cdc_watermark_checkpoint.csv": [asdict(row) for row in build_watermark_checkpoints(events)],
        "cdc_topic_offset_checkpoint.csv": [asdict(row) for row in build_topic_offset_checkpoints(events)],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, rows in rows_by_file.items():
        path = output_dir / filename
        with path.open("w", newline="", encoding="utf-8") as fh:
            fieldnames = list(rows[0].keys()) if rows else []
            if not fieldnames:
                # Preserve a tangible empty artifact for incident reviews.
                if filename == "cdc_ordering_anomalies.csv":
                    fieldnames = [field.name for field in OrderingAnomalyRow.__dataclass_fields__.values()]
                elif filename == "cdc_event_deduplication_log.csv":
                    fieldnames = [field.name for field in DeduplicationLogRow.__dataclass_fields__.values()]
                elif filename == "cdc_watermark_checkpoint.csv":
                    fieldnames = [field.name for field in WatermarkCheckpointRow.__dataclass_fields__.values()]
                else:
                    fieldnames = [field.name for field in TopicOffsetCheckpointRow.__dataclass_fields__.values()]
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    return {filename: len(rows) for filename, rows in rows_by_file.items()}
