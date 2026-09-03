from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


CONTRACT_PATH = Path(__file__).resolve().parents[1] / "contracts" / "cdc_payload_contracts.json"


@lru_cache(maxsize=1)
def load_contracts(path: str | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path else CONTRACT_PATH
    if not contract_path.exists():
        return {}
    with contract_path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _matches_type(value: Any, expected_type: str) -> bool:
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "object":
        return isinstance(value, dict)
    if expected_type == "array":
        return isinstance(value, list)
    return True


def validate_payload_contract(source_table: str, payload: dict[str, Any] | None) -> None:
    if payload is None:
        raise ValueError("payload is required for contract validation")

    contracts = load_contracts()
    contract = contracts.get(source_table)
    if not contract:
        return

    required = set(contract.get("required", []))
    nullable = set(contract.get("nullable", []))
    missing = sorted(field for field in required if field not in payload or payload.get(field) is None)
    if missing:
        raise ValueError(f"payload contract violation for {source_table}: missing required fields {missing}")

    types = contract.get("types", {})
    for field, expected_type in types.items():
        if field not in payload:
            continue
        value = payload[field]
        if value is None and field in nullable:
            continue
        if value is None and field not in nullable:
            raise ValueError(f"payload contract violation for {source_table}: {field} may not be null")
        if not _matches_type(value, expected_type):
            raise ValueError(
                f"payload contract violation for {source_table}: {field} expected {expected_type}, got {type(value).__name__}"
            )

    enums = contract.get("enums", {})
    for field, allowed_values in enums.items():
        if field not in payload or payload[field] is None:
            continue
        if payload[field] not in allowed_values:
            raise ValueError(
                f"payload contract violation for {source_table}: {field}={payload[field]!r} not in {allowed_values}"
            )

