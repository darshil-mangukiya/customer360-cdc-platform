"""Explainable, tenant-safe fuzzy identity candidate scoring.

This module never mutates canonical mappings.  It produces conservative decisions
for deterministic auto-match or human review, with every contributing signal
included in the result.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
import re
from typing import Any, Iterable


SOURCE_TRUST = {
    "account_app": 1.0,
    "billing_platform": 0.95,
    "commerce_platform": 0.8,
    "support_desk": 0.7,
    "marketing_automation": 0.6,
    "product_analytics": 0.5,
}


@dataclass(frozen=True)
class IdentityCandidate:
    tenant_id: str
    record_id: str
    source_system: str
    email: str | None = None
    phone: str | None = None
    name: str | None = None
    address: str | None = None
    updated_at: str | None = None
    email_verified: bool = False
    phone_verified: bool = False


@dataclass(frozen=True)
class MatchSignal:
    name: str
    value: float
    weight: float
    contribution: float
    explanation: str


@dataclass(frozen=True)
class MatchDecision:
    left_record_id: str
    right_record_id: str
    tenant_id: str
    decision: str
    score: float
    blocked_on: tuple[str, ...]
    signals: tuple[MatchSignal, ...]

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["signals"] = [asdict(signal) for signal in self.signals]
        return result


def _text(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _email(value: str | None) -> tuple[str, str] | None:
    normalized = (value or "").strip().lower()
    if "@" not in normalized:
        return None
    local, domain = normalized.split("@", 1)
    return local.split("+", 1)[0], domain


def _phone(value: str | None) -> str:
    digits = re.sub(r"\D", "", value or "")
    return ("1" + digits) if len(digits) == 10 else digits


def _similarity(left: str | None, right: str | None) -> float:
    a, b = _text(left), _text(right)
    return SequenceMatcher(None, a, b).ratio() if a and b else 0.0


def _recency(value: str | None, *, now: datetime | None = None) -> float:
    if not value:
        return 0.5
    observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    reference = now or datetime.now(timezone.utc)
    days = max(0.0, (reference - observed.astimezone(timezone.utc)).total_seconds() / 86400)
    return max(0.2, 1.0 - min(days, 730) / 912.5)


def blocking_keys(record: IdentityCandidate) -> set[str]:
    """Cheap candidate-generation keys; always scoped by tenant."""
    keys: set[str] = set()
    email = _email(record.email)
    phone = _phone(record.phone)
    name = _text(record.name)
    if email:
        keys.add(f"{record.tenant_id}:email_domain:{email[1]}")
        keys.add(f"{record.tenant_id}:email_prefix:{email[0][:4]}")
    if len(phone) >= 7:
        keys.add(f"{record.tenant_id}:phone_suffix:{phone[-7:]}")
    if name:
        keys.add(f"{record.tenant_id}:name_prefix:{name[:5]}")
    return keys


def generate_candidates(records: Iterable[IdentityCandidate]) -> list[tuple[IdentityCandidate, IdentityCandidate]]:
    buckets: dict[str, list[IdentityCandidate]] = {}
    for record in records:
        for key in blocking_keys(record):
            buckets.setdefault(key, []).append(record)
    pairs: dict[tuple[str, str, str], tuple[IdentityCandidate, IdentityCandidate]] = {}
    for bucket in buckets.values():
        for index, left in enumerate(bucket):
            for right in bucket[index + 1 :]:
                if left.tenant_id != right.tenant_id or left.record_id == right.record_id:
                    continue
                ordered = sorted((left, right), key=lambda row: row.record_id)
                pairs[(left.tenant_id, ordered[0].record_id, ordered[1].record_id)] = (ordered[0], ordered[1])
    return [pairs[key] for key in sorted(pairs)]


def score_candidate(left: IdentityCandidate, right: IdentityCandidate) -> MatchDecision:
    if left.tenant_id != right.tenant_id:
        raise ValueError("cross-tenant identity scoring is forbidden")
    left_email, right_email = _email(left.email), _email(right.email)
    email_exact = float(bool(left_email and left_email == right_email))
    phone_exact = float(bool(_phone(left.phone) and _phone(left.phone) == _phone(right.phone)))
    values = {
        "email_exact": (email_exact, 0.30, "normalized email exact match"),
        "phone_exact": (phone_exact, 0.24, "normalized phone exact match"),
        "name_similarity": (_similarity(left.name, right.name), 0.16, "normalized name similarity"),
        "address_similarity": (_similarity(left.address, right.address), 0.10, "normalized address similarity"),
        "verification": ((float(left.email_verified and right.email_verified) * email_exact + float(left.phone_verified and right.phone_verified) * phone_exact) / 2, 0.08, "both matching identifiers are verified"),
        "source_trust": ((SOURCE_TRUST.get(left.source_system, 0.4) + SOURCE_TRUST.get(right.source_system, 0.4)) / 2, 0.07, "mean source-system trust"),
        "recency": ((_recency(left.updated_at) + _recency(right.updated_at)) / 2, 0.05, "recent evidence receives a small boost"),
    }
    signals = tuple(
        MatchSignal(name, round(value, 4), weight, round(value * weight, 4), explanation)
        for name, (value, weight, explanation) in values.items()
    )
    score = round(sum(signal.contribution for signal in signals), 4)
    # AUTO_MATCH requires a verified exact identifier plus corroboration. Fuzzy-only
    # pairs never auto-merge, even when their aggregate score is high.
    verified_exact = (email_exact and left.email_verified and right.email_verified) or (
        phone_exact and left.phone_verified and right.phone_verified
    )
    decision = "AUTO_MATCH" if verified_exact and score >= 0.44 else "REVIEW" if score >= 0.32 else "NO_MATCH"
    blocked = tuple(sorted(blocking_keys(left) & blocking_keys(right)))
    return MatchDecision(left.record_id, right.record_id, left.tenant_id, decision, score, blocked, signals)


def evaluate(decisions: Iterable[MatchDecision], labels: dict[tuple[str, str], str]) -> dict[str, float | int]:
    rows = list(decisions)
    confusion = {"tp": 0, "fp": 0, "tn": 0, "fn": 0, "ambiguous": 0}
    for row in rows:
        key = tuple(sorted((row.left_record_id, row.right_record_id)))
        expected = labels[key]
        predicted = row.decision == "AUTO_MATCH"
        if expected == "AMBIGUOUS":
            confusion["ambiguous"] += 1
        elif expected == "TRUE_MATCH":
            confusion["tp" if predicted else "fn"] += 1
        else:
            confusion["fp" if predicted else "tn"] += 1
    precision = confusion["tp"] / max(1, confusion["tp"] + confusion["fp"])
    recall = confusion["tp"] / max(1, confusion["tp"] + confusion["fn"])
    return {**confusion, "precision": round(precision, 4), "recall": round(recall, 4), "evaluated": len(rows)}
