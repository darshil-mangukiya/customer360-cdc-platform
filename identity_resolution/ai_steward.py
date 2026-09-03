"""Guarded AI-steward interface; the offline provider is deterministic and CI-safe."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


ALLOWED_RECOMMENDATIONS = {"MERGE", "DO_NOT_MERGE", "NEEDS_MORE_INFORMATION"}


@dataclass(frozen=True)
class StewardRecommendation:
    recommendation: str
    confidence: float
    reason_codes: tuple[str, ...]
    conflicting_fields: tuple[str, ...]
    recommended_survivorship: dict[str, str]
    requires_human_review: bool = True

    def validate(self) -> None:
        if self.recommendation not in ALLOWED_RECOMMENDATIONS:
            raise ValueError("invalid AI steward recommendation")
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between zero and one")
        if not self.requires_human_review:
            raise ValueError("AI recommendations may not bypass human review")


class StewardProvider(Protocol):
    def recommend(self, masked_evidence: dict[str, Any]) -> StewardRecommendation: ...


class OfflineDeterministicProvider:
    """A test provider, explicitly not an LLM or Cortex execution path."""

    def recommend(self, masked_evidence: dict[str, Any]) -> StewardRecommendation:
        score = float(masked_evidence.get("deterministic_score", 0))
        conflicts = tuple(sorted(str(item) for item in masked_evidence.get("conflicting_fields", [])))
        if score >= 0.78 and not conflicts:
            recommendation, reasons = "MERGE", ("HIGH_DETERMINISTIC_SCORE",)
        elif score < 0.25:
            recommendation, reasons = "DO_NOT_MERGE", ("INSUFFICIENT_MATCH_SIGNALS",)
        else:
            recommendation, reasons = "NEEDS_MORE_INFORMATION", ("AMBIGUOUS_EVIDENCE",)
        result = StewardRecommendation(recommendation, round(score, 4), reasons, conflicts, {}, True)
        result.validate()
        return result


def safe_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    """Allowlist evidence so raw identifiers cannot be sent to an AI provider."""
    allowed = {"case_id", "deterministic_score", "signal_names", "verification_flags", "source_trust", "recency", "conflicting_fields", "survivorship_rules"}
    return {key: payload[key] for key in allowed if key in payload}
