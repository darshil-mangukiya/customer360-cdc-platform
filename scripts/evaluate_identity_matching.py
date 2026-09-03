from __future__ import annotations

import json
from pathlib import Path

from identity_resolution.ai_steward import OfflineDeterministicProvider, safe_evidence
from identity_resolution.fuzzy import IdentityCandidate, evaluate, score_candidate


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    cases = json.loads((root / "identity_resolution/fixtures/labeled_identity_pairs.json").read_text(encoding="utf-8"))
    decisions = []
    labels = {}
    ai_agreement = ai_false_positive = ai_false_negative = ai_abstained = 0
    detail = []
    provider = OfflineDeterministicProvider()
    for case in cases:
        decision = score_candidate(IdentityCandidate(**case["left"]), IdentityCandidate(**case["right"]))
        decisions.append(decision)
        labels[tuple(sorted((decision.left_record_id, decision.right_record_id)))] = case["expected"]
        ai = provider.recommend(safe_evidence({"case_id": f"{decision.left_record_id}|{decision.right_record_id}", "deterministic_score": decision.score, "conflicting_fields": []}))
        expected_merge = case["expected"] == "TRUE_MATCH"
        ai_merge = ai.recommendation == "MERGE"
        ai_agreement += int((ai_merge and expected_merge) or (ai.recommendation == "DO_NOT_MERGE" and case["expected"] == "TRUE_NON_MATCH") or (ai.recommendation == "NEEDS_MORE_INFORMATION" and case["expected"] == "AMBIGUOUS"))
        ai_false_positive += int(ai_merge and not expected_merge)
        ai_false_negative += int(expected_merge and not ai_merge)
        ai_abstained += int(ai.recommendation == "NEEDS_MORE_INFORMATION")
        detail.append({"pair": [decision.left_record_id, decision.right_record_id], "expected": case["expected"], "deterministic_decision": decision.decision, "score": decision.score, "ai_offline_recommendation": ai.recommendation})
    metrics = evaluate(decisions, labels)
    ai_metrics = {"agreement": ai_agreement / len(cases), "false_positive_merge_recommendations": ai_false_positive, "false_negative_merge_recommendations": ai_false_negative, "invalid_structured_outputs": 0, "abstention_rate": ai_abstained / len(cases)}
    output = root / "identity_resolution/output"
    output.mkdir(parents=True, exist_ok=True)
    payload = {"provider": "offline_deterministic_not_cortex", "identity_metrics": metrics, "ai_metrics": ai_metrics, "cases": detail}
    (output / "identity_evaluation.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
