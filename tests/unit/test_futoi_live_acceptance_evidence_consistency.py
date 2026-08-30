from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GOVERNANCE_PATH = REPO_ROOT / "contracts/intelligence/usdrubf_futoi_live_acceptance_governance_v1.json"
IMPLEMENTATION_PATH = REPO_ROOT / "contracts/intelligence/futoi_live_factual_refresh_implementation_v1.json"
ACCEPTANCE_EVIDENCE_PATH = REPO_ROOT / "contracts/intelligence/futoi_live_smoke_snapshot_acceptance_2026-08-30.json"


def _load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_futoi_governance_and_implementation_evidence_are_consistent() -> None:
    governance = _load(GOVERNANCE_PATH)
    implementation = _load(IMPLEMENTATION_PATH)
    evidence = _load(ACCEPTANCE_EVIDENCE_PATH)
    gates = {gate["gate_id"]: gate for gate in governance["gates"]}

    assert governance["status"] == "FUTOI_GOVERNED_BLOCKED"
    assert gates["canonical_live_smoke"]["status"] == "PASS"
    assert gates["snapshot_live_enable"]["status"] == "PASS"
    assert gates["recurring_live_quality_and_freshness"]["status"] == "BLOCKED"

    assert implementation["status"] == "IMPLEMENTATION_READY_RECURRING_ACCEPTANCE_PENDING"
    assert implementation["runtime_live_smoke_passed"] is True
    assert implementation["snapshot_component_enabled"] is True
    assert implementation["snapshot_candidate_factual_evidence_available"] is True
    assert implementation["recurring_runtime_proof_passed"] is False
    assert implementation["remaining_blocked_gate"] == "recurring_live_quality_and_freshness"
    assert implementation["live_smoke_snapshot_acceptance_ref"] == governance["source_of_truth"]["live_smoke_snapshot_evidence_ref"]

    assert evidence["canonical_live_smoke"]["status"] == "PASS"
    assert evidence["snapshot_integration_smoke"]["status"] == "PASS"
    assert evidence["remaining_acceptance_blocker"] == "recurring_live_quality_and_freshness"

    assert governance["authority"]["factual_live_authority"] is False
    assert implementation["factual_live_authority"] is False
    assert implementation["consumer_factual_use_allowed"] is False
    assert governance["authority"]["directional_authority"] is False
    assert implementation["directional_authority"] is False
    assert governance["authority"]["action_authority"] is False
    assert implementation["action_authority"] is False
