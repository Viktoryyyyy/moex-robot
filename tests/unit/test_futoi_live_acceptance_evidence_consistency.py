from __future__ import annotations

import json
import hashlib
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

    assert governance["status"] == "FUTOI_LIVE_ACCEPTED_FACTUAL_CONTEXT_ONLY_FOR_EXPLICITLY_ACCEPTED_INSTRUMENTS"
    assert gates["canonical_live_smoke"]["status"] == "PASS"
    assert gates["snapshot_live_enable"]["status"] == "PASS"
    assert gates["recurring_live_quality_and_freshness"]["status"] == "PASS"

    # Preserve the old checkpoint; it must not masquerade as current acceptance.
    assert implementation["evidence_scope"] == "historical_implementation_checkpoint_before_2026_09_07_factual_acceptance"
    assert REPO_ROOT / implementation["current_governance_ref"] == GOVERNANCE_PATH
    recurring_ref = governance["source_of_truth"]["recurring_scheduled_evidence_ref"]
    assert implementation["subsequent_acceptance_evidence_ref"] == recurring_ref
    recurring = _load(REPO_ROOT / recurring_ref)
    assert recurring["acceptance_decision"]["recurring_scheduled_gate"] == "PASS"
    assert recurring["scheduled_run"]["journal_binding"]["manifest_content_matches"] is True
    assert recurring["frozen_evidence"]["all_eight_digests_verified"] is True
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

    assert governance["authority"]["factual_live_authority"] is True
    assert governance["authority"]["factual_live_instrument_scope"] == ["si_futures_family"]
    assert governance["instrument_acceptance"]["cr_futures_family"]["factual_live_authority"] is False
    assert implementation["factual_live_authority"] is False
    assert implementation["consumer_factual_use_allowed"] is False
    assert governance["authority"]["directional_authority"] is False
    assert implementation["directional_authority"] is False
    assert governance["authority"]["action_authority"] is False
    assert implementation["action_authority"] is False


def test_cr_current_pair_scope_has_hash_bound_acceptance_without_broad_authority():
    contract = _load(GOVERNANCE_PATH)
    cr = contract["instrument_acceptance"]["cr_futures_family"]
    entry = cr["current_pair_acceptance"]
    path = REPO_ROOT / entry["evidence_ref"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["evidence_sha256"]
    evidence = _load(path)
    assert entry["accepted"] is True
    assert entry["scope"] == evidence["scope"] == "current_intraday_latest_pair_only"
    assert evidence["canonical_live_smoke"] == evidence["negative_replay"] == "PASS"
    assert evidence["attachment_scope_check"] == evidence["read_expiry_check"] == "PASS"
    assert evidence["rejected_archives_survive_next_refresh"] == "PASS"
    assert evidence["historical_authority"] is False
    assert evidence["provider_root_cause_established"] is False
    assert cr["factual_live_authority"] is False
    assert entry["previous_session_authority"] is entry["delta_statistics_authority"] is False
