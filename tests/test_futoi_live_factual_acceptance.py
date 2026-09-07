"""Recorded acceptance and unchanged runtime authority boundaries.

These tests validate repository policy and integration behavior. They do not
claim to re-read the server-only frozen artifacts; their actual byte/replay
verification is recorded separately in the Applied State evidence record.
"""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from moex_data import step10_rub_refresh_dispatcher as dispatcher
from moex_data import step10_rub_refresh_scheduler as scheduler
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current_context as context
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as futoi


REPO = Path(__file__).resolve().parents[1]
SI = "si_futures_family"
CR = "cr_futures_family"
EVIDENCE_REF = "contracts/intelligence/futoi_live_factual_acceptance_evidence_2026-09-07.json"


def governance():
    return json.loads((REPO / futoi.FUTOI_GOVERNANCE_RELATIVE_PATH).read_text(encoding="utf-8"))


def evidence():
    return json.loads((REPO / EVIDENCE_REF).read_text(encoding="utf-8"))


def test_recorded_partial_acceptance_is_internally_consistent():
    values = governance()
    states = {key: futoi._governance_state(values, key) for key in (SI, CR)}
    assert states[SI]["factual_use_allowed"] is True
    assert states[CR]["factual_use_allowed"] is False
    assert all(state["all_required_gates_pass"] for state in states.values())
    required = [gate for gate in values["gates"] if gate["required"] is True]
    passed = [gate for gate in required if gate["status"] == "PASS"]
    progress = values["acceptance_progress"]
    assert len(required) == len(passed) == progress["required_gate_count"] == progress["passed_gate_count"] == 10
    assert progress["blocked_gate_ids"] == []
    assert progress["accepted_instrument_ids"] == [SI]
    assert progress["blocked_instrument_ids"] == [CR]
    assert values["authority"]["factual_live_instrument_scope"] == [SI]
    assert states[SI]["local_blockers"] == []
    assert states[CR]["canonical_live_smoke_accepted"] is False
    assert "cr_current_intraday_balance_failure_unresolved" in states[CR]["local_blockers"]
    for authority in [values["authority"], *values["instrument_acceptance"].values()]:
        assert authority["directional_authority"] is False
        assert authority["action_authority"] is False
    assert values["authority"]["buy_sell_authority"] is False
    assert all(item["standalone_buy_sell_authority"] is False for item in values["instrument_acceptance"].values())


def test_scheduled_evidence_does_not_relabel_manual_smoke_or_accept_cr():
    values, proof = governance(), evidence()
    gate = next(g for g in values["gates"] if g["gate_id"] == "recurring_live_quality_and_freshness")
    assert gate["status"] == "PASS"
    assert "blocker" not in gate
    assert gate["evidence"]["artifact_ref"] == EVIDENCE_REF
    run = proof["scheduled_run"]
    assert gate["evidence"]["run_id"] == run["run_id"] == "step10_daily_20260907_213001"
    assert gate["evidence"]["scheduled_applied_sha"] == run["scheduled_applied_sha"]
    assert run["scheduled_applied_sha"] != proof["verification_applied_sha"]
    assert run["journal_binding"]["status"] == "PASS"
    assert run["journal_binding"]["manifest_content_matches"] is True
    assert run["journal_binding"]["manual_smoke_relabelled_as_scheduled"] is False
    assert proof["frozen_evidence"]["all_eight_digests_verified"] is True
    assert set(proof["frozen_evidence"]["instrument_results"]) == {SI, CR}
    for key, item in proof["frozen_evidence"]["instrument_results"].items():
        assert item["run_id"] == run["run_id"] + "_futoi_factual_" + key
        assert item["trade_date"] == run["through_date"] == gate["evidence"]["trade_date"]
        assert item["frozen_replay"] == item["quality_status"] == "PASS"
        assert item["freshness_at_run"] == "FRESH"
        assert item["fiz_seqnum"] == item["yur_seqnum"] == 218
        assert item["fiz_net"] + item["yur_net"] == 0
        assert item["total_long"] == item["total_short"]
        for field in ("run_evidence_sha256", "raw_partition_sha256", "raw_quality_report_sha256", "raw_refresh_manifest_sha256"):
            digest = item[field]
            assert len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)
    assert proof["freshness_semantics"]["calendar_dependency"] is False
    assert proof["freshness_semantics"]["weekday_weekend_inference"] is False
    assert all(value is False for value in proof["preserved_boundaries"].values())
    assert values["instrument_acceptance"][CR]["factual_live_authority"] is False


@pytest.mark.parametrize("gate_index", range(10))
def test_any_required_gate_failure_still_blocks_si(gate_index):
    values = governance()
    values["gates"][gate_index]["status"] = "BLOCKED"
    assert futoi._governance_state(values, SI)["factual_use_allowed"] is False


def test_global_acceptance_and_a_cr_flag_do_not_bypass_cr_local_gates():
    values = governance()
    values["instrument_acceptance"][CR]["factual_live_authority"] = True
    assert futoi._governance_state(values, CR)["factual_use_allowed"] is False


def test_factual_acceptance_keeps_stage5_disabled():
    state = scheduler._futoi_stage5_promotion_governance(REPO)
    assert state["all_required_gates_pass"] is True
    assert state["factual_live_authority"] is True
    assert state["stage5_promotion_authority"] is False
    assert state["promotion_allowed"] is False
    assert dispatcher.STAGE5_FULL_MODE_READY is False


@pytest.mark.parametrize("current_status,has_factual,expected_status", [
    ("FRESH", True, "READY"),
    ("RETAINED_STALE", True, "RETAINED_PREVIOUS"),
    ("UNAVAILABLE", False, "UNAVAILABLE"),
])
def test_snapshot_readiness_remains_per_instrument_and_quality_gated(
    monkeypatch, current_status, has_factual, expected_status,
):
    values = governance()
    monkeypatch.setattr(futoi, "_load_governance", lambda: deepcopy(values))
    monkeypatch.setattr(futoi, "_recompute_readiness", lambda snapshot: None)
    snapshot = {"components": {}, "authority": {}}
    instruments = {}
    for key in (SI, CR):
        factual = {"snapshot_ts": "2026-09-07T12:35:00+00:00"} if has_factual else None
        instruments[key] = {
            "current_intraday": {"status": current_status, "factual": factual},
            "previous_completed_session": {"status": "FRESH" if has_factual else "UNAVAILABLE", "factual": factual},
        }
    delta = {"status": "PARTIAL", "deltas": {"delta_20d": {"status": "UNAVAILABLE"}}}
    context._attach_futoi_context(
        snapshot,
        {"instrument_results": instruments},
        {"instrument_results": {SI: deepcopy(delta), CR: deepcopy(delta)}},
    )
    si = snapshot["components"]["futoi_live"]
    cr = snapshot["components"]["futoi_live_cr"]
    assert si["status"] == expected_status
    assert si["data"]["consumer_factual_use_allowed"] is has_factual
    assert si["data"]["delta_statistics"] == delta
    assert cr["status"] == "UNAVAILABLE"
    assert cr["data"]["consumer_factual_use_allowed"] is False
    for component in (si, cr):
        for field in ("directional_authority", "action_authority", "standalone_buy_sell_authority", "stage5_full_mode_ready", "stage5_pointer_promotion_performed"):
            assert component["data"][field] is False
    if current_status != "FRESH":
        assert si["status"] != "READY"
