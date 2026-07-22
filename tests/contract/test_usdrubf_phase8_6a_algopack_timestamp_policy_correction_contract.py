from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parents[2]
CONTRACT_PATH = (
    ROOT
    / "contracts/experiments/usdrubf_phase8_6a_algopack_cnyrub_timestamp_policy_correction_v1.json"
)
CONTRACT = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
RUNTIME = (
    ROOT
    / "src/moex_research/runners/usdrubf_phase8_6a_algopack_cnyrub_runtime.py"
).read_text(encoding="utf-8")
POLICY_SOURCE = (
    ROOT
    / "src/moex_research/external_data/moex_cnyrub_algopack_timestamp_policy.py"
).read_text(encoding="utf-8")


def test_contract_identity_and_exact_source() -> None:
    identity = CONTRACT["contract_identity"]
    assert identity == {
        "contract_id": "usdrubf_phase8_6a_algopack_cnyrub_timestamp_policy_correction_v1",
        "contract_version": "1.0",
        "project": "MOEX Bot",
        "task_id": "ema_3_19_ai_phase_8_6a_algopack_cnyrub_timestamp_policy_correction",
        "lane": "ema_3_19_ai",
        "phase": "8.6A",
        "status": "runtime_correction_only",
    }
    source = CONTRACT["source_identity"]
    assert source["source_id"] == "moex_algopack_cnyrub_tom_tradestats_5m"
    assert source["security_id"] == "CNYRUB_TOM"
    assert source["board_id"] == "CETS"
    assert source["bucket_interval_minutes"] == 5


def test_live_evidence_rejects_the_legacy_plus_five_minute_assumption() -> None:
    evidence = CONTRACT["live_evidence"]
    assert evidence["http_status"] == 200
    assert evidence["page_rows"] == evidence["cursor_total"] == 109
    assert evidence["systime_minus_tradetime_min_seconds"] == 10
    assert evidence["systime_minus_tradetime_max_seconds"] == 30
    assert evidence["systime_before_tradetime_count"] == 0
    assert evidence["systime_before_tradetime_plus_five_minutes_count"] == 109
    assert evidence["token_value_exposed"] is False


def test_timestamp_policy_is_exact_and_bound_to_canonical_runtime() -> None:
    policy = CONTRACT["timestamp_policy"]
    assert policy["policy_id"] == (
        "algopack_tradetime_is_five_minute_interval_end_v1"
    )
    assert policy["provider_tradetime_role"] == "five_minute_interval_end"
    assert policy["bucket_begin"] == "bucket_end minus five minutes"
    assert policy["source_available_at"] == "SYSTIME in Europe/Moscow"
    assert policy["row_completion_required"] == (
        "SYSTIME must be greater than or equal to bucket_end"
    )
    assert policy["forecast_anchor_rule_unchanged"] is True

    runtime = CONTRACT["runtime_policy"]
    assert runtime["install_before_validation_required"] is True
    assert "install_timestamp_policy()" in RUNTIME
    assert "return validation_main(argv)" in RUNTIME
    assert policy["policy_id"] in POLICY_SOURCE
    assert "bucket_begin = bucket_end - timedelta" in POLICY_SOURCE
    assert "source_available_at < bucket_end" in POLICY_SOURCE


def test_authority_remains_fail_closed() -> None:
    authority = CONTRACT["authority_boundary"]
    assert authority["direct_main_write_allowed"] is False
    assert authority["merge_allowed"] is False
    assert authority["server_apply_allowed"] is False
    assert authority["runtime_rerun_allowed"] is False
    assert authority["model_fit_allowed"] is False
    assert authority["model_promotion_allowed"] is False
    assert authority["broker_action_allowed"] is False
    assert authority["trading_allowed"] is False
