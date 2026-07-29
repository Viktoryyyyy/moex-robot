from __future__ import annotations

import json
from pathlib import Path


CONTRACT_PATH = Path(
    "contracts/experiments/usdrubf_phase8_6a_algopack_cnyrubf_fo_source_correction_v1.json"
)


def load_contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_and_route() -> None:
    contract = load_contract()
    identity = contract["contract_identity"]
    source = contract["source_identity"]

    assert identity["project"] == "MOEX_Bot"
    assert identity["task_id"] == "ema_3_19_ai_phase_8_6a_cnyrubf_fo_source_correction_v1"
    assert identity["execution_mode"] == "browser_controlled_github_route"
    assert source["tradestats_route"] == (
        "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/CNYRUBF.json"
    )
    assert source["security_id"] == "CNYRUBF"
    assert source["asset_code"] == "CNYRUBTOM"
    assert source["board_id"] == "RFUD"
    assert source["engine"] == "futures"
    assert source["market"] == "forts"
    assert source["algopack_market_code"] == "FO"
    assert source["security_type"] == "perpetual_one_day_future"
    assert source["contract_roll_mapping_required"] is False


def test_owner_decision_forbids_spot_and_substitutions() -> None:
    contract = load_contract()
    decision = contract["owner_decision"]
    identity_policy = contract["identity_validation_policy"]
    timestamp_policy = contract["timestamp_policy"]

    assert decision["target_security_id"] == "CNYRUBF"
    assert decision["spot_source_allowed"] is False
    assert decision["spot_fallback_allowed"] is False
    assert decision["synthetic_cross_allowed"] is False
    assert decision["bank_of_russia_rate_allowed_as_causal_input"] is False
    assert identity_policy["secid_must_equal"] == "CNYRUBF"
    assert identity_policy["asset_code_must_equal"] == "CNYRUBTOM"
    assert identity_policy["substitute_security_allowed"] is False
    assert identity_policy["substitute_asset_code_allowed"] is False
    assert identity_policy["substitute_market_allowed"] is False
    assert timestamp_policy["target_day_data_allowed"] is False
    assert timestamp_policy["forward_fill_allowed"] is False
    assert timestamp_policy["backward_fill_allowed"] is False


def test_live_probe_and_timestamp_semantics_are_frozen() -> None:
    contract = load_contract()
    probe = contract["live_probe_evidence"]
    timestamp_policy = contract["timestamp_policy"]

    assert probe["http_status"] == 200
    assert probe["row_count"] == 178
    assert probe["security_ids"] == ["CNYRUBF"]
    assert probe["asset_codes"] == ["CNYRUBTOM"]
    assert probe["systime_minus_tradetime_min_seconds"] == 2
    assert probe["systime_minus_tradetime_max_seconds"] == 8
    assert probe["rows_with_systime_before_tradetime"] == 0
    assert probe["token_exposed"] is False
    assert probe["payload_persisted"] is False
    assert timestamp_policy["tradetime_semantics"] == "completed_five_minute_interval_end"
    assert timestamp_policy["bucket_end"] == "tradedate + tradetime"
    assert timestamp_policy["bucket_begin"] == "bucket_end - 5 minutes"
    assert timestamp_policy["source_available_at"] == "SYSTIME"
    assert timestamp_policy["row_completion_rule"] == "SYSTIME >= bucket_end"


def test_spot_runtime_is_superseded_and_preserved() -> None:
    contract = load_contract()
    supersession = contract["supersession"]

    assert supersession["superseded_source_id"] == "moex_algopack_cnyrub_tom_tradestats_5m"
    assert (
        supersession["superseded_runtime_id"]
        == "phase8_6a_algopack_cnyrub_source_validation_20260729_v1"
    )
    assert (
        supersession["superseded_runtime_classification"]
        == "superseded_non_target_source_evidence"
    )
    assert supersession["superseded_artifacts_must_be_preserved"] is True
    assert supersession["superseded_runtime_must_not_be_repeated"] is True
    assert supersession["superseded_result_must_not_enter_phase8_6b"] is True


def test_runtime_artifact_inventory_and_authority_boundary() -> None:
    contract = load_contract()

    assert contract["required_runtime_artifacts"] == [
        "input_identity_verification.json",
        "official_route_validation.json",
        "cnyrubf_security_identity.json",
        "cnyrubf_daily_candles_normalized.parquet",
        "cnyrubf_pit_acceptance_matrix.parquet",
        "coverage_by_source.csv",
        "session_alignment_diagnostics.csv",
        "source_blocker_register.json",
        "gate_results.json",
    ]
    assert contract["source_identity"]["pass_status"] == (
        "moex_algopack_cnyrubf_source_candidate_for_phase8_6b"
    )
    assert contract["source_identity"]["fail_status"] == (
        "moex_algopack_cnyrubf_source_not_ready"
    )
    assert "no server apply" in contract["non_authorizations"]
    assert "no controlled runtime" in contract["non_authorizations"]
    assert "no trading action" in contract["non_authorizations"]
