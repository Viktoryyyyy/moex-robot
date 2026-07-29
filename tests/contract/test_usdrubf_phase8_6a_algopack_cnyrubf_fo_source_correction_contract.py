from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CONTRACT_PATH = Path(
    "contracts/experiments/usdrubf_phase8_6a_algopack_cnyrubf_fo_source_correction_v1.json"
)


def load_contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_and_stage_are_exact() -> None:
    contract = load_contract()
    identity = contract["contract_identity"]

    assert identity["project"] == "MOEX_Bot"
    assert identity["task_id"] == "ema_3_19_ai_phase_8_6a_cnyrubf_fo_source_correction_v1"
    assert identity["execution_mode"] == "browser_controlled_github_route"
    assert identity["contract_version"] == "1.1"
    assert identity["status"] == "source_correction_contract_pending_implementation"
    assert contract["approved_branch"] == (
        "research/ema-3-19-ai/phase-8-6a-cnyrubf-fo-source-correction-v1"
    )


def test_exact_algopack_and_iss_routes_are_frozen() -> None:
    contract = load_contract()
    source = contract["source_identity"]

    assert source["tradestats_route"] == (
        "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/CNYRUBF.json"
    )
    assert source["security_metadata_route"] == (
        "https://iss.moex.com/iss/securities/CNYRUBF.json?"
        "iss.meta=off&iss.only=description%2Cboards"
    )
    assert source["security_id"] == "CNYRUBF"
    assert source["asset_code"] == "CNYRUBTOM"
    assert source["board_id"] == "RFUD"
    assert source["engine"] == "futures"
    assert source["market"] == "forts"
    assert source["algopack_market_code"] == "FO"
    assert source["security_type"] == "perpetual_one_day_future"
    assert source["contract_roll_mapping_required"] is False
    assert source["implicit_contract_month_selection_allowed"] is False


def test_metadata_identity_is_fail_closed() -> None:
    policy = load_contract()["metadata_identity_policy"]

    assert policy["metadata_route_must_equal"] == (
        "https://iss.moex.com/iss/securities/CNYRUBF.json?"
        "iss.meta=off&iss.only=description%2Cboards"
    )
    assert policy["metadata_query_must_equal"] == {
        "iss.meta": "off",
        "iss.only": "description,boards",
    }
    assert policy["required_blocks"] == ["description", "boards"]
    assert policy["required_description_fields"] == ["SECID"]
    assert set(policy["required_board_fields"]) == {
        "secid",
        "boardid",
        "engine",
        "market",
        "is_traded",
        "is_primary",
        "history_from",
        "history_till",
    }
    assert policy["secid_must_equal"] == "CNYRUBF"
    assert policy["boardid_must_equal"] == "RFUD"
    assert policy["engine_must_equal"] == "futures"
    assert policy["market_must_equal"] == "forts"
    assert policy["is_primary_must_equal"] == 1
    assert policy["is_traded_must_equal"] == 1
    assert policy["exactly_one_matching_board_required"] is True
    assert policy["history_from_required"] is True
    assert policy["history_interval_must_be_valid"] is True
    assert policy["metadata_live_probe_required_before_controlled_runtime"] is True
    assert policy["identity_mismatch_blocker"] == "security_identity_not_reproducible"
    assert policy["schema_mismatch_blocker"] == "official_schema_not_stable"
    assert policy["route_mismatch_blocker"] == "provenance_not_sufficient"


def test_owner_decision_and_source_policy_forbid_substitutions() -> None:
    contract = load_contract()
    decision = contract["owner_decision"]
    identity = contract["identity_validation_policy"]
    source_policy = contract["official_source_policy"]

    assert decision["target_security_id"] == "CNYRUBF"
    assert decision["target_algopack_market_code"] == "FO"
    assert decision["spot_source_allowed"] is False
    assert decision["spot_fallback_allowed"] is False
    assert decision["synthetic_cross_allowed"] is False
    assert decision["bank_of_russia_rate_allowed_as_causal_input"] is False
    assert identity["secid_must_equal"] == "CNYRUBF"
    assert identity["asset_code_must_equal"] == "CNYRUBTOM"
    assert identity["substitute_security_allowed"] is False
    assert identity["substitute_asset_code_allowed"] is False
    assert identity["substitute_market_allowed"] is False
    assert identity["mixed_security_rows_allowed"] is False
    assert source_policy["allowed_hosts"] == ["apim.moex.com", "iss.moex.com"]
    assert source_policy["exact_scheme_host_port_path_query_required"] is True
    assert source_policy["redirects_allowed"] is False
    assert source_policy["cross_host_redirect_allowed"] is False
    assert source_policy["authorization_on_redirect_allowed"] is False
    assert source_policy["spot_route_allowed"] is False
    assert source_policy["synthetic_cross_allowed"] is False
    assert source_policy["bank_of_russia_rate_allowed"] is False


def test_live_probe_and_timestamp_semantics_are_frozen() -> None:
    contract = load_contract()
    probe = contract["live_probe_evidence"]
    timestamp = contract["timestamp_policy"]

    assert probe["http_status"] == 200
    assert probe["row_count"] == 178
    assert probe["security_ids"] == ["CNYRUBF"]
    assert probe["asset_codes"] == ["CNYRUBTOM"]
    assert probe["cursor_columns"] == ["INDEX", "TOTAL", "PAGESIZE"]
    assert probe["cursor_data"] == [[0, 178, 1000]]
    assert probe["systime_minus_tradetime_min_seconds"] == 2
    assert probe["systime_minus_tradetime_max_seconds"] == 8
    assert probe["rows_with_systime_before_tradetime"] == 0
    assert probe["token_exposed"] is False
    assert probe["payload_persisted"] is False
    assert timestamp["tradetime_semantics"] == "completed_five_minute_interval_end"
    assert timestamp["bucket_end"] == "tradedate + tradetime"
    assert timestamp["bucket_begin"] == "bucket_end - 5 minutes"
    assert timestamp["source_available_at"] == "SYSTIME"
    assert timestamp["row_completion_rule"] == "SYSTIME >= bucket_end"
    assert timestamp["source_available_at_before_anchor_required"] is True
    assert timestamp["target_day_data_allowed"] is False
    assert timestamp["later_observation_allowed"] is False
    assert timestamp["forward_fill_allowed"] is False
    assert timestamp["backward_fill_allowed"] is False
    assert timestamp["arbitrary_last_available_date_allowed"] is False


def test_pagination_http_and_secret_boundaries_are_complete() -> None:
    contract = load_contract()
    pagination = contract["pagination_policy"]
    http = contract["http_outcome_policy"]
    auth = contract["authorization_policy"]

    assert pagination["cursor_index_must_equal_requested_start"] is True
    assert pagination["cursor_total_must_be_constant"] is True
    assert pagination["returned_rows_must_not_exceed_remaining_total"] is True
    assert pagination["final_accumulated_count_must_equal_total"] is True
    assert pagination["start_greater_than_total_allowed"] is False
    assert pagination["overlapping_pages_allowed"] is False
    assert pagination["skipped_pages_allowed"] is False
    assert pagination["duplicate_provider_row_identity_allowed"] is False
    assert pagination["premature_empty_page_allowed"] is False
    assert http["HTTP_401"] == "algopack_authentication_failed"
    assert http["HTTP_403"] == "algopack_subscription_not_entitled"
    assert http["HTTP_404_route"] == "official_route_not_reproducible"
    assert http["HTTP_404_ticker"] == "cnyrubf_not_available"
    assert http["HTTP_429"] == "algopack_rate_limit_blocked"
    assert http["HTTP_5XX"] == "algopack_tradestats_not_available"
    assert http["malformed_json_or_schema"] == "algopack_schema_not_stable"
    assert http["response_body_in_blocker_reason_allowed"] is False
    assert auth["required_environment_variable"] == "MOEX_ALGOPACK_TOKEN"
    assert auth["exact_route_validation_before_request"] is True
    assert auth["redirects_allowed"] is False
    assert auth["authorization_on_redirect_allowed"] is False
    assert auth["token_in_url_allowed"] is False
    assert auth["token_in_logs_allowed"] is False
    assert auth["token_in_artifacts_allowed"] is False
    assert auth["token_in_repository_allowed"] is False
    assert auth["token_in_exception_text_allowed"] is False


def test_spot_runtime_is_superseded_and_preserved() -> None:
    supersession = load_contract()["supersession"]

    assert supersession["superseded_source_id"] == "moex_algopack_cnyrub_tom_tradestats_5m"
    assert supersession["superseded_runtime_id"] == (
        "phase8_6a_algopack_cnyrub_source_validation_20260729_v1"
    )
    assert supersession["superseded_runtime_classification"] == (
        "superseded_non_target_source_evidence"
    )
    assert supersession["superseded_artifacts_must_be_preserved"] is True
    assert supersession["superseded_runtime_must_not_be_repeated"] is True
    assert supersession["superseded_result_must_not_enter_phase8_6b"] is True


def test_next_pr_and_runtime_authority_are_separate() -> None:
    contract = load_contract()
    next_scope = contract["implementation_scope_next_pr"]

    assert next_scope["implementation_requires_separate_pr"] is True
    assert next_scope["spot_identity_constants_must_not_be_reused"] is True
    assert next_scope["server_apply_allowed"] is False
    assert next_scope["controlled_runtime_allowed"] is False
    assert next_scope["controlled_runtime_requires_separate_explicit_authority"] is True
    assert contract["gates"]["G2"] == (
        "official CNYRUBF/FO TradeStats route and exact ISS metadata identity are reproducible"
    )
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
    assert "no server apply" in contract["non_authorizations"]
    assert "no controlled runtime" in contract["non_authorizations"]
    assert "no trading action" in contract["non_authorizations"]
