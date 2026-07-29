from __future__ import annotations

import json
from pathlib import Path


CONTRACT_PATH = Path(
    "contracts/experiments/usdrubf_phase8_6a_algopack_cnyrubf_fo_source_correction_v1.json"
)

FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS = [
    "target_phase_label",
    "target_is_labeled",
    "target_source",
    "fold_id",
    "y_true",
    "candidate_y_pred",
    "prediction",
    "probability_B",
    "probability_S",
    "probability_OUT",
]


def load_contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_and_route() -> None:
    contract = load_contract()
    identity = contract["contract_identity"]
    source = contract["source_identity"]

    assert identity["project"] == "MOEX_Bot"
    assert identity["contract_version"] == "1.2"
    assert identity["task_id"] == "ema_3_19_ai_phase_8_6a_cnyrubf_fo_source_correction_v1"
    assert identity["execution_mode"] == "browser_controlled_github_route"
    assert identity["status"] == "source_correction_contract_pending_implementation"
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


def test_official_metadata_identity_is_fail_closed() -> None:
    contract = load_contract()
    source = contract["source_identity"]
    metadata = contract["metadata_identity_policy"]

    assert source["security_metadata_route"] == (
        "https://iss.moex.com/iss/securities/CNYRUBF.json?"
        "iss.meta=off&iss.only=description%2Cboards"
    )
    assert metadata["metadata_route_must_equal"] == source["security_metadata_route"]
    assert metadata["metadata_query_must_equal"] == {
        "iss.meta": "off",
        "iss.only": "description,boards",
    }
    assert metadata["required_blocks"] == ["description", "boards"]
    assert metadata["required_description_columns"] == ["name", "value"]
    assert metadata["required_description_value_keys"] == ["SECID"]
    assert "SECID" not in metadata["required_description_columns"]
    assert metadata["secid_must_equal"] == "CNYRUBF"
    assert metadata["boardid_must_equal"] == "RFUD"
    assert metadata["engine_must_equal"] == "futures"
    assert metadata["market_must_equal"] == "forts"
    assert metadata["is_primary_must_equal"] == 1
    assert metadata["is_traded_must_equal"] == 1
    assert metadata["exactly_one_matching_board_required"] is True
    assert metadata["metadata_live_probe_required_before_controlled_runtime"] is True
    assert metadata["identity_mismatch_blocker"] == "security_identity_not_reproducible"
    assert metadata["schema_mismatch_blocker"] == "official_schema_not_stable"
    assert metadata["route_mismatch_blocker"] == "provenance_not_sufficient"


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


def test_acceptance_matrix_forbids_target_derived_leakage() -> None:
    contract = load_contract()
    leakage = contract["acceptance_matrix_leakage_policy"]

    assert leakage["artifact"] == "cnyrubf_pit_acceptance_matrix.parquet"
    assert leakage["forbidden_acceptance_matrix_fields"] == (
        FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS
    )
    assert leakage["forbidden_fields_must_be_absent"] is True
    assert leakage["target_labels_allowed"] is False
    assert leakage["predictions_allowed"] is False
    assert leakage["class_probabilities_allowed"] is False
    assert leakage["fold_assignments_allowed"] is False
    assert leakage["failure_blocker"] == "target_derived_field_leakage"
    assert leakage["phase8_6b_entry_allowed_on_failure"] is False
    assert "target-derived fields" in contract["gates"]["G8"]


def test_pagination_http_and_secret_boundaries() -> None:
    contract = load_contract()
    official = contract["official_source_policy"]
    pagination = contract["pagination_policy"]
    outcomes = contract["http_outcome_policy"]
    authorization = contract["authorization_policy"]

    assert official["allowed_hosts"] == ["apim.moex.com", "iss.moex.com"]
    assert official["redirects_allowed"] is False
    assert official["cross_host_redirect_allowed"] is False
    assert official["authorization_on_redirect_allowed"] is False
    assert official["full_pagination_required"] is True
    assert official["cursor_validation_required"] is True
    assert pagination["cursor_index_must_equal_requested_start"] is True
    assert pagination["cursor_total_must_be_constant"] is True
    assert pagination["final_accumulated_count_must_equal_total"] is True
    assert pagination["start_greater_than_total_allowed"] is False
    assert pagination["overlapping_pages_allowed"] is False
    assert pagination["skipped_pages_allowed"] is False
    assert pagination["premature_empty_page_allowed"] is False
    assert outcomes["HTTP_401"] == "algopack_authentication_failed"
    assert outcomes["HTTP_403"] == "algopack_subscription_not_entitled"
    assert outcomes["HTTP_404_ticker"] == "cnyrubf_not_available"
    assert outcomes["response_body_in_blocker_reason_allowed"] is False
    assert authorization["token_in_url_allowed"] is False
    assert authorization["token_in_logs_allowed"] is False
    assert authorization["token_in_artifacts_allowed"] is False
    assert authorization["token_in_repository_allowed"] is False


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
    implementation = contract["implementation_scope_next_pr"]

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
    assert implementation["implementation_requires_separate_pr"] is True
    assert implementation["server_apply_allowed"] is False
    assert implementation["controlled_runtime_allowed"] is False
    assert implementation["controlled_runtime_requires_separate_explicit_authority"] is True
    assert "no server apply" in contract["non_authorizations"]
    assert "no controlled runtime" in contract["non_authorizations"]
    assert "no trading action" in contract["non_authorizations"]
