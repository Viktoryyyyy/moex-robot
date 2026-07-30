from __future__ import annotations

import json
from pathlib import Path


CONTRACT_PATH = Path(
    "contracts/experiments/"
    "usdrubf_phase8_7a_futoi_si_source_and_feature_contract_v1.json"
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


def test_contract_identity_and_canonical_futoi_si_source() -> None:
    contract = load_contract()
    identity = contract["contract_identity"]
    owner = contract["owner_decision"]
    source = contract["source_identity"]

    assert identity["project"] == "MOEX_Bot"
    assert identity["contract_version"] == "1.3"
    assert identity["phase"] == "8.7A"
    assert identity["task_id"] == (
        "ema_3_19_ai_phase_8_7a_futoi_si_source_contract_v1"
    )
    assert identity["execution_mode"] == "browser_controlled_github_route"
    assert owner["target_security_id"] == "USDRUBF"
    assert owner["approved_source_ticker"] == "Si"
    assert owner["participant_groups"] == ["FIZ", "YUR"]
    assert owner["open_interest_interpretation_allowed"] is False
    assert owner["usdrubf_exact_contract_endpoint_allowed"] is False
    assert source["exact_path"] == (
        "/iss/analyticalproducts/futoi/securities/si.json"
    )
    assert source["source_ticker"] == "Si"
    assert source["approved_normalized_source_scope"] == "family_aggregate_futoi"
    assert source["family_aggregate_is_approved_source_not_fallback"] is True
    assert source["exact_contract_futoi_scope_allowed"] is False


def test_contract_points_to_implemented_futoi_dataset() -> None:
    contract = load_contract()
    components = contract["existing_canonical_components"]
    implementation = contract["implementation_scope_next_pr"]

    assert components["availability_orchestration_reference"] == (
        "moex_data.futures.all_universe_futoi_raw_backfill_slice"
    )
    assert components["orchestration_reuse_required"] is False
    assert components["normalizer_module"] == (
        "moex_data.futures.futoi_raw_loader"
    )
    assert components["transport_module"] == (
        "moex_data.futures.liquidity_history_metrics_probe"
    )
    assert components["implemented_dataset_contract"] == (
        "contracts/datasets/futures_futoi_5m_raw_contract.md"
    )
    assert components["implemented_schema_version"] == "futures_futoi_5m_raw.v1"
    assert components["normalizer_and_transport_reuse_required"] is True
    assert components["duplicate_source_client_allowed"] is False
    assert components["required_source_selection"] == "explicit_source_ticker_si"
    assert components["source_ticker_required"] == "Si"
    assert components["canonical_storage_secid"] == "USDRUBF"
    assert components["canonical_storage_family_code"] == "USDRUBF"
    assert components["source_ticker_must_not_be_used_as_storage_family"] is True
    assert components["secid_and_family_filter_intersection_for_si_allowed"] is False
    assert implementation[
        "must_reuse_existing_normalizer_and_transport_primitives"
    ] is True
    assert implementation["explicit_si_source_selection_required"] is True
    assert implementation["all_universe_orchestrator_reuse_required"] is False
    assert implementation["canonical_storage_secid"] == "USDRUBF"
    assert implementation["canonical_storage_family_code"] == "USDRUBF"
    assert implementation["source_ticker_required"] == "Si"
    assert implementation["duplicate_http_transport_allowed"] is False


def test_limited_probe_does_not_prove_historical_readiness() -> None:
    evidence = load_contract()["limited_probe_evidence"]

    assert evidence["route"] == (
        "https://apim.moex.com/iss/analyticalproducts/futoi/"
        "securities/si.json"
    )
    assert evidence["query_policy"] == (
        "from=trade_date&till=trade_date&latest=1"
    )
    assert evidence["source_requests"] == 27
    assert evidence["raw_rows"] == 54
    assert evidence["daily_rows"] == 27
    assert evidence["trade_dates"] == 27
    assert evidence["participant_groups"] == ["FIZ", "YUR"]
    assert evidence["historical_coverage_proven"] is False
    assert evidence["pit_correctness_proven"] is False
    assert evidence["model_use_authorized"] is False


def test_raw_schema_and_pairing_use_seqnum_as_row_provenance_only() -> None:
    contract = load_contract()
    schema = contract["raw_schema_policy"]
    pairing = contract["daily_pairing_policy"]

    assert schema["required_participant_groups"] == ["FIZ", "YUR"]
    assert schema["ticker_must_equal"] == "Si"
    assert schema["net_identity"] == "pos == pos_long + pos_short"
    assert schema["cross_group_zero_sum_identity"] == "FIZ.pos + YUR.pos == 0"
    assert schema["seqnum_semantics"] == (
        "provider row sequence identifier retained separately for provenance"
    )
    assert schema["cross_group_seqnum_equality_required"] is False
    assert pairing["candidate_pair_key"] == [
        "trade_date",
        "moment",
        "sess_id",
    ]
    assert pairing["group_row_identity"] == [
        "trade_date",
        "moment",
        "sess_id",
        "clgroup",
        "seqnum",
    ]
    assert pairing["latest_common_pair_only"] is True
    assert pairing["fiz_yur_moment_mismatch_allowed"] is False
    assert pairing["fiz_yur_session_mismatch_allowed"] is False
    assert pairing["fiz_yur_seqnum_mismatch_allowed"] is True
    assert pairing["seqnum_cross_group_join_allowed"] is False
    assert pairing["seqnum_storage_rule"] == (
        "retain separate fiz_seqnum and yur_seqnum in normalized evidence"
    )
    assert pairing["independent_latest_row_per_group_allowed"] is False
    assert pairing["forward_fill_allowed"] is False
    assert pairing["backward_fill_allowed"] is False
    assert pairing["nearest_date_substitution_allowed"] is False


def test_pit_and_revision_semantics_are_fail_closed() -> None:
    contract = load_contract()
    pit = contract["pit_and_revision_policy"]
    counts = contract["frozen_identity_counts"]

    assert pit["timezone"] == "Europe/Moscow"
    assert pit["forecast_anchor_local_time"] == "06:00:00"
    assert pit["target_join_date"] == "exact frozen prior_trade_date"
    assert pit["availability_field"] == "systime"
    assert pit["daily_pair_source_available_at"] == "max(FIZ.systime,YUR.systime)"
    assert pit["historical_original_publication_time_required"] is True
    assert pit["current_revision_timestamp_is_not_sufficient"] is True
    assert pit["revision_or_restatement_behavior_must_be_proven"] is True
    assert pit["fail_closed_if_publication_time_not_provable"] is True
    assert pit["same_day_or_future_observation_allowed"] is False
    assert counts["eligible_identity_count"] == 472
    assert counts["validation_identity_count"] == 320


def test_owner_requested_factor_semantics_are_frozen() -> None:
    features = load_contract()["feature_semantics_for_phase8_7b"]
    formulas = features["base_level_formulas"]
    rolling = features["rolling_policy"]
    decomposition = features["within_group_net_change_decomposition"]
    structure = features["participant_change_structure"]
    divergence = features["divergence_formulas"]

    assert features["computation_authorized_in_phase8_7a"] is False
    assert features["model_evaluation_authorized_in_phase8_7a"] is False
    assert formulas["fiz_short_abs"] == "-FIZ.pos_short"
    assert formulas["fiz_gross_position"] == "FIZ.pos_long - FIZ.pos_short"
    assert formulas["yur_short_abs"] == "-YUR.pos_short"
    assert formulas["yur_gross_position"] == "YUR.pos_long - YUR.pos_short"
    assert features["one_day_change_rule"] == (
        "delta_x_1d = x_D - x_previous_accepted_source_trade_date"
    )
    assert "delta_fiz_pos_1d" in features["one_day_changes"]
    assert "delta_yur_pos_1d" in features["one_day_changes"]
    assert rolling["windows"] == [5, 20, 60]
    assert rolling["current_D_included"] is False
    assert rolling["minimum_observations"] == "exactly the full window size"
    assert rolling["partial_window_allowed"] is False
    assert rolling["calendar_fill_allowed"] is False
    assert rolling["standard_deviation"] == (
        "population standard deviation with ddof=0"
    )
    assert decomposition["formula"] == (
        "delta_pos = delta_pos_long - delta_short_abs"
    )
    assert decomposition["opening_long_component"] == "max(delta_pos_long,0)"
    assert decomposition["closing_short_component"] == (
        "max(-delta_short_abs,0)"
    )
    assert structure["fiz_activity_contribution_share"] == (
        "fiz_group_activity / total_group_activity"
    )
    assert structure["yur_activity_contribution_share"] == (
        "yur_group_activity / total_group_activity"
    )
    assert "does not identify trade initiator or causality" in structure[
        "interpretation"
    ]
    assert divergence["fiz_yur_net_divergence"] == "fiz_pos - yur_pos"
    assert divergence["delta_fiz_yur_net_divergence_1d"] == (
        "delta_fiz_pos_1d - delta_yur_pos_1d"
    )


def test_acceptance_matrix_and_authority_boundaries() -> None:
    contract = load_contract()
    leakage = contract["acceptance_matrix_leakage_policy"]
    implementation = contract["implementation_scope_next_pr"]

    assert leakage["artifact"] == "futoi_si_pit_acceptance_matrix.parquet"
    assert leakage["forbidden_acceptance_matrix_fields"] == (
        FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS
    )
    assert leakage["forbidden_fields_must_be_absent"] is True
    assert leakage["target_labels_allowed"] is False
    assert leakage["predictions_allowed"] is False
    assert leakage["class_probabilities_allowed"] is False
    assert leakage["fold_assignments_allowed"] is False
    assert list(contract["gates"]) == [
        "G1",
        "G2",
        "G3",
        "G4",
        "G5",
        "G6",
        "G7",
        "G8",
        "G9",
    ]
    assert implementation["feature_computation_requires_phase8_7a_pass"] is True
    assert implementation[
        "incremental_value_evaluation_requires_separate_phase8_7b"
    ] is True
    assert implementation["server_apply_allowed"] is False
    assert implementation["controlled_runtime_allowed"] is False
    assert "no server apply" in contract["non_authorizations"]
    assert "no controlled runtime" in contract["non_authorizations"]
    assert "no feature computation" in contract["non_authorizations"]
    assert "no trading action" in contract["non_authorizations"]
