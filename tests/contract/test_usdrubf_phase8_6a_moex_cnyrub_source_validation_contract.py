from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).parents[2]
CONTRACT_PATH = (
    ROOT
    / "contracts/experiments/usdrubf_phase8_6a_moex_cnyrub_source_validation_v1.json"
)
CONTRACT = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
APPROVED_FILES = [
    "contracts/experiments/usdrubf_phase8_6a_moex_cnyrub_source_validation_v1.json",
    "src/moex_research/external_data/moex_cnyrub_history.py",
    "src/moex_research/runners/usdrubf_phase8_6a_moex_cnyrub_source_validation.py",
    "tests/unit/test_usdrubf_phase8_6a_moex_cnyrub_history.py",
    "tests/unit/test_usdrubf_phase8_6a_moex_cnyrub_source_validation.py",
    "tests/contract/test_usdrubf_phase8_6a_moex_cnyrub_source_validation_contract.py",
]
ARTIFACTS = [
    "input_identity_verification.json",
    "official_route_validation.json",
    "cnyrub_security_identity.json",
    "cnyrub_daily_candles_normalized.parquet",
    "cnyrub_pit_acceptance_matrix.parquet",
    "coverage_by_source.csv",
    "session_alignment_diagnostics.csv",
    "source_blocker_register.json",
    "gate_results.json",
]


def test_contract_identity_task_lane_phase_mode_and_branch() -> None:
    assert CONTRACT["contract_identity"] == {
        "contract_id": "usdrubf_phase8_6a_moex_cnyrub_source_validation_v1",
        "contract_version": "1.0",
        "project": "MOEX Bot",
        "task_id": "ema_3_19_ai_phase_8_6a_moex_cnyrub_source_validation_v1",
        "lane": "ema_3_19_ai",
        "phase": "8.6A",
        "execution_mode": "browser_chatgpt_github_direct",
        "status": "source_validation_only",
    }
    assert CONTRACT["approved_branch"] == (
        "research/ema-3-19-ai/phase-8-6a-moex-cnyrub-source-validation"
    )


def test_exact_six_file_create_only_scope() -> None:
    scope = CONTRACT["approved_file_scope"]
    assert scope["create_only"] == APPROVED_FILES
    assert scope["existing_files_to_modify"] == []
    assert scope["exact_file_count"] == scope["maximum_changed_file_count"] == 6
    assert scope["scope_widening_allowed"] is False


def test_source_identity_and_no_fallback_policy() -> None:
    assert CONTRACT["source_identity"] | {} == CONTRACT["source_identity"]
    assert CONTRACT["source_identity"]["security_id"] == "CNYRUB_TOM"
    assert CONTRACT["source_identity"]["board_id"] == "CETS"
    assert CONTRACT["source_identity"]["engine"] == "currency"
    assert CONTRACT["source_identity"]["market"] == "selt"
    policy = CONTRACT["official_source_policy"]
    assert policy["allowed_hosts"] == ["iss.moex.com"]
    assert policy["bank_of_russia_rate_allowed"] is False
    assert policy["cnyrubf_allowed"] is False
    assert policy["synthetic_cross_allowed"] is False
    assert policy["substitute_security_or_board_allowed"] is False


def test_pit_policy_is_exact_prior_date_without_fill() -> None:
    policy = CONTRACT["point_in_time_policy"]
    assert policy["forecast_anchor_local_time"] == "06:00:00"
    assert policy["cnyrub_trade_date_rule"] == "cnyrub_trade_date == prior_trade_date"
    for key in (
        "target_day_candle_allowed",
        "same_day_close_volume_or_value_allowed",
        "later_observation_allowed",
        "forward_fill_allowed",
        "backward_fill_allowed",
        "arbitrary_last_available_date_allowed",
        "future_session_missing_fill_allowed",
        "zero_fill_allowed",
    ):
        assert policy[key] is False


def test_exact_source_and_acceptance_schemas() -> None:
    assert CONTRACT["normalized_source_required_fields"] == [
        "source_id",
        "security_id",
        "board_id",
        "engine",
        "market",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "candle_begin",
        "candle_end",
        "source_route",
        "retrieved_at_utc",
        "raw_payload_sha256",
        "source_revision_status",
        "historical_model_use_status",
    ]
    assert CONTRACT["acceptance_matrix_fields"] == [
        "target_trade_date",
        "target_instrument_id",
        "prior_trade_date",
        "cnyrub_security_id",
        "cnyrub_board_id",
        "cnyrub_trade_date",
        "cnyrub_open",
        "cnyrub_high",
        "cnyrub_low",
        "cnyrub_close",
        "cnyrub_volume",
        "cnyrub_value",
        "cnyrub_candle_begin",
        "cnyrub_candle_end",
        "cnyrub_source_route",
        "cnyrub_payload_sha256",
        "cnyrub_retrieved_at_utc",
    ]
    assert set(CONTRACT["acceptance_matrix_fields"]).isdisjoint(
        CONTRACT["forbidden_acceptance_matrix_fields"]
    )


def test_exact_nine_artifact_inventory_and_output_policy() -> None:
    assert CONTRACT["runtime_artifacts"] == ARTIFACTS
    policy = CONTRACT["artifact_policy"]
    assert policy["exact_count"] == 9
    assert policy["preexisting_output_directory_allowed"] is False
    assert policy["output_outside_explicit_output_directory_allowed"] is False
    assert policy["model_file_allowed"] is False


def test_g1_through_g9_and_blocker_classes_are_exact() -> None:
    assert list(CONTRACT["gates"]) == [
        "G1_immutable_inputs",
        "G2_official_security_identity",
        "G3_official_candle_route_and_schema",
        "G4_point_in_time_session_correctness",
        "G5_exact_coverage",
        "G6_numerical_and_chronology_integrity",
        "G7_provenance",
        "G8_leakage_and_scope",
        "G9_final_source_readiness",
    ]
    assert CONTRACT["blocker_classifications"] == [
        "security_identity_not_reproducible",
        "official_daily_candles_not_available",
        "point_in_time_cutoff_not_provable",
        "incomplete_identity_coverage",
        "official_schema_not_stable",
        "numerical_or_chronology_integrity_failure",
        "provenance_not_sufficient",
        "other_fail_closed_with_exact_reason",
    ]


def test_runtime_id_format_and_planned_runtime_id() -> None:
    policy = CONTRACT["runtime_id_policy"]
    assert policy["planned_runtime_id"] == (
        "phase8_6a_moex_cnyrub_source_validation_20260717_v1"
    )
    assert re.fullmatch(policy["format_regex"], policy["planned_runtime_id"])


def test_no_model_promotion_runtime_server_or_trading_authority() -> None:
    boundary = CONTRACT["authority_boundary"]
    for key in (
        "direct_main_write_allowed",
        "merge_allowed",
        "server_apply_allowed",
        "controlled_runtime_allowed",
        "model_fit_allowed",
        "model_evaluation_allowed",
        "model_serialization_allowed",
        "model_promotion_allowed",
        "strategy_promotion_allowed",
        "production_prediction_allowed",
        "broker_action_allowed",
        "trading_allowed",
    ):
        assert boundary[key] is False
