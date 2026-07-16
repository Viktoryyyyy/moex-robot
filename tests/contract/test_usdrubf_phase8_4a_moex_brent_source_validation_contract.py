from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    ROOT
    / "contracts/experiments/usdrubf_phase8_4a_moex_brent_source_validation_v1.json"
)
APPROVED_FILES = [
    "contracts/experiments/usdrubf_phase8_4a_moex_brent_source_validation_v1.json",
    "src/moex_research/external_data/moex_brent_history.py",
    "src/moex_research/runners/usdrubf_phase8_4a_moex_brent_source_validation.py",
    "tests/unit/test_usdrubf_phase8_4a_moex_brent_history.py",
    "tests/unit/test_usdrubf_phase8_4a_moex_brent_source_validation.py",
    "tests/contract/test_usdrubf_phase8_4a_moex_brent_source_validation_contract.py",
]
ARTIFACTS = [
    "input_identity_verification.json",
    "official_route_validation.json",
    "brent_contract_universe.parquet",
    "brent_daily_candles_normalized.parquet",
    "brent_pit_acceptance_matrix.parquet",
    "coverage_by_source.csv",
    "contract_roll_diagnostics.csv",
    "source_blocker_register.json",
    "gate_results.json",
]


def _contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_exact_project_phase_lane_task_branch_and_create_only_scope() -> None:
    contract = _contract()
    assert contract["contract_identity"] == {
        "contract_id": "usdrubf_phase8_4a_moex_brent_source_validation_v1",
        "contract_version": "1.0",
        "project": "MOEX Bot",
        "task_id": "ema_3_19_ai_market_phase_phase_8_4a_moex_brent_source_validation_v1",
        "lane": "ema_3_19_ai",
        "phase": "8.4A",
        "execution_mode": "browser_chatgpt_github_direct",
        "status": "source_validation_only",
    }
    assert contract["approved_branch"] == (
        "research/ema-3-19-ai/phase-8-4a-moex-brent-source-validation"
    )
    scope = contract["approved_file_scope"]
    assert scope["create_only"] == APPROVED_FILES
    assert scope["existing_files_to_modify"] == []
    assert scope["exact_file_count"] == scope["maximum_changed_file_count"] == 6
    assert all((ROOT / path).is_file() for path in APPROVED_FILES)


def test_exact_official_source_policy_and_routes() -> None:
    policy = _contract()["official_source_policy"]
    assert policy["allowed_hosts"] == ["iss.moex.com"]
    routes = policy["allowed_routes"]
    assert routes == {
        "historical_contract_enumeration": "https://iss.moex.com/iss/history/engines/futures/markets/forts/boards/RFUD/securities.json?date={prior_trade_date}&assetcode=BR",
        "exact_security_description": "https://iss.moex.com/iss/securities/{officially_enumerated_contract_code}.json",
        "explicit_contract_daily_candles": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/{officially_enumerated_contract_code}/candles.json",
    }
    for field in (
        "third_party_market_data_allowed",
        "manual_contract_calendar_allowed",
        "inferred_or_guessed_contract_code_allowed",
        "generated_month_code_allowed",
        "continuous_alias_allowed",
        "synthetic_continuous_ticker_allowed",
        "current_active_contract_list_as_historical_proof_allowed",
        "later_contract_fill_allowed",
    ):
        assert policy[field] is False


def test_exact_frozen_selection_and_PIT_cutoff_policy() -> None:
    contract = _contract()
    selection = contract["contract_selection_rule"]
    assert selection["minimum_days_to_expiration"] == 7
    assert selection["universe_as_of"] == "frozen prior_trade_date"
    assert selection["same_day_or_future_volume_allowed"] is False
    assert selection["same_day_or_future_open_interest_allowed"] is False
    assert selection["volume_based_roll_allowed"] is False
    assert selection["open_interest_based_roll_allowed"] is False
    assert selection["silent_fallback_to_next_contract_allowed"] is False
    assert selection["cross_contract_return_allowed"] is False
    pit = contract["point_in_time_policy"]
    assert pit == {
        "timezone": "Europe/Moscow",
        "decision_cutoff_local_time": "08:45:00",
        "candle_trade_date": "exact frozen prior_trade_date",
        "candle_end_rule": "candle_end < D 08:45:00 Europe/Moscow",
        "target_day_candle_allowed": False,
        "later_candle_allowed": False,
        "missing_prior_session_candle_policy": "fail_closed",
    }


def test_exact_nine_artifacts_gates_and_blockers() -> None:
    contract = _contract()
    assert contract["runtime_artifacts"] == ARTIFACTS
    assert contract["artifact_policy"] == {
        "exact_count": 9,
        "undeclared_artifact_allowed": False,
        "output_outside_explicit_output_directory_allowed": False,
        "preexisting_output_directory_allowed": False,
        "runtime_artifact_commit_allowed": False,
        "model_file_allowed": False,
    }
    assert list(contract["gates"]) == [
        "G1_immutable_inputs",
        "G2_official_expired_contract_universe",
        "G3_explicit_contract_selection",
        "G4_point_in_time_candle_correctness",
        "G5_exact_coverage",
        "G6_roll_integrity",
        "G7_provenance",
        "G8_leakage_and_scope",
        "G9_final_source_readiness",
    ]
    assert contract["blocker_classifications"] == [
        "expired_contract_universe_not_reproducible",
        "expired_contract_candles_not_available",
        "point_in_time_cutoff_not_provable",
        "incomplete_identity_coverage",
        "official_schema_not_stable",
        "provenance_not_sufficient",
        "other_fail_closed_with_exact_reason",
    ]


def test_no_model_feature_evaluation_promotion_server_or_trading_authority() -> None:
    authority = _contract()["authority_boundary"]
    assert authority["conditional_merge_authorized"] is True
    for field in (
        "direct_main_write_allowed",
        "server_apply_allowed",
        "real_runtime_allowed",
        "model_fit_allowed",
        "model_evaluation_allowed",
        "model_serialization_allowed",
        "model_promotion_allowed",
        "strategy_promotion_allowed",
        "live_prediction_allowed",
        "broker_action_allowed",
        "trading_allowed",
    ):
        assert authority[field] is False


def test_no_server_path_dependency_change_or_runtime_artifact_in_scope() -> None:
    implementation = "\n".join(
        (ROOT / path).read_text(encoding="utf-8")
        for path in APPROVED_FILES
        if Path(path).suffix in {".json", ".py"} and not path.startswith("tests/")
    )
    assert "/home/trader" not in implementation
    assert "moex_robot" not in implementation
    assert "requirements.txt" not in APPROVED_FILES
    assert "pyproject.toml" not in APPROVED_FILES
    assert not any(Path(path).suffix in {".csv", ".parquet", ".pkl", ".joblib"} for path in APPROVED_FILES)


def test_exact_production_CLI_excludes_retrieval_timestamp_override() -> None:
    assert _contract()["required_cli_args"] == [
        "--modeling-dataset-path",
        "--dataset-manifest-path",
        "--feature-schema-path",
        "--m0-validation-predictions-path",
        "--phase8-3-aggregate-metrics-path",
        "--phase8-3-gate-results-path",
        "--phase8-1-source-contract-path",
        "--experiment-contract-path",
        "--output-dir",
        "--run-id",
        "--git-commit-sha",
    ]
    assert "--retrieved-at-utc" not in _contract()["required_cli_args"]


def test_per_payload_post_fetch_timestamp_policy_is_fail_closed() -> None:
    policy = _contract()["retrieval_timestamp_policy"]
    assert policy == {
        "production_cli_override_allowed": False,
        "production_clock": "datetime.now(timezone.utc).replace(microsecond=0)",
        "capture_order": [
            "build_and_validate_official_route",
            "call_injected_transport",
            "call_injected_UTC_clock_immediately_after_transport_returns",
            "validate_timezone_aware_UTC",
            "assign_timestamp_to_exact_returned_payload",
        ],
        "timestamp_scope": "one timestamp per exact response payload",
        "history_page_timestamp_reuse_allowed": False,
        "metadata_timestamp_reuse_as_candle_timestamp_allowed": False,
        "run_level_timestamp_reuse_allowed": False,
        "payload_content_timestamp_derivation_allowed": False,
        "post_artifact_timestamp_modification_allowed": False,
        "naive_datetime_allowed": False,
        "non_UTC_datetime_allowed": False,
        "synthetic_injected_clock_allowed": True,
        "synthetic_injected_clock_scope": "tests_only",
        "equal_numeric_timestamps_across_requests_allowed": True,
    }
    required = _contract()["normalized_contract_required_fields"]
    assert "enumeration_retrieved_at_utc" in required
    assert "metadata_retrieved_at_utc" in required
    assert "retrieved_at_utc" in _contract()["normalized_candle_required_fields"]
    assert "post-fetch UTC clock timestamp" in _contract()["gates"]["G7_provenance"]


def test_bounded_transient_http_retry_policy_is_exact_and_not_a_fallback() -> None:
    policy = _contract()["transient_http_retry_policy"]
    assert policy == {
        "bounded_transient_retry_enabled": True,
        "enabled_for_source_id": "moex_brent_futures_daily",
        "phase_scope": "8.4A_only",
        "maximum_total_attempts": 5,
        "retry_delays_seconds": [0.5, 1.0, 2.0, 4.0],
        "random_jitter_allowed": False,
        "same_exact_official_route_only": True,
        "route_substitution_allowed": False,
        "fallback_source_allowed": False,
        "fallback_contract_allowed": False,
        "fallback_date_allowed": False,
        "retryable_exception": "ExternalDataError(external-data request failed)",
        "semantic_failures_retried": False,
        "failed_attempts_enter_provenance": False,
        "successful_payload_timestamp_policy": (
            "one exact post-fetch UTC timestamp after successful transport return"
        ),
        "source_acceptance_still_requires": [
            "G1",
            "G2",
            "G3",
            "G4",
            "G5",
            "G6",
            "G7",
            "G8",
            "G9",
        ],
        "retry_constitutes_source_fallback": False,
    }
