from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    ROOT
    / "contracts/experiments/usdrubf_phase8_2_external_data_pit_acceptance_matrix_v1.json"
)
APPROVED_FILES = [
    "contracts/experiments/usdrubf_phase8_2_external_data_pit_acceptance_matrix_v1.json",
    "src/moex_research/external_data/pit_alignment.py",
    "src/moex_research/runners/usdrubf_phase8_2_external_data_pit_acceptance_matrix.py",
    "tests/unit/test_usdrubf_phase8_2_external_data_pit_alignment.py",
    "tests/unit/test_usdrubf_phase8_2_external_data_pit_acceptance_matrix.py",
    "tests/contract/test_usdrubf_phase8_2_external_data_pit_acceptance_matrix_contract.py",
]
ACCEPTED = ["cbr_ruonia_daily", "cbr_key_rate_daily"]
BLOCKED = {
    "moex_brent_futures_daily": "blocked_pending_source_validation",
    "cme_wti_pre_moex": "blocked_pending_license",
    "cbr_banking_liquidity_daily": "blocked_pending_vintage_policy",
    "ine_shanghai_crude_pre_moex": "blocked_pending_historical_intraday_source",
}
ARTIFACTS = [
    "input_identity_verification.json",
    "source_fetch_manifest.json",
    "ruonia_normalized.parquet",
    "key_rate_normalized.parquet",
    "external_pit_acceptance_matrix.parquet",
    "coverage_by_source.csv",
    "staleness_by_source.csv",
    "source_blocker_register.json",
    "gate_results.json",
]


def _contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_exact_identity_branch_and_create_only_scope() -> None:
    contract = _contract()
    assert contract["contract_identity"] == {
        "contract_id": "usdrubf_phase8_2_external_data_pit_acceptance_matrix_v1",
        "contract_version": "1.0",
        "project": "MOEX Bot",
        "task_id": "ema_3_19_ai_market_phase_phase_8_2_external_data_pit_acceptance_matrix_v1",
        "lane": "ema_3_19_ai",
        "phase": "8.2",
        "execution_mode": "browser_chatgpt_github_direct",
        "status": "controlled_runtime_contract",
    }
    assert contract["approved_branch"] == (
        "research/ema-3-19-ai/phase-8-2-external-data-pit-acceptance-matrix"
    )
    scope = contract["approved_file_scope"]
    assert scope["create_only"] == APPROVED_FILES
    assert scope["existing_files_to_modify"] == []
    assert scope["exact_file_count"] == scope["maximum_changed_file_count"] == 6
    assert all((ROOT / path).is_file() for path in APPROVED_FILES)


def test_exact_sources_cutoff_and_point_in_time_policies() -> None:
    contract = _contract()
    assert contract["accepted_sources"] == ACCEPTED
    assert contract["blocked_sources"] == BLOCKED
    policy = contract["availability_policy"]
    assert policy["timezone"] == "Europe/Moscow"
    assert policy["decision_cutoff_local_time"] == "08:45:00"
    assert policy["ruonia_eligibility"] == "publication_date < target_trade_date"
    assert policy["ruonia_same_day_publication_allowed"] is False
    assert policy["key_rate_eligibility"] == "effective_date <= target_trade_date"
    assert policy["ruonia_interpolation_allowed"] is False
    assert policy["key_rate_retrospective_application_allowed"] is False


def test_exact_artifacts_gates_history_buffer_and_manifest_fields() -> None:
    contract = _contract()
    assert contract["runtime_artifacts"] == ARTIFACTS
    assert contract["artifact_policy"]["exact_count"] == 9
    assert contract["artifact_policy"]["undeclared_artifact_allowed"] is False
    assert contract["historical_request_range"] == {
        "derived_from": "minimum and maximum exact eligible target_trade_date",
        "history_buffer_calendar_days": 31,
        "hidden_dynamic_widening_allowed": False,
        "missing_first_row_policy": "fail_closed",
    }
    assert list(contract["gates"]) == [
        "G1_input_identity",
        "G2_exact_identity_preservation",
        "G3_ruonia_point_in_time_correctness",
        "G4_key_rate_point_in_time_correctness",
        "G5_coverage",
        "G6_blocked_source_exclusion",
        "G7_provenance",
        "G8_leakage_and_scope",
        "G9_final_acceptance",
    ]
    for required in (
        "source_id",
        "exact_requested_route",
        "retrieved_at_utc",
        "raw_payload_sha256",
        "normalized_row_count",
        "first_business_date",
        "last_business_date",
    ):
        assert required in contract["source_fetch_manifest_required_fields"]


def test_identity_matrix_and_staleness_contracts_are_exact() -> None:
    contract = _contract()
    identity = contract["identity_contract"]
    assert identity["eligible_identity_count"] == 472
    assert identity["validation_identity_count"] == 320
    assert identity["instrument"] == "forts.usdrubf"
    assert contract["diagnostic_only_fields"] == ["ruonia_minus_key_rate_pp"]
    assert "target_phase_label" not in contract["matrix_columns"]
    assert "probability_B" not in contract["matrix_columns"]
    staleness = contract["staleness_policy"]
    assert staleness["ruonia_age_buckets"] == [
        "1 calendar day",
        "2-3 calendar days",
        "4-7 calendar days",
        "more than 7 calendar days",
    ]
    assert staleness["key_rate_age_buckets"] == [
        "0-30 calendar days",
        "31-90 calendar days",
        "91-180 calendar days",
        "more than 180 calendar days",
    ]
    assert staleness["acceptance_threshold_applied"] is False


def test_no_model_promotion_broker_trading_or_real_runtime_authority() -> None:
    contract = _contract()
    assert not any(contract["fixed_research_components"].values())
    authority = contract["authority_boundary"]
    for field in (
        "server_apply_allowed",
        "real_external_data_acquisition_during_repository_implementation_allowed",
        "model_fit_allowed",
        "model_evaluation_allowed",
        "model_promotion_allowed",
        "strategy_promotion_allowed",
        "live_prediction_allowed",
        "broker_action_allowed",
        "trading_allowed",
    ):
        assert authority[field] is False


def test_no_server_path_runtime_artifact_secret_or_dependency_change_in_scope() -> None:
    combined = "\n".join(
        (ROOT / path).read_text(encoding="utf-8")
        for path in APPROVED_FILES
        if Path(path).suffix in {".json", ".py"}
        and not path.startswith("tests/")
    )
    assert "/home/trader" not in combined
    assert "moex_robot" not in combined
    assert not any(Path(path).suffix in {".csv", ".parquet", ".pkl", ".joblib"} for path in APPROVED_FILES)
    assert "api_key=" not in combined.lower()
    assert "authorization: bearer" not in combined.lower()
    assert not any(path in APPROVED_FILES for path in ("requirements.txt", "pyproject.toml"))
