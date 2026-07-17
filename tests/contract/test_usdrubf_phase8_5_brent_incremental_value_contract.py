from __future__ import annotations

import hashlib
import json
from pathlib import Path

from moex_research.features import brent_incremental_features as features
from moex_research.runners import (
    usdrubf_phase8_3_external_factor_incremental_value_experiment as phase83,
)
from moex_research.runners import usdrubf_phase8_5_brent_incremental_value as runner


CONTRACT_PATH = Path(
    "contracts/experiments/usdrubf_phase8_5_brent_incremental_value_v1.json"
)


def _contract() -> dict[str, object]:
    value = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_exact_project_phase_lane_task_and_execution_identity() -> None:
    identity = _contract()["experiment_identity"]
    assert identity == {
        "contract_id": "usdrubf_phase8_5_brent_incremental_value_v1",
        "contract_version": "1.0",
        "project": "MOEX Bot",
        "phase": "8.5",
        "lane": "ema_3_19_ai",
        "task_id": "ema_3_19_ai_market_phase_phase_8_5_brent_incremental_value_experiment_v1",
        "execution_mode": "browser_chatgpt_github_direct",
        "status": "repository_implementation_only",
    }


def test_exact_approved_branch() -> None:
    assert _contract()["approved_branch"] == (
        "research/ema-3-19-ai/phase-8-5-brent-incremental-value"
    )


def test_exact_six_create_only_files_and_zero_existing_file_modifications() -> None:
    scope = _contract()["approved_file_scope"]
    assert tuple(scope["create_only"]) == runner.APPROVED_FILES
    assert scope["exact_file_count"] == 6
    assert scope["maximum_changed_file_count"] == 6
    assert scope["existing_files_to_modify"] == []
    assert scope["scope_widening_allowed"] is False


def test_exact_thirteen_immutable_hashes() -> None:
    hashes = _contract()["upstream_sha256"]
    assert hashes == runner.FROZEN_INPUT_SHA256
    assert len(hashes) == 13
    assert all(len(value) == 64 for value in hashes.values())


def test_exact_accepted_phase84a_source_identity() -> None:
    accepted = _contract()["accepted_phase8_4a_input"]
    assert accepted["run_id"] == runner.ACCEPTED_PHASE84A_RUN_ID
    assert accepted["source_git_commit_sha"] == runner.EXPECTED_SOURCE_COMMIT
    assert accepted["source_id"] == "moex_brent_futures_daily"
    assert accepted["status"] == "candidate_for_phase8_5"
    assert accepted["eligible_identities"] == 472
    assert accepted["validation_identities"] == 320
    assert accepted["runtime_must_not_be_repeated"] is True


def test_exact_six_Brent_features_and_formulas() -> None:
    contract = _contract()
    assert tuple(contract["feature_restrictions"]["authorized_features"]) == (
        features.BRENT_FEATURES
    )
    assert contract["feature_restrictions"]["exact_feature_count"] == 6
    assert contract["brent_feature_definitions"] == runner.BRENT_FEATURE_DEFINITIONS


def test_exact_five_matrix_inventory_and_E1_is_sole_candidate() -> None:
    inventory = _contract()["matrix_inventory"]
    assert tuple(inventory) == (
        "E0_FROZEN_PHASE7_2_CONTROL",
        "E1_M0_PLUS_BRENT_FULL",
        "E2_M0_PLUS_BRENT_PRICE_ACTION",
        "E3_M0_PLUS_BRENT_ACTIVITY",
        "E4_BRENT_ONLY",
    )
    assert [name for name, row in inventory.items() if row["acceptance_eligible"]] == [
        "E1_M0_PLUS_BRENT_FULL"
    ]
    assert inventory["E0_FROZEN_PHASE7_2_CONTROL"]["refit"] is False


def test_exact_phase83_protocol_and_thresholds_are_reused_unchanged() -> None:
    contract = _contract()
    protocol = contract["frozen_protocol"]
    assert protocol["reused_from_phase"] == "8.3_unchanged"
    assert protocol["splitter"] == {
        "type": "sklearn.model_selection.TimeSeriesSplit",
        **phase83.SPLITTER_CONSTRUCTOR,
    }
    assert protocol["estimator"] == {
        "type": "sklearn.linear_model.LogisticRegression",
        **phase83.MODEL_CONSTRUCTOR,
    }
    assert contract["absolute_limits"] == {
        name: {"comparator": comparator, "threshold": threshold}
        for name, (comparator, threshold) in phase83.ABSOLUTE_LIMITS.items()
    }
    assert contract["incremental_gates"]["reused_from_phase8_3_without_relaxation"] is True


def test_exact_twelve_declared_runtime_artifacts() -> None:
    contract = _contract()
    assert tuple(contract["declared_output_artifacts"]) == runner.DECLARED_OUTPUT_ARTIFACTS
    assert contract["artifact_policy"]["exact_count"] == 12
    assert contract["artifact_policy"]["undeclared_artifact_allowed"] is False


def test_no_ruonia_key_rate_or_other_phase83_external_feature_reuse() -> None:
    contract = _contract()
    feature_names = tuple(contract["brent_feature_definitions"])
    matrix_text = json.dumps(contract["matrix_inventory"]).lower()
    assert not any("ruonia" in name or "key_rate" in name for name in feature_names)
    assert "ruonia" not in matrix_text
    assert "key_rate" not in matrix_text
    assert contract["feature_restrictions"]["ruonia_or_key_rate_feature_allowed"] is False
    assert contract["feature_restrictions"]["phase8_3_external_feature_allowed"] is False


def test_no_network_or_source_regeneration_authority() -> None:
    source = _contract()["source_acceptance_requirements"]
    assert source["network_access_allowed"] is False
    assert source["source_artifact_regeneration_allowed"] is False
    assert source["source_artifact_modification_allowed"] is False


def test_no_runtime_model_fit_evaluation_serialization_or_promotion_authority() -> None:
    authority = _contract()["authority_boundary"]
    for name in (
        "server_apply_allowed",
        "real_runtime_allowed",
        "real_model_fit_allowed",
        "real_model_evaluation_allowed",
        "model_serialization_allowed",
        "model_promotion_allowed",
        "strategy_promotion_allowed",
        "live_prediction_allowed",
        "broker_action_allowed",
        "trading_allowed",
    ):
        assert authority[name] is False


def test_final_statuses_and_diagnostics_cannot_authorize_acceptance() -> None:
    policy = _contract()["final_status"]
    assert policy["passed"] == "brent_incremental_value_supported"
    assert policy["failed"] == "brent_incremental_value_not_supported"
    assert policy["diagnostic_matrices_can_authorize_acceptance"] is False
    assert policy["promotion_authorized"] is False


def test_runner_validates_the_frozen_contract_and_exact_hash() -> None:
    runner.validate_experiment_contract(_contract())
    observed = hashlib.sha256(CONTRACT_PATH.read_bytes()).hexdigest()
    assert observed == runner.EXPECTED_EXPERIMENT_CONTRACT_SHA256
