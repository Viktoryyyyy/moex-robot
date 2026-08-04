from __future__ import annotations

import json
from pathlib import Path

from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_experimental_runtime as runtime,
)


def test_experimental_runtime_authority_contract() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / runtime.AUTHORITY_REPO_PATH
    contract = json.loads(path.read_text(encoding="utf-8"))

    identity = contract["contract_identity"]
    authority = contract["authority"]
    gate_policy = contract["gate_policy"]
    runtime_policy = contract["runtime_policy"]
    single_execution = contract["single_execution"]

    assert identity["project"] == "MOEX_Bot"
    assert identity["task_id"] == runtime.TASK_ID
    assert identity["contract_id"] == runtime.AUTHORITY_CONTRACT_ID
    assert identity["contract_version"] == runtime.AUTHORITY_CONTRACT_VERSION
    assert authority["mode"] == runtime.AUTHORITY_MODE
    assert authority["authority_owner"] == "PM_L2_PHASE_OWNER"
    assert authority["historical_authenticated_retrieval_allowed"] is True
    assert authority["local_research_artifact_storage_allowed"] is True
    assert authority["phase8_7a_source_validation_allowed"] is True

    forbidden_true = (
        "phase8_7b_feature_computation_allowed",
        "model_fitting_allowed",
        "production_prediction_allowed",
        "model_or_strategy_promotion_allowed",
        "raw_payload_redistribution_allowed",
        "broker_action_allowed",
        "trading_action_allowed",
    )
    assert all(authority[name] is False for name in forbidden_true)
    assert tuple(
        gate_policy["experimental_dataset_status_requires_technical_gates"]
    ) == runtime.TECHNICAL_GATES
    assert gate_policy["experimental_dataset_status"] == runtime.EXPERIMENTAL_STATUS
    assert gate_policy["g3_or_g5_must_not_be_forced_to_pass"] is True
    assert runtime_policy["output_artifact_count"] == 10
    assert runtime_policy["fallback_or_substitution_allowed"] is False
    assert runtime_policy["raw_response_persistence_allowed"] is False

    assert single_execution["authorized_run_id"] == runtime.AUTHORIZED_RUN_ID
    assert single_execution["authority_reuse_allowed"] is False
    assert single_execution["authority_file_repo_path"] == runtime.AUTHORITY_REPO_PATH
    assert single_execution["authorized_data_root"] == (
        runtime.AUTHORIZED_DATA_ROOT.as_posix()
    )
    assert (
        single_execution["applied_repo_head_must_equal_git_commit_sha"]
        is True
    )
    assert (
        single_execution["applied_commit_must_track_exact_authority_blob"]
        is True
    )
    assert single_execution["canonical_output_relative_path"] == (
        runtime.CANONICAL_OUTPUT_RELATIVE.as_posix()
    )
    assert single_execution["consumption_marker_relative_path"] == (
        runtime.CONSUMPTION_MARKER_RELATIVE.as_posix()
    )
    assert single_execution["consumption_marker_create_mode"] == (
        "atomic_create_exclusive"
    )
