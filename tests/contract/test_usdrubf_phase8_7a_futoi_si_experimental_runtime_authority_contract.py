from __future__ import annotations

import json
from pathlib import Path

from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_experimental_runtime as runtime,
)


def test_experimental_runtime_policy_contract() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / runtime.POLICY_REPO_PATH
    contract = json.loads(path.read_text(encoding="utf-8"))

    identity = contract["contract_identity"]
    boundary = contract["policy_boundary"]
    authority = contract["authority_boundaries"]
    gate_policy = contract["gate_policy"]
    runtime_policy = contract["runtime_policy"]

    assert identity["project"] == "MOEX_Bot"
    assert identity["task_id"] == runtime.TASK_ID
    assert identity["contract_id"] == runtime.POLICY_CONTRACT_ID
    assert identity["contract_version"] == runtime.POLICY_CONTRACT_VERSION
    assert identity["status"] == "experimental_runtime_policy_active"

    assert boundary["checked_in_policy_is_runtime_authority"] is False
    assert boundary["separate_runtime_authority_evidence_required"] is True
    assert boundary["required_runtime_authority_flag"] == (
        runtime.RUNTIME_AUTHORITY_FLAG
    )
    assert boundary["runtime_authority_must_not_be_stored_in_repository"] is True
    assert boundary["runtime_authority_must_bind_exact_git_sha"] is True
    assert boundary["runtime_authority_must_bind_exact_run_id"] is True
    assert boundary["runtime_authority_must_bind_exact_output_dir"] is True
    assert boundary["canonical_data_root"] == (
        runtime.AUTHORIZED_DATA_ROOT.as_posix()
    )
    assert boundary["module_claims_global_single_use"] is False
    assert (
        boundary["one_run_per_operational_authority_is_orchestration_responsibility"]
        is True
    )

    assert authority["mode"] == runtime.AUTHORITY_MODE
    assert authority["approved_by"] == "PM_L2_PHASE_OWNER"
    assert authority["historical_authenticated_retrieval_allowed"] is True
    assert authority["phase8_7a_source_validation_allowed"] is True
    forbidden = (
        "phase8_7b_feature_computation_allowed",
        "model_fitting_allowed",
        "production_prediction_allowed",
        "model_or_strategy_promotion_allowed",
        "raw_payload_redistribution_allowed",
        "broker_action_allowed",
        "trading_action_allowed",
    )
    assert all(authority[name] is False for name in forbidden)

    assert tuple(
        gate_policy["experimental_dataset_status_requires_technical_gates"]
    ) == runtime.TECHNICAL_GATES
    assert gate_policy["g3_or_g5_must_not_be_forced_to_pass"] is True
    assert gate_policy["experimental_dataset_status"] == runtime.EXPERIMENTAL_STATUS
    assert runtime_policy["required_policy_flag"] == runtime.POLICY_FLAG
    assert runtime_policy["output_artifact_count"] == 10
    assert runtime_policy["fallback_or_substitution_allowed"] is False
    assert runtime_policy["raw_response_persistence_allowed"] is False
