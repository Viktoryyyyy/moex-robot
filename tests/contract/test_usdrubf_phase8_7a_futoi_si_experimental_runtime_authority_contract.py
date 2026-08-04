from __future__ import annotations

import json
from pathlib import Path

import pytest

from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_experimental_runtime as runtime,
)


def test_experimental_runtime_policy_contract() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / runtime.POLICY_REPO_PATH
    contract = json.loads(path.read_text(encoding="utf-8"))

    identity = contract["contract_identity"]
    boundary = contract["policy_boundary"]
    required_fields = contract["required_runtime_authority_fields"]
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
    assert boundary["runtime_authority_must_bind_data_root_identity"] is True
    assert boundary["canonical_data_root"] == (
        runtime.AUTHORIZED_DATA_ROOT.as_posix()
    )
    assert boundary["canonical_data_root_open_mode"] == (
        "nofollow_dirfd_from_filesystem_root"
    )
    assert boundary["data_root_ancestor_group_world_writable_allowed"] is False
    assert boundary["trusted_runtime_authority_root"] == (
        runtime.TRUSTED_AUTHORITY_ROOT.as_posix()
    )
    assert boundary["trusted_runtime_authority_owner_uid"] == (
        runtime.TRUSTED_AUTHORITY_OWNER_UID
    )
    assert boundary["trusted_runtime_authority_filename_rule"] == (
        "authorization_id.json"
    )
    assert (
        boundary["trusted_runtime_authority_group_world_writable_allowed"]
        is False
    )
    assert boundary["trusted_runtime_authority_ancestors_must_be_root_owned"] is True
    assert boundary["module_claims_global_single_use"] is False
    assert (
        boundary["one_run_per_operational_authority_is_orchestration_responsibility"]
        is True
    )
    assert "data_root_device" in required_fields
    assert "data_root_inode" in required_fields

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
    assert (
        runtime_policy[
            "data_root_identity_verified_before_retrieval_and_artifact_write"
        ]
        is True
    )
    assert runtime_policy["fallback_or_substitution_allowed"] is False
    assert runtime_policy["raw_response_persistence_allowed"] is False


def test_git_environment_uses_trusted_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PATH", "/tmp/fake-git:/usr/local/bin")
    monkeypatch.setenv("HOME", "/tmp/fake-home")
    monkeypatch.setenv("XDG_CONFIG_HOME", "/tmp/fake-xdg")
    monkeypatch.setenv("GIT_DIR", "/tmp/decoy.git")
    monkeypatch.setenv("GIT_WORK_TREE", "/tmp/decoy-worktree")

    environment = runtime._sanitized_git_environment()

    assert environment["PATH"] == runtime.TRUSTED_GIT_PATH
    assert environment["HOME"] == runtime.TRUSTED_GIT_CONFIG_HOME
    assert environment["XDG_CONFIG_HOME"] == runtime.TRUSTED_GIT_CONFIG_HOME
    assert "/tmp/fake-git" not in environment["PATH"]
    assert all(not key.upper().startswith("GIT_") for key in environment)


def test_git_command_disables_fsmonitor_and_untracked_cache() -> None:
    repo_root = Path("/tmp/repository")

    command = runtime._git_command(repo_root, "status")

    assert command[:3] == ["git", "-C", str(repo_root.resolve())]
    assert command[3:7] == [
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.untrackedCache=false",
    ]
    assert command[-1] == "status"


@pytest.mark.parametrize(
    "tagged",
    [
        "h hidden.py\0",
        "S skipped.py\0",
    ],
)
def test_hidden_index_flags_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    tagged: str,
) -> None:
    monkeypatch.setattr(runtime, "_run_git", lambda *_args, **_kwargs: tagged)

    with pytest.raises(runtime.base.validation.FutoiSiSourceValidationError):
        runtime._verify_no_hidden_index_flags(tmp_path)


def test_normal_index_flags_are_accepted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        runtime,
        "_run_git",
        lambda *_args, **_kwargs: "H normal.py\0",
    )

    runtime._verify_no_hidden_index_flags(tmp_path)
