import pytest

from moex_core.contracts.route_b_github_execution import (
    APPROVED_ROUTE_B_EXECUTOR_PATHS,
    REQUEST_SCHEMA_VERSION,
    RESULT_SCHEMA_VERSION,
    RouteBGithubExecutionContractError,
    validate_route_b_github_execution_request_values,
    validate_route_b_github_execution_result_values,
)


def _role_ref(role_id: str) -> dict[str, object]:
    return {
        "artifact_ref": f"docs/sot/context/roles/{role_id}.v1.yaml",
        "role_context_ref": {
            "role_id": role_id,
            "role_context_version": "v1",
        },
    }


def _request() -> dict[str, object]:
    branch_name = "n8n/route_b_github_executor_v1-test"
    return {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "workflow_run_id": "route_b_github_executor_v1",
        "request_id": "phase1",
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "base_branch": "main",
        "base_sha": "base-sha",
        "route_b_context_registry_ref": {
            "path": "docs/sot/context/registry.route_b.v1.yaml",
            "repo_ref": "base-sha",
            "source_of_truth": "github_repo",
        },
        "pm_l2_request_ref": _role_ref("PM_L2_PHASE_OWNER"),
        "pm_l3_package_ref": _role_ref("PM_L3_DELIVERY_VALIDATION_OWNER"),
        "execution_scope": {
            "scope_type": "route_b_github_branch_pr_executor_phase1",
            "allowed_paths": APPROVED_ROUTE_B_EXECUTOR_PATHS,
            "delete_files_allowed": False,
        },
        "branch_plan": {"branch_name": branch_name, "source": "base_sha"},
        "pr_plan": {"base_branch": "main", "head_branch": branch_name, "draft": False},
        "validation_requirements": {"github_actions_workflow": "tests"},
        "governance_flags": {
            "direct_main_write_allowed": False,
            "n8n_merge_allowed": False,
            "force_push_allowed": False,
            "file_delete_allowed": False,
            "production_runtime_allowed": False,
        },
        "rejection_rules": {
            "reject_dynamic_marker_tokens": True,
            "reject_target_role_context_ref": True,
            "reject_missing_role_context_ref": True,
            "reject_non_n8n_branch_prefix": True,
            "reject_direct_main_write": True,
            "reject_n8n_merge": True,
            "reject_force_push": True,
            "reject_file_delete": True,
            "reject_runtime_or_broker_scope": True,
        },
    }


def _changed_files() -> tuple[dict[str, str], ...]:
    return tuple({"path": path, "status": "added"} for path in APPROVED_ROUTE_B_EXECUTOR_PATHS)


def _result() -> dict[str, object]:
    head_sha = "implementation-sha"
    feature_branch = "n8n/route_b_github_executor_v1-test"
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "workflow_run_id": "route_b_github_executor_v1",
        "request_id": "phase1",
        "status": "ci_passed",
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "base_branch": "main",
        "base_sha": "base-sha",
        "feature_branch": feature_branch,
        "branch_ref": {"branch_name": feature_branch, "commit_sha": head_sha},
        "branch_created_at": "2026-06-08T00:00:00Z",
        "implementation_commit_sha": head_sha,
        "implementation_tree_sha": "tree-sha",
        "changed_files": _changed_files(),
        "pr_refs": {
            "pr_number": 135,
            "pr_url": "https://github.com/Viktoryyyyy/moex-robot/pull/135",
            "head_branch": feature_branch,
            "base_branch": "main",
            "head_sha": head_sha,
            "status": "open",
        },
        "ci_refs": {
            "workflow_name": "tests",
            "run_id": 1,
            "job_name": "python-tests",
            "head_sha": head_sha,
            "conclusion": "success",
        },
        "evidence_refs": {"workflow_ref": "https://github.com/Viktoryyyyy/moex-robot/actions/runs/1"},
        "authority_boundary": {
            "merge_authority": "PM_L2_ONLY",
            "merge_performed_by_executor": False,
            "approved_for_merge": False,
            "pm_l2_approval_claimed_by_executor": False,
            "n8n_merge_allowed": False,
            "direct_main_write_allowed": False,
        },
        "error": None,
    }


@pytest.mark.parametrize(
    "flag",
    (
        "direct_main_write_allowed",
        "n8n_merge_allowed",
        "force_push_allowed",
        "file_delete_allowed",
        "production_runtime_allowed",
    ),
)
def test_request_rejects_forbidden_governance_flags(flag: str) -> None:
    request = _request()
    request["governance_flags"][flag] = True

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_request_values(request)


def test_request_requires_n8n_branch_prefix() -> None:
    request = _request()
    request["branch_plan"]["branch_name"] = "feature/route_b_github_executor_v1-test"
    request["pr_plan"]["head_branch"] = "feature/route_b_github_executor_v1-test"

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_request_values(request)


def test_request_rejects_old_route_b_branch_prefix() -> None:
    request = _request()
    request["branch_plan"]["branch_name"] = "route-b/route_b_github_executor_v1-test"
    request["pr_plan"]["head_branch"] = "route-b/route_b_github_executor_v1-test"

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_request_values(request)


def test_request_rejects_target_role_context_ref() -> None:
    request = _request()
    request["pm_l3_package_ref"]["target_role_context_ref"] = {
        "role_id": "SUBCHAT_IMPLEMENTATION",
        "role_context_version": "v1",
    }

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_request_values(request)


def test_request_requires_role_context_ref() -> None:
    request = _request()
    del request["pm_l3_package_ref"]["role_context_ref"]

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_request_values(request)


@pytest.mark.parametrize("marker", ("late" + "st", "cur" + "rent", "auto" + "detect"))
def test_request_rejects_dynamic_markers(marker: str) -> None:
    request = _request()
    request["route_b_context_registry_ref"]["repo_ref"] = marker

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_request_values(request)


def test_result_rejects_deleted_files() -> None:
    result = _result()
    result["changed_files"] = (
        {"path": "docs/sot/route_b/github_branch_pr_executor.v1.md", "status": "deleted"},
    )

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_result_values(result)


def test_result_rejects_branch_ref_not_pointing_to_implementation_commit() -> None:
    result = _result()
    result["branch_ref"]["commit_sha"] = "different-sha"

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_result_values(result)


def test_result_rejects_ci_head_sha_mismatch() -> None:
    result = _result()
    result["ci_refs"]["head_sha"] = "different-sha"

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_result_values(result)


@pytest.mark.parametrize(
    "field",
    ("merge_performed_by_executor", "approved_for_merge", "pm_l2_approval_claimed_by_executor"),
)
def test_result_rejects_executor_merge_or_approval_claims(field: str) -> None:
    result = _result()
    result["authority_boundary"][field] = True

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_github_execution_result_values(result)
