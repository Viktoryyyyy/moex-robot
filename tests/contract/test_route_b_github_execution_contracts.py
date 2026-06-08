from moex_core.contracts.route_b_github_execution import (
    APPROVED_ROUTE_B_EXECUTOR_PATHS,
    REQUEST_SCHEMA_VERSION,
    RESULT_SCHEMA_VERSION,
    VALIDATION_PACKAGE_SCHEMA_VERSION,
    validate_route_b_github_execution_request_values,
    validate_route_b_github_execution_result_values,
    validate_route_b_pr_validation_package_values,
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
        "branch_plan": {
            "branch_name": branch_name,
            "source": "base_sha",
        },
        "pr_plan": {
            "base_branch": "main",
            "head_branch": branch_name,
            "draft": False,
        },
        "validation_requirements": {
            "github_actions_workflow": "tests",
            "pm_l2_review_required": True,
        },
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


def _ci_refs(head_sha: str) -> dict[str, object]:
    return {
        "workflow_name": "tests",
        "run_id": 1,
        "job_name": "python-tests",
        "head_sha": head_sha,
        "conclusion": "success",
    }


def _pr_refs(head_sha: str, feature_branch: str) -> dict[str, object]:
    return {
        "pr_number": 135,
        "pr_url": "https://github.com/Viktoryyyyy/moex-robot/pull/135",
        "head_branch": feature_branch,
        "base_branch": "main",
        "head_sha": head_sha,
        "status": "open",
    }


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
        "pr_refs": _pr_refs(head_sha, feature_branch),
        "ci_refs": _ci_refs(head_sha),
        "evidence_refs": {
            "compare_ref": "https://github.com/Viktoryyyyy/moex-robot/pull/135/files",
            "workflow_ref": "https://github.com/Viktoryyyyy/moex-robot/actions/runs/1",
        },
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


def _validation_package() -> dict[str, object]:
    head_sha = "implementation-sha"
    feature_branch = "n8n/route_b_github_executor_v1-test"
    return {
        "schema_version": VALIDATION_PACKAGE_SCHEMA_VERSION,
        "validation_package_id": "route_b_pr_validation_package_test",
        "workflow_run_id": "route_b_github_executor_v1",
        "request_id": "phase1",
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "context_registry_binding": {
            "registry_path": "docs/sot/context/registry.route_b.v1.yaml",
            "registry_ref": "base-sha",
            "source_of_truth": "github_repo",
        },
        "scope_validation": {
            "approved_scope_paths": APPROVED_ROUTE_B_EXECUTOR_PATHS,
            "changed_files_within_scope": True,
            "file_scope_exact": True,
            "forbidden_operations_absent": True,
        },
        "git_refs": {
            "base_branch": "main",
            "base_sha": "base-sha",
            "feature_branch": feature_branch,
            "head_sha": head_sha,
        },
        "pr_refs": _pr_refs(head_sha, feature_branch),
        "ci_refs": _ci_refs(head_sha),
        "changed_file_refs": _changed_files(),
        "pm_l3_validation": {
            "validation_verdict": "pass",
            "evidence_complete": True,
        },
        "pm_l2_boundary": {
            "pm_l2_review_required": True,
            "merge_authority": "PM_L2_ONLY",
            "n8n_merge_allowed": False,
            "approved_for_merge": False,
            "explicit_pm_l2_approval_package_ref": None,
        },
    }


def test_execution_request_contract_accepts_valid_phase1_request() -> None:
    request = validate_route_b_github_execution_request_values(_request())

    assert request.schema_version == REQUEST_SCHEMA_VERSION
    assert request.branch_name.startswith("n8n/")
    assert set(request.allowed_paths) == set(APPROVED_ROUTE_B_EXECUTOR_PATHS)


def test_execution_result_contract_accepts_valid_pr_ci_result() -> None:
    result = validate_route_b_github_execution_result_values(_result())

    assert result.schema_version == RESULT_SCHEMA_VERSION
    assert result.status == "ci_passed"
    assert result.implementation_commit_sha == "implementation-sha"


def test_pr_validation_package_contract_accepts_valid_package() -> None:
    package = validate_route_b_pr_validation_package_values(_validation_package())

    assert package.schema_version == VALIDATION_PACKAGE_SCHEMA_VERSION
    assert package.feature_branch.startswith("n8n/")
    assert set(package.changed_files) == set(APPROVED_ROUTE_B_EXECUTOR_PATHS)
