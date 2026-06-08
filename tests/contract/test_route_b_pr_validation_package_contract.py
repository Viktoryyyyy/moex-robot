import pytest

from moex_core.contracts.route_b_github_execution import (
    APPROVED_ROUTE_B_EXECUTOR_PATHS,
    VALIDATION_PACKAGE_SCHEMA_VERSION,
    RouteBGithubExecutionContractError,
    validate_route_b_pr_validation_package_values,
)


def _changed_files() -> tuple[dict[str, str], ...]:
    return tuple({"path": path, "status": "added"} for path in APPROVED_ROUTE_B_EXECUTOR_PATHS)


def _package() -> dict[str, object]:
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


def test_validation_package_requires_pm_l2_review() -> None:
    package = _package()
    package["pm_l2_boundary"]["pm_l2_review_required"] = False

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_pr_validation_package_values(package)


def test_validation_package_requires_pm_l2_only_merge_authority() -> None:
    package = _package()
    package["pm_l2_boundary"]["merge_authority"] = "N8N_EXECUTOR"

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_pr_validation_package_values(package)


def test_validation_package_rejects_n8n_merge_allowed() -> None:
    package = _package()
    package["pm_l2_boundary"]["n8n_merge_allowed"] = True

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_pr_validation_package_values(package)


def test_validation_package_rejects_approved_for_merge_without_pm_l2_approval_package() -> None:
    package = _package()
    package["pm_l2_boundary"]["approved_for_merge"] = True

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_pr_validation_package_values(package)


def test_validation_package_accepts_approval_only_with_explicit_pm_l2_approval_package() -> None:
    package = _package()
    package["pm_l2_boundary"]["approved_for_merge"] = True
    package["pm_l2_boundary"]["explicit_pm_l2_approval_package_ref"] = "docs/sot/route_b/pm_l2_approval_package.v1.md"

    validated = validate_route_b_pr_validation_package_values(package)

    assert validated.head_sha == "implementation-sha"


def test_validation_package_requires_pr_and_ci_evidence() -> None:
    package = _package()
    del package["ci_refs"]

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_pr_validation_package_values(package)


def test_validation_package_rejects_ci_not_tied_to_pr_head_sha() -> None:
    package = _package()
    package["ci_refs"]["head_sha"] = "different-sha"

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_pr_validation_package_values(package)


def test_validation_package_rejects_changed_file_outside_scope() -> None:
    package = _package()
    package["changed_file_refs"] = _changed_files() + (
        {"path": "src/moex_runtime/forbidden.py", "status": "added"},
    )

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_pr_validation_package_values(package)


def test_validation_package_rejects_old_route_b_branch_prefix() -> None:
    package = _package()
    package["git_refs"]["feature_branch"] = "route-b/route_b_github_executor_v1-test"
    package["pr_refs"]["head_branch"] = "route-b/route_b_github_executor_v1-test"

    with pytest.raises(RouteBGithubExecutionContractError):
        validate_route_b_pr_validation_package_values(package)
