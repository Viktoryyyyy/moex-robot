import pytest

from moex_core.contracts.route_b_result_return import (
    APPROVED_ROUTE_B_RESULT_RETURN_PATHS,
    PM_L3_RETURN_INTAKE_REQUEST_SCHEMA_VERSION,
    PM_L3_RETURN_PACKAGE_SCHEMA_VERSION,
    RouteBResultReturnContractError,
    validate_route_b_pm_l3_return_intake_request_values,
    validate_route_b_pm_l3_return_package_values,
)


def _authority_boundary() -> dict[str, object]:
    return {
        "pm_l2_final_verdict_authority": True,
        "pm_l2_merge_approval_authority": True,
        "pm_l3_validation_return_only": True,
        "subchat_return_to_role": "PM_L3_DELIVERY_VALIDATION_OWNER",
        "merge_performed_by_executor": False,
        "n8n_merge_allowed": False,
        "direct_main_write_allowed": False,
        "force_push_allowed": False,
        "file_delete_allowed": False,
        "approved_for_merge": False,
        "ci_passed_is_merge_approval": False,
        "pm_l2_approval_claimed_by_pm_l3": False,
        "explicit_pm_l2_approval_package_ref": None,
    }


def _intake_request() -> dict[str, object]:
    return {
        "schema_version": PM_L3_RETURN_INTAKE_REQUEST_SCHEMA_VERSION,
        "workflow_run_id": "route_b_cycle_001",
        "request_id": "request_001",
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "source_role": "PM_L3_DELIVERY_VALIDATION_OWNER",
        "return_to_role": "PM_L2_PHASE_OWNER",
        "return_type": "pm_l3_after_github_pr_validation",
        "pm_l3_validation_report": {"validation_status": "pass"},
        "evidence_refs": ("artifacts/route_b/pm_l3_validation_report_001.yaml",),
        "authority_boundary": _authority_boundary(),
    }


def _return_package() -> dict[str, object]:
    return {
        "schema_version": PM_L3_RETURN_PACKAGE_SCHEMA_VERSION,
        "workflow_run_id": "route_b_cycle_001",
        "request_id": "request_001",
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "pm_l3_validation_status": "pass",
        "acceptance_criteria_check": {"status": "pass"},
        "repo_scope_check": {"status": "pass"},
        "artifact_contract_check": {"status": "pass"},
        "test_or_ci_check": {"status": "pass", "workflow_name": "tests"},
        "pr_refs": ("https://github.com/Viktoryyyyy/moex-robot/pull/136",),
        "ci_refs": ("https://github.com/Viktoryyyyy/moex-robot/actions/runs/1",),
        "changed_file_refs": tuple({"path": path, "status": "added"} for path in APPROVED_ROUTE_B_RESULT_RETURN_PATHS),
        "blockers": (),
        "required_fixes": (),
        "final_pm_l2_review_required": True,
        "pm_l2_decision_needed": "final_phase_verdict_or_merge_review",
        "authority_boundary": _authority_boundary(),
    }


def test_pm_l3_return_intake_accepts_valid_pm_l3_to_pm_l2_request() -> None:
    result = validate_route_b_pm_l3_return_intake_request_values(_intake_request())

    assert result.source_role == "PM_L3_DELIVERY_VALIDATION_OWNER"
    assert result.return_to_role == "PM_L2_PHASE_OWNER"
    assert result.return_type == "pm_l3_after_github_pr_validation"


@pytest.mark.parametrize("return_type", ("pm_l3_after_subchat_validation", "pm_l3_after_github_pr_validation", "pm_l3_blocker_return"))
def test_pm_l3_return_intake_accepts_allowed_return_types(return_type: str) -> None:
    request = _intake_request()
    request["return_type"] = return_type

    result = validate_route_b_pm_l3_return_intake_request_values(request)

    assert result.return_type == return_type


def test_pm_l3_return_intake_rejects_missing_workflow_run_id() -> None:
    request = _intake_request()
    del request["workflow_run_id"]

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_intake_request_values(request)


def test_pm_l3_return_intake_rejects_wrong_source_role() -> None:
    request = _intake_request()
    request["source_role"] = "SUBCHAT_IMPLEMENTATION"

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_intake_request_values(request)


def test_pm_l3_return_intake_rejects_wrong_return_role() -> None:
    request = _intake_request()
    request["return_to_role"] = "SUBCHAT_IMPLEMENTATION"

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_intake_request_values(request)


def test_pm_l3_return_intake_rejects_repository_mismatch() -> None:
    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_intake_request_values(_intake_request(), expected_repository_full_name="other/repo")


def test_pm_l3_return_package_accepts_valid_package() -> None:
    result = validate_route_b_pm_l3_return_package_values(_return_package())

    assert result.schema_version == PM_L3_RETURN_PACKAGE_SCHEMA_VERSION
    assert result.pm_l3_validation_status == "pass"
    assert result.final_pm_l2_review_required is True


@pytest.mark.parametrize("status", ("pass", "conditional_pass", "fail", "blocked"))
def test_pm_l3_return_package_accepts_validation_status_values(status: str) -> None:
    package = _return_package()
    package["pm_l3_validation_status"] = status

    result = validate_route_b_pm_l3_return_package_values(package)

    assert result.pm_l3_validation_status == status


def test_pm_l3_return_package_rejects_pm_l2_review_not_required() -> None:
    package = _return_package()
    package["final_pm_l2_review_required"] = False

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_package_values(package)


def test_pm_l3_return_package_rejects_pm_l2_approval_claim() -> None:
    package = _return_package()
    package["authority_boundary"]["pm_l2_approval_claimed_by_pm_l3"] = True

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_package_values(package)


def test_pm_l3_return_package_rejects_approved_for_merge_without_pm_l2_package() -> None:
    package = _return_package()
    package["authority_boundary"]["approved_for_merge"] = True

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_package_values(package)
