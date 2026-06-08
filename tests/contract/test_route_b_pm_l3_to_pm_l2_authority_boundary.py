import pytest

from moex_core.contracts.route_b_result_return import (
    PM_L3_RETURN_PACKAGE_SCHEMA_VERSION,
    RESULT_QUERY_RESPONSE_SCHEMA_VERSION,
    RouteBResultReturnContractError,
    validate_route_b_pm_l3_return_package_values,
    validate_route_b_result_query_response_values,
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


def _return_package() -> dict[str, object]:
    return {
        "schema_version": PM_L3_RETURN_PACKAGE_SCHEMA_VERSION,
        "workflow_run_id": "route_b_cycle_001",
        "request_id": "request_001",
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "pm_l3_validation_status": "conditional_pass",
        "acceptance_criteria_check": {"status": "conditional_pass"},
        "repo_scope_check": {"status": "pass"},
        "artifact_contract_check": {"status": "pass"},
        "test_or_ci_check": {"status": "pass", "ci_passed": True},
        "pr_refs": ("https://github.com/Viktoryyyyy/moex-robot/pull/136",),
        "ci_refs": ("https://github.com/Viktoryyyyy/moex-robot/actions/runs/1",),
        "changed_file_refs": ({"path": "docs/sot/route_b/result_query_and_pm_l3_return_interfaces.v1.md", "status": "added"},),
        "blockers": (),
        "required_fixes": (),
        "final_pm_l2_review_required": True,
        "pm_l2_decision_needed": "final_phase_verdict_or_merge_review",
        "authority_boundary": _authority_boundary(),
    }


def _query_response() -> dict[str, object]:
    return {
        "schema_version": RESULT_QUERY_RESPONSE_SCHEMA_VERSION,
        "workflow_run_id": "route_b_cycle_001",
        "idempotency_key": "idem_001",
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "status": "pm_l2_review_required",
        "current_state": "pm_l3_validation_returned",
        "current_phase": "pm_l2_review",
        "pm_l3_package": {"artifact_ref": "docs/sot/context/packages/pm_l3_package_001.yaml"},
        "github_execution_result": {"artifact_ref": "artifacts/route_b/github_execution_result_001.yaml"},
        "pr_validation_package": {"artifact_ref": "artifacts/route_b/pr_validation_package_001.yaml"},
        "pm_l3_return_package": {"artifact_ref": "artifacts/route_b/pm_l3_return_package_001.yaml"},
        "evidence_refs": ("docs/sot/route_b/result_query_and_pm_l3_return_interfaces.v1.md",),
        "pr_refs": ("https://github.com/Viktoryyyyy/moex-robot/pull/136",),
        "ci_refs": ("https://github.com/Viktoryyyyy/moex-robot/actions/runs/1",),
        "changed_file_refs": ({"path": "docs/sot/route_b/result_query_and_pm_l3_return_interfaces.v1.md", "status": "added"},),
        "blockers": (),
        "required_fixes": (),
        "pm_l2_review_required": True,
        "authority_boundary": _authority_boundary(),
        "events_summary": ({"event": "ci_passed"},),
        "steps_summary": ({"step": "pm_l3_return_to_pm_l2"},),
    }


@pytest.mark.parametrize(
    "flag",
    ("merge_performed_by_executor", "n8n_merge_allowed", "direct_main_write_allowed", "force_push_allowed", "file_delete_allowed"),
)
def test_pm_l3_return_package_rejects_forbidden_operations(flag: str) -> None:
    package = _return_package()
    package["authority_boundary"][flag] = True

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_package_values(package)


def test_pm_l3_return_package_rejects_subchat_direct_return_to_pm_l2() -> None:
    package = _return_package()
    package["authority_boundary"]["subchat_return_to_role"] = "PM_L2_PHASE_OWNER"

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_package_values(package)


def test_pm_l3_return_package_rejects_nested_subchat_to_pm_l2_return() -> None:
    package = _return_package()
    package["repo_scope_check"]["subchat_return"] = {
        "source_role": "SUBCHAT_IMPLEMENTATION",
        "return_to_role": "PM_L2_PHASE_OWNER",
    }

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_package_values(package)


def test_pm_l3_return_package_rejects_ci_passed_as_approval() -> None:
    package = _return_package()
    package["authority_boundary"]["ci_passed_is_merge_approval"] = True

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_pm_l3_return_package_values(package)


def test_result_query_response_rejects_n8n_merge_claim() -> None:
    response = _query_response()
    response["authority_boundary"]["n8n_merge_allowed"] = True

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_result_query_response_values(response)


def test_result_query_response_rejects_pm_l3_claiming_final_verdict_authority() -> None:
    response = _query_response()
    response["authority_boundary"]["pm_l3_validation_return_only"] = False

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_result_query_response_values(response)
