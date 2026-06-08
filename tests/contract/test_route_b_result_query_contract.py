import pytest

from moex_core.contracts.route_b_result_return import (
    APPROVED_ROUTE_B_RESULT_RETURN_PATHS,
    REQUIRED_RESULT_QUERY_SECTIONS,
    RESULT_QUERY_REQUEST_SCHEMA_VERSION,
    RESULT_QUERY_RESPONSE_SCHEMA_VERSION,
    RouteBResultReturnContractError,
    validate_route_b_result_query_request_values,
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
        "explicit_pm_l2_approval_package_ref": None,
    }


def _request() -> dict[str, object]:
    return {
        "schema_version": RESULT_QUERY_REQUEST_SCHEMA_VERSION,
        "workflow_run_id": "route_b_cycle_001",
        "idempotency_key": "idem_001",
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "requested_by_role": "PM_L2_PHASE_OWNER",
        "include_sections": REQUIRED_RESULT_QUERY_SECTIONS,
    }


def _response() -> dict[str, object]:
    return {
        "schema_version": RESULT_QUERY_RESPONSE_SCHEMA_VERSION,
        "workflow_run_id": "route_b_cycle_001",
        "idempotency_key": "idem_001",
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "status": "pm_l3_return_ready_for_pm_l2_review",
        "current_state": "pm_l3_validation_returned",
        "current_phase": "pm_l2_review_required",
        "pm_l3_package": {"artifact_ref": "docs/sot/context/packages/pm_l3_package_001.yaml"},
        "github_execution_result": {"artifact_ref": "artifacts/route_b/github_execution_result_001.yaml"},
        "pr_validation_package": {"artifact_ref": "artifacts/route_b/pr_validation_package_001.yaml"},
        "pm_l3_return_package": {"artifact_ref": "artifacts/route_b/pm_l3_return_package_001.yaml"},
        "evidence_refs": ("docs/sot/route_b/result_query_and_pm_l3_return_interfaces.v1.md",),
        "pr_refs": ("https://github.com/Viktoryyyyy/moex-robot/pull/136",),
        "ci_refs": ("https://github.com/Viktoryyyyy/moex-robot/actions/runs/1",),
        "changed_file_refs": tuple({"path": path, "status": "added"} for path in APPROVED_ROUTE_B_RESULT_RETURN_PATHS),
        "blockers": (),
        "required_fixes": (),
        "pm_l2_review_required": True,
        "authority_boundary": _authority_boundary(),
        "events_summary": ({"event": "pm_l3_return_intake_received"},),
        "steps_summary": ({"step": "result_query_response_built"},),
    }


def test_result_query_request_accepts_full_pm_l2_evidence_request() -> None:
    result = validate_route_b_result_query_request_values(_request(), expected_repository_full_name="Viktoryyyyy/moex-robot")

    assert result.schema_version == RESULT_QUERY_REQUEST_SCHEMA_VERSION
    assert set(result.include_sections) == set(REQUIRED_RESULT_QUERY_SECTIONS)
    assert result.requested_by_role == "PM_L2_PHASE_OWNER"


def test_result_query_request_accepts_idempotency_key_without_workflow_run_id() -> None:
    request = _request()
    del request["workflow_run_id"]

    result = validate_route_b_result_query_request_values(request)

    assert result.workflow_run_id is None
    assert result.idempotency_key == "idem_001"


def test_result_query_request_rejects_status_only_include_sections() -> None:
    request = _request()
    request["include_sections"] = ("status",)

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_result_query_request_values(request)


def test_result_query_request_rejects_non_pm_l2_requester() -> None:
    request = _request()
    request["requested_by_role"] = "PM_L3_DELIVERY_VALIDATION_OWNER"

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_result_query_request_values(request)


def test_result_query_request_rejects_repository_mismatch() -> None:
    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_result_query_request_values(_request(), expected_repository_full_name="other/repo")


def test_result_query_response_accepts_full_cycle_evidence_package() -> None:
    result = validate_route_b_result_query_response_values(_response())

    assert result.schema_version == RESULT_QUERY_RESPONSE_SCHEMA_VERSION
    assert result.pm_l2_review_required is True
    assert set(result.changed_files) == set(APPROVED_ROUTE_B_RESULT_RETURN_PATHS)


def test_result_query_response_rejects_missing_evidence_section_without_blocker_note() -> None:
    response = _response()
    response["pm_l3_return_package"] = None

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_result_query_response_values(response)


def test_result_query_response_accepts_explicit_null_section_with_blocker_note() -> None:
    response = _response()
    response["pm_l3_return_package"] = None
    response["blockers"] = ("pm_l3_return_package missing because PM L3 returned blocker",)

    result = validate_route_b_result_query_response_values(response)

    assert result.workflow_run_id == "route_b_cycle_001"


def test_result_query_response_rejects_status_only_pm_l2_verdict_package() -> None:
    response = _response()
    response["pm_l3_package"] = None
    response["github_execution_result"] = None
    response["pr_validation_package"] = None
    response["pm_l3_return_package"] = None
    response["blockers"] = (
        "pm_l3_package missing",
        "github_execution_result missing",
        "pr_validation_package missing",
        "pm_l3_return_package missing",
    )

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_result_query_response_values(response)


def test_result_query_response_rejects_pm_l2_review_false() -> None:
    response = _response()
    response["pm_l2_review_required"] = False

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_result_query_response_values(response)


def test_result_query_response_rejects_ci_passed_as_merge_approval() -> None:
    response = _response()
    response["authority_boundary"]["ci_passed_is_merge_approval"] = True

    with pytest.raises(RouteBResultReturnContractError):
        validate_route_b_result_query_response_values(response)
