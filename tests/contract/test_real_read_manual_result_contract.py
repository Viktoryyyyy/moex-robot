import pytest

from moex_research.runners.real_read_manual_result import (
    ALLOWED_INTAKE_MODES,
    ALLOWED_INTAKE_STATES,
    ALLOWED_RESULT_SCOPES,
    DECISION_FIELDS,
    INTAKE_FIELDS,
    RESULT_FIELDS,
    RealReadManualResult,
    RealReadManualResultError,
    RealReadManualResultIntake,
    RealReadManualResultIntakeDecision,
    evaluate_real_read_manual_result_intake,
    validate_real_read_manual_result,
    validate_real_read_manual_result_intake,
    validate_real_read_manual_result_intake_decision,
)
from moex_research.runners.real_read_owner_run import RealReadOwnerAckDecision


def _ack(status: str = "ready_for_manual_apply") -> RealReadOwnerAckDecision:
    return RealReadOwnerAckDecision(
        decision_status=status,
        ack_id="ema_3_19.real_read.owner_ack.test",
        handoff_id="ema_3_19.real_read.handoff.test",
        dataset_ref_id="canonical.dataset.si.d1.v1",
        instrument_id="Si",
        timeframe="D1",
        real_read_allowed=False,
        manual_apply_required=True if status == "ready_for_manual_apply" else False,
        manual_result_required=True if status == "ready_for_manual_apply" else False,
        reason_or_none=None if status == "ready_for_manual_apply" else "ack blocked",
    )


def _result(**overrides: object) -> RealReadManualResult:
    values: dict[str, object] = {
        "result_id": "ema_3_19.real_read.manual_result.test",
        "owner_ack_decision": _ack(),
        "operator_report_ref": "operator_report.real_read.test",
        "artifact_manifest_ref": "artifact_manifest.real_read.test",
        "quality_report_ref": "quality_report.real_read.test",
        "lineage_report_ref": "lineage_report.real_read.test",
        "result_scope": "manual_result_metadata_only",
        "read_executed": True,
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadManualResult(**values)


def _intake(**overrides: object) -> RealReadManualResultIntake:
    values: dict[str, object] = {
        "intake_id": "ema_3_19.real_read.manual_intake.test",
        "manual_result": _result(),
        "intake_mode": "manual_result_intake_only",
        "requested_intake_state": "accepted_for_pm_review",
        "allow_registry_write": False,
        "allow_runtime": False,
        "allow_promotion": False,
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadManualResultIntake(**values)


def test_valid_manual_result_passes_metadata_only():
    result = _result()

    assert validate_real_read_manual_result(result) is result
    assert frozenset(result.__dict__) == frozenset(RESULT_FIELDS)
    assert ALLOWED_RESULT_SCOPES == frozenset({"manual_result_metadata_only"})


def test_manual_result_requires_ready_ack():
    with pytest.raises(RealReadManualResultError):
        _result(owner_ack_decision=_ack("blocked"))


def test_manual_result_requires_read_executed_and_metadata():
    with pytest.raises(RealReadManualResultError):
        _result(read_executed=False)
    with pytest.raises(RealReadManualResultError):
        _result(metadata_only=False)


def test_valid_intake_passes_without_downstream_authorization():
    intake = _intake()

    assert validate_real_read_manual_result_intake(intake) is intake
    assert frozenset(intake.__dict__) == frozenset(INTAKE_FIELDS)
    assert ALLOWED_INTAKE_MODES == frozenset({"manual_result_intake_only"})
    assert ALLOWED_INTAKE_STATES == frozenset({"blocked", "accepted_for_pm_review", "rejected"})


@pytest.mark.parametrize("field_name", ("allow_registry_write", "allow_runtime", "allow_promotion"))
def test_downstream_flags_must_be_false(field_name: str):
    with pytest.raises(RealReadManualResultError):
        _intake(**{field_name: True})


def test_intake_accepts_for_pm_review_only():
    decision = evaluate_real_read_manual_result_intake(_intake())

    assert decision.decision_status == "accepted_for_pm_review"
    assert decision.registry_write_allowed is False
    assert decision.runtime_allowed is False
    assert decision.promotion_allowed is False
    assert decision.reason_or_none is None
    assert validate_real_read_manual_result_intake_decision(decision) is decision
    assert frozenset(decision.__dict__) == frozenset(DECISION_FIELDS)


def test_blocked_and_rejected_intake_states_do_not_allow_downstream():
    blocked = evaluate_real_read_manual_result_intake(_intake(requested_intake_state="blocked"))
    rejected = evaluate_real_read_manual_result_intake(_intake(requested_intake_state="rejected"))

    assert blocked.decision_status == "blocked"
    assert blocked.registry_write_allowed is False
    assert blocked.runtime_allowed is False
    assert blocked.promotion_allowed is False
    assert rejected.decision_status == "rejected"
    assert rejected.registry_write_allowed is False
    assert rejected.runtime_allowed is False
    assert rejected.promotion_allowed is False


def test_decision_cannot_enable_registry_runtime_or_promotion():
    with pytest.raises(RealReadManualResultError):
        RealReadManualResultIntakeDecision(
            decision_status="accepted_for_pm_review",
            intake_id="intake.fixture",
            result_id="result.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            registry_write_allowed=True,
            runtime_allowed=False,
            promotion_allowed=False,
            reason_or_none=None,
        )
    with pytest.raises(RealReadManualResultError):
        RealReadManualResultIntakeDecision(
            decision_status="accepted_for_pm_review",
            intake_id="intake.fixture",
            result_id="result.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            registry_write_allowed=False,
            runtime_allowed=True,
            promotion_allowed=False,
            reason_or_none=None,
        )
    with pytest.raises(RealReadManualResultError):
        RealReadManualResultIntakeDecision(
            decision_status="accepted_for_pm_review",
            intake_id="intake.fixture",
            result_id="result.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            registry_write_allowed=False,
            runtime_allowed=False,
            promotion_allowed=True,
            reason_or_none=None,
        )
