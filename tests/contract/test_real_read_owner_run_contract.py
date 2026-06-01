import pytest

from moex_research.runners.real_read_execution_review import RealReadExecutionReviewDecision
from moex_research.runners.real_read_owner_run import (
    ACK_FIELDS,
    ALLOWED_ACK_MODES,
    ALLOWED_ACK_STATES,
    ALLOWED_HANDOFF_SCOPES,
    DECISION_FIELDS,
    HANDOFF_FIELDS,
    RealReadOwnerAck,
    RealReadOwnerAckDecision,
    RealReadOwnerHandoff,
    RealReadOwnerRunError,
    evaluate_real_read_owner_ack,
    validate_real_read_owner_ack,
    validate_real_read_owner_ack_decision,
    validate_real_read_owner_handoff,
)


def _execution(status: str = "eligible_for_owner_run_real_read") -> RealReadExecutionReviewDecision:
    return RealReadExecutionReviewDecision(
        decision_status=status,
        review_id="ema_3_19.real_read.execution_review.test",
        design_id="ema_3_19.real_read.design.test",
        dataset_ref_id="canonical.dataset.si.d1.v1",
        instrument_id="Si",
        timeframe="D1",
        real_read_allowed=False,
        owner_run_required=True if status == "eligible_for_owner_run_real_read" else False,
        reason_or_none=None if status == "eligible_for_owner_run_real_read" else "blocked upstream",
    )


def _handoff(**overrides: object) -> RealReadOwnerHandoff:
    values: dict[str, object] = {
        "handoff_id": "ema_3_19.real_read.handoff.test",
        "execution_review_decision": _execution(),
        "operator_role": "repo_operator",
        "approved_repo_commit": "25d3a996b4b9568241e26d779a14b84a39cd4e8d",
        "manual_apply_required": True,
        "manual_result_required": True,
        "handoff_scope": "manual_apply_instruction_metadata_only",
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadOwnerHandoff(**values)


def _ack(**overrides: object) -> RealReadOwnerAck:
    values: dict[str, object] = {
        "ack_id": "ema_3_19.real_read.owner_ack.test",
        "handoff": _handoff(),
        "ack_mode": "owner_handoff_ack_only",
        "requested_ack_state": "ready_for_manual_apply",
        "allow_real_read": False,
        "allow_network": False,
        "allow_registry_write": False,
        "allow_runtime": False,
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadOwnerAck(**values)


def test_valid_handoff_passes_metadata_only():
    handoff = _handoff()

    assert validate_real_read_owner_handoff(handoff) is handoff
    assert frozenset(handoff.__dict__) == frozenset(HANDOFF_FIELDS)
    assert ALLOWED_HANDOFF_SCOPES == frozenset({"manual_apply_instruction_metadata_only"})


def test_handoff_requires_eligible_execution_review_decision():
    with pytest.raises(RealReadOwnerRunError):
        _handoff(execution_review_decision=_execution("blocked"))


def test_handoff_requires_manual_flags():
    with pytest.raises(RealReadOwnerRunError):
        _handoff(manual_apply_required=False)
    with pytest.raises(RealReadOwnerRunError):
        _handoff(manual_result_required=False)


def test_valid_ack_passes_without_authorizing_read():
    ack = _ack()

    assert validate_real_read_owner_ack(ack) is ack
    assert frozenset(ack.__dict__) == frozenset(ACK_FIELDS)
    assert ALLOWED_ACK_MODES == frozenset({"owner_handoff_ack_only"})
    assert ALLOWED_ACK_STATES == frozenset({"blocked", "ready_for_manual_apply", "rejected"})


@pytest.mark.parametrize("field_name", ("allow_real_read", "allow_network", "allow_registry_write", "allow_runtime"))
def test_side_effect_flags_must_be_false(field_name: str):
    with pytest.raises(RealReadOwnerRunError):
        _ack(**{field_name: True})


def test_metadata_only_is_required_on_handoff_and_ack():
    with pytest.raises(RealReadOwnerRunError):
        _handoff(metadata_only=False)
    with pytest.raises(RealReadOwnerRunError):
        _ack(metadata_only=False)


def test_ack_can_mark_manual_apply_ready_but_not_authorize_read():
    decision = evaluate_real_read_owner_ack(_ack())

    assert decision.decision_status == "ready_for_manual_apply"
    assert decision.real_read_allowed is False
    assert decision.manual_apply_required is True
    assert decision.manual_result_required is True
    assert decision.reason_or_none is None
    assert validate_real_read_owner_ack_decision(decision) is decision
    assert frozenset(decision.__dict__) == frozenset(DECISION_FIELDS)


def test_blocked_and_rejected_ack_states_do_not_require_manual_apply():
    blocked = evaluate_real_read_owner_ack(_ack(requested_ack_state="blocked"))
    rejected = evaluate_real_read_owner_ack(_ack(requested_ack_state="rejected"))

    assert blocked.decision_status == "blocked"
    assert blocked.real_read_allowed is False
    assert blocked.manual_apply_required is False
    assert blocked.manual_result_required is False
    assert rejected.decision_status == "rejected"
    assert rejected.real_read_allowed is False
    assert rejected.manual_apply_required is False
    assert rejected.manual_result_required is False


def test_decision_cannot_authorize_direct_read():
    with pytest.raises(RealReadOwnerRunError):
        RealReadOwnerAckDecision(
            decision_status="ready_for_manual_apply",
            ack_id="ack.fixture",
            handoff_id="handoff.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            real_read_allowed=True,
            manual_apply_required=True,
            manual_result_required=True,
            reason_or_none=None,
        )
