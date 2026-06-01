import pytest

from moex_research.runners.real_read_chain import RealReadChainCloseoutDecision
from moex_research.runners.real_read_phase_status import (
    ALLOWED_PHASE_SCOPES,
    ALLOWED_PHASE_STATES,
    ALLOWED_STATUS_MODES,
    DECISION_FIELDS,
    PACKAGE_FIELDS,
    STATUS_FIELDS,
    RealReadPhaseStatus,
    RealReadPhaseStatusDecision,
    RealReadPhaseStatusError,
    RealReadPhaseStatusPackage,
    evaluate_real_read_phase_status,
    validate_real_read_phase_status,
    validate_real_read_phase_status_decision,
    validate_real_read_phase_status_package,
)


def _chain(status: str = "accepted_chain") -> RealReadChainCloseoutDecision:
    return RealReadChainCloseoutDecision(
        decision_status=status,
        closeout_id="real_read.chain.closeout.test",
        chain_id="real_read.chain.test",
        dataset_ref_id="canonical.dataset.si.d1.v1",
        instrument_id="Si",
        timeframe="D1",
        chain_status="real_read_review_chain_accepted" if status == "accepted_chain" else "blocked",
        registry_write_allowed=False,
        runtime_allowed=False,
        promotion_allowed=False,
        reason_or_none=None if status == "accepted_chain" else "chain blocked",
    )


def _package(**overrides: object) -> RealReadPhaseStatusPackage:
    values: dict[str, object] = {
        "package_id": "real_read.phase.package.test",
        "chain_closeout_decision": _chain(),
        "phase_id": "real_read.readiness.phase",
        "phase_scope": "repo_only_real_read_readiness_phase",
        "repo_evidence_ref": "repo.evidence.real_read.chain",
        "status_note_ref": "status.note.real_read.phase",
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadPhaseStatusPackage(**values)


def _status(**overrides: object) -> RealReadPhaseStatus:
    values: dict[str, object] = {
        "status_id": "real_read.phase.status.test",
        "status_package": _package(),
        "status_mode": "phase_status_closeout_only",
        "requested_phase_state": "closed_repo_only",
        "allow_registry_write": False,
        "allow_runtime": False,
        "allow_promotion": False,
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadPhaseStatus(**values)


def test_valid_phase_package_passes_metadata_only():
    package = _package()

    assert validate_real_read_phase_status_package(package) is package
    assert frozenset(package.__dict__) == frozenset(PACKAGE_FIELDS)
    assert ALLOWED_PHASE_SCOPES == frozenset({"repo_only_real_read_readiness_phase"})


def test_package_requires_accepted_chain():
    with pytest.raises(RealReadPhaseStatusError):
        _package(chain_closeout_decision=_chain("blocked"))


def test_valid_phase_status_passes_metadata_only():
    status = _status()

    assert validate_real_read_phase_status(status) is status
    assert frozenset(status.__dict__) == frozenset(STATUS_FIELDS)
    assert ALLOWED_STATUS_MODES == frozenset({"phase_status_closeout_only"})
    assert ALLOWED_PHASE_STATES == frozenset({"blocked", "closed_repo_only", "rejected"})


@pytest.mark.parametrize("field_name", ("allow_registry_write", "allow_runtime", "allow_promotion"))
def test_phase_flags_must_be_false(field_name: str):
    with pytest.raises(RealReadPhaseStatusError):
        _status(**{field_name: True})


def test_metadata_only_required():
    with pytest.raises(RealReadPhaseStatusError):
        _package(metadata_only=False)
    with pytest.raises(RealReadPhaseStatusError):
        _status(metadata_only=False)


def test_phase_closes_repo_only():
    decision = evaluate_real_read_phase_status(_status())

    assert decision.decision_status == "closed_repo_only"
    assert decision.phase_status == "repo_only_real_read_readiness_closed"
    assert decision.registry_write_allowed is False
    assert decision.runtime_allowed is False
    assert decision.promotion_allowed is False
    assert decision.reason_or_none is None
    assert validate_real_read_phase_status_decision(decision) is decision
    assert frozenset(decision.__dict__) == frozenset(DECISION_FIELDS)


def test_blocked_and_rejected_phase_states_stay_closed():
    blocked = evaluate_real_read_phase_status(_status(requested_phase_state="blocked"))
    rejected = evaluate_real_read_phase_status(_status(requested_phase_state="rejected"))

    assert blocked.decision_status == "blocked"
    assert blocked.registry_write_allowed is False
    assert blocked.runtime_allowed is False
    assert blocked.promotion_allowed is False
    assert rejected.decision_status == "rejected"
    assert rejected.registry_write_allowed is False
    assert rejected.runtime_allowed is False
    assert rejected.promotion_allowed is False


def test_decision_flags_must_be_false():
    with pytest.raises(RealReadPhaseStatusError):
        RealReadPhaseStatusDecision(
            decision_status="closed_repo_only",
            status_id="status.fixture",
            package_id="package.fixture",
            phase_id="phase.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            phase_status="repo_only_real_read_readiness_closed",
            registry_write_allowed=True,
            runtime_allowed=False,
            promotion_allowed=False,
            reason_or_none=None,
        )
