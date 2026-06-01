import pytest

from moex_research.runners.real_read_chain import (
    ALLOWED_CHAIN_SCOPES,
    ALLOWED_CLOSEOUT_MODES,
    ALLOWED_CLOSEOUT_STATES,
    CHAIN_FIELDS,
    CLOSEOUT_FIELDS,
    DECISION_FIELDS,
    RealReadChain,
    RealReadChainCloseout,
    RealReadChainCloseoutDecision,
    RealReadChainError,
    evaluate_real_read_chain_closeout,
    validate_real_read_chain,
    validate_real_read_chain_closeout,
    validate_real_read_chain_closeout_decision,
)
from moex_research.runners.real_read_pm_review import RealReadPMReviewDecision


def _pm(status: str = "accepted_as_real_read_evidence") -> RealReadPMReviewDecision:
    return RealReadPMReviewDecision(
        decision_status=status,
        pm_review_id="real_read.pm_review.test",
        package_id="real_read.pm_review.package.test",
        dataset_ref_id="canonical.dataset.si.d1.v1",
        instrument_id="Si",
        timeframe="D1",
        evidence_status="real_read_evidence_accepted" if status == "accepted_as_real_read_evidence" else "blocked",
        registry_write_allowed=False,
        runtime_allowed=False,
        promotion_allowed=False,
        reason_or_none=None if status == "accepted_as_real_read_evidence" else "pm blocked",
    )


def _chain(**overrides: object) -> RealReadChain:
    values: dict[str, object] = {
        "chain_id": "real_read.chain.test",
        "pm_review_decision": _pm(),
        "gate_commit_ref": "abf14b54f4fe1e5acfe7762979007d66533e2dc8",
        "review_commit_ref": "54f15056dfd098e190c42b9160ebff515cdc1bb8",
        "design_commit_ref": "454f2a50706efa573863a4c9b90167209efb59dc",
        "execution_review_commit_ref": "25d3a996b4b9568241e26d779a14b84a39cd4e8d",
        "handoff_commit_ref": "7dfb9467b20a250b38d59b4d419d43408b66a8d1",
        "manual_intake_commit_ref": "3b226e6bbf540cc3d4bf6632ae9cbafa1935391d",
        "pm_review_commit_ref": "b2fe2054038a0486d17fa745016b7a8e05d789ab",
        "chain_scope": "real_read_review_chain_metadata_only",
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadChain(**values)


def _closeout(**overrides: object) -> RealReadChainCloseout:
    values: dict[str, object] = {
        "closeout_id": "real_read.chain.closeout.test",
        "evidence_chain": _chain(),
        "closeout_mode": "real_read_chain_closeout_only",
        "requested_closeout_state": "accepted_chain",
        "allow_registry_write": False,
        "allow_runtime": False,
        "allow_promotion": False,
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadChainCloseout(**values)


def test_valid_chain_passes_metadata_only():
    chain = _chain()

    assert validate_real_read_chain(chain) is chain
    assert frozenset(chain.__dict__) == frozenset(CHAIN_FIELDS)
    assert ALLOWED_CHAIN_SCOPES == frozenset({"real_read_review_chain_metadata_only"})


def test_chain_requires_pm_accepted_evidence():
    with pytest.raises(RealReadChainError):
        _chain(pm_review_decision=_pm("blocked"))


def test_valid_closeout_passes_metadata_only():
    closeout = _closeout()

    assert validate_real_read_chain_closeout(closeout) is closeout
    assert frozenset(closeout.__dict__) == frozenset(CLOSEOUT_FIELDS)
    assert ALLOWED_CLOSEOUT_MODES == frozenset({"real_read_chain_closeout_only"})
    assert ALLOWED_CLOSEOUT_STATES == frozenset({"blocked", "accepted_chain", "rejected"})


@pytest.mark.parametrize("field_name", ("allow_registry_write", "allow_runtime", "allow_promotion"))
def test_closeout_flags_must_be_false(field_name: str):
    with pytest.raises(RealReadChainError):
        _closeout(**{field_name: True})


def test_metadata_only_required():
    with pytest.raises(RealReadChainError):
        _chain(metadata_only=False)
    with pytest.raises(RealReadChainError):
        _closeout(metadata_only=False)


def test_closeout_accepts_chain_only():
    decision = evaluate_real_read_chain_closeout(_closeout())

    assert decision.decision_status == "accepted_chain"
    assert decision.chain_status == "real_read_review_chain_accepted"
    assert decision.registry_write_allowed is False
    assert decision.runtime_allowed is False
    assert decision.promotion_allowed is False
    assert decision.reason_or_none is None
    assert validate_real_read_chain_closeout_decision(decision) is decision
    assert frozenset(decision.__dict__) == frozenset(DECISION_FIELDS)


def test_blocked_and_rejected_closeout_stay_closed():
    blocked = evaluate_real_read_chain_closeout(_closeout(requested_closeout_state="blocked"))
    rejected = evaluate_real_read_chain_closeout(_closeout(requested_closeout_state="rejected"))

    assert blocked.decision_status == "blocked"
    assert blocked.registry_write_allowed is False
    assert blocked.runtime_allowed is False
    assert blocked.promotion_allowed is False
    assert rejected.decision_status == "rejected"
    assert rejected.registry_write_allowed is False
    assert rejected.runtime_allowed is False
    assert rejected.promotion_allowed is False


def test_decision_flags_must_be_false():
    with pytest.raises(RealReadChainError):
        RealReadChainCloseoutDecision(
            decision_status="accepted_chain",
            closeout_id="closeout.fixture",
            chain_id="chain.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            chain_status="real_read_review_chain_accepted",
            registry_write_allowed=True,
            runtime_allowed=False,
            promotion_allowed=False,
            reason_or_none=None,
        )
