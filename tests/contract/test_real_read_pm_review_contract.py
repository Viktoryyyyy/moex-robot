import pytest

from moex_research.runners.real_read_manual_result import RealReadManualResultIntakeDecision
from moex_research.runners.real_read_pm_review import (
    ALLOWED_PM_REVIEW_SCOPES,
    ALLOWED_REVIEW_MODES,
    ALLOWED_REVIEW_STATES,
    DECISION_FIELDS,
    PACKAGE_FIELDS,
    REVIEW_FIELDS,
    RealReadPMReview,
    RealReadPMReviewDecision,
    RealReadPMReviewError,
    RealReadPMReviewPackage,
    evaluate_real_read_pm_review,
    validate_real_read_pm_review,
    validate_real_read_pm_review_decision,
    validate_real_read_pm_review_package,
)


def _intake(status: str = "accepted_for_pm_review") -> RealReadManualResultIntakeDecision:
    return RealReadManualResultIntakeDecision(
        decision_status=status,
        intake_id="real_read.manual_intake.test",
        result_id="real_read.manual_result.test",
        dataset_ref_id="canonical.dataset.si.d1.v1",
        instrument_id="Si",
        timeframe="D1",
        registry_write_allowed=False,
        runtime_allowed=False,
        promotion_allowed=False,
        reason_or_none=None if status == "accepted_for_pm_review" else "intake blocked",
    )


def _package(**overrides: object) -> RealReadPMReviewPackage:
    values: dict[str, object] = {
        "package_id": "real_read.pm_review.package.test",
        "manual_result_intake_decision": _intake(),
        "evidence_review_ref": "evidence_review.real_read.test",
        "quality_review_ref": "quality_review.real_read.test",
        "lineage_review_ref": "lineage_review.real_read.test",
        "pm_review_scope": "manual_result_pm_review_metadata_only",
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadPMReviewPackage(**values)


def _review(**overrides: object) -> RealReadPMReview:
    values: dict[str, object] = {
        "pm_review_id": "real_read.pm_review.test",
        "review_package": _package(),
        "review_mode": "pm_real_read_evidence_review_only",
        "requested_review_state": "accepted_as_real_read_evidence",
        "allow_registry_write": False,
        "allow_runtime": False,
        "allow_promotion": False,
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadPMReview(**values)


def test_valid_package_passes_metadata_only():
    package = _package()

    assert validate_real_read_pm_review_package(package) is package
    assert frozenset(package.__dict__) == frozenset(PACKAGE_FIELDS)
    assert ALLOWED_PM_REVIEW_SCOPES == frozenset({"manual_result_pm_review_metadata_only"})


def test_package_requires_accepted_intake():
    with pytest.raises(RealReadPMReviewError):
        _package(manual_result_intake_decision=_intake("blocked"))


def test_valid_review_passes_metadata_only():
    review = _review()

    assert validate_real_read_pm_review(review) is review
    assert frozenset(review.__dict__) == frozenset(REVIEW_FIELDS)
    assert ALLOWED_REVIEW_MODES == frozenset({"pm_real_read_evidence_review_only"})
    assert ALLOWED_REVIEW_STATES == frozenset({"blocked", "accepted_as_real_read_evidence", "rejected"})


@pytest.mark.parametrize("field_name", ("allow_registry_write", "allow_runtime", "allow_promotion"))
def test_review_flags_must_be_false(field_name: str):
    with pytest.raises(RealReadPMReviewError):
        _review(**{field_name: True})


def test_metadata_only_required():
    with pytest.raises(RealReadPMReviewError):
        _package(metadata_only=False)
    with pytest.raises(RealReadPMReviewError):
        _review(metadata_only=False)


def test_review_accepts_evidence_for_pm_only():
    decision = evaluate_real_read_pm_review(_review())

    assert decision.decision_status == "accepted_as_real_read_evidence"
    assert decision.evidence_status == "real_read_evidence_accepted"
    assert decision.registry_write_allowed is False
    assert decision.runtime_allowed is False
    assert decision.promotion_allowed is False
    assert decision.reason_or_none is None
    assert validate_real_read_pm_review_decision(decision) is decision
    assert frozenset(decision.__dict__) == frozenset(DECISION_FIELDS)


def test_blocked_and_rejected_stay_closed():
    blocked = evaluate_real_read_pm_review(_review(requested_review_state="blocked"))
    rejected = evaluate_real_read_pm_review(_review(requested_review_state="rejected"))

    assert blocked.decision_status == "blocked"
    assert blocked.registry_write_allowed is False
    assert blocked.runtime_allowed is False
    assert blocked.promotion_allowed is False
    assert rejected.decision_status == "rejected"
    assert rejected.registry_write_allowed is False
    assert rejected.runtime_allowed is False
    assert rejected.promotion_allowed is False


def test_decision_flags_must_be_false():
    with pytest.raises(RealReadPMReviewError):
        RealReadPMReviewDecision(
            decision_status="accepted_as_real_read_evidence",
            pm_review_id="review.fixture",
            package_id="package.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            evidence_status="real_read_evidence_accepted",
            registry_write_allowed=True,
            runtime_allowed=False,
            promotion_allowed=False,
            reason_or_none=None,
        )
