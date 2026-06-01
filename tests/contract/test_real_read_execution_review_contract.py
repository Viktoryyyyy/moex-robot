from pathlib import Path

import pytest

from moex_research.runners.real_read_design import RealReadDesignDecision
from moex_research.runners.real_read_execution_review import (
    ALLOWED_REVIEW_MODES,
    ALLOWED_REVIEW_SCOPES,
    ALLOWED_REVIEW_STATES,
    DECISION_FIELDS,
    PLAN_FIELDS,
    REVIEW_FIELDS,
    RealReadExecutionPlan,
    RealReadExecutionReview,
    RealReadExecutionReviewDecision,
    RealReadExecutionReviewError,
    evaluate_real_read_execution_review,
    validate_real_read_execution_plan,
    validate_real_read_execution_review,
    validate_real_read_execution_review_decision,
)

SOURCE_PATH = Path(__file__).resolve().parents[2] / "src" / "moex_research" / "runners" / "real_read_execution_review.py"


def _design(status: str = "eligible_for_controlled_real_read_execution_review") -> RealReadDesignDecision:
    return RealReadDesignDecision(
        decision_status=status,
        design_id="ema_3_19.real_read.design.test",
        review_package_id="ema_3_19.real_read_review.package.test",
        dataset_ref_id="canonical.dataset.si.d1.v1",
        instrument_id="Si",
        timeframe="D1",
        real_read_allowed=False,
        reason_or_none=None if status == "eligible_for_controlled_real_read_execution_review" else "design blocked",
    )


def _plan(**overrides: object) -> RealReadExecutionPlan:
    values: dict[str, object] = {
        "plan_id": "ema_3_19.real_read.execution_plan.test",
        "design_decision": _design(),
        "owner_runbook_ref": "runbooks.real_read.owner_run.v1",
        "preflight_check_ref": "contracts.preflight.real_read.v1",
        "artifact_manifest_ref": "contracts.artifacts.real_read_manifest.v1",
        "rollback_note_ref": "contracts.rollback.no_state_mutation.v1",
        "review_scope": "owner_run_preflight_metadata_only",
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadExecutionPlan(**values)


def _review(**overrides: object) -> RealReadExecutionReview:
    values: dict[str, object] = {
        "review_id": "ema_3_19.real_read.execution_review.test",
        "execution_plan": _plan(),
        "review_mode": "controlled_real_read_execution_review_only",
        "requested_review_state": "eligible_for_owner_run_real_read",
        "allow_real_read": False,
        "allow_network": False,
        "allow_registry_write": False,
        "allow_runtime": False,
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadExecutionReview(**values)


def test_valid_execution_plan_passes_metadata_only():
    plan = _plan()

    assert validate_real_read_execution_plan(plan) is plan
    assert frozenset(plan.__dict__) == frozenset(PLAN_FIELDS)
    assert ALLOWED_REVIEW_SCOPES == frozenset({"owner_run_preflight_metadata_only"})


def test_plan_requires_eligible_design_decision():
    with pytest.raises(RealReadExecutionReviewError):
        _plan(design_decision=_design("blocked"))


def test_valid_execution_review_passes_without_authorizing_read():
    review = _review()

    assert validate_real_read_execution_review(review) is review
    assert frozenset(review.__dict__) == frozenset(REVIEW_FIELDS)
    assert ALLOWED_REVIEW_MODES == frozenset({"controlled_real_read_execution_review_only"})
    assert ALLOWED_REVIEW_STATES == frozenset({"blocked", "eligible_for_owner_run_real_read", "rejected"})


@pytest.mark.parametrize("field_name", ("allow_real_read", "allow_network", "allow_registry_write", "allow_runtime"))
def test_side_effect_flags_must_be_false(field_name: str):
    with pytest.raises(RealReadExecutionReviewError):
        _review(**{field_name: True})


def test_metadata_only_is_required_on_plan_and_review():
    with pytest.raises(RealReadExecutionReviewError):
        _plan(metadata_only=False)
    with pytest.raises(RealReadExecutionReviewError):
        _review(metadata_only=False)


def test_execution_review_can_request_owner_run_but_not_authorize_read():
    decision = evaluate_real_read_execution_review(_review())

    assert decision.decision_status == "eligible_for_owner_run_real_read"
    assert decision.real_read_allowed is False
    assert decision.owner_run_required is True
    assert decision.reason_or_none is None
    assert validate_real_read_execution_review_decision(decision) is decision
    assert frozenset(decision.__dict__) == frozenset(DECISION_FIELDS)


def test_blocked_and_rejected_review_states_do_not_require_owner_run():
    blocked = evaluate_real_read_execution_review(_review(requested_review_state="blocked"))
    rejected = evaluate_real_read_execution_review(_review(requested_review_state="rejected"))

    assert blocked.decision_status == "blocked"
    assert blocked.real_read_allowed is False
    assert blocked.owner_run_required is False
    assert blocked.reason_or_none == "execution review requested blocked decision"
    assert rejected.decision_status == "rejected"
    assert rejected.real_read_allowed is False
    assert rejected.owner_run_required is False
    assert rejected.reason_or_none == "execution review rejected"


def test_decision_cannot_authorize_direct_read():
    with pytest.raises(RealReadExecutionReviewError):
        RealReadExecutionReviewDecision(
            decision_status="eligible_for_owner_run_real_read",
            review_id="review.fixture",
            design_id="design.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            real_read_allowed=True,
            owner_run_required=True,
            reason_or_none=None,
        )


def test_source_has_no_forbidden_execution_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8")
    forbidden = (
        "load_market_data",
        "open(",
        "read_csv",
        "read_parquet",
        "requests.",
        "http",
        "write_registry",
        "broker",
        "order",
        "live_execution",
        "runtime_execution",
        "data_root",
        "latest",
        "current",
        "autodetect",
        "calculate_ema",
        "backtest_engine",
        "run_backtest",
    )
    for marker in forbidden:
        assert marker not in source
