from pathlib import Path

import pytest

from moex_research.runners.real_read_gate import RealReadGateResult
from moex_research.runners.real_read_review import (
    ALLOWED_DECISION_STATES,
    ALLOWED_EVIDENCE_SCOPES,
    ALLOWED_REVIEW_MODES,
    DECISION_FIELDS,
    EVIDENCE_FIELDS,
    PACKAGE_FIELDS,
    RealReadReviewDecision,
    RealReadReviewError,
    RealReadReviewEvidence,
    RealReadReviewPackage,
    evaluate_real_read_review_package,
    validate_real_read_review_decision,
    validate_real_read_review_evidence,
    validate_real_read_review_package,
)

SOURCE_PATH = Path(__file__).resolve().parents[2] / "src" / "moex_research" / "runners" / "real_read_review.py"


def _gate(status: str = "eligible_for_separate_review") -> RealReadGateResult:
    return RealReadGateResult(
        gate_status=status,
        gate_request_id="ema_3_19.real_read_gate.test",
        dataset_ref_id="canonical.dataset.si.d1.v1",
        instrument_id="Si",
        timeframe="D1",
        sample_pipeline_status="written" if status == "eligible_for_separate_review" else "rejected",
        real_read_allowed=False,
        reason_or_none=None if status == "eligible_for_separate_review" else "sample pipeline not accepted",
    )


def _evidence(**overrides: object) -> RealReadReviewEvidence:
    values: dict[str, object] = {
        "evidence_id": "ema_3_19.real_read_review.evidence.test",
        "sample_pipeline_ref": "pr32.sample_pipeline.accepted",
        "sample_signal_ref": "pr32.sample_signal.accepted",
        "sample_backtest_ref": "pr33.sample_backtest.accepted",
        "full_pipeline_ref": "pr34.full_pipeline.accepted",
        "gate_result_ref": "ema_3_19.real_read_gate.test",
        "evidence_scope": "accepted_sample_pipeline_and_gate_only",
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadReviewEvidence(**values)


def _package(**overrides: object) -> RealReadReviewPackage:
    values: dict[str, object] = {
        "package_id": "ema_3_19.real_read_review.package.test",
        "gate_result": _gate(),
        "evidence": _evidence(),
        "review_mode": "real_read_review_handoff_only",
        "requested_decision_state": "eligible_for_real_read_design",
        "allow_real_read": False,
        "allow_network": False,
        "allow_registry_write": False,
        "allow_runtime": False,
    }
    values.update(overrides)
    return RealReadReviewPackage(**values)


def test_valid_review_evidence_passes_metadata_only():
    evidence = _evidence()

    assert validate_real_read_review_evidence(evidence) is evidence
    assert frozenset(evidence.__dict__) == frozenset(EVIDENCE_FIELDS)
    assert ALLOWED_EVIDENCE_SCOPES == frozenset({"accepted_sample_pipeline_and_gate_only"})


def test_valid_review_package_passes_and_remains_handoff_only():
    package = _package()

    assert validate_real_read_review_package(package) is package
    assert frozenset(package.__dict__) == frozenset(PACKAGE_FIELDS)
    assert ALLOWED_REVIEW_MODES == frozenset({"real_read_review_handoff_only"})
    assert ALLOWED_DECISION_STATES == frozenset({"blocked", "eligible_for_real_read_design", "rejected"})


@pytest.mark.parametrize("field_name", ("allow_real_read", "allow_network", "allow_registry_write", "allow_runtime"))
def test_side_effect_flags_must_be_false(field_name: str):
    with pytest.raises(RealReadReviewError):
        _package(**{field_name: True})


def test_package_must_link_to_gate_result():
    with pytest.raises(RealReadReviewError):
        _package(evidence=_evidence(gate_result_ref="other.gate"))


def test_metadata_only_evidence_is_required():
    with pytest.raises(RealReadReviewError):
        _evidence(metadata_only=False)


def test_review_can_only_make_real_read_design_eligible():
    decision = evaluate_real_read_review_package(_package())

    assert decision.decision_status == "eligible_for_real_read_design"
    assert decision.real_read_allowed is False
    assert decision.reason_or_none is None
    assert validate_real_read_review_decision(decision) is decision
    assert frozenset(decision.__dict__) == frozenset(DECISION_FIELDS)


def test_blocked_gate_blocks_review_decision():
    decision = evaluate_real_read_review_package(_package(gate_result=_gate("blocked")))

    assert decision.decision_status == "blocked"
    assert decision.real_read_allowed is False
    assert decision.reason_or_none == "linked gate is not eligible"


def test_rejected_request_stays_rejected_without_read_authorization():
    decision = evaluate_real_read_review_package(_package(requested_decision_state="rejected"))

    assert decision.decision_status == "rejected"
    assert decision.real_read_allowed is False
    assert decision.reason_or_none == "review package rejected"


def test_decision_cannot_authorize_direct_read():
    with pytest.raises(RealReadReviewError):
        RealReadReviewDecision(
            decision_status="eligible_for_real_read_design",
            package_id="package.fixture",
            gate_request_id="gate.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            gate_status="eligible_for_separate_review",
            real_read_allowed=True,
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
    )
    for marker in forbidden:
        assert marker not in source
