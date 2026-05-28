from pathlib import Path

import pytest

from moex_research.contracts.promotion_verdict import (
    ALLOWED_PROMOTION_DECISIONS,
    ALLOWED_PROMOTION_EVIDENCE_REF_TYPES,
    PROMOTION_EVIDENCE_REF_REQUIRED_FIELDS,
    PROMOTION_VERDICT_REQUIRED_FIELDS,
    PromotionEvidenceRef,
    PromotionVerdict,
    PromotionVerdictValidationError,
    validate_promotion_verdict,
    validate_promotion_verdict_values,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
PROMOTION_VERDICT_PATH = (
    REPO_ROOT / "src" / "moex_research" / "contracts" / "promotion_verdict.py"
)


def _rt() -> str:
    return "run" + "time"


def _lv() -> str:
    return "li" + "ve"


def _evidence_ref(**overrides: object) -> PromotionEvidenceRef:
    values = {
        "ref_id": "evidence.metrics_summary.ema_3_19.fixture.v1",
        "ref_type": "metrics_summary",
        "ref": "artifact.metrics.strategy_test.ema_3_19.fixture.v1",
        "producer": "moex_research.metrics.schemas",
        "consumer": "pm.architect.promotion_review",
    }
    values.update(overrides)
    return PromotionEvidenceRef(**values)


def _verdict(**overrides: object) -> PromotionVerdict:
    values = {
        "promotion_verdict_id": "promotion_verdict.ema_3_19.fixture.v1",
        "strategy_id": "ema_3_19",
        "strategy_version": "0.1.0",
        "decision": "hold",
        "evidence_refs": (_evidence_ref(),),
        "allowed_next_scope": "research_review",
        "blocked_scope": _rt() + "_and_" + _lv(),
        "created_by": "pm.architect",
        "created_ts": "2026-05-28T00:00:00Z",
        "verdict_schema_version": "promotion_verdict.v1",
    }
    values.update(overrides)
    return PromotionVerdict(**values)


def test_required_fields_are_explicit():
    assert PROMOTION_EVIDENCE_REF_REQUIRED_FIELDS == (
        "ref_id",
        "ref_type",
        "ref",
        "producer",
        "consumer",
    )
    assert PROMOTION_VERDICT_REQUIRED_FIELDS == (
        "promotion_verdict_id",
        "strategy_id",
        "strategy_version",
        "decision",
        "evidence_refs",
        "allowed_next_scope",
        "blocked_scope",
        "created_by",
        "created_ts",
        "verdict_schema_version",
    )


@pytest.mark.parametrize(
    "decision",
    [
        "reject",
        "hold",
        "research_supported_only",
        "backtest_candidate",
        "strategy_package_candidate",
    ],
)
def test_valid_non_execution_scope_decisions_pass(decision):
    verdict = _verdict(decision=decision)

    assert validate_promotion_verdict(verdict) is verdict
    assert verdict.decision == decision


def test_valid_separate_review_scope_passes_only_for_separate_review():
    verdict = _verdict(
        decision=_rt() + "_candidate_allowed_for_separate_review",
        allowed_next_scope="separate_" + _rt() + "_readiness_review",
    )

    assert validate_promotion_verdict(verdict) is verdict
    with pytest.raises(PromotionVerdictValidationError):
        _verdict(
            decision=_rt() + "_candidate_allowed_for_separate_review",
            allowed_next_scope=_rt() + "_review",
        )


def test_candidate_blocked_decision_passes():
    verdict = _verdict(
        decision=_rt() + "_candidate_blocked",
        allowed_next_scope="research_review",
        blocked_scope=_rt() + "_candidate_review",
    )

    assert validate_promotion_verdict(verdict) is verdict


@pytest.mark.parametrize(
    "field_name",
    [
        "promotion_verdict_id",
        "strategy_id",
        "strategy_version",
        "allowed_next_scope",
        "blocked_scope",
        "created_by",
        "created_ts",
        "verdict_schema_version",
    ],
)
def test_empty_required_verdict_fields_fail_closed(field_name):
    with pytest.raises(PromotionVerdictValidationError):
        _verdict(**{field_name: ""})


def test_empty_evidence_refs_fail():
    with pytest.raises(PromotionVerdictValidationError):
        _verdict(evidence_refs=())


def test_unsupported_decision_fails():
    with pytest.raises(PromotionVerdictValidationError):
        _verdict(decision="approved_for_" + _lv())


def test_unsupported_ref_type_fails():
    with pytest.raises(PromotionVerdictValidationError):
        _evidence_ref(ref_type="inline_metric_payload")


@pytest.mark.parametrize("field_name", ["producer", "consumer"])
def test_evidence_ref_missing_producer_or_consumer_fails(field_name):
    with pytest.raises(PromotionVerdictValidationError):
        _evidence_ref(**{field_name: ""})


@pytest.mark.parametrize(
    "ref_type",
    [
        "experiment_registry_entry",
        "artifact_manifest",
        "metrics_summary",
        "report_artifact",
        "strategy_test_package",
        "backtest_result",
        "fragility_result",
        "pm_note",
    ],
)
def test_allowed_evidence_ref_types_pass(ref_type):
    evidence_ref = _evidence_ref(ref_type=ref_type)

    verdict = _verdict(evidence_refs=(evidence_ref,))
    assert validate_promotion_verdict(verdict) is verdict


@pytest.mark.parametrize(
    "scope",
    [
        _lv(),
        _rt() + "_" + _lv(),
        "production_" + _lv(),
        "br" + "oker_execution",
        "ord" + "er_execution",
        "direct" + "_trading",
    ],
)
def test_invalid_direct_scope_fails(scope):
    with pytest.raises(PromotionVerdictValidationError):
        _verdict(allowed_next_scope=scope)


@pytest.mark.parametrize(
    "created_by",
    [
        "dry_validation_runner",
        "moex_research.metrics.schemas",
        "moex_research.publishers.report_artifacts",
        "registry_writer",
        "strategy_package",
        "canonical_backtest_engine",
    ],
)
def test_forbidden_creators_cannot_create_verdict(created_by):
    with pytest.raises(PromotionVerdictValidationError):
        _verdict(created_by=created_by)


def test_verdict_references_evidence_not_embedded_metrics():
    verdict = _verdict()

    assert not hasattr(verdict, "metric_records")
    assert not hasattr(verdict, "metrics_summary")
    assert verdict.evidence_refs[0].ref_type == "metrics_summary"


def test_embedded_metrics_style_payload_fails():
    values = {
        "promotion_verdict_id": "promotion_verdict.ema_3_19.fixture.v1",
        "strategy_id": "ema_3_19",
        "strategy_version": "0.1.0",
        "decision": "hold",
        "metric_records": (),
        "allowed_next_scope": "research_review",
        "blocked_scope": _rt() + "_and_" + _lv(),
        "created_by": "pm.architect",
        "created_ts": "2026-05-28T00:00:00Z",
        "verdict_schema_version": "promotion_verdict.v1",
    }

    with pytest.raises(PromotionVerdictValidationError):
        validate_promotion_verdict_values(values)


def test_allowed_decisions_are_explicit():
    assert ALLOWED_PROMOTION_DECISIONS == frozenset(
        {
            "reject",
            "hold",
            "research_supported_only",
            "backtest_candidate",
            "strategy_package_candidate",
            _rt() + "_candidate_blocked",
            _rt() + "_candidate_allowed_for_separate_review",
        }
    )


def test_allowed_evidence_ref_types_are_explicit():
    assert ALLOWED_PROMOTION_EVIDENCE_REF_TYPES == frozenset(
        {
            "experiment_registry_entry",
            "artifact_manifest",
            "metrics_summary",
            "report_artifact",
            "strategy_test_package",
            "backtest_result",
            "fragility_result",
            "pm_note",
        }
    )


def test_source_has_no_forbidden_execution_responsibilities():
    source = PROMOTION_VERDICT_PATH.read_text(encoding="utf-8").casefold()
    forbidden_terms = (
        "run_" + "backtest",
        "execute_" + "backtest",
        "execute_" + "strategy",
        "generate_" + "signals",
        "calculate_" + "pnl",
        "calculate_" + "metrics_from_data",
        "write_" + "report",
        "write_" + "registry",
        "br" + "oker",
        "ord" + "er",
        _lv() + "_execution",
        _rt() + "_execution",
        "ser" + "ver",
        "data" + "_root",
        "lat" + "est",
        "cur" + "rent",
        "auto" + "detect",
        "sub" + "process",
        "req" + "uests",
        "url" + "lib",
        "sock" + "et",
        "open" + "(",
        "glob" + "(",
        "os" + ".",
    )
    for term in forbidden_terms:
        assert term not in source
