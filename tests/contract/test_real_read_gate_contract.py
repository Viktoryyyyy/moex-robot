from pathlib import Path

import pytest

from moex_research.runners.real_read_gate import (
    ALLOWED_READINESS_MODES,
    REQ_FIELDS,
    RES_FIELDS,
    RealReadGateError,
    RealReadGateRequest,
    RealReadGateResult,
    evaluate_real_read_gate,
    validate_real_read_gate_request,
    validate_real_read_gate_result,
)
from moex_research.runners.sample_pipeline import SamplePipelineResult

SOURCE_PATH = Path(__file__).resolve().parents[2] / "src" / "moex_research" / "runners" / "real_read_gate.py"


def _sample_result(status: str = "written", dataset: str = "canonical.dataset.si.d1.v1") -> SamplePipelineResult:
    return SamplePipelineResult(
        pipeline_status=status,
        pipeline_id="ema_3_19.sample_pipeline.test",
        request_id="ema_3_19.sample_backtest.test",
        strategy_id="ema_3_19",
        strategy_test_id="ema_3_19.strategy_test.canonical_read.v1",
        dataset_ref_id=dataset,
        instrument_id="Si",
        timeframe="D1",
        signal_status="written" if status == "written" else "rejected",
        backtest_status="written" if status == "written" else "rejected",
        backtest_output_path_or_none="/tmp/sample_backtest.json" if status == "written" else None,
        metrics_summary_or_none=None if status != "written" else _metrics_stub(),
        report_artifact_or_none=None if status != "written" else _report_stub(),
        error_message_or_none=None if status == "written" else "sample rejected",
    )


def _metrics_stub():
    from moex_research.metrics.schemas import MetricRecord, MetricsSummary

    return MetricsSummary(
        run_id="ema_3_19.sample_backtest.test",
        strategy_id="ema_3_19",
        test_type="canonical_sample_backtest",
        scope_level="canonical_sample_only",
        result_status="sample_only",
        canonicality_status="not_canonical_result",
        metric_schema_version="metrics.v1",
        metric_records=(
            MetricRecord(
                metric_id="sample_total_return",
                metric_name="Sample total return",
                metric_value=0.0,
                metric_unit="ratio",
                scope="canonical_sample_only",
                gross_or_net="gross",
                producer="sample_backtest",
                consumer="pm_review_only",
            ),
        ),
        artifact_ref="metrics.ema_3_19.sample_backtest.test",
    )


def _report_stub():
    from moex_research.publishers.report_artifacts import ReportArtifactSpec, ReportSectionSpec

    return ReportArtifactSpec(
        report_id="report.ema_3_19.sample_backtest.test",
        run_id="ema_3_19.sample_backtest.test",
        strategy_id="ema_3_19",
        report_schema_version="report.v1",
        artifact_class="repo_relative",
        producer="sample_backtest",
        consumer="pm_review_only",
        format="json",
        required_sections=(ReportSectionSpec(section_id="scope", title="Scope", required=True),),
    )


def _values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "gate_request_id": "ema_3_19.real_read_gate.test",
        "sample_pipeline_result": _sample_result(),
        "dataset_ref_id": "canonical.dataset.si.d1.v1",
        "instrument_id": "Si",
        "timeframe": "D1",
        "readiness_mode": "sample_to_real_read_gate_only",
        "allow_real_read": False,
        "allow_network": False,
        "allow_registry_write": False,
        "allow_runtime": False,
    }
    values.update(overrides)
    return values


def test_valid_gate_request_passes():
    request = RealReadGateRequest(**_values())

    assert validate_real_read_gate_request(request) is request
    assert frozenset(request.__dict__) == frozenset(REQ_FIELDS)
    assert ALLOWED_READINESS_MODES == frozenset({"sample_to_real_read_gate_only"})


@pytest.mark.parametrize("field_name", ("allow_real_read", "allow_network", "allow_registry_write", "allow_runtime"))
def test_side_effect_flags_must_be_false(field_name: str):
    with pytest.raises(RealReadGateError):
        RealReadGateRequest(**_values(**{field_name: True}))


def test_gate_is_eligible_but_real_read_stays_false():
    result = evaluate_real_read_gate(RealReadGateRequest(**_values()))

    assert result.gate_status == "eligible_for_separate_review"
    assert result.real_read_allowed is False
    assert result.reason_or_none is None
    assert validate_real_read_gate_result(result) is result
    assert frozenset(result.__dict__) == frozenset(RES_FIELDS)


def test_gate_blocks_sample_failure_and_dataset_mismatch():
    failed = evaluate_real_read_gate(RealReadGateRequest(**_values(sample_pipeline_result=_sample_result("rejected"))))
    mismatch = evaluate_real_read_gate(RealReadGateRequest(**_values(dataset_ref_id="canonical.dataset.other.v1")))

    assert failed.gate_status == "blocked"
    assert failed.real_read_allowed is False
    assert mismatch.gate_status == "blocked"
    assert mismatch.reason_or_none == "dataset mismatch"


def test_result_cannot_authorize_direct_read():
    with pytest.raises(RealReadGateError):
        RealReadGateResult(
            gate_status="eligible_for_separate_review",
            gate_request_id="gate.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            sample_pipeline_status="written",
            real_read_allowed=True,
            reason_or_none=None,
        )


def test_source_has_no_forbidden_execution_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8")
    forbidden = (
        "load_market_data",
        "write_registry",
        "broker",
        "order",
        "live_execution",
        "runtime_execution",
        "data_root",
        "latest",
        "current",
        "autodetect",
        "d1_" + "ts" + "mom",
    )
    for marker in forbidden:
        assert marker not in source
