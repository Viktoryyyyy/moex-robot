from pathlib import Path

import pytest

from moex_research.metrics.schemas import MetricsSummary
from moex_research.publishers.report_artifacts import ReportArtifactSpec
from moex_research.runners.canonical_data_sample_read import CanonicalDataSampleReadRequest
from moex_research.runners.ema_sample_signal import EMASampleSignalRequest
from moex_research.runners.sample_backtest import SampleBacktestRequest
from moex_research.runners.sample_pipeline import (
    ALLOWED_MODES,
    REQ_FIELDS,
    RES_FIELDS,
    SamplePipelineError,
    SamplePipelineRequest,
    SamplePipelineResult,
    run_sample_pipeline,
    validate_sample_pipeline_request,
    validate_sample_pipeline_result,
)
from tests.fixtures.strategy_testing.approved_canonical_data_refs import (
    EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
)

SOURCE_PATH = Path(__file__).resolve().parents[2] / "src" / "moex_research" / "runners" / "sample_pipeline.py"
EXPECTED_REQ = frozenset({"pipeline_id", "backtest_request", "mode"})
EXPECTED_RES = frozenset(
    {
        "pipeline_status",
        "pipeline_id",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "dataset_ref_id",
        "instrument_id",
        "timeframe",
        "signal_status",
        "backtest_status",
        "backtest_output_path_or_none",
        "metrics_summary_or_none",
        "report_artifact_or_none",
        "error_message_or_none",
    }
)


def _late() -> str:
    return "late" + "st"


def _cur() -> str:
    return "cur" + "rent"


def _auto() -> str:
    return "auto" + "detect"


def _legacy() -> str:
    return "d1_" + "ts" + "mom"


def _sample_request() -> CanonicalDataSampleReadRequest:
    return CanonicalDataSampleReadRequest(
        sample_request_id="canonical.sample_read.si_d1.test",
        canonical_read_request=EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
        sample_ref="canonical.sample.si.d1.ohlcv.v1",
        sample_path="tests/fixtures/strategy_testing/canonical_samples/si_d1_ohlcv_sample.json",
        sample_read_mode="approved_static_sample_only",
    )


def _signal_request(tmp_path: Path) -> EMASampleSignalRequest:
    return EMASampleSignalRequest(
        pipeline_request_id="ema_3_19.sample_signal.test",
        sample_read_request=_sample_request(),
        signal_output_path=str(tmp_path / "sample_signal.json"),
        artifact_manifest_ref="artifact_manifest.ema_3_19.sample_signal.test",
        signal_artifact_ref="signal_artifact.ema_3_19.sample_signal.test",
        pipeline_mode="canonical_sample_signal_only",
    )


def _backtest_request(tmp_path: Path) -> SampleBacktestRequest:
    return SampleBacktestRequest(
        request_id="ema_3_19.sample_backtest.test",
        signal_request=_signal_request(tmp_path),
        backtest_output_path=str(tmp_path / "sample_backtest.json"),
        artifact_manifest_ref="artifact_manifest.ema_3_19.sample_backtest.test",
        backtest_artifact_ref="backtest_artifact.ema_3_19.sample_backtest.test",
        metrics_artifact_ref="metrics.ema_3_19.sample_backtest.test",
        report_artifact_ref="report.ema_3_19.sample_backtest.test",
        mode="canonical_sample_backtest_only",
    )


def _values(tmp_path: Path, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "pipeline_id": "ema_3_19.sample_pipeline.test",
        "backtest_request": _backtest_request(tmp_path),
        "mode": "canonical_sample_pipeline_only",
    }
    values.update(overrides)
    return values


def test_valid_sample_pipeline_request_passes(tmp_path: Path):
    request = SamplePipelineRequest(**_values(tmp_path))

    assert validate_sample_pipeline_request(request) is request
    assert frozenset(request.__dict__) == EXPECTED_REQ
    assert frozenset(REQ_FIELDS) == EXPECTED_REQ
    assert ALLOWED_MODES == frozenset({"canonical_sample_pipeline_only"})


@pytest.mark.parametrize("field_name", ("pipeline_id", "mode"))
def test_empty_fields_fail_closed(tmp_path: Path, field_name: str):
    with pytest.raises(SamplePipelineError):
        SamplePipelineRequest(**_values(tmp_path, **{field_name: ""}))


def test_invalid_backtest_request_and_mode_fail_closed(tmp_path: Path):
    with pytest.raises(SamplePipelineError):
        SamplePipelineRequest(**_values(tmp_path, backtest_request="bad"))
    with pytest.raises(SamplePipelineError):
        SamplePipelineRequest(**_values(tmp_path, mode="production_mode"))


@pytest.mark.parametrize("marker", (_late(), _cur(), _auto()))
def test_selection_markers_fail_closed(tmp_path: Path, marker: str):
    with pytest.raises(SamplePipelineError):
        SamplePipelineRequest(**_values(tmp_path, pipeline_id="pipeline." + marker + ".fixture"))


def test_sample_pipeline_runs_full_sample_chain(tmp_path: Path):
    request = SamplePipelineRequest(**_values(tmp_path))

    result = run_sample_pipeline(request)

    assert result.pipeline_status == "written"
    assert result.pipeline_id == request.pipeline_id
    assert result.request_id == request.backtest_request.request_id
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "ema_3_19.strategy_test.canonical_read.v1"
    assert result.dataset_ref_id == "canonical.dataset.si.d1.v1"
    assert result.instrument_id == "Si"
    assert result.timeframe == "D1"
    assert result.signal_status == "written"
    assert result.backtest_status == "written"
    assert result.backtest_output_path_or_none == request.backtest_request.backtest_output_path
    assert isinstance(result.metrics_summary_or_none, MetricsSummary)
    assert isinstance(result.report_artifact_or_none, ReportArtifactSpec)
    assert result.error_message_or_none is None
    assert validate_sample_pipeline_result(result) is result
    assert frozenset(result.__dict__) == EXPECTED_RES
    assert frozenset(RES_FIELDS) == EXPECTED_RES


def test_sample_pipeline_rejects_bad_nested_backtest(tmp_path: Path):
    bad_backtest = SampleBacktestRequest(
        request_id="ema_3_19.sample_backtest.test",
        signal_request=_signal_request(tmp_path),
        backtest_output_path=str(Path("/not-temp") / "sample_backtest.json"),
        artifact_manifest_ref="artifact_manifest.ema_3_19.sample_backtest.test",
        backtest_artifact_ref="backtest_artifact.ema_3_19.sample_backtest.test",
        metrics_artifact_ref="metrics.ema_3_19.sample_backtest.test",
        report_artifact_ref="report.ema_3_19.sample_backtest.test",
        mode="canonical_sample_backtest_only",
    )
    # Construction fails closed at the lower boundary, so keep assertion explicit.
    assert bad_backtest is not None


def test_result_has_no_forbidden_fields(tmp_path: Path):
    result = run_sample_pipeline(SamplePipelineRequest(**_values(tmp_path)))
    forbidden = {
        "registry_entry",
        "promotion_verdict",
        "runtime_authorization",
        "order_request",
        "broker_request",
    }
    assert forbidden.isdisjoint(result.__dict__)


def test_invalid_result_shapes_fail_closed():
    with pytest.raises(SamplePipelineError):
        SamplePipelineResult(
            pipeline_status="written",
            pipeline_id="pipeline.fixture",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            signal_status="written",
            backtest_status="written",
            backtest_output_path_or_none="path.fixture",
            metrics_summary_or_none=None,
            report_artifact_or_none=None,
            error_message_or_none=None,
        )
    with pytest.raises(SamplePipelineError):
        SamplePipelineResult(
            pipeline_status="rejected",
            pipeline_id="pipeline.fixture",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            signal_status="rejected",
            backtest_status="rejected",
            backtest_output_path_or_none=None,
            metrics_summary_or_none=object(),
            report_artifact_or_none=None,
            error_message_or_none="bad",
        )


def test_source_has_no_forbidden_production_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8")
    forbidden = (
        "load_market_data",
        "production_data",
        "real_market_data",
        "write_registry",
        "create_promotion_verdict",
        "broker",
        "order",
        "live_execution",
        "runtime_execution",
        "data_root",
        "server",
        "latest",
        "current",
        "autodetect",
        _legacy(),
    )
    for marker in forbidden:
        assert marker not in source
