import json
from pathlib import Path

import pytest

from moex_research.metrics.schemas import MetricsSummary, validate_metrics_summary
from moex_research.publishers.report_artifacts import ReportArtifactSpec, validate_report_artifact_spec
from moex_research.runners.canonical_data_sample_read import CanonicalDataSampleReadRequest
from moex_research.runners.ema_sample_signal import EMASampleSignalRequest
from moex_research.runners.sample_backtest import (
    ALLOWED_MODES,
    REQ_FIELDS,
    RES_FIELDS,
    SampleBacktestError,
    SampleBacktestRequest,
    SampleBacktestResult,
    run_sample_backtest_pipeline,
    validate_sample_backtest_request,
    validate_sample_backtest_result,
)
from tests.fixtures.strategy_testing.approved_canonical_data_refs import (
    EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
)

SOURCE_PATH = Path(__file__).resolve().parents[2] / "src" / "moex_research" / "runners" / "sample_backtest.py"
EXPECTED_REQ = frozenset(
    {
        "request_id",
        "signal_request",
        "backtest_output_path",
        "artifact_manifest_ref",
        "backtest_artifact_ref",
        "metrics_artifact_ref",
        "report_artifact_ref",
        "mode",
    }
)
EXPECTED_RES = frozenset(
    {
        "status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "dataset_ref_id",
        "instrument_id",
        "timeframe",
        "signal_status",
        "backtest_output_path_or_none",
        "backtest_artifact_ref_or_none",
        "row_count_or_none",
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


def _values(tmp_path: Path, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "ema_3_19.sample_backtest.test",
        "signal_request": _signal_request(tmp_path),
        "backtest_output_path": str(tmp_path / "sample_backtest.json"),
        "artifact_manifest_ref": "artifact_manifest.ema_3_19.sample_backtest.test",
        "backtest_artifact_ref": "backtest_artifact.ema_3_19.sample_backtest.test",
        "metrics_artifact_ref": "metrics.ema_3_19.sample_backtest.test",
        "report_artifact_ref": "report.ema_3_19.sample_backtest.test",
        "mode": "canonical_sample_backtest_only",
    }
    values.update(overrides)
    return values


def test_valid_sample_backtest_request_passes(tmp_path: Path):
    request = SampleBacktestRequest(**_values(tmp_path))

    assert validate_sample_backtest_request(request) is request
    assert frozenset(request.__dict__) == EXPECTED_REQ
    assert frozenset(REQ_FIELDS) == EXPECTED_REQ
    assert ALLOWED_MODES == frozenset({"canonical_sample_backtest_only"})


@pytest.mark.parametrize(
    "field_name",
    (
        "request_id",
        "backtest_output_path",
        "artifact_manifest_ref",
        "backtest_artifact_ref",
        "metrics_artifact_ref",
        "report_artifact_ref",
        "mode",
    ),
)
def test_empty_fields_fail_closed(tmp_path: Path, field_name: str):
    with pytest.raises(SampleBacktestError):
        SampleBacktestRequest(**_values(tmp_path, **{field_name: ""}))


def test_invalid_signal_request_and_mode_fail_closed(tmp_path: Path):
    with pytest.raises(SampleBacktestError):
        SampleBacktestRequest(**_values(tmp_path, signal_request="bad"))
    with pytest.raises(SampleBacktestError):
        SampleBacktestRequest(**_values(tmp_path, mode="production_mode"))


@pytest.mark.parametrize("marker", (_late(), _cur(), _auto()))
def test_selection_markers_fail_closed(tmp_path: Path, marker: str):
    with pytest.raises(SampleBacktestError):
        SampleBacktestRequest(**_values(tmp_path, backtest_artifact_ref="backtest." + marker + ".fixture"))


def test_non_temp_output_path_fails_closed(tmp_path: Path):
    with pytest.raises(SampleBacktestError):
        SampleBacktestRequest(**_values(tmp_path, backtest_output_path=str(Path("/not-temp") / "backtest.json")))


def test_sample_backtest_pipeline_writes_result_and_drafts(tmp_path: Path):
    output_path = tmp_path / "sample_backtest.json"
    request = SampleBacktestRequest(**_values(tmp_path, backtest_output_path=str(output_path)))

    result = run_sample_backtest_pipeline(request)

    assert result.status == "written"
    assert result.request_id == request.request_id
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "ema_3_19.strategy_test.canonical_read.v1"
    assert result.dataset_ref_id == "canonical.dataset.si.d1.v1"
    assert result.instrument_id == "Si"
    assert result.timeframe == "D1"
    assert result.signal_status == "written"
    assert result.backtest_output_path_or_none == str(output_path)
    assert result.backtest_artifact_ref_or_none == request.backtest_artifact_ref
    assert result.row_count_or_none == 2
    assert isinstance(result.metrics_summary_or_none, MetricsSummary)
    assert isinstance(result.report_artifact_or_none, ReportArtifactSpec)
    assert result.error_message_or_none is None
    assert validate_sample_backtest_result(result) is result
    assert validate_metrics_summary(result.metrics_summary_or_none) is result.metrics_summary_or_none
    assert validate_report_artifact_spec(result.report_artifact_or_none) is result.report_artifact_or_none
    assert frozenset(result.__dict__) == EXPECTED_RES
    assert frozenset(RES_FIELDS) == EXPECTED_RES

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["artifact_manifest_ref"] == request.artifact_manifest_ref
    assert payload["backtest_artifact_ref"] == request.backtest_artifact_ref
    assert payload["metrics_artifact_ref"] == request.metrics_artifact_ref
    assert payload["report_artifact_ref"] == request.report_artifact_ref
    assert payload["scope"] == "canonical_sample_only"
    assert len(payload["result_rows"]) == 2
    assert [row["synthetic_return"] for row in payload["result_rows"]] == pytest.approx([0.0, 0.0049261084])


def test_sample_backtest_rejects_bad_signal_output_path(tmp_path: Path):
    bad_signal = EMASampleSignalRequest(
        pipeline_request_id="ema_3_19.sample_signal.test",
        sample_read_request=_sample_request(),
        signal_output_path=str(Path("/not-temp") / "sample_signal.json"),
        artifact_manifest_ref="artifact_manifest.ema_3_19.sample_signal.test",
        signal_artifact_ref="signal_artifact.ema_3_19.sample_signal.test",
        pipeline_mode="canonical_sample_signal_only",
    )
    request = SampleBacktestRequest(**_values(tmp_path, signal_request=bad_signal))

    result = run_sample_backtest_pipeline(request)

    assert result.status == "rejected"
    assert result.signal_status == "rejected"
    assert result.row_count_or_none is None
    assert result.metrics_summary_or_none is None
    assert result.report_artifact_or_none is None
    assert result.error_message_or_none


def test_result_has_no_forbidden_fields(tmp_path: Path):
    result = run_sample_backtest_pipeline(SampleBacktestRequest(**_values(tmp_path)))
    forbidden = {
        "registry_entry",
        "promotion_verdict",
        "runtime_authorization",
        "order_request",
        "broker_request",
    }
    assert forbidden.isdisjoint(result.__dict__)


def test_invalid_result_shapes_fail_closed():
    with pytest.raises(SampleBacktestError):
        SampleBacktestResult(
            status="written",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            signal_status="written",
            backtest_output_path_or_none="path.fixture",
            backtest_artifact_ref_or_none="artifact.fixture",
            row_count_or_none=None,
            metrics_summary_or_none=None,
            report_artifact_or_none=None,
            error_message_or_none=None,
        )
    with pytest.raises(SampleBacktestError):
        SampleBacktestResult(
            status="rejected",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            signal_status="rejected",
            backtest_output_path_or_none=None,
            backtest_artifact_ref_or_none=None,
            row_count_or_none=1,
            metrics_summary_or_none=None,
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
