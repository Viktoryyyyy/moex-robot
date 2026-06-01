import json
from pathlib import Path

import pytest

from moex_research.runners.canonical_data_sample_read import CanonicalDataSampleReadRequest
from moex_research.runners.ema_sample_signal import (
    ALLOWED_PIPELINE_MODES,
    EMASampleSignalError,
    EMASampleSignalRequest,
    EMASampleSignalResult,
    REQ_FIELDS,
    RES_FIELDS,
    run_ema_sample_signal_pipeline,
    validate_ema_sample_signal_request,
    validate_ema_sample_signal_result,
)
from tests.fixtures.strategy_testing.approved_canonical_data_refs import (
    EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
)

SOURCE_PATH = Path(__file__).resolve().parents[2] / "src" / "moex_research" / "runners" / "ema_sample_signal.py"
EXPECTED_REQ = frozenset(
    {
        "pipeline_request_id",
        "sample_read_request",
        "signal_output_path",
        "artifact_manifest_ref",
        "signal_artifact_ref",
        "pipeline_mode",
    }
)
EXPECTED_RES = frozenset(
    {
        "pipeline_status",
        "pipeline_request_id",
        "sample_request_id",
        "strategy_id",
        "strategy_test_id",
        "dataset_ref_id",
        "instrument_id",
        "timeframe",
        "sample_status",
        "signal_status",
        "signal_artifact_id_or_none",
        "signal_output_path_or_none",
        "row_count_or_none",
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


def _values(tmp_path: Path, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "pipeline_request_id": "ema_3_19.sample_signal.test",
        "sample_read_request": _sample_request(),
        "signal_output_path": str(tmp_path / "sample_signal.json"),
        "artifact_manifest_ref": "artifact_manifest.ema_3_19.sample_signal.test",
        "signal_artifact_ref": "signal_artifact.ema_3_19.sample_signal.test",
        "pipeline_mode": "canonical_sample_signal_only",
    }
    values.update(overrides)
    return values


def test_valid_sample_signal_request_passes(tmp_path: Path):
    request = EMASampleSignalRequest(**_values(tmp_path))

    assert validate_ema_sample_signal_request(request) is request
    assert frozenset(request.__dict__) == EXPECTED_REQ
    assert frozenset(REQ_FIELDS) == EXPECTED_REQ
    assert ALLOWED_PIPELINE_MODES == frozenset({"canonical_sample_signal_only"})


@pytest.mark.parametrize(
    "field_name",
    ("pipeline_request_id", "signal_output_path", "artifact_manifest_ref", "signal_artifact_ref", "pipeline_mode"),
)
def test_empty_fields_fail_closed(tmp_path: Path, field_name: str):
    with pytest.raises(EMASampleSignalError):
        EMASampleSignalRequest(**_values(tmp_path, **{field_name: ""}))


def test_invalid_sample_request_and_mode_fail_closed(tmp_path: Path):
    with pytest.raises(EMASampleSignalError):
        EMASampleSignalRequest(**_values(tmp_path, sample_read_request="bad"))
    with pytest.raises(EMASampleSignalError):
        EMASampleSignalRequest(**_values(tmp_path, pipeline_mode="production_mode"))


@pytest.mark.parametrize("marker", (_late(), _cur(), _auto()))
def test_selection_markers_fail_closed(tmp_path: Path, marker: str):
    with pytest.raises(EMASampleSignalError):
        EMASampleSignalRequest(**_values(tmp_path, signal_artifact_ref="signal." + marker + ".fixture"))


def test_sample_signal_pipeline_writes_temp_signal_artifact(tmp_path: Path):
    output_path = tmp_path / "sample_signal.json"
    request = EMASampleSignalRequest(**_values(tmp_path, signal_output_path=str(output_path)))

    result = run_ema_sample_signal_pipeline(request)

    assert result.pipeline_status == "written"
    assert result.sample_status == "validated"
    assert result.signal_status == "written"
    assert result.pipeline_request_id == request.pipeline_request_id
    assert result.sample_request_id == request.sample_read_request.sample_request_id
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "ema_3_19.strategy_test.canonical_read.v1"
    assert result.dataset_ref_id == "canonical.dataset.si.d1.v1"
    assert result.instrument_id == "Si"
    assert result.timeframe == "D1"
    assert result.signal_artifact_id_or_none == request.signal_artifact_ref
    assert result.signal_output_path_or_none == str(output_path)
    assert result.row_count_or_none == 3
    assert result.error_message_or_none is None
    assert validate_ema_sample_signal_result(result) is result
    assert frozenset(result.__dict__) == EXPECTED_RES
    assert frozenset(RES_FIELDS) == EXPECTED_RES

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["artifact_id"] == request.signal_artifact_ref
    assert payload["artifact_role"] == "synthetic_signal_table"
    assert payload["artifact_class"] == "temporary_test_path"
    assert [row["signal_value"] for row in payload["rows"]] == [0, 1, 1]


def test_non_temp_output_rejected_by_writer(tmp_path: Path):
    request = EMASampleSignalRequest(**_values(tmp_path, signal_output_path=str(Path("/not-temp") / "sample_signal.json")))

    result = run_ema_sample_signal_pipeline(request)

    assert result.pipeline_status == "rejected"
    assert result.sample_status == "rejected"
    assert result.signal_status == "rejected"
    assert result.row_count_or_none is None
    assert result.error_message_or_none


def test_result_has_no_forbidden_fields(tmp_path: Path):
    result = run_ema_sample_signal_pipeline(EMASampleSignalRequest(**_values(tmp_path)))
    forbidden = {
        "backtest_result",
        "metrics",
        "registry_entry",
        "promotion_verdict",
        "runtime_authorization",
    }
    assert forbidden.isdisjoint(result.__dict__)


def test_invalid_result_shapes_fail_closed():
    with pytest.raises(EMASampleSignalError):
        EMASampleSignalResult(
            pipeline_status="written",
            pipeline_request_id="request.fixture",
            sample_request_id="sample.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            sample_status="validated",
            signal_status="written",
            signal_artifact_id_or_none="artifact.fixture",
            signal_output_path_or_none="path.fixture",
            row_count_or_none=None,
            error_message_or_none=None,
        )
    with pytest.raises(EMASampleSignalError):
        EMASampleSignalResult(
            pipeline_status="rejected",
            pipeline_request_id="request.fixture",
            sample_request_id="sample.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            sample_status="rejected",
            signal_status="rejected",
            signal_artifact_id_or_none=None,
            signal_output_path_or_none=None,
            row_count_or_none=1,
            error_message_or_none="bad",
        )


def test_source_has_no_forbidden_production_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8")
    forbidden = (
        "load_market_data",
        "run_backtest",
        "execute_backtest",
        "run_research",
        "calculate_metrics",
        "write_registry",
        "create_promotion_verdict",
        "broker",
        "order",
        "live_execution",
        "runtime_execution",
        "server",
        "latest",
        "current",
        "autodetect",
        _legacy(),
    )
    for marker in forbidden:
        assert marker not in source
