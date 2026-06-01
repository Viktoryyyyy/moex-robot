from pathlib import Path

import pytest

from moex_research.runners.canonical_data_read import CanonicalDataReadRequest
from moex_research.runners.canonical_data_sample_read import (
    ALLOWED_SAMPLE_READ_MODES,
    APPROVED_SAMPLE_PATHS,
    APPROVED_SAMPLE_REFS,
    CANONICAL_DATA_SAMPLE_READ_REQUEST_FIELDS,
    CANONICAL_DATA_SAMPLE_READ_RESULT_FIELDS,
    REQUIRED_OHLCV_COLUMNS,
    CanonicalDataSampleReadRequest,
    CanonicalDataSampleReadResult,
    CanonicalDataSampleReadValidationError,
    read_canonical_data_sample_dry_run,
    validate_canonical_data_sample_read_request,
    validate_canonical_data_sample_read_result,
)
from tests.fixtures.strategy_testing.approved_canonical_data_refs import (
    EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "canonical_data_sample_read.py"
SAMPLE_PATH = REPO_ROOT / "tests" / "fixtures" / "strategy_testing" / "canonical_samples" / "si_d1_ohlcv_sample.json"
EXPECTED_REQUEST_FIELDS = frozenset(
    {
        "sample_request_id",
        "canonical_read_request",
        "sample_ref",
        "sample_path",
        "sample_read_mode",
    }
)
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "sample_status",
        "sample_request_id",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "dataset_ref_id",
        "instrument_id",
        "timeframe",
        "schema_ref",
        "row_count_or_none",
        "schema_status_or_none",
        "error_message_or_none",
    }
)


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _legacy_strategy_marker() -> str:
    return "d1_" + "ts" + "mom"


def _host_marker() -> str:
    return "ser" + "ver"


def _scheduler_marker() -> str:
    return "run" + "time"


def _market_access_marker() -> str:
    return "li" + "ve"


def _network_marker() -> str:
    return "net" + "work"


def _request_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "sample_request_id": "canonical.sample_read.si_d1.test",
        "canonical_read_request": EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
        "sample_ref": "canonical.sample.si.d1.ohlcv.v1",
        "sample_path": "tests/fixtures/strategy_testing/canonical_samples/si_d1_ohlcv_sample.json",
        "sample_read_mode": "approved_static_sample_only",
    }
    values.update(overrides)
    return values


def test_valid_sample_read_request_passes():
    request = CanonicalDataSampleReadRequest(**_request_values())

    assert validate_canonical_data_sample_read_request(request) is request
    assert request.sample_ref == "canonical.sample.si.d1.ohlcv.v1"
    assert request.sample_path == "tests/fixtures/strategy_testing/canonical_samples/si_d1_ohlcv_sample.json"
    assert request.sample_read_mode == "approved_static_sample_only"
    assert frozenset(request.__dict__) == EXPECTED_REQUEST_FIELDS
    assert frozenset(CANONICAL_DATA_SAMPLE_READ_REQUEST_FIELDS) == EXPECTED_REQUEST_FIELDS
    assert ALLOWED_SAMPLE_READ_MODES == frozenset({"approved_static_sample_only"})
    assert APPROVED_SAMPLE_REFS == frozenset({"canonical.sample.si.d1.ohlcv.v1"})
    assert APPROVED_SAMPLE_PATHS == frozenset({"tests/fixtures/strategy_testing/canonical_samples/si_d1_ohlcv_sample.json"})
    assert REQUIRED_OHLCV_COLUMNS == (
        "timestamp",
        "instrument_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
    )


@pytest.mark.parametrize(
    "field_name",
    ("sample_request_id", "sample_ref", "sample_path", "sample_read_mode"),
)
def test_empty_sample_request_fields_fail_closed(field_name: str):
    with pytest.raises(CanonicalDataSampleReadValidationError):
        CanonicalDataSampleReadRequest(**_request_values(**{field_name: ""}))


def test_invalid_canonical_read_request_fails_closed():
    with pytest.raises(CanonicalDataSampleReadValidationError):
        CanonicalDataSampleReadRequest(**_request_values(canonical_read_request="not-request"))


def test_unsupported_sample_ref_path_and_mode_fail_closed():
    with pytest.raises(CanonicalDataSampleReadValidationError):
        CanonicalDataSampleReadRequest(**_request_values(sample_ref="canonical.sample.other.v1"))
    with pytest.raises(CanonicalDataSampleReadValidationError):
        CanonicalDataSampleReadRequest(**_request_values(sample_path="tests/fixtures/strategy_testing/canonical_samples/other.json"))
    with pytest.raises(CanonicalDataSampleReadValidationError):
        CanonicalDataSampleReadRequest(**_request_values(sample_read_mode="production_read"))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_markers_fail_closed(marker: str):
    with pytest.raises(CanonicalDataSampleReadValidationError):
        CanonicalDataSampleReadRequest(**_request_values(sample_ref="canonical.sample." + marker + ".v1"))


@pytest.mark.parametrize(
    "marker",
    (_host_marker(), _scheduler_marker(), _market_access_marker(), "data" + "lake", _network_marker()),
)
def test_platform_markers_fail_closed(marker: str):
    with pytest.raises(CanonicalDataSampleReadValidationError):
        CanonicalDataSampleReadRequest(**_request_values(sample_ref="canonical.sample." + marker + ".v1"))


def test_approved_static_sample_file_exists_inside_repo():
    assert SAMPLE_PATH.exists()
    assert SAMPLE_PATH.is_file()
    assert SAMPLE_PATH.parts[-4:] == (
        "strategy_testing",
        "canonical_samples",
        "si_d1_ohlcv_sample.json",
    )


def test_canonical_data_sample_read_succeeds_with_metadata_only_result():
    request = CanonicalDataSampleReadRequest(**_request_values())

    result = read_canonical_data_sample_dry_run(request)

    assert result.sample_status == "validated"
    assert result.sample_request_id == request.sample_request_id
    assert result.request_id == EMA_3_19_SI_D1_CANONICAL_READ_REQUEST.request_id
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "ema_3_19.strategy_test.canonical_read.v1"
    assert result.dataset_ref_id == "canonical.dataset.si.d1.v1"
    assert result.instrument_id == "Si"
    assert result.timeframe == "D1"
    assert result.schema_ref == "canonical.schema.ohlcv.d1.v1"
    assert result.row_count_or_none == 3
    assert result.schema_status_or_none == "validated"
    assert result.error_message_or_none is None
    assert validate_canonical_data_sample_read_result(result) is result
    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(CANONICAL_DATA_SAMPLE_READ_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS


def test_sample_read_rejects_invalid_request_without_payload_fields():
    result = read_canonical_data_sample_dry_run("not-request")

    assert result.sample_status == "rejected"
    assert result.row_count_or_none is None
    assert result.schema_status_or_none is None
    assert result.error_message_or_none


def test_result_object_has_no_execution_or_data_fields():
    result = CanonicalDataSampleReadResult(
        sample_status="validated",
        sample_request_id="sample.request.fixture",
        request_id="request.fixture",
        strategy_id="ema_3_19",
        strategy_test_id="strategy_test.fixture",
        dataset_ref_id="canonical.dataset.si.d1.fixture",
        instrument_id="Si",
        timeframe="D1",
        schema_ref="canonical.schema.fixture",
        row_count_or_none=3,
        schema_status_or_none="validated",
        error_message_or_none=None,
    )
    forbidden = {
        "rows",
        "dataframe",
        "file_path",
        "signals",
        "metrics",
        "backtest_result",
        "registry_entry",
        "promotion_verdict",
        "runtime_authorization",
    }
    assert forbidden.isdisjoint(result.__dict__)


def test_invalid_result_shape_fails_closed():
    with pytest.raises(CanonicalDataSampleReadValidationError):
        CanonicalDataSampleReadResult(
            sample_status="validated",
            sample_request_id="sample.request.fixture",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            dataset_ref_id="canonical.dataset.si.d1.fixture",
            instrument_id="Si",
            timeframe="D1",
            schema_ref="canonical.schema.fixture",
            row_count_or_none=None,
            schema_status_or_none="validated",
            error_message_or_none=None,
        )
    with pytest.raises(CanonicalDataSampleReadValidationError):
        CanonicalDataSampleReadResult(
            sample_status="rejected",
            sample_request_id="sample.request.fixture",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            dataset_ref_id="canonical.dataset.si.d1.fixture",
            instrument_id="Si",
            timeframe="D1",
            schema_ref="canonical.schema.fixture",
            row_count_or_none=1,
            schema_status_or_none=None,
            error_message_or_none="bad",
        )


def test_source_has_no_forbidden_execution_or_unapproved_access_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8")
    forbidden = (
        "load_market_data",
        "calculate_ema",
        "generate_signals",
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
        _legacy_strategy_marker(),
    )
    for marker in forbidden:
        assert marker not in source
