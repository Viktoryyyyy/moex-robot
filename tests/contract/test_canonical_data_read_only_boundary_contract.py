from pathlib import Path

import pytest

from moex_research.runners.canonical_data_read import (
    ALLOWED_DATASET_CLASSES,
    ALLOWED_INSTRUMENTS,
    ALLOWED_READ_MODES,
    ALLOWED_READ_PURPOSES,
    ALLOWED_TIMEFRAMES,
    CANONICAL_DATASET_REF_FIELDS,
    CANONICAL_DATA_READ_REQUEST_FIELDS,
    CANONICAL_DATA_READ_RESULT_FIELDS,
    CanonicalDataReadRequest,
    CanonicalDataReadResult,
    CanonicalDataReadValidationError,
    CanonicalDatasetRef,
    dry_validate_canonical_data_read_request,
    validate_canonical_data_read_request,
    validate_canonical_data_read_result,
    validate_canonical_dataset_ref,
)
from tests.fixtures.strategy_testing.approved_canonical_data_refs import (
    APPROVED_SI_D1_CANONICAL_DATASET_REF,
    APPROVED_USDRUBF_5M_CANONICAL_DATASET_REF,
    EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "canonical_data_read.py"
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "strategy_testing" / "approved_canonical_data_refs.py"
EXPECTED_REF_FIELDS = frozenset(
    {
        "dataset_ref_id",
        "dataset_class",
        "instrument_id",
        "timeframe",
        "schema_ref",
        "storage_ref",
        "calendar_ref",
        "source_granularity",
        "read_mode",
    }
)
EXPECTED_REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "dataset_ref",
        "read_purpose",
        "read_mode",
    }
)
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "read_status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "dataset_ref_id",
        "instrument_id",
        "timeframe",
        "schema_ref",
        "read_mode",
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


def _ref_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "dataset_ref_id": "canonical.dataset.si.d1.test",
        "dataset_class": "canonical_bars",
        "instrument_id": "Si",
        "timeframe": "D1",
        "schema_ref": "canonical.schema.ohlcv.d1.test",
        "storage_ref": "canonical.store.si.d1.test",
        "calendar_ref": "canonical.calendar.moex.days.test",
        "source_granularity": "bar",
        "read_mode": "dry_run_reference_validation_only",
    }
    values.update(overrides)
    return values


def _request_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "ema_3_19.canonical_read.test",
        "strategy_id": "ema_3_19",
        "strategy_test_id": "ema_3_19.strategy_test.canonical_read.test",
        "dataset_ref": CanonicalDatasetRef(**_ref_values()),
        "read_purpose": "strategy_testing_planned",
        "read_mode": "dry_run_reference_validation_only",
    }
    values.update(overrides)
    return values


def test_approved_canonical_dataset_refs_pass():
    assert validate_canonical_dataset_ref(APPROVED_SI_D1_CANONICAL_DATASET_REF) is APPROVED_SI_D1_CANONICAL_DATASET_REF
    assert validate_canonical_dataset_ref(APPROVED_USDRUBF_5M_CANONICAL_DATASET_REF) is APPROVED_USDRUBF_5M_CANONICAL_DATASET_REF
    assert APPROVED_SI_D1_CANONICAL_DATASET_REF.instrument_id == "Si"
    assert APPROVED_SI_D1_CANONICAL_DATASET_REF.timeframe == "D1"
    assert APPROVED_USDRUBF_5M_CANONICAL_DATASET_REF.instrument_id == "USDRUBF"
    assert APPROVED_USDRUBF_5M_CANONICAL_DATASET_REF.timeframe == "5m"
    assert frozenset(APPROVED_SI_D1_CANONICAL_DATASET_REF.__dict__) == EXPECTED_REF_FIELDS
    assert frozenset(CANONICAL_DATASET_REF_FIELDS) == EXPECTED_REF_FIELDS
    assert ALLOWED_DATASET_CLASSES == frozenset({"canonical_bars"})
    assert ALLOWED_INSTRUMENTS == frozenset({"Si", "USDRUBF"})
    assert ALLOWED_TIMEFRAMES == frozenset({"D1", "5m"})


def test_valid_canonical_data_read_request_passes():
    request = CanonicalDataReadRequest(**_request_values())

    assert validate_canonical_data_read_request(request) is request
    assert frozenset(request.__dict__) == EXPECTED_REQUEST_FIELDS
    assert frozenset(CANONICAL_DATA_READ_REQUEST_FIELDS) == EXPECTED_REQUEST_FIELDS
    assert request.read_mode == "dry_run_reference_validation_only"
    assert ALLOWED_READ_MODES == frozenset({"dry_run_reference_validation_only"})
    assert ALLOWED_READ_PURPOSES == frozenset({"strategy_testing_planned"})


@pytest.mark.parametrize(
    "field_name",
    (
        "dataset_ref_id",
        "dataset_class",
        "instrument_id",
        "timeframe",
        "schema_ref",
        "storage_ref",
        "calendar_ref",
        "source_granularity",
        "read_mode",
    ),
)
def test_empty_canonical_dataset_ref_fields_fail_closed(field_name: str):
    with pytest.raises(CanonicalDataReadValidationError):
        CanonicalDatasetRef(**_ref_values(**{field_name: ""}))


@pytest.mark.parametrize(
    "override",
    (
        {"dataset_class": "raw_bars"},
        {"instrument_id": "GAZP"},
        {"timeframe": "1m"},
        {"source_granularity": "tick"},
        {"read_mode": "production_read"},
    ),
)
def test_unsupported_canonical_ref_values_fail_closed(override: dict[str, object]):
    with pytest.raises(CanonicalDataReadValidationError):
        CanonicalDatasetRef(**_ref_values(**override))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_markers_fail_closed(marker: str):
    with pytest.raises(CanonicalDataReadValidationError):
        CanonicalDatasetRef(**_ref_values(dataset_ref_id="canonical." + marker + ".dataset"))


@pytest.mark.parametrize(
    "marker",
    (
        _host_marker(),
        _scheduler_marker(),
        _market_access_marker(),
        "data" + "lake",
        _network_marker(),
    ),
)
def test_platform_markers_fail_closed(marker: str):
    with pytest.raises(CanonicalDataReadValidationError):
        CanonicalDatasetRef(**_ref_values(storage_ref="canonical." + marker + ".ref"))


def test_request_mode_must_match_dataset_ref():
    with pytest.raises(CanonicalDataReadValidationError):
        CanonicalDataReadRequest(**_request_values(read_mode="dry_run_reference_validation_only_alt"))


def test_dry_validate_canonical_data_read_request_returns_metadata_only():
    result = dry_validate_canonical_data_read_request(EMA_3_19_SI_D1_CANONICAL_READ_REQUEST)

    assert result.read_status == "validated"
    assert result.request_id == "ema_3_19.canonical_data_read.si_d1.v1"
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "ema_3_19.strategy_test.canonical_read.v1"
    assert result.dataset_ref_id == "canonical.dataset.si.d1.v1"
    assert result.instrument_id == "Si"
    assert result.timeframe == "D1"
    assert result.schema_ref == "canonical.schema.ohlcv.d1.v1"
    assert result.read_mode == "dry_run_reference_validation_only"
    assert result.error_message_or_none is None
    assert validate_canonical_data_read_result(result) is result
    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(CANONICAL_DATA_READ_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS


def test_dry_validate_rejects_invalid_request_without_data_read():
    result = dry_validate_canonical_data_read_request("not-request")

    assert result.read_status == "rejected"
    assert result.error_message_or_none
    assert result.dataset_ref_id == "unavailable"


def test_result_object_has_no_execution_or_data_fields():
    result = CanonicalDataReadResult(
        read_status="validated",
        request_id="request.fixture",
        strategy_id="ema_3_19",
        strategy_test_id="strategy_test.fixture",
        dataset_ref_id="canonical.dataset.si.d1.fixture",
        instrument_id="Si",
        timeframe="D1",
        schema_ref="canonical.schema.fixture",
        read_mode="dry_run_reference_validation_only",
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
    with pytest.raises(CanonicalDataReadValidationError):
        CanonicalDataReadResult(
            read_status="validated",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            dataset_ref_id="canonical.dataset.si.d1.fixture",
            instrument_id="Si",
            timeframe="D1",
            schema_ref="canonical.schema.fixture",
            read_mode="dry_run_reference_validation_only",
            error_message_or_none="bad",
        )
    with pytest.raises(CanonicalDataReadValidationError):
        CanonicalDataReadResult(
            read_status="rejected",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            dataset_ref_id="canonical.dataset.si.d1.fixture",
            instrument_id="Si",
            timeframe="D1",
            schema_ref="canonical.schema.fixture",
            read_mode="dry_run_reference_validation_only",
            error_message_or_none=None,
        )


def test_source_has_no_forbidden_execution_or_data_access_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8") + "\n" + FIXTURE_PATH.read_text(encoding="utf-8")
    forbidden = (
        "open(",
        "read_text(",
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
