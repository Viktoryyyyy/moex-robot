from pathlib import Path

import pytest

from moex_research.runners.data_binding import (
    ALLOWED_ARTIFACT_CLASSES,
    ALLOWED_READ_MODES,
    READ_ONLY_DATA_BINDING_FIELDS,
    READ_ONLY_DATA_REQUEST_FIELDS,
    READ_ONLY_DATA_RESULT_FIELDS,
    READ_ONLY_DATASET_SCHEMA_FIELDS,
    ReadOnlyDataBinding,
    ReadOnlyDataBindingValidationError,
    ReadOnlyDataRequest,
    ReadOnlyDataResult,
    ReadOnlyDatasetSchema,
    validate_read_only_data_binding,
    validate_read_only_data_request,
    validate_read_only_data_result,
    validate_read_only_data_rows,
    validate_read_only_dataset_schema,
)
from tests.fixtures.strategy_testing.ema_3_19_read_only_data_fixture import (
    EMA_3_19_READ_ONLY_BINDING,
    EMA_3_19_READ_ONLY_REQUEST,
    EMA_3_19_READ_ONLY_ROWS,
    EMA_3_19_READ_ONLY_SCHEMA,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "data_binding.py"
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "strategy_testing" / "ema_3_19_read_only_data_fixture.py"
EXPECTED_BINDING_FIELDS = frozenset(
    {
        "binding_id",
        "dataset_ref_id",
        "strategy_id",
        "strategy_test_id",
        "artifact_class",
        "artifact_ref",
        "schema_ref",
        "read_mode",
    }
)
EXPECTED_SCHEMA_FIELDS = frozenset(
    {
        "schema_id",
        "schema_version",
        "required_columns",
        "timestamp_column",
        "instrument_column",
        "price_columns",
    }
)
EXPECTED_REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "dataset_binding",
        "dataset_schema",
        "read_mode",
    }
)
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "read_status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "schema_id",
        "row_count_or_none",
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


def _storage_marker() -> str:
    return "data" + "root"


def _network_marker() -> str:
    return "net" + "work"


def _binding_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "binding_id": "ema_3_19.read_only.binding.test",
        "dataset_ref_id": "dataset.ema_3_19.fixture.test",
        "strategy_id": "ema_3_19",
        "strategy_test_id": "ema_3_19.strategy_test.fixture.test",
        "artifact_class": "temporary_test_path",
        "artifact_ref": "test_fixture://ema_3_19/read_only_rows.test",
        "schema_ref": "schema.ema_3_19.read_only.test",
        "read_mode": "test_fixture_read_only",
    }
    values.update(overrides)
    return values


def _schema_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "schema_id": "schema.ema_3_19.read_only.test",
        "schema_version": "read_only_schema.test.v1",
        "required_columns": ("timestamp", "instrument_id", "close"),
        "timestamp_column": "timestamp",
        "instrument_column": "instrument_id",
        "price_columns": ("close",),
    }
    values.update(overrides)
    return values


def _request_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "ema_3_19.read_only.request.test",
        "strategy_id": "ema_3_19",
        "strategy_test_id": "ema_3_19.strategy_test.fixture.test",
        "dataset_binding": ReadOnlyDataBinding(**_binding_values()),
        "dataset_schema": ReadOnlyDatasetSchema(**_schema_values()),
        "read_mode": "test_fixture_read_only",
    }
    values.update(overrides)
    return values


def test_read_only_data_binding_schema_and_request_pass():
    binding = ReadOnlyDataBinding(**_binding_values())
    schema = ReadOnlyDatasetSchema(**_schema_values())
    request = ReadOnlyDataRequest(**_request_values(dataset_binding=binding, dataset_schema=schema))

    assert validate_read_only_data_binding(binding) is binding
    assert validate_read_only_dataset_schema(schema) is schema
    assert validate_read_only_data_request(request) is request
    assert frozenset(binding.__dict__) == EXPECTED_BINDING_FIELDS
    assert frozenset(schema.__dict__) == EXPECTED_SCHEMA_FIELDS
    assert frozenset(request.__dict__) == EXPECTED_REQUEST_FIELDS
    assert frozenset(READ_ONLY_DATA_BINDING_FIELDS) == EXPECTED_BINDING_FIELDS
    assert frozenset(READ_ONLY_DATASET_SCHEMA_FIELDS) == EXPECTED_SCHEMA_FIELDS
    assert frozenset(READ_ONLY_DATA_REQUEST_FIELDS) == EXPECTED_REQUEST_FIELDS
    assert "test_fixture_read_only" in ALLOWED_READ_MODES
    assert "temporary_test_path" in ALLOWED_ARTIFACT_CLASSES


@pytest.mark.parametrize(
    "field_name",
    (
        "binding_id",
        "dataset_ref_id",
        "strategy_id",
        "strategy_test_id",
        "artifact_ref",
        "schema_ref",
    ),
)
def test_empty_read_only_data_binding_fields_fail_closed(field_name: str):
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataBinding(**_binding_values(**{field_name: ""}))


def test_unsupported_read_only_data_binding_values_fail_closed():
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataBinding(**_binding_values(artifact_class="unsupported_class"))
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataBinding(**_binding_values(read_mode="production_read"))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_markers_fail_closed(marker: str):
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataBinding(**_binding_values(artifact_ref="test_fixture://" + marker + "/rows"))


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
def test_platform_access_markers_fail_closed(marker: str):
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataBinding(**_binding_values(artifact_ref="test_fixture://" + marker + "/rows"))


def test_read_only_dataset_schema_requires_minimal_ema_columns():
    schema = ReadOnlyDatasetSchema(**_schema_values())

    assert schema.required_columns == ("timestamp", "instrument_id", "close")
    assert schema.timestamp_column == "timestamp"
    assert schema.instrument_column == "instrument_id"
    assert schema.price_columns == ("close",)


@pytest.mark.parametrize(
    "override",
    (
        {"required_columns": ("timestamp", "instrument_id")},
        {"timestamp_column": "not_declared"},
        {"instrument_column": "not_declared"},
        {"price_columns": ("not_declared",)},
        {"required_columns": ()},
    ),
)
def test_invalid_read_only_dataset_schema_fails_closed(override: dict[str, object]):
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDatasetSchema(**_schema_values(**override))


def test_read_only_data_request_cross_reference_mismatch_fails_closed():
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataRequest(**_request_values(strategy_id="other_strategy"))
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataRequest(**_request_values(read_mode="schema_validation_only"))
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataRequest(
            **_request_values(
                dataset_schema=ReadOnlyDatasetSchema(**_schema_values(schema_id="schema.other")),
            )
        )


def test_valid_in_test_fixture_rows_pass_schema_validation():
    result = validate_read_only_data_rows(EMA_3_19_READ_ONLY_REQUEST, EMA_3_19_READ_ONLY_ROWS)

    assert result.read_status == "validated"
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "ema_3_19.strategy_test.fixture.v1"
    assert result.schema_id == "schema.ema_3_19.read_only.v1"
    assert result.row_count_or_none == len(EMA_3_19_READ_ONLY_ROWS)
    assert result.error_message_or_none is None
    assert validate_read_only_data_result(result) is result
    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(READ_ONLY_DATA_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS
    assert validate_read_only_data_binding(EMA_3_19_READ_ONLY_BINDING) is EMA_3_19_READ_ONLY_BINDING
    assert validate_read_only_dataset_schema(EMA_3_19_READ_ONLY_SCHEMA) is EMA_3_19_READ_ONLY_SCHEMA


def test_missing_required_close_column_fails_closed():
    rows = ({"timestamp": "2026-01-05", "instrument_id": "SYNTH_FIXTURE"},)

    result = validate_read_only_data_rows(EMA_3_19_READ_ONLY_REQUEST, rows)

    assert result.read_status == "rejected"
    assert result.row_count_or_none is None
    assert result.error_message_or_none


def test_empty_fixture_rows_fail_closed():
    result = validate_read_only_data_rows(EMA_3_19_READ_ONLY_REQUEST, ())

    assert result.read_status == "rejected"
    assert result.row_count_or_none is None
    assert result.error_message_or_none


def test_read_only_data_result_minimal_schema_only():
    result = ReadOnlyDataResult(
        read_status="validated",
        request_id="request.fixture",
        strategy_id="ema_3_19",
        strategy_test_id="strategy_test.fixture",
        schema_id="schema.fixture",
        row_count_or_none=1,
        error_message_or_none=None,
    )

    assert validate_read_only_data_result(result) is result
    forbidden = {
        "signals",
        "metrics",
        "backtest_result",
        "research_result",
        "registry_entry",
        "promotion_verdict",
        "runtime_authorization",
    }
    assert forbidden.isdisjoint(result.__dict__)


def test_invalid_read_only_data_result_fails_closed():
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataResult(
            read_status="validated",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            schema_id="schema.fixture",
            row_count_or_none=None,
            error_message_or_none=None,
        )
    with pytest.raises(ReadOnlyDataBindingValidationError):
        ReadOnlyDataResult(
            read_status="rejected",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            schema_id="schema.fixture",
            row_count_or_none=1,
            error_message_or_none="bad",
        )


def test_source_has_no_forbidden_execution_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8") + "\n" + FIXTURE_PATH.read_text(encoding="utf-8")
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
        "data_root",
        "server",
        "latest",
        "current",
        "autodetect",
        _legacy_strategy_marker(),
    )
    for marker in forbidden:
        assert marker not in source
