import json
from pathlib import Path

import pytest

from moex_research.runners.ema_test_fixture_signal_calculation import (
    ALLOWED_CALCULATION_MODES,
    EMA_TEST_FIXTURE_SIGNAL_CALCULATION_REQUEST_FIELDS,
    EMA_TEST_FIXTURE_SIGNAL_CALCULATION_RESULT_FIELDS,
    EMATestFixtureSignalCalculationError,
    EMATestFixtureSignalCalculationRequest,
    EMATestFixtureSignalCalculationResult,
    calculate_ema_3_19_test_fixture_signals_dry_run,
    validate_ema_test_fixture_signal_calculation_request,
    validate_ema_test_fixture_signal_calculation_result,
)
from tests.fixtures.strategy_testing.ema_3_19_read_only_data_fixture import (
    EMA_3_19_READ_ONLY_REQUEST,
    EMA_3_19_READ_ONLY_ROWS,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "ema_test_fixture_signal_calculation.py"
EXPECTED_REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "data_request",
        "rows",
        "fast_period",
        "slow_period",
        "signal_id",
        "signal_version",
        "output_path",
        "artifact_manifest_ref",
        "calculation_mode",
    }
)
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "calculation_status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "row_count_or_none",
        "signal_artifact_id_or_none",
        "output_path_or_none",
        "artifact_manifest_ref_or_none",
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


def _request_values(tmp_path: Path, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "ema_3_19.signal_calc.request.test",
        "strategy_id": "ema_3_19",
        "strategy_test_id": "ema_3_19.strategy_test.fixture.v1",
        "data_request": EMA_3_19_READ_ONLY_REQUEST,
        "rows": EMA_3_19_READ_ONLY_ROWS,
        "fast_period": 3,
        "slow_period": 19,
        "signal_id": "ema_3_19.signal.test_fixture.v1",
        "signal_version": "ema_3_19.signal_schema.v1",
        "output_path": str(tmp_path / "ema_3_19_signal_table.json"),
        "artifact_manifest_ref": "artifact_manifest.ema_3_19.test_fixture.v1",
        "calculation_mode": "test_fixture_signal_calculation_only",
    }
    values.update(overrides)
    return values


def test_valid_ema_test_fixture_signal_calculation_request_passes(tmp_path: Path):
    request = EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path))

    assert validate_ema_test_fixture_signal_calculation_request(request) is request
    assert request.strategy_id == "ema_3_19"
    assert request.fast_period == 3
    assert request.slow_period == 19
    assert request.rows == EMA_3_19_READ_ONLY_ROWS
    assert frozenset(request.__dict__) == EXPECTED_REQUEST_FIELDS
    assert frozenset(EMA_TEST_FIXTURE_SIGNAL_CALCULATION_REQUEST_FIELDS) == EXPECTED_REQUEST_FIELDS
    assert ALLOWED_CALCULATION_MODES == frozenset({"test_fixture_signal_calculation_only"})


@pytest.mark.parametrize(
    "field_name",
    (
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "signal_id",
        "signal_version",
        "output_path",
        "artifact_manifest_ref",
    ),
)
def test_empty_required_request_fields_fail_closed(tmp_path: Path, field_name: str):
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path, **{field_name: ""}))


def test_invalid_periods_and_modes_fail_closed(tmp_path: Path):
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path, fast_period=19, slow_period=3))
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path, fast_period=0))
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path, calculation_mode="production_mode"))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_markers_fail_closed(tmp_path: Path, marker: str):
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationRequest(
            **_request_values(tmp_path, signal_id="ema_3_19." + marker + ".signal"),
        )


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
def test_platform_path_markers_fail_closed(tmp_path: Path, marker: str):
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationRequest(
            **_request_values(tmp_path, output_path=str(tmp_path / marker / "signals.json")),
        )


def test_invalid_rows_fail_closed(tmp_path: Path):
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path, rows=()))
    bad_rows = tuple(dict(row) for row in EMA_3_19_READ_ONLY_ROWS)
    del bad_rows[0]["close"]
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path, rows=bad_rows))


def test_ema_test_fixture_signal_calculation_writes_synthetic_signal_artifact(tmp_path: Path):
    output_path = tmp_path / "ema_3_19_signal_table.json"
    request = EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path, output_path=str(output_path)))

    result = calculate_ema_3_19_test_fixture_signals_dry_run(request)

    assert result.calculation_status == "written"
    assert result.request_id == request.request_id
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == request.strategy_test_id
    assert result.row_count_or_none == len(EMA_3_19_READ_ONLY_ROWS)
    assert result.signal_artifact_id_or_none == "ema_3_19.test_fixture.signal_table.v1"
    assert result.output_path_or_none == str(output_path)
    assert result.artifact_manifest_ref_or_none == request.artifact_manifest_ref
    assert result.error_message_or_none is None
    assert validate_ema_test_fixture_signal_calculation_result(result) is result
    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(EMA_TEST_FIXTURE_SIGNAL_CALCULATION_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["artifact_id"] == "ema_3_19.test_fixture.signal_table.v1"
    assert payload["artifact_role"] == "synthetic_signal_table"
    assert payload["artifact_class"] == "temporary_test_path"
    assert payload["source_type"] == "synthetic_test_only"
    assert len(payload["rows"]) == len(EMA_3_19_READ_ONLY_ROWS)
    assert [row["signal_value"] for row in payload["rows"]] == [0, 1, 1, 1, 1]
    assert [row["timestamp"] for row in payload["rows"]] == [
        row["timestamp"] for row in EMA_3_19_READ_ONLY_ROWS
    ]


def test_ema_test_fixture_signal_calculation_rejects_bad_output_path(tmp_path: Path):
    request = EMATestFixtureSignalCalculationRequest(
        **_request_values(tmp_path, output_path=str(Path("/not-temp") / "signals.json")),
    )

    result = calculate_ema_3_19_test_fixture_signals_dry_run(request)

    assert result.calculation_status == "rejected"
    assert result.row_count_or_none is None
    assert result.signal_artifact_id_or_none is None
    assert result.output_path_or_none is None
    assert result.artifact_manifest_ref_or_none is None
    assert result.error_message_or_none


def test_non_numeric_close_rejects_without_writing(tmp_path: Path):
    rows = tuple(dict(row) for row in EMA_3_19_READ_ONLY_ROWS)
    rows[0]["close"] = "not_numeric"
    request = EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path, rows=rows))

    result = calculate_ema_3_19_test_fixture_signals_dry_run(request)

    assert result.calculation_status == "rejected"
    assert result.error_message_or_none


def test_result_object_has_no_forbidden_result_fields(tmp_path: Path):
    result = calculate_ema_3_19_test_fixture_signals_dry_run(
        EMATestFixtureSignalCalculationRequest(**_request_values(tmp_path)),
    )
    forbidden = {
        "metrics",
        "report_output",
        "registry_entry",
        "promotion_verdict",
        "backtest_result",
        "research_result",
        "runtime_authorization",
    }
    assert forbidden.isdisjoint(result.__dict__)


def test_invalid_result_fails_closed():
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationResult(
            calculation_status="written",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            row_count_or_none=None,
            signal_artifact_id_or_none="artifact.fixture",
            output_path_or_none="path.fixture",
            artifact_manifest_ref_or_none="manifest.fixture",
            error_message_or_none=None,
        )
    with pytest.raises(EMATestFixtureSignalCalculationError):
        EMATestFixtureSignalCalculationResult(
            calculation_status="rejected",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            row_count_or_none=1,
            signal_artifact_id_or_none=None,
            output_path_or_none=None,
            artifact_manifest_ref_or_none=None,
            error_message_or_none="bad",
        )


def test_source_has_no_forbidden_production_execution_markers():
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
        "data_root",
        "server",
        "latest",
        "current",
        "autodetect",
        _legacy_strategy_marker(),
    )
    for marker in forbidden:
        assert marker not in source
