import json
from pathlib import Path

import pytest

from moex_research.metrics.schemas import MetricsSummary, validate_metrics_summary
from moex_research.publishers.report_artifacts import ReportArtifactSpec, validate_report_artifact_spec
from moex_research.runners.ema_test_fixture_backtest_dry_run import (
    ALLOWED_DRY_RUN_MODES,
    EMA_TEST_FIXTURE_BACKTEST_REQUEST_FIELDS,
    EMA_TEST_FIXTURE_BACKTEST_RESULT_FIELDS,
    EMATestFixtureBacktestDryRunError,
    EMATestFixtureBacktestRequest,
    EMATestFixtureBacktestResult,
    run_ema_3_19_test_fixture_backtest_dry_run,
    validate_ema_test_fixture_backtest_request,
    validate_ema_test_fixture_backtest_result,
)
from moex_research.runners.synthetic_signal_artifact import SyntheticSignalRow
from tests.fixtures.strategy_testing.ema_3_19_read_only_data_fixture import (
    EMA_3_19_READ_ONLY_REQUEST,
    EMA_3_19_READ_ONLY_ROWS,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "ema_test_fixture_backtest_dry_run.py"
EXPECTED_REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "data_request",
        "data_rows",
        "signal_rows",
        "output_path",
        "artifact_manifest_ref",
        "metrics_artifact_ref",
        "report_artifact_ref",
        "dry_run_mode",
    }
)
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "dry_run_status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "row_count_or_none",
        "output_path_or_none",
        "metrics_summary_or_none",
        "report_artifact_or_none",
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


def _signal_rows() -> tuple[SyntheticSignalRow, ...]:
    values = (0, 1, 1, 1, 1)
    return tuple(
        SyntheticSignalRow(
            strategy_id="ema_3_19",
            strategy_test_id="ema_3_19.strategy_test.fixture.v1",
            signal_id="ema_3_19.signal.test_fixture.v1",
            instrument_id=str(row["instrument_id"]),
            timestamp=str(row["timestamp"]),
            signal_value=signal_value,
            signal_version="ema_3_19.signal_schema.v1",
            source_type="synthetic_test_only",
        )
        for row, signal_value in zip(EMA_3_19_READ_ONLY_ROWS, values, strict=True)
    )


def _request_values(tmp_path: Path, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "ema_3_19.fixture_backtest.request.v1",
        "strategy_id": "ema_3_19",
        "strategy_test_id": "ema_3_19.strategy_test.fixture.v1",
        "data_request": EMA_3_19_READ_ONLY_REQUEST,
        "data_rows": EMA_3_19_READ_ONLY_ROWS,
        "signal_rows": _signal_rows(),
        "output_path": str(tmp_path / "ema_3_19_fixture_backtest_result.json"),
        "artifact_manifest_ref": "artifact_manifest.ema_3_19.fixture_backtest.v1",
        "metrics_artifact_ref": "metrics.ema_3_19.fixture_backtest.v1",
        "report_artifact_ref": "report.ema_3_19.fixture_backtest.v1",
        "dry_run_mode": "test_fixture_backtest_only",
    }
    values.update(overrides)
    return values


def test_valid_ema_fixture_backtest_request_passes(tmp_path: Path):
    request = EMATestFixtureBacktestRequest(**_request_values(tmp_path))

    assert validate_ema_test_fixture_backtest_request(request) is request
    assert request.strategy_id == "ema_3_19"
    assert request.strategy_test_id == "ema_3_19.strategy_test.fixture.v1"
    assert len(request.data_rows) == len(EMA_3_19_READ_ONLY_ROWS)
    assert len(request.signal_rows) == len(EMA_3_19_READ_ONLY_ROWS)
    assert frozenset(request.__dict__) == EXPECTED_REQUEST_FIELDS
    assert frozenset(EMA_TEST_FIXTURE_BACKTEST_REQUEST_FIELDS) == EXPECTED_REQUEST_FIELDS
    assert ALLOWED_DRY_RUN_MODES == frozenset({"test_fixture_backtest_only"})


@pytest.mark.parametrize(
    "field_name",
    (
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "output_path",
        "artifact_manifest_ref",
        "metrics_artifact_ref",
        "report_artifact_ref",
    ),
)
def test_empty_required_request_fields_fail_closed(tmp_path: Path, field_name: str):
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestRequest(**_request_values(tmp_path, **{field_name: ""}))


def test_invalid_modes_and_identifiers_fail_closed(tmp_path: Path):
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestRequest(**_request_values(tmp_path, dry_run_mode="production_mode"))
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestRequest(**_request_values(tmp_path, strategy_id="other_strategy"))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_markers_fail_closed(tmp_path: Path, marker: str):
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestRequest(
            **_request_values(tmp_path, artifact_manifest_ref="artifact." + marker + ".fixture"),
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
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestRequest(
            **_request_values(tmp_path, output_path=str(tmp_path / marker / "result.json")),
        )


def test_alignment_failures_fail_closed(tmp_path: Path):
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestRequest(**_request_values(tmp_path, signal_rows=_signal_rows()[:-1]))
    bad_signal_rows = list(_signal_rows())
    bad_signal_rows[0] = SyntheticSignalRow(
        strategy_id="ema_3_19",
        strategy_test_id="ema_3_19.strategy_test.fixture.v1",
        signal_id="ema_3_19.signal.test_fixture.v1",
        instrument_id="OTHER",
        timestamp=str(EMA_3_19_READ_ONLY_ROWS[0]["timestamp"]),
        signal_value=0,
        signal_version="ema_3_19.signal_schema.v1",
        source_type="synthetic_test_only",
    )
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestRequest(**_request_values(tmp_path, signal_rows=tuple(bad_signal_rows)))


def test_ema_test_fixture_backtest_dry_run_writes_result_and_drafts(tmp_path: Path):
    output_path = tmp_path / "ema_3_19_fixture_backtest_result.json"
    request = EMATestFixtureBacktestRequest(**_request_values(tmp_path, output_path=str(output_path)))

    result = run_ema_3_19_test_fixture_backtest_dry_run(request)

    assert result.dry_run_status == "written"
    assert result.request_id == request.request_id
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == request.strategy_test_id
    assert result.row_count_or_none == len(EMA_3_19_READ_ONLY_ROWS) - 1
    assert result.output_path_or_none == str(output_path)
    assert isinstance(result.metrics_summary_or_none, MetricsSummary)
    assert isinstance(result.report_artifact_or_none, ReportArtifactSpec)
    assert result.error_message_or_none is None
    assert validate_ema_test_fixture_backtest_result(result) is result
    assert validate_metrics_summary(result.metrics_summary_or_none) is result.metrics_summary_or_none
    assert validate_report_artifact_spec(result.report_artifact_or_none) is result.report_artifact_or_none
    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(EMA_TEST_FIXTURE_BACKTEST_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["artifact_manifest_ref"] == request.artifact_manifest_ref
    assert payload["metrics_artifact_ref"] == request.metrics_artifact_ref
    assert payload["report_artifact_ref"] == request.report_artifact_ref
    assert payload["strategy_id"] == "ema_3_19"
    assert payload["scope"] == "test_fixture_only"
    assert len(payload["result_rows"]) == len(EMA_3_19_READ_ONLY_ROWS) - 1
    assert [row["synthetic_return"] for row in payload["result_rows"]] == pytest.approx(
        [0.0, 0.0148514851, -0.0097560976, 0.0147783251],
    )


def test_ema_test_fixture_backtest_rejects_non_temporary_path(tmp_path: Path):
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestRequest(
            **_request_values(tmp_path, output_path=str(Path("/not-temp") / "result.json")),
        )


def test_non_numeric_close_rejects_without_output(tmp_path: Path):
    rows = tuple(dict(row) for row in EMA_3_19_READ_ONLY_ROWS)
    rows[1]["close"] = "bad"
    request = EMATestFixtureBacktestRequest(**_request_values(tmp_path, data_rows=rows))

    result = run_ema_3_19_test_fixture_backtest_dry_run(request)

    assert result.dry_run_status == "rejected"
    assert result.error_message_or_none


def test_result_object_has_no_forbidden_fields(tmp_path: Path):
    result = run_ema_3_19_test_fixture_backtest_dry_run(
        EMATestFixtureBacktestRequest(**_request_values(tmp_path)),
    )
    forbidden = {
        "registry_entry",
        "promotion_verdict",
        "runtime_authorization",
        "order_request",
        "broker_request",
    }
    assert forbidden.isdisjoint(result.__dict__)


def test_invalid_result_shape_fails_closed():
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestResult(
            dry_run_status="written",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            row_count_or_none=None,
            output_path_or_none="path.fixture",
            metrics_summary_or_none=None,
            report_artifact_or_none=None,
            error_message_or_none=None,
        )
    with pytest.raises(EMATestFixtureBacktestDryRunError):
        EMATestFixtureBacktestResult(
            dry_run_status="rejected",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            row_count_or_none=1,
            output_path_or_none=None,
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
        _legacy_strategy_marker(),
    )
    for marker in forbidden:
        assert marker not in source
