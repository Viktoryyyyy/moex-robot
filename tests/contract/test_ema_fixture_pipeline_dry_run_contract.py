import json
from pathlib import Path

import pytest

from moex_research.metrics.schemas import MetricsSummary, validate_metrics_summary
from moex_research.publishers.report_artifacts import ReportArtifactSpec, validate_report_artifact_spec
from moex_research.runners.ema_fixture_pipeline_dry_run import (
    ALLOWED_PIPELINE_MODES,
    EMA_FIXTURE_PIPELINE_REQUEST_FIELDS,
    EMA_FIXTURE_PIPELINE_RESULT_FIELDS,
    EMAFixturePipelineDryRunError,
    EMAFixturePipelineRequest,
    EMAFixturePipelineResult,
    run_ema_fixture_pipeline_dry_run,
    validate_ema_fixture_pipeline_request,
    validate_ema_fixture_pipeline_result,
)
from tests.fixtures.strategy_testing.ema_3_19_read_only_data_fixture import (
    EMA_3_19_READ_ONLY_REQUEST,
    EMA_3_19_READ_ONLY_ROWS,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "ema_fixture_pipeline_dry_run.py"
EXPECTED_REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "data_request",
        "data_rows",
        "signal_output_path",
        "backtest_output_path",
        "artifact_manifest_ref",
        "signal_artifact_ref",
        "backtest_artifact_ref",
        "metrics_artifact_ref",
        "report_artifact_ref",
        "pipeline_mode",
    }
)
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "pipeline_status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "read_status",
        "signal_status",
        "backtest_status",
        "artifact_manifest_draft_id_or_none",
        "signal_output_path_or_none",
        "backtest_output_path_or_none",
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


def _request_values(tmp_path: Path, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "ema_3_19.fixture_pipeline.request.v1",
        "strategy_id": "ema_3_19",
        "strategy_test_id": "ema_3_19.strategy_test.fixture.v1",
        "data_request": EMA_3_19_READ_ONLY_REQUEST,
        "data_rows": EMA_3_19_READ_ONLY_ROWS,
        "signal_output_path": str(tmp_path / "ema_3_19_pipeline_signal.json"),
        "backtest_output_path": str(tmp_path / "ema_3_19_pipeline_backtest.json"),
        "artifact_manifest_ref": "artifact_manifest.ema_3_19.fixture_pipeline.v1",
        "signal_artifact_ref": "signal_artifact.ema_3_19.fixture_pipeline.v1",
        "backtest_artifact_ref": "backtest_artifact.ema_3_19.fixture_pipeline.v1",
        "metrics_artifact_ref": "metrics.ema_3_19.fixture_pipeline.v1",
        "report_artifact_ref": "report.ema_3_19.fixture_pipeline.v1",
        "pipeline_mode": "fixture_pipeline_dry_run_only",
    }
    values.update(overrides)
    return values


def test_valid_ema_fixture_pipeline_request_passes(tmp_path: Path):
    request = EMAFixturePipelineRequest(**_request_values(tmp_path))

    assert validate_ema_fixture_pipeline_request(request) is request
    assert request.strategy_id == "ema_3_19"
    assert request.strategy_test_id == "ema_3_19.strategy_test.fixture.v1"
    assert len(request.data_rows) == len(EMA_3_19_READ_ONLY_ROWS)
    assert frozenset(request.__dict__) == EXPECTED_REQUEST_FIELDS
    assert frozenset(EMA_FIXTURE_PIPELINE_REQUEST_FIELDS) == EXPECTED_REQUEST_FIELDS
    assert ALLOWED_PIPELINE_MODES == frozenset({"fixture_pipeline_dry_run_only"})


@pytest.mark.parametrize(
    "field_name",
    (
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "signal_output_path",
        "backtest_output_path",
        "artifact_manifest_ref",
        "signal_artifact_ref",
        "backtest_artifact_ref",
        "metrics_artifact_ref",
        "report_artifact_ref",
    ),
)
def test_empty_required_request_fields_fail_closed(tmp_path: Path, field_name: str):
    with pytest.raises(EMAFixturePipelineDryRunError):
        EMAFixturePipelineRequest(**_request_values(tmp_path, **{field_name: ""}))


def test_invalid_pipeline_mode_and_strategy_fail_closed(tmp_path: Path):
    with pytest.raises(EMAFixturePipelineDryRunError):
        EMAFixturePipelineRequest(**_request_values(tmp_path, pipeline_mode="production_mode"))
    with pytest.raises(EMAFixturePipelineDryRunError):
        EMAFixturePipelineRequest(**_request_values(tmp_path, strategy_id="other_strategy"))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_markers_fail_closed(tmp_path: Path, marker: str):
    with pytest.raises(EMAFixturePipelineDryRunError):
        EMAFixturePipelineRequest(
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
    with pytest.raises(EMAFixturePipelineDryRunError):
        EMAFixturePipelineRequest(
            **_request_values(tmp_path, signal_output_path=str(tmp_path / marker / "signals.json")),
        )


def test_non_temporary_path_fails_closed(tmp_path: Path):
    with pytest.raises(EMAFixturePipelineDryRunError):
        EMAFixturePipelineRequest(
            **_request_values(tmp_path, backtest_output_path=str(Path("/not-temp") / "backtest.json")),
        )


def test_invalid_fixture_rows_fail_closed(tmp_path: Path):
    bad_rows = tuple(dict(row) for row in EMA_3_19_READ_ONLY_ROWS)
    del bad_rows[0]["close"]
    with pytest.raises(EMAFixturePipelineDryRunError):
        EMAFixturePipelineRequest(**_request_values(tmp_path, data_rows=bad_rows))


def test_ema_fixture_pipeline_runs_all_fixture_stages(tmp_path: Path):
    signal_path = tmp_path / "ema_3_19_pipeline_signal.json"
    backtest_path = tmp_path / "ema_3_19_pipeline_backtest.json"
    request = EMAFixturePipelineRequest(
        **_request_values(
            tmp_path,
            signal_output_path=str(signal_path),
            backtest_output_path=str(backtest_path),
        )
    )

    result = run_ema_fixture_pipeline_dry_run(request)

    assert result.pipeline_status == "written"
    assert result.read_status == "validated"
    assert result.signal_status == "written"
    assert result.backtest_status == "written"
    assert result.request_id == request.request_id
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == request.strategy_test_id
    assert result.artifact_manifest_draft_id_or_none == request.request_id + ".artifact_manifest_draft"
    assert result.signal_output_path_or_none == str(signal_path)
    assert result.backtest_output_path_or_none == str(backtest_path)
    assert isinstance(result.metrics_summary_or_none, MetricsSummary)
    assert isinstance(result.report_artifact_or_none, ReportArtifactSpec)
    assert validate_ema_fixture_pipeline_result(result) is result
    assert validate_metrics_summary(result.metrics_summary_or_none) is result.metrics_summary_or_none
    assert validate_report_artifact_spec(result.report_artifact_or_none) is result.report_artifact_or_none
    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(EMA_FIXTURE_PIPELINE_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS

    signal_payload = json.loads(signal_path.read_text(encoding="utf-8"))
    backtest_payload = json.loads(backtest_path.read_text(encoding="utf-8"))
    assert signal_payload["artifact_role"] == "synthetic_signal_table"
    assert len(signal_payload["rows"]) == len(EMA_3_19_READ_ONLY_ROWS)
    assert [row["signal_value"] for row in signal_payload["rows"]] == [0, 1, 1, 1, 1]
    assert backtest_payload["artifact_manifest_ref"] == request.artifact_manifest_ref
    assert backtest_payload["metrics_artifact_ref"] == request.metrics_artifact_ref
    assert backtest_payload["report_artifact_ref"] == request.report_artifact_ref
    assert len(backtest_payload["result_rows"]) == len(EMA_3_19_READ_ONLY_ROWS) - 1


def test_pipeline_rejects_bad_read_rows_without_outputs(tmp_path: Path):
    rows = tuple(dict(row) for row in EMA_3_19_READ_ONLY_ROWS)
    rows[0]["close"] = "bad"
    request = EMAFixturePipelineRequest(**_request_values(tmp_path, data_rows=rows))

    result = run_ema_fixture_pipeline_dry_run(request)

    assert result.pipeline_status == "rejected"
    assert result.read_status == "rejected"
    assert result.signal_status == "rejected"
    assert result.backtest_status == "rejected"
    assert result.metrics_summary_or_none is None
    assert result.report_artifact_or_none is None
    assert result.error_message_or_none


def test_result_object_has_no_forbidden_fields(tmp_path: Path):
    result = run_ema_fixture_pipeline_dry_run(EMAFixturePipelineRequest(**_request_values(tmp_path)))
    forbidden = {
        "registry_entry",
        "runtime_authorization",
        "order_request",
        "broker_request",
    }
    assert forbidden.isdisjoint(result.__dict__)


def test_invalid_result_shape_fails_closed():
    with pytest.raises(EMAFixturePipelineDryRunError):
        EMAFixturePipelineResult(
            pipeline_status="written",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            read_status="validated",
            signal_status="written",
            backtest_status="written",
            artifact_manifest_draft_id_or_none="manifest.draft",
            signal_output_path_or_none="signal.path",
            backtest_output_path_or_none="backtest.path",
            metrics_summary_or_none=None,
            report_artifact_or_none=None,
            error_message_or_none=None,
        )
    with pytest.raises(EMAFixturePipelineDryRunError):
        EMAFixturePipelineResult(
            pipeline_status="rejected",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            read_status="rejected",
            signal_status="rejected",
            backtest_status="rejected",
            artifact_manifest_draft_id_or_none=None,
            signal_output_path_or_none=None,
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
