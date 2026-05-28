from pathlib import Path

import pytest

from moex_research.metrics.schemas import (
    ALLOWED_GROSS_OR_NET,
    METRICS_SUMMARY_REQUIRED_FIELDS,
    METRIC_RECORD_REQUIRED_FIELDS,
    MetricRecord,
    MetricsArtifactSpec,
    MetricsSummary,
    MetricsValidationError,
    validate_metrics_artifact_spec,
    validate_metrics_summary,
)
from moex_research.publishers.report_artifacts import (
    ALLOWED_REPORT_ARTIFACT_CLASSES,
    ALLOWED_REPORT_FORMATS,
    REPORT_ARTIFACT_SPEC_REQUIRED_FIELDS,
    REPORT_SECTION_SPEC_REQUIRED_FIELDS,
    ReportArtifactSpec,
    ReportArtifactValidationError,
    ReportSectionSpec,
    validate_report_artifact_spec,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
METRICS_SCHEMA_PATH = REPO_ROOT / "src" / "moex_research" / "metrics" / "schemas.py"
REPORT_ARTIFACT_PATH = REPO_ROOT / "src" / "moex_research" / "publishers" / "report_artifacts.py"


def _metric_record(**overrides: object) -> MetricRecord:
    values = {
        "metric_id": "metric.strategy_test.mean_return.v1",
        "metric_name": "mean_return",
        "metric_value": 0.0,
        "metric_unit": "return_fraction",
        "scope": "pooled_primary_labels",
        "gross_or_net": "gross",
        "producer": "moex_research.metrics.schema_contract",
        "consumer": "moex_research.publisher.schema_contract",
    }
    values.update(overrides)
    return MetricRecord(**values)


def _metrics_summary(**overrides: object) -> MetricsSummary:
    values = {
        "run_id": "strategy_test_run.ema_3_19.fixture.v1",
        "strategy_id": "ema_3_19",
        "test_type": "signal_only_research",
        "scope_level": "strategy_test",
        "result_status": "not_evaluated_skeleton",
        "canonicality_status": "not_applicable_skeleton",
        "metric_schema_version": "metrics_summary.v1",
        "metric_records": (_metric_record(),),
        "artifact_ref": "artifact.metrics.strategy_test.ema_3_19.fixture.v1",
    }
    values.update(overrides)
    return MetricsSummary(**values)


def _section(**overrides: object) -> ReportSectionSpec:
    values = {
        "section_id": "metrics_summary",
        "title": "Metrics summary",
        "required": True,
    }
    values.update(overrides)
    return ReportSectionSpec(**values)


def _report_artifact(**overrides: object) -> ReportArtifactSpec:
    values = {
        "report_id": "report.strategy_test.ema_3_19.fixture.v1",
        "run_id": "strategy_test_run.ema_3_19.fixture.v1",
        "strategy_id": "ema_3_19",
        "report_schema_version": "report_artifact.v1",
        "artifact_class": "repo_relative",
        "producer": "moex_research.publishers.report_artifacts",
        "consumer": "moex_research.strategy_testing",
        "format": "markdown",
        "required_sections": (_section(),),
    }
    values.update(overrides)
    return ReportArtifactSpec(**values)


def test_metrics_required_fields_are_explicit():
    assert METRIC_RECORD_REQUIRED_FIELDS == (
        "metric_id",
        "metric_name",
        "metric_value",
        "metric_unit",
        "scope",
        "gross_or_net",
        "producer",
        "consumer",
    )
    assert METRICS_SUMMARY_REQUIRED_FIELDS == (
        "run_id",
        "strategy_id",
        "test_type",
        "scope_level",
        "result_status",
        "canonicality_status",
        "metric_schema_version",
        "metric_records",
        "artifact_ref",
    )


def test_valid_metrics_summary_passes():
    summary = _metrics_summary()

    assert validate_metrics_summary(summary) is summary
    assert summary.run_id == "strategy_test_run.ema_3_19.fixture.v1"
    assert summary.strategy_id == "ema_3_19"
    assert summary.artifact_ref == "artifact.metrics.strategy_test.ema_3_19.fixture.v1"
    assert summary.metric_records[0].gross_or_net == "gross"


@pytest.mark.parametrize(
    "field_name",
    ["run_id", "strategy_id", "metric_schema_version", "artifact_ref"],
)
def test_invalid_metrics_summary_fails_closed(field_name):
    with pytest.raises(MetricsValidationError):
        _metrics_summary(**{field_name: ""})


def test_empty_metric_records_fail():
    with pytest.raises(MetricsValidationError):
        _metrics_summary(metric_records=())


def test_unsupported_gross_or_net_fails():
    with pytest.raises(MetricsValidationError):
        _metric_record(gross_or_net="after_cost")


def test_metric_record_requires_producer_and_consumer():
    with pytest.raises(MetricsValidationError):
        _metric_record(producer="")
    with pytest.raises(MetricsValidationError):
        _metric_record(consumer="")


def test_metrics_artifact_spec_is_schema_only():
    spec = MetricsArtifactSpec(
        artifact_ref="artifact.metrics.strategy_test.ema_3_19.fixture.v1",
        artifact_class="repo_relative",
        format="json",
        metric_schema_version="metrics_summary.v1",
        producer="moex_research.metrics.schemas",
        consumer="moex_research.strategy_testing",
    )

    assert validate_metrics_artifact_spec(spec) is spec


def test_report_required_fields_are_explicit():
    assert REPORT_SECTION_SPEC_REQUIRED_FIELDS == ("section_id", "title", "required")
    assert REPORT_ARTIFACT_SPEC_REQUIRED_FIELDS == (
        "report_id",
        "run_id",
        "strategy_id",
        "report_schema_version",
        "artifact_class",
        "producer",
        "consumer",
        "format",
        "required_sections",
    )


def test_valid_report_artifact_spec_passes():
    spec = _report_artifact()

    assert validate_report_artifact_spec(spec) is spec
    assert spec.report_id == "report.strategy_test.ema_3_19.fixture.v1"
    assert spec.artifact_class == "repo_relative"
    assert spec.format == "markdown"
    assert spec.required_sections[0].required is True


@pytest.mark.parametrize(
    "field_name",
    ["report_id", "run_id", "strategy_id", "report_schema_version", "producer", "consumer"],
)
def test_invalid_report_artifact_spec_fails_closed(field_name):
    with pytest.raises(ReportArtifactValidationError):
        _report_artifact(**{field_name: ""})


def test_unsupported_artifact_class_fails():
    with pytest.raises(ReportArtifactValidationError):
        _report_artifact(artifact_class="implicit_path")


def test_unsupported_report_format_fails():
    with pytest.raises(ReportArtifactValidationError):
        _report_artifact(format="html")


def test_required_sections_are_explicit():
    spec = _report_artifact(required_sections=(_section(section_id="summary"), _section(section_id="scope")))

    assert tuple(section.section_id for section in spec.required_sections) == ("summary", "scope")
    with pytest.raises(ReportArtifactValidationError):
        _report_artifact(required_sections=())
    with pytest.raises(ReportArtifactValidationError):
        _section(required="yes")


def test_allowed_report_artifact_classes_are_contract_classes():
    assert ALLOWED_REPORT_ARTIFACT_CLASSES == frozenset(
        {"repo_relative", "external_pattern", "cli_argument", "env_contract"}
    )


def test_allowed_report_formats_are_markdown_and_json_only():
    assert ALLOWED_REPORT_FORMATS == frozenset({"markdown", "json"})


def test_allowed_gross_or_net_values_are_explicit():
    assert ALLOWED_GROSS_OR_NET == frozenset({"gross", "net", "not_applicable"})


def test_no_metrics_calculation_is_performed():
    record = _metric_record(metric_value="precomputed_external_value")
    summary = _metrics_summary(metric_records=(record,))

    assert validate_metrics_summary(summary) is summary
    assert summary.metric_records[0].metric_value == "precomputed_external_value"


def test_no_report_file_is_written(tmp_path):
    before = set(tmp_path.iterdir())
    spec = _report_artifact()

    assert validate_report_artifact_spec(spec) is spec
    assert set(tmp_path.iterdir()) == before


def test_no_registry_entry_is_written():
    spec = _report_artifact()

    assert not hasattr(spec, "registry_entry")
    assert not hasattr(spec, "registry_writer")


def test_no_promotion_verdict_is_created():
    summary = _metrics_summary()
    spec = _report_artifact()

    assert not hasattr(summary, "promotion_verdict")
    assert not hasattr(spec, "promotion_verdict")


def test_source_has_no_forbidden_execution_responsibilities():
    source = "\n".join(
        [
            METRICS_SCHEMA_PATH.read_text(encoding="utf-8"),
            REPORT_ARTIFACT_PATH.read_text(encoding="utf-8"),
        ]
    ).casefold()
    forbidden_terms = (
        "run_backtest",
        "execute_backtest",
        "execute_strategy",
        "generate_signals",
        "calculate_pnl",
        "calculate_metrics_from_data",
        "write_report",
        "write_registry",
        "promotion_verdict",
        "broker",
        "order",
        "live",
        "server",
        "data_root",
        "latest",
        "current",
        "autodetect",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "open(",
        "glob(",
        "os.",
    )
    for term in forbidden_terms:
        assert term not in source
