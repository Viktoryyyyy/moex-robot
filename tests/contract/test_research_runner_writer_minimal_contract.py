from pathlib import Path

import pytest

from moex_research.runners.dry_run import (
    MinimalResearchRunner,
    ResearchRunRequest,
    ResearchRunnerValidationError,
)
from strategies.reference_fixture_strategy.manifest import MANIFEST, STRATEGY_ID

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_ROOT = REPO_ROOT / "src" / "moex_research" / "runners"


def _request(**overrides):
    values = {
        "run_id": "fixture_dry_run_2026_06_03_001",
        "strategy_package_ref": "strategies.reference_fixture_strategy",
        "strategy_id": STRATEGY_ID,
        "strategy_version": MANIFEST.version,
        "config_ref": "configs/strategies/reference_fixture_strategy.v1.yaml",
        "dataset_ref": MANIFEST.required_datasets[0],
        "feature_ref": MANIFEST.required_features[0],
        "backtest_config_ref": "configs/backtests/reference_fixture_dry_run.v1.yaml",
        "repo_commit": "73344213145acea816f719fa8cf9369d2ab63964",
        "created_ts": "2026-06-03T00:00:00Z",
        "features": {"rows": ({"timestamp": 1}, {"timestamp": 2}, {"timestamp": 3})},
        "bars": (
            {"timestamp": 1, "open": 100.0, "close": 100.0},
            {"timestamp": 2, "open": 101.0, "close": 101.0},
            {"timestamp": 3, "open": 102.0, "close": 102.0},
            {"timestamp": 4, "open": 103.0, "close": 103.0},
        ),
        "strategy_config": {},
        "backtest_config": {},
        "artifact_root": "artifacts/research/dry_runs",
        "registry_write_mode": "dry_write",
        "production_registry_write_allowed": False,
        "non_research_execution_allowed": False,
    }
    values.update(overrides)
    return ResearchRunRequest(**values)


def test_minimal_research_runner_produces_structured_non_stdout_outputs():
    result = MinimalResearchRunner().run(_request())

    assert result.run_id == "fixture_dry_run_2026_06_03_001"
    assert result.signals == (
        {"strategy_id": STRATEGY_ID, "timestamp": 1, "target_position": 0, "reason_code": "reference_fixture_noop"},
        {"strategy_id": STRATEGY_ID, "timestamp": 2, "target_position": 0, "reason_code": "reference_fixture_noop"},
        {"strategy_id": STRATEGY_ID, "timestamp": 3, "target_position": 0, "reason_code": "reference_fixture_noop"},
    )
    assert result.backtest_result.metrics["engine_id"] == "canonical_backtest_engine_minimal_v1"
    assert result.backtest_result.metrics["signal_count"] == 3
    assert result.experiment_registry_entry.run_status == "executed"
    assert result.experiment_registry_entry.result_status == "blocked"
    assert result.experiment_registry_entry.canonicality_status == "non_canonical"
    assert result.registry_write_result.persisted is False
    assert result.pm_review_artifact["execution_path_enabled"] is False
    assert result.pm_review_artifact["decision_action_enabled"] is False
    assert result.outputs_created
    assert "stdout" not in result.outputs_created


def test_runner_is_deterministic_for_same_fixture_input():
    request = _request()
    first = MinimalResearchRunner().run(request)
    second = MinimalResearchRunner().run(request)

    assert first.signals == second.signals
    assert first.backtest_result.metrics == second.backtest_result.metrics
    assert first.artifact_manifest == second.artifact_manifest
    assert first.experiment_registry_entry == second.experiment_registry_entry
    assert first.outputs_created == second.outputs_created


def test_artifact_manifest_declares_explicit_producer_consumer_format_and_class():
    result = MinimalResearchRunner().run(_request())
    roles = {item.artifact_role for item in result.artifact_manifest.artifacts}

    assert roles == {
        "run_metadata",
        "signals",
        "primary_result",
        "metrics",
        "pm_review",
        "experiment_registry_entry",
    }
    for item in result.artifact_manifest.artifacts:
        assert item.producer == "moex_research.runners.minimal_dry_run"
        assert item.consumer == "PM_L3_DELIVERY_VALIDATION_OWNER"
        assert item.format == "json"
        assert item.artifact_class
        assert item.path.startswith("artifacts/research/dry_runs/fixture_dry_run_2026_06_03_001/")
        assert item.path.endswith(".json")


def test_metrics_summary_artifact_is_explicit_and_non_empty():
    result = MinimalResearchRunner().run(_request())

    assert result.metrics_summary.artifact_ref.endswith("/metrics_summary.json")
    assert len(result.metrics_summary.metric_records) == 6
    assert {record.metric_name for record in result.metrics_summary.metric_records} == {
        "signal_count",
        "trade_count",
        "rejected_signal_count",
        "ending_equity",
        "total_pnl",
        "total_cost",
    }
    for record in result.metrics_summary.metric_records:
        assert record.producer == "moex_research.metrics.summary"
        assert record.consumer == "PM_L3_DELIVERY_VALIDATION_OWNER"


def test_missing_or_dangling_refs_fail_closed():
    with pytest.raises(ResearchRunnerValidationError):
        _request(dataset_ref="dataset.undeclared.v1")
    with pytest.raises(ResearchRunnerValidationError):
        _request(feature_ref="feature.undeclared.v1")
    with pytest.raises(ResearchRunnerValidationError):
        _request(strategy_package_ref="strategies.other_strategy")
    with pytest.raises(ResearchRunnerValidationError):
        _request(strategy_id="other_strategy")
    with pytest.raises(ResearchRunnerValidationError):
        _request(strategy_version="999.0")


def test_no_data_file_path_or_implicit_lookup_is_accepted():
    with pytest.raises(ResearchRunnerValidationError):
        _request(features={"rows": "server/path/to/features.csv"})
    with pytest.raises(ResearchRunnerValidationError):
        _request(bars="server/path/to/bars.csv")
    with pytest.raises(ResearchRunnerValidationError):
        _request(config_ref="configs/strategies/latest.yaml")
    with pytest.raises(ResearchRunnerValidationError):
        _request(dataset_ref="dataset.current.v1")
    with pytest.raises(ResearchRunnerValidationError):
        _request(feature_ref="feature.autodetect.v1")
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="/home/trader/moex_bot/artifacts")


def test_stdout_only_and_dynamic_artifact_references_are_rejected():
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="stdout")
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="artifacts/research/latest")
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="artifacts/research/dry_runs/")
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="artifacts/research/*")


def test_non_research_execution_production_registry_and_registry_mode_paths_are_blocked():
    with pytest.raises(ResearchRunnerValidationError):
        _request(non_research_execution_allowed=True)
    with pytest.raises(ResearchRunnerValidationError):
        _request(production_registry_write_allowed=True)
    with pytest.raises(ResearchRunnerValidationError):
        _request(review_decision_ref="reports/decision.json")
    with pytest.raises(ResearchRunnerValidationError):
        _request(registry_write_mode="production_write")


def test_runner_source_has_no_file_io_network_operational_or_custom_pnl_engine_terms():
    forbidden_terms = (
        "open(",
        ".read_text(",
        ".write_text(",
        "requests",
        "urllib",
        "socket",
        "glob(",
        "os.",
        "moex_" + "run" + "time",
        "broker",
        "order_send",
        "custom_pnl",
    )
    for path in RUNNER_ROOT.glob("*.py"):
        source = path.read_text(encoding="utf-8").casefold()
        for term in forbidden_terms:
            assert term not in source
