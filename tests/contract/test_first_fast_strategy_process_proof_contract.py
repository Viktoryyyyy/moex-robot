from pathlib import Path

import pytest

from moex_backtest.engine.interfaces import BacktestResult
from moex_core.contracts import RegistryContractError, validate_registry_entry_values
from moex_research.runners.dry_run import (
    MinimalResearchRunner,
    ResearchRunRequest,
    ResearchRunnerValidationError,
)
from moex_research.registry.schemas import ArtifactManifest, ExperimentRegistryEntry
from moex_strategy_sdk.errors import UnsupportedModeError
from strategies.reference_fixture_strategy.live_adapter import ReferenceFixtureLiveAdapter
from strategies.reference_fixture_strategy.manifest import MANIFEST, STRATEGY_ID

PROCESS_PROOF_RUN_ID = "first_fast_strategy_process_proof_2026_06_04_001"
PROCESS_PROOF_STRATEGY_CONFIG_REF = "configs/strategies/reference_fixture_strategy.process_proof.v1.yaml"
PROCESS_PROOF_BACKTEST_CONFIG_REF = "configs/backtests/reference_fixture_strategy.process_proof.v1.yaml"
PROCESS_PROOF_ARTIFACT_ROOT = "artifacts/research/process_proofs"
PROCESS_PROOF_REPO_COMMIT = "699eba0652c414ec72543d4ff556dc9ffeb44fac"
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _declared_strategy_registry_entry(**overrides):
    values = {
        "entry_id": "strategy.reference_fixture_strategy.v1",
        "registry_kind": "strategy",
        "config_id": "strategy_registry.reference_fixture_strategy.v1",
        "artifact_class": "repo_relative",
        "repo_path": PROCESS_PROOF_STRATEGY_CONFIG_REF,
        "dependencies": {
            "dataset": (MANIFEST.required_datasets[0],),
            "feature": (MANIFEST.required_features[0],),
            "instrument": ("instrument.Si.v1",),
        },
        "enabled": True,
        "registry_mutation_allowed": False,
        "promotion_ref_or_none": None,
        "strategy": {
            "strategy_id": STRATEGY_ID,
            "package_ref": "strategies.reference_fixture_strategy",
            "version": MANIFEST.version,
            "process_proof_only": True,
        },
    }
    values.update(overrides)
    return values


def _request(**overrides):
    values = {
        "run_id": PROCESS_PROOF_RUN_ID,
        "strategy_package_ref": "strategies.reference_fixture_strategy",
        "strategy_id": STRATEGY_ID,
        "strategy_version": MANIFEST.version,
        "config_ref": PROCESS_PROOF_STRATEGY_CONFIG_REF,
        "dataset_ref": MANIFEST.required_datasets[0],
        "feature_ref": MANIFEST.required_features[0],
        "backtest_config_ref": PROCESS_PROOF_BACKTEST_CONFIG_REF,
        "repo_commit": PROCESS_PROOF_REPO_COMMIT,
        "created_ts": "2026-06-04T00:00:00Z",
        "features": {"rows": ({"timestamp": 1}, {"timestamp": 2}, {"timestamp": 3})},
        "bars": (
            {"timestamp": 1, "open": 100.0, "close": 100.0},
            {"timestamp": 2, "open": 101.0, "close": 101.0},
            {"timestamp": 3, "open": 102.0, "close": 102.0},
            {"timestamp": 4, "open": 103.0, "close": 103.0},
        ),
        "strategy_config": {},
        "backtest_config": {},
        "artifact_root": PROCESS_PROOF_ARTIFACT_ROOT,
        "registry_write_mode": "dry_write",
        "production_registry_write_allowed": False,
        "non_research_execution_allowed": False,
    }
    values.update(overrides)
    return ResearchRunRequest(**values)


def _validate_process_proof_selection(registry_entry_values, request):
    entry = validate_registry_entry_values(registry_entry_values)
    if entry.entry_id != "strategy.reference_fixture_strategy.v1":
        raise RegistryContractError("unexpected strategy registry entry")
    if entry.repo_path != request.config_ref:
        raise RegistryContractError("strategy config ref must match declared registry repo_path")
    if request.dataset_ref not in entry.dependencies["dataset"]:
        raise RegistryContractError("dataset ref is not declared by strategy registry entry")
    if request.feature_ref not in entry.dependencies["feature"]:
        raise RegistryContractError("feature ref is not declared by strategy registry entry")
    if request.strategy_id != STRATEGY_ID:
        raise RegistryContractError("strategy_id mismatch")
    payload = entry.payload or {}
    if payload.get("strategy_id") != STRATEGY_ID:
        raise RegistryContractError("strategy payload does not declare reference fixture strategy")
    if payload.get("package_ref") != request.strategy_package_ref:
        raise RegistryContractError("strategy package ref mismatch")
    if payload.get("version") != request.strategy_version:
        raise RegistryContractError("strategy version mismatch")
    return entry


def _assert_repo_relative_config_ref_materialized(config_ref):
    normalized = config_ref.replace("\\", "/")
    path = Path(normalized)
    assert normalized == config_ref
    assert not path.is_absolute()
    assert not any(marker in path.parts for marker in ("latest", "current", "autodetect"))
    assert not any(token in normalized for token in ("*", "?", "[", "]", "{", "}"))
    resolved = _REPO_ROOT / path
    assert resolved.is_file()
    assert "repo_path: " + normalized in resolved.read_text(encoding="utf-8")


def test_first_fast_strategy_process_proof_config_refs_are_materialized_repo_relative_files():
    _assert_repo_relative_config_ref_materialized(PROCESS_PROOF_STRATEGY_CONFIG_REF)
    _assert_repo_relative_config_ref_materialized(PROCESS_PROOF_BACKTEST_CONFIG_REF)


def test_first_fast_strategy_process_proof_connects_selection_runner_backtest_and_artifacts():
    request = _request()
    registry_entry = _validate_process_proof_selection(_declared_strategy_registry_entry(), request)

    result = MinimalResearchRunner().run(request)

    assert registry_entry.repo_path == PROCESS_PROOF_STRATEGY_CONFIG_REF
    assert result.request.config_ref == registry_entry.repo_path
    assert result.request.dataset_ref == MANIFEST.required_datasets[0]
    assert result.request.feature_ref == MANIFEST.required_features[0]
    assert result.request.backtest_config_ref == PROCESS_PROOF_BACKTEST_CONFIG_REF
    assert result.signals == (
        {"strategy_id": STRATEGY_ID, "timestamp": 1, "target_position": 0, "reason_code": "reference_fixture_noop"},
        {"strategy_id": STRATEGY_ID, "timestamp": 2, "target_position": 0, "reason_code": "reference_fixture_noop"},
        {"strategy_id": STRATEGY_ID, "timestamp": 3, "target_position": 0, "reason_code": "reference_fixture_noop"},
    )
    assert isinstance(result.backtest_result, BacktestResult)
    assert result.backtest_result.metrics["engine_id"] == "canonical_backtest_engine_minimal_v1"
    assert result.backtest_result.metrics["signal_count"] == 3
    assert isinstance(result.artifact_manifest, ArtifactManifest)
    assert isinstance(result.experiment_registry_entry, ExperimentRegistryEntry)
    assert result.experiment_registry_entry.artifact_manifest_ref == result.artifact_manifest.artifact_manifest_id
    assert result.experiment_registry_entry.result_status == "blocked"
    assert result.experiment_registry_entry.canonicality_status == "non_canonical"
    assert result.registry_write_result.persisted is False
    assert result.pm_review_artifact["artifact_manifest_ref"] == result.artifact_manifest.artifact_manifest_id
    assert result.pm_review_artifact["run_status"] == "executed"
    assert result.pm_review_artifact["result_status"] == "blocked"
    assert result.pm_review_artifact["execution_path_enabled"] is False
    assert result.pm_review_artifact["decision_action_enabled"] is False
    assert tuple(item.artifact_role for item in result.artifact_manifest.artifacts) == (
        "run_metadata",
        "signals",
        "primary_result",
        "metrics",
        "pm_review",
        "experiment_registry_entry",
    )
    assert result.outputs_created == tuple(item.path for item in result.artifact_manifest.artifacts)
    assert all(path.startswith(PROCESS_PROOF_ARTIFACT_ROOT + "/" + PROCESS_PROOF_RUN_ID + "/") for path in result.outputs_created)
    assert all(path.endswith(".json") for path in result.outputs_created)
    assert "market_conclusion" not in result.pm_review_artifact
    assert "market_conclusion" not in result.experiment_registry_entry.metrics
    assert result.experiment_registry_entry.promotion_verdict_ref is None


def test_first_fast_strategy_process_proof_fails_closed_for_missing_or_dangling_refs():
    request = _request()

    with pytest.raises(RegistryContractError):
        _validate_process_proof_selection(
            _declared_strategy_registry_entry(repo_path="configs/strategies/other_strategy.v1.yaml"),
            request,
        )
    with pytest.raises(RegistryContractError):
        _validate_process_proof_selection(
            _declared_strategy_registry_entry(dependencies={"dataset": ("dataset.dangling.v1",), "feature": (MANIFEST.required_features[0],), "instrument": ("instrument.Si.v1",)}),
            request,
        )
    with pytest.raises(ResearchRunnerValidationError):
        _request(strategy_package_ref="strategies.other_strategy")
    with pytest.raises(ResearchRunnerValidationError):
        _request(strategy_id="other_strategy")
    with pytest.raises(ResearchRunnerValidationError):
        _request(strategy_version="999.0")
    with pytest.raises(ResearchRunnerValidationError):
        _request(config_ref="")
    with pytest.raises(ResearchRunnerValidationError):
        _request(dataset_ref="dataset.dangling.v1")
    with pytest.raises(ResearchRunnerValidationError):
        _request(feature_ref="feature.dangling.v1")


def test_first_fast_strategy_process_proof_rejects_stdout_dynamic_lookup_and_non_fixture_paths():
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="stdout")
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="artifacts/research/latest")
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="artifacts/research/current")
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="artifacts/research/autodetect")
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="artifacts/research/*")
    with pytest.raises(ResearchRunnerValidationError):
        _request(artifact_root="/home/trader/moex_bot/artifacts")
    with pytest.raises(ResearchRunnerValidationError):
        _request(features={"rows": "features.csv"})
    with pytest.raises(ResearchRunnerValidationError):
        _request(bars="bars.csv")


def test_first_fast_strategy_process_proof_keeps_runtime_live_promotion_and_market_result_blocked():
    result = MinimalResearchRunner().run(_request())

    assert MANIFEST.supports_live is False
    assert ReferenceFixtureLiveAdapter().supports_live is False
    with pytest.raises(UnsupportedModeError):
        ReferenceFixtureLiveAdapter().to_live_intents(result.signals, {})
    with pytest.raises(ResearchRunnerValidationError):
        _request(non_research_execution_allowed=True)
    with pytest.raises(ResearchRunnerValidationError):
        _request(production_registry_write_allowed=True)
    with pytest.raises(ResearchRunnerValidationError):
        _request(review_decision_ref="reports/decision.json")
    assert result.experiment_registry_entry.result_status == "blocked"
    assert result.experiment_registry_entry.canonicality_status == "non_canonical"
    assert result.pm_review_artifact["review_boundary"] == "PM review only; no market conclusion or strategy readiness claim"
    assert result.pm_review_artifact["decision_action_enabled"] is False
    assert result.pm_review_artifact["execution_path_enabled"] is False
