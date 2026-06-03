from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from moex_backtest.engine.canonical import CanonicalBacktestEngine
from moex_backtest.engine.interfaces import BacktestResult
from moex_research.metrics.summary import build_backtest_metrics_summary
from moex_research.publishers.artifact_manifest import build_artifact_manifest
from moex_research.publishers.review_artifact import build_pm_review_artifact
from moex_research.registry.dry_write import DryExperimentRegistryWriter, DryRegistryWriteResult
from moex_research.registry.schemas import (
    ArtifactManifest,
    ArtifactManifestItem,
    ExperimentRegistryEntry,
)
from moex_research.registry.validation import validate_registry_entry_against_manifest
from strategies.reference_fixture_strategy.backtest_adapter import ReferenceFixtureBacktestAdapter
from strategies.reference_fixture_strategy.manifest import MANIFEST, STRATEGY_ID
from strategies.reference_fixture_strategy.signal_engine import ReferenceFixtureSignalEngine


class ResearchRunnerValidationError(ValueError):
    pass


_ALLOWED_STRATEGY_PACKAGE_REF = "strategies.reference_fixture_strategy"
_ALLOWED_REGISTRY_WRITE_MODES = frozenset({"dry_write", "controlled_write"})
_FORBIDDEN_MARKERS = ("latest", "current", "autodetect")
_RESULT_STATUS = "blocked"
_CANONICALITY_STATUS = "non_canonical"
_RUN_STATUS = "executed"
_PRODUCER_COMPONENT = "moex_research.runners.minimal_dry_run"


@dataclass(frozen=True)
class ResearchRunRequest:
    run_id: str
    strategy_package_ref: str
    strategy_id: str
    strategy_version: str
    config_ref: str
    dataset_ref: str
    feature_ref: str
    backtest_config_ref: str
    repo_commit: str
    created_ts: str
    features: Mapping[str, Any]
    bars: Sequence[Mapping[str, object]]
    strategy_config: Mapping[str, object] = field(default_factory=dict)
    backtest_config: Mapping[str, object] = field(default_factory=dict)
    artifact_root: str = "artifacts/research/dry_runs"
    registry_write_mode: str = "dry_write"
    production_registry_write_allowed: bool = False
    runtime_live_allowed: bool = False
    promotion_verdict_ref: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "run_id",
            "strategy_package_ref",
            "strategy_id",
            "strategy_version",
            "config_ref",
            "dataset_ref",
            "feature_ref",
            "backtest_config_ref",
            "repo_commit",
            "created_ts",
            "artifact_root",
            "registry_write_mode",
        ):
            _require_text(getattr(self, field_name), field_name)
        if self.strategy_package_ref != _ALLOWED_STRATEGY_PACKAGE_REF:
            raise ResearchRunnerValidationError("only reference_fixture_strategy package is supported in minimal slice")
        if self.strategy_id != STRATEGY_ID:
            raise ResearchRunnerValidationError("strategy_id must match reference fixture strategy")
        if self.strategy_version != MANIFEST.version:
            raise ResearchRunnerValidationError("strategy_version must match reference fixture strategy manifest")
        if self.dataset_ref not in MANIFEST.required_datasets:
            raise ResearchRunnerValidationError("dataset_ref must be declared by the strategy manifest")
        if self.feature_ref not in MANIFEST.required_features:
            raise ResearchRunnerValidationError("feature_ref must be declared by the strategy manifest")
        if self.registry_write_mode not in _ALLOWED_REGISTRY_WRITE_MODES:
            raise ResearchRunnerValidationError("unsupported registry_write_mode")
        if not isinstance(self.production_registry_write_allowed, bool):
            raise ResearchRunnerValidationError("production_registry_write_allowed must be boolean")
        if self.production_registry_write_allowed:
            raise ResearchRunnerValidationError("production registry write is blocked in minimal dry-run slice")
        if not isinstance(self.runtime_live_allowed, bool):
            raise ResearchRunnerValidationError("runtime_live_allowed must be boolean")
        if self.runtime_live_allowed:
            raise ResearchRunnerValidationError("runtime/live path is blocked in minimal dry-run slice")
        if self.promotion_verdict_ref is not None:
            raise ResearchRunnerValidationError("promotion verdict reference is not accepted in minimal dry-run slice")
        if not isinstance(self.features, Mapping):
            raise ResearchRunnerValidationError("features must be an explicit in-memory mapping")
        rows = self.features.get("rows")
        if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
            raise ResearchRunnerValidationError("features.rows must be an explicit sequence")
        if isinstance(self.bars, (str, bytes)) or not isinstance(self.bars, Sequence):
            raise ResearchRunnerValidationError("bars must be an explicit in-memory sequence")
        if not self.bars:
            raise ResearchRunnerValidationError("bars must be non-empty")
        if not isinstance(self.strategy_config, Mapping):
            raise ResearchRunnerValidationError("strategy_config must be a mapping")
        if not isinstance(self.backtest_config, Mapping):
            raise ResearchRunnerValidationError("backtest_config must be a mapping")
        for value in (
            self.config_ref,
            self.dataset_ref,
            self.feature_ref,
            self.backtest_config_ref,
            self.artifact_root,
        ):
            _reject_dynamic_marker(value)
        _validate_artifact_root(self.artifact_root)


@dataclass(frozen=True)
class ResearchRunResult:
    run_id: str
    request: ResearchRunRequest
    signals: tuple[dict[str, object], ...]
    backtest_result: BacktestResult
    metrics_summary: object
    artifact_manifest: ArtifactManifest
    experiment_registry_entry: ExperimentRegistryEntry
    registry_write_result: DryRegistryWriteResult
    pm_review_artifact: Mapping[str, object]
    outputs_created: tuple[str, ...]


class MinimalResearchRunner:
    runner_id = _PRODUCER_COMPONENT

    def run(self, request: ResearchRunRequest) -> ResearchRunResult:
        if not isinstance(request, ResearchRunRequest):
            raise ResearchRunnerValidationError("request must be ResearchRunRequest")
        signals = ReferenceFixtureSignalEngine().generate_signals(
            request.features,
            request.strategy_config,
        )
        backtest_input = ReferenceFixtureBacktestAdapter().to_backtest_inputs(
            signals=signals,
            context={
                "bars": tuple(request.bars),
                "cost_config": request.backtest_config.get("cost_config", {}),
                "execution_config": request.backtest_config.get("execution_config", {}),
            },
        )
        backtest_result = CanonicalBacktestEngine().run(backtest_input)
        artifact_paths = _build_artifact_paths(request)
        metrics_summary = build_backtest_metrics_summary(
            run_id=request.run_id,
            strategy_id=request.strategy_id,
            result_status=_RESULT_STATUS,
            canonicality_status=_CANONICALITY_STATUS,
            metrics_artifact_ref=artifact_paths["metrics"],
            backtest_result=backtest_result,
        )
        artifact_manifest = _build_manifest(request, artifact_paths)
        registry_entry = _build_registry_entry(request, artifact_manifest, backtest_result, artifact_paths)
        validate_registry_entry_against_manifest(registry_entry, artifact_manifest)
        registry_write_result = DryExperimentRegistryWriter().write(
            registry_entry,
            artifact_manifest,
            mode=request.registry_write_mode,
        )
        pm_review_artifact = build_pm_review_artifact(
            run_id=request.run_id,
            artifact_ref=artifact_paths["pm_review"],
            artifact_manifest=artifact_manifest,
            registry_entry=registry_entry,
            metrics_summary=metrics_summary,
        )
        return ResearchRunResult(
            run_id=request.run_id,
            request=request,
            signals=tuple(dict(row) for row in signals),
            backtest_result=backtest_result,
            metrics_summary=metrics_summary,
            artifact_manifest=artifact_manifest,
            experiment_registry_entry=registry_entry,
            registry_write_result=registry_write_result,
            pm_review_artifact=pm_review_artifact,
            outputs_created=tuple(artifact_paths.values()),
        )


def _build_artifact_paths(request: ResearchRunRequest) -> dict[str, str]:
    root = request.artifact_root.rstrip("/")
    run_root = root + "/" + request.run_id
    return {
        "run_metadata": run_root + "/run_metadata.json",
        "signals": run_root + "/signals.json",
        "backtest_result": run_root + "/canonical_backtest_result.json",
        "metrics": run_root + "/metrics_summary.json",
        "pm_review": run_root + "/pm_review_artifact.json",
        "experiment_registry": run_root + "/experiment_registry_entry.json",
    }


def _build_manifest(request: ResearchRunRequest, artifact_paths: Mapping[str, str]) -> ArtifactManifest:
    artifacts = (
        _manifest_item("run_metadata", "metadata_table", "json", artifact_paths["run_metadata"], True),
        _manifest_item("signals", "result_table", "json", artifact_paths["signals"], False),
        _manifest_item("primary_result", "backtest_result", "json", artifact_paths["backtest_result"], True),
        _manifest_item("metrics", "summary_table", "json", artifact_paths["metrics"], True),
        _manifest_item("pm_review", "report_artifact", "json", artifact_paths["pm_review"], False),
        _manifest_item(
            "experiment_registry_entry",
            "registry_entry",
            "json",
            artifact_paths["experiment_registry"],
            False,
        ),
    )
    return build_artifact_manifest(
        artifact_manifest_id="artifact_manifest." + request.run_id + ".v1",
        run_id=request.run_id,
        schema_version="research_artifact_manifest.v1",
        created_ts=request.created_ts,
        producer_component=_PRODUCER_COMPONENT,
        repo_commit=request.repo_commit,
        artifacts=artifacts,
    )


def _manifest_item(
    artifact_role: str,
    artifact_class: str,
    artifact_format: str,
    path: str,
    required_for_canonical: bool,
) -> ArtifactManifestItem:
    return ArtifactManifestItem(
        artifact_id="research_runner." + artifact_role + ".v1",
        artifact_role=artifact_role,
        artifact_class=artifact_class,
        producer=_PRODUCER_COMPONENT,
        consumer="PM_L3_DELIVERY_VALIDATION_OWNER",
        format=artifact_format,
        schema_version="research_runner_writer_minimal.v1",
        path=path,
        required_for_canonical=required_for_canonical,
    )


def _build_registry_entry(
    request: ResearchRunRequest,
    manifest: ArtifactManifest,
    backtest_result: BacktestResult,
    artifact_paths: Mapping[str, str],
) -> ExperimentRegistryEntry:
    return ExperimentRegistryEntry(
        registry_entry_id="experiment_registry." + request.run_id + ".v1",
        run_id=request.run_id,
        strategy_id=request.strategy_id,
        strategy_version=request.strategy_version,
        test_type="fixture_dry_run_research_runner",
        instrument_scope=MANIFEST.instrument_scope,
        timeframe_scope=(MANIFEST.timeframe,),
        run_status=_RUN_STATUS,
        result_status=_RESULT_STATUS,
        canonicality_status=_CANONICALITY_STATUS,
        artifact_manifest_ref=manifest.artifact_manifest_id,
        repo_commit=request.repo_commit,
        created_ts=request.created_ts,
        metrics={
            "signal_count": backtest_result.metrics["signal_count"],
            "trade_count": backtest_result.metrics["trade_count"],
            "ending_equity": backtest_result.metrics["ending_equity"],
            "metrics_artifact_ref": artifact_paths["metrics"],
            "fixture_only": True,
        },
    )


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchRunnerValidationError(field_name + " is required")
    return value


def _reject_dynamic_marker(value: str) -> None:
    normalized = value.casefold()
    segments = [segment for segment in normalized.replace("\\", "/").split("/") if segment]
    if any(marker in segment for marker in _FORBIDDEN_MARKERS for segment in segments):
        raise ResearchRunnerValidationError("dynamic latest/current/autodetect markers are forbidden")


def _validate_artifact_root(value: str) -> None:
    normalized = value.replace("\\", "/")
    if normalized.casefold() == "stdout":
        raise ResearchRunnerValidationError("stdout-only artifact root is forbidden")
    if normalized.startswith("/"):
        raise ResearchRunnerValidationError("artifact_root must not be an absolute server path")
    if normalized in {".", "./"} or normalized.endswith("/"):
        raise ResearchRunnerValidationError("artifact_root must be an explicit relative artifact root")
    if any(token in normalized for token in ("*", "?", "[", "]", "{", "}")):
        raise ResearchRunnerValidationError("artifact_root must not contain glob or template tokens")


__all__ = [
    "MinimalResearchRunner",
    "ResearchRunRequest",
    "ResearchRunResult",
    "ResearchRunnerValidationError",
]
