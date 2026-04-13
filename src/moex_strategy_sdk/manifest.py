from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Final


_SEMVER_RE: Final[re.Pattern[str]] = re.compile(r"^\d+\.\d+\.\d+$")


@dataclass(frozen=True)
class StrategyManifest:
    strategy_id: str
    version: str
    instrument_scope: tuple[str, ...]
    timeframe: str
    required_datasets: tuple[str, ...]
    required_features: tuple[str, ...]
    required_labels: tuple[str, ...]
    supports_backtest: bool
    supports_live: bool
    report_schema_version: int
    artifact_contract_version: int
    tags: tuple[str, ...] = field(default_factory=tuple)
    owner: str | None = None
    default_portfolio_group: str | None = None
    default_risk_profile: str | None = None


def validate_strategy_manifest(manifest: StrategyManifest) -> StrategyManifest:
    if not isinstance(manifest, StrategyManifest):
        raise TypeError("manifest must be StrategyManifest")
    if not manifest.strategy_id:
        raise ValueError("strategy_id is required")
    if not _SEMVER_RE.match(manifest.version):
        raise ValueError("version must be semver")
    if not manifest.instrument_scope:
        raise ValueError("instrument_scope must be non-empty")
    if not manifest.timeframe:
        raise ValueError("timeframe is required")
    if not manifest.required_datasets:
        raise ValueError("required_datasets must be non-empty")
    if not manifest.required_features:
        raise ValueError("required_features must be non-empty")
    if manifest.report_schema_version < 1:
        raise ValueError("report_schema_version must be >= 1")
    if manifest.artifact_contract_version < 1:
        raise ValueError("artifact_contract_version must be >= 1")
    if not manifest.supports_backtest and not manifest.supports_live:
        raise ValueError("at least one mode must be supported")
    return manifest
