from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Final, Iterable

from .errors import ManifestValidationError

_SEMVER_RE: Final[re.Pattern[str]] = re.compile(r"^\d+\.\d+\.\d+$")

REQUIRED_MANIFEST_FIELDS: Final[tuple[str, ...]] = (
    "strategy_id",
    "version",
    "instrument_scope",
    "timeframe",
    "required_datasets",
    "required_features",
    "required_labels",
    "supports_backtest",
    "supports_live",
    "report_schema_version",
    "artifact_contract_version",
)


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ManifestValidationError(f"{field_name} is required")
    return value


def _require_tuple(value: Iterable[str], field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise ManifestValidationError(f"{field_name} must be a non-empty tuple of strings")
    try:
        items = tuple(value)
    except TypeError as exc:
        raise ManifestValidationError(f"{field_name} must be a non-empty tuple of strings") from exc
    if not items:
        raise ManifestValidationError(f"{field_name} must be non-empty")
    if any(not isinstance(item, str) or not item.strip() for item in items):
        raise ManifestValidationError(f"{field_name} must contain non-empty strings")
    return items


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
    report_schema_version: str | int
    artifact_contract_version: str | int
    tags: tuple[str, ...] = field(default_factory=tuple)
    owner: str | None = None
    default_portfolio_group: str | None = None
    default_risk_profile: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_id", _require_text(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "version", _require_text(self.version, "version"))
        object.__setattr__(self, "instrument_scope", _require_tuple(self.instrument_scope, "instrument_scope"))
        object.__setattr__(self, "timeframe", _require_text(self.timeframe, "timeframe"))
        object.__setattr__(self, "required_datasets", _require_tuple(self.required_datasets, "required_datasets"))
        object.__setattr__(self, "required_features", _require_tuple(self.required_features, "required_features"))
        object.__setattr__(self, "required_labels", _require_tuple(self.required_labels, "required_labels"))
        if not isinstance(self.supports_backtest, bool):
            raise ManifestValidationError("supports_backtest must be bool")
        if not isinstance(self.supports_live, bool):
            raise ManifestValidationError("supports_live must be bool")
        if isinstance(self.report_schema_version, int):
            if self.report_schema_version < 1:
                raise ManifestValidationError("report_schema_version must be >= 1")
        else:
            _require_text(self.report_schema_version, "report_schema_version")
        if isinstance(self.artifact_contract_version, int):
            if self.artifact_contract_version < 1:
                raise ManifestValidationError("artifact_contract_version must be >= 1")
        else:
            _require_text(self.artifact_contract_version, "artifact_contract_version")
        object.__setattr__(self, "tags", tuple(self.tags))


def validate_strategy_manifest(manifest: StrategyManifest) -> StrategyManifest:
    if not isinstance(manifest, StrategyManifest):
        raise TypeError("manifest must be StrategyManifest")
    if not _SEMVER_RE.match(manifest.version):
        raise ManifestValidationError("version must be semver")
    return manifest
