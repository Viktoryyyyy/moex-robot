from dataclasses import dataclass, field
from typing import Iterable

from .errors import ManifestValidationError

REQUIRED_MANIFEST_FIELDS = (
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
        raise ManifestValidationError(f"{field_name} must be a non-empty string")
    return value


def _require_bool(value: bool, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ManifestValidationError(f"{field_name} must be a bool")
    return value


def _require_text_tuple(value: Iterable[str], field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise ManifestValidationError(f"{field_name} must be a non-empty iterable of strings")
    try:
        items = tuple(value)
    except TypeError as exc:
        raise ManifestValidationError(f"{field_name} must be a non-empty iterable of strings") from exc
    if not items:
        raise ManifestValidationError(f"{field_name} must not be empty")
    for item in items:
        if not isinstance(item, str) or not item.strip():
            raise ManifestValidationError(f"{field_name} must contain only non-empty strings")
    return items


@dataclass(frozen=True)
class StrategyManifest:
    strategy_id: str
    version: str
    instrument_scope: str
    timeframe: str
    required_datasets: tuple[str, ...]
    required_features: tuple[str, ...]
    required_labels: tuple[str, ...]
    supports_backtest: bool
    supports_live: bool
    report_schema_version: str
    artifact_contract_version: str
    tags: tuple[str, ...] = field(default_factory=tuple)
    owner: str | None = None
    default_portfolio_group: str | None = None
    default_risk_profile: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_id", _require_text(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "version", _require_text(self.version, "version"))
        object.__setattr__(self, "instrument_scope", _require_text(self.instrument_scope, "instrument_scope"))
        object.__setattr__(self, "timeframe", _require_text(self.timeframe, "timeframe"))
        object.__setattr__(self, "required_datasets", _require_text_tuple(self.required_datasets, "required_datasets"))
        object.__setattr__(self, "required_features", _require_text_tuple(self.required_features, "required_features"))
        object.__setattr__(self, "required_labels", _require_text_tuple(self.required_labels, "required_labels"))
        object.__setattr__(self, "supports_backtest", _require_bool(self.supports_backtest, "supports_backtest"))
        object.__setattr__(self, "supports_live", _require_bool(self.supports_live, "supports_live"))
        object.__setattr__(self, "report_schema_version", _require_text(self.report_schema_version, "report_schema_version"))
        object.__setattr__(
            self,
            "artifact_contract_version",
            _require_text(self.artifact_contract_version, "artifact_contract_version"),
        )
        object.__setattr__(self, "tags", tuple(self.tags))
