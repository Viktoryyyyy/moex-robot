from __future__ import annotations

from datetime import datetime

from src.moex_strategy_sdk.errors import InterfaceValidationError
from src.moex_strategy_sdk.interfaces import LiveAdapterDecision, LiveStrategyInput, StrategySignalFrame
from src.strategies.reference_flat_15m_validation.config import StrategyConfig
from src.strategies.reference_flat_15m_validation.manifest import STRATEGY_MANIFEST

_REASON_CODE = "architecture_validation_force_flat"


def _coerce_decision_ts(value: object, fallback: datetime | None) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise InterfaceValidationError("invalid signal decision_ts") from exc
    if isinstance(fallback, datetime):
        return fallback
    raise InterfaceValidationError("decision_ts is required")


def build_live_decision(*, inputs: LiveStrategyInput, signals: StrategySignalFrame, config: StrategyConfig) -> LiveAdapterDecision:
    if inputs.instrument_id != config.instrument_id:
        raise InterfaceValidationError("live input instrument_id does not match config.instrument_id")
    decision_ts = _coerce_decision_ts(None, inputs.decision_ts)
    state_patch = {
        "last_desired_position": 0.0,
        "last_reason_code": _REASON_CODE,
        "last_signal_count": int(len(signals)),
    }
    return LiveAdapterDecision(
        strategy_id=STRATEGY_MANIFEST.strategy_id,
        strategy_version=STRATEGY_MANIFEST.version,
        instrument_id=config.instrument_id,
        decision_ts=decision_ts,
        desired_position=0.0,
        reason_code=_REASON_CODE,
        supports_execution=True,
        state_patch=state_patch,
    )
