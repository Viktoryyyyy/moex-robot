from __future__ import annotations

from datetime import datetime

from src.moex_strategy_sdk.errors import InterfaceValidationError
from src.moex_strategy_sdk.interfaces import LiveAdapterDecision, LiveStrategyInput, StrategySignalFrame
from src.strategies.ema_3_19_15m.config import StrategyConfig
from src.strategies.ema_3_19_15m.manifest import STRATEGY_MANIFEST


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


def _coerce_desired_position(value: object) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise InterfaceValidationError("desired_position must be numeric")
    if float(value) not in (-1.0, 0.0, 1.0):
        raise InterfaceValidationError("desired_position must be one of -1.0, 0.0, 1.0")
    return float(value)


def _coerce_state_position(value: object) -> float:
    if value is None:
        return 0.0
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise InterfaceValidationError("last_desired_position must be numeric")
    if float(value) not in (-1.0, 0.0, 1.0):
        raise InterfaceValidationError("last_desired_position must be one of -1.0, 0.0, 1.0")
    return float(value)


def build_live_decision(*, inputs: LiveStrategyInput, signals: StrategySignalFrame, config: StrategyConfig) -> LiveAdapterDecision:
    if inputs.instrument_id != config.instrument_id:
        raise InterfaceValidationError("live input instrument_id does not match config.instrument_id")
    latest_signal = signals[-1] if signals else None
    if latest_signal is not None:
        signal_instrument_id = latest_signal.get("instrument_id")
        if signal_instrument_id != config.instrument_id:
            raise InterfaceValidationError("latest signal instrument_id does not match config.instrument_id")
        decision_ts = _coerce_decision_ts(latest_signal.get("decision_ts"), inputs.decision_ts)
        desired_position = _coerce_desired_position(latest_signal.get("desired_position"))
        reason_code_raw = latest_signal.get("reason_code") or latest_signal.get("signal_code") or "signal_present_without_reason_code"
        if not isinstance(reason_code_raw, str) or not reason_code_raw.strip():
            raise InterfaceValidationError("reason_code must be non-empty")
        reason_code = reason_code_raw
        state_patch = {"last_desired_position": desired_position, "last_signal_decision_ts": decision_ts.isoformat(), "last_signal_code": latest_signal.get("signal_code"), "last_reason_code": reason_code}
    else:
        decision_ts = _coerce_decision_ts(None, inputs.decision_ts)
        desired_position = _coerce_state_position(inputs.state.get("last_desired_position"))
        reason_code = "hold_last_desired_position"
        state_patch = {"last_desired_position": desired_position, "last_reason_code": reason_code}
    return LiveAdapterDecision(strategy_id=STRATEGY_MANIFEST.strategy_id, strategy_version=STRATEGY_MANIFEST.version, instrument_id=config.instrument_id, decision_ts=decision_ts, desired_position=desired_position, reason_code=reason_code, supports_execution=True, state_patch=state_patch)
