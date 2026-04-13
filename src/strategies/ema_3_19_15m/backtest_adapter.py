from __future__ import annotations

from datetime import datetime

from src.moex_strategy_sdk.errors import InterfaceValidationError
from src.moex_strategy_sdk.interfaces import BacktestAdapterRequest, StrategyInputFrame, StrategySignalFrame
from src.strategies.ema_3_19_15m.config import StrategyConfig
from src.strategies.ema_3_19_15m.manifest import STRATEGY_MANIFEST


def _normalize_signals(signals: StrategySignalFrame) -> StrategySignalFrame:
    normalized: list[dict[str, object]] = []
    previous_ts: datetime | None = None
    for row in signals:
        instrument_id = row.get("instrument_id")
        decision_ts = row.get("decision_ts")
        desired_position = row.get("desired_position")

        if not isinstance(instrument_id, str) or not instrument_id.strip():
            raise InterfaceValidationError("signal instrument_id is required")
        if not isinstance(decision_ts, datetime):
            raise InterfaceValidationError("signal decision_ts must be datetime")
        if previous_ts is not None and decision_ts < previous_ts:
            raise InterfaceValidationError("signals must be sorted by decision_ts")
        if not isinstance(desired_position, (int, float)) or isinstance(desired_position, bool):
            raise InterfaceValidationError("signal desired_position must be numeric")

        normalized_row = {
            "instrument_id": instrument_id,
            "decision_ts": decision_ts,
            "desired_position": float(desired_position),
        }
        if "signal_code" in row:
            normalized_row["signal_code"] = row["signal_code"]
        if "signal_strength" in row:
            normalized_row["signal_strength"] = row["signal_strength"]
        if "reason_code" in row:
            normalized_row["reason_code"] = row["reason_code"]
        normalized.append(normalized_row)
        previous_ts = decision_ts

    return tuple(normalized)


def build_backtest_request(
    *,
    inputs: StrategyInputFrame,
    signals: StrategySignalFrame,
    config: StrategyConfig,
) -> BacktestAdapterRequest:
    del inputs
    del config
    normalized_signals = _normalize_signals(signals)
    return BacktestAdapterRequest(
        strategy_id=STRATEGY_MANIFEST.strategy_id,
        strategy_version=STRATEGY_MANIFEST.version,
        normalized_signals=normalized_signals,
        hook_overrides={},
    )
