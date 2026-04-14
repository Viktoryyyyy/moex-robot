from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from src.moex_strategy_sdk.errors import StrategyRegistrationError
from src.moex_strategy_sdk.interfaces import LiveAdapterDecision


@dataclass(frozen=True)
class RuntimePositionTransition:
    current_position: float
    desired_position: float
    position_changed: bool
    action: str | None
    updated_state: dict[str, object]


def _coerce_position(value: object) -> float:
    if value is None:
        return 0.0
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise StrategyRegistrationError("runtime state current_position must be numeric")
    out = float(value)
    if out not in (-1.0, 0.0, 1.0):
        raise StrategyRegistrationError("runtime state current_position must be one of -1.0, 0.0, 1.0")
    return out


def _action(prev_position: float, new_position: float) -> str:
    if prev_position == -1.0 and new_position == 1.0:
        return "REVERSE_TO_LONG"
    if prev_position == 1.0 and new_position == -1.0:
        return "REVERSE_TO_SHORT"
    if prev_position == 0.0 and new_position == 1.0:
        return "OPEN_LONG"
    if prev_position == 0.0 and new_position == -1.0:
        return "OPEN_SHORT"
    if prev_position == 1.0 and new_position == 0.0:
        return "CLOSE_LONG"
    if prev_position == -1.0 and new_position == 0.0:
        return "CLOSE_SHORT"
    raise StrategyRegistrationError("runtime position change action undefined")


def resolve_runtime_position_transition(
    *,
    prior_state: Mapping[str, object],
    last_trade_log_row: Mapping[str, str] | None,
    decision: LiveAdapterDecision,
    strategy_id: str,
    portfolio_id: str,
    environment_id: str,
    instrument_id: str,
    trade_date: str,
    latest_bar_end_iso: str,
    next_trade_seq: int,
    updated_at_iso: str,
) -> RuntimePositionTransition:
    current_position = _coerce_position(prior_state.get("current_position"))
    if current_position == 0.0 and last_trade_log_row is not None and "new_pos" in last_trade_log_row:
        try:
            current_position = _coerce_position(float(last_trade_log_row["new_pos"]))
        except ValueError as exc:
            raise StrategyRegistrationError("runtime trade log new_pos must be numeric") from exc
    desired_position = float(decision.desired_position)
    position_changed = desired_position != current_position
    action = _action(current_position, desired_position) if position_changed else None
    updated_state = dict(prior_state)
    updated_state.update(dict(decision.state_patch))
    updated_state.update(
        {
            "strategy_id": strategy_id,
            "portfolio_id": portfolio_id,
            "environment_id": environment_id,
            "instrument_id": instrument_id,
            "trade_date": trade_date,
            "current_position": desired_position,
            "last_bar_end": latest_bar_end_iso,
            "last_decision_ts": decision.decision_ts.isoformat(),
            "last_reason_code": decision.reason_code,
            "last_trade_seq": next_trade_seq if position_changed else int(prior_state.get("last_trade_seq", 0)),
            "updated_at": updated_at_iso,
        }
    )
    return RuntimePositionTransition(
        current_position=current_position,
        desired_position=desired_position,
        position_changed=position_changed,
        action=action,
        updated_state=updated_state,
    )
