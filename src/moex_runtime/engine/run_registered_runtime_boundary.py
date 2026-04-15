from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from src.moex_core.contracts.external_pattern_artifact_path_resolver import resolve_external_pattern_artifact_path
from src.moex_core.contracts.registry_loader import load_registered_runtime_boundary
from src.moex_runtime.execution.runtime_position_transition import resolve_runtime_position_transition
from src.moex_runtime.state_store.file_backed_runtime_session_store import append_trade_log_row, load_runtime_state, next_trade_seq, read_last_trade_log_row, save_runtime_state
from src.moex_strategy_sdk.errors import StrategyRegistrationError
from src.moex_strategy_sdk.interfaces import LiveStrategyInput


def _to_strategy_inputs(feature_frame: pd.DataFrame) -> tuple[dict[str, object], ...]:
    columns = [column for column in ["instrument_id", "end", "open", "high", "low", "close", "volume"] if column in feature_frame.columns]
    return tuple(dict(row) for row in feature_frame[columns].to_dict(orient="records"))


def _trade_date_from_end(value: object) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        raise StrategyRegistrationError("runtime boundary requires timezone-aware finalized bars")
    return (ts - pd.Timedelta("1ns")).date().isoformat()


def run_registered_runtime_boundary(*, strategy_id: str, portfolio_id: str, environment_id: str) -> dict[str, object]:
    resolved = load_registered_runtime_boundary(strategy_id=strategy_id, portfolio_id=portfolio_id, environment_id=environment_id)
    instrument_id = str(resolved.instrument_record["instrument_id"])
    dataset_path = resolve_external_pattern_artifact_path(locator_ref=str(resolved.dataset_contract["locator_ref"]), environment_record=resolved.environment_record, format_kwargs={})
    feature_frame = resolved.runtime_feature_builder(dataset_artifact_path=dataset_path, instrument_id=instrument_id, timezone_name=str(resolved.instrument_record["timezone"]))
    if feature_frame.empty:
        raise StrategyRegistrationError("runtime feature frame is empty")
    signals = resolved.runtime_signal_builder(inputs=_to_strategy_inputs(feature_frame), config=resolved.strategy_config)
    latest_bar = feature_frame.iloc[-1]
    latest_bar_end = pd.Timestamp(latest_bar["end"])
    latest_bar_end_iso = latest_bar_end.isoformat()
    trade_date = _trade_date_from_end(latest_bar_end)
    state_path = resolve_external_pattern_artifact_path(locator_ref=resolved.runtime_state_contract.locator_ref, environment_record=resolved.environment_record, format_kwargs={"trade_date": trade_date})
    trade_log_path = resolve_external_pattern_artifact_path(locator_ref=resolved.runtime_trade_log_contract.locator_ref, environment_record=resolved.environment_record, format_kwargs={"trade_date": trade_date})
    prior_state = load_runtime_state(state_path)
    last_trade_log_row = read_last_trade_log_row(trade_log_path)
    decision = resolved.runtime_live_decision_builder(inputs=LiveStrategyInput(instrument_id=instrument_id, decision_ts=latest_bar_end.to_pydatetime(), state=prior_state, runtime_metadata={"strategy_id": strategy_id, "portfolio_id": portfolio_id, "environment_id": environment_id}), signals=signals, config=resolved.strategy_config)
    next_seq = next_trade_seq(prior_state=prior_state, last_trade_log_row=last_trade_log_row)
    transition = resolve_runtime_position_transition(prior_state=prior_state, last_trade_log_row=last_trade_log_row, decision=decision, strategy_id=strategy_id, portfolio_id=portfolio_id, environment_id=environment_id, instrument_id=instrument_id, trade_date=trade_date, latest_bar_end_iso=latest_bar_end_iso, next_trade_seq=next_seq, updated_at_iso=datetime.now(timezone.utc).isoformat())
    if transition.position_changed:
        close_price = latest_bar.get("close")
        if not isinstance(close_price, (int, float)) or isinstance(close_price, bool):
            raise StrategyRegistrationError("latest finalized close must be numeric")
        append_trade_log_row(trade_log_path, {"trade_date": trade_date, "seq": next_seq, "bar_end": latest_bar_end_iso, "action": transition.action, "prev_pos": transition.current_position, "new_pos": transition.desired_position, "price": float(close_price), "reason_code": decision.reason_code})
    save_runtime_state(state_path, transition.updated_state)
    return {"strategy_id": strategy_id, "portfolio_id": portfolio_id, "environment_id": environment_id, "dataset_path": str(dataset_path), "state_path": str(state_path), "trade_log_path": str(trade_log_path), "signal_count": int(len(signals)), "current_position": transition.current_position, "desired_position": transition.desired_position, "position_changed": transition.position_changed, "action": transition.action}
