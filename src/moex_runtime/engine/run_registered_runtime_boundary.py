from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

import pandas as pd

from src.moex_core.contracts.registry_loader import load_registered_runtime_boundary
from src.moex_features.intraday.si_15m_ohlc_from_5m import materialize_feature_frame
from src.moex_strategy_sdk.errors import StrategyRegistrationError
from src.moex_strategy_sdk.interfaces import LiveStrategyInput
from src.strategies.ema_3_19_15m.live_adapter import build_live_decision
from src.strategies.ema_3_19_15m.signal_engine import generate_signals


FIELDNAMES = ["trade_date", "seq", "bar_end", "action", "prev_pos", "new_pos", "price", "reason_code"]


def _resolve_external_pattern_path(*, locator_ref: str, environment_record: Mapping[str, object], format_kwargs: Mapping[str, object]) -> Path:
    if not isinstance(locator_ref, str) or not locator_ref:
        raise StrategyRegistrationError("locator_ref is required")
    artifact_root_refs = environment_record.get("artifact_root_refs")
    if not isinstance(artifact_root_refs, list) or len(artifact_root_refs) != 1:
        raise StrategyRegistrationError("runtime boundary requires exactly one artifact_root_ref")
    artifact_root_key = artifact_root_refs[0]
    if not isinstance(artifact_root_key, str) or not artifact_root_key:
        raise StrategyRegistrationError("invalid artifact_root_ref")
    artifact_root = os.environ.get(artifact_root_key)
    if not artifact_root:
        raise StrategyRegistrationError("missing required artifact root env var: " + artifact_root_key)
    return Path(artifact_root) / locator_ref.format(**format_kwargs)


def _to_strategy_inputs(feature_frame: pd.DataFrame) -> tuple[dict[str, object], ...]:
    rows: list[dict[str, object]] = []
    columns = [column for column in ["instrument_id", "end", "open", "high", "low", "close", "volume"] if column in feature_frame.columns]
    for row in feature_frame[columns].to_dict(orient="records"):
        rows.append(row)
    return tuple(rows)


def _trade_date_from_end(value: object) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        raise StrategyRegistrationError("runtime boundary requires timezone-aware finalized bars")
    return (ts - pd.Timedelta("1ns")).date().isoformat()


def _coerce_position(value: object) -> float:
    if value is None:
        return 0.0
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise StrategyRegistrationError("runtime state current_position must be numeric")
    out = float(value)
    if out not in (-1.0, 0.0, 1.0):
        raise StrategyRegistrationError("runtime state current_position must be one of -1.0, 0.0, 1.0")
    return out


def _load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise StrategyRegistrationError("runtime state payload must be object")
    return dict(payload)


def _save_state(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(dict(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.replace(path)


def _read_last_trade_log_row(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return None
    return dict(rows[-1])


def _next_seq(prior_state: Mapping[str, object], last_trade_log_row: Mapping[str, str] | None) -> int:
    if last_trade_log_row is not None:
        raw = last_trade_log_row.get("seq")
        if raw is None:
            raise StrategyRegistrationError("runtime trade log last row missing seq")
        try:
            return int(raw) + 1
        except ValueError as exc:
            raise StrategyRegistrationError("runtime trade log seq must be int") from exc
    raw_state_seq = prior_state.get("last_trade_seq", 0)
    if isinstance(raw_state_seq, bool) or not isinstance(raw_state_seq, int) or raw_state_seq < 0:
        raise StrategyRegistrationError("runtime state last_trade_seq must be non-negative int")
    return raw_state_seq + 1


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


def _append_trade_log_row(path: Path, row: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerow(dict(row))


def run_registered_runtime_boundary(*, strategy_id: str, portfolio_id: str, environment_id: str) -> dict[str, object]:
    resolved = load_registered_runtime_boundary(strategy_id=strategy_id, portfolio_id=portfolio_id, environment_id=environment_id)
    dataset_path = _resolve_external_pattern_path(locator_ref=str(resolved.dataset_contract["locator_ref"]), environment_record=resolved.environment_record, format_kwargs={})
    feature_frame = materialize_feature_frame(dataset_artifact_path=dataset_path, instrument_id=str(resolved.instrument_record["instrument_id"]), timezone_name=str(resolved.instrument_record["timezone"]))
    if feature_frame.empty:
        raise StrategyRegistrationError("runtime feature frame is empty")
    strategy_inputs = _to_strategy_inputs(feature_frame)
    signals = generate_signals(inputs=strategy_inputs, config=resolved.strategy_config)
    latest_bar = feature_frame.iloc[-1]
    latest_bar_end = pd.Timestamp(latest_bar["end"])
    trade_date = _trade_date_from_end(latest_bar_end)
    state_path = _resolve_external_pattern_path(locator_ref=resolved.runtime_state_contract.locator_ref, environment_record=resolved.environment_record, format_kwargs={"trade_date": trade_date})
    trade_log_path = _resolve_external_pattern_path(locator_ref=resolved.runtime_trade_log_contract.locator_ref, environment_record=resolved.environment_record, format_kwargs={"trade_date": trade_date})
    prior_state = _load_state(state_path)
    last_trade_log_row = _read_last_trade_log_row(trade_log_path)
    current_position = _coerce_position(prior_state.get("current_position"))
    if current_position == 0.0 and last_trade_log_row is not None and "new_pos" in last_trade_log_row:
        try:
            current_position = _coerce_position(float(last_trade_log_row["new_pos"]))
        except ValueError as exc:
            raise StrategyRegistrationError("runtime trade log new_pos must be numeric") from exc
    decision = build_live_decision(inputs=LiveStrategyInput(instrument_id=str(resolved.instrument_record["instrument_id"]), decision_ts=latest_bar_end.to_pydatetime(), state=prior_state, runtime_metadata={"strategy_id": strategy_id, "portfolio_id": portfolio_id, "environment_id": environment_id}), signals=signals, config=resolved.strategy_config)
    next_position = float(decision.desired_position)
    position_changed = next_position != current_position
    next_seq = _next_seq(prior_state, last_trade_log_row)
    action = None
    if position_changed:
        close_price = latest_bar.get("close")
        if not isinstance(close_price, (int, float)) or isinstance(close_price, bool):
            raise StrategyRegistrationError("latest finalized close must be numeric")
        action = _action(current_position, next_position)
        _append_trade_log_row(trade_log_path, {"trade_date": trade_date, "seq": next_seq, "bar_end": latest_bar_end.isoformat(), "action": action, "prev_pos": current_position, "new_pos": next_position, "price": float(close_price), "reason_code": decision.reason_code})
    updated_state = dict(prior_state)
    updated_state.update(dict(decision.state_patch))
    updated_state.update({"strategy_id": strategy_id, "portfolio_id": portfolio_id, "environment_id": environment_id, "instrument_id": str(resolved.instrument_record["instrument_id"]), "trade_date": trade_date, "current_position": next_position, "last_bar_end": latest_bar_end.isoformat(), "last_decision_ts": decision.decision_ts.isoformat(), "last_reason_code": decision.reason_code, "last_trade_seq": next_seq if position_changed else int(prior_state.get("last_trade_seq", 0)), "updated_at": datetime.now(timezone.utc).isoformat()})
    _save_state(state_path, updated_state)
    return {"strategy_id": strategy_id, "portfolio_id": portfolio_id, "environment_id": environment_id, "dataset_path": str(dataset_path), "state_path": str(state_path), "trade_log_path": str(trade_log_path), "signal_count": int(len(signals)), "current_position": current_position, "desired_position": next_position, "position_changed": position_changed, "action": action}
