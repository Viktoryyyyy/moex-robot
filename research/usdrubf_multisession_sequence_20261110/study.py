"""Causal research for USDRUBF reversals formed by several daily sessions.

Unlike the earlier five-session-extreme experiment, setup creation here does
not depend on a rolling high or low.  A setup is a 3-5 session sequence: a
directional drive followed by a two-session stall/turn.  Confirmation may only
arrive in a later session and is evaluated on native AlgoPack five-minute bars.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.usdrubf_multiday_reversal_20261110 import study as base  # noqa: E402


PATTERNS = ("stall_turn", "two_close_turn")
CONFIRMATIONS = (
    "price",
    "price_delta",
    "price_vwap_flow",
    "session_multifactor",
)
FOLDS = base.FOLDS
HOLDOUT = base.HOLDOUT


_WORKER_MARKET: tuple[pd.DataFrame, pd.DataFrame] | None = None
_WORKER_FUNDING: dict[str, float | None] | None = None
_WORKER_CATALOGS: dict[tuple[str, int], list[dict[str, Any]]] | None = None


@dataclass(frozen=True)
class Variant:
    pattern: str
    formation: int
    confirmation: str
    life: int
    stop_buffer: float
    hold: int

    @property
    def key(self) -> str:
        return (
            f"{self.pattern}|formation={self.formation}|confirm={self.confirmation}"
            f"|life={self.life}|buffer={self.stop_buffer:g}|hold={self.hold}"
        )


def build_market(paths: Sequence[Path]) -> tuple[pd.DataFrame, pd.DataFrame]:
    bars, daily = base.build_market(paths)
    bars["session_cum_delta"] = bars.groupby("date", sort=False)["delta"].cumsum()
    return bars, daily


def _finite(value: Any) -> bool:
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def detect_setup(
    daily: pd.DataFrame,
    end: int,
    side: int,
    pattern: str,
    formation: int,
    *,
    drive_atr: float = 0.75,
    maximum_progress_atr: float = 0.35,
    recovery_fraction: float = 0.55,
) -> dict[str, Any] | None:
    """Return a completed multi-session setup without using future rows.

    The first part of the window must contain pressure in the direction that is
    expected to reverse.  The last two sessions then have to show limited new
    progress and an improving close (stall_turn), or two consecutive improving
    closes (two_close_turn).  No rolling-extreme condition is consulted.
    """
    if pattern not in PATTERNS:
        raise ValueError("unsupported pattern: " + pattern)
    start = end - formation + 1
    if formation < 3 or start < 0:
        return None
    window = daily.iloc[start : end + 1]
    if len(window) != formation:
        return None
    atr = float(window.iloc[-1]["atr20_prior"])
    if not np.isfinite(atr) or atr <= 0:
        return None

    first = window.iloc[0]
    previous = window.iloc[-2]
    last = window.iloc[-1]
    transition = window.iloc[-2:]
    closes = window["close"].to_numpy(dtype=float)
    prior_changes = np.diff(closes[:-1])
    first_body = float(first["close"] - first["open"])

    if side == 1:
        extreme = float(window["low"].min())
        drive = float(first["open"]) - extreme
        recent_extreme = float(transition["low"].min()) <= extreme + 1e-9
        pressure_evidence = first_body < 0 or bool(np.any(prior_changes < 0))
        progress = max(0.0, float(previous["low"]) - float(last["low"]))
        transition_low = float(transition["low"].min())
        transition_high = float(transition["high"].max())
        span = transition_high - transition_low
        recovered = span > 0 and (float(last["close"]) - transition_low) / span >= recovery_fraction
        improving_last = float(last["close"]) > float(previous["close"])
        double_turn = (
            formation >= 3
            and float(window.iloc[-2]["close"]) > float(window.iloc[-3]["close"])
            and improving_last
        )
        trigger = transition_high + 0.01
    elif side == -1:
        extreme = float(window["high"].max())
        drive = extreme - float(first["open"])
        recent_extreme = float(transition["high"].max()) >= extreme - 1e-9
        pressure_evidence = first_body > 0 or bool(np.any(prior_changes > 0))
        progress = max(0.0, float(last["high"]) - float(previous["high"]))
        transition_low = float(transition["low"].min())
        transition_high = float(transition["high"].max())
        span = transition_high - transition_low
        recovered = span > 0 and (transition_high - float(last["close"])) / span >= recovery_fraction
        improving_last = float(last["close"]) < float(previous["close"])
        double_turn = (
            formation >= 3
            and float(window.iloc[-2]["close"]) < float(window.iloc[-3]["close"])
            and improving_last
        )
        trigger = transition_low - 0.01
    else:
        raise ValueError("side must be 1 or -1")

    if drive < drive_atr * atr or not recent_extreme or not pressure_evidence:
        return None
    if progress > maximum_progress_atr * atr or not recovered:
        return None
    if pattern == "stall_turn" and not improving_last:
        return None
    if pattern == "two_close_turn" and not double_turn:
        return None

    return {
        "side": side,
        "setup_session": int(end),
        "setup_date": str(last["date"]),
        "formation_start_session": int(start),
        "formation_start_date": str(first["date"]),
        "formation_sessions": int(formation),
        "formation_high": float(window["high"].max()),
        "formation_low": float(window["low"].min()),
        "transition_high": transition_high,
        "transition_low": transition_low,
        "setup_atr": atr,
        "drive_atr": float(drive / atr),
        "progress_atr": float(progress / atr),
        "recovery_fraction": float(
            (float(last["close"]) - transition_low) / span
            if side == 1 else (transition_high - float(last["close"])) / span
        ),
        "trigger": float(trigger),
        "setup_rvol20": float(last["rvol20"]) if _finite(last["rvol20"]) else None,
        "setup_delta": float(last["delta"]),
        "setup_oi_pct": float(last["oi_pct"]) if _finite(last["oi_pct"]) else None,
    }


def setup_catalog(daily: pd.DataFrame, pattern: str, formation: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for end in range(formation - 1, len(daily)):
        for side in (1, -1):
            setup = detect_setup(daily, end, side, pattern, formation)
            if setup is not None:
                result.append(setup)
    return result


def _worker_init(input_paths: Sequence[str], history_paths: Sequence[str]) -> None:
    global _WORKER_MARKET, _WORKER_FUNDING, _WORKER_CATALOGS
    _WORKER_MARKET = build_market([Path(value) for value in input_paths])
    _WORKER_FUNDING = base.load_funding([Path(value) for value in history_paths])
    _WORKER_CATALOGS = {
        (pattern, formation): setup_catalog(_WORKER_MARKET[1], pattern, formation)
        for pattern in PATTERNS
        for formation in (3, 4, 5)
    }


def _worker_run(variant: Variant) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    if _WORKER_MARKET is None or _WORKER_FUNDING is None or _WORKER_CATALOGS is None:
        raise RuntimeError("research worker was not initialized")
    bars, daily = _WORKER_MARKET
    trades, signals = run_variant(
        bars,
        daily,
        _WORKER_FUNDING,
        variant,
        _WORKER_CATALOGS[(variant.pattern, variant.formation)],
    )
    return variant.key, trades, signals


def confirmed(
    mode: str,
    side: int,
    bar: Any,
    setup: Mapping[str, Any],
    day: Mapping[str, Any],
) -> bool:
    if mode not in CONFIRMATIONS:
        raise ValueError("unsupported confirmation: " + mode)
    close = float(base._field(bar, "close"))
    trigger = float(setup["trigger"])
    price = close >= trigger if side == 1 else close <= trigger
    bar_delta = float(base._field(bar, "delta"))
    delta = bar_delta > 0 if side == 1 else bar_delta < 0
    running_vwap = float(base._field(bar, "running_vwap"))
    vwap = close > running_vwap if side == 1 else close < running_vwap
    cumulative_delta = float(base._field(bar, "session_cum_delta"))
    flow = cumulative_delta > 0 if side == 1 else cumulative_delta < 0
    if mode == "price":
        return price
    if mode == "price_delta":
        return price and delta
    if mode == "price_vwap_flow":
        return price and vwap and flow
    day_delta = float(base._field(day, "delta"))
    daily_flow = day_delta > 0 if side == 1 else day_delta < 0
    day_close = float(base._field(day, "close"))
    day_vwap = float(base._field(day, "vwap"))
    daily_vwap = day_close > day_vwap if side == 1 else day_close < day_vwap
    return bool(
        base._field(bar, "is_last")
        and price
        and daily_vwap
        and daily_flow
        and float(base._field(day, "rvol20")) >= 1.1
        and float(base._field(day, "oi_pct")) <= -0.5
    )


def _invalidated(setup: Mapping[str, Any], side: int, bar: Any, stop_buffer: float) -> bool:
    atr = float(setup["setup_atr"])
    if side == 1:
        level = float(setup["formation_low"]) - stop_buffer * atr
        return float(base._field(bar, "low")) <= level
    level = float(setup["formation_high"]) + stop_buffer * atr
    return float(base._field(bar, "high")) >= level


def _candidate_confirmations(
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    variant: Variant,
    setups: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Vectorize eligible confirmation bars for each causally active setup."""
    sessions = bars["session"].to_numpy(dtype=int)
    changes = np.r_[True, sessions[1:] != sessions[:-1]]
    starts = np.flatnonzero(changes)
    ends = np.r_[starts[1:] - 1, len(bars) - 1]
    close = bars["close"].to_numpy(dtype=float)
    high = bars["high"].to_numpy(dtype=float)
    low = bars["low"].to_numpy(dtype=float)
    delta = bars["delta"].to_numpy(dtype=float)
    running_vwap = bars["running_vwap"].to_numpy(dtype=float)
    cumulative_delta = bars["session_cum_delta"].to_numpy(dtype=float)
    is_last = bars["is_last"].to_numpy(dtype=bool)
    daily_close = daily["close"].to_numpy(dtype=float)[sessions]
    daily_vwap = daily["vwap"].to_numpy(dtype=float)[sessions]
    daily_delta = daily["delta"].to_numpy(dtype=float)[sessions]
    daily_rvol = daily["rvol20"].to_numpy(dtype=float)[sessions]
    daily_oi = daily["oi_pct"].to_numpy(dtype=float)[sessions]

    ordered = sorted((dict(item) for item in setups), key=lambda item: int(item["setup_session"]))
    next_same: dict[int, int] = {}
    next_session_by_row: dict[int, int | None] = {}
    for row in reversed(ordered):
        side = int(row["side"])
        next_session_by_row[id(row)] = next_same.get(side)
        next_same[side] = int(row["setup_session"])

    candidates: list[dict[str, Any]] = []
    last_session = len(daily) - 1
    for setup in ordered:
        side = int(setup["side"])
        start_session = int(setup["setup_session"]) + 1
        end_session = min(int(setup["setup_session"]) + variant.life, last_session)
        replacement = next_session_by_row[id(setup)]
        if replacement is not None:
            # A setup completed in session N replaces the older one at the
            # start of N+1, so the older setup remains eligible through N.
            end_session = min(end_session, int(replacement))
        if start_session > end_session:
            continue
        left = int(starts[start_session])
        right = int(ends[end_session]) + 1
        if left >= len(bars) - 1:
            continue
        local = slice(left, right)
        if side == 1:
            invalid = low[local] <= float(setup["formation_low"]) - variant.stop_buffer * float(setup["setup_atr"])
            price = close[local] >= float(setup["trigger"])
            correct_delta = delta[local] > 0
            vwap_side = close[local] > running_vwap[local]
            flow_side = cumulative_delta[local] > 0
            daily_vwap_side = daily_close[local] > daily_vwap[local]
            daily_flow_side = daily_delta[local] > 0
        else:
            invalid = high[local] >= float(setup["formation_high"]) + variant.stop_buffer * float(setup["setup_atr"])
            price = close[local] <= float(setup["trigger"])
            correct_delta = delta[local] < 0
            vwap_side = close[local] < running_vwap[local]
            flow_side = cumulative_delta[local] < 0
            daily_vwap_side = daily_close[local] < daily_vwap[local]
            daily_flow_side = daily_delta[local] < 0
        invalid_positions = np.flatnonzero(invalid)
        eligible_length = int(invalid_positions[0]) if len(invalid_positions) else right - left
        if eligible_length <= 0:
            continue
        if variant.confirmation == "price":
            mask = price
        elif variant.confirmation == "price_delta":
            mask = price & correct_delta
        elif variant.confirmation == "price_vwap_flow":
            mask = price & vwap_side & flow_side
        else:
            mask = (
                price
                & is_last[local]
                & daily_vwap_side
                & daily_flow_side
                & (daily_rvol[local] >= 1.1)
                & (daily_oi[local] <= -0.5)
            )
        indices = left + np.flatnonzero(mask[:eligible_length])
        indices = indices[indices < len(bars) - 1]
        if len(indices):
            candidates.append({"setup": setup, "indices": indices})
    return candidates


def run_variant(
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    funding: Mapping[str, float | None],
    variant: Variant,
    setups: Sequence[Mapping[str, Any]],
    *,
    cost: float = 0.025,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Event-driven simulator equivalent to scanning every five-minute bar."""
    candidates = _candidate_confirmations(bars, daily, variant, setups)
    consumed = [False] * len(candidates)
    signals: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    sessions = bars["session"].to_numpy(dtype=int)
    changes = np.r_[True, sessions[1:] != sessions[:-1]]
    starts = np.flatnonzero(changes)
    ends = np.r_[starts[1:] - 1, len(bars) - 1]
    lows = bars["low"].to_numpy(dtype=float)
    highs = bars["high"].to_numpy(dtype=float)
    opens = bars["open"].to_numpy(dtype=float)
    closes = bars["close"].to_numpy(dtype=float)
    days = daily.to_dict("records")
    cursor = 0

    while cursor < len(bars) - 1:
        choices: list[tuple[int, int]] = []
        for candidate_index, candidate in enumerate(candidates):
            if consumed[candidate_index]:
                continue
            indices = candidate["indices"]
            offset = int(np.searchsorted(indices, cursor, side="left"))
            if offset < len(indices):
                choices.append((int(indices[offset]), candidate_index))
        if not choices:
            break
        confirmation_index = min(item[0] for item in choices)
        matches = [item[1] for item in choices if item[0] == confirmation_index]
        confirmation_bar = bars.iloc[confirmation_index]
        if len(matches) > 1:
            signals.append(
                {
                    "variant": variant.key,
                    "side": "conflict",
                    "confirmation_date": str(confirmation_bar["date"]),
                    "confirmation_ts": str(confirmation_bar["ts"]),
                    "status": "skipped_simultaneous_opposites",
                }
            )
            cursor = confirmation_index + 1
            continue

        candidate_index = matches[0]
        consumed[candidate_index] = True
        setup = candidates[candidate_index]["setup"]
        side = int(setup["side"])
        session = int(confirmation_bar["session"])
        day = days[session]
        signal = {
            "variant": variant.key,
            "pattern": variant.pattern,
            "confirmation": variant.confirmation,
            "side": "long" if side == 1 else "short",
            **{key: value for key, value in setup.items() if key != "side"},
            "confirmation_date": str(confirmation_bar["date"]),
            "confirmation_ts": str(confirmation_bar["ts"]),
            "confirmation_close": float(confirmation_bar["close"]),
            "confirmation_vwap": float(confirmation_bar["running_vwap"]),
            "bar_delta": float(confirmation_bar["delta"]),
            "session_cum_delta": float(confirmation_bar["session_cum_delta"]),
            "daily_delta": float(day["delta"]),
            "daily_rvol20": float(day["rvol20"]),
            "daily_oi_pct": float(day["oi_pct"]),
            "status": "scheduled",
        }
        signals.append(signal)
        entry_index = confirmation_index + 1
        entry_session = int(sessions[entry_index])
        entry = float(opens[entry_index])
        raw_stop = (
            float(setup["formation_low"]) - variant.stop_buffer * float(setup["setup_atr"])
            if side == 1
            else float(setup["formation_high"]) + variant.stop_buffer * float(setup["setup_atr"])
        )
        stop = base._round_stop(raw_stop, side)
        risk = side * (entry - stop)
        if risk <= 0 or risk > 3.0 * float(setup["setup_atr"]):
            signals[-1]["status"] = "rejected_risk"
            cursor = entry_index
            continue

        signals[-1]["status"] = "entered"
        target_session = entry_session + variant.hold - 1
        final_session = min(target_session, len(daily) - 1)
        exit_limit = int(ends[final_session])
        if side == 1:
            hit_positions = np.flatnonzero(lows[entry_index : exit_limit + 1] <= stop)
        else:
            hit_positions = np.flatnonzero(highs[entry_index : exit_limit + 1] >= stop)
        if len(hit_positions):
            exit_index = entry_index + int(hit_positions[0])
            gap = side * (float(opens[exit_index]) - stop) <= 0
            exit_price = float(opens[exit_index]) if gap else stop
            reason = "stop_gap" if gap else "stop"
        else:
            exit_index = exit_limit
            exit_price = float(closes[exit_index])
            reason = "time" if target_session <= len(daily) - 1 else "data_end"
        position = {
            **setup,
            "entry_index": int(entry_index),
            "entry_session": entry_session,
            "entry_date": str(bars.iloc[entry_index]["date"]),
            "entry_ts": str(bars.iloc[entry_index]["ts"]),
            "entry": entry,
            "stop": stop,
            "risk": risk,
            "confirmation_date": str(confirmation_bar["date"]),
            "confirmation_ts": str(confirmation_bar["ts"]),
        }
        trades.append(
            base._close_trade(position, exit_index, exit_price, reason, bars, daily, funding, cost)
        )
        cursor = exit_index
    return trades, signals


def metrics(
    trades: Iterable[Mapping[str, Any]], start: str | None = None, end: str | None = None
) -> dict[str, Any]:
    return base.metrics(trades, start, end)


def walk_forward_trades(trades: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return base.walk_forward_trades(trades)


def _direct_neighbors(variant: Variant, variants: Sequence[Variant]) -> list[Variant]:
    result: list[Variant] = []
    for candidate in variants:
        differences = sum(
            left != right
            for left, right in (
                (candidate.pattern, variant.pattern),
                (candidate.formation, variant.formation),
                (candidate.confirmation, variant.confirmation),
                (candidate.life, variant.life),
                (candidate.stop_buffer, variant.stop_buffer),
                (candidate.hold, variant.hold),
            )
        )
        if differences == 1:
            result.append(candidate)
    return result


def select_variant(
    variants: Sequence[Variant], trade_map: Mapping[str, Sequence[Mapping[str, Any]]]
) -> tuple[Variant, pd.DataFrame, pd.DataFrame]:
    fold_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []
    combined_by_key: dict[str, dict[str, Any]] = {}
    for variant in variants:
        weighted_score = 0.0
        combined: list[Mapping[str, Any]] = []
        positive_folds = 0
        for name, start, end, weight in FOLDS:
            fold_trades = [
                trade
                for trade in trade_map[variant.key]
                if str(trade["entry_date"]) >= start and str(trade["exit_date"]) <= end
            ]
            value = metrics(fold_trades)
            fold_rows.append({"variant": variant.key, "fold": name, **value})
            combined.extend(fold_trades)
            weighted_score += weight * float(value["net_r"]) / max(3.0, float(value["max_drawdown_r"]))
            positive_folds += int(float(value["net_r"]) > 0)
        combined_metrics = metrics(combined)
        combined_by_key[variant.key] = combined_metrics
        selection_rows.append(
            {
                "variant": variant.key,
                **asdict(variant),
                "weighted_score": weighted_score,
                "positive_folds": positive_folds,
                **{f"wf_{key}": value for key, value in combined_metrics.items()},
            }
        )
    for row, variant in zip(selection_rows, variants):
        neighbors = [combined_by_key[item.key]["net_r"] for item in _direct_neighbors(variant, variants)]
        positive_fraction = float(np.mean(np.asarray(neighbors) > 0)) if neighbors else 0.0
        row["neighbor_count"] = len(neighbors)
        row["positive_neighbor_fraction"] = positive_fraction
        row["neighbor_median_net_r"] = float(np.median(neighbors)) if neighbors else None
        row["eligible"] = bool(
            int(row["wf_trades"]) >= 15
            and int(row["wf_long_trades"]) >= 3
            and int(row["wf_short_trades"]) >= 3
            and float(row["wf_without_best_r"]) > 0
            and row["wf_top3_positive_profit_share"] is not None
            and float(row["wf_top3_positive_profit_share"]) <= 0.7
            and positive_fraction >= 0.5
        )
    selection = pd.DataFrame(selection_rows).sort_values(
        ["eligible", "weighted_score"], ascending=[False, False]
    )
    winner_key = str(selection.iloc[0]["variant"])
    winner = next(item for item in variants if item.key == winner_key)
    return winner, pd.DataFrame(fold_rows), selection


def _json_value(value: Any) -> Any:
    return base._json_value(value)


def write_json(path: Path, payload: Any) -> None:
    base.write_json(path, payload)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tradestats", action="append", required=True)
    parser.add_argument("--history", action="append", required=True)
    parser.add_argument("--rules", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1))
    args = parser.parse_args(argv)

    input_paths = [Path(value).resolve(strict=True) for value in args.tradestats]
    history_paths = [Path(value).resolve(strict=True) for value in args.history]
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise FileExistsError("output directory must be empty: " + str(output))

    bars, daily = build_market(input_paths)
    funding = base.load_funding(history_paths)
    variants = [
        Variant(pattern, formation, confirmation, life, buffer, hold)
        for pattern in PATTERNS
        for formation in (3, 4, 5)
        for confirmation in CONFIRMATIONS
        for life in (3, 5)
        for buffer in (0.1, 0.25)
        for hold in (3, 5)
    ]
    catalogs = {
        (pattern, formation): setup_catalog(daily, pattern, formation)
        for pattern in PATTERNS
        for formation in (3, 4, 5)
    }
    trade_map: dict[str, list[dict[str, Any]]] = {}
    signal_map: dict[str, list[dict[str, Any]]] = {}
    if args.workers <= 1:
        results = (
            (
                variant.key,
                *run_variant(
                    bars,
                    daily,
                    funding,
                    variant,
                    catalogs[(variant.pattern, variant.formation)],
                ),
            )
            for variant in variants
        )
        for index, (key, trades, signals) in enumerate(results, start=1):
            trade_map[key] = trades
            signal_map[key] = signals
            if index % 24 == 0:
                print(f"completed {index}/{len(variants)} variants", file=sys.stderr, flush=True)
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers,
            initializer=_worker_init,
            initargs=(tuple(str(path) for path in input_paths), tuple(str(path) for path in history_paths)),
        ) as executor:
            for index, (key, trades, signals) in enumerate(executor.map(_worker_run, variants), start=1):
                trade_map[key] = trades
                signal_map[key] = signals
                if index % 24 == 0:
                    print(f"completed {index}/{len(variants)} variants", file=sys.stderr, flush=True)

    winner, folds, selection = select_variant(variants, trade_map)
    folds.to_csv(output / "variant_fold_metrics.csv", index=False)
    selection.to_csv(output / "variant_selection.csv", index=False)
    pd.DataFrame(
        [
            {"variant": item.key, **asdict(item), **metrics(trade_map[item.key], *HOLDOUT)}
            for item in variants
        ]
    ).sort_values("net_r", ascending=False).to_csv(output / "variant_holdout_metrics.csv", index=False)

    setup_rows = [
        {"pattern": pattern, "formation": formation, **setup}
        for (pattern, formation), values in catalogs.items()
        for setup in values
    ]
    pd.DataFrame(setup_rows).to_csv(output / "setup_catalog.csv", index=False)
    best_trades = trade_map[winner.key]
    best_signals = signal_map[winner.key]
    pd.DataFrame(best_trades).to_csv(output / "best_variant_trades.csv", index=False)
    pd.DataFrame(best_signals).to_csv(output / "best_variant_signals.csv", index=False)
    pd.DataFrame(
        [
            signal
            for item in variants
            for signal in signal_map[item.key]
            if HOLDOUT[0] <= str(signal.get("confirmation_date", "")) <= HOLDOUT[1]
        ]
    ).to_csv(output / "holdout_signal_catalog.csv", index=False)

    rules = json.loads(Path(args.rules).read_text(encoding="utf-8"))
    old_signals = base.one_day_signals(bars, daily, rules)
    old_trades = base.simulate_one_day(bars, daily, funding, old_signals)
    pd.DataFrame(old_trades).to_csv(output / "one_day_monitor_trades.csv", index=False)
    pd.DataFrame(old_signals).to_csv(output / "one_day_monitor_signals.csv", index=False)

    stress_trades, _ = run_variant(
        bars,
        daily,
        funding,
        winner,
        catalogs[(winner.pattern, winner.formation)],
        cost=0.05,
    )
    pd.DataFrame(stress_trades).to_csv(output / "best_variant_stress_trades.csv", index=False)
    winner_row = selection.loc[selection["variant"].eq(winner.key)].iloc[0]
    summary = {
        "schema_version": "usdrubf_multisession_sequence_study.v1",
        "data_range": [str(daily.iloc[0]["date"]), str(daily.iloc[-1]["date"])],
        "bars": len(bars),
        "sessions": len(daily),
        "variants": len(variants),
        "setup_definition": "3-5 session drive plus two-session stall/turn; no rolling-extreme requirement",
        "catalog_counts": {
            f"{pattern}|{formation}": len(values)
            for (pattern, formation), values in catalogs.items()
        },
        "selected_variant": asdict(winner),
        "selected_key": winner.key,
        "selection_result": (
            "eligible_variant_found"
            if bool(winner_row["eligible"])
            else "no_variant_passed_predeclared_gates_best_ranked_sent_to_holdout_for_diagnosis"
        ),
        "selected_walk_forward": metrics(walk_forward_trades(best_trades)),
        "selected_holdout": metrics(best_trades, *HOLDOUT),
        "selected_full": metrics(best_trades, "2023-04-01", HOLDOUT[1]),
        "selected_stress_cost_holdout": metrics(stress_trades, *HOLDOUT),
        "one_day_walk_forward": metrics(walk_forward_trades(old_trades)),
        "one_day_holdout": metrics(old_trades, *HOLDOUT),
        "one_day_full": metrics(old_trades, "2023-04-01", HOLDOUT[1]),
    }
    write_json(output / "summary.json", summary)
    write_json(
        output / "data_audit.json",
        {
            "inputs": {
                str(path): base.sha256(path)
                for path in [*input_paths, *history_paths, Path(args.rules).resolve(strict=True)]
            },
            "first_bar": str(bars.iloc[0]["ts"]),
            "last_bar": str(bars.iloc[-1]["ts"]),
            "bars": len(bars),
            "sessions": len(daily),
            "duplicate_timestamps": int(bars["ts"].duplicated().sum()),
            "holdout": list(HOLDOUT),
            "holdout_used_in_selection": False,
            "rolling_extreme_used_for_setup": False,
        },
    )
    print(json.dumps(_json_value(summary), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
