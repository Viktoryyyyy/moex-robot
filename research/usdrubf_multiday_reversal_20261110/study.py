"""Causal research engine for multi-session USDRUBF reversal setups.

This module is deliberately outside the operational monitor.  It reads frozen
inputs, writes a new result directory, and has no deployment or alerting code.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from moex_research.intelligence import usdrubf_reversal_candle_monitor as monitor


MODES = (
    "touch_setup_extreme",
    "bar_close_setup_extreme",
    "bar_close_extreme_delta",
    "session_close_extreme_vwap",
    "session_close_multifactor",
)
FOLDS = (
    ("2025", "2025-01-01", "2025-12-31", 1.0),
    ("2026_h1", "2026-01-01", "2026-06-30", 2.0),
    ("2026_q3", "2026-07-01", "2026-09-30", 3.0),
)
HOLDOUT = ("2026-10-01", "2026-11-09")


@dataclass(frozen=True)
class Variant:
    mode: str
    life: int
    stop_buffer: float
    hold: int

    @property
    def key(self) -> str:
        return f"{self.mode}|life={self.life}|buffer={self.stop_buffer:g}|hold={self.hold}"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _round_stop(value: float, side: int) -> float:
    scaled = value * 100.0
    return (math.floor(scaled + 1e-7) if side == 1 else math.ceil(scaled - 1e-7)) / 100.0


def load_tradestats(paths: Sequence[Path]) -> pd.DataFrame:
    frames = [pd.read_csv(path, low_memory=False) for path in paths]
    work = pd.concat(frames, ignore_index=True)
    work.columns = [str(column).strip().lower() for column in work.columns]
    required = {
        "tradedate", "tradetime", "secid", "pr_open", "pr_high", "pr_low",
        "pr_close", "vol", "val", "vol_b", "vol_s", "oi_open", "oi_close",
    }
    missing = sorted(required - set(work.columns))
    if missing:
        raise ValueError("TradeStats missing columns: " + ",".join(missing))
    work = work.loc[work["secid"].astype(str).str.upper().eq("USDRUBF")].copy()
    work["date"] = pd.to_datetime(work["tradedate"], errors="raise").dt.strftime("%Y-%m-%d")
    work["time"] = work["tradetime"].astype(str).str.slice(0, 8)
    work["ts"] = pd.to_datetime(work["date"] + " " + work["time"], errors="raise")
    numeric = [
        "pr_open", "pr_high", "pr_low", "pr_close", "vol", "val", "vol_b",
        "vol_s", "oi_open", "oi_close",
    ]
    for column in numeric:
        work[column] = pd.to_numeric(work[column], errors="raise")
    work = work.sort_values(["ts"]).drop_duplicates(["ts"], keep="last").reset_index(drop=True)
    if bool((work["vol"] <= 0).any()):
        raise ValueError("non-positive five-minute volume")
    if bool((work["vol_b"] + work["vol_s"] != work["vol"]).any()):
        raise ValueError("TradeStats volume identity failed")
    work = work.rename(
        columns={
            "pr_open": "open", "pr_high": "high", "pr_low": "low",
            "pr_close": "close", "vol": "volume",
        }
    )
    work["delta"] = work["vol_b"] - work["vol_s"]
    work["typical_vwap"] = work["val"] / (work["volume"] * 1000.0)
    if bool((work["typical_vwap"] <= 0).any()):
        raise ValueError("invalid value/volume VWAP")
    return work


def build_market(paths: Sequence[Path]) -> tuple[pd.DataFrame, pd.DataFrame]:
    bars = load_tradestats(paths)
    grouped = bars.groupby("date", sort=True)
    daily = grouped.agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"),
        close=("close", "last"), volume=("volume", "sum"), value=("val", "sum"),
        vol_b=("vol_b", "sum"), vol_s=("vol_s", "sum"),
        oi_open=("oi_open", "first"), oi_close=("oi_close", "last"),
        rows=("close", "size"), first_ts=("ts", "first"), last_ts=("ts", "last"),
    ).reset_index()
    daily["vwap"] = daily["value"] / (daily["volume"] * 1000.0)
    daily["delta"] = daily["vol_b"] - daily["vol_s"]
    previous_close = daily["close"].shift(1)
    daily["true_range"] = pd.concat(
        [
            daily["high"] - daily["low"],
            (daily["high"] - previous_close).abs(),
            (daily["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    daily["atr20_prior"] = daily["true_range"].shift(1).rolling(20).mean()
    daily["prior5_low"] = daily["low"].shift(1).rolling(5).min()
    daily["prior5_high"] = daily["high"].shift(1).rolling(5).max()
    daily["new_low"] = daily["low"].le(daily["prior5_low"])
    daily["new_high"] = daily["high"].ge(daily["prior5_high"])
    daily["oi_pct"] = 100.0 * (daily["oi_close"] / daily["oi_close"].shift(1) - 1.0)
    daily["flow_pct"] = 100.0 * daily["delta"] / daily["volume"]
    kinds = pd.to_datetime(daily["date"]).dt.dayofweek.lt(5)
    relative_volume: list[float] = []
    for index in range(len(daily)):
        prior = daily.iloc[:index].loc[kinds.iloc[:index].eq(bool(kinds.iloc[index]))].tail(20)
        relative_volume.append(
            float(daily.iloc[index]["volume"] / prior["volume"].median())
            if len(prior) >= 5 and float(prior["volume"].median()) > 0 else float("nan")
        )
    daily["rvol20"] = relative_volume
    date_to_index = {value: index for index, value in enumerate(daily["date"].tolist())}
    bars["session"] = bars["date"].map(date_to_index).astype(int)
    bars["session_cum_volume"] = bars.groupby("date")["volume"].cumsum()
    bars["session_cum_value"] = bars.groupby("date")["val"].cumsum()
    bars["running_vwap"] = bars["session_cum_value"] / (bars["session_cum_volume"] * 1000.0)
    bars["is_last"] = False
    bars.loc[bars.groupby("date", sort=True).tail(1).index, "is_last"] = True
    return bars.reset_index(drop=True), daily.reset_index(drop=True)


def load_funding(paths: Sequence[Path]) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        rows = payload.get("rows", payload.get("records", []))
        for row in rows:
            date_value = row.get("TRADEDATE", row.get("tradedate"))
            rate = row.get("SWAPRATE", row.get("swaprate"))
            if date_value:
                result[str(date_value)] = None if rate is None else float(rate)
    return result


def _funding_cost(
    entry_ts: pd.Timestamp,
    exit_ts: pd.Timestamp,
    side: int,
    entry_price: float,
    daily: pd.DataFrame,
    funding: Mapping[str, float | None],
) -> tuple[float, int]:
    signed_cost = 0.0
    missing = 0
    for date_value in daily["date"]:
        clear_time = "18:50:00" if date_value < "2026-03-23" else "23:50:00"
        clear_ts = pd.Timestamp(f"{date_value} {clear_time}")
        if entry_ts <= clear_ts < exit_ts:
            rate = funding.get(str(date_value))
            if rate is None:
                signed_cost += 0.0035 * entry_price
                missing += 1
            else:
                signed_cost += side * float(rate)
    return float(signed_cost), missing


def _field(row: Any, name: str) -> Any:
    return row[name] if isinstance(row, Mapping) else getattr(row, name)


def _confirmed(mode: str, side: int, bar: Any, setup: Mapping[str, Any], day: Any) -> bool:
    level = float(setup["trigger"])
    beyond_touch = float(_field(bar, "high")) >= level if side == 1 else float(_field(bar, "low")) <= level
    beyond_close = float(_field(bar, "close")) >= level if side == 1 else float(_field(bar, "close")) <= level
    correct_delta = float(_field(bar, "delta")) > 0 if side == 1 else float(_field(bar, "delta")) < 0
    vwap_side = float(_field(day, "close")) > float(_field(day, "vwap")) if side == 1 else float(_field(day, "close")) < float(_field(day, "vwap"))
    if mode == "touch_setup_extreme":
        return beyond_touch
    if mode == "bar_close_setup_extreme":
        return beyond_close
    if mode == "bar_close_extreme_delta":
        return beyond_close and correct_delta
    if mode == "session_close_extreme_vwap":
        return bool(_field(bar, "is_last")) and beyond_close and vwap_side
    if mode == "session_close_multifactor":
        day_delta = float(_field(day, "delta")) > 0 if side == 1 else float(_field(day, "delta")) < 0
        return bool(
            _field(bar, "is_last") and beyond_close and vwap_side and day_delta
            and float(_field(day, "rvol20")) >= 1.1 and float(_field(day, "oi_pct")) <= -0.5
        )
    raise ValueError("unsupported mode: " + mode)


def _close_trade(
    position: dict[str, Any], exit_index: int, exit_price: float, reason: str,
    bars: pd.DataFrame, daily: pd.DataFrame, funding: Mapping[str, float | None], cost: float,
) -> dict[str, Any]:
    side = int(position["side"])
    path = bars.iloc[int(position["entry_index"]): exit_index + 1]
    entry = float(position["entry"])
    favorable = (path["high"].max() - entry) if side == 1 else (entry - path["low"].min())
    adverse = (path["low"].min() - entry) if side == 1 else (entry - path["high"].max())
    funding_cost, missing = _funding_cost(
        pd.Timestamp(position["entry_ts"]), pd.Timestamp(bars.iloc[exit_index]["ts"]),
        side, entry, daily, funding,
    )
    gross = side * (float(exit_price) - entry)
    net = gross - 2.0 * cost - funding_cost
    risk = float(position["risk"])
    return {
        **position,
        "exit_index": int(exit_index), "exit_date": str(bars.iloc[exit_index]["date"]),
        "exit_ts": str(bars.iloc[exit_index]["ts"]), "exit": float(exit_price),
        "exit_reason": reason, "gross_points": gross, "cost_points": 2.0 * cost,
        "funding_cost_points": funding_cost, "missing_funding_events": missing,
        "net_points": net, "net_r": net / risk, "net_return_pct": 100.0 * net / entry,
        "mae_points": float(adverse), "mfe_points": float(favorable),
        "mae_r": float(adverse / risk), "mfe_r": float(favorable / risk),
        "net_rub": 1000.0 * net,
    }


def run_variant(
    bars: pd.DataFrame, daily: pd.DataFrame, funding: Mapping[str, float | None],
    variant: Variant, *, cost: float = 0.025,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    active: dict[int, dict[str, Any]] = {}
    scheduled: list[dict[str, Any]] = []
    position: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    previous_session = -1
    days = daily.to_dict("records")
    for bar_index, bar in enumerate(bars.itertuples(index=False)):
        session = int(bar.session)
        if session != previous_session:
            previous = session - 1
            if previous >= 0:
                completed = days[previous]
                for side, flag in ((1, "new_low"), (-1, "new_high")):
                    if bool(completed[flag]) and np.isfinite(float(completed["atr20_prior"])):
                        active[side] = {
                            "side": side, "setup_session": previous,
                            "setup_date": str(completed["date"]),
                            "setup_high": float(completed["high"]),
                            "setup_low": float(completed["low"]),
                            "setup_atr": float(completed["atr20_prior"]),
                            "trigger": float(completed["high"] + 0.01) if side == 1 else float(completed["low"] - 0.01),
                        }
            active = {
                side: setup for side, setup in active.items()
                if session <= int(setup["setup_session"]) + variant.life
            }
            previous_session = session

        arriving = [order for order in scheduled if int(order["entry_index"]) == bar_index]
        scheduled = [order for order in scheduled if int(order["entry_index"]) != bar_index]
        if position is None and len(arriving) == 1:
            order = arriving[0]
            side = int(order["side"])
            entry = float(bar.open)
            raw_stop = (
                float(order["setup_low"]) - variant.stop_buffer * float(order["setup_atr"])
                if side == 1 else
                float(order["setup_high"]) + variant.stop_buffer * float(order["setup_atr"])
            )
            stop = _round_stop(raw_stop, side)
            risk = side * (entry - stop)
            if risk > 0 and risk <= 3.0 * float(order["setup_atr"]):
                position = {
                    **order, "entry_index": int(bar_index), "entry_session": session,
                    "entry_date": str(bar.date), "entry_ts": str(bar.ts),
                    "entry": entry, "stop": stop, "risk": risk,
                }
                signals[int(order["signal_row"])]["status"] = "entered"
            else:
                signals[int(order["signal_row"])]["status"] = "rejected_risk"
                signals[int(order["signal_row"])]["entry_risk_atr"] = (
                    risk / float(order["setup_atr"]) if risk > 0 else None
                )
        elif arriving:
            for order in arriving:
                signals[int(order["signal_row"])]["status"] = "skipped_conflict_or_position"

        if position is not None:
            side = int(position["side"])
            stop = float(position["stop"])
            hit = float(bar.low) <= stop if side == 1 else float(bar.high) >= stop
            if hit:
                gap = side * (float(bar.open) - stop) <= 0
                exit_price = float(bar.open) if gap else stop
                trades.append(_close_trade(position, bar_index, exit_price, "stop_gap" if gap else "stop", bars, daily, funding, cost))
                position = None
            elif bool(bar.is_last) and session >= int(position["entry_session"]) + variant.hold - 1:
                trades.append(_close_trade(position, bar_index, float(bar.close), "time", bars, daily, funding, cost))
                position = None

        if position is None and not scheduled and bar_index + 1 < len(bars):
            confirmations: list[dict[str, Any]] = []
            day = days[session]
            for side, setup in list(active.items()):
                if session <= int(setup["setup_session"]):
                    continue
                if _confirmed(variant.mode, side, bar, setup, day):
                    confirmations.append(setup)
            if len(confirmations) == 1:
                setup = confirmations[0]
                signal = {
                    "variant": variant.key, "mode": variant.mode,
                    "side": "long" if int(setup["side"]) == 1 else "short",
                    "setup_date": setup["setup_date"], "setup_high": setup["setup_high"],
                    "setup_low": setup["setup_low"], "setup_atr": setup["setup_atr"],
                    "trigger": setup["trigger"], "confirmation_date": str(bar.date),
                    "confirmation_ts": str(bar.ts), "confirmation_close": float(bar.close),
                    "confirmation_vwap": float(bar.running_vwap),
                    "bar_delta": float(bar.delta), "daily_delta": float(day["delta"]),
                    "daily_rvol20": float(day["rvol20"]), "daily_oi_pct": float(day["oi_pct"]),
                    "status": "scheduled",
                }
                signals.append(signal)
                order = {
                    **setup, "signal_row": len(signals) - 1, "entry_index": int(bar_index + 1),
                    "confirmation_date": str(bar.date), "confirmation_ts": str(bar.ts),
                    "confirmation_close": float(bar.close),
                }
                scheduled.append(order)
                active.pop(int(setup["side"]), None)
            elif len(confirmations) > 1:
                signals.append({
                    "variant": variant.key, "mode": variant.mode, "side": "conflict",
                    "setup_date": ",".join(str(item["setup_date"]) for item in confirmations),
                    "confirmation_date": str(bar.date), "confirmation_ts": str(bar.ts),
                    "status": "skipped_simultaneous_opposites",
                })
    if position is not None:
        last = len(bars) - 1
        trades.append(_close_trade(position, last, float(bars.iloc[last]["close"]), "data_end", bars, daily, funding, cost))
    return trades, signals


def metrics(trades: Iterable[Mapping[str, Any]], start: str | None = None, end: str | None = None) -> dict[str, Any]:
    selected = [
        dict(trade) for trade in trades
        if (start is None or str(trade["entry_date"]) >= start)
        and (end is None or str(trade["exit_date"]) <= end)
    ]
    r = np.array([float(trade["net_r"]) for trade in selected], dtype=float)
    points = np.array([float(trade["net_points"]) for trade in selected], dtype=float)
    equity = np.r_[0.0, np.cumsum(r)]
    drawdown = float((np.maximum.accumulate(equity) - equity).max()) if len(equity) else 0.0
    positive = np.sort(r[r > 0])[::-1]
    positive_sum = float(positive.sum())
    top1 = float(positive[:1].sum() / positive_sum) if positive_sum > 0 else None
    top3 = float(positive[:3].sum() / positive_sum) if positive_sum > 0 else None
    longs = sum(trade.get("side") in (1, "1", "long") for trade in selected)
    shorts = len(selected) - longs
    long_r = sum(float(trade["net_r"]) for trade in selected if trade.get("side") in (1, "1", "long"))
    short_r = sum(float(trade["net_r"]) for trade in selected if trade.get("side") in (-1, "-1", "short"))
    return {
        "trades": len(selected), "long_trades": longs, "short_trades": shorts,
        "long_net_r": float(long_r), "short_net_r": float(short_r),
        "net_points": float(points.sum()), "net_rub": float(points.sum() * 1000.0),
        "net_return_pct": float(sum(float(trade["net_return_pct"]) for trade in selected)),
        "net_r": float(r.sum()), "max_drawdown_r": drawdown,
        "win_rate": float(np.mean(r > 0)) if len(r) else None,
        "false_signals": int(np.sum(r <= 0)),
        "stopped": sum(str(trade["exit_reason"]).startswith("stop") for trade in selected),
        "mae_r_mean": float(np.mean([float(t["mae_r"]) for t in selected])) if selected else None,
        "mfe_r_mean": float(np.mean([float(t["mfe_r"]) for t in selected])) if selected else None,
        "top1_positive_profit_share": top1, "top3_positive_profit_share": top3,
        "without_best_r": float(r.sum() - (positive[0] if len(positive) else 0.0)),
    }


def _direct_neighbors(variant: Variant, variants: Sequence[Variant]) -> list[Variant]:
    result = []
    for candidate in variants:
        if candidate.mode != variant.mode:
            continue
        differences = sum(
            left != right for left, right in (
                (candidate.life, variant.life),
                (candidate.stop_buffer, variant.stop_buffer),
                (candidate.hold, variant.hold),
            )
        )
        if differences == 1:
            result.append(candidate)
    return result


def walk_forward_trades(trades: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [
        trade for trade in trades
        if any(
            str(trade["entry_date"]) >= start and str(trade["exit_date"]) <= end
            for _name, start, end, _weight in FOLDS
        )
    ]


def select_variant(
    variants: Sequence[Variant], trade_map: Mapping[str, Sequence[Mapping[str, Any]]]
) -> tuple[Variant | None, pd.DataFrame, pd.DataFrame]:
    fold_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []
    combined_by_key: dict[str, dict[str, Any]] = {}
    for variant in variants:
        weighted_score = 0.0
        combined: list[Mapping[str, Any]] = []
        positive_folds = 0
        for name, start, end, weight in FOLDS:
            fold_trades = [t for t in trade_map[variant.key] if str(t["entry_date"]) >= start and str(t["exit_date"]) <= end]
            value = metrics(fold_trades)
            fold_rows.append({"variant": variant.key, "fold": name, **value})
            combined.extend(fold_trades)
            weighted_score += weight * float(value["net_r"]) / max(3.0, float(value["max_drawdown_r"]))
            positive_folds += int(float(value["net_r"]) > 0)
        combined_metrics = metrics(combined)
        combined_by_key[variant.key] = combined_metrics
        selection_rows.append({
            "variant": variant.key, **asdict(variant), "weighted_score": weighted_score,
            "positive_folds": positive_folds, **{f"wf_{key}": value for key, value in combined_metrics.items()},
        })
    for row, variant in zip(selection_rows, variants):
        neighbor_values = [combined_by_key[item.key]["net_r"] for item in _direct_neighbors(variant, variants)]
        positive_fraction = float(np.mean(np.asarray(neighbor_values) > 0)) if neighbor_values else 0.0
        row["neighbor_count"] = len(neighbor_values)
        row["positive_neighbor_fraction"] = positive_fraction
        row["neighbor_median_net_r"] = float(np.median(neighbor_values)) if neighbor_values else None
        row["eligible"] = bool(
            int(row["wf_trades"]) >= 15
            and int(row["wf_long_trades"]) >= 3
            and int(row["wf_short_trades"]) >= 3
            and float(row["wf_without_best_r"]) > 0
            and row["wf_top3_positive_profit_share"] is not None
            and float(row["wf_top3_positive_profit_share"]) <= 0.7
            and positive_fraction >= 0.5
        )
    selection = pd.DataFrame(selection_rows).sort_values(["eligible", "weighted_score"], ascending=[False, False])
    folds = pd.DataFrame(fold_rows)
    eligible = selection.loc[selection["eligible"]]
    # Always carry the best pre-holdout ranking into the locked holdout as a
    # diagnostic.  Eligibility remains a separate, immutable acceptance gate.
    ranked = eligible if not eligible.empty else selection
    winner_key = str(ranked.iloc[0]["variant"])
    return next(variant for variant in variants if variant.key == winner_key), folds, selection


def _one_day_metrics_row(bars: pd.DataFrame, daily: pd.DataFrame, index: int) -> dict[str, Any]:
    current = daily.iloc[index]
    previous = daily.iloc[index - 1]
    part = bars.loc[bars["session"].eq(index)].copy()
    monitor_part = pd.DataFrame({
        "ts": part["ts"], "publication_ts": part["ts"] + pd.Timedelta(seconds=1),
        "pr_high": part["high"], "pr_low": part["low"], "pr_close": part["close"],
        "vol": part["volume"], "delta": part["delta"],
    })
    intra = monitor.intraday_features(monitor_part)
    value_range = float(current["high"] - current["low"])
    previous_range = float(previous["high"] - previous["low"])
    result: dict[str, Any] = {
        "trade_date": str(current["date"]), "high": float(current["high"]),
        "low": float(current["low"]), "close": float(current["close"]),
        "vwap": float(current["vwap"]), "relative_volume_20": float(current["rvol20"]),
        "volume_vs_previous": float(current["volume"] / previous["volume"]),
        "daily_flow_pct": float(current["flow_pct"]), "daily_oi_pct": float(current["oi_pct"]),
        "close_location": float((current["close"] - current["low"]) / value_range),
        "upper_wick_fraction": float((current["high"] - max(current["open"], current["close"])) / value_range),
        "lower_wick_fraction": float((min(current["open"], current["close"]) - current["low"]) / value_range),
        "close_below_session_vwap": bool(current["close"] < current["vwap"]),
        "close_above_session_vwap": bool(current["close"] > current["vwap"]),
        "atr20": float(current["atr20_prior"]),
        "peak_to_close_atr": float((current["high"] - current["close"]) / current["atr20_prior"]),
        "low_to_close_atr": float((current["close"] - current["low"]) / current["atr20_prior"]),
        "new_5_session_high": bool(current["new_high"]),
        "new_5_session_low": bool(current["new_low"]),
        "previous_session_up": bool(previous["close"] > daily.iloc[index - 2]["close"]),
        "previous_session_down": bool(previous["close"] < daily.iloc[index - 2]["close"]),
        "previous_session_close_location": float((previous["close"] - previous["low"]) / previous_range),
    }
    result.update(intra)
    return result


def one_day_signals(bars: pd.DataFrame, daily: pd.DataFrame, rules: Mapping[str, Any]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for index in range(21, len(daily) - 1):
        row = daily.iloc[index]
        if float(row["high"] - row["low"]) <= 0 or not np.isfinite(float(row["rvol20"])):
            continue
        values = _one_day_metrics_row(bars, daily, index)
        for direction, side in (("bullish", 1), ("bearish", -1)):
            decision = monitor.evaluate(values, rules, direction)
            if decision["status"] == "ALERT":
                signals.append({
                    "side": side, "setup_session": index, "setup_date": str(row["date"]),
                    "setup_high": float(row["high"]), "setup_low": float(row["low"]),
                    "setup_atr": float(row["atr20_prior"]), "score": int(decision["score"]),
                })
    return signals


def simulate_one_day(
    bars: pd.DataFrame, daily: pd.DataFrame, funding: Mapping[str, float | None],
    source_signals: Sequence[Mapping[str, Any]], cost: float = 0.025,
) -> list[dict[str, Any]]:
    by_entry_session: dict[int, list[dict[str, Any]]] = {}
    for item in source_signals:
        by_entry_session.setdefault(int(item["setup_session"]) + 1, []).append(dict(item))
    trades: list[dict[str, Any]] = []
    position: dict[str, Any] | None = None
    for bar_index, bar in bars.iterrows():
        session = int(bar["session"])
        if (
            position is None and session in by_entry_session
            and len(by_entry_session[session]) == 1
            and bar_index == int(bars.index[bars["session"].eq(session)][0])
        ):
            signal = by_entry_session[session][0]
            side = int(signal["side"])
            entry = float(bar["open"])
            raw_stop = (
                float(signal["setup_low"]) - 0.1 * float(signal["setup_atr"])
                if side == 1 else float(signal["setup_high"]) + 0.1 * float(signal["setup_atr"])
            )
            stop = _round_stop(raw_stop, side)
            risk = side * (entry - stop)
            if 0 < risk <= 3 * float(signal["setup_atr"]):
                position = {
                    **signal, "side": "long" if side == 1 else "short", "side_value": side,
                    "entry_index": int(bar_index), "entry_session": session,
                    "entry_date": str(bar["date"]), "entry_ts": str(bar["ts"]),
                    "entry": entry, "stop": stop, "risk": risk,
                }
        if position is not None:
            side = int(position["side_value"])
            position_for_close = {**position, "side": side}
            stop = float(position["stop"])
            hit = float(bar["low"]) <= stop if side == 1 else float(bar["high"]) >= stop
            if hit:
                gap = side * (float(bar["open"]) - stop) <= 0
                exit_price = float(bar["open"]) if gap else stop
                closed = _close_trade(position_for_close, bar_index, exit_price, "stop_gap" if gap else "stop", bars, daily, funding, cost)
                closed["side"] = "long" if side == 1 else "short"
                trades.append(closed); position = None
            elif bool(bar["is_last"]) and session >= int(position["entry_session"]) + 4:
                closed = _close_trade(position_for_close, bar_index, float(bar["close"]), "time", bars, daily, funding, cost)
                closed["side"] = "long" if side == 1 else "short"
                trades.append(closed); position = None
    return trades


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_json_value(payload), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tradestats", action="append", required=True)
    parser.add_argument("--history", action="append", required=True)
    parser.add_argument("--rules", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)
    input_paths = [Path(value).resolve(strict=True) for value in args.tradestats]
    history_paths = [Path(value).resolve(strict=True) for value in args.history]
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise FileExistsError("output directory must be empty: " + str(output))
    bars, daily = build_market(input_paths)
    funding = load_funding(history_paths)
    variants = [
        Variant(mode, life, buffer, hold)
        for mode in MODES for life in (3, 5) for buffer in (0.1, 0.25) for hold in (3, 5)
    ]
    trade_map: dict[str, list[dict[str, Any]]] = {}
    signal_map: dict[str, list[dict[str, Any]]] = {}
    for variant in variants:
        trades, signals = run_variant(bars, daily, funding, variant)
        trade_map[variant.key] = trades
        signal_map[variant.key] = signals
    winner, folds, selection = select_variant(variants, trade_map)
    folds.to_csv(output / "variant_fold_metrics.csv", index=False)
    selection.to_csv(output / "variant_selection.csv", index=False)
    holdout_rows = [
        {"variant": variant.key, **asdict(variant), **metrics(trade_map[variant.key], *HOLDOUT)}
        for variant in variants
    ]
    pd.DataFrame(holdout_rows).sort_values("net_r", ascending=False).to_csv(
        output / "variant_holdout_metrics.csv", index=False
    )
    holdout_signals = [
        signal for variant in variants for signal in signal_map[variant.key]
        if HOLDOUT[0] <= str(signal.get("confirmation_date", "")) <= HOLDOUT[1]
    ]
    pd.DataFrame(holdout_signals).to_csv(output / "holdout_signal_catalog.csv", index=False)
    rules = json.loads(Path(args.rules).read_text(encoding="utf-8"))
    old_signals = one_day_signals(bars, daily, rules)
    old_trades = simulate_one_day(bars, daily, funding, old_signals)
    pd.DataFrame(old_trades).to_csv(output / "one_day_monitor_trades.csv", index=False)
    pd.DataFrame(old_signals).to_csv(output / "one_day_monitor_signals.csv", index=False)
    if winner is None:
        best_trades: list[dict[str, Any]] = []
        best_signals: list[dict[str, Any]] = []
    else:
        best_trades = trade_map[winner.key]
        best_signals = signal_map[winner.key]
    pd.DataFrame(best_trades).to_csv(output / "best_variant_trades.csv", index=False)
    pd.DataFrame(best_signals).to_csv(output / "best_variant_signals.csv", index=False)
    summary = {
        "schema_version": "usdrubf_multiday_reversal_study.v1",
        "data_range": [str(daily.iloc[0]["date"]), str(daily.iloc[-1]["date"])],
        "bars": len(bars), "sessions": len(daily), "variants": len(variants),
        "selected_variant": asdict(winner) if winner else None,
        "selected_key": winner.key if winner else None,
        "selection_result": (
            "eligible_variant_found"
            if winner is not None and bool(selection.loc[selection["variant"].eq(winner.key), "eligible"].iloc[0])
            else "no_variant_passed_predeclared_gates_best_ranked_sent_to_holdout_for_diagnosis"
        ),
        "selected_walk_forward": metrics(walk_forward_trades(best_trades)) if winner else metrics([]),
        "selected_holdout": metrics(best_trades, *HOLDOUT) if winner else metrics([]),
        "selected_full": metrics(best_trades, "2023-04-01", "2026-11-09") if winner else metrics([]),
        "selected_stress_cost_holdout": None,
        "one_day_walk_forward": metrics(walk_forward_trades(old_trades)),
        "one_day_holdout": metrics(old_trades, *HOLDOUT),
        "one_day_full": metrics(old_trades, "2023-04-01", "2026-11-09"),
    }
    if winner:
        stress_trades, _ = run_variant(bars, daily, funding, winner, cost=0.05)
        summary["selected_stress_cost_holdout"] = metrics(stress_trades, *HOLDOUT)
        pd.DataFrame(stress_trades).to_csv(output / "best_variant_stress_trades.csv", index=False)
    write_json(output / "summary.json", summary)
    write_json(output / "data_audit.json", {
        "inputs": {str(path): sha256(path) for path in [*input_paths, *history_paths, Path(args.rules).resolve(strict=True)]},
        "first_bar": str(bars.iloc[0]["ts"]), "last_bar": str(bars.iloc[-1]["ts"]),
        "bars": len(bars), "sessions": len(daily), "duplicate_timestamps": int(bars["ts"].duplicated().sum()),
        "holdout": list(HOLDOUT), "holdout_used_in_selection": False,
    })
    print(json.dumps(_json_value(summary), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
