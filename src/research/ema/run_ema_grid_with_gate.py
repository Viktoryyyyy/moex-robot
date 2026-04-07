#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import math
import os
import re
import sys
from typing import Dict, List, Tuple

import pandas as pd

from src.applied.context_filter.d_day_context import build_context_payload


DEFAULT_MASTER_GLOB = "/home/trader/moex_bot/data/master/master_5m_si_cny_futoi_obstats_*.csv"


def die(msg: str) -> None:
    print("ERROR: " + str(msg), file=sys.stderr)
    raise SystemExit(1)


def pick_latest_file(path_glob: str) -> str:
    files = sorted(glob.glob(path_glob))
    if not files:
        die("No files match: " + str(path_glob))
    return files[-1]


def resolve_master_path(raw: str) -> str:
    path = str(raw).strip()
    if not path:
        return pick_latest_file(DEFAULT_MASTER_GLOB)
    if any(ch in path for ch in ["*", "?", "["]):
        return pick_latest_file(path)
    if not os.path.exists(path):
        die("master path not found: " + path)
    return path


def resolve_ohlc_cols(df_cols) -> Dict[str, str]:
    need = ["end", "open", "high", "low", "close"]
    if all(c in df_cols for c in need):
        return {k: k for k in need}
    candidates = [
        {"end": "end", "open": "open_fo", "high": "high_fo", "low": "low_fo", "close": "close_fo"},
        {"end": "end", "open": "OPEN", "high": "HIGH", "low": "LOW", "close": "CLOSE"},
    ]
    for m in candidates:
        if all(m[k] in df_cols for k in need):
            return m
    die("Cannot resolve OHLC columns. Available columns sample: " + str(list(df_cols)[:30]))
    return {}


def parse_tf_minutes(tf: str) -> int:
    s = str(tf).strip().lower()
    m = re.fullmatch(r"(\d+)(m|h|d)", s)
    if not m:
        die("Unsupported timeframe format: " + str(tf))
    qty = int(m.group(1))
    unit = m.group(2)
    if qty <= 0:
        die("Timeframe must be positive: " + str(tf))
    if unit == "m":
        return qty
    if unit == "h":
        return qty * 60
    if unit == "d":
        return qty * 1440
    die("Unsupported timeframe unit: " + str(tf))
    return 0


def tf_to_rule(tf: str) -> str:
    minutes = parse_tf_minutes(tf)
    if minutes % 1440 == 0:
        return str(minutes // 1440) + "D"
    if minutes % 60 == 0:
        return str(minutes // 60) + "h"
    return str(minutes) + "min"


def parse_int_csv(raw: str, label: str) -> List[int]:
    items = [x.strip() for x in str(raw).split(",") if x.strip()]
    if not items:
        die(label + " list is empty")
    out: List[int] = []
    for item in items:
        try:
            val = int(item)
        except Exception:
            die("Invalid integer in " + label + ": " + str(item))
        if val <= 0:
            die(label + " values must be positive")
        if val not in out:
            out.append(val)
    return out


def parse_grid_specs(specs: List[str]) -> Dict[str, List[Tuple[int, int]]]:
    out: Dict[str, List[Tuple[int, int]]] = {}
    for spec in specs:
        parts = [x.strip() for x in str(spec).split(":")]
        if len(parts) != 3:
            die("Grid spec must be timeframe:fast_list:slow_list, got: " + str(spec))
        tf = parts[0]
        if tf in out:
            die("Duplicate timeframe in grid spec: " + str(tf))
        fasts = parse_int_csv(parts[1], "fast")
        slows = parse_int_csv(parts[2], "slow")
        pairs: List[Tuple[int, int]] = []
        for fast in fasts:
            for slow in slows:
                if fast < slow:
                    pairs.append((fast, slow))
        if not pairs:
            die("Grid spec produced zero valid fast<slow pairs for timeframe: " + str(tf))
        out[tf] = pairs
    if not out:
        die("No grid specs provided")
    return out


def resample_from_5m(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if timeframe == "5m":
        return df.copy()
    target_minutes = parse_tf_minutes(timeframe)
    if target_minutes < 5:
        die("Target timeframe cannot be smaller than 5m: " + str(timeframe))
    if target_minutes % 5 != 0:
        die("Target timeframe must be a multiple of 5m: " + str(timeframe))
    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    }
    out = (
        df.sort_values("ts", ascending=True)
        .set_index("ts")
        .resample(tf_to_rule(timeframe), label="right", closed="right")
        .agg(agg)
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    if out.empty:
        die("Resampling produced zero rows for timeframe: " + str(timeframe))
    return out


def generate_ema_signals(bars: pd.DataFrame, fast: int, slow: int) -> pd.DataFrame:
    if fast <= 0 or slow <= 0 or fast >= slow:
        die("EMA span contract requires positive fast < slow")
    out = bars.copy(deep=True).sort_values("ts", ascending=True).reset_index(drop=True)
    out["ema_fast"] = out["close"].ewm(span=fast, adjust=False).mean()
    out["ema_slow"] = out["close"].ewm(span=slow, adjust=False).mean()
    diff = out["ema_fast"] - out["ema_slow"]
    out["signal"] = diff.apply(lambda x: 1.0 if x > 0 else (-1.0 if x < 0 else 0.0))
    out["position"] = out["signal"].shift(1).fillna(0.0)
    return out


def run_point_backtest(bars: pd.DataFrame, fee: float) -> pd.DataFrame:
    out = bars.copy(deep=True).sort_values("ts", ascending=True).reset_index(drop=True)
    out["trades"] = out["position"].diff().abs().fillna(out["position"].abs())
    out["fee"] = out["trades"] * float(fee)
    out["next_open"] = out["open"].shift(-1)
    out["terminal_fee"] = 0.0
    if not out.empty:
        out.loc[out.index[-1], "terminal_fee"] = abs(float(out.iloc[-1]["position"])) * float(fee)
    out["pnl_bar"] = out.apply(
        lambda r: float(r["position"]) * ((float(r["next_open"]) - float(r["open"])) if pd.notna(r["next_open"]) else (float(r["close"]) - float(r["open"]))),
        axis=1,
    )
    out["pnl_bar"] = out["pnl_bar"] - out["fee"] - out["terminal_fee"]
    return out


def summarize_backtest_by_day(bars: pd.DataFrame) -> pd.DataFrame:
    work = bars.copy(deep=True)
    work["date"] = pd.to_datetime(work["ts"], errors="coerce").dt.normalize()
    work = work.dropna(subset=["date"])
    days = (
        work.groupby("date", as_index=False)
        .agg(pnl_day=("pnl_bar", "sum"), num_trades_day=("trades", "sum"))
        .sort_values("date", ascending=True)
        .reset_index(drop=True)
    )
    days["num_trades_day"] = days["num_trades_day"].astype(float)
    days["cum_pnl_day"] = days["pnl_day"].cumsum()
    days["dd_day"] = days["cum_pnl_day"] - days["cum_pnl_day"].cummax()
    return days


def run_backtest(bars_5m: pd.DataFrame, timeframe: str, fast: int, slow: int, fee: float) -> pd.DataFrame:
    bars = resample_from_5m(bars_5m, timeframe)
    bars = generate_ema_signals(bars, fast=fast, slow=slow)
    bars = run_point_backtest(bars, fee=fee)
    out = summarize_backtest_by_day(bars).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)
    out["max_dd_day"] = pd.to_numeric(out["dd_day"], errors="coerce").fillna(0.0).mul(-1.0)
    out["EMA_EDGE_DAY"] = (pd.to_numeric(out["pnl_day"], errors="coerce").fillna(0.0) > 0.0).astype(int)
    out.insert(0, "ema_slow", slow)
    out.insert(0, "ema_fast", fast)
    out.insert(0, "timeframe", timeframe)
    return out[["timeframe", "ema_fast", "ema_slow", "date", "pnl_day", "max_dd_day", "num_trades_day", "EMA_EDGE_DAY"]].copy()


def build_context_series(master_csv: str, target_days: List[str]) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    seen = set()
    for target_day in sorted(target_days):
        day = str(target_day)
        if not day or day in seen:
            continue
        seen.add(day)
        payload = build_context_payload(master_path=master_csv, target_day=day)
        features = payload.get("features") or {}
        rows.append(
            {
                "target_day": payload.get("target_day"),
                "source_trade_date": payload.get("source_trade_date"),
                "d1_vol_z": features.get("d1_vol_z"),
                "d1_body_ratio": features.get("d1_body_ratio"),
                "score": payload.get("score"),
                "band": payload.get("band"),
                "decision": payload.get("decision"),
                "blocked": payload.get("blocked"),
                "status": payload.get("status"),
                "reason": payload.get("reason"),
                "generated_at": payload.get("generated_at"),
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        die("Context series produced zero rows")
    return out.sort_values("target_day", ascending=True).reset_index(drop=True)


def apply_block_adverse(raw_days: pd.DataFrame, ctx: pd.DataFrame) -> pd.DataFrame:
    merged = raw_days.merge(ctx, left_on="date", right_on="target_day", how="left", validate="many_to_one")
    if merged["decision"].isna().any():
        die("Context join missing decision for some RAW rows")
    out = merged.loc[merged["blocked"] != True].copy()
    return out[[
        "timeframe",
        "ema_fast",
        "ema_slow",
        "date",
        "pnl_day",
        "max_dd_day",
        "num_trades_day",
        "EMA_EDGE_DAY",
        "target_day",
        "source_trade_date",
        "d1_vol_z",
        "d1_body_ratio",
        "score",
        "band",
        "decision",
        "blocked",
        "status",
        "reason",
        "generated_at",
    ]].copy()


def summarize_mode(days: pd.DataFrame, mode_name: str, slice_from: str) -> pd.DataFrame:
    work = days.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.date.astype(str)
    parts: List[pd.DataFrame] = []

    def _agg(df: pd.DataFrame, slice_label: str) -> pd.DataFrame:
        g = (
            df.groupby(["timeframe", "ema_fast", "ema_slow"], as_index=False)
            .agg(
                rows=("date", "count"),
                days=("date", "nunique"),
                first_date=("date", "min"),
                last_date=("date", "max"),
                total_pnl=("pnl_day", "sum"),
                max_drawdown=("max_dd_day", "max"),
                trades_sum=("num_trades_day", "sum"),
                edge_rate=("EMA_EDGE_DAY", "mean"),
            )
            .sort_values(["timeframe", "ema_fast", "ema_slow"], ascending=True)
            .reset_index(drop=True)
        )
        g.insert(0, "slice_label", slice_label)
        g.insert(0, "mode_name", mode_name)
        g["pnl_max_dd"] = g.apply(lambda r: (float(r["total_pnl"]) / float(r["max_drawdown"])) if float(r["max_drawdown"]) != 0.0 else math.nan, axis=1)
        return g[["mode_name", "slice_label", "timeframe", "ema_fast", "ema_slow", "rows", "days", "first_date", "last_date", "total_pnl", "max_drawdown", "pnl_max_dd", "trades_sum", "edge_rate"]].copy()

    parts.append(_agg(work, "full"))
    if slice_from:
        cut = str(slice_from)
        sliced = work.loc[work["date"] >= cut].copy()
        if sliced.empty:
            die("slice_from produced zero rows: " + cut)
        parts.append(_agg(sliced, "from_" + cut))
    return pd.concat(parts, ignore_index=True)


def ensure_parent_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def load_bars_5m(master_csv: str) -> pd.DataFrame:
    df = pd.read_csv(master_csv)
    colmap = resolve_ohlc_cols(df.columns)
    x = df[[colmap["end"], colmap["open"], colmap["high"], colmap["low"], colmap["close"]]].copy()
    x.columns = ["ts", "open", "high", "low", "close"]
    x["ts"] = pd.to_datetime(x["ts"], errors="coerce")
    for c in ["open", "high", "low", "close"]:
        x[c] = pd.to_numeric(x[c], errors="coerce")
    x = x.dropna(subset=["ts", "open", "high", "low", "close"]).sort_values("ts").reset_index(drop=True)
    if x.empty:
        die("No valid OHLC rows after normalization")
    return x


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master_csv", default="", help="Path or glob to 5m master CSV")
    ap.add_argument("--grid", action="append", required=True, help="Repeatable spec timeframe:fast_csv:slow_csv")
    ap.add_argument("--commission_points", type=float, default=6.0)
    ap.add_argument("--slice_from", default="2025-06-01")
    ap.add_argument("--out_raw_csv", required=True)
    ap.add_argument("--out_context_csv", required=True)
    ap.add_argument("--out_block_adverse_csv", required=True)
    ap.add_argument("--out_summary_csv", required=True)
    args = ap.parse_args()

    master_csv = resolve_master_path(args.master_csv)
    grid_by_tf = parse_grid_specs(args.grid)
    bars_5m = load_bars_5m(master_csv)

    raw_parts: List[pd.DataFrame] = []
    for timeframe in sorted(grid_by_tf.keys(), key=parse_tf_minutes):
        for fast, slow in grid_by_tf[timeframe]:
            raw_parts.append(run_backtest(bars_5m, timeframe=timeframe, fast=fast, slow=slow, fee=args.commission_points))

    raw_out = pd.concat(raw_parts, ignore_index=True)
    ctx = build_context_series(master_csv, raw_out["date"].astype(str).drop_duplicates().tolist())
    blocked = apply_block_adverse(raw_out, ctx)
    summary = pd.concat([
        summarize_mode(raw_out, "RAW", args.slice_from),
        summarize_mode(blocked, "BLOCK_ADVERSE", args.slice_from),
    ], ignore_index=True)

    for path in [args.out_raw_csv, args.out_context_csv, args.out_block_adverse_csv, args.out_summary_csv]:
        ensure_parent_dir(path)
    raw_out.to_csv(args.out_raw_csv, index=False)
    ctx.to_csv(args.out_context_csv, index=False)
    blocked.to_csv(args.out_block_adverse_csv, index=False)
    summary.to_csv(args.out_summary_csv, index=False)

    print("MASTER: " + master_csv)
    print("RAW_OUT: " + args.out_raw_csv)
    print("CTX_OUT: " + args.out_context_csv)
    print("BLOCK_OUT: " + args.out_block_adverse_csv)
    print("SUMMARY_OUT: " + args.out_summary_csv)
    print("RAW_ROWS: " + str(len(raw_out)))
    print("CTX_ROWS: " + str(len(ctx)))
    print("BLOCK_ROWS: " + str(len(blocked)))
    print("SUMMARY_ROWS: " + str(len(summary)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
