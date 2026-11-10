"""Run the frozen EMA 3/38 reproducibility engine on an extended frozen copy."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_bars(paths: Sequence[Path]) -> pd.DataFrame:
    work = pd.concat([pd.read_csv(path, low_memory=False) for path in paths], ignore_index=True)
    work.columns = [str(column).lower() for column in work.columns]
    work = work.loc[work["secid"].astype(str).str.upper().eq("USDRUBF")].copy()
    work["date"] = pd.to_datetime(work["tradedate"]).dt.strftime("%Y-%m-%d")
    work["time"] = work["tradetime"].astype(str).str.slice(0, 8)
    numeric = {"open": "pr_open", "high": "pr_high", "low": "pr_low", "close": "pr_close", "volume": "vol"}
    for target, source in numeric.items():
        work[target] = pd.to_numeric(work[source], errors="raise")
    return work[["date", "time", *numeric]].sort_values(["date", "time"]).drop_duplicates(["date", "time"], keep="last").reset_index(drop=True)


def _history_records(paths: Sequence[Path]) -> list[dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        for row in payload.get("rows", payload.get("records", [])):
            date_value = row.get("TRADEDATE", row.get("tradedate"))
            rate = row.get("SWAPRATE", row.get("swaprate"))
            if date_value:
                result[str(date_value)] = {"tradedate": str(date_value), "swaprate": rate}
    return [result[key] for key in sorted(result)]


def _metrics(trades: Sequence[Mapping[str, Any]], data: Any, start: str, end: str) -> dict[str, Any]:
    selected = [trade for trade in trades if str(trade["entry_date"]) >= start and str(trade["exit_date"]) <= end]
    pnl = np.zeros(data.n)
    enriched = []
    for trade in selected:
        for index, value in trade["path"]:
            if start <= str(data.date[index]) <= end:
                pnl[index] += float(value) / float(trade["risk"])
        side = int(trade["side"])
        path = data.b.iloc[int(trade["ei"]): int(trade["xi"]) + 1]
        entry = float(trade["entry"])
        mae = float(path["low"].min() - entry) if side == 1 else float(entry - path["high"].max())
        mfe = float(path["high"].max() - entry) if side == 1 else float(entry - path["low"].min())
        enriched.append((trade, mae / float(trade["risk"]), mfe / float(trade["risk"])))
    mask = (data.date >= start) & (data.date <= end)
    curve = np.r_[0.0, np.cumsum(pnl[mask])]
    dd = float((np.maximum.accumulate(curve) - curve).max())
    rr = np.array([float(trade["r"]) for trade in selected])
    positive = np.sort(rr[rr > 0])[::-1]
    positive_sum = float(positive.sum())
    return {
        "trades": len(selected), "long_trades": sum(int(t["side"]) == 1 for t in selected),
        "short_trades": sum(int(t["side"]) == -1 for t in selected),
        "net_points": float(sum(float(t["net"]) for t in selected)),
        "net_rub": float(1000 * sum(float(t["net"]) for t in selected)),
        "net_return_pct": float(sum(100 * float(t["net"]) / float(t["entry"]) for t in selected)),
        "net_r": float(rr.sum()), "max_drawdown_r": dd,
        "win_rate": float(np.mean(rr > 0)) if len(rr) else None,
        "false_signals": int(np.sum(rr <= 0)),
        "stopped": sum(str(t["reason"]).startswith("stop") for t in selected),
        "mae_r_mean": float(np.mean([item[1] for item in enriched])) if enriched else None,
        "mfe_r_mean": float(np.mean([item[2] for item in enriched])) if enriched else None,
        "top1_positive_profit_share": float(positive[:1].sum() / positive_sum) if positive_sum else None,
        "top3_positive_profit_share": float(positive[:3].sum() / positive_sum) if positive_sum else None,
        "without_best_r": float(rr.sum() - (positive[0] if len(positive) else 0.0)),
    }


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _walk_forward_trades(trades: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    folds = (
        ("2025-01-01", "2025-12-31"),
        ("2026-01-01", "2026-06-30"),
        ("2026-07-01", "2026-09-30"),
    )
    return [
        trade for trade in trades
        if any(str(trade["entry_date"]) >= start and str(trade["exit_date"]) <= end for start, end in folds)
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tradestats", action="append", required=True)
    parser.add_argument("--history", action="append", required=True)
    parser.add_argument("--reference-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--end", default="2026-11-09")
    args = parser.parse_args()
    inputs = [Path(value).resolve(strict=True) for value in args.tradestats]
    histories = [Path(value).resolve(strict=True) for value in args.history]
    reference = Path(args.reference_root).resolve(strict=True)
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise FileExistsError("output directory must be empty")

    bars = _load_bars(inputs)
    cache = reference / "work/ema-trailing-20261007/bars.npz"
    np.savez_compressed(cache, **{
        column: bars[column].to_numpy(dtype="U10" if column in ("date", "time") else float)
        for column in bars.columns
    })
    funding_path = reference / "work/ema-practical-20261007/funding_refresh.json"
    funding_path.write_text(json.dumps({"records": _history_records(histories)}, ensure_ascii=False), encoding="utf-8")

    asym_path = reference / "work/ema-asymmetric-20261007/asym.py"
    spec = importlib.util.spec_from_file_location("frozen_ema_asym", asym_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load frozen EMA module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    data = module.load_data(end=args.end)
    config = module.Config(
        lf=3, ls=38, sf=3, ss=38, le="break10", se="break10",
        lx="atr", sx="close_atr", lk=2, sk=2, policy="skip_conflict",
        cost=0.025, atr_kind="simple", side=0,
    )
    trades, orders, info = module.Engine(data).run(config)
    wf_trades = _walk_forward_trades(trades)
    summary = {
        "schema_version": "usdrubf_frozen_ema_3_38_comparison.v1",
        "config": module.asdict(config), "engine_sha256": _sha256(asym_path),
        "data_range": [str(data.date[0]), str(data.date[-1])],
        "walk_forward_folds": {
            "2025": _metrics(trades, data, "2025-01-01", "2025-12-31"),
            "2026_h1": _metrics(trades, data, "2026-01-01", "2026-06-30"),
            "2026_q3": _metrics(trades, data, "2026-07-01", "2026-09-30"),
        },
        "walk_forward": _metrics(wf_trades, data, "2025-01-01", "2026-09-30"),
        "holdout": _metrics(trades, data, "2026-10-01", "2026-11-09"),
        "full": _metrics(trades, data, "2023-04-01", "2026-11-09"),
        "engine_info": info, "orders": len(orders), "trades_total": len(trades),
    }
    (output / "ema_summary.json").write_text(json.dumps(_clean(summary), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    pd.DataFrame([_clean(dict(trade)) for trade in trades]).to_json(output / "ema_trades.json", orient="records", force_ascii=False, indent=2)
    print(json.dumps(_clean(summary), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
