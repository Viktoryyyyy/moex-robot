#!/usr/bin/env python3
import math
import sys
from typing import Any, Dict, List

import pandas as pd

from research import build_d1_tsmom_research_package as base

INSTRUMENT_BLOCKERS: List[Dict[str, Any]] = []


def fixed_normalize_ohlcv(frame: pd.DataFrame, instrument: str, dataset_id: str, schema_version: str) -> pd.DataFrame:
    source = frame.reset_index(drop=True).copy()
    out = pd.DataFrame(index=source.index)
    out["instrument"] = instrument
    out["trade_date"] = source["trade_date"].map(lambda x: base.parse_date(x, "trade_date"))
    out["session_date"] = source["session_date"].map(lambda x: base.parse_date(x, "session_date"))
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(source[col], errors="coerce")
    out["dataset_id"] = dataset_id
    out["schema_version"] = schema_version
    out["source_path"] = source["_source_path"].astype(str).values
    bad_ohlc = int(out[["open", "high", "low", "close"]].isna().any(axis=1).sum())
    if bad_ohlc:
        base.die(instrument + " has null OHLC rows=" + str(bad_ohlc))
    invalid = (out["high"] < out["low"]) | (out["open"] > out["high"]) | (out["open"] < out["low"]) | (out["close"] > out["high"]) | (out["close"] < out["low"])
    invalid_count = int(invalid.fillna(True).sum())
    if invalid_count:
        base.die(instrument + " invalid OHLC rows=" + str(invalid_count))
    dupes = int(out.duplicated(subset=["instrument", "trade_date"]).sum())
    if dupes:
        base.die(instrument + " duplicate D1 rows=" + str(dupes))
    return out.sort_values(["instrument", "trade_date"]).reset_index(drop=True)


def fixed_build_positions(all_bars: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    INSTRUMENT_BLOCKERS.clear()
    for instrument, part in all_bars.groupby("instrument", sort=True):
        if len(part) < base.MIN_HISTORY:
            INSTRUMENT_BLOCKERS.append({"instrument": str(instrument), "blocker": "available_d1_rows_below_min_history", "available_rows": int(len(part)), "min_history": int(base.MIN_HISTORY)})
            continue
        bars = base.make_signal_frame(part)
        for i in range(base.LOOKBACK, len(bars)):
            e1 = i + 1
            e4 = i + 4
            if e1 >= len(bars):
                continue
            row_s = bars.iloc[i]
            row_e1 = bars.iloc[e1]
            row_e4 = bars.iloc[e4] if e4 < len(bars) else None
            signal = int(row_s["signal"])
            if signal == 0:
                continue
            payload = {
                "instrument": str(instrument),
                "setup_day_s": str(row_s["trade_date"]),
                "event_day_e": str(row_e1["trade_date"]),
                "event_day_e_plus_4": str(row_e4["trade_date"]) if row_e4 is not None else None,
                "setup_index": int(row_s["setup_index"]),
                "event_index": int(row_e1["setup_index"]),
                "lookback": base.LOOKBACK,
                "min_history": base.MIN_HISTORY,
                "signal": signal,
                "close_s": float(row_s["close"]),
                "open_e": float(row_e1["open"]),
                "close_e": float(row_e1["close"]),
                "close_e_plus_4": float(row_e4["close"]) if row_e4 is not None else math.nan,
                "open_e_plus_4": float(row_e4["open"]) if row_e4 is not None else math.nan,
                "primary_close_s_to_close_e_return": signal * (float(row_e1["close"]) / float(row_s["close"]) - 1.0),
                "primary_close_s_to_close_e_plus_4_return": signal * (float(row_e4["close"]) / float(row_s["close"]) - 1.0) if row_e4 is not None else math.nan,
                "secondary_open_e_to_close_e_return": signal * (float(row_e1["close"]) / float(row_e1["open"]) - 1.0),
                "secondary_open_e_to_close_e_plus_4_return": signal * (float(row_e4["close"]) / float(row_e1["open"]) - 1.0) if row_e4 is not None else math.nan,
                "has_e_plus_4": bool(row_e4 is not None),
                "label_class_primary": "statistical_research_close_s_anchor",
                "label_class_secondary": "execution_compatible_open_e_anchor",
            }
            if instrument == "Si":
                payload["source_contracts"] = str(row_s.get("source_contracts", ""))
                payload["roll_map_id"] = str(row_s.get("roll_map_id", ""))
                payload["roll_policy_id"] = str(row_s.get("roll_policy_id", ""))
                payload["adjustment_policy_id"] = str(row_s.get("adjustment_policy_id", ""))
                payload["has_roll_boundary_s"] = bool(row_s.get("has_roll_boundary", False))
            else:
                payload["source_contracts"] = ""
                payload["roll_map_id"] = ""
                payload["roll_policy_id"] = ""
                payload["adjustment_policy_id"] = ""
                payload["has_roll_boundary_s"] = False
            rows.append(payload)
    out = pd.DataFrame(rows)
    if out.empty:
        base.die("positions table is empty after applying min_history blockers=" + str(INSTRUMENT_BLOCKERS))
    return out.sort_values(["instrument", "setup_day_s"]).reset_index(drop=True)


_ORIGINAL_BUILD_QUALITY_SUMMARY = base.build_quality_summary


def fixed_build_quality_summary(all_bars: pd.DataFrame, positions: pd.DataFrame, manifest_path, manifest_sha: str, roll_map_sha: str) -> Dict[str, Any]:
    summary = _ORIGINAL_BUILD_QUALITY_SUMMARY(all_bars, positions, manifest_path, manifest_sha, roll_map_sha)
    summary["instrument_min_history_blockers"] = list(INSTRUMENT_BLOCKERS)
    if INSTRUMENT_BLOCKERS:
        summary["final_quality_gate_verdict"] = "fail"
    return summary


base.normalize_ohlcv = fixed_normalize_ohlcv
base.build_positions = fixed_build_positions
base.build_quality_summary = fixed_build_quality_summary


if __name__ == "__main__":
    raise SystemExit(base.main())
