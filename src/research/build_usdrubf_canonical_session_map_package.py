#!/usr/bin/env python3
import argparse
import json
import os
import sys
import urllib.parse
import urllib.request

import numpy as np
import pandas as pd


INSTRUMENT = "USDRUBF"
TZ = "Europe/Moscow"
DEFAULT_IN_CSV = "data/master/usdrubf_5m_2022-04-26_2026-04-06.csv"
DEFAULT_OUT_DIR = "data/research/usdrubf_canonical_session_map_package"
SESSION_GAP_HOURS = 6.0
REQUIRED_BAR_COLS = ["end", "open", "high", "low", "close"]
REQUIRED_OFF_DAYS_COLS = ["tradedate", "is_traded", "reason", "trade_session_date"]


def _die(msg: str) -> None:
    raise SystemExit("ERROR: " + msg)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _read_json(url: str) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            raw = r.read().decode("utf-8")
    except Exception as e:
        _die("ISS request failed: " + url + " reason=" + str(e))
    try:
        return json.loads(raw)
    except Exception as e:
        _die("ISS response is not valid JSON: " + url + " reason=" + str(e))


def _table_to_df(payload: dict, name: str) -> pd.DataFrame:
    if name not in payload:
        _die("ISS response missing table: " + name)
    table = payload[name]
    if "columns" not in table or "data" not in table:
        _die("ISS table has invalid shape: " + name)
    return pd.DataFrame(table["data"], columns=table["columns"])


def _fetch_off_days(start_date: str, end_date: str) -> pd.DataFrame:
    frames = []
    for year in range(int(start_date[:4]), int(end_date[:4]) + 1):
        q = urllib.parse.urlencode({"from": str(year) + "-01-01", "till": str(year) + "-12-31", "show_all_days": "1", "iss.only": "off_days", "iss.meta": "off"})
        url = "https://iss.moex.com/iss/calendars/futures.json?" + q
        frames.append(_table_to_df(_read_json(url), "off_days"))
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if out.empty:
        _die("ISS futures off_days returned no rows")
    missing = [c for c in REQUIRED_OFF_DAYS_COLS if c not in out.columns]
    if missing:
        _die("ISS futures off_days missing required columns: " + str(missing))
    out = out[REQUIRED_OFF_DAYS_COLS].copy()
    out["tradedate"] = pd.to_datetime(out["tradedate"], errors="coerce").dt.date.astype(str)
    out["trade_session_date"] = pd.to_datetime(out["trade_session_date"], errors="coerce").dt.date.astype(str)
    out.loc[out["trade_session_date"] == "NaT", "trade_session_date"] = ""
    out["reason"] = out["reason"].astype("string")
    out["is_traded"] = pd.to_numeric(out["is_traded"], errors="coerce")
    out = out[(out["tradedate"] >= start_date) & (out["tradedate"] <= end_date)].copy()
    if out["tradedate"].isna().any() or out["tradedate"].duplicated().any():
        _die("ISS futures off_days has invalid or duplicate tradedate rows")
    return out.sort_values("tradedate").reset_index(drop=True)


def _fetch_security() -> pd.DataFrame:
    urls = [
        "https://iss.moex.com/iss/engines/futures/markets/forts/securities/" + INSTRUMENT + ".json?iss.only=securities&iss.meta=off",
        "https://iss.moex.com/iss/engines/futures/markets/forts/boards/rfud/securities/" + INSTRUMENT + ".json?iss.only=securities&iss.meta=off",
    ]
    last_error = ""
    for url in urls:
        try:
            df = _table_to_df(_read_json(url), "securities")
            if not df.empty:
                df["source_url"] = url
                return df
        except SystemExit as e:
            last_error = str(e)
    _die("ISS futures securities returned no rows for " + INSTRUMENT + " last_error=" + last_error)


def _extract_weekend_session(security: pd.DataFrame) -> int:
    if "SECID" in security.columns:
        hit = security[security["SECID"].astype(str) == INSTRUMENT].copy()
        if not hit.empty:
            security = hit
    if "weekend_session" not in security.columns:
        _die("ISS futures securities missing weekend_session for " + INSTRUMENT)
    vals = pd.to_numeric(security["weekend_session"], errors="coerce").dropna().astype(int).unique().tolist()
    if len(vals) != 1:
        _die("weekend_session is not singular for " + INSTRUMENT + ": " + str(vals))
    return int(vals[0])


def _load_bars(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        _die("input csv not found: " + path)
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_BAR_COLS if c not in df.columns]
    if missing:
        _die("input csv missing required columns: " + str(missing))
    work = df[REQUIRED_BAR_COLS].copy()
    ts = pd.to_datetime(work["end"], errors="coerce")
    if ts.isna().any():
        _die("invalid bar end timestamps")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize(TZ)
    else:
        ts = ts.dt.tz_convert(TZ)
    work["bar_end_moscow"] = ts
    work["base_date"] = ts.dt.date.astype(str)
    for c in ["open", "high", "low", "close"]:
        work[c] = pd.to_numeric(work[c], errors="coerce")
    if work[["open", "high", "low", "close"]].isna().any().any():
        _die("invalid OHLC values")
    if work["bar_end_moscow"].duplicated().any():
        _die("duplicate bar end timestamps")
    return work.sort_values("bar_end_moscow").reset_index(drop=True)


def _include_row(reason: str, is_traded: float, weekend_session: int) -> tuple[int, str]:
    if pd.isna(reason) or str(reason) == "<NA" or str(reason).strip() == "":
        return 0, "fail_closed_null_reason"
    r = str(reason).strip()
    traded = int(is_traded) if np.isfinite(is_traded) else -1
    if r in ["N", "T"]:
        return (1, "include_" + r) if traded == 1 else (0, "exclude_not_traded_" + r)
    if r == "W":
        return (1, "include_W_weekend_session_0") if traded == 1 and weekend_session == 0 else (0, "exclude_W_weekend_ineligible")
    if r == "H":
        return 0, "exclude_H"
    return 0, "fail_closed_unknown_reason_" + r


def _build_gap_sessions(bars: pd.DataFrame) -> pd.DataFrame:
    work = bars.copy()
    work["gap_hours"] = work["bar_end_moscow"].diff().dt.total_seconds().div(3600.0)
    work["gap_new_session"] = ((work["gap_hours"].isna()) | (work["gap_hours"] > SESSION_GAP_HOURS)).astype(int)
    work["gap_session_index"] = work["gap_new_session"].cumsum() - 1
    return _aggregate_sessions(work, "gap_session_index", "gap_session_date")


def _aggregate_sessions(work: pd.DataFrame, index_col: str, date_col: str) -> pd.DataFrame:
    rows = []
    for idx, g in work.groupby(index_col, sort=True):
        g = g.sort_values("bar_end_moscow").reset_index(drop=True)
        rows.append({
            index_col: int(idx),
            date_col: str(g.iloc[-1][date_col]) if date_col in g.columns else str(g.iloc[-1]["base_date"]),
            "session_start": pd.Timestamp(g.iloc[0]["bar_end_moscow"]).isoformat(),
            "session_end": pd.Timestamp(g.iloc[-1]["bar_end_moscow"]).isoformat(),
            "bar_count": int(len(g)),
            "open": float(g.iloc[0]["open"]),
            "high": float(g["high"].max()),
            "low": float(g["low"].min()),
            "close": float(g.iloc[-1]["close"]),
        })
    return pd.DataFrame(rows).sort_values(index_col).reset_index(drop=True)


def _build_canonical_map(bars: pd.DataFrame, off_days: pd.DataFrame, weekend_session: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    joined = bars.merge(off_days, left_on="base_date", right_on="tradedate", how="left", validate="many_to_one")
    if joined["tradedate"].isna().any():
        missing = sorted(joined.loc[joined["tradedate"].isna(), "base_date"].unique().tolist())
        _die("bars have dates missing from ISS futures off_days: " + str(missing[:10]))
    decisions = joined.apply(lambda r: _include_row(r["reason"], r["is_traded"], weekend_session), axis=1)
    joined["canonical_include"] = [x[0] for x in decisions]
    joined["canonical_reason"] = [x[1] for x in decisions]
    joined["canonical_session_date"] = joined["trade_session_date"].where(joined["trade_session_date"].astype(str) != "", joined["base_date"])
    included = joined[joined["canonical_include"] == 1].copy()
    if included.empty:
        _die("canonical session map has zero included bars")
    ordered_dates = included["canonical_session_date"].drop_duplicates().tolist()
    date_to_idx = {d: i for i, d in enumerate(ordered_dates)}
    included["canonical_session_index"] = included["canonical_session_date"].map(date_to_idx).astype(int)
    session_map = included[["bar_end_moscow", "base_date", "tradedate", "reason", "is_traded", "trade_session_date", "canonical_session_date", "canonical_session_index", "canonical_reason", "open", "high", "low", "close"]].copy()
    sessions = _aggregate_sessions(included, "canonical_session_index", "canonical_session_date")
    return session_map, sessions


def _compare(gap_sessions: pd.DataFrame, canonical_sessions: pd.DataFrame) -> pd.DataFrame:
    n_gap = int(len(gap_sessions))
    n_can = int(len(canonical_sessions))
    common = min(n_gap, n_can)
    rows = []
    for i in range(common):
        g = gap_sessions.iloc[i]
        c = canonical_sessions.iloc[i]
        rows.append({
            "row_index": i,
            "gap_session_date": str(g["gap_session_date"]),
            "canonical_session_date": str(c["canonical_session_date"]),
            "same_start": int(str(g["session_start"]) == str(c["session_start"])),
            "same_end": int(str(g["session_end"]) == str(c["session_end"])),
            "same_bar_count": int(int(g["bar_count"]) == int(c["bar_count"])),
            "same_ohlc": int(all(abs(float(g[x]) - float(c[x])) < 1e-9 for x in ["open", "high", "low", "close"])),
            "gap_bar_count": int(g["bar_count"]),
            "canonical_bar_count": int(c["bar_count"]),
            "gap_close": float(g["close"]),
            "canonical_close": float(c["close"]),
        })
    extra = abs(n_gap - n_can)
    out = pd.DataFrame(rows)
    if out.empty:
        _die("comparison table is empty")
    out.attrs["gap_session_count"] = n_gap
    out.attrs["canonical_session_count"] = n_can
    out.attrs["extra_session_count_delta"] = extra
    return out


def _materiality(comparison: pd.DataFrame, gap_sessions: pd.DataFrame, canonical_sessions: pd.DataFrame) -> tuple[str, pd.DataFrame]:
    mismatched = comparison[(comparison["same_start"] == 0) | (comparison["same_end"] == 0) | (comparison["same_bar_count"] == 0) | (comparison["same_ohlc"] == 0)]
    n_gap = int(len(gap_sessions))
    n_can = int(len(canonical_sessions))
    material = int(n_gap != n_can or len(mismatched) > 0)
    summary = pd.DataFrame([
        {"metric": "gap_session_count", "value": n_gap},
        {"metric": "canonical_session_count", "value": n_can},
        {"metric": "matched_prefix_sessions", "value": int(len(comparison))},
        {"metric": "mismatched_prefix_sessions", "value": int(len(mismatched))},
        {"metric": "partition_material", "value": material},
        {"metric": "decision", "value": "rerun_required" if material else "current_negative_result_can_be_upgraded_without_rerun"},
    ])
    return "material" if material else "immaterial", summary


def _write_report(path: str, status: str, summary: pd.DataFrame, weekend_session: int) -> None:
    lines = []
    lines.append("# USDRUBF canonical session map package")
    lines.append("")
    lines.append("## Contract applied")
    lines.append("")
    lines.append("- base date: bar_end interpreted in Europe/Moscow")
    lines.append("- join: bar base_date to ISS futures off_days.tradedate")
    lines.append("- canonical session date: trade_session_date when present, otherwise base date")
    lines.append("- N/T included, H excluded, W conditional on is_traded=1 and USDRUBF.weekend_session=0")
    lines.append("- ISS futures session endpoint is not used as historical ground truth")
    lines.append("- USDRUBF weekend_session: " + str(weekend_session))
    lines.append("")
    lines.append("## Decision")
    lines.append("")
    for _, row in summary.iterrows():
        lines.append("- " + str(row["metric"]) + ": " + str(row["value"]))
    lines.append("")
    lines.append("## Label note")
    lines.append("")
    lines.append("This package compares session partitions only. It does not recompute primary event-anchored labels or secondary delayed execution-compatible labels.")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", default=DEFAULT_IN_CSV)
    ap.add_argument("--out_dir", default=DEFAULT_OUT_DIR)
    args = ap.parse_args()
    bars = _load_bars(args.in_csv)
    start_date = str(bars["base_date"].min())
    end_date = str(bars["base_date"].max())
    off_days = _fetch_off_days(start_date, end_date)
    security = _fetch_security()
    weekend_session = _extract_weekend_session(security)
    session_map, canonical_sessions = _build_canonical_map(bars, off_days, weekend_session)
    gap_bars = bars.copy()
    gap_bars["gap_session_date"] = gap_bars["base_date"]
    gap_sessions = _build_gap_sessions(gap_bars)
    comparison = _compare(gap_sessions, canonical_sessions)
    status, summary = _materiality(comparison, gap_sessions, canonical_sessions)
    _ensure_dir(args.out_dir)
    off_days.to_csv(os.path.join(args.out_dir, "iss_futures_off_days_v1.csv"), index=False)
    security.to_csv(os.path.join(args.out_dir, "iss_futures_securities_v1.csv"), index=False)
    session_map.to_csv(os.path.join(args.out_dir, "USDRUBF_session_map_v1.csv"), index=False)
    gap_sessions.to_csv(os.path.join(args.out_dir, "gap_sessions_v1.csv"), index=False)
    canonical_sessions.to_csv(os.path.join(args.out_dir, "canonical_sessions_v1.csv"), index=False)
    comparison.to_csv(os.path.join(args.out_dir, "gap_vs_canonical_session_comparison_v1.csv"), index=False)
    summary.to_csv(os.path.join(args.out_dir, "session_partition_summary_v1.csv"), index=False)
    _write_report(os.path.join(args.out_dir, "report.md"), status, summary, weekend_session)
    print("OUT_DIR=" + args.out_dir)
    print("GAP_SESSIONS=" + str(len(gap_sessions)))
    print("CANONICAL_SESSIONS=" + str(len(canonical_sessions)))
    print("PARTITION_DIFFERENCE=" + status)
    print("DECISION=" + str(summary[summary["metric"] == "decision"]["value"].iloc[0]))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        print("ERROR: " + str(e), file=sys.stderr)
        raise SystemExit(1)
