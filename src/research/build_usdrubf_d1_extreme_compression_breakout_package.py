#!/usr/bin/env python3
import argparse
import os
import sys

import numpy as np
import pandas as pd


DEFAULT_IN_CSV = "data/master/usdrubf_5m_2022-04-26_2026-04-06.csv"
DEFAULT_OUT_DIR = "data/research/usdrubf_d1_extreme_compression_breakout_package"
TZ = "Europe/Moscow"
MIN_HISTORY_SESSIONS = 60
REQUIRED_COLS = ["end", "open", "high", "low", "close"]
EVENT_QS = [0.70, 0.80, 0.90]
COMPRESSION_QS = [0.20, 0.30, 0.40]
PRIMARY_EVENT_Q = 0.80
PRIMARY_COMPRESSION_Q = 0.30


def _die(msg):
    raise SystemExit("ERROR: " + str(msg))


def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def _safe_div(a, b):
    if not np.isfinite(a) or not np.isfinite(b) or b == 0.0:
        return float("nan")
    return float(a / b)


def _q_col(prefix, q):
    return prefix + str(int(round(q * 100)))


def _load_intraday(path):
    if not os.path.exists(path):
        _die("input csv not found: " + path)
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        _die("missing required columns: " + str(missing))
    work = df[REQUIRED_COLS].copy()
    ts = pd.to_datetime(work["end"], errors="coerce")
    if ts.isna().any():
        _die("invalid timestamp values in column end")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize(TZ)
    else:
        ts = ts.dt.tz_convert(TZ)
    work["end"] = ts
    work["calendar_session_date"] = ts.dt.date.astype(str)
    for c in ["open", "high", "low", "close"]:
        work[c] = pd.to_numeric(work[c], errors="coerce")
    if work[["open", "high", "low", "close"]].isna().any().any():
        _die("non-numeric or missing OHLC values found")
    if work["end"].duplicated().any():
        _die("duplicate intraday timestamps found")
    work = work.sort_values("end", ascending=True).reset_index(drop=True)
    if work.empty:
        _die("input csv has zero valid rows")
    return work


def _build_sessions(work):
    dates = work["calendar_session_date"].drop_duplicates().tolist()
    date_to_idx = {str(d): i for i, d in enumerate(dates)}
    rows = []
    for session_date, g in work.groupby("calendar_session_date", sort=True):
        g = g.sort_values("end", ascending=True).reset_index(drop=True)
        o = float(g.iloc[0]["open"])
        h = float(g["high"].max())
        l = float(g["low"].min())
        c = float(g.iloc[-1]["close"])
        if not np.isfinite([o, h, l, c]).all():
            _die("non-finite OHLC for session " + str(session_date))
        if h < l:
            _die("high < low for session " + str(session_date))
        body = c - o
        direction = 0
        if body > 0.0:
            direction = 1
        elif body < 0.0:
            direction = -1
        rng = h - l
        rows.append({
            "session_index": int(date_to_idx[str(session_date)]),
            "session_date": str(session_date),
            "session_start": pd.Timestamp(g.iloc[0]["end"]).isoformat(),
            "session_end": pd.Timestamp(g.iloc[-1]["end"]).isoformat(),
            "weekday": int(pd.Timestamp(str(session_date)).weekday()),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "range": float(rng),
            "abs_body": float(abs(body)),
            "body": float(body),
            "direction": int(direction),
            "inside_prev": 0,
        })
    sessions = pd.DataFrame(rows).sort_values("session_index", ascending=True).reset_index(drop=True)
    if len(sessions) < MIN_HISTORY_SESSIONS + 3:
        _die("not enough sessions for requested warmup")
    inside = []
    for i in range(len(sessions)):
        if i == 0:
            inside.append(0)
        else:
            prev = sessions.iloc[i - 1]
            cur = sessions.iloc[i]
            inside.append(int(float(cur["high"]) <= float(prev["high"]) and float(cur["low"]) >= float(prev["low"])))
    sessions["inside_prev"] = inside
    return sessions


def _expanding_quantile(values, q, end_exclusive):
    hist = [float(x) for x in values[:end_exclusive] if np.isfinite(float(x))]
    if len(hist) < MIN_HISTORY_SESSIONS:
        return float("nan")
    return float(np.quantile(np.asarray(hist, dtype=float), q))


def _first_breakout_fill(work, d2_date, direction, d1_high, d1_low, d2_close):
    g = work[work["calendar_session_date"] == d2_date].copy().sort_values("end", ascending=True).reset_index(drop=True)
    if g.empty:
        return {"secondary_triggered": 0, "secondary_breakout_ts": "", "secondary_fill_ts": "", "secondary_fill_price": float("nan"), "secondary_exit_price": float("nan"), "secondary_return": float("nan")}
    trigger_idx = None
    if direction > 0:
        for i in range(len(g)):
            if float(g.iloc[i]["high"]) > d1_high:
                trigger_idx = i
                break
    elif direction < 0:
        for i in range(len(g)):
            if float(g.iloc[i]["low"]) < d1_low:
                trigger_idx = i
                break
    if trigger_idx is None:
        return {"secondary_triggered": 0, "secondary_breakout_ts": "", "secondary_fill_ts": "", "secondary_fill_price": float("nan"), "secondary_exit_price": float("nan"), "secondary_return": float("nan")}
    fill_idx = trigger_idx + 1
    if fill_idx >= len(g):
        return {"secondary_triggered": 1, "secondary_breakout_ts": pd.Timestamp(g.iloc[trigger_idx]["end"]).isoformat(), "secondary_fill_ts": "", "secondary_fill_price": float("nan"), "secondary_exit_price": float("nan"), "secondary_return": float("nan")}
    fill_price = float(g.iloc[fill_idx]["open"])
    exit_price = float(d2_close)
    ret = _safe_div(exit_price - fill_price, fill_price)
    if direction < 0:
        ret = _safe_div(fill_price - exit_price, fill_price)
    return {"secondary_triggered": 1, "secondary_breakout_ts": pd.Timestamp(g.iloc[trigger_idx]["end"]).isoformat(), "secondary_fill_ts": pd.Timestamp(g.iloc[fill_idx]["end"]).isoformat(), "secondary_fill_price": fill_price, "secondary_exit_price": exit_price, "secondary_return": ret}


def _build_table(work, sessions):
    ranges = sessions["range"].tolist()
    abs_bodies = sessions["abs_body"].tolist()
    rows = []
    for d1_i in range(1, len(sessions) - 1):
        d0_i = d1_i - 1
        d2_i = d1_i + 1
        d0 = sessions.iloc[d0_i]
        d1 = sessions.iloc[d1_i]
        d2 = sessions.iloc[d2_i]
        qvals = {}
        for q in EVENT_QS:
            qvals[_q_col("event_q", q)] = _expanding_quantile(abs_bodies, q, d0_i)
        for q in COMPRESSION_QS:
            qvals[_q_col("compression_q", q)] = _expanding_quantile(ranges, q, d1_i)
        event_threshold = qvals[_q_col("event_q", PRIMARY_EVENT_Q)]
        compression_threshold = qvals[_q_col("compression_q", PRIMARY_COMPRESSION_Q)]
        event_any = int(np.isfinite(event_threshold) and int(d0["direction"]) != 0 and float(d0["abs_body"]) >= event_threshold)
        compression_range = int(np.isfinite(compression_threshold) and float(d1["range"]) <= compression_threshold)
        inside_only = int(d1["inside_prev"])
        primary_setup = int(event_any == 1 and compression_range == 1)
        breakout_up = int(float(d2["high"]) > float(d1["high"]))
        breakout_down = int(float(d2["low"]) < float(d1["low"]))
        dir_value = int(d0["direction"])
        if dir_value > 0:
            primary_breakout_in_direction = breakout_up
            opposite_breakout = breakout_down
        elif dir_value < 0:
            primary_breakout_in_direction = breakout_down
            opposite_breakout = breakout_up
        else:
            primary_breakout_in_direction = 0
            opposite_breakout = 0
        secondary = _first_breakout_fill(work, str(d2["session_date"]), dir_value, float(d1["high"]), float(d1["low"]), float(d2["close"]))
        row = {
            "candidate_id": str(d0["session_date"]) + "__" + str(d1["session_date"]) + "__" + str(d2["session_date"]),
            "d0_session_index": int(d0["session_index"]),
            "d1_session_index": int(d1["session_index"]),
            "d2_session_index": int(d2["session_index"]),
            "d0_date": str(d0["session_date"]),
            "d1_date": str(d1["session_date"]),
            "d2_date": str(d2["session_date"]),
            "d0_direction": dir_value,
            "d0_open": float(d0["open"]),
            "d0_high": float(d0["high"]),
            "d0_low": float(d0["low"]),
            "d0_close": float(d0["close"]),
            "d0_abs_body": float(d0["abs_body"]),
            "d0_range": float(d0["range"]),
            "d1_high": float(d1["high"]),
            "d1_low": float(d1["low"]),
            "d1_range": float(d1["range"]),
            "d1_inside_d0": int(d1["inside_prev"]),
            "d2_high": float(d2["high"]),
            "d2_low": float(d2["low"]),
            "d2_close": float(d2["close"]),
            "event_any_primary_q80": event_any,
            "compression_range_primary_q30": compression_range,
            "primary_setup_event_compression": primary_setup,
            "d2_breakout_up_vs_d1": breakout_up,
            "d2_breakout_down_vs_d1": breakout_down,
            "primary_label_breakout_in_d0_direction": int(primary_breakout_in_direction),
            "primary_label_breakout_opposite_d0_direction": int(opposite_breakout),
            "secondary_label_execution_compatible_return": float(secondary["secondary_return"]),
            "secondary_triggered": int(secondary["secondary_triggered"]),
            "secondary_breakout_ts": secondary["secondary_breakout_ts"],
            "secondary_fill_ts": secondary["secondary_fill_ts"],
            "secondary_fill_price": float(secondary["secondary_fill_price"]),
            "secondary_exit_price": float(secondary["secondary_exit_price"]),
        }
        row.update(qvals)
        rows.append(row)
    table = pd.DataFrame(rows)
    if table.empty:
        _die("candidate table is empty")
    return table


def _slice_metrics(df, group):
    n = int(len(df))
    primary = df["primary_label_breakout_in_d0_direction"].dropna() if n else pd.Series(dtype=float)
    opposite = df["primary_label_breakout_opposite_d0_direction"].dropna() if n else pd.Series(dtype=float)
    sec = df["secondary_label_execution_compatible_return"].dropna() if n else pd.Series(dtype=float)
    return {
        "group": group,
        "n": n,
        "primary_breakout_rate": float(primary.mean()) if len(primary) else float("nan"),
        "opposite_breakout_rate": float(opposite.mean()) if len(opposite) else float("nan"),
        "secondary_trigger_rate": float(df["secondary_triggered"].mean()) if n else float("nan"),
        "secondary_mean_return": float(sec.mean()) if len(sec) else float("nan"),
        "secondary_median_return": float(sec.median()) if len(sec) else float("nan"),
    }


def _build_groups(table):
    test = table[table["primary_setup_event_compression"] == 1].copy()
    baseline = table[table["primary_setup_event_compression"] == 0].copy()
    opposite = table[(table["event_any_primary_q80"] == 1) & (table["compression_range_primary_q30"] == 1) & (table["d0_direction"] != 0)].copy()
    compression_without_extreme = table[(table["compression_range_primary_q30"] == 1) & (table["event_any_primary_q80"] == 0)].copy()
    summary = pd.DataFrame([
        _slice_metrics(test, "event_q80_plus_compression_q30"),
        _slice_metrics(baseline, "mutually_exclusive_baseline_without_event_compression"),
        _slice_metrics(opposite[opposite["d0_direction"] == 1], "up_extreme_plus_compression_control"),
        _slice_metrics(opposite[opposite["d0_direction"] == -1], "down_extreme_plus_compression_control"),
        _slice_metrics(compression_without_extreme, "compression_without_extreme_control"),
    ])
    base = summary[summary["group"] == "mutually_exclusive_baseline_without_event_compression"].iloc[0]
    comp_rows = []
    for _, row in summary.iterrows():
        if row["group"] == base["group"]:
            continue
        comp_rows.append({
            "comparison": str(row["group"]) + "_minus_baseline",
            "lhs_group": str(row["group"]),
            "rhs_group": str(base["group"]),
            "lhs_n": int(row["n"]),
            "rhs_n": int(base["n"]),
            "delta_primary_breakout_rate": float(row["primary_breakout_rate"] - base["primary_breakout_rate"]),
            "delta_secondary_mean_return": float(row["secondary_mean_return"] - base["secondary_mean_return"]),
        })
    return summary, pd.DataFrame(comp_rows)


def _build_fragility(table):
    rows = []
    for eq in EVENT_QS:
        e_col = _q_col("event_q", eq)
        for cq in COMPRESSION_QS:
            c_col = _q_col("compression_q", cq)
            for mode in ["range_only", "inside_only"]:
                event_mask = table[e_col].notna() & (table["d0_direction"] != 0) & (table["d0_abs_body"] >= table[e_col])
                if mode == "range_only":
                    comp_mask = table[c_col].notna() & (table["d1_range"] <= table[c_col])
                else:
                    comp_mask = table["d1_inside_d0"] == 1
                g = table[event_mask & comp_mask].copy()
                m = _slice_metrics(g, "tmp")
                rows.append({
                    "event_quantile": "q" + str(int(round(eq * 100))),
                    "compression_quantile": "q" + str(int(round(cq * 100))),
                    "compression_mode": mode,
                    "n": int(m["n"]),
                    "primary_breakout_rate": float(m["primary_breakout_rate"]),
                    "secondary_mean_return": float(m["secondary_mean_return"]),
                })
    return pd.DataFrame(rows)


def _build_diagnostics(table):
    setup = table[table["primary_setup_event_compression"] == 1].copy()
    rows = []
    rows.append({"metric": "valid_candidate_count", "value": int(len(table))})
    rows.append({"metric": "primary_setup_count", "value": int(len(setup))})
    rows.append({"metric": "baseline_count", "value": int((table["primary_setup_event_compression"] == 0).sum())})
    if setup.empty:
        rows.append({"metric": "max_adjacent_cluster_length", "value": 0})
        rows.append({"metric": "adjacent_event_overlap_pairs", "value": 0})
        rows.append({"metric": "top_year_share", "value": float("nan")})
    else:
        idxs = [int(x) for x in setup["d1_session_index"].tolist()]
        cluster_lengths = []
        cur = 1
        for i in range(1, len(idxs)):
            if idxs[i] == idxs[i - 1] + 1:
                cur += 1
            else:
                cluster_lengths.append(cur)
                cur = 1
        cluster_lengths.append(cur)
        overlap = 0
        idx_set = set(idxs)
        for x in idx_set:
            if x + 1 in idx_set:
                overlap += 1
        years = setup["d1_date"].str.slice(0, 4).value_counts(normalize=True)
        rows.append({"metric": "max_adjacent_cluster_length", "value": int(max(cluster_lengths))})
        rows.append({"metric": "adjacent_event_overlap_pairs", "value": int(overlap)})
        rows.append({"metric": "top_year_share", "value": float(years.iloc[0])})
    return pd.DataFrame(rows)


def _build_concentration(table):
    setup = table[table["primary_setup_event_compression"] == 1].copy()
    if setup.empty:
        return pd.DataFrame(columns=["period", "primary_setup_n", "share"])
    setup["period"] = setup["d1_date"].str.slice(0, 7)
    out = setup.groupby("period", as_index=False).size().rename(columns={"size": "primary_setup_n"})
    out["share"] = out["primary_setup_n"] / float(len(setup))
    return out.sort_values(["primary_setup_n", "period"], ascending=[False, True]).reset_index(drop=True)


def _metadata(sessions, table):
    return pd.DataFrame([
        {"field": "instrument", "value": "USDRUBF"},
        {"field": "timeframe", "value": "D1 derived from 5m OHLCV"},
        {"field": "session_indexing_rule", "value": "observed trading-session index by date(end in Europe/Moscow)"},
        {"field": "calendar_binding", "value": "observed_bars_only_without_iss_calendar_join"},
        {"field": "result_status_expected", "value": "provisional_without_iss_calendar_validation"},
        {"field": "min_history_sessions", "value": int(MIN_HISTORY_SESSIONS)},
        {"field": "primary_event_quantile", "value": "q80 expanding before D0"},
        {"field": "primary_compression_quantile", "value": "q30 expanding before D1"},
        {"field": "primary_label", "value": "D2 breakout in D0 direction relative to D1 high/low"},
        {"field": "secondary_label", "value": "D2 intraday breakout then next 5m open fill, exit at D2 close"},
        {"field": "candidate_count", "value": int(len(table))},
        {"field": "session_count", "value": int(len(sessions))},
        {"field": "date_span_start", "value": str(sessions.iloc[0]["session_date"])},
        {"field": "date_span_end", "value": str(sessions.iloc[-1]["session_date"])},
    ])


def _write_report(path, meta, summary, comparison, fragility, diagnostics, concentration):
    lines = []
    lines.append("# USDRUBF D1 extreme directional day -> compression -> D2 breakout continuation")
    lines.append("")
    lines.append("## Status")
    lines.append("")
    lines.append("- result_status: provisional_without_iss_calendar_validation")
    lines.append("- scope: research-only")
    lines.append("- no runtime/live logic")
    lines.append("")
    lines.append("## Semantics")
    lines.append("")
    lines.append("- D0: extreme directional day, abs body >= expanding q80 known before D0.")
    lines.append("- D1: compression day, range <= expanding q30 known before D1.")
    lines.append("- D2: outcome day, breakout measured against D1 high/low.")
    lines.append("- Primary label: D2 breakout in D0 direction relative to D1 high/low.")
    lines.append("- Secondary label: intraday breakout on D2, delayed fill at next 5m open, exit at D2 close.")
    lines.append("- Baseline: all valid D2 candidates without primary event+compression; mutually exclusive with test group.")
    lines.append("")
    lines.append("## Metadata")
    lines.append("")
    for _, row in meta.iterrows():
        lines.append("- " + str(row["field"]) + ": " + str(row["value"]))
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(summary.to_markdown(index=False))
    lines.append("")
    lines.append("## Comparisons")
    lines.append("")
    lines.append(comparison.to_markdown(index=False))
    lines.append("")
    lines.append("## Fragility")
    lines.append("")
    lines.append(fragility.to_markdown(index=False))
    lines.append("")
    lines.append("## Diagnostics")
    lines.append("")
    lines.append(diagnostics.to_markdown(index=False))
    lines.append("")
    lines.append("## Monthly concentration")
    lines.append("")
    if concentration.empty:
        lines.append("No primary setup events.")
    else:
        lines.append(concentration.to_markdown(index=False))
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", default=DEFAULT_IN_CSV)
    ap.add_argument("--out_dir", default=DEFAULT_OUT_DIR)
    args = ap.parse_args()
    work = _load_intraday(args.in_csv)
    sessions = _build_sessions(work)
    table = _build_table(work, sessions)
    summary, comparison = _build_groups(table)
    fragility = _build_fragility(table)
    diagnostics = _build_diagnostics(table)
    concentration = _build_concentration(table)
    meta = _metadata(sessions, table)
    _ensure_dir(args.out_dir)
    table.to_csv(os.path.join(args.out_dir, "candidate_table.csv"), index=False)
    sessions.to_csv(os.path.join(args.out_dir, "session_table.csv"), index=False)
    meta.to_csv(os.path.join(args.out_dir, "metadata.csv"), index=False)
    summary.to_csv(os.path.join(args.out_dir, "summary_table.csv"), index=False)
    comparison.to_csv(os.path.join(args.out_dir, "comparison_table.csv"), index=False)
    fragility.to_csv(os.path.join(args.out_dir, "fragility_table.csv"), index=False)
    diagnostics.to_csv(os.path.join(args.out_dir, "diagnostics_table.csv"), index=False)
    concentration.to_csv(os.path.join(args.out_dir, "monthly_concentration.csv"), index=False)
    _write_report(os.path.join(args.out_dir, "report.md"), meta, summary, comparison, fragility, diagnostics, concentration)
    primary = summary[summary["group"] == "event_q80_plus_compression_q30"].iloc[0]
    baseline = summary[summary["group"] == "mutually_exclusive_baseline_without_event_compression"].iloc[0]
    print("IN=" + args.in_csv)
    print("OUT_DIR=" + args.out_dir)
    print("SESSION_RULE=observed trading-session index by date(end in Europe/Moscow)")
    print("RESULT_STATUS=provisional")
    print("CANDIDATES=" + str(len(table)))
    print("PRIMARY_N=" + str(int(primary["n"])))
    print("BASELINE_N=" + str(int(baseline["n"])))
    print("PRIMARY_BREAKOUT_RATE=" + str(float(primary["primary_breakout_rate"])))
    print("BASELINE_BREAKOUT_RATE=" + str(float(baseline["primary_breakout_rate"])))
    print("SECONDARY_MEAN_RETURN=" + str(float(primary["secondary_mean_return"])))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        print("ERROR: " + str(e), file=sys.stderr)
        raise SystemExit(1)
