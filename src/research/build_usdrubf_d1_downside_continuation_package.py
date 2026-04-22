#!/usr/bin/env python3
import argparse
import os
import sys

import numpy as np
import pandas as pd


DEFAULT_IN_CSV = "data/master/usdrubf_5m_2022-04-26_2026-04-06.csv"
DEFAULT_OUT_DIR = "data/research/usdrubf_d1_downside_continuation_package"
SESSION_GAP_HOURS = 6.0
MIN_HISTORY_SESSIONS = 20
FRAGILITY_QS = [0.75, 0.80, 0.85]
FRAGILITY_CLOSE_CUTS = [0.10, 0.15, 0.20]
REQUIRED_COLS = ["end", "open", "high", "low", "close"]


def _die(msg: str) -> None:
    raise SystemExit("ERROR: " + msg)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _safe_div(a: float, b: float) -> float:
    if not np.isfinite(a) or not np.isfinite(b) or b == 0.0:
        return float("nan")
    return float(a / b)


def _load_intraday(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        _die("input csv not found: " + path)

    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        _die("missing required columns: " + str(missing))

    work = df[REQUIRED_COLS].copy()
    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    for c in ["open", "high", "low", "close"]:
        work[c] = pd.to_numeric(work[c], errors="coerce")

    if work["end"].isna().any():
        _die("invalid timestamp values in column end")
    if work[["open", "high", "low", "close"]].isna().any().any():
        _die("non-numeric or missing OHLC values found")
    if work["end"].duplicated().any():
        _die("duplicate intraday timestamps found")

    work = work.sort_values("end", ascending=True).reset_index(drop=True)
    if work.empty:
        _die("input csv has zero valid rows")
    return work


def _build_sessions(work: pd.DataFrame, session_gap_hours: float) -> pd.DataFrame:
    if session_gap_hours <= 0.0:
        _die("session_gap_hours must be > 0")

    work = work.copy()
    work["gap_hours"] = work["end"].diff().dt.total_seconds().div(3600.0)
    work["new_session"] = ((work["gap_hours"].isna()) | (work["gap_hours"] > session_gap_hours)).astype(int)
    work["session_index"] = work["new_session"].cumsum() - 1

    rows = []
    for session_index, g in work.groupby("session_index", sort=True):
        g = g.sort_values("end", ascending=True).reset_index(drop=True)
        session_start = pd.Timestamp(g.iloc[0]["end"])
        session_end = pd.Timestamp(g.iloc[-1]["end"])
        session_open = float(g.iloc[0]["open"])
        session_high = float(g["high"].max())
        session_low = float(g["low"].min())
        session_close = float(g.iloc[-1]["close"])
        if not np.isfinite([session_open, session_high, session_low, session_close]).all():
            _die("non-finite aggregated OHLC for session_index=" + str(session_index))
        if session_high < session_low:
            _die("session_high < session_low for session_index=" + str(session_index))

        rows.append(
            {
                "session_index": int(session_index),
                "session_start": session_start,
                "session_end": session_end,
                "setup_day": session_end.date().isoformat(),
                "open": session_open,
                "high": session_high,
                "low": session_low,
                "close": session_close,
            }
        )

    sessions = pd.DataFrame(rows).sort_values("session_index", ascending=True).reset_index(drop=True)
    if len(sessions) < 4:
        _die("need at least 4 completed trading sessions")
    if sessions["session_index"].duplicated().any():
        _die("duplicate session_index rows found")
    return sessions


def _expanding_quantile_before(values: list[float], q: float, end_exclusive: int) -> float:
    hist = [x for x in values[:end_exclusive] if np.isfinite(x)]
    if len(hist) < MIN_HISTORY_SESSIONS:
        return float("nan")
    return float(np.quantile(np.asarray(hist, dtype=float), q))


def _build_experiment_table(sessions: pd.DataFrame) -> pd.DataFrame:
    body_pct = []
    close_to_low = []
    close_to_high = []
    direction = []

    for i in range(len(sessions)):
        row = sessions.iloc[i]
        body = float(row["close"] - row["open"])
        rng = float(row["high"] - row["low"])
        close_to_low_ratio = _safe_div(float(row["close"] - row["low"]), rng)
        close_to_high_ratio = _safe_div(float(row["high"] - row["close"]), rng)
        body_pct_value = abs(_safe_div(body, float(row["open"])))
        body_pct.append(body_pct_value)
        close_to_low.append(close_to_low_ratio)
        close_to_high.append(close_to_high_ratio)
        if body > 0.0:
            direction.append(1)
        elif body < 0.0:
            direction.append(-1)
        else:
            direction.append(0)

    rows = []
    for i in range(1, len(sessions)):
        setup = sessions.iloc[i]
        prev = sessions.iloc[i - 1]
        next1 = sessions.iloc[i + 1] if i + 1 < len(sessions) else None
        next2 = sessions.iloc[i + 2] if i + 2 < len(sessions) else None

        prev_body_pct = float(body_pct[i - 1])
        prev_close_to_low = float(close_to_low[i - 1])
        prev_close_to_high = float(close_to_high[i - 1])
        prev_dir = int(direction[i - 1])

        q75 = _expanding_quantile_before(body_pct, 0.75, i - 1)
        q80 = _expanding_quantile_before(body_pct, 0.80, i - 1)
        q85 = _expanding_quantile_before(body_pct, 0.85, i - 1)

        event_down_extreme = int(
            np.isfinite(q80)
            and prev_dir == -1
            and prev_body_pct >= q80
            and np.isfinite(prev_close_to_low)
            and prev_close_to_low <= 0.15
        )
        event_up_mirror = int(
            np.isfinite(q80)
            and prev_dir == 1
            and prev_body_pct >= q80
            and np.isfinite(prev_close_to_high)
            and prev_close_to_high <= 0.15
        )

        y_post1 = _safe_div(float(setup["close"]), float(prev["close"])) - 1.0
        y_post2 = float("nan")
        y_exec_leg1 = float("nan")
        y_exec_leg2 = float("nan")

        if next1 is not None:
            y_post2 = _safe_div(float(next1["close"]), float(prev["close"])) - 1.0
            y_exec_leg1 = _safe_div(float(next1["close"]), float(setup["close"])) - 1.0
        if next2 is not None:
            y_exec_leg2 = _safe_div(float(next2["close"]), float(setup["close"])) - 1.0

        rows.append(
            {
                "setup_day": str(setup["setup_day"]),
                "setup_session_index": int(setup["session_index"]),
                "prev_session_end": pd.Timestamp(prev["session_end"]).isoformat(),
                "setup_session_end": pd.Timestamp(setup["session_end"]).isoformat(),
                "prev_session_index": int(prev["session_index"]),
                "event_down_extreme": event_down_extreme,
                "event_up_mirror": event_up_mirror,
                "prev_dir": prev_dir,
                "prev_open": float(prev["open"]),
                "prev_high": float(prev["high"]),
                "prev_low": float(prev["low"]),
                "prev_close": float(prev["close"]),
                "prev_abs_body_pct": prev_body_pct,
                "prev_close_to_low": prev_close_to_low,
                "prev_close_to_high": prev_close_to_high,
                "expanding_q75_through_d_minus_2": q75,
                "expanding_q80_through_d_minus_2": q80,
                "expanding_q85_through_d_minus_2": q85,
                "y_post1": y_post1,
                "y_post2": y_post2,
                "y_exec_leg1": y_exec_leg1,
                "y_exec_leg2": y_exec_leg2,
            }
        )

    out = pd.DataFrame(rows).sort_values("setup_session_index", ascending=True).reset_index(drop=True)
    if out.empty:
        _die("experiment table is empty")
    return out


def _neg_rate(series: pd.Series) -> float:
    valid = series.dropna()
    if valid.empty:
        return float("nan")
    return float((valid < 0.0).mean())


def _summarize_slice(df: pd.DataFrame, label: str) -> dict:
    return {
        "group": label,
        "n": int(len(df)),
        "mean_y_post1": float(df["y_post1"].mean()) if len(df) else float("nan"),
        "median_y_post1": float(df["y_post1"].median()) if len(df) else float("nan"),
        "negative_rate_y_post1": _neg_rate(df["y_post1"]),
        "mean_y_post2": float(df["y_post2"].mean()) if len(df) else float("nan"),
        "median_y_post2": float(df["y_post2"].median()) if len(df) else float("nan"),
        "negative_rate_y_post2": _neg_rate(df["y_post2"]),
    }


def _build_summary_and_comparison(experiment: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    downside = experiment[experiment["event_down_extreme"] == 1].copy()
    non_event = experiment[experiment["event_down_extreme"] == 0].copy()
    mirror = experiment[experiment["event_up_mirror"] == 1].copy()

    summary = pd.DataFrame(
        [
            _summarize_slice(downside, "downside_event"),
            _summarize_slice(non_event, "non_event"),
            _summarize_slice(mirror, "mirror_up_event"),
        ]
    )

    summary_map = {row["group"]: row for _, row in summary.iterrows()}

    def compare(a: str, b: str, label: str) -> dict:
        ra = summary_map[a]
        rb = summary_map[b]
        return {
            "comparison": label,
            "lhs_group": a,
            "rhs_group": b,
            "lhs_n": int(ra["n"]),
            "rhs_n": int(rb["n"]),
            "delta_mean_y_post1": float(ra["mean_y_post1"] - rb["mean_y_post1"]),
            "delta_median_y_post1": float(ra["median_y_post1"] - rb["median_y_post1"]),
            "delta_negative_rate_y_post1": float(ra["negative_rate_y_post1"] - rb["negative_rate_y_post1"]),
            "delta_mean_y_post2": float(ra["mean_y_post2"] - rb["mean_y_post2"]),
            "delta_median_y_post2": float(ra["median_y_post2"] - rb["median_y_post2"]),
            "delta_negative_rate_y_post2": float(ra["negative_rate_y_post2"] - rb["negative_rate_y_post2"]),
        }

    comparisons = pd.DataFrame(
        [
            compare("downside_event", "non_event", "downside_event_minus_non_event"),
            compare("downside_event", "mirror_up_event", "downside_event_minus_mirror_up_event"),
        ]
    )
    return summary, comparisons


def _build_fragility_table(experiment: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for q in FRAGILITY_QS:
        threshold_col = "expanding_q" + str(int(round(q * 100))) + "_through_d_minus_2"
        for cut in FRAGILITY_CLOSE_CUTS:
            valid_threshold = experiment[threshold_col]
            down_mask = (
                valid_threshold.notna()
                & (experiment["prev_dir"] == -1)
                & (experiment["prev_abs_body_pct"] >= valid_threshold)
                & (experiment["prev_close_to_low"] <= cut)
            )
            up_mask = (
                valid_threshold.notna()
                & (experiment["prev_dir"] == 1)
                & (experiment["prev_abs_body_pct"] >= valid_threshold)
                & (experiment["prev_close_to_high"] <= cut)
            )
            rows.append(
                {
                    "quantile_cut": "q" + str(int(round(q * 100))),
                    "close_location_cut": cut,
                    "downside_event_count": int(down_mask.sum()),
                    "mirror_up_event_count": int(up_mask.sum()),
                }
            )

    return pd.DataFrame(rows)


def _build_dependence_tables(experiment: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    downside = experiment[experiment["event_down_extreme"] == 1].copy().sort_values("setup_session_index", ascending=True)
    if downside.empty:
        diag = pd.DataFrame(
            [
                {"metric": "downside_event_count", "value": 0},
                {"metric": "consecutive_cluster_count", "value": 0},
                {"metric": "max_cluster_length", "value": 0},
                {"metric": "events_in_clusters_share", "value": float("nan")},
                {"metric": "y_post2_overlap_pairs", "value": 0},
                {"metric": "top_year_share", "value": float("nan")},
            ]
        )
        year = pd.DataFrame(columns=["year", "downside_event_n", "share"])
        return diag, year

    idxs = downside["setup_session_index"].tolist()
    cluster_lengths = []
    current_len = 1
    for j in range(1, len(idxs)):
        if int(idxs[j]) == int(idxs[j - 1]) + 1:
            current_len += 1
        else:
            cluster_lengths.append(current_len)
            current_len = 1
    cluster_lengths.append(current_len)

    events_in_clusters = int(sum(x for x in cluster_lengths if x >= 2))
    overlap_pairs = 0
    idx_set = set(int(x) for x in idxs)
    for x in idx_set:
        if (x + 1) in idx_set:
            overlap_pairs += 1

    downside["year"] = downside["setup_day"].str.slice(0, 4)
    year = downside.groupby("year", as_index=False).size().rename(columns={"size": "downside_event_n"})
    year["share"] = year["downside_event_n"] / float(len(downside))
    year = year.sort_values(["downside_event_n", "year"], ascending=[False, True]).reset_index(drop=True)

    diag = pd.DataFrame(
        [
            {"metric": "downside_event_count", "value": int(len(downside))},
            {"metric": "consecutive_cluster_count", "value": int(sum(1 for x in cluster_lengths if x >= 2))},
            {"metric": "max_cluster_length", "value": int(max(cluster_lengths))},
            {"metric": "events_in_clusters_share", "value": float(events_in_clusters / len(downside))},
            {"metric": "y_post2_overlap_pairs", "value": int(overlap_pairs)},
            {"metric": "top_year_share", "value": float(year.iloc[0]["share"]) if not year.empty else float("nan")},
        ]
    )
    return diag, year


def _build_metadata(experiment: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"field": "instrument", "value": "USDRUBF"},
            {"field": "timeframe", "value": "D1 futures sessions"},
            {"field": "session_indexing_rule", "value": "observed completed futures sessions from 5m bars using gap > " + str(SESSION_GAP_HOURS) + "h"},
            {"field": "date_span_start", "value": str(sessions.iloc[0]["setup_day"])},
            {"field": "date_span_end", "value": str(sessions.iloc[-1]["setup_day"])},
            {"field": "session_count", "value": int(len(sessions))},
            {"field": "experiment_row_count", "value": int(len(experiment))},
            {"field": "min_history_sessions_for_expanding_threshold", "value": MIN_HISTORY_SESSIONS},
            {"field": "primary_quantile_cut", "value": "q80"},
            {"field": "primary_close_location_cut", "value": 0.15},
            {"field": "fragility_quantiles", "value": "q75,q80,q85"},
            {"field": "fragility_close_location_cuts", "value": "0.10,0.15,0.20"},
            {"field": "downside_event_count", "value": int(experiment["event_down_extreme"].sum())},
            {"field": "mirror_up_event_count", "value": int(experiment["event_up_mirror"].sum())},
            {"field": "non_event_count", "value": int((experiment["event_down_extreme"] == 0).sum())},
        ]
    )


def _pick_verdict(summary: pd.DataFrame, comparisons: pd.DataFrame) -> tuple[str, str, str]:
    sm = {row["group"]: row for _, row in summary.iterrows()}
    cp = {row["comparison"]: row for _, row in comparisons.iterrows()}

    downside = sm["downside_event"]
    n = int(downside["n"])

    continuation_supported = (
        n >= 1
        and np.isfinite(downside["mean_y_post1"])
        and np.isfinite(downside["mean_y_post2"])
        and downside["mean_y_post1"] < 0.0
        and downside["mean_y_post2"] < 0.0
        and cp["downside_event_minus_non_event"]["delta_mean_y_post1"] < 0.0
        and cp["downside_event_minus_non_event"]["delta_mean_y_post2"] < 0.0
    )
    asymmetry_supported = (
        n >= 1
        and cp["downside_event_minus_mirror_up_event"]["delta_mean_y_post1"] < 0.0
        and cp["downside_event_minus_mirror_up_event"]["delta_mean_y_post2"] < 0.0
    )
    sample_sufficient = "sufficient" if n >= 20 else "insufficient"

    continuation = "supported" if continuation_supported else "not_supported"
    asymmetry = "supported" if asymmetry_supported else "not_supported"
    return continuation, asymmetry, sample_sufficient


def _write_report(
    out_path: str,
    metadata: pd.DataFrame,
    summary: pd.DataFrame,
    comparisons: pd.DataFrame,
    fragility: pd.DataFrame,
    dependence: pd.DataFrame,
    year_conc: pd.DataFrame,
    continuation: str,
    asymmetry: str,
    sample_sufficient: str,
) -> None:
    lines = []
    lines.append("# USDRUBF D1 downside continuation after extreme previous down-day")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append("- continuation: " + continuation)
    lines.append("- asymmetry: " + asymmetry)
    lines.append("- sample: " + sample_sufficient)
    lines.append("")
    lines.append("## Semantics")
    lines.append("")
    lines.append("- primary labels are event-anchored on completed D-1")
    lines.append("- secondary delayed labels are reported separately and are not the primary hypothesis answer")
    lines.append("- session indexing uses observed completed futures sessions from the canonical 5m series")
    lines.append("")
    lines.append("## Metadata")
    lines.append("")
    for _, row in metadata.iterrows():
        lines.append("- " + str(row["field"]) + ": " + str(row["value"]))
    lines.append("")
    lines.append("## Primary summary")
    lines.append("")
    lines.append(summary.to_markdown(index=False))
    lines.append("")
    lines.append("## Comparisons")
    lines.append("")
    lines.append(comparisons.to_markdown(index=False))
    lines.append("")
    lines.append("## Fragility counts")
    lines.append("")
    lines.append(fragility.to_markdown(index=False))
    lines.append("")
    lines.append("## Dependence diagnostics")
    lines.append("")
    lines.append(dependence.to_markdown(index=False))
    lines.append("")
    lines.append("## Year concentration")
    lines.append("")
    if year_conc.empty:
        lines.append("No downside events under primary definition.")
    else:
        lines.append(year_conc.to_markdown(index=False))
    lines.append("")
    lines.append("## Non-iid note")
    lines.append("")
    lines.append("Primary label y_post2 overlaps whenever downside events occur on adjacent setup sessions; treat standard errors and significance claims as non-iid sensitive.")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", default=DEFAULT_IN_CSV, help="Canonical USDRUBF 5m input CSV")
    ap.add_argument("--out_dir", default=DEFAULT_OUT_DIR, help="Output package directory")
    ap.add_argument("--session_gap_hours", type=float, default=SESSION_GAP_HOURS, help="Gap threshold used to start a new observed futures session")
    args = ap.parse_args()

    work = _load_intraday(args.in_csv)
    sessions = _build_sessions(work, args.session_gap_hours)
    experiment = _build_experiment_table(sessions)
    metadata = _build_metadata(experiment, sessions)
    summary, comparisons = _build_summary_and_comparison(experiment)
    fragility = _build_fragility_table(experiment)
    dependence, year_conc = _build_dependence_tables(experiment)
    continuation, asymmetry, sample_sufficient = _pick_verdict(summary, comparisons)

    _ensure_dir(args.out_dir)

    experiment_path = os.path.join(args.out_dir, "experiment_table.csv")
    metadata_path = os.path.join(args.out_dir, "metadata.csv")
    summary_path = os.path.join(args.out_dir, "primary_summary.csv")
    comparison_path = os.path.join(args.out_dir, "comparison_table.csv")
    fragility_path = os.path.join(args.out_dir, "fragility_table.csv")
    dependence_path = os.path.join(args.out_dir, "dependence_diagnostics.csv")
    year_conc_path = os.path.join(args.out_dir, "year_concentration.csv")
    report_path = os.path.join(args.out_dir, "report.md")

    experiment.to_csv(experiment_path, index=False)
    metadata.to_csv(metadata_path, index=False)
    summary.to_csv(summary_path, index=False)
    comparisons.to_csv(comparison_path, index=False)
    fragility.to_csv(fragility_path, index=False)
    dependence.to_csv(dependence_path, index=False)
    year_conc.to_csv(year_conc_path, index=False)
    _write_report(
        report_path,
        metadata,
        summary,
        comparisons,
        fragility,
        dependence,
        year_conc,
        continuation,
        asymmetry,
        sample_sufficient,
    )

    topline = summary[summary["group"] == "downside_event"].iloc[0]
    print("IN=" + args.in_csv)
    print("OUT_DIR=" + args.out_dir)
    print("ROWS=" + str(len(experiment)))
    print("DOWNSIDE_N=" + str(int(topline["n"])))
    print("MIRROR_UP_N=" + str(int(summary[summary["group"] == "mirror_up_event"]["n"].iloc[0])))
    print("MEAN_Y_POST1=" + str(topline["mean_y_post1"]))
    print("MEAN_Y_POST2=" + str(topline["mean_y_post2"]))
    print("CONTINUATION=" + continuation)
    print("ASYMMETRY=" + asymmetry)
    print("SAMPLE=" + sample_sufficient)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        print("ERROR: " + str(e), file=sys.stderr)
        raise SystemExit(1)
