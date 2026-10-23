"""Reproduce the USDRUBF L3/L4/L5 completed-session minimum study.

The daily panel supplies retrospective nested L labels. Exact-date ALGOPACK
TradeStats and optional FUTOI are fetched with the project's existing read-only
MOEX source adapter, or read from a frozen raw cache. The script writes only
aggregated research artifacts and never places orders.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DATASETS = ("tradestats", "futoi")
FUTOI_GROUPS = {"FIZ", "YUR"}


def load_env(path: Path | None) -> None:
    if path is None:
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() and key.strip() not in os.environ:
            os.environ[key.strip()] = value.strip().strip('"').strip("'")


def clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(clean(payload), ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )


def load_panel(path: Path, analysis_end: str) -> pd.DataFrame:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    panel = pd.DataFrame(raw["rows"], columns=raw["headers"])
    panel = panel.loc[panel["USDRUBF_close"].notna()].copy()
    panel = panel.loc[panel["trade_date"].astype(str) <= analysis_end].reset_index(drop=True)
    numeric = [
        "USDRUBF_open",
        "USDRUBF_high",
        "USDRUBF_low",
        "USDRUBF_close",
        "USDRUBF_volume",
        "USDRUBF_oi_close",
        "USDRUBF_L_order",
    ]
    for column in numeric:
        panel[column] = pd.to_numeric(panel[column], errors="coerce")
    panel["L"] = panel["USDRUBF_L_order"].fillna(0).astype(int)
    panel["weekday"] = pd.to_datetime(panel["trade_date"]).dt.dayofweek
    high = panel["USDRUBF_high"].to_numpy(float)
    low = panel["USDRUBF_low"].to_numpy(float)
    close = panel["USDRUBF_close"].to_numpy(float)
    previous_close = np.r_[np.nan, close[:-1]]
    true_range = np.maximum(high - low, np.maximum(abs(high - previous_close), abs(low - previous_close)))
    panel["atr20"] = pd.Series(true_range).rolling(20).mean()
    panel["prior5atr"] = (
        panel["USDRUBF_close"].shift() - panel["USDRUBF_close"].shift(6)
    ) / panel["atr20"].shift()
    panel["priorvol"] = panel["USDRUBF_volume"].shift() / (
        panel["USDRUBF_volume"].shift(2).rolling(20).median()
    )
    panel["atrpct"] = panel["atr20"].shift() / panel["USDRUBF_close"].shift()
    return panel


def select_protocol(panel: pd.DataFrame, analysis_end: str) -> dict[str, Any]:
    events = [
        int(index)
        for index in panel.index
        if int(panel.loc[index, "L"]) >= 3 and index >= 25 and index < len(panel.index) - 1
    ]
    pool = [
        int(index)
        for index in panel.index
        if int(panel.loc[index, "L"]) in {1, 2}
        and index >= 25
        and index < len(panel.index) - 1
        and all(abs(index - event) > 3 for event in events)
    ]
    used: list[int] = []
    matches: list[dict[str, Any]] = []
    for event in events:
        candidates = [
            control
            for control in pool
            if abs(control - event) <= 180
            and (int(panel.loc[control, "weekday"]) < 5)
            == (int(panel.loc[event, "weekday"]) < 5)
            and all(abs(control - selected) > 3 for selected in used)
        ]

        def distance(control: int) -> float:
            return float(
                abs(panel.loc[event, "prior5atr"] - panel.loc[control, "prior5atr"]) / 2
                + abs(np.log(panel.loc[event, "priorvol"] / panel.loc[control, "priorvol"]))
                + abs(np.log(panel.loc[event, "atrpct"] / panel.loc[control, "atrpct"]))
                + 0.15 * abs(control - event) / 180
            )

        picked: list[int] = []
        for control in sorted(candidates, key=lambda value: (distance(value), abs(event - value))):
            if all(abs(control - prior) > 3 for prior in picked):
                picked.append(control)
            if len(picked) == 2:
                break
        if len(picked) != 2:
            raise RuntimeError(f"could not find two controls for {panel.loc[event, 'trade_date']}")
        used.extend(picked)
        for control in picked:
            matches.append(
                {
                    "event": str(panel.loc[event, "trade_date"]),
                    "control": str(panel.loc[control, "trade_date"]),
                    "distance": distance(control),
                }
            )

    selected: list[dict[str, Any]] = []
    for kind, indices in (("event", events), ("control", used)):
        for index in indices:
            selected.append(
                {
                    "kind": kind,
                    "i": index,
                    "date": str(panel.loc[index, "trade_date"]),
                    "L_order": int(panel.loc[index, "L"]),
                    "low": float(panel.loc[index, "USDRUBF_low"]),
                    "windows": {
                        str(offset): str(panel.loc[index + offset, "trade_date"])
                        for offset in (-2, -1, 0, 1)
                        if 0 <= index + offset < len(panel.index)
                    },
                    "prior5atr": float(panel.loc[index, "prior5atr"]),
                    "priorvol": float(panel.loc[index, "priorvol"]),
                    "atrpct": float(panel.loc[index, "atrpct"]),
                }
            )
    dates = sorted({day for item in selected for day in item["windows"].values()})
    return {
        "schema_version": "usdrubf_reversal_minima_study.v1",
        "asof": analysis_end + " completed session",
        "label_policy": (
            "Use the frozen panel's exact highest retrospective L order; report cumulative "
            "L>=3/L>=4/L>=5. L labels use future data and are never live inputs."
        ),
        "controls": (
            "Two distinct L1/L2 dates per L3+ event within 180 sessions, same weekday/weekend "
            "type, separated by more than three sessions from event centers and other selected "
            "controls; match D-1-known prior five-session return/ATR20, relative volume and ATR%."
        ),
        "session_policy": "D-1 and D+1 are previous/next observed sessions, including weekends.",
        "missing_policy": (
            "Do not impute missing TradeStats or FUTOI. Require contiguous five-minute windows; "
            "exclude unbalanced FIZ/YUR FUTOI endpoints."
        ),
        "events": selected,
        "matches": matches,
        "fetch_dates": dates,
    }


def fetch_raw(
    protocol: dict[str, Any], repo_root: Path, env_file: Path | None, workers: int
) -> dict[str, Any]:
    load_env(env_file)
    os.chdir(repo_root)
    sys.path[:0] = [str(repo_root), str(repo_root / "src")]
    from moex_data.futures import liquidity_history_metrics_probe as source

    def fetch(job: tuple[str, str]) -> dict[str, Any]:
        day, dataset = job
        path = (
            "/iss/analyticalproducts/futoi/securities/usdrubf.json"
            if dataset == "futoi"
            else "/iss/datashop/algopack/fo/tradestats/USDRUBF.json"
        )
        params: dict[str, Any] = {"from": day, "till": day}
        if dataset == "futoi":
            params["latest"] = 0
        metadata = {
            "date": day,
            "dataset": dataset,
            "url": "https://apim.moex.com" + path,
            "params": params,
        }
        for attempt in range(2):
            try:
                frame = source.fetch_paged_frame(
                    "https://apim.moex.com", path, params, dataset, 35, True
                )
                frame.columns = [str(column).lower() for column in frame.columns]
                if not frame.empty and "tradedate" in frame:
                    if set(frame["tradedate"].astype(str)) != {day}:
                        raise RuntimeError("source returned rows outside exact requested date")
                return {
                    **metadata,
                    "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                    "rows": json.loads(
                        frame.to_json(orient="records", date_format="iso", double_precision=15)
                    ),
                }
            except Exception as exc:  # noqa: BLE001 - preserve source failure classification
                if attempt == 1:
                    return {**metadata, "error_type": type(exc).__name__, "rows": []}
                time.sleep(0.3)
        raise AssertionError("unreachable")

    jobs = [(day, dataset) for day in protocol["fetch_dates"] for dataset in DATASETS]
    results: list[dict[str, Any]] = []
    with contextlib.redirect_stdout(io.StringIO()):
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(fetch, job) for job in jobs]
            for future in as_completed(futures):
                results.append(future.result())
    return {"tables": sorted(results, key=lambda item: (item["date"], item["dataset"]))}


def normalize_tables(raw: dict[str, Any]) -> tuple[dict[tuple[str, str], pd.DataFrame], list[dict[str, Any]]]:
    frames: dict[tuple[str, str], pd.DataFrame] = {}
    quality: list[dict[str, Any]] = []
    for table in raw["tables"]:
        key = (str(table["date"]), str(table["dataset"]))
        frame = pd.DataFrame(table.get("rows", []))
        record: dict[str, Any] = {
            "date": key[0],
            "dataset": key[1],
            "rows": int(len(frame.index)),
            "error_type": table.get("error_type"),
        }
        if not frame.empty:
            frame.columns = [str(column).lower() for column in frame.columns]
            frame["ts"] = pd.to_datetime(
                frame["tradedate"].astype(str) + " " + frame["tradetime"].astype(str)
            )
            frame["publication_ts"] = pd.to_datetime(frame["systime"])
            if key[1] == "tradestats":
                numeric = [
                    "pr_open", "pr_high", "pr_low", "pr_close", "pr_vwap", "vol",
                    "vol_b", "vol_s", "oi_open", "oi_close",
                ]
                for column in numeric:
                    frame[column] = pd.to_numeric(frame[column], errors="raise")
                frame = frame.sort_values(["ts", "publication_ts"]).drop_duplicates("ts", keep="first")
                if not bool((frame["vol_b"] + frame["vol_s"] == frame["vol"]).all()):
                    raise RuntimeError(f"TradeStats volume identity failed for {key[0]}")
                frame["delta"] = frame["vol_b"] - frame["vol_s"]
                frame["running_vwap"] = (frame["pr_vwap"] * frame["vol"]).cumsum() / frame["vol"].cumsum()
                frame["previous_high_6"] = frame["pr_high"].shift().rolling(6).max()
                frame["break_high_6"] = (
                    (frame["pr_close"] > frame["previous_high_6"] + 1e-8) & (frame["delta"] > 0)
                )
                record["volume_identity"] = "PASS"
            else:
                required = {"clgroup", "pos", "pos_long", "pos_short"}
                if not required.issubset(frame.columns):
                    frame = pd.DataFrame()
                    record["futoi_quality"] = "MISSING_COLUMNS"
                else:
                    frame["clgroup"] = frame["clgroup"].astype(str).str.upper()
                    frame = frame.loc[frame["clgroup"].isin(FUTOI_GROUPS)].copy()
                    for column in ("pos", "pos_long", "pos_short"):
                        frame[column] = pd.to_numeric(frame[column], errors="coerce")
                    frame = frame.dropna(subset=["pos", "pos_long", "pos_short"])
                    frame = frame.sort_values(["ts", "publication_ts"]).drop_duplicates(
                        ["ts", "clgroup"], keep="first"
                    )
                    record["balanced_endpoints"] = int(
                        sum(
                            set(group["clgroup"]) == FUTOI_GROUPS
                            and float(group["pos"].sum()) == 0
                            for _, group in frame.groupby("ts")
                        )
                    )
        frames[key] = frame.reset_index(drop=True)
        quality.append(record)
    return frames, quality


def complete_window(
    frame: pd.DataFrame, start_exclusive: pd.Timestamp, end_inclusive: pd.Timestamp, count: int
) -> pd.DataFrame | None:
    selected = frame.loc[(frame["ts"] > start_exclusive) & (frame["ts"] <= end_inclusive)].copy()
    if len(selected.index) != count:
        return None
    if count > 1 and not bool(selected["ts"].diff().dropna().eq(pd.Timedelta(minutes=5)).all()):
        return None
    return selected


def window_stats(frame: pd.DataFrame | None, prefix: str) -> dict[str, Any]:
    if frame is None or frame.empty or float(frame["vol"].sum()) <= 0:
        return {}
    return {
        prefix + "_flow_pct": float(100 * frame["delta"].sum() / frame["vol"].sum()),
        prefix + "_oi_pct": float(100 * (frame.iloc[-1]["oi_close"] / frame.iloc[0]["oi_open"] - 1)),
        prefix + "_return_pct": float(100 * (frame.iloc[-1]["pr_close"] / frame.iloc[0]["pr_open"] - 1)),
        prefix + "_volume": int(frame["vol"].sum()),
    }


def futoi_snapshot(frame: pd.DataFrame, endpoint: pd.Timestamp, maximum_age_minutes: int = 10) -> dict[str, Any] | None:
    if frame.empty:
        return None
    candidates = frame.loc[frame["ts"] <= endpoint]
    for timestamp in sorted(candidates["ts"].unique(), reverse=True):
        ts = pd.Timestamp(timestamp)
        if endpoint - ts > pd.Timedelta(minutes=maximum_age_minutes):
            return None
        group = candidates.loc[candidates["ts"].eq(ts)]
        if set(group["clgroup"]) != FUTOI_GROUPS or float(group["pos"].sum()) != 0:
            continue
        by_group = {str(row["clgroup"]): row for _, row in group.iterrows()}
        denominator = float(by_group["FIZ"]["pos_long"] + by_group["YUR"]["pos_long"])
        if denominator <= 0:
            return None
        return {"ts": ts, "denominator": denominator, "groups": by_group}
    return None


def futoi_diff(first: dict[str, Any] | None, second: dict[str, Any] | None) -> dict[str, Any]:
    if first is None or second is None:
        return {}
    denominator = float(first["denominator"])
    first_fiz = first["groups"]["FIZ"]
    second_fiz = second["groups"]["FIZ"]
    return {
        "fiz_long_change_pp": float(100 * (second_fiz["pos_long"] - first_fiz["pos_long"]) / denominator),
        "fiz_net_change_pp": float(100 * (second_fiz["pos"] - first_fiz["pos"]) / denominator),
        "futoi_start_ts": first["ts"],
        "futoi_end_ts": second["ts"],
    }


def analyze(
    panel: pd.DataFrame,
    protocol: dict[str, Any],
    frames: dict[tuple[str, str], pd.DataFrame],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    panel_index = {str(day): int(index) for index, day in enumerate(panel["trade_date"].astype(str))}
    daily: dict[str, dict[str, Any]] = {}
    for day in protocol["fetch_dates"]:
        frame = frames[(day, "tradestats")]
        if frame.empty:
            continue
        index = panel_index[day]
        previous = panel.iloc[index - 1]
        prior = panel.iloc[:index].copy()
        weekday_kind = pd.Timestamp(day).dayofweek < 5
        same_kind = prior.loc[(prior["weekday"] < 5).eq(weekday_kind)].tail(20)
        session_range = float(frame["pr_high"].max() - frame["pr_low"].min())
        volume = float(frame["vol"].sum())
        vwap = float((frame["pr_vwap"] * frame["vol"]).sum() / volume)
        low = float(frame["pr_low"].min())
        close = float(frame.iloc[-1]["pr_close"])
        open_price = float(frame.iloc[0]["pr_open"])
        atr20 = float(panel.loc[index, "atr20"])
        record: dict[str, Any] = {
            "date": day,
            "open": open_price,
            "high": float(frame["pr_high"].max()),
            "low": low,
            "close": close,
            "volume": volume,
            "rvol20": float(volume / same_kind["USDRUBF_volume"].median()),
            "volume_ratio_previous": float(volume / previous["USDRUBF_volume"]),
            "return_pct": float(100 * (close / previous["USDRUBF_close"] - 1)),
            "flow_pct": float(100 * frame["delta"].sum() / volume),
            "oi_pct": float(100 * (frame.iloc[-1]["oi_close"] / previous["USDRUBF_oi_close"] - 1)),
            "close_location": float((close - low) / session_range),
            "lower_wick": float((min(open_price, close) - low) / session_range),
            "close_above_vwap": bool(close > vwap),
            "close_vs_vwap_pct": float(100 * (close / vwap - 1)),
            "low_to_close_atr": float((close - low) / atr20),
            "new_5_session_low": bool(low <= prior.tail(5)["USDRUBF_low"].min()),
            "previous_session_down": bool(
                previous["USDRUBF_close"] < panel.iloc[index - 2]["USDRUBF_close"]
            ),
            "previous_session_close_location": float(
                (previous["USDRUBF_close"] - previous["USDRUBF_low"])
                / (previous["USDRUBF_high"] - previous["USDRUBF_low"])
            ),
        }
        previous_day = str(previous["trade_date"])
        previous_ts = frames.get((previous_day, "tradestats"), pd.DataFrame())
        previous_futoi = frames.get((previous_day, "futoi"), pd.DataFrame())
        target_futoi = frames.get((day, "futoi"), pd.DataFrame())
        if not previous_ts.empty:
            record.update(
                futoi_diff(
                    futoi_snapshot(previous_futoi, pd.Timestamp(previous_ts.iloc[-1]["ts"])),
                    futoi_snapshot(target_futoi, pd.Timestamp(frame.iloc[-1]["ts"])),
                )
            )
        daily[day] = record

    events: list[dict[str, Any]] = []
    for selected in protocol["events"]:
        day = str(selected["date"])
        frame = frames[(day, "tradestats")]
        if frame.empty:
            continue
        first_low_index = int(frame.index[frame["pr_low"].eq(frame["pr_low"].min())][0])
        trough = frame.loc[first_low_index]
        trough_ts = pd.Timestamp(trough["ts"])
        pre30 = complete_window(frame, trough_ts - pd.Timedelta(minutes=35), trough_ts - pd.Timedelta(minutes=5), 6)
        prior30 = complete_window(frame, trough_ts - pd.Timedelta(minutes=65), trough_ts - pd.Timedelta(minutes=35), 6)
        post30 = complete_window(frame, trough_ts, trough_ts + pd.Timedelta(minutes=30), 6)
        post60 = complete_window(frame, trough_ts, trough_ts + pd.Timedelta(minutes=60), 12)
        record = {
            **selected,
            "trough_time": trough_ts.time().isoformat(),
            "trough_fraction_session": float(first_low_index / max(1, len(frame.index) - 1)),
            "trough_flow_pct": float(100 * trough["delta"] / trough["vol"]) if trough["vol"] else None,
            "day": {
                offset: daily[date]
                for offset, date in selected["windows"].items()
                if offset in {"-1", "0", "1"} and date in daily
            },
            **window_stats(pre30, "pre30"),
            **window_stats(prior30, "prior30"),
            **window_stats(post30, "post30"),
            **window_stats(post60, "post60"),
        }
        if post60 is not None:
            signals = post60.loc[post60["break_high_6"]]
            record["break_high_6_positive_delta_within_60m"] = not signals.empty
            record["above_vwap_within_60m"] = bool(
                (post60["pr_close"] > post60["running_vwap"]).any()
            )
            record["post60_rise_from_low_pct"] = float(
                100 * (post60["pr_high"].max() / trough["pr_low"] - 1)
            )
            if not signals.empty:
                signal = signals.iloc[0]
                record["break_high_6_delay_minutes"] = float(
                    (pd.Timestamp(signal["ts"]) - trough_ts).total_seconds() / 60
                )
                record["first_break"] = {
                    "time": pd.Timestamp(signal["ts"]).time().isoformat(),
                    "close": float(signal["pr_close"]),
                    "publication_ts": pd.Timestamp(signal["publication_ts"]).isoformat(),
                }
            futoi = frames.get((day, "futoi"), pd.DataFrame())
            record.update(
                {
                    "post60_" + key: value
                    for key, value in futoi_diff(
                        futoi_snapshot(futoi, trough_ts),
                        futoi_snapshot(futoi, trough_ts + pd.Timedelta(minutes=60)),
                    ).items()
                }
            )
        events.append(record)

    event_rows = [row for row in events if row["kind"] == "event"]
    control_rows = [row for row in events if row["kind"] == "control"]
    lookup = {row["date"]: row for row in events}

    def values(group: list[dict[str, Any]], field: str, offset: str | None = None) -> list[Any]:
        if offset is None:
            return [row.get(field) for row in group]
        return [row.get("day", {}).get(offset, {}).get(field) for row in group]

    def summarize(items: list[Any]) -> dict[str, Any]:
        numeric = np.array(
            [float(item) for item in items if item is not None and not pd.isna(item)], dtype=float
        )
        return {
            "n": int(len(numeric)),
            "median": float(np.median(numeric)) if len(numeric) else None,
            "q25": float(np.quantile(numeric, 0.25)) if len(numeric) else None,
            "q75": float(np.quantile(numeric, 0.75)) if len(numeric) else None,
            "positive": int((numeric > 0).sum()),
            "negative": int((numeric < 0).sum()),
        }

    daily_fields = [
        "return_pct", "rvol20", "volume_ratio_previous", "flow_pct", "oi_pct",
        "close_location", "lower_wick", "close_vs_vwap_pct", "close_above_vwap",
        "low_to_close_atr", "fiz_long_change_pp", "new_5_session_low",
        "previous_session_down", "previous_session_close_location",
    ]
    intraday_fields = [
        "pre30_flow_pct", "prior30_flow_pct", "pre30_oi_pct", "post30_flow_pct",
        "post60_flow_pct", "post60_oi_pct", "break_high_6_positive_delta_within_60m",
        "break_high_6_delay_minutes", "above_vwap_within_60m",
        "post60_rise_from_low_pct", "post60_fiz_long_change_pp",
    ]
    groups = {
        "L3plus": event_rows,
        "exact_L3": [row for row in event_rows if int(row["L_order"]) == 3],
        "L4plus": [row for row in event_rows if int(row["L_order"]) >= 4],
        "L5plus": [row for row in event_rows if int(row["L_order"]) >= 5],
        "controls": control_rows,
        "modern_L3plus": [row for row in event_rows if row["date"] >= "2023-06-01"],
        "modern_controls": [row for row in control_rows if row["date"] >= "2023-06-01"],
    }
    summary: dict[str, Any] = {}
    for name, group in groups.items():
        summary[name] = {
            "n": len(group),
            "dates": [row["date"] for row in group],
            "daily": {
                offset: {field: summarize(values(group, field, offset)) for field in daily_fields}
                for offset in ("-1", "0", "1")
            },
            "intraday": {field: summarize(values(group, field)) for field in intraday_fields},
        }

    def score_candidate(row: dict[str, Any]) -> dict[str, Any]:
        day = row["day"].get("0", {})
        checks = {
            "lower_wick_fraction_gte_0_4": float(day.get("lower_wick", -1)) >= 0.4,
            "close_location_gte_0_65": float(day.get("close_location", -1)) >= 0.65,
            "close_above_session_vwap": bool(day.get("close_above_vwap", False)),
            "break_previous_6_bar_high_with_positive_delta_within_60m": bool(
                row.get("break_high_6_positive_delta_within_60m", False)
            ),
            "low_to_close_atr_gte_0_5": float(day.get("low_to_close_atr", -1)) >= 0.5,
            "relative_volume_gte_1_3": float(day.get("rvol20", -1)) >= 1.3,
            "volume_vs_previous_gte_1_25": float(day.get("volume_ratio_previous", -1)) >= 1.25,
            "daily_flow_pct_lte_minus_2": float(day.get("flow_pct", 999)) <= -2.0,
            "daily_oi_pct_lte_minus_1": float(day.get("oi_pct", 999)) <= -1.0,
            "previous_session_down_and_close_location_lte_0_25": bool(
                day.get("previous_session_down", False)
            ) and float(day.get("previous_session_close_location", 999)) <= 0.25,
        }
        weights = {
            "lower_wick_fraction_gte_0_4": 2,
            "close_location_gte_0_65": 2,
            "close_above_session_vwap": 1,
            "break_previous_6_bar_high_with_positive_delta_within_60m": 2,
            "low_to_close_atr_gte_0_5": 1,
            "relative_volume_gte_1_3": 2,
            "volume_vs_previous_gte_1_25": 1,
            "daily_flow_pct_lte_minus_2": 1,
            "daily_oi_pct_lte_minus_1": 1,
            "previous_session_down_and_close_location_lte_0_25": 1,
        }
        new_low = bool(day.get("new_5_session_low", False))
        rejection = (
            float(day.get("lower_wick", -1)) >= 0.4
            and float(day.get("close_location", -1)) >= 0.5
        )
        confirmation = bool(row.get("break_high_6_positive_delta_within_60m", False)) or (
            bool(day.get("close_above_vwap", False))
            and float(day.get("low_to_close_atr", -1)) >= 0.5
        )
        score = sum(weights[key] for key, passed in checks.items() if passed)
        return {
            "score": score,
            "checks": checks,
            "mandatory": {
                "new_5_session_low": new_low,
                "price_rejection": rejection,
                "intraday_confirmation": confirmation,
            },
        }

    diagnostics: dict[str, Any] = {"thresholds": {}}
    scored = [(row, score_candidate(row)) for row in events]
    for threshold in range(7, 13):
        record: dict[str, Any] = {}
        for kind in ("event", "control"):
            subset = [(row, decision) for row, decision in scored if row["kind"] == kind]
            alerts = [
                row["date"]
                for row, decision in subset
                if all(decision["mandatory"].values()) and decision["score"] >= threshold
            ]
            watches = [
                row["date"]
                for row, decision in subset
                if decision["mandatory"]["new_5_session_low"]
                and decision["mandatory"]["price_rejection"]
                and decision["score"] >= 7
                and row["date"] not in alerts
            ]
            record[kind] = {"n": len(subset), "alerts": alerts, "watches": watches}
        diagnostics["thresholds"][str(threshold)] = record
    diagnostics["rows"] = [
        {
            "date": row["date"],
            "kind": row["kind"],
            "L_order": row["L_order"],
            **decision,
        }
        for row, decision in scored
    ]
    return events, summary, diagnostics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-panel", type=Path, required=True)
    parser.add_argument("--analysis-end", default="2026-10-06")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--raw-cache", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--refresh-raw", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    panel = load_panel(args.daily_panel, args.analysis_end)
    protocol = select_protocol(panel, args.analysis_end)
    write_json(args.output_dir / "protocol.json", protocol)
    if args.refresh_raw or not args.raw_cache.exists():
        raw = fetch_raw(protocol, args.repo_root.resolve(), args.env_file, args.workers)
        write_json(args.raw_cache, raw)
    else:
        raw = json.loads(args.raw_cache.read_text(encoding="utf-8"))
    frames, quality = normalize_tables(raw)
    failures = [
        row for row in quality
        if row.get("error_type") or (row["dataset"] == "tradestats" and row["rows"] == 0)
    ]
    if failures:
        write_json(args.output_dir / "quality.json", quality)
        raise RuntimeError(f"mandatory source failures: {failures[:5]}")
    events, summary, diagnostics = analyze(panel, protocol, frames)
    write_json(args.output_dir / "events.json", events)
    write_json(args.output_dir / "summary.json", summary)
    write_json(args.output_dir / "rule_diagnostics.json", diagnostics)
    write_json(args.output_dir / "quality.json", quality)
    print(
        json.dumps(
            {
                "events": summary["L3plus"]["n"],
                "controls": summary["controls"]["n"],
                "L4plus": summary["L4plus"]["n"],
                "L5plus": summary["L5plus"]["n"],
                "thresholds": diagnostics["thresholds"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
