#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path.cwd() / "src"))

import pandas as pd
import requests

from moex_data.futures import liquidity_history_metrics_probe as base


def _xml_attr(row: ET.Element, name: str) -> str:
    by_lower = {str(k).lower(): v for k, v in row.attrib.items()}
    value = by_lower.get(name.lower(), "")
    if value is None:
        return ""
    return str(value).strip()


def _truthy_workday(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip().lower()
    if not text:
        return False
    try:
        return int(float(text)) == 1
    except Exception:
        return text in ["1", "true", "t", "yes", "y"]


def _apim_calendar_base_url() -> str:
    return os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL).strip() or base.DEFAULT_APIM_BASE_URL


def _max_workers_default() -> int:
    raw = os.getenv("MOEX_LIQUIDITY_HISTORY_MAX_WORKERS", "4").strip()
    try:
        return int(raw)
    except Exception:
        return 4


def _bounded_workers(requested: int, item_count: int) -> int:
    try:
        value = int(requested)
    except Exception:
        value = 1
    if value < 1:
        value = 1
    if item_count > 0:
        value = min(value, int(item_count))
    return value


def fetch_futures_calendar(screen_from: str, screen_till: str, timeout: float, unused_iss_base_url: str) -> Tuple[Optional[Set[str]], str]:
    out: Set[str] = set()
    required = ["tradedate", "futures_workday", "futures_reason"]
    parsed_rows = 0
    try:
        for chunk_from, chunk_till in base.year_chunks(screen_from, screen_till):
            params: Dict[str, Any] = {
                "from": chunk_from,
                "till": chunk_till,
                "show_all_days": "1",
                "iss.only": "off_days",
                "iss.meta": "off",
            }
            response = requests.get(
                base.url_join(_apim_calendar_base_url(), "/iss/calendars"),
                params=params,
                headers=base.auth_headers(True),
                timeout=timeout,
            )
            response.raise_for_status()
            root = ET.fromstring(response.content)
            for row in root.iter():
                if str(row.tag).split("}")[-1] != "row":
                    continue
                attrs = {str(k).lower() for k in row.attrib.keys()}
                if not all(x in attrs for x in required):
                    continue
                parsed_rows += 1
                if not _truthy_workday(_xml_attr(row, "futures_workday")):
                    continue
                canonical_trade_date = base.parse_iso_date(_xml_attr(row, "tradedate"))
                if canonical_trade_date:
                    out.add(canonical_trade_date)
    except Exception as exc:
        return None, "unresolved: " + exc.__class__.__name__ + ": " + str(exc)[:300]
    if parsed_rows == 0:
        return None, "unresolved: apim_xml_off_days_rows_not_found"
    if not out:
        return None, "unresolved: apim_xml_no_futures_workdays_detected"
    return out, "canonical_apim_futures_xml"


_original_compute_one_metrics = base.compute_one_metrics


def compute_one_metrics(*args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    liquidity, history = _original_compute_one_metrics(*args, **kwargs)
    expected_calendar = args[3] if len(args) > 3 else kwargs.get("expected_calendar")
    calendar_note = args[4] if len(args) > 4 else kwargs.get("calendar_note", "")
    if expected_calendar is not None and calendar_note == "canonical_apim_futures_xml":
        status = "canonical_apim_futures_xml"
    else:
        status = "unresolved"
        history["history_depth_status"] = "review_required"
        history["review_notes"] = "calendar denominator not safely computed; " + str(calendar_note)
        history["validation_status"] = "metrics_computed"
        history["review_status"] = "ready_for_pm_review"
    liquidity["calendar_denominator_status"] = status
    history["calendar_denominator_status"] = status
    return liquidity, history


def _compute_for_instrument(
    index: int,
    instrument: pd.Series,
    screen_from: str,
    screen_till: str,
    expected_calendar: Optional[Set[str]],
    calendar_note: str,
    liquidity_profile: Dict[str, Any],
    history_profile: Dict[str, Any],
    recent_gap_days: int,
    full_history_proven: bool,
    allow_bounded_pass: bool,
    timeout: float,
    apim_base_url: str,
    iss_base_url: str,
    snapshot_date: str,
) -> Tuple[int, Dict[str, Any], Dict[str, Any]]:
    liquidity, history = compute_one_metrics(
        instrument,
        screen_from,
        screen_till,
        expected_calendar,
        calendar_note,
        liquidity_profile,
        history_profile,
        recent_gap_days,
        full_history_proven,
        allow_bounded_pass,
        timeout,
        apim_base_url,
        iss_base_url,
    )
    liquidity["snapshot_date"] = snapshot_date
    history["snapshot_date"] = snapshot_date
    return index, liquidity, history


def _compute_all_metrics(
    instruments: pd.DataFrame,
    max_workers: int,
    screen_from: str,
    screen_till: str,
    expected_calendar: Optional[Set[str]],
    calendar_note: str,
    liquidity_profile: Dict[str, Any],
    history_profile: Dict[str, Any],
    recent_gap_days: int,
    full_history_proven: bool,
    allow_bounded_pass: bool,
    timeout: float,
    apim_base_url: str,
    iss_base_url: str,
    snapshot_date: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    selected_count = int(len(instruments))
    workers = _bounded_workers(max_workers, selected_count)
    tasks = [(int(pos), row.copy()) for pos, (_, row) in enumerate(instruments.iterrows())]
    started = time.time()
    results: List[Tuple[int, Dict[str, Any], Dict[str, Any]]] = []
    if workers == 1:
        for pos, instrument in tasks:
            results.append(_compute_for_instrument(
                pos,
                instrument,
                screen_from,
                screen_till,
                expected_calendar,
                calendar_note,
                liquidity_profile,
                history_profile,
                recent_gap_days,
                full_history_proven,
                allow_bounded_pass,
                timeout,
                apim_base_url,
                iss_base_url,
                snapshot_date,
            ))
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_by_pos = {
                executor.submit(
                    _compute_for_instrument,
                    pos,
                    instrument,
                    screen_from,
                    screen_till,
                    expected_calendar,
                    calendar_note,
                    liquidity_profile,
                    history_profile,
                    recent_gap_days,
                    full_history_proven,
                    allow_bounded_pass,
                    timeout,
                    apim_base_url,
                    iss_base_url,
                    snapshot_date,
                ): pos
                for pos, instrument in tasks
            }
            for future in as_completed(future_by_pos):
                results.append(future.result())
    results = sorted(results, key=lambda item: item[0])
    liquidity_rows = [item[1] for item in results]
    history_rows = [item[2] for item in results]
    guard = {
        "selected_instrument_count": selected_count,
        "probe_count": int(len(results)),
        "liquidity_row_count": int(len(liquidity_rows)),
        "history_row_count": int(len(history_rows)),
        "row_count_matches_selected_instruments": bool(len(liquidity_rows) == selected_count and len(history_rows) == selected_count),
    }
    if not guard["row_count_matches_selected_instruments"]:
        raise RuntimeError("liquidity_history_row_count_guard_failed: " + json.dumps(guard, sort_keys=True))
    summary = dict(guard)
    summary.update({
        "max_workers": workers,
        "duration_sec": round(time.time() - started, 3),
        "ordering": "input_order_preserved_after_concurrent_execution",
        "sequential_fallback": bool(workers == 1),
    })
    return liquidity_rows, history_rows, summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=base.today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--history-lookback-days", type=int, default=365)
    parser.add_argument("--recent-gap-days", type=int, default=10)
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=45.0)
    parser.add_argument("--max-workers", dest="liquidity_max_workers", type=int, default=_max_workers_default())
    parser.add_argument("--liquidity-max-workers", dest="liquidity_max_workers", type=int)
    parser.add_argument("--full-history-proven", action="store_true")
    parser.add_argument("--allow-bounded-pass", action="store_true")
    args = parser.parse_args()

    total_started = time.time()
    section_timing: Dict[str, float] = {}
    root = base.repo_root()
    snapshot_date = str(args.snapshot_date).strip()
    data_root = base.resolve_data_root(args)
    screen_from, screen_till = base.date_range_defaults(snapshot_date, args)

    setup_started = time.time()
    base.assert_files_exist(root, base.REQUIRED_CONTRACTS + base.REQUIRED_CONFIGS)
    contracts = base.load_contract_values(root)
    liquidity_profile = base.threshold_profile(base.read_json(root / "configs/datasets/futures_liquidity_screen_thresholds_config.json"))
    history_profile = base.threshold_profile(base.read_json(root / "configs/datasets/futures_history_depth_thresholds_config.json"))
    normalized_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["normalized_registry"], snapshot_date)
    availability_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["tradestats_availability"], snapshot_date)
    liquidity_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["liquidity_screen"], snapshot_date)
    history_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["history_depth_screen"], snapshot_date)
    if not normalized_path.exists():
        raise FileNotFoundError("Missing normalized registry artifact: " + str(normalized_path))
    if not availability_path.exists():
        raise FileNotFoundError("Missing tradestats availability artifact: " + str(availability_path))
    section_timing["setup_sec"] = round(time.time() - setup_started, 3)

    load_started = time.time()
    normalized = pd.read_parquet(normalized_path)
    availability = pd.read_parquet(availability_path)
    instruments = base.selected_instruments_from_artifacts(normalized, availability)
    selected_count = int(len(instruments))
    section_timing["artifact_load_sec"] = round(time.time() - load_started, 3)

    calendar_started = time.time()
    expected_calendar, calendar_note = fetch_futures_calendar(screen_from, screen_till, float(args.timeout), str(args.iss_base_url))
    section_timing["calendar_fetch_sec"] = round(time.time() - calendar_started, 3)

    liquidity_rows, history_rows, probe_summary = _compute_all_metrics(
        instruments,
        int(args.liquidity_max_workers),
        screen_from,
        screen_till,
        expected_calendar,
        calendar_note,
        liquidity_profile,
        history_profile,
        int(args.recent_gap_days),
        bool(args.full_history_proven),
        bool(args.allow_bounded_pass),
        float(args.timeout),
        str(args.apim_base_url),
        str(args.iss_base_url),
        snapshot_date,
    )
    section_timing["instrument_probe_sec"] = float(probe_summary["duration_sec"])

    write_started = time.time()
    liquidity_df = pd.DataFrame(liquidity_rows)
    history_df = pd.DataFrame(history_rows)
    base.validate_primary_key(liquidity_df, ["liquidity_screen_id", "snapshot_date", "board", "secid"], "futures_liquidity_screen")
    base.validate_primary_key(history_df, ["history_depth_screen_id", "snapshot_date", "board", "secid"], "futures_history_depth_screen")
    base.write_parquet(liquidity_df, liquidity_path)
    base.write_parquet(history_df, history_path)
    section_timing["write_outputs_sec"] = round(time.time() - write_started, 3)
    section_timing["total_sec"] = round(time.time() - total_started, 3)

    output_paths = {
        "futures_liquidity_screen": str(liquidity_path),
        "futures_history_depth_screen": str(history_path),
    }
    selected = instruments[["board", "secid", "family_code"]].to_dict("records")
    history_window = {
        "screen_from": screen_from,
        "screen_till": screen_till,
        "history_lookback_days": int(args.history_lookback_days),
        "full_history_proven": bool(args.full_history_proven),
        "calendar_status": "computed" if expected_calendar is not None else "unavailable",
        "calendar_note": calendar_note or None,
    }
    timing_summary = {
        "selected_instrument_count": selected_count,
        "max_workers": int(probe_summary["max_workers"]),
        "section_timing_sec": section_timing,
        "instrument_probe": probe_summary,
        "calendar_status": history_window["calendar_status"],
        "row_count_guard": {
            "liquidity_row_count": int(len(liquidity_df)),
            "history_row_count": int(len(history_df)),
            "selected_instrument_count": selected_count,
            "row_count_matches_selected_instruments": bool(len(liquidity_df) == selected_count and len(history_df) == selected_count),
        },
    }

    base.print_json_line("output_artifacts_created", output_paths)
    base.print_json_line("selected_instruments_covered", selected)
    base.print_json_line("history_window_checked", history_window)
    base.print_json_line("liquidity_metrics_summary", base.summarize_screen(liquidity_df, "liquidity_status"))
    base.print_json_line("history_depth_metrics_summary", base.summarize_screen(history_df, "history_depth_status"))
    base.print_json_line("liquidity_history_probe_timing_summary", timing_summary)
    return 0


base.fetch_futures_calendar = fetch_futures_calendar
base.compute_one_metrics = compute_one_metrics


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
