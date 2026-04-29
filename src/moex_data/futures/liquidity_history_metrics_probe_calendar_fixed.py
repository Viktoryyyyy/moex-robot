#!/usr/bin/env python3
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import pandas as pd
import requests

from src.moex_data.futures import liquidity_history_metrics_probe as base


CALENDAR_STATUS_CANDIDATES = [
    "futures_workday",
    "FUTURES_WORKDAY",
    "futures",
    "FUTURES",
    "futures_",
    "FUTURES_",
    "is_traded",
    "IS_TRADED",
    "workday",
    "WORKDAY",
    "is_workday",
    "IS_WORKDAY",
]


def _response_note(response: requests.Response) -> str:
    text = (response.text or "").replace("\n", " ").replace("\r", " ")[:180]
    content_type = response.headers.get("Content-Type", "")
    return "status=" + str(response.status_code) + " content_type=" + str(content_type) + " body_prefix=" + text


def _request_calendar_json(base_url: str, path: str, params: Dict[str, Any], timeout: float) -> Tuple[Optional[Dict[str, Any]], str]:
    url = base.url_join(base_url, path)
    try:
        response = requests.get(url, params=params, headers=base.auth_headers(False), timeout=timeout)
    except Exception as exc:
        return None, path + " request_error=" + exc.__class__.__name__ + ": " + str(exc)[:220]
    if response.status_code < 200 or response.status_code >= 300:
        return None, path + " http_error " + _response_note(response)
    try:
        data = response.json()
    except Exception as exc:
        return None, path + " non_json_response " + exc.__class__.__name__ + ": " + _response_note(response)
    if not isinstance(data, dict):
        return None, path + " json_root_not_object"
    return data, ""


def _calendar_frame_from_response(data: Dict[str, Any]) -> pd.DataFrame:
    frame = base.block_to_frame(data, ["off_days", "data", "calendar"])
    if frame.empty:
        return frame
    date_col = base.canonical_column(frame, ["date", "DATE", "tradedate", "TRADEDATE"])
    status_col = base.canonical_column(frame, CALENDAR_STATUS_CANDIDATES)
    if not date_col or not status_col:
        return pd.DataFrame()
    frame = frame.copy()
    frame["__calendar_date_col"] = date_col
    frame["__calendar_status_col"] = status_col
    return frame


def _fetch_calendar_frame_once(base_urls: Iterable[str], path: str, params: Dict[str, Any], timeout: float) -> Tuple[pd.DataFrame, str]:
    notes = []
    for base_url in base_urls:
        data, note = _request_calendar_json(base_url, path, params, timeout)
        if note:
            notes.append(note)
            continue
        if data is None:
            continue
        frame = _calendar_frame_from_response(data)
        if not frame.empty:
            return frame, ""
        notes.append(path + " calendar_columns_not_found")
    return pd.DataFrame(), "; ".join(notes)[:700]


def fetch_futures_calendar(screen_from: str, screen_till: str, timeout: float, iss_base_url: str) -> Tuple[Optional[Set[str]], str]:
    frames = []
    errors = []
    base_urls = []
    if iss_base_url:
        base_urls.append(str(iss_base_url))
    if base.DEFAULT_ISS_BASE_URL not in base_urls:
        base_urls.append(base.DEFAULT_ISS_BASE_URL)
    paths = ["/iss/calendars.json", "/iss/calendars/futures.json"]
    for chunk_from, chunk_till in base.year_chunks(screen_from, screen_till):
        params = {
            "from": chunk_from,
            "till": chunk_till,
            "iss.only": "off_days",
            "show_all_days": "1",
            "iss.meta": "off",
        }
        chunk_frame = pd.DataFrame()
        chunk_notes = []
        for path in paths:
            frame, note = _fetch_calendar_frame_once(base_urls, path, params, timeout)
            if not frame.empty:
                chunk_frame = frame
                break
            if note:
                chunk_notes.append(note)
        if chunk_frame.empty:
            errors.append(chunk_from + ".." + chunk_till + " " + "; ".join(chunk_notes)[:700])
        else:
            frames.append(chunk_frame)
    if not frames:
        return None, "; ".join(errors)[:900] if errors else "calendar_empty_response"
    calendar = pd.concat(frames, ignore_index=True)
    out: Set[str] = set()
    for _, row in calendar.iterrows():
        date_col = row.get("__calendar_date_col")
        status_col = row.get("__calendar_status_col")
        if not date_col or not status_col or date_col not in row.index or status_col not in row.index:
            continue
        dt = base.parse_iso_date(row.get(date_col))
        if not dt:
            continue
        value = row.get(status_col)
        if pd.isna(value):
            continue
        try:
            is_open = int(float(value)) == 1
        except Exception:
            is_open = str(value).strip().lower() in ["1", "true", "t", "yes", "y"]
        if is_open:
            out.add(dt)
    if not out:
        return None, "calendar_no_trading_days_detected"
    return out, ""


base.fetch_futures_calendar = fetch_futures_calendar
main = base.main


if __name__ == "__main__":
    raise SystemExit(main())
