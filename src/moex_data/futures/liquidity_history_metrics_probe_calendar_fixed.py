#!/usr/bin/env python3
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import pandas as pd
import requests

from src.moex_data.futures import liquidity_history_metrics_probe as base


CANONICAL_CALENDAR_PATH = "/iss/calendars/futures.json"
CANONICAL_CALENDAR_FIELDS = ["tradedate", "is_traded", "trade_session_date", "reason", "updatetime"]
CALENDAR_STATUS_CANDIDATES = ["is_traded", "IS_TRADED"]


def _response_note(response: requests.Response) -> str:
    text = (response.text or "").replace("\n", " ").replace("\r", " ")[:180]
    content_type = response.headers.get("Content-Type", "")
    return "status=" + str(response.status_code) + " content_type=" + str(content_type) + " body_prefix=" + text


def _calendar_frame_from_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    by_upper = {str(c).upper(): str(c) for c in frame.columns}
    missing = [field for field in CANONICAL_CALENDAR_FIELDS if field.upper() not in by_upper]
    if missing:
        return pd.DataFrame()
    date_col = base.canonical_column(frame, ["tradedate", "TRADEDATE"])
    status_col = base.canonical_column(frame, CALENDAR_STATUS_CANDIDATES)
    if not date_col or not status_col:
        return pd.DataFrame()
    out = frame.copy()
    out["__calendar_date_col"] = date_col
    out["__calendar_status_col"] = status_col
    out["__calendar_observed_fields"] = ",".join([str(c) for c in frame.columns])
    return out


def _calendar_frame_from_json(data: Dict[str, Any]) -> pd.DataFrame:
    raw = data.get("off_days")
    if not isinstance(raw, dict):
        return pd.DataFrame()
    columns = raw.get("columns") or []
    rows = raw.get("data") or []
    if not isinstance(columns, list) or not isinstance(rows, list) or not columns:
        return pd.DataFrame()
    return _calendar_frame_from_table(pd.DataFrame(rows, columns=columns))


def _request_calendar_frame(base_url: str, path: str, params: Dict[str, Any], timeout: float) -> Tuple[pd.DataFrame, str]:
    url = base.url_join(base_url, path)
    try:
        response = requests.get(url, params=params, headers=base.auth_headers(False), timeout=timeout)
    except Exception as exc:
        return pd.DataFrame(), path + " request_error=" + exc.__class__.__name__ + ": " + str(exc)[:220]
    if response.status_code < 200 or response.status_code >= 300:
        return pd.DataFrame(), path + " http_error " + _response_note(response)
    try:
        data = response.json()
    except Exception as exc:
        return pd.DataFrame(), path + " non_json_response " + exc.__class__.__name__ + ": " + _response_note(response)
    if not isinstance(data, dict):
        return pd.DataFrame(), path + " json_root_not_object"
    frame = _calendar_frame_from_json(data)
    if frame.empty:
        return pd.DataFrame(), path + " off_days_required_fields_not_found"
    return frame, ""


def _fetch_calendar_frame_once(base_urls: Iterable[str], params: Dict[str, Any], timeout: float) -> Tuple[pd.DataFrame, str]:
    notes = []
    for base_url in base_urls:
        frame, note = _request_calendar_frame(base_url, CANONICAL_CALENDAR_PATH, params, timeout)
        if not frame.empty:
            return frame, ""
        if note:
            notes.append(note)
    return pd.DataFrame(), "; ".join(notes)[:700]


def fetch_futures_calendar(screen_from: str, screen_till: str, timeout: float, iss_base_url: str) -> Tuple[Optional[Set[str]], str]:
    frames = []
    errors = []
    base_urls = []
    if iss_base_url:
        base_urls.append(str(iss_base_url))
    if base.DEFAULT_ISS_BASE_URL not in base_urls:
        base_urls.append(base.DEFAULT_ISS_BASE_URL)
    for chunk_from, chunk_till in base.year_chunks(screen_from, screen_till):
        params = {
            "from": chunk_from,
            "till": chunk_till,
            "iss.only": "off_days",
            "show_all_days": "1",
            "iss.meta": "off",
        }
        frame, note = _fetch_calendar_frame_once(base_urls, params, timeout)
        if frame.empty:
            errors.append(chunk_from + ".." + chunk_till + " " + note)
        else:
            frames.append(frame)
    if not frames:
        return None, "; ".join(errors)[:900] if errors else "canonical_iss_futures_calendar_empty_response"
    calendar = pd.concat(frames, ignore_index=True)
    out: Set[str] = set()
    observed_fields = sorted(set([str(x) for x in calendar.columns if not str(x).startswith("__calendar_")]))
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
        return None, "canonical_iss_futures_calendar_no_trading_days_detected"
    note = "calendar_binding_status=canonical_iss_futures_json; endpoint=" + CANONICAL_CALENDAR_PATH + "; fields=" + ",".join(observed_fields)
    return out, note


base.fetch_futures_calendar = fetch_futures_calendar
main = base.main


if __name__ == "__main__":
    raise SystemExit(main())
