#!/usr/bin/env python3
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import pandas as pd
import requests

from src.moex_data.futures import liquidity_history_metrics_probe as base


DEDICATED_CALENDAR_PATH = "/iss/calendars/futures.json"
DEDICATED_CALENDAR_FIELDS = ["tradedate", "is_traded", "trade_session_date", "reason", "updatetime"]
DEDICATED_STATUS_CANDIDATES = ["is_traded", "IS_TRADED"]
DEDICATED_SESSION_DATE_CANDIDATES = ["trade_session_date", "TRADE_SESSION_DATE"]
AGGREGATE_CALENDAR_PATH = "/iss/calendars.json"
AGGREGATE_CALENDAR_FIELDS = ["tradedate", "futures_workday", "futures_trade_session_date", "futures_reason"]
AGGREGATE_STATUS_CANDIDATES = ["futures_workday", "FUTURES_WORKDAY"]
AGGREGATE_SESSION_DATE_CANDIDATES = ["futures_trade_session_date", "FUTURES_TRADE_SESSION_DATE"]


def _response_note(response: requests.Response) -> str:
    text = (response.text or "").replace("\n", " ").replace("\r", " ")[:180]
    content_type = response.headers.get("Content-Type", "")
    return "status=" + str(response.status_code) + " content_type=" + str(content_type) + " body_prefix=" + text


def _truthy_one(value: Any) -> bool:
    if pd.isna(value):
        return False
    try:
        return int(float(value)) == 1
    except Exception:
        return str(value).strip().lower() in ["1", "true", "t", "yes", "y"]


def _field_map(frame: pd.DataFrame) -> Dict[str, str]:
    return {str(c).upper(): str(c) for c in frame.columns}


def _calendar_frame_from_table(
    frame: pd.DataFrame,
    required_fields: Iterable[str],
    date_candidates: Iterable[str],
    status_candidates: Iterable[str],
    session_date_candidates: Iterable[str],
    denominator_status: str,
    endpoint_path: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    by_upper = _field_map(frame)
    missing = [field for field in required_fields if field.upper() not in by_upper]
    if missing:
        return pd.DataFrame()
    date_col = base.canonical_column(frame, date_candidates)
    status_col = base.canonical_column(frame, status_candidates)
    session_col = base.canonical_column(frame, session_date_candidates)
    if not date_col or not status_col:
        return pd.DataFrame()
    out = frame.copy()
    out["__calendar_date_col"] = date_col
    out["__calendar_status_col"] = status_col
    out["__calendar_session_date_col"] = session_col or ""
    out["__calendar_denominator_status"] = denominator_status
    out["__calendar_endpoint_path"] = endpoint_path
    out["__calendar_observed_fields"] = ",".join([str(c) for c in frame.columns])
    return out


def _calendar_frame_from_json(
    data: Dict[str, Any],
    required_fields: Iterable[str],
    date_candidates: Iterable[str],
    status_candidates: Iterable[str],
    session_date_candidates: Iterable[str],
    denominator_status: str,
    endpoint_path: str,
) -> pd.DataFrame:
    raw = data.get("off_days")
    if not isinstance(raw, dict):
        return pd.DataFrame()
    columns = raw.get("columns") or []
    rows = raw.get("data") or []
    if not isinstance(columns, list) or not isinstance(rows, list) or not columns:
        return pd.DataFrame()
    frame = pd.DataFrame(rows, columns=columns)
    return _calendar_frame_from_table(
        frame,
        required_fields,
        date_candidates,
        status_candidates,
        session_date_candidates,
        denominator_status,
        endpoint_path,
    )


def _request_calendar_frame(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    timeout: float,
    required_fields: Iterable[str],
    date_candidates: Iterable[str],
    status_candidates: Iterable[str],
    session_date_candidates: Iterable[str],
    denominator_status: str,
) -> Tuple[pd.DataFrame, str]:
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
    frame = _calendar_frame_from_json(
        data,
        required_fields,
        date_candidates,
        status_candidates,
        session_date_candidates,
        denominator_status,
        path,
    )
    if frame.empty:
        return pd.DataFrame(), path + " off_days_required_fields_not_found"
    return frame, ""


def _fetch_calendar_frame_once(
    base_urls: Iterable[str],
    path: str,
    params: Dict[str, Any],
    timeout: float,
    required_fields: Iterable[str],
    date_candidates: Iterable[str],
    status_candidates: Iterable[str],
    session_date_candidates: Iterable[str],
    denominator_status: str,
) -> Tuple[pd.DataFrame, str]:
    notes = []
    for base_url in base_urls:
        frame, note = _request_calendar_frame(
            base_url,
            path,
            params,
            timeout,
            required_fields,
            date_candidates,
            status_candidates,
            session_date_candidates,
            denominator_status,
        )
        if not frame.empty:
            return frame, ""
        if note:
            notes.append(note)
    return pd.DataFrame(), "; ".join(notes)[:700]


def _fetch_dedicated_calendar_frame(base_urls: Iterable[str], params: Dict[str, Any], timeout: float) -> Tuple[pd.DataFrame, str]:
    return _fetch_calendar_frame_once(
        base_urls,
        DEDICATED_CALENDAR_PATH,
        params,
        timeout,
        DEDICATED_CALENDAR_FIELDS,
        ["tradedate", "TRADEDATE"],
        DEDICATED_STATUS_CANDIDATES,
        DEDICATED_SESSION_DATE_CANDIDATES,
        "canonical_iss_futures_json",
    )


def _fetch_aggregate_calendar_frame(base_urls: Iterable[str], params: Dict[str, Any], timeout: float) -> Tuple[pd.DataFrame, str]:
    return _fetch_calendar_frame_once(
        base_urls,
        AGGREGATE_CALENDAR_PATH,
        params,
        timeout,
        AGGREGATE_CALENDAR_FIELDS,
        ["tradedate", "TRADEDATE"],
        AGGREGATE_STATUS_CANDIDATES,
        AGGREGATE_SESSION_DATE_CANDIDATES,
        "canonical_aggregate_futures_iss_fallback",
    )


def fetch_futures_calendar(screen_from: str, screen_till: str, timeout: float, iss_base_url: str) -> Tuple[Optional[Set[str]], str]:
    frames = []
    errors = []
    dedicated_notes = []
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
        frame, dedicated_note = _fetch_dedicated_calendar_frame(base_urls, params, timeout)
        if frame.empty:
            if dedicated_note:
                dedicated_notes.append(chunk_from + ".." + chunk_till + " " + dedicated_note)
            frame, aggregate_note = _fetch_aggregate_calendar_frame(base_urls, params, timeout)
            if frame.empty:
                errors.append(chunk_from + ".." + chunk_till + " dedicated=" + dedicated_note + " aggregate=" + aggregate_note)
            else:
                frames.append(frame)
        else:
            frames.append(frame)
    if not frames:
        return None, "; ".join(errors)[:900] if errors else "calendar_denominator_status=unresolved; calendar_empty_response"
    calendar = pd.concat(frames, ignore_index=True)
    out: Set[str] = set()
    observed_fields = sorted(set([str(x) for x in calendar.columns if not str(x).startswith("__calendar_")]))
    denominator_statuses = sorted(set([str(x) for x in calendar["__calendar_denominator_status"].dropna().astype(str).tolist()]))
    endpoint_paths = sorted(set([str(x) for x in calendar["__calendar_endpoint_path"].dropna().astype(str).tolist()]))
    for _, row in calendar.iterrows():
        date_col = row.get("__calendar_date_col")
        status_col = row.get("__calendar_status_col")
        session_col = row.get("__calendar_session_date_col")
        if not date_col or not status_col or date_col not in row.index or status_col not in row.index:
            continue
        value = row.get(status_col)
        if not _truthy_one(value):
            continue
        session_date = None
        if session_col and session_col in row.index:
            session_date = base.parse_iso_date(row.get(session_col))
        tradedate = base.parse_iso_date(row.get(date_col))
        dt = session_date or tradedate
        if dt:
            out.add(dt)
    if not out:
        return None, "calendar_denominator_status=unresolved; futures_calendar_no_trading_days_detected"
    if len(denominator_statuses) == 1:
        denominator_status = denominator_statuses[0]
    else:
        denominator_status = "mixed:" + ",".join(denominator_statuses)
    note = (
        "calendar_denominator_status=" + denominator_status
        + "; endpoint=" + ",".join(endpoint_paths)
        + "; fields=" + ",".join(observed_fields)
    )
    if dedicated_notes and denominator_status == "canonical_aggregate_futures_iss_fallback":
        note = note + "; dedicated_calendar_failed=" + "; ".join(dedicated_notes)[:500]
    return out, note


_original_compute_one_metrics = base.compute_one_metrics


def compute_one_metrics(*args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    liquidity, history = _original_compute_one_metrics(*args, **kwargs)
    calendar_note = str(liquidity.get("calendar_note") or history.get("calendar_note") or "")
    status = "unresolved"
    prefix = "calendar_denominator_status="
    if prefix in calendar_note:
        status = calendar_note.split(prefix, 1)[1].split(";", 1)[0].strip() or "unresolved"
    for row in [liquidity, history]:
        row["calendar_denominator_status"] = status
    return liquidity, history


def summarize_screen(df: pd.DataFrame, status_col: str) -> Dict[str, Any]:
    counts = df[status_col].astype(str).value_counts(dropna=False).to_dict() if status_col in df.columns else {}
    instruments = []
    cols = [x for x in [
        "secid",
        "family_code",
        status_col,
        "first_available_date",
        "last_available_date",
        "available_trading_days",
        "expected_trading_days",
        "active_days",
        "median_daily_volume",
        "median_daily_value",
        "median_daily_trades",
        "coverage_ratio",
        "recent_gap_count",
        "calendar_denominator_status",
        "review_notes",
    ] if x in df.columns]
    for _, row in df[cols].iterrows():
        item = {str(k): (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        instruments.append(item)
    denominator_counts = {}
    if "calendar_denominator_status" in df.columns:
        denominator_counts = {str(k): int(v) for k, v in df["calendar_denominator_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    return {
        "rows": int(len(df)),
        "status_counts": {str(k): int(v) for k, v in counts.items()},
        "calendar_denominator_status_counts": denominator_counts,
        "instruments": instruments,
    }


base.fetch_futures_calendar = fetch_futures_calendar
base.compute_one_metrics = compute_one_metrics
base.summarize_screen = summarize_screen
main = base.main


if __name__ == "__main__":
    raise SystemExit(main())
