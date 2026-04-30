#!/usr/bin/env python3
import json
import os
import sys
import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional, Set, Tuple

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


def fetch_futures_calendar(screen_from: str, screen_till: str, timeout: float, unused_iss_base_url: str) -> Tuple[Optional[Set[str]], str]:
    out: Set[str] = set()
    required = ["tradedate", "futures_workday", "futures_trade_session_date", "futures_reason"]
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
                canonical_session_date = base.parse_iso_date(_xml_attr(row, "futures_trade_session_date"))
                if not canonical_session_date:
                    canonical_session_date = base.parse_iso_date(_xml_attr(row, "tradedate"))
                if canonical_session_date:
                    out.add(canonical_session_date)
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


base.fetch_futures_calendar = fetch_futures_calendar
base.compute_one_metrics = compute_one_metrics


if __name__ == "__main__":
    try:
        raise SystemExit(base.main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
