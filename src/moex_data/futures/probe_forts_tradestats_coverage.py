from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Final, Sequence

import requests

from . import materialize_raw_5m as core

DEFAULT_START_DATE: Final[str] = "2020-01-01"
DEFAULT_END_DATE: Final[str] = "2026-08-17"
MATERIALIZATION_ENDPOINT: Final[str] = core.SOURCE_ENDPOINT_APIM_FO_TRADESTATS
AVAILABILITY_PROBE_ENDPOINT_PATTERN: Final[str] = "/iss/datashop/algopack/fo/tradestats/{SECID}.json"
MAX_WINDOW_PAGES: Final[int] = 100


class TradestatsCoverageError(ValueError):
    pass


def _require_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise TradestatsCoverageError(field_name + " must be YYYY-MM-DD") from exc


def _require_token(value: str, field_name: str) -> str:
    text = str(value or "").strip()
    if not text or any(marker in text for marker in ("/", "\\", "*", "{", "}", "`", "$(")):
        raise TradestatsCoverageError(field_name + " must be an explicit safe token")
    return text


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists():
        raise TradestatsCoverageError("env_file does not exist")
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _base_url(explicit: str | None) -> str:
    value = str(explicit or os.environ.get("MOEX_API_URL", core.DEFAULT_APIM_BASE_URL)).strip().rstrip("/")
    if not value:
        raise TradestatsCoverageError("MOEX_API_URL is required")
    return value


def _headers() -> dict[str, str]:
    token = str(os.environ.get("MOEX_API_KEY", "")).strip()
    if not token:
        raise TradestatsCoverageError("MOEX_API_KEY is required")
    return {
        "Authorization": "Bearer " + token,
        "User-Agent": str(os.environ.get("MOEX_UA", "moex-robot-stage2-tradestats-coverage/1.0")).strip()
        or "moex-robot-stage2-tradestats-coverage/1.0",
    }


def _rows(payload: object) -> tuple[list[str], list[list[object]]]:
    if not isinstance(payload, dict):
        raise TradestatsCoverageError("tradestats response root is not an object")
    frame = core._block_to_frame(payload)
    columns = [str(column) for column in frame.columns]
    if not columns:
        raise TradestatsCoverageError("tradestats compatible data block is missing")
    lowered = [column.strip().lower() for column in columns]
    if "error_message" in lowered:
        raise TradestatsCoverageError("tradestats returned ERROR_MESSAGE payload")
    return columns, frame.values.tolist()


def _probe_endpoint(secid: str) -> str:
    return AVAILABILITY_PROBE_ENDPOINT_PATTERN.replace("{SECID}", secid)


def _request(
    session: requests.Session,
    base_url: str,
    endpoint: str,
    params: dict[str, object],
    timeout: float,
) -> tuple[list[str], list[list[object]]]:
    url = base_url + "/" + endpoint.lstrip("/")
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return _rows(response.json())


def _identity_indexes(columns: list[str]) -> tuple[int, int]:
    lowered = [str(column).strip().lower() for column in columns]
    if "secid" not in lowered:
        raise TradestatsCoverageError("tradestats response missing SECID identity field")
    date_name = "tradedate" if "tradedate" in lowered else ("date" if "date" in lowered else None)
    if date_name is None:
        raise TradestatsCoverageError("tradestats response missing TRADEDATE/DATE field")
    return lowered.index("secid"), lowered.index(date_name)


def _contains_requested_identity(
    columns: list[str],
    rows: list[list[object]],
    secid: str,
    lower: date,
    upper: date,
) -> bool:
    if not rows:
        return False
    secid_index, date_index = _identity_indexes(columns)
    expected = secid.upper()
    for row in rows:
        if len(row) <= max(secid_index, date_index):
            raise TradestatsCoverageError("tradestats row is shorter than declared columns")
        if str(row[secid_index]).strip().upper() != expected:
            continue
        parsed = core._parse_trade_date(row[date_index])
        if not parsed:
            continue
        try:
            observed = date.fromisoformat(parsed)
        except ValueError:
            continue
        if lower <= observed <= upper:
            return True
    return False


def _exact_has_data(
    session: requests.Session,
    base_url: str,
    secid: str,
    trade_date: date,
    timeout: float,
) -> bool:
    day = trade_date.isoformat()
    columns, rows = _request(
        session,
        base_url,
        _probe_endpoint(secid),
        {
            "date": day,
            "from": day,
            "till": day,
            "secid": secid,
            "start": 0,
            "iss.meta": "off",
            "iss.only": "tradestats",
        },
        timeout,
    )
    return _contains_requested_identity(columns, rows, secid, trade_date, trade_date)


def _month_bounds(value: date) -> tuple[date, date]:
    first = value.replace(day=1)
    if first.month == 12:
        following = date(first.year + 1, 1, 1)
    else:
        following = date(first.year, first.month + 1, 1)
    return first, following - timedelta(days=1)


def _month_has_data(
    session: requests.Session,
    base_url: str,
    secid: str,
    month: date,
    lower: date,
    upper: date,
    timeout: float,
) -> bool:
    first, last = _month_bounds(month)
    first = max(first, lower)
    last = min(last, upper)
    if first > last:
        return False
    endpoint = _probe_endpoint(secid)
    start = 0
    seen_signatures: set[tuple[object, ...]] = set()
    for _ in range(MAX_WINDOW_PAGES):
        columns, rows = _request(
            session,
            base_url,
            endpoint,
            {
                "from": first.isoformat(),
                "till": last.isoformat(),
                "secid": secid,
                "start": start,
                "iss.meta": "off",
                "iss.only": "tradestats",
            },
            timeout,
        )
        if not rows:
            return False
        if _contains_requested_identity(columns, rows, secid, first, last):
            return True
        signature = (len(rows), tuple(str(value) for value in rows[0]), tuple(str(value) for value in rows[-1]))
        if signature in seen_signatures:
            raise TradestatsCoverageError("availability probe pagination did not advance")
        seen_signatures.add(signature)
        start += len(rows)
    raise TradestatsCoverageError("availability probe pagination exceeded max_pages guard")


def _months(start: date, end: date) -> list[date]:
    current = start.replace(day=1)
    last = end.replace(day=1)
    result: list[date] = []
    while current <= last:
        result.append(current)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return result


def probe_coverage(
    secid: str,
    start_date: str,
    end_date: str,
    timeout: float = 30.0,
    apim_base_url: str | None = None,
) -> dict[str, object]:
    checked_secid = _require_token(secid, "secid")
    start = _require_date(start_date, "start_date")
    end = _require_date(end_date, "end_date")
    if start > end:
        raise TradestatsCoverageError("start_date must be <= end_date")
    base_url = _base_url(apim_base_url)
    session = requests.Session()
    session.headers.update(_headers())

    first_available: date | None = None
    for month in _months(start, end):
        if not _month_has_data(session, base_url, checked_secid, month, start, end, timeout):
            continue
        first, last = _month_bounds(month)
        current = max(first, start)
        last = min(last, end)
        while current <= last:
            if _exact_has_data(session, base_url, checked_secid, current, timeout):
                first_available = current
                break
            current += timedelta(days=1)
        if first_available is not None:
            break

    if first_available is None:
        raise TradestatsCoverageError("requested SECID was not observed in the requested coverage interval")

    last_available: date | None = None
    current = end
    while current >= first_available:
        if _exact_has_data(session, base_url, checked_secid, current, timeout):
            last_available = current
            break
        current -= timedelta(days=1)
    if last_available is None:
        raise TradestatsCoverageError("requested SECID has no observed last date after first availability")

    return {
        "source_id": "moex_algopack_fo_tradestats_5m",
        "secid": checked_secid,
        "requested_from": start.isoformat(),
        "requested_till": end.isoformat(),
        "first_available": first_available.isoformat(),
        "last_available": last_available.isoformat(),
        "latest_autodetect_used": False,
        "transport": "authenticated_apim",
        "coverage_endpoint_path": _probe_endpoint(checked_secid),
        "materialization_endpoint_path": MATERIALIZATION_ENDPOINT,
        "identity_filter": "SECID+TRADEDATE",
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe FORTS tradestats coverage using the source-contract APIM availability endpoint and explicit SECID/date identity validation.")
    parser.add_argument("--secid", action="append", required=True)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--apim-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        for secid in args.secid:
            print(json.dumps(probe_coverage(secid, args.start_date, args.end_date, args.timeout, args.apim_base_url), ensure_ascii=False, sort_keys=True))
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
