from __future__ import annotations

import argparse
import json
import math
import os
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Final
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from moex_data.futures import front_next_binding


SCHEMA_VERSION: Final[str] = "synchronized_live_market_oi_context.v1"
FORTS_SOURCE_ID: Final[str] = "moex_apim_forts_rfud_live_marketdata"
CETS_SOURCE_ID: Final[str] = "moex_apim_cets_cnyrub_tom_live_marketdata"
DEFAULT_BASE_URL: Final[str] = "https://apim.moex.com"
API_URL_ENV: Final[str] = "MOEX_API_URL"
API_KEY_ENV: Final[str] = "MOEX_API_KEY"
FORTS_ENDPOINT: Final[str] = "/iss/engines/futures/markets/forts/boards/RFUD/securities.json"
CETS_ENDPOINT: Final[str] = "/iss/engines/currency/markets/selt/boards/CETS/securities/CNYRUB_TOM.json"
MAX_SKEW_SECONDS: Final[int] = 60
MAX_FRESHNESS_SECONDS: Final[int] = 60
MAX_FUTURE_CLOCK_SKEW_SECONDS: Final[int] = 5
MAX_FORTS_PAGES: Final[int] = 100
FORTS_ROW_RECEIPTS_KEY: Final[str] = "_marketdata_received_at_utc_by_secid"
MOSCOW: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")

FUTURES_SECURITY_COLUMNS: Final[tuple[str, ...]] = (
    "SECID",
    "BOARDID",
    "LASTTRADEDATE",
    "MINSTEP",
    "STEPPRICE",
)
FUTURES_MARKETDATA_COLUMNS: Final[tuple[str, ...]] = (
    "SECID",
    "OPEN",
    "HIGH",
    "LOW",
    "LAST",
    "VOLTODAY",
    "VALTODAY",
    "NUMTRADES",
    "OPENPOSITION",
    "BID",
    "OFFER",
    "SYSTIME",
)
CETS_MARKETDATA_COLUMNS: Final[tuple[str, ...]] = (
    "SECID",
    "OPEN",
    "HIGH",
    "LOW",
    "LAST",
    "WAPRICE",
    "VOLTODAY",
    "NUMTRADES",
    "BID",
    "OFFER",
    "SYSTIME",
)
FUTURES_LOGICAL_ORDER: Final[tuple[str, ...]] = (
    "usdrubf",
    "si_front",
    "si_next",
    "cnyrubf",
    "cr_front",
    "cr_next",
)
LOGICAL_ORDER: Final[tuple[str, ...]] = FUTURES_LOGICAL_ORDER + ("cnyrub_tom",)
STATIC_FUTURES_SECIDS: Final[dict[str, str]] = {
    "usdrubf": "USDRUBF",
    "cnyrubf": "CNYRUBF",
}
DISPLAY_LABELS: Final[dict[str, str]] = {
    "usdrubf": "USDRUBF",
    "si_front": "Si front",
    "si_next": "Si next",
    "cnyrubf": "CNYRUBF",
    "cr_front": "CR front",
    "cr_next": "CR next",
    "cnyrub_tom": "CNYRUB_TOM",
}


class SynchronizedLiveMarketOIError(RuntimeError):
    pass


HTTPGet = Callable[..., requests.Response]
NowFn = Callable[[], datetime]


def _aware_utc(value: datetime | str, field: str) -> datetime:
    try:
        parsed = (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            if isinstance(value, str)
            else value
        )
    except ValueError as exc:
        raise SynchronizedLiveMarketOIError(f"{field} must be an ISO timestamp") from exc
    if not isinstance(parsed, datetime) or parsed.tzinfo is None:
        raise SynchronizedLiveMarketOIError(f"{field} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _source_event_time(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise SynchronizedLiveMarketOIError(f"{field} is missing")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SynchronizedLiveMarketOIError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=MOSCOW)
    return parsed.astimezone(timezone.utc)


def _table_parts(
    payload: Mapping[str, object],
    block_name: str,
    *,
    allow_empty: bool,
) -> tuple[list[str], list[list[object]]]:
    block = payload.get(block_name)
    if not isinstance(block, Mapping):
        raise SynchronizedLiveMarketOIError(f"MOEX ISS response missing {block_name} block")
    columns = block.get("columns")
    rows = block.get("data")
    if not isinstance(columns, list) or not all(isinstance(column, str) for column in columns):
        raise SynchronizedLiveMarketOIError(f"{block_name} columns are invalid")
    if not isinstance(rows, list) or not all(isinstance(row, list) for row in rows):
        raise SynchronizedLiveMarketOIError(f"{block_name} data is invalid")
    if not allow_empty and not rows:
        raise SynchronizedLiveMarketOIError(f"{block_name} block is empty")
    return list(columns), [list(row) for row in rows]


def _table_frame(payload: Mapping[str, object], block_name: str) -> pd.DataFrame:
    columns, rows = _table_parts(payload, block_name, allow_empty=False)
    return pd.DataFrame(rows, columns=columns)


def _require_columns(frame: pd.DataFrame, required: Sequence[str], block_name: str) -> None:
    available = {str(column).upper() for column in frame.columns}
    missing = [name for name in required if name.upper() not in available]
    if missing:
        raise SynchronizedLiveMarketOIError(
            f"{block_name} is missing required columns: {','.join(missing)}"
        )


def _row_by_secid(frame: pd.DataFrame, secid: str, *, block_name: str) -> Mapping[str, object]:
    by_upper = {str(column).upper(): column for column in frame.columns}
    secid_column = by_upper.get("SECID")
    if secid_column is None:
        raise SynchronizedLiveMarketOIError(f"{block_name} SECID column is missing")
    rows = frame.loc[frame[secid_column].astype(str).str.upper().eq(secid.upper())]
    if len(rows.index) != 1:
        raise SynchronizedLiveMarketOIError(
            f"{block_name} must contain exactly one row for {secid}; found {len(rows.index)}"
        )
    return rows.iloc[0].to_dict()


def _number(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise SynchronizedLiveMarketOIError(f"marketdata numeric value is invalid: {value!r}") from exc
    if not math.isfinite(numeric):
        raise SynchronizedLiveMarketOIError(f"marketdata numeric value is non-finite: {value!r}")
    return numeric


def _integer(value: object) -> int | None:
    numeric = _number(value)
    if numeric is None:
        return None
    if not float(numeric).is_integer():
        raise SynchronizedLiveMarketOIError(f"marketdata integer value is invalid: {value!r}")
    return int(numeric)


def _forts_session_wap(
    *,
    secid: str,
    marketdata_row: Mapping[str, object],
    security_row: Mapping[str, object],
) -> float | None:
    volume = _number(marketdata_row.get("VOLTODAY"))
    value_rub = _number(marketdata_row.get("VALTODAY"))
    if volume is None or value_rub is None or volume <= 0:
        return None
    min_step = _number(security_row.get("MINSTEP"))
    step_price = _number(security_row.get("STEPPRICE"))
    if min_step is None or step_price is None or min_step <= 0 or step_price <= 0:
        raise SynchronizedLiveMarketOIError(f"{secid} MINSTEP/STEPPRICE is invalid")
    rub_per_quote_unit = step_price / min_step
    return value_rub / volume / rub_per_quote_unit


def _normalize_row(
    *,
    logical_id: str,
    secid: str,
    row: Mapping[str, object],
    source_id: str,
    received_at_utc: datetime,
    freshness_reference_utc: datetime,
    is_future: bool,
    security_row: Mapping[str, object] | None = None,
) -> dict[str, object]:
    event_time = _source_event_time(row.get("SYSTIME"), f"{secid}.SYSTIME")
    future_seconds = (event_time - freshness_reference_utc).total_seconds()
    if future_seconds > MAX_FUTURE_CLOCK_SKEW_SECONDS:
        raise SynchronizedLiveMarketOIError(
            f"{secid}.SYSTIME is {future_seconds:.3f}s ahead of snapshot completion; "
            f"allowed={MAX_FUTURE_CLOCK_SKEW_SECONDS}s"
        )
    age_seconds = max(0.0, (freshness_reference_utc - event_time).total_seconds())
    bid = _number(row.get("BID"))
    ask = _number(row.get("OFFER"))
    spread = ask - bid if bid is not None and ask is not None else None
    volume = _number(row.get("VOLTODAY"))
    trades = _integer(row.get("NUMTRADES"))
    if volume is not None and volume < 0:
        raise SynchronizedLiveMarketOIError(f"{secid}.VOLTODAY must be nonnegative")
    if trades is not None and trades < 0:
        raise SynchronizedLiveMarketOIError(f"{secid}.NUMTRADES must be nonnegative")
    oi = _integer(row.get("OPENPOSITION")) if is_future else None
    if is_future and oi is not None and oi < 0:
        raise SynchronizedLiveMarketOIError(f"{secid}.OPENPOSITION must be nonnegative")
    if is_future:
        if security_row is None:
            raise SynchronizedLiveMarketOIError(f"{secid} security row is required")
        wap = _forts_session_wap(
            secid=secid,
            marketdata_row=row,
            security_row=security_row,
        )
        wap_method = "VALTODAY/VOLTODAY/(STEPPRICE/MINSTEP)"
    else:
        wap = _number(row.get("WAPRICE"))
        wap_method = "WAPRICE"
    return {
        "logical_id": logical_id,
        "label": DISPLAY_LABELS[logical_id],
        "secid": secid,
        "asset_type": "future" if is_future else "spot",
        "last": _number(row.get("LAST")),
        "open": _number(row.get("OPEN")),
        "high": _number(row.get("HIGH")),
        "low": _number(row.get("LOW")),
        "wap": wap,
        "wap_method": wap_method,
        "volume": volume,
        "trades": trades,
        "oi": oi,
        "oi_status": "available" if is_future and oi is not None else (
            "missing" if is_future else "not_applicable"
        ),
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "timestamp": _iso(event_time),
        "received_at_utc": _iso(received_at_utc),
        "freshness_reference_utc": _iso(freshness_reference_utc),
        "age_seconds": round(age_seconds, 3),
        "future_clock_skew_seconds": round(max(0.0, future_seconds), 3),
        "stale": age_seconds > MAX_FRESHNESS_SECONDS,
        "source_id": source_id,
        "price_oi_same_source_row": bool(is_future),
        "price_oi_source_field": "OPENPOSITION" if is_future else None,
        "price_oi_usable": False,
    }


def _bindings_from_forts(
    securities: pd.DataFrame,
    *,
    as_of_date: str,
    availability_ts_utc: str,
) -> dict[str, str]:
    bindings: dict[str, str] = dict(STATIC_FUTURES_SECIDS)
    for root, prefix in (("Si", "si"), ("CR", "cr")):
        selected = front_next_binding.bind_front_next(
            securities,
            root=root,
            as_of_date=as_of_date,
            availability_ts_utc=availability_ts_utc,
        )
        by_role = {str(item["role"]): str(item["secid"]) for item in selected}
        if set(by_role) != {"front", "next"}:
            raise SynchronizedLiveMarketOIError(f"{root} front/next binding is incomplete")
        bindings[f"{prefix}_front"] = by_role["front"]
        bindings[f"{prefix}_next"] = by_role["next"]
    return bindings


def _forts_row_receipts(
    payload: Mapping[str, object],
    *,
    completion_utc: datetime,
) -> dict[str, datetime] | None:
    raw = payload.get(FORTS_ROW_RECEIPTS_KEY)
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise SynchronizedLiveMarketOIError("FORTS row receipt map is invalid")
    receipts: dict[str, datetime] = {}
    for raw_secid, raw_received in raw.items():
        secid = str(raw_secid).strip().upper()
        if not secid:
            raise SynchronizedLiveMarketOIError("FORTS row receipt map contains empty SECID")
        received = _aware_utc(raw_received, f"FORTS row receipt {secid}")
        if received > completion_utc:
            raise SynchronizedLiveMarketOIError(
                f"FORTS row receipt {secid} is later than pagination completion"
            )
        receipts[secid] = received
    return receipts


def build_snapshot_from_payloads(
    *,
    forts_payload: Mapping[str, object],
    cets_payload: Mapping[str, object],
    forts_received_at_utc: datetime | str,
    cets_received_at_utc: datetime | str,
    forts_source_url: str = FORTS_ENDPOINT,
    cets_source_url: str = CETS_ENDPOINT,
) -> dict[str, object]:
    forts_received = _aware_utc(forts_received_at_utc, "forts_received_at_utc")
    cets_received = _aware_utc(cets_received_at_utc, "cets_received_at_utc")
    observed_at = max(forts_received, cets_received)
    as_of_date = observed_at.astimezone(MOSCOW).date().isoformat()
    forts_row_receipts = _forts_row_receipts(
        forts_payload,
        completion_utc=forts_received,
    )

    securities = _table_frame(forts_payload, "securities")
    forts_marketdata = _table_frame(forts_payload, "marketdata")
    cets_marketdata = _table_frame(cets_payload, "marketdata")
    _require_columns(securities, FUTURES_SECURITY_COLUMNS, "securities")
    _require_columns(forts_marketdata, FUTURES_MARKETDATA_COLUMNS, "FORTS marketdata")
    _require_columns(cets_marketdata, CETS_MARKETDATA_COLUMNS, "CETS marketdata")

    bindings = _bindings_from_forts(
        securities,
        as_of_date=as_of_date,
        availability_ts_utc=_iso(observed_at),
    )

    instruments: dict[str, dict[str, object]] = {}
    for logical_id in FUTURES_LOGICAL_ORDER:
        secid = bindings[logical_id]
        if forts_row_receipts is None:
            row_received = forts_received
        else:
            row_received = forts_row_receipts.get(secid.upper())
            if row_received is None:
                raise SynchronizedLiveMarketOIError(
                    f"FORTS row receipt is missing for selected {secid}"
                )
        instruments[logical_id] = _normalize_row(
            logical_id=logical_id,
            secid=secid,
            row=_row_by_secid(forts_marketdata, secid, block_name="FORTS marketdata"),
            security_row=_row_by_secid(securities, secid, block_name="FORTS securities"),
            source_id=FORTS_SOURCE_ID,
            received_at_utc=row_received,
            freshness_reference_utc=observed_at,
            is_future=True,
        )
    instruments["cnyrub_tom"] = _normalize_row(
        logical_id="cnyrub_tom",
        secid="CNYRUB_TOM",
        row=_row_by_secid(cets_marketdata, "CNYRUB_TOM", block_name="CETS marketdata"),
        source_id=CETS_SOURCE_ID,
        received_at_utc=cets_received,
        freshness_reference_utc=observed_at,
        is_future=False,
    )

    timestamps = [
        _aware_utc(item["timestamp"], f"{key}.timestamp")
        for key, item in instruments.items()
    ]
    oldest = min(timestamps)
    newest = max(timestamps)
    max_skew_seconds = (newest - oldest).total_seconds()
    all_fresh = all(item["stale"] is False for item in instruments.values())
    synchronized = max_skew_seconds <= MAX_SKEW_SECONDS and all_fresh

    futures_price_oi_usable: dict[str, bool] = {}
    for logical_id in FUTURES_LOGICAL_ORDER:
        item = instruments[logical_id]
        last = item["last"]
        usable = bool(
            synchronized
            and isinstance(last, (int, float))
            and last > 0
            and item["oi"] is not None
            and item["stale"] is False
            and item["price_oi_same_source_row"] is True
        )
        item["price_oi_usable"] = usable
        futures_price_oi_usable[logical_id] = usable

    spot_last = instruments["cnyrub_tom"]["last"]
    spot_price_usable = bool(
        synchronized
        and isinstance(spot_last, (int, float))
        and spot_last > 0
        and instruments["cnyrub_tom"]["stale"] is False
    )
    analysis_usable = bool(
        synchronized
        and all(futures_price_oi_usable.values())
        and spot_price_usable
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "READY" if analysis_usable else "UNAVAILABLE",
        "snapshot_received_at_utc": _iso(observed_at),
        "synchronization": {
            "status": "PASS" if synchronized else "FAIL",
            "synchronized": synchronized,
            "as_of_utc": _iso(newest),
            "oldest_timestamp_utc": _iso(oldest),
            "freshness_reference_utc": _iso(observed_at),
            "max_skew_seconds": round(max_skew_seconds, 3),
            "max_skew_threshold_seconds": MAX_SKEW_SECONDS,
            "freshness_threshold_seconds": MAX_FRESHNESS_SECONDS,
            "future_clock_skew_threshold_seconds": MAX_FUTURE_CLOCK_SKEW_SECONDS,
            "all_instruments_fresh": all_fresh,
        },
        "bindings": {
            logical_id: instruments[logical_id]["secid"]
            for logical_id in LOGICAL_ORDER
        },
        "instruments": {
            logical_id: instruments[logical_id]
            for logical_id in LOGICAL_ORDER
        },
        "quality": {
            "status": "PASS" if analysis_usable else "FAIL",
            "analysis_usable": analysis_usable,
            "price_oi_all_futures_usable": all(futures_price_oi_usable.values()),
            "price_oi_usable_by_instrument": futures_price_oi_usable,
            "spot_price_usable": spot_price_usable,
            "fail_closed": True,
        },
        "provenance": {
            "forts": {
                "source_id": FORTS_SOURCE_ID,
                "source_url": forts_source_url,
                "received_at_utc": _iso(forts_received),
                "price_and_oi_same_marketdata_row": True,
                "wap_method": "VALTODAY/VOLTODAY/(STEPPRICE/MINSTEP)",
                "authenticated_gateway": True,
                "pagination_complete": True,
                "row_receipt_times_preserved": forts_row_receipts is not None,
            },
            "cnyrub_tom": {
                "source_id": CETS_SOURCE_ID,
                "source_url": cets_source_url,
                "received_at_utc": _iso(cets_received),
                "oi_not_applicable": True,
                "wap_method": "WAPRICE",
                "authenticated_gateway": True,
            },
        },
    }


def _api_base_url(base_url: str | None, env: Mapping[str, str]) -> str:
    value = str(base_url or env.get(API_URL_ENV, DEFAULT_BASE_URL)).strip().rstrip("/")
    if not value:
        raise SynchronizedLiveMarketOIError(f"{API_URL_ENV} is required")
    return value


def _auth_headers(env: Mapping[str, str]) -> dict[str, str]:
    token = str(env.get(API_KEY_ENV, "")).strip()
    if not token:
        raise SynchronizedLiveMarketOIError(
            f"{API_KEY_ENV} is required for canonical authenticated MOEX live source"
        )
    return {
        "User-Agent": "moex_bot_synchronized_live_market_oi_context/1.0",
        "Authorization": "Bearer " + token,
    }


def _validated_response_url(requested_url: str, response_url: str) -> str:
    requested = urlsplit(requested_url)
    received = urlsplit(response_url)
    requested_route = (requested.scheme.lower(), requested.netloc.lower(), requested.path)
    received_route = (received.scheme.lower(), received.netloc.lower(), received.path)
    if requested_route != received_route:
        raise SynchronizedLiveMarketOIError(
            "MOEX authenticated source redirected or changed route: "
            f"requested={requested_url} received={response_url}"
        )
    return response_url


def _fetch_json(
    *,
    url: str,
    params: Mapping[str, object],
    headers: Mapping[str, str],
    timeout: float,
    http_get: HTTPGet,
    now_fn: NowFn,
) -> tuple[dict[str, object], str, datetime]:
    response = http_get(
        url,
        params=dict(params),
        timeout=timeout,
        headers=dict(headers),
        allow_redirects=False,
    )
    try:
        status_code = int(getattr(response, "status_code", 200))
    except (TypeError, ValueError) as exc:
        raise SynchronizedLiveMarketOIError("MOEX API response status is invalid") from exc
    if 300 <= status_code < 400:
        raise SynchronizedLiveMarketOIError(
            f"MOEX authenticated source redirect rejected: HTTP {status_code}"
        )
    response.raise_for_status()
    response_url = _validated_response_url(url, str(getattr(response, "url", url)))
    payload = response.json()
    if not isinstance(payload, dict):
        raise SynchronizedLiveMarketOIError("MOEX API response root must be an object")
    received_at = _aware_utc(now_fn(), "now_fn")
    return payload, response_url, received_at


def _forts_cursor(payload: Mapping[str, object]) -> tuple[int, int, int]:
    block = payload.get("securities.cursor")
    if not isinstance(block, Mapping):
        raise SynchronizedLiveMarketOIError(
            "MOEX ISS response missing securities.cursor; RFUD pagination completeness is unproven"
        )
    columns = block.get("columns")
    rows = block.get("data")
    if not isinstance(columns, list) or not all(isinstance(column, str) for column in columns):
        raise SynchronizedLiveMarketOIError("securities.cursor columns are invalid")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], list):
        raise SynchronizedLiveMarketOIError("securities.cursor data is invalid")
    by_upper = {str(column).upper(): index for index, column in enumerate(columns)}
    missing = [name for name in ("INDEX", "TOTAL", "PAGESIZE") if name not in by_upper]
    if missing:
        raise SynchronizedLiveMarketOIError(
            f"securities.cursor is missing required columns: {','.join(missing)}"
        )
    row = rows[0]
    try:
        index = int(row[by_upper["INDEX"]])
        total = int(row[by_upper["TOTAL"]])
        page_size = int(row[by_upper["PAGESIZE"]])
    except (IndexError, TypeError, ValueError) as exc:
        raise SynchronizedLiveMarketOIError("securities.cursor values are invalid") from exc
    if index < 0 or total < 0 or page_size <= 0 or index > total:
        raise SynchronizedLiveMarketOIError("securities.cursor values are out of range")
    return index, total, page_size


def _merge_iss_block_by_secid(
    aggregate: dict[str, object],
    page: Mapping[str, object],
    block_name: str,
) -> None:
    columns, rows = _table_parts(page, block_name, allow_empty=True)
    if block_name not in aggregate:
        aggregate[block_name] = {"columns": columns, "data": []}
    target = aggregate[block_name]
    if not isinstance(target, dict):
        raise SynchronizedLiveMarketOIError(f"aggregated {block_name} block is invalid")
    target_columns = target.get("columns")
    target_rows = target.get("data")
    if target_columns != columns or not isinstance(target_rows, list):
        raise SynchronizedLiveMarketOIError(f"{block_name} columns changed across RFUD pages")
    by_upper = {str(column).upper(): index for index, column in enumerate(columns)}
    secid_index = by_upper.get("SECID")
    if secid_index is None:
        raise SynchronizedLiveMarketOIError(f"{block_name} SECID column is missing")

    positions: dict[str, int] = {}
    for position, existing in enumerate(target_rows):
        if not isinstance(existing, list) or secid_index >= len(existing):
            raise SynchronizedLiveMarketOIError(f"aggregated {block_name} row is invalid")
        positions[str(existing[secid_index]).upper()] = position

    for row in rows:
        if secid_index >= len(row):
            raise SynchronizedLiveMarketOIError(f"{block_name} row is invalid")
        secid = str(row[secid_index]).upper()
        if not secid:
            raise SynchronizedLiveMarketOIError(f"{block_name} SECID value is missing")
        if secid in positions:
            target_rows[positions[secid]] = row
        else:
            positions[secid] = len(target_rows)
            target_rows.append(row)


def _record_marketdata_receipts(
    page: Mapping[str, object],
    *,
    received_at_utc: datetime,
    target: dict[str, str],
) -> None:
    columns, rows = _table_parts(page, "marketdata", allow_empty=True)
    by_upper = {str(column).upper(): index for index, column in enumerate(columns)}
    secid_index = by_upper.get("SECID")
    if secid_index is None:
        raise SynchronizedLiveMarketOIError("marketdata SECID column is missing")
    for row in rows:
        if secid_index >= len(row):
            raise SynchronizedLiveMarketOIError("marketdata row is invalid")
        secid = str(row[secid_index]).strip().upper()
        if not secid:
            raise SynchronizedLiveMarketOIError("marketdata SECID value is missing")
        target[secid] = _iso(received_at_utc)


def _aggregate_row_count(aggregate: Mapping[str, object], block_name: str) -> int:
    block = aggregate.get(block_name)
    if not isinstance(block, Mapping):
        raise SynchronizedLiveMarketOIError(f"aggregated {block_name} block is missing")
    rows = block.get("data")
    if not isinstance(rows, list):
        raise SynchronizedLiveMarketOIError(f"aggregated {block_name} data is invalid")
    return len(rows)


def _fetch_forts_all_pages(
    *,
    url: str,
    params: Mapping[str, object],
    headers: Mapping[str, str],
    timeout: float,
    http_get: HTTPGet,
    now_fn: NowFn,
) -> tuple[dict[str, object], str, datetime]:
    aggregate: dict[str, object] = {}
    marketdata_receipts: dict[str, str] = {}
    page_params = dict(params)
    page_params.pop("start", None)
    expected_start = 0
    expected_total: int | None = None
    expected_page_size: int | None = None
    source_url = url
    received_at: datetime | None = None

    for _page_number in range(MAX_FORTS_PAGES):
        if expected_start:
            page_params["start"] = expected_start
        else:
            page_params.pop("start", None)
        page, page_source_url, page_received = _fetch_json(
            url=url,
            params=page_params,
            headers=headers,
            timeout=timeout,
            http_get=http_get,
            now_fn=now_fn,
        )
        index, total, page_size = _forts_cursor(page)
        if index != expected_start:
            raise SynchronizedLiveMarketOIError(
                f"RFUD pagination cursor mismatch: expected INDEX={expected_start}, got {index}"
            )
        if expected_total is None:
            expected_total = total
            expected_page_size = page_size
        elif total != expected_total:
            raise SynchronizedLiveMarketOIError(
                f"RFUD pagination TOTAL changed: expected {expected_total}, got {total}"
            )
        elif page_size != expected_page_size:
            raise SynchronizedLiveMarketOIError(
                f"RFUD pagination PAGESIZE changed: expected {expected_page_size}, got {page_size}"
            )

        _security_columns, security_rows = _table_parts(page, "securities", allow_empty=True)
        expected_rows = min(page_size, max(0, total - index))
        if len(security_rows) != expected_rows:
            raise SynchronizedLiveMarketOIError(
                f"RFUD securities page cardinality mismatch at INDEX={index}: "
                f"expected {expected_rows}, got {len(security_rows)}"
            )

        _merge_iss_block_by_secid(aggregate, page, "securities")
        _record_marketdata_receipts(
            page,
            received_at_utc=page_received,
            target=marketdata_receipts,
        )
        _merge_iss_block_by_secid(aggregate, page, "marketdata")
        source_url = page_source_url
        received_at = page_received

        next_start = index + page_size
        if next_start >= total:
            unique_securities = _aggregate_row_count(aggregate, "securities")
            if unique_securities != total:
                raise SynchronizedLiveMarketOIError(
                    f"RFUD pagination incomplete: TOTAL={total}, unique_securities={unique_securities}"
                )
            aggregate["securities.cursor"] = {
                "columns": ["INDEX", "TOTAL", "PAGESIZE"],
                "data": [[index, total, page_size]],
            }
            aggregate[FORTS_ROW_RECEIPTS_KEY] = dict(marketdata_receipts)
            if received_at is None:
                raise SynchronizedLiveMarketOIError("RFUD pagination completion timestamp is missing")
            return aggregate, source_url, received_at
        if next_start <= index:
            raise SynchronizedLiveMarketOIError("RFUD pagination did not advance")
        expected_start = next_start

    raise SynchronizedLiveMarketOIError(
        f"RFUD pagination exceeded safety limit of {MAX_FORTS_PAGES} pages"
    )


def fetch_live_snapshot(
    *,
    timeout: float = 12.0,
    base_url: str | None = None,
    http_get: HTTPGet = requests.get,
    now_fn: NowFn = lambda: datetime.now(timezone.utc),
    env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    active_env = os.environ if env is None else env
    base = _api_base_url(base_url, active_env)
    headers = _auth_headers(active_env)
    forts_url = base + FORTS_ENDPOINT
    cets_url = base + CETS_ENDPOINT
    forts_params = {
        "iss.meta": "off",
        "iss.only": "securities,marketdata,securities.cursor",
        "securities.columns": ",".join(FUTURES_SECURITY_COLUMNS),
        "marketdata.columns": ",".join(FUTURES_MARKETDATA_COLUMNS),
    }
    cets_params = {
        "iss.meta": "off",
        "iss.only": "marketdata",
        "marketdata.columns": ",".join(CETS_MARKETDATA_COLUMNS),
    }
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="moex-live-snapshot") as executor:
        forts_future = executor.submit(
            _fetch_forts_all_pages,
            url=forts_url,
            params=forts_params,
            headers=headers,
            timeout=timeout,
            http_get=http_get,
            now_fn=now_fn,
        )
        cets_future = executor.submit(
            _fetch_json,
            url=cets_url,
            params=cets_params,
            headers=headers,
            timeout=timeout,
            http_get=http_get,
            now_fn=now_fn,
        )
        forts_payload, forts_source_url, forts_received = forts_future.result()
        cets_payload, cets_source_url, cets_received = cets_future.result()

    return build_snapshot_from_payloads(
        forts_payload=forts_payload,
        cets_payload=cets_payload,
        forts_received_at_utc=forts_received,
        cets_received_at_utc=cets_received,
        forts_source_url=forts_source_url,
        cets_source_url=cets_source_url,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Return synchronized live USDRUBF/Si/CNYRUBF/CR/CNYRUB_TOM market+OI context."
    )
    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--api-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = fetch_live_snapshot(timeout=args.timeout, base_url=args.api_base_url)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "FAILED",
                    "error_class": exc.__class__.__name__,
                    "error": str(exc),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
