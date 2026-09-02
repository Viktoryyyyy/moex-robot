from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Final
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from moex_data.futures import front_next_binding


SCHEMA_VERSION: Final[str] = "synchronized_live_market_oi_context.v1"
FORTS_SOURCE_ID: Final[str] = "moex_iss_forts_rfud_live_marketdata"
CETS_SOURCE_ID: Final[str] = "moex_iss_cets_cnyrub_tom_live_marketdata"
DEFAULT_BASE_URL: Final[str] = "https://iss.moex.com"
FORTS_ENDPOINT: Final[str] = "/iss/engines/futures/markets/forts/boards/RFUD/securities.json"
CETS_ENDPOINT: Final[str] = "/iss/engines/currency/markets/selt/boards/CETS/securities/CNYRUB_TOM.json"
MAX_SKEW_SECONDS: Final[int] = 60
MAX_FRESHNESS_SECONDS: Final[int] = 60
MOSCOW: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")

FUTURES_MARKETDATA_COLUMNS: Final[tuple[str, ...]] = (
    "SECID",
    "OPEN",
    "HIGH",
    "LOW",
    "LAST",
    "WAPRICE",
    "VOLTODAY",
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


def _table_frame(payload: Mapping[str, object], block_name: str) -> pd.DataFrame:
    block = payload.get(block_name)
    if not isinstance(block, Mapping):
        raise SynchronizedLiveMarketOIError(f"MOEX ISS response missing {block_name} block")
    columns = block.get("columns")
    rows = block.get("data")
    if not isinstance(columns, list) or not all(isinstance(column, str) for column in columns):
        raise SynchronizedLiveMarketOIError(f"{block_name} columns are invalid")
    if not isinstance(rows, list):
        raise SynchronizedLiveMarketOIError(f"{block_name} data is invalid")
    frame = pd.DataFrame(rows, columns=columns)
    if frame.empty:
        raise SynchronizedLiveMarketOIError(f"{block_name} block is empty")
    return frame


def _require_columns(frame: pd.DataFrame, required: Sequence[str], block_name: str) -> None:
    available = {str(column).upper() for column in frame.columns}
    missing = [name for name in required if name.upper() not in available]
    if missing:
        raise SynchronizedLiveMarketOIError(
            f"{block_name} is missing required columns: {','.join(missing)}"
        )


def _row_by_secid(frame: pd.DataFrame, secid: str) -> Mapping[str, object]:
    by_upper = {str(column).upper(): column for column in frame.columns}
    secid_column = by_upper.get("SECID")
    if secid_column is None:
        raise SynchronizedLiveMarketOIError("marketdata SECID column is missing")
    rows = frame.loc[frame[secid_column].astype(str).str.upper().eq(secid.upper())]
    if len(rows.index) != 1:
        raise SynchronizedLiveMarketOIError(
            f"marketdata must contain exactly one row for {secid}; found {len(rows.index)}"
        )
    return rows.iloc[0].to_dict()


def _number(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise SynchronizedLiveMarketOIError(f"marketdata numeric value is invalid: {value!r}") from exc


def _integer(value: object) -> int | None:
    numeric = _number(value)
    if numeric is None:
        return None
    if not float(numeric).is_integer():
        raise SynchronizedLiveMarketOIError(f"marketdata integer value is invalid: {value!r}")
    return int(numeric)


def _normalize_row(
    *,
    logical_id: str,
    secid: str,
    row: Mapping[str, object],
    source_id: str,
    received_at_utc: datetime,
    is_future: bool,
) -> dict[str, object]:
    event_time = _source_event_time(row.get("SYSTIME"), f"{secid}.SYSTIME")
    age_seconds = max(0.0, (received_at_utc - event_time).total_seconds())
    bid = _number(row.get("BID"))
    ask = _number(row.get("OFFER"))
    spread = ask - bid if bid is not None and ask is not None else None
    oi = _integer(row.get("OPENPOSITION")) if is_future else None
    return {
        "logical_id": logical_id,
        "label": DISPLAY_LABELS[logical_id],
        "secid": secid,
        "asset_type": "future" if is_future else "spot",
        "last": _number(row.get("LAST")),
        "open": _number(row.get("OPEN")),
        "high": _number(row.get("HIGH")),
        "low": _number(row.get("LOW")),
        "wap": _number(row.get("WAPRICE")),
        "volume": _number(row.get("VOLTODAY")),
        "trades": _integer(row.get("NUMTRADES")),
        "oi": oi,
        "oi_status": "available" if is_future and oi is not None else (
            "missing" if is_future else "not_applicable"
        ),
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "timestamp": _iso(event_time),
        "received_at_utc": _iso(received_at_utc),
        "age_seconds": round(age_seconds, 3),
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

    securities = _table_frame(forts_payload, "securities")
    forts_marketdata = _table_frame(forts_payload, "marketdata")
    cets_marketdata = _table_frame(cets_payload, "marketdata")
    _require_columns(securities, ("SECID", "BOARDID", "LASTTRADEDATE"), "securities")
    _require_columns(forts_marketdata, FUTURES_MARKETDATA_COLUMNS, "FORTS marketdata")
    _require_columns(cets_marketdata, CETS_MARKETDATA_COLUMNS, "CETS marketdata")

    bindings = _bindings_from_forts(
        securities,
        as_of_date=as_of_date,
        availability_ts_utc=_iso(forts_received),
    )

    instruments: dict[str, dict[str, object]] = {}
    for logical_id in FUTURES_LOGICAL_ORDER:
        secid = bindings[logical_id]
        instruments[logical_id] = _normalize_row(
            logical_id=logical_id,
            secid=secid,
            row=_row_by_secid(forts_marketdata, secid),
            source_id=FORTS_SOURCE_ID,
            received_at_utc=forts_received,
            is_future=True,
        )
    instruments["cnyrub_tom"] = _normalize_row(
        logical_id="cnyrub_tom",
        secid="CNYRUB_TOM",
        row=_row_by_secid(cets_marketdata, "CNYRUB_TOM"),
        source_id=CETS_SOURCE_ID,
        received_at_utc=cets_received,
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
        usable = bool(
            synchronized
            and item["last"] is not None
            and item["oi"] is not None
            and item["stale"] is False
            and item["price_oi_same_source_row"] is True
        )
        item["price_oi_usable"] = usable
        futures_price_oi_usable[logical_id] = usable

    spot_price_usable = bool(
        synchronized
        and instruments["cnyrub_tom"]["last"] is not None
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
            "max_skew_seconds": round(max_skew_seconds, 3),
            "max_skew_threshold_seconds": MAX_SKEW_SECONDS,
            "freshness_threshold_seconds": MAX_FRESHNESS_SECONDS,
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
            },
            "cnyrub_tom": {
                "source_id": CETS_SOURCE_ID,
                "source_url": cets_source_url,
                "received_at_utc": _iso(cets_received),
                "oi_not_applicable": True,
            },
        },
    }


def _fetch_json(
    *,
    url: str,
    params: Mapping[str, object],
    timeout: float,
    http_get: HTTPGet,
    now_fn: NowFn,
) -> tuple[dict[str, object], str, datetime]:
    response = http_get(
        url,
        params=dict(params),
        timeout=timeout,
        headers={"User-Agent": "moex_bot_synchronized_live_market_oi_context/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise SynchronizedLiveMarketOIError("MOEX ISS response root must be an object")
    received_at = _aware_utc(now_fn(), "now_fn")
    return payload, str(getattr(response, "url", url)), received_at


def fetch_live_snapshot(
    *,
    timeout: float = 12.0,
    base_url: str | None = None,
    http_get: HTTPGet = requests.get,
    now_fn: NowFn = lambda: datetime.now(timezone.utc),
) -> dict[str, object]:
    base = str(base_url or os.environ.get("MOEX_ISS_URL", DEFAULT_BASE_URL)).strip().rstrip("/")
    if not base:
        raise SynchronizedLiveMarketOIError("MOEX ISS base URL is required")
    forts_url = base + FORTS_ENDPOINT
    cets_url = base + CETS_ENDPOINT
    forts_params = {
        "iss.meta": "off",
        "iss.only": "securities,marketdata",
        "securities.columns": "SECID,BOARDID,LASTTRADEDATE",
        "marketdata.columns": ",".join(FUTURES_MARKETDATA_COLUMNS),
    }
    cets_params = {
        "iss.meta": "off",
        "iss.only": "marketdata",
        "marketdata.columns": ",".join(CETS_MARKETDATA_COLUMNS),
    }
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="moex-live-snapshot") as executor:
        forts_future = executor.submit(
            _fetch_json,
            url=forts_url,
            params=forts_params,
            timeout=timeout,
            http_get=http_get,
            now_fn=now_fn,
        )
        cets_future = executor.submit(
            _fetch_json,
            url=cets_url,
            params=cets_params,
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
    parser.add_argument("--iss-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = fetch_live_snapshot(timeout=args.timeout, base_url=args.iss_base_url)
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
