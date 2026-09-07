"""Narrow Brent acceptance: latest published official trading-results CLOSE.

This is not an intraday feed, a continuous series, or a historical PIT source.
Only three source requests are made; neither calendars nor backfill are used.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import re
import time
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .moex_brent_history import SOURCE_ID, select_nearest_contract


POLICY_ID = "oil_brent_factual_acceptance_v1"
PRICE_SEMANTICS = "latest_published_official_trading_results_close"
UNIT_TEXT = "в долларах США за 1 баррель"
MOSCOW = ZoneInfo("Europe/Moscow")
BASE = "https://iss.moex.com/iss"
MARKET = "/engines/futures/markets/forts"
MAX_RECEIPT_AGE_SECONDS = 1200
MAX_FUTURE_SKEW_SECONDS = 5
MAX_BODY_BYTES = 1_000_000
REQUEST_TIMEOUT_SECONDS = 2.0
COLLECTION_BUDGET_SECONDS = 6.0
FALSE_FLAGS = (
    "live_quote", "intraday_fresh", "historical_pit_eligible",
    "latest_completed_session_proven", "directional_authority",
    "action_authority", "standalone_buy_sell_authority",
    "stage5_full_mode_ready", "stage5_pointer_promotion_performed",
)
FACTUAL_FLAGS = ("factual_authority", "consumer_factual_use_allowed")


class BrentFactualError(ValueError):
    """No fallback or acceptance when a source/policy gate is unproven."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise BrentFactualError(message)


def _utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    _require(isinstance(parsed, datetime) and parsed.tzinfo is not None
             and parsed.utcoffset() is not None, "aware timestamp required")
    return parsed.astimezone(timezone.utc)


def _date(value: object) -> date:
    _require(isinstance(value, str), "source date must be an ISO string")
    parsed = date.fromisoformat(value)
    _require(parsed.isoformat() == value, "source date is not canonical ISO")
    return parsed


def _route(path: str, **params: object) -> str:
    return BASE + path + "?" + urlencode(params)


def _official_url(url: str) -> None:
    parts = urlsplit(url)
    _require(parts.scheme == "https" and parts.hostname == "iss.moex.com"
             and parts.port in (None, 443) and not parts.username
             and not parts.password and not parts.fragment
             and parts.path.startswith("/iss/"), "non-official source route")


def _fetch(url: str) -> bytes:
    _official_url(url)
    request = Request(url, headers={"Accept": "application/json",
                                   "User-Agent": "moex-robot-brent-factual/1"})
    with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        _require(response.geturl() == url, "source redirect refused")
        raw = response.read(MAX_BODY_BYTES + 1)
    _require(0 < len(raw) <= MAX_BODY_BYTES, "source body size invalid")
    return raw


def _rows(document: object, block_name: str) -> list[dict]:
    _require(isinstance(document, dict), "source root must be an object")
    block = document.get(block_name)
    _require(isinstance(block, dict), "missing source block: " + block_name)
    columns, rows = block.get("columns"), block.get("data")
    _require(isinstance(columns, list) and all(isinstance(c, str) for c in columns)
             and len(columns) == len(set(columns)) and isinstance(rows, list),
             "malformed source block: " + block_name)
    result = []
    for row in rows:
        _require(isinstance(row, list) and len(row) == len(columns),
                 "source row width mismatch")
        result.append(dict(zip(columns, row, strict=True)))
    return result


def _number(value: object, field: str) -> float:
    _require(type(value) in (int, float) and math.isfinite(value) and value > 0,
             "invalid positive finite price/size: " + field)
    return float(value)


def _ohlc(row: Mapping) -> dict[str, float]:
    result = {name.lower(): _number(row.get(name), name)
              for name in ("OPEN", "HIGH", "LOW", "CLOSE")}
    _require(result["low"] <= min(result["open"], result["close"])
             <= max(result["open"], result["close"]) <= result["high"],
             "inconsistent OHLC")
    return result


@dataclass(frozen=True)
class _Candidate:
    # The existing selector only reads expiration_date; do not invent full
    # historical BrentContract provenance for current universe metadata.
    secid: str
    expiration_date: date
    row: Mapping


def _select(document: object, evaluated: date) -> _Candidate:
    candidates = []
    for row in _rows(document, "securities"):
        if row.get("BOARDID") != "RFUD" or row.get("ASSETCODE") != "BR":
            continue
        secid = row.get("SECID")
        _require(isinstance(secid, str) and
                 re.fullmatch(r"[A-Z0-9_]{2,32}", secid) is not None
                 and secid != "BR", "invalid explicit contract identity")
        expiry = _date(row.get("LASTTRADEDATE"))
        candidates.append(_Candidate(secid, expiry, row))
    _require(bool(candidates) and len({c.secid for c in candidates}) == len(candidates),
             "empty or duplicate BR universe")
    # Reuse the existing seven-calendar-day rule, including tie rejection.
    return select_nearest_contract(candidates, target_trade_date=evaluated)


def load_factual_brent(
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    transport: Callable[[str], bytes] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict:
    """Collect one current-contract, latest-published result with receipt provenance."""
    started = monotonic()
    started_at = _utc(clock())
    evaluated = started_at.astimezone(MOSCOW).date()
    provenance = []
    fetch = _fetch if transport is None else transport

    def get(role: str, url: str) -> dict:
        _official_url(url)
        _require(monotonic() - started <= COLLECTION_BUDGET_SECONDS,
                 "Brent collection budget exceeded")
        requested = _utc(clock())
        raw = fetch(url)
        received = _utc(clock())
        _require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_BODY_BYTES,
                 "source body size invalid")
        _require(received >= requested >= started_at, "source receipt clock regressed")
        if provenance:
            _require(requested >= _utc(provenance[-1]["received_at"]),
                     "source request clock regressed")
        _require(monotonic() - started <= COLLECTION_BUDGET_SECONDS,
                 "Brent collection budget exceeded")
        document = json.loads(raw)
        _require(isinstance(document, dict), "source root must be an object")
        provenance.append({
            "role": role, "source_route": url,
            "requested_at": requested.isoformat(), "received_at": received.isoformat(),
            "raw_payload_sha256": hashlib.sha256(raw).hexdigest(),
        })
        return document

    universe = get("universe", _route(MARKET + "/securities.json",
                                      assetcode="BR", **{"iss.only": "securities"}))
    selected = _select(universe, evaluated)
    code = quote(selected.secid, safe="")
    metadata = get("identity", _route("/securities/" + code + ".json",
                                      **{"iss.only": "description,boards"}))
    description_rows = _rows(metadata, "description")
    _require(all(isinstance(r.get("name"), str) and r["name"] for r in description_rows),
             "invalid description names")
    description = {r["name"]: r.get("value") for r in description_rows}
    _require(len(description) == len(description_rows), "duplicate metadata field")
    _require(description.get("SECID") == selected.secid
             and description.get("ASSETCODE") == "BR"
             and description.get("GROUP") == "futures_forts"
             and description.get("TYPE") == "futures"
             and description.get("SHORTNAME") == selected.row.get("SHORTNAME"),
             "contract description identity mismatch")
    _require(description.get("UNIT") == UNIT_TEXT, "unproven Brent quote unit")
    first_trade = _date(description.get("FRSTTRADE"))
    expiry = _date(description.get("LSTTRADE"))
    delivery = _date(description.get("LSTDELDATE"))
    _require(first_trade <= evaluated <= expiry
             and expiry == selected.expiration_date
             and delivery == _date(selected.row.get("LASTDELDATE"))
             and delivery >= expiry, "contract lifecycle metadata mismatch")
    lot_text = description.get("LOTSIZE")
    _require(isinstance(lot_text, str) and re.fullmatch(r"[1-9][0-9]*", lot_text) is not None,
             "unproven contract size")
    contract_size = int(lot_text)
    _require(_number(selected.row.get("LOTVOLUME"), "LOTVOLUME") == contract_size,
             "contract size metadata mismatch")
    boards = [r for r in _rows(metadata, "boards")
              if r.get("secid") == selected.secid and r.get("boardid") == "RFUD"]
    _require(len(boards) == 1 and boards[0].get("engine") == "futures"
             and boards[0].get("market") == "forts", "RFUD board identity mismatch")
    published_date = _date(boards[0].get("history_till"))
    history_from = _date(boards[0].get("history_from"))
    _require(first_trade <= history_from <= published_date < evaluated,
             "published prior-date history bound not proven")

    history = get("history", _route(
        "/history" + MARKET + "/boards/RFUD/securities/" + code + ".json",
        **{"from": published_date.isoformat(), "till": published_date.isoformat(),
           "start": 0, "limit": 100, "iss.only": "history,history.cursor"}))
    rows, cursor = _rows(history, "history"), _rows(history, "history.cursor")
    _require(len(rows) == 1 and len(cursor) == 1
             and type(cursor[0].get("INDEX")) is int and cursor[0]["INDEX"] == 0
             and type(cursor[0].get("TOTAL")) is int and cursor[0]["TOTAL"] == 1
             and type(cursor[0].get("PAGESIZE")) is int and cursor[0]["PAGESIZE"] >= 1,
             "history empty, duplicate, or pagination incomplete")
    row = rows[0]
    _require(row.get("SECID") == selected.secid and row.get("BOARDID") == "RFUD"
             and row.get("ASSETCODE") == "BR"
             and _date(row.get("TRADEDATE")) == published_date,
             "history identity/date mismatch")
    ohlc = _ohlc(row)
    received = _utc(provenance[-1]["received_at"])
    _require(received.astimezone(MOSCOW).date() == evaluated,
             "selection date changed during collection")
    return {
        "source_id": SOURCE_ID, "acceptance_policy": POLICY_ID,
        "source_acceptance": "FACTUAL_ONLY",
        "mode": "LATEST_PUBLISHED_HISTORY",
        "secid": selected.secid, "board": "RFUD",
        "engine": "futures", "market": "forts", "asset_code": "BR",
        "selection_rule": "nearest_expiry_ge_evaluation_date_plus_7_calendar_days",
        "selection_evaluated_date_moscow": evaluated.isoformat(),
        "expiry": expiry.isoformat(), "last_delivery_date": delivery.isoformat(),
        "quote_currency": "USD", "price_unit": "USD/barrel",
        "source_unit_text": description["UNIT"],
        "contract_size_barrels": contract_size,
        "price": ohlc["close"], "ohlc": ohlc, "price_field": "CLOSE",
        "price_semantics": PRICE_SEMANTICS,
        "source_trade_date": published_date.isoformat(),
        "source_history_till": published_date.isoformat(),
        "source_event_time": None, "source_published_at": None,
        "received_at": received.isoformat(), "data_as_of": received.isoformat(),
        "data_as_of_semantics": "receipt_of_current_revision_not_price_event_time",
        "availability_semantics": "known_available_at_receipt_no_historical_PIT_claim",
        "source_revision_status": "official_iss_current_revision",
        "session_status": "PUBLISHED_PRIOR_DATE_TRADING_RESULTS",
        "current_market_session": "UNKNOWN",
        "freshness_basis": "receipt_recheck_not_intraday_price_age",
        "maximum_receipt_age_seconds": MAX_RECEIPT_AGE_SECONDS,
        "quality_passed": True, "provenance": provenance,
        "collection_elapsed_seconds": round(monotonic() - started, 6),
        **{key: True for key in FACTUAL_FLAGS},
        **{key: False for key in FALSE_FLAGS},
    }


def factual_usable(component: object) -> bool:
    if not isinstance(component, Mapping) or component.get("status") != "READY":
        return False
    data = component.get("data")
    return isinstance(data, Mapping) and (
        data.get("source_id") == SOURCE_ID
        and data.get("acceptance_policy") == POLICY_ID
        and data.get("source_acceptance") == "FACTUAL_ONLY"
        and data.get("price_semantics") == PRICE_SEMANTICS
        and data.get("price_unit") == "USD/barrel"
        and data.get("quote_currency") == "USD"
        and data.get("source_unit_text") == UNIT_TEXT
        and data.get("session_status") == "PUBLISHED_PRIOR_DATE_TRADING_RESULTS"
        and data.get("source_event_time") is None
        and data.get("source_published_at") is None
        and data.get("quality_passed") is True
        and all(data.get(key) is True for key in FACTUAL_FLAGS)
        and all(data.get(key) is False for key in FALSE_FLAGS)
    )


def reconcile_component(component: Mapping, *, now: datetime) -> dict:
    """Downgrade only. Neither re-fetch, current-generation stamps nor retention renew a fact."""
    result = deepcopy(dict(component))
    data = result.get("data")
    if not isinstance(data, dict):
        if result.get("status") == "READY":
            result["status"] = "UNAVAILABLE"
        return result
    reason = None
    age = None
    try:
        now = _utc(now)
        received = _utc(data.get("received_at"))
        age = (now - received).total_seconds()
        _require(factual_usable(result), "persisted factual acceptance not usable")
        _require(_utc(result.get("data_as_of")) == received
                 and _utc(data.get("data_as_of")) == received,
                 "receipt and data_as_of mismatch")
        _require(-MAX_FUTURE_SKEW_SECONDS <= age <= MAX_RECEIPT_AGE_SECONDS,
                 "receipt stale or in the future")
        _require(_date(data.get("selection_evaluated_date_moscow"))
                 == now.astimezone(MOSCOW).date(), "contract selection needs date recheck")
        _require(_date(data.get("source_trade_date")) == _date(data.get("source_history_till"))
                 < now.astimezone(MOSCOW).date(), "published result date invalid")
        _require(_date(data.get("expiry")) >= now.astimezone(MOSCOW).date() + timedelta(days=7),
                 "selected contract no longer eligible")
        ohlc = data.get("ohlc")
        _require(isinstance(ohlc, dict), "missing accepted OHLC")
        checked = _ohlc({key.upper(): value for key, value in ohlc.items()})
        _require(_number(data.get("price"), "price") == checked["close"]
                 and data.get("price_field") == "CLOSE", "accepted CLOSE mismatch")
    except (ValueError, TypeError, OverflowError, KeyError) as exc:
        reason = str(exc)
    if reason:
        for key in FACTUAL_FLAGS:
            data[key] = False
        if result.get("status") == "READY":
            result["status"] = "UNAVAILABLE"
    # Forbidden permissions never survive a malformed/retained component either.
    for key in FALSE_FLAGS:
        data[key] = False
    data["receipt_age_seconds"] = age
    data["freshness_evaluated_at"] = _utc(now).isoformat()
    data["receipt_recheck_stale"] = reason is not None
    data["read_freshness_reason"] = reason
    return result


def apply_oil_freshness(snapshot: dict, *, now: datetime) -> None:
    """Update only oil and its readiness bookkeeping in an owned snapshot view."""
    components = snapshot.get("components")
    if not isinstance(components, dict) or not isinstance(components.get("oil"), Mapping):
        return
    component = components["oil"]
    # Historical governance placeholders have no accepted value to downgrade.
    if component.get("status") == "GOVERNED_BLOCKED":
        return
    components["oil"] = reconcile_component(component, now=now)
    readiness = snapshot.get("readiness")
    if isinstance(readiness, dict):
        statuses = {key: item.get("status") for key, item in components.items()
                    if isinstance(item, Mapping)}
        readiness["component_statuses"] = statuses
        for field, status in (("unavailable_components", "UNAVAILABLE"),
                              ("retained_previous_components", "RETAINED_PREVIOUS")):
            readiness[field] = sorted(key for key, value in statuses.items() if value == status)
        if components["oil"].get("status") in {"UNAVAILABLE", "RETAINED_PREVIOUS"}:
            readiness["status"] = "PARTIAL"
