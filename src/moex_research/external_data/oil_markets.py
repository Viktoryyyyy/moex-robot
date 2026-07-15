from __future__ import annotations

import inspect
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Final
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from .models import (
    ExternalDataError,
    HttpTransport,
    fetch_bytes,
    parse_date,
    parse_datetime,
    parse_integer,
    parse_json_object,
    parse_number,
    provenance,
    raw_payload_sha256,
    require_retrieved_at_utc,
)


MOEX_SECURITIES_ROUTE: Final[str] = (
    "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"
)
MOEX_CANDLES_ROUTE: Final[str] = (
    "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/"
    "securities/{contract_code}/candles.json"
)
CME_QUOTES_ROUTE: Final[str] = (
    "https://www.cmegroup.com/CmeWS/mvc/quotes/v2/425"
)
CME_PRODUCT_CALENDAR_ROUTE: Final[str] = (
    "https://www.cmegroup.com/CmeWS/mvc/ProductCalendar/Future/425"
)
CME_DATAMINE_ROUTE: Final[str] = "https://www.cmegroup.com/datamine.html"
CME_QUOTE_DELAY_DECLARATION: Final[str] = "10 minutes"
CME_QUOTE_DELAY: Final[timedelta] = timedelta(minutes=10)
CME_QUOTE_DELAY_MINUTES: Final[int] = 10

MOEX_CONTRACT_COLUMNS = (
    "SECID",
    "SHORTNAME",
    "ASSETCODE",
    "BOARDID",
    "LASTTRADEDATE",
    "LASTDELDATE",
)
MOEX_CANDLE_COLUMNS = ("begin", "end", "open", "high", "low", "close", "volume", "value")
CME_QUOTE_REQUIRED_FIELDS = frozenset(
    {
        "quoteCode",
        "productCode",
        "lastTradeDate",
        "last",
        "open",
        "high",
        "low",
        "priorSettle",
        "volume",
        "updated",
    }
)
MUTABLE_ALIASES = frozenset({"latest", "current", "autodetect", "continuous"})
MOSCOW = ZoneInfo("Europe/Moscow")
CHICAGO = ZoneInfo("America/Chicago")


def _explicit_contract_code(value: object) -> str:
    code = str(value).strip()
    if code.lower() in MUTABLE_ALIASES or "CONT" in code.upper():
        raise ExternalDataError("mutable or continuous contract alias is forbidden")
    if not re.fullmatch(r"[A-Za-z0-9-]{2,20}", code):
        raise ExternalDataError("explicit contract code is malformed")
    return code


def _iss_table(payload: bytes, block: str, columns: tuple[str, ...]) -> list[list[object]]:
    root = parse_json_object(payload)
    table = root.get(block)
    if not isinstance(table, dict):
        raise ExternalDataError(f"ISS {block} response block is absent")
    if tuple(table.get("columns", ())) != columns:
        raise ExternalDataError(f"ISS {block} columns differ from expected schema")
    rows = table.get("data")
    if not isinstance(rows, list):
        raise ExternalDataError(f"ISS {block} data must be a list")
    for row in rows:
        if not isinstance(row, list) or len(row) != len(columns):
            raise ExternalDataError(f"ISS {block} row width mismatch")
    return rows


def parse_moex_brent_contracts(
    payload: bytes,
    *,
    retrieved_at_utc: datetime,
    source_route: str = MOEX_SECURITIES_ROUTE,
) -> list[dict[str, object]]:
    rows = _iss_table(payload, "securities", MOEX_CONTRACT_COLUMNS)
    base = provenance(
        source_id="moex_brent_futures_daily",
        source_route=source_route,
        payload=payload,
        retrieved_at_utc=retrieved_at_utc,
        source_revision_status="official_iss_current_view",
        historical_model_use_status="blocked_pending_source_validation",
    )
    records: list[dict[str, object]] = []
    identities: set[str] = set()
    for secid, shortname, assetcode, boardid, last_trade, last_delivery in rows:
        if assetcode != "BR":
            continue
        code = _explicit_contract_code(secid)
        if boardid != "RFUD":
            raise ExternalDataError("Brent contract is not on RFUD")
        expiration = parse_date(last_trade, field="expiration_date")
        parse_date(last_delivery, field="last_delivery_date")
        if code in identities:
            raise ExternalDataError("duplicate Brent contract identity")
        identities.add(code)
        records.append(
            {
                "contract_code": code,
                "short_name": str(shortname),
                "expiration_date": expiration.isoformat(),
                "last_delivery_date": parse_date(
                    last_delivery, field="last_delivery_date"
                ).isoformat(),
                **base,
            }
        )
    if not records:
        raise ExternalDataError("MOEX Brent contract list is empty")
    return records


def parse_moex_brent_daily_candles(
    payload: bytes,
    *,
    contract_code: str,
    expiration_date: date,
    retrieved_at_utc: datetime,
    source_route: str | None = None,
) -> list[dict[str, object]]:
    code = _explicit_contract_code(contract_code)
    rows = _iss_table(payload, "candles", MOEX_CANDLE_COLUMNS)
    if not rows:
        raise ExternalDataError("requested non-empty Brent interval returned no rows")
    route = source_route or MOEX_CANDLES_ROUTE.format(contract_code=code)
    base = provenance(
        source_id="moex_brent_futures_daily",
        source_route=route,
        payload=payload,
        retrieved_at_utc=retrieved_at_utc,
        source_revision_status="official_iss_current_view",
        historical_model_use_status="blocked_pending_source_validation",
    )
    records: list[dict[str, object]] = []
    identities: set[tuple[str, str]] = set()
    for begin, end, open_, high, low, close, volume, value in rows:
        try:
            candle_begin = datetime.strptime(str(begin), "%Y-%m-%d %H:%M:%S")
            candle_end = datetime.strptime(str(end), "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise ExternalDataError("MOEX candle timestamp is malformed") from exc
        if candle_begin.date() != candle_end.date() or candle_end <= candle_begin:
            raise ExternalDataError("MOEX daily candle time identity is invalid")
        identity = (code, candle_begin.date().isoformat())
        if identity in identities:
            raise ExternalDataError("duplicate Brent candle date")
        identities.add(identity)
        records.append(
            {
                "contract_code": code,
                "trade_date": candle_begin.date().isoformat(),
                "open": parse_number(open_, field="open"),
                "high": parse_number(high, field="high"),
                "low": parse_number(low, field="low"),
                "close": parse_number(close, field="close"),
                "volume": parse_number(volume, field="volume"),
                "value": parse_number(value, field="value"),
                "candle_begin": str(begin),
                "candle_end": str(end),
                "expiration_date": expiration_date.isoformat(),
                **base,
            }
        )
    return records


def select_contract_for_day(
    contracts: list[dict[str, object]], modeled_day: date
) -> str:
    eligible: list[tuple[date, str]] = []
    for item in contracts:
        code = _explicit_contract_code(item.get("contract_code"))
        expiration = parse_date(item.get("expiration_date"), field="expiration_date")
        if expiration >= modeled_day + timedelta(days=7):
            eligible.append((expiration, code))
    if not eligible:
        raise ExternalDataError("no contract has at least seven calendar days to expiration")
    return min(eligible)[1]


def load_moex_brent_contracts(
    *,
    retrieved_at_utc: datetime,
    transport: HttpTransport = fetch_bytes,
) -> list[dict[str, object]]:
    query = urlencode(
        {
            "iss.meta": "off",
            "iss.only": "securities",
            "securities.columns": ",".join(MOEX_CONTRACT_COLUMNS),
            "lang": "en",
        }
    )
    source_route = MOEX_SECURITIES_ROUTE + "?" + query
    return parse_moex_brent_contracts(
        transport(source_route),
        retrieved_at_utc=retrieved_at_utc,
        source_route=source_route,
    )


def load_moex_brent_daily_candles(
    contract_code: str,
    expiration_date: date,
    start: date,
    end: date,
    *,
    retrieved_at_utc: datetime,
    transport: HttpTransport = fetch_bytes,
) -> list[dict[str, object]]:
    if start > end:
        raise ExternalDataError("requested Brent interval is reversed")
    code = _explicit_contract_code(contract_code)
    query = urlencode(
        {
            "iss.meta": "off",
            "iss.only": "candles",
            "interval": "24",
            "from": start.isoformat(),
            "till": end.isoformat(),
            "candles.columns": ",".join(MOEX_CANDLE_COLUMNS),
        }
    )
    source_route = MOEX_CANDLES_ROUTE.format(contract_code=code) + "?" + query
    payload = transport(source_route)
    return parse_moex_brent_daily_candles(
        payload,
        contract_code=code,
        expiration_date=expiration_date,
        retrieved_at_utc=retrieved_at_utc,
        source_route=source_route,
    )


def parse_cme_wti_contract_calendar(
    payload: bytes, *, retrieved_at_utc: datetime
) -> list[dict[str, object]]:
    import json

    try:
        root = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalDataError("CME product calendar is not valid UTF-8 JSON") from exc
    if not isinstance(root, list) or not root:
        raise ExternalDataError("CME product calendar is empty")
    result: list[dict[str, object]] = []
    identities: set[str] = set()
    required = {"contractMonth", "productCode", "lastTrade", "settlement", "expirationDate"}
    base = provenance(
        source_id="cme_wti_pre_moex",
        source_route=CME_PRODUCT_CALENDAR_ROUTE,
        payload=payload,
        retrieved_at_utc=retrieved_at_utc,
        source_revision_status="official_product_calendar_current_view",
        historical_model_use_status="blocked_pending_license",
    )
    for item in root:
        if not isinstance(item, dict) or not required.issubset(item):
            raise ExternalDataError("CME product calendar schema mismatch")
        code = _explicit_contract_code(item["productCode"])
        if not code.startswith("CL"):
            raise ExternalDataError("CME WTI product code must start with CL")
        if code in identities:
            raise ExternalDataError("duplicate CME WTI contract identity")
        identities.add(code)
        expiration = parse_date(item["lastTrade"], field="expiration_date")
        result.append(
            {
                "contract_code": code,
                "contract_month": str(item["contractMonth"]),
                "expiration_date": expiration.isoformat(),
                "settlement_date": parse_date(
                    item["settlement"], field="settlement_date"
                ).isoformat(),
                **base,
            }
        )
    return result


def parse_cme_wti_current_quotes(
    payload: bytes,
    *,
    expiration_by_contract: dict[str, date],
    retrieved_at_utc: datetime,
) -> list[dict[str, object]]:
    root = parse_json_object(payload)
    quotes = root.get("quotes")
    if not isinstance(quotes, list) or not quotes:
        raise ExternalDataError("CME current quote response is empty")
    if (
        root.get("quoteDelayed") is not True
        or root.get("quoteDelay") != CME_QUOTE_DELAY_DECLARATION
    ):
        raise ExternalDataError("CME quote delay declaration mismatch")
    exchange_trading_date = parse_date(
        root.get("tradeDate"), field="exchange_trading_date"
    )
    retrieved = require_retrieved_at_utc(retrieved_at_utc)
    base = provenance(
        source_id="cme_wti_pre_moex",
        source_route=CME_QUOTES_ROUTE,
        payload=payload,
        retrieved_at_utc=retrieved,
        source_revision_status="official_delayed_current_snapshot",
        historical_model_use_status="blocked_pending_license",
    )
    records: list[dict[str, object]] = []
    identities: set[tuple[str, str]] = set()
    for quote in quotes:
        if not isinstance(quote, dict) or not CME_QUOTE_REQUIRED_FIELDS.issubset(quote):
            raise ExternalDataError("CME current quote schema mismatch")
        if quote.get("close") not in (None, "-", "") or any(
            field in quote for field in ("fullDayClose", "postCutoffSettlement")
        ):
            raise ExternalDataError("full-day close cannot enter a pre-MOEX quote")
        display_code = _explicit_contract_code(quote["quoteCode"])
        if quote["productCode"] != "CL" or not display_code.startswith("CL"):
            raise ExternalDataError("CME quote is not a WTI CL contract")
        price_values = tuple(quote[field] for field in ("last", "open", "high", "low"))
        has_price_observation = not all(
            value in (None, "-", "") for value in price_values
        )
        if has_price_observation and any(
            value in (None, "-", "") for value in price_values
        ):
            raise ExternalDataError("CME current quote price fields are incomplete")
        quote_last_trade = parse_datetime(
            quote["lastTradeDate"], field="last_trade_timestamp_utc"
        ).date()
        matching_codes = [
            candidate
            for candidate, expiration in expiration_by_contract.items()
            if expiration == quote_last_trade and candidate.startswith("CL")
        ]
        if len(matching_codes) != 1:
            raise ExternalDataError("CME quote is missing expiration metadata or it is ambiguous")
        code = _explicit_contract_code(matching_codes[0])
        observed = parse_datetime(quote["updated"], field="observation_timestamp_utc")
        observed_utc = observed.astimezone(timezone.utc)
        if observed_utc > retrieved:
            raise ExternalDataError("CME observation timestamp is after retrieval")
        identity = (code, observed_utc.isoformat())
        if identity in identities:
            raise ExternalDataError("duplicate CME timestamp-contract identity")
        identities.add(identity)
        record = {
            "exchange": "CME/NYMEX",
            "instrument_family": "WTI Light Sweet Crude Oil futures",
            "contract_code": code,
            "display_quote_code": display_code,
            "exchange_trading_date": exchange_trading_date.isoformat(),
            "observation_timestamp_utc": observed_utc.isoformat().replace("+00:00", "Z"),
            "observation_timestamp_moscow": observed_utc.astimezone(MOSCOW).isoformat(),
            "quote_delay_minutes": CME_QUOTE_DELAY_MINUTES,
            "has_price_observation": has_price_observation,
            "expiration_date": expiration_by_contract[code].isoformat(),
            **base,
        }
        if not has_price_observation:
            record.update(
                {
                    "previous_official_settlement": None,
                    "first_price_in_observation_window": None,
                    "high_to_cutoff": None,
                    "low_to_cutoff": None,
                    "last_price_at_cutoff": None,
                    "volume_to_cutoff": None,
                    "open_interest_at_cutoff": None,
                    "return_from_previous_settlement": None,
                    "return_from_window_open": None,
                    "range_to_cutoff": None,
                    "minutes_since_last_trade": None,
                }
            )
            records.append(record)
            continue
        last = parse_number(quote["last"], field="last")
        prior = parse_number(quote["priorSettle"], field="priorSettle")
        open_ = parse_number(quote["open"], field="open")
        high = parse_number(quote["high"], field="high")
        low = parse_number(quote["low"], field="low")
        record.update(
            {
                "previous_official_settlement": prior,
                "first_price_in_observation_window": open_,
                "high_to_cutoff": high,
                "low_to_cutoff": low,
                "last_price_at_cutoff": last,
                "volume_to_cutoff": parse_integer(quote["volume"], field="volume"),
                "open_interest_at_cutoff": None,
                "return_from_previous_settlement": last / prior - 1.0,
                "return_from_window_open": last / open_ - 1.0,
                "range_to_cutoff": high / low - 1.0,
                "minutes_since_last_trade": (retrieved - observed_utc).total_seconds() / 60.0,
            }
        )
        records.append(record)
    return records


def build_pre_moex_observation(
    quotes: list[dict[str, object]],
    modeled_day: date,
    *,
    cutoff_local_time: time = time(8, 45),
) -> dict[str, object]:
    cutoff = datetime.combine(modeled_day, cutoff_local_time, tzinfo=MOSCOW)
    session_start = datetime.combine(modeled_day, time(8, 50), tzinfo=MOSCOW)
    if cutoff >= session_start:
        raise ExternalDataError("pre-MOEX cutoff must precede the 08:50 session start")
    contracts: dict[str, dict[str, object]] = {}
    for quote in quotes:
        code = _explicit_contract_code(quote.get("contract_code"))
        expiration = parse_date(quote.get("expiration_date"), field="expiration_date")
        contract = {
            "contract_code": code,
            "expiration_date": expiration.isoformat(),
        }
        existing = contracts.get(code)
        if existing is not None and existing != contract:
            raise ExternalDataError("CME contract expiration metadata is ambiguous")
        contracts[code] = contract
    selected = select_contract_for_day(list(contracts.values()), modeled_day)

    candidates: list[dict[str, object]] = []
    for quote in quotes:
        if _explicit_contract_code(quote.get("contract_code")) != selected:
            continue
        delay_minutes = quote.get("quote_delay_minutes")
        if type(delay_minutes) is not int or delay_minutes != CME_QUOTE_DELAY_MINUTES:
            raise ExternalDataError("CME quote delay semantics are absent or ambiguous")
        has_price_observation = quote.get("has_price_observation")
        if type(has_price_observation) is not bool:
            raise ExternalDataError("CME price observation semantics are absent or ambiguous")
        if not has_price_observation:
            continue
        timestamp = parse_datetime(
            quote.get("observation_timestamp_utc"), field="observation_timestamp_utc"
        )
        retrieved = parse_datetime(quote.get("retrieved_at_utc"), field="retrieved_at_utc")
        available_at = timestamp + CME_QUOTE_DELAY
        if available_at > retrieved:
            raise ExternalDataError("CME quote was not visible at retrieval time")
        if available_at <= cutoff:
            candidates.append(quote)
    if not candidates:
        raise ExternalDataError(
            "selected CME contract has no observation visible at or before the cutoff"
        )
    result = max(
        candidates,
        key=lambda item: parse_datetime(
            item["observation_timestamp_utc"], field="observation_timestamp_utc"
        ),
    ).copy()
    result["cutoff_timestamp_moscow"] = cutoff.isoformat()
    return result


def assert_cme_market_open_at_cutoff(
    modeled_day: date, *, cutoff_local_time: time = time(8, 45)
) -> None:
    cutoff_chicago = datetime.combine(
        modeled_day, cutoff_local_time, tzinfo=MOSCOW
    ).astimezone(CHICAGO)
    weekday = cutoff_chicago.weekday()
    local_clock = cutoff_chicago.timetz().replace(tzinfo=None)

    if weekday == 5:
        raise ExternalDataError("CME session is closed on Saturday")
    if weekday == 6:
        if local_clock < time(17, 0):
            raise ExternalDataError("CME Sunday session is closed before 17:00 Chicago")
        return
    if weekday == 4 and local_clock >= time(16, 0):
        raise ExternalDataError("CME Friday session is closed from 16:00 Chicago")
    if weekday <= 3 and time(16, 0) <= local_clock < time(17, 0):
        raise ExternalDataError("CME daily maintenance break overlaps cutoff")


def load_cme_wti_contract_calendar(
    *,
    retrieved_at_utc: datetime,
    transport: HttpTransport = fetch_bytes,
) -> list[dict[str, object]]:
    return parse_cme_wti_contract_calendar(
        transport(CME_PRODUCT_CALENDAR_ROUTE), retrieved_at_utc=retrieved_at_utc
    )


def load_cme_wti_current_quotes(
    expiration_by_contract: dict[str, date],
    *,
    retrieved_at_utc: datetime,
    transport: HttpTransport = fetch_bytes,
) -> list[dict[str, object]]:
    return parse_cme_wti_current_quotes(
        transport(CME_QUOTES_ROUTE),
        expiration_by_contract=expiration_by_contract,
        retrieved_at_utc=retrieved_at_utc,
    )


def parse_cme_wti_historical_intraday(payload: bytes) -> list[dict[str, object]]:
    root = parse_json_object(payload)
    if root.get("dataset") != "CME_DataMine_Market_by_Order" or root.get("licensed") is not True:
        raise ExternalDataError("CME historical intraday is blocked_pending_license")
    rows = root.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ExternalDataError("licensed CME historical intraday interval is empty")
    required = {"contract_code", "event_timestamp_utc", "price", "quantity"}
    result: list[dict[str, object]] = []
    identities: set[tuple[str, str]] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != required:
            raise ExternalDataError("CME historical intraday schema mismatch")
        code = _explicit_contract_code(row["contract_code"])
        timestamp = parse_datetime(row["event_timestamp_utc"], field="event_timestamp_utc")
        identity = (code, timestamp.astimezone(timezone.utc).isoformat())
        if identity in identities:
            raise ExternalDataError("duplicate CME historical timestamp-contract identity")
        identities.add(identity)
        result.append(
            {
                "contract_code": code,
                "event_timestamp_utc": identity[1],
                "price": parse_number(row["price"], field="price"),
                "quantity": parse_integer(row["quantity"], field="quantity"),
                "source_route": CME_DATAMINE_ROUTE,
                "raw_payload_sha256": raw_payload_sha256(payload),
                "historical_model_use_status": "diagnostic_only",
            }
        )
    return result


def contract_selection_uses_no_volume() -> bool:
    source = inspect.getsource(select_contract_for_day)
    return "volume" not in source.lower()
