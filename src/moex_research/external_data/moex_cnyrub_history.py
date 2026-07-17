from __future__ import annotations

import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Final
from urllib.parse import parse_qsl, urlencode, urlsplit
from zoneinfo import ZoneInfo

from .models import ExternalDataError, HttpTransport, fetch_bytes, parse_json_object, raw_payload_sha256

Sleeper = Callable[[float], None]
UtcClock = Callable[[], datetime]
SOURCE_ID: Final = "moex_cnyrub_tom_daily"
SECURITY_ID: Final = "CNYRUB_TOM"
BOARD_ID: Final = "CETS"
ENGINE: Final = "currency"
MARKET: Final = "selt"
MOEX_ISS_HOST: Final = "iss.moex.com"
MOSCOW: Final = ZoneInfo("Europe/Moscow")
SECURITY_METADATA_ROUTE: Final = "https://iss.moex.com/iss/securities/CNYRUB_TOM.json"
CANDLE_ROUTE: Final = (
    "https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/"
    "securities/CNYRUB_TOM/candles.json"
)
SOURCE_REVISION_STATUS: Final = "official_iss_current_revision"
HISTORICAL_MODEL_USE_STATUS: Final = "source_validation_only"
TRANSIENT_HTTP_ERROR_MESSAGE: Final = "external-data request failed"
CNYRUB_HTTP_MAX_ATTEMPTS: Final = 5
CNYRUB_HTTP_RETRY_DELAYS_SECONDS: Final = (0.5, 1.0, 2.0, 4.0)
CNYRUB_MAX_PAGES: Final = 10_000
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CANDLE_COLUMNS: Final = ("open", "close", "high", "low", "value", "volume", "begin", "end")


class CnyrubHistoryError(ValueError):
    def __init__(self, message: str, *, blocker: str = "other_fail_closed_with_exact_reason") -> None:
        super().__init__(message)
        self.blocker = blocker


@dataclass(frozen=True)
class CnyrubSecurityIdentity:
    source_id: str
    security_id: str
    board_id: str
    engine: str
    market: str
    primary_board: bool
    active_board: bool
    history_from: date
    history_till: date | None
    metadata_route: str
    retrieved_at_utc: datetime
    raw_payload_sha256: str
    source_revision_status: str
    historical_model_use_status: str

    def as_record(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CnyrubDailyCandle:
    source_id: str
    security_id: str
    board_id: str
    engine: str
    market: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    value: float
    candle_begin: datetime
    candle_end: datetime
    source_route: str
    retrieved_at_utc: datetime
    raw_payload_sha256: str
    source_revision_status: str
    historical_model_use_status: str

    def as_record(self) -> dict[str, object]:
        return asdict(self)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CnyrubHistoryError("retrieval timestamp must be timezone-aware", blocker="provenance_not_sufficient")
    if value.utcoffset() != timedelta(0):
        raise CnyrubHistoryError("retrieval timestamp must be expressed in UTC", blocker="provenance_not_sufficient")
    return value.astimezone(timezone.utc)


def fetch_cnyrub_bytes_with_retry(
    url: str,
    *,
    transport: HttpTransport = fetch_bytes,
    sleeper: Sleeper = time.sleep,
) -> bytes:
    for attempt in range(CNYRUB_HTTP_MAX_ATTEMPTS):
        try:
            return transport(url)
        except ExternalDataError as exc:
            if exc.args != (TRANSIENT_HTTP_ERROR_MESSAGE,):
                raise
            if attempt + 1 == CNYRUB_HTTP_MAX_ATTEMPTS:
                raise ExternalDataError(
                    f"{TRANSIENT_HTTP_ERROR_MESSAGE}; official route={url}; attempts={CNYRUB_HTTP_MAX_ATTEMPTS}"
                ) from exc
            sleeper(CNYRUB_HTTP_RETRY_DELAYS_SECONDS[attempt])
    raise AssertionError("unreachable retry state")


def _official(url: str, path: str) -> dict[str, str]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != MOEX_ISS_HOST
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.path != path
    ):
        raise CnyrubHistoryError("route is not the exact allowlisted official MOEX ISS route", blocker="provenance_not_sufficient")
    return dict(parse_qsl(parsed.query, keep_blank_values=True))


def _block(
    payload: bytes, name: str, required: Sequence[str]
) -> tuple[list[dict[str, object]], tuple[str, ...], str, Mapping[str, Any]]:
    try:
        root = parse_json_object(payload)
    except ExternalDataError as exc:
        raise CnyrubHistoryError("official ISS response is not valid UTF-8 JSON", blocker="official_schema_not_stable") from exc
    value = root.get(name)
    if not isinstance(value, Mapping):
        raise CnyrubHistoryError(f"official ISS response lacks {name} block", blocker="official_schema_not_stable")
    columns, data = value.get("columns"), value.get("data")
    if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
        raise CnyrubHistoryError(f"official ISS {name} columns are malformed", blocker="official_schema_not_stable")
    if not set(required).issubset(columns):
        raise CnyrubHistoryError(f"official ISS {name} schema is missing required columns", blocker="official_schema_not_stable")
    if not isinstance(data, list):
        raise CnyrubHistoryError(f"official ISS {name} data is malformed", blocker="official_schema_not_stable")
    rows: list[dict[str, object]] = []
    for raw in data:
        if not isinstance(raw, list) or len(raw) != len(columns):
            raise CnyrubHistoryError(f"official ISS {name} row width mismatch", blocker="official_schema_not_stable")
        rows.append(dict(zip(columns, raw, strict=True)))
    digest = raw_payload_sha256(payload)
    if not _SHA256.fullmatch(digest):  # pragma: no cover
        raise CnyrubHistoryError("official payload digest is invalid", blocker="provenance_not_sufficient")
    return rows, tuple(columns), digest, root


def build_security_metadata_url() -> str:
    return SECURITY_METADATA_ROUTE + "?" + urlencode({"iss.meta": "off", "iss.only": "description,boards"})


def _optional_date(value: object, field: str) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise CnyrubHistoryError(f"official security metadata {field} is invalid", blocker="official_schema_not_stable") from exc


def parse_security_metadata_response(
    payload: bytes, *, route: str, retrieved_at_utc: datetime
) -> CnyrubSecurityIdentity:
    query = _official(route, "/iss/securities/CNYRUB_TOM.json")
    if query != {"iss.meta": "off", "iss.only": "description,boards"}:
        raise CnyrubHistoryError("security metadata route parameters are not exact", blocker="provenance_not_sufficient")
    description, _, digest, root = _block(payload, "description", ("name", "value"))
    values: dict[str, object] = {}
    for row in description:
        key = str(row["name"] or "").strip()
        if not key or key in values:
            raise CnyrubHistoryError("official security description has duplicate or empty field identity", blocker="official_schema_not_stable")
        values[key] = row["value"]
    if str(values.get("SECID") or "").strip() != SECURITY_ID:
        raise CnyrubHistoryError("official SECID does not match CNYRUB_TOM", blocker="security_identity_not_reproducible")
    boards = root.get("boards")
    required = {"secid", "boardid", "engine", "market", "is_traded", "is_primary", "history_from", "history_till"}
    if not isinstance(boards, Mapping):
        raise CnyrubHistoryError("official security boards block is absent", blocker="official_schema_not_stable")
    columns, data = boards.get("columns"), boards.get("data")
    if (
        not isinstance(columns, list)
        or not all(isinstance(item, str) for item in columns)
        or not required.issubset(columns)
        or not isinstance(data, list)
    ):
        raise CnyrubHistoryError("official security boards schema is missing required identity columns", blocker="official_schema_not_stable")
    try:
        rows = [dict(zip(columns, raw, strict=True)) for raw in data]
    except (TypeError, ValueError) as exc:
        raise CnyrubHistoryError("official security board row width mismatch", blocker="official_schema_not_stable") from exc
    matching = [
        row for row in rows
        if str(row.get("secid") or "").strip() == SECURITY_ID
        and str(row.get("boardid") or "").strip() == BOARD_ID
        and str(row.get("engine") or "").strip() == ENGINE
        and str(row.get("market") or "").strip() == MARKET
    ]
    if len(matching) != 1:
        raise CnyrubHistoryError("CNYRUB_TOM does not resolve to one official currency/selt/CETS identity", blocker="security_identity_not_reproducible")
    board = matching[0]
    if int(board.get("is_primary") or 0) != 1 or int(board.get("is_traded") or 0) != 1:
        raise CnyrubHistoryError("official CETS board is not both primary and active", blocker="security_identity_not_reproducible")
    history_from = _optional_date(board.get("history_from"), "history_from")
    history_till = _optional_date(board.get("history_till"), "history_till")
    if history_from is None or (history_till is not None and history_till < history_from):
        raise CnyrubHistoryError("official security history interval is invalid", blocker="security_identity_not_reproducible")
    return CnyrubSecurityIdentity(
        SOURCE_ID, SECURITY_ID, BOARD_ID, ENGINE, MARKET, True, True,
        history_from, history_till, route, _utc(retrieved_at_utc), digest,
        SOURCE_REVISION_STATUS, HISTORICAL_MODEL_USE_STATUS,
    )


def load_security_identity(
    *, transport: HttpTransport = fetch_cnyrub_bytes_with_retry, clock: UtcClock = utc_now
) -> CnyrubSecurityIdentity:
    route = build_security_metadata_url()
    _official(route, "/iss/securities/CNYRUB_TOM.json")
    try:
        payload = transport(route)
    except Exception as exc:
        raise CnyrubHistoryError(f"official exact-security metadata request failed: {exc}", blocker="security_identity_not_reproducible") from exc
    return parse_security_metadata_response(payload, route=route, retrieved_at_utc=_utc(clock()))


def build_candle_url(from_date: date, till_date: date, *, start: int = 0) -> str:
    if till_date < from_date or start < 0:
        raise CnyrubHistoryError("candle range or pagination start is invalid")
    query = urlencode({
        "from": from_date.isoformat(), "till": till_date.isoformat(), "interval": 24,
        "start": start, "iss.meta": "off", "iss.only": "candles",
        "candles.columns": ",".join(_CANDLE_COLUMNS),
    })
    return f"{CANDLE_ROUTE}?{query}"


def _number(value: object, field: str, *, nonnegative: bool = False) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise CnyrubHistoryError(f"candle {field} is malformed", blocker="numerical_or_chronology_integrity_failure") from exc
    if not math.isfinite(result) or (nonnegative and result < 0):
        condition = "finite and non-negative" if nonnegative else "finite"
        raise CnyrubHistoryError(f"candle {field} must be {condition}", blocker="numerical_or_chronology_integrity_failure")
    return result


def _timestamp(value: object, field: str) -> datetime:
    try:
        parsed = datetime.strptime(str(value or "").strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise CnyrubHistoryError(f"candle {field} timestamp is malformed", blocker="numerical_or_chronology_integrity_failure") from exc
    return parsed.replace(tzinfo=MOSCOW)


def parse_candle_page_response(
    payload: bytes,
    *,
    from_date: date,
    till_date: date,
    start: int,
    route: str,
    retrieved_at_utc: datetime,
) -> tuple[list[CnyrubDailyCandle], tuple[str, ...]]:
    query = _official(
        route,
        "/iss/engines/currency/markets/selt/boards/CETS/securities/CNYRUB_TOM/candles.json",
    )
    expected = {
        "from": from_date.isoformat(), "till": till_date.isoformat(), "interval": "24",
        "start": str(start), "iss.meta": "off", "iss.only": "candles",
        "candles.columns": ",".join(_CANDLE_COLUMNS),
    }
    if query != expected:
        raise CnyrubHistoryError("candle route does not pin the exact security, board, range, interval, and page", blocker="provenance_not_sufficient")
    rows, columns, digest, _ = _block(payload, "candles", _CANDLE_COLUMNS)
    retrieved = _utc(retrieved_at_utc)
    result: list[CnyrubDailyCandle] = []
    previous: date | None = None
    for row in rows:
        open_ = _number(row["open"], "open")
        close = _number(row["close"], "close")
        high = _number(row["high"], "high")
        low = _number(row["low"], "low")
        value = _number(row["value"], "value", nonnegative=True)
        volume = _number(row["volume"], "volume", nonnegative=True)
        begin, end = _timestamp(row["begin"], "begin"), _timestamp(row["end"], "end")
        trade_date = begin.date()
        if end.date() != trade_date or end < begin or not from_date <= trade_date <= till_date:
            raise CnyrubHistoryError("daily candle timestamp or range is invalid", blocker="numerical_or_chronology_integrity_failure")
        if high < max(open_, close, low) or low > min(open_, close, high):
            raise CnyrubHistoryError("daily candle OHLC values are inconsistent", blocker="numerical_or_chronology_integrity_failure")
        if previous is not None and trade_date <= previous:
            raise CnyrubHistoryError("daily candle dates are duplicated or not chronological", blocker="numerical_or_chronology_integrity_failure")
        previous = trade_date
        result.append(CnyrubDailyCandle(
            SOURCE_ID, SECURITY_ID, BOARD_ID, ENGINE, MARKET, trade_date,
            open_, high, low, close, volume, value, begin, end, route, retrieved,
            digest, SOURCE_REVISION_STATUS, HISTORICAL_MODEL_USE_STATUS,
        ))
    return result, columns


def load_daily_history(
    identity: CnyrubSecurityIdentity,
    *,
    from_date: date,
    till_date: date,
    transport: HttpTransport = fetch_cnyrub_bytes_with_retry,
    clock: UtcClock = utc_now,
) -> list[CnyrubDailyCandle]:
    if (
        (identity.security_id, identity.board_id, identity.engine, identity.market)
        != (SECURITY_ID, BOARD_ID, ENGINE, MARKET)
        or not identity.primary_board
        or not identity.active_board
    ):
        raise CnyrubHistoryError("security identity changed before candle retrieval", blocker="security_identity_not_reproducible")
    candles: list[CnyrubDailyCandle] = []
    start = 0
    schema: tuple[str, ...] | None = None
    for _ in range(CNYRUB_MAX_PAGES):
        route = build_candle_url(from_date, till_date, start=start)
        try:
            payload = transport(route)
        except Exception as exc:
            raise CnyrubHistoryError(f"official daily candle request failed: {exc}", blocker="official_daily_candles_not_available") from exc
        page, page_schema = parse_candle_page_response(
            payload, from_date=from_date, till_date=till_date, start=start,
            route=route, retrieved_at_utc=_utc(clock()),
        )
        if schema is not None and page_schema != schema:
            raise CnyrubHistoryError("official candle schema changed during retrieval", blocker="official_schema_not_stable")
        schema = page_schema
        if not page:
            break
        candles.extend(page)
        start += len(page)
    else:
        raise CnyrubHistoryError("official candle pagination exceeded the bounded page limit", blocker="official_schema_not_stable")
    if not candles:
        raise CnyrubHistoryError("official daily candle history is unavailable", blocker="official_daily_candles_not_available")
    dates = [item.trade_date for item in candles]
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise CnyrubHistoryError("daily candle dates are duplicated or not chronological across pages", blocker="numerical_or_chronology_integrity_failure")
    return candles


def validate_prior_session_candle(
    candle: CnyrubDailyCandle, *, target_trade_date: date, prior_trade_date: date
) -> None:
    anchor = datetime.combine(
        target_trade_date, datetime.strptime("06:00:00", "%H:%M:%S").time(), tzinfo=MOSCOW
    )
    if candle.trade_date != prior_trade_date or candle.candle_end >= anchor:
        raise CnyrubHistoryError("CNYRUB candle violates the frozen prior-session 06:00 forecast anchor", blocker="point_in_time_cutoff_not_provable")
