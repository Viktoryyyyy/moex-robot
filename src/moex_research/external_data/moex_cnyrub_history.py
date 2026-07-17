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

SOURCE_ID: Final[str] = "moex_cnyrub_tom_daily"
SECURITY_ID: Final[str] = "CNYRUB_TOM"
BOARD_ID: Final[str] = "CETS"
ENGINE: Final[str] = "currency"
MARKET: Final[str] = "selt"
MOEX_ISS_HOST: Final[str] = "iss.moex.com"
MOSCOW: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")
SECURITY_METADATA_ROUTE: Final[str] = "https://iss.moex.com/iss/securities/CNYRUB_TOM.json"
CANDLE_ROUTE: Final[str] = (
    "https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/"
    "securities/CNYRUB_TOM/candles.json"
)
SOURCE_REVISION_STATUS: Final[str] = "official_iss_current_revision"
HISTORICAL_MODEL_USE_STATUS: Final[str] = "source_validation_only"
TRANSIENT_HTTP_ERROR_MESSAGE: Final[str] = "external-data request failed"
CNYRUB_HTTP_MAX_ATTEMPTS: Final[int] = 5
CNYRUB_HTTP_RETRY_DELAYS_SECONDS: Final[tuple[float, ...]] = (0.5, 1.0, 2.0, 4.0)
_SHA256_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_CANDLE_COLUMNS: Final[tuple[str, ...]] = (
    "open",
    "close",
    "high",
    "low",
    "value",
    "volume",
    "begin",
    "end",
)


class CnyrubHistoryError(ValueError):
    """Fail-closed official-source, schema, identity, chronology, or PIT error."""

    def __init__(
        self,
        message: str,
        *,
        blocker: str = "other_fail_closed_with_exact_reason",
    ) -> None:
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


def fetch_cnyrub_bytes_with_retry(
    url: str,
    *,
    transport: HttpTransport = fetch_bytes,
    sleeper: Sleeper = time.sleep,
) -> bytes:
    for attempt in range(1, CNYRUB_HTTP_MAX_ATTEMPTS + 1):
        try:
            return transport(url)
        except ExternalDataError as exc:
            if exc.args != (TRANSIENT_HTTP_ERROR_MESSAGE,):
                raise
            if attempt == CNYRUB_HTTP_MAX_ATTEMPTS:
                raise ExternalDataError(
                    f"{TRANSIENT_HTTP_ERROR_MESSAGE}; official route={url}; "
                    f"attempts={CNYRUB_HTTP_MAX_ATTEMPTS}"
                ) from exc
            sleeper(CNYRUB_HTTP_RETRY_DELAYS_SECONDS[attempt - 1])
    raise AssertionError("unreachable CNYRUB HTTP retry state")


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CnyrubHistoryError(
            "retrieval timestamp must be timezone-aware",
            blocker="provenance_not_sufficient",
        )
    if value.utcoffset() != timedelta(0):
        raise CnyrubHistoryError(
            "retrieval timestamp must be expressed in UTC",
            blocker="provenance_not_sufficient",
        )
    return value.astimezone(timezone.utc)


def _official_route(url: str, *, expected_path: str) -> dict[str, str]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != MOEX_ISS_HOST
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.path != expected_path
    ):
        raise CnyrubHistoryError(
            "route is not the exact allowlisted official MOEX ISS route",
            blocker="provenance_not_sufficient",
        )
    return dict(parse_qsl(parsed.query, keep_blank_values=True))


def _iss_block(
    payload: bytes,
    *,
    block_name: str,
    required_columns: Sequence[str],
) -> tuple[list[dict[str, object]], tuple[str, ...], str, Mapping[str, Any]]:
    try:
        decoded = parse_json_object(payload)
    except ExternalDataError as exc:
        raise CnyrubHistoryError(
            "official ISS response is not valid UTF-8 JSON",
            blocker="official_schema_not_stable",
        ) from exc
    block = decoded.get(block_name)
    if not isinstance(block, Mapping):
        raise CnyrubHistoryError(
            f"official ISS response lacks {block_name} block",
            blocker="official_schema_not_stable",
        )
    columns = block.get("columns")
    data = block.get("data")
    if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
        raise CnyrubHistoryError(
            f"official ISS {block_name} columns are malformed",
            blocker="official_schema_not_stable",
        )
    if not set(required_columns).issubset(columns):
        raise CnyrubHistoryError(
            f"official ISS {block_name} schema is missing required columns",
            blocker="official_schema_not_stable",
        )
    if not isinstance(data, list):
        raise CnyrubHistoryError(
            f"official ISS {block_name} data is malformed",
            blocker="official_schema_not_stable",
        )
    rows: list[dict[str, object]] = []
    for raw in data:
        if not isinstance(raw, list) or len(raw) != len(columns):
            raise CnyrubHistoryError(
                f"official ISS {block_name} row width mismatch",
                blocker="official_schema_not_stable",
            )
        rows.append(dict(zip(columns, raw, strict=True)))
    digest = raw_payload_sha256(payload)
    if not _SHA256_PATTERN.fullmatch(digest):  # pragma: no cover
        raise CnyrubHistoryError(
            "official payload digest is invalid",
            blocker="provenance_not_sufficient",
        )
    return rows, tuple(columns), digest, decoded


def build_security_metadata_url() -> str:
    query = urlencode(
        {
            "iss.meta": "off",
            "iss.only": "description,boards",
        }
    )
    return f"{SECURITY_METADATA_ROUTE}?{query}"


def _optional_date(value: object, *, field: str) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise CnyrubHistoryError(
            f"official security metadata {field} is invalid",
            blocker="official_schema_not_stable",
        ) from exc


def parse_security_metadata_response(
    payload: bytes,
    *,
    route: str,
    retrieved_at_utc: datetime,
) -> CnyrubSecurityIdentity:
    query = _official_route(route, expected_path="/iss/securities/CNYRUB_TOM.json")
    if query != {"iss.meta": "off", "iss.only": "description,boards"}:
        raise CnyrubHistoryError(
            "security metadata route parameters are not exact",
            blocker="provenance_not_sufficient",
        )
    description, _, digest, decoded = _iss_block(
        payload,
        block_name="description",
        required_columns=("name", "value"),
    )
    values: dict[str, object] = {}
    for row in description:
        name = str(row["name"] or "").strip()
        if not name or name in values:
            raise CnyrubHistoryError(
                "official security description has duplicate or empty field identity",
                blocker="official_schema_not_stable",
            )
        values[name] = row["value"]
    if str(values.get("SECID") or "").strip() != SECURITY_ID:
        raise CnyrubHistoryError(
            "official SECID does not match CNYRUB_TOM",
            blocker="security_identity_not_reproducible",
        )
    boards = decoded.get("boards")
    if not isinstance(boards, Mapping):
        raise CnyrubHistoryError(
            "official security boards block is absent",
            blocker="official_schema_not_stable",
        )
    columns = boards.get("columns")
    data = boards.get("data")
    required = {
        "secid",
        "boardid",
        "engine",
        "market",
        "is_traded",
        "is_primary",
        "history_from",
        "history_till",
    }
    if (
        not isinstance(columns, list)
        or not all(isinstance(item, str) for item in columns)
        or not required.issubset(columns)
        or not isinstance(data, list)
    ):
        raise CnyrubHistoryError(
            "official security boards schema is missing required identity columns",
            blocker="official_schema_not_stable",
        )
    try:
        board_rows = [dict(zip(columns, raw, strict=True)) for raw in data]
    except (TypeError, ValueError) as exc:
        raise CnyrubHistoryError(
            "official security board row width mismatch",
            blocker="official_schema_not_stable",
        ) from exc
    matching = [
        row
        for row in board_rows
        if str(row.get("secid") or "").strip() == SECURITY_ID
        and str(row.get("boardid") or "").strip() == BOARD_ID
        and str(row.get("engine") or "").strip() == ENGINE
        and str(row.get("market") or "").strip() == MARKET
    ]
    if len(matching) != 1:
        raise CnyrubHistoryError(
            "CNYRUB_TOM does not resolve to one official currency/selt/CETS identity",
            blocker="security_identity_not_reproducible",
        )
    board = matching[0]
    if int(board.get("is_primary") or 0) != 1 or int(board.get("is_traded") or 0) != 1:
        raise CnyrubHistoryError(
            "official CETS board is not both primary and active",
            blocker="security_identity_not_reproducible",
        )
    history_from = _optional_date(board.get("history_from"), field="history_from")
    history_till = _optional_date(board.get("history_till"), field="history_till")
    if history_from is None or (history_till is not None and history_till < history_from):
        raise CnyrubHistoryError(
            "official security history interval is invalid",
            blocker="security_identity_not_reproducible",
        )
    return CnyrubSecurityIdentity(
        source_id=SOURCE_ID,
        security_id=SECURITY_ID,
        board_id=BOARD_ID,
        engine=ENGINE,
        market=MARKET,
        primary_board=True,
        active_board=True,
        history_from=history_from,
        history_till=history_till,
        metadata_route=route,
        retrieved_at_utc=_utc(retrieved_at_utc),
        raw_payload_sha256=digest,
        source_revision_status=SOURCE_REVISION_STATUS,
        historical_model_use_status=HISTORICAL_MODEL_USE_STATUS,
    )


def load_security_identity(
    *,
    transport: HttpTransport = fetch_cnyrub_bytes_with_retry,
    clock: UtcClock = utc_now,
) -> CnyrubSecurityIdentity:
    route = build_security_metadata_url()
    _official_route(route, expected_path="/iss/securities/CNYRUB_TOM.json")
    try:
        payload = transport(route)
    except Exception as exc:
        raise CnyrubHistoryError(
            f"official exact-security metadata request failed: {exc}",
            blocker="security_identity_not_reproducible",
        ) from exc
    return parse_security_metadata_response(
        payload,
        route=route,
        retrieved_at_utc=_utc(clock()),
    )


def build_candle_url(
    from_date: date,
    till_date: date,
    *,
    start: int = 0,
) -> str:
    if till_date < from_date or start < 0:
        raise CnyrubHistoryError("candle range or pagination start is invalid")
    query = urlencode(
        {
            "from": from_date.isoformat(),
            "till": till_date.isoformat(),
            "interval": 24,
            "start": start,
            "iss.meta": "off",
            "iss.only": "candles,candles.cursor",
            "candles.columns": ",".join(_REQUIRED_CANDLE_COLUMNS),
        }
    )
    return f"{CANDLE_ROUTE}?{query}"


def _finite_number(value: object, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CnyrubHistoryError(
            f"candle {field} is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        ) from exc
    if not math.isfinite(number):
        raise CnyrubHistoryError(
            f"candle {field} must be finite",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return number


def _finite_nonnegative(value: object, *, field: str) -> float:
    number = _finite_number(value, field=field)
    if number < 0:
        raise CnyrubHistoryError(
            f"candle {field} must be non-negative",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return number


def _candle_timestamp(value: object, *, field: str) -> datetime:
    text = str(value or "").strip()
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise CnyrubHistoryError(
            f"candle {field} timestamp is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        ) from exc
    return parsed.replace(tzinfo=MOSCOW)


def _cursor(decoded: Mapping[str, Any]) -> tuple[int, int, int]:
    block = decoded.get("candles.cursor")
    if not isinstance(block, Mapping):
        raise CnyrubHistoryError(
            "official candle cursor is absent",
            blocker="official_schema_not_stable",
        )
    columns = block.get("columns")
    data = block.get("data")
    if not isinstance(columns, list) or not isinstance(data, list) or len(data) != 1:
        raise CnyrubHistoryError(
            "official candle cursor is malformed",
            blocker="official_schema_not_stable",
        )
    try:
        row = dict(zip(columns, data[0], strict=True))
        cursor = (int(row["INDEX"]), int(row["TOTAL"]), int(row["PAGESIZE"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise CnyrubHistoryError(
            "official candle cursor fields are invalid",
            blocker="official_schema_not_stable",
        ) from exc
    if cursor[0] < 0 or cursor[1] < 0 or cursor[2] <= 0:
        raise CnyrubHistoryError(
            "official candle cursor values are invalid",
            blocker="official_schema_not_stable",
        )
    return cursor


def parse_candle_page_response(
    payload: bytes,
    *,
    from_date: date,
    till_date: date,
    start: int,
    route: str,
    retrieved_at_utc: datetime,
) -> tuple[list[CnyrubDailyCandle], tuple[int, int, int], tuple[str, ...]]:
    query = _official_route(
        route,
        expected_path=(
            "/iss/engines/currency/markets/selt/boards/CETS/"
            "securities/CNYRUB_TOM/candles.json"
        ),
    )
    expected_query = {
        "from": from_date.isoformat(),
        "till": till_date.isoformat(),
        "interval": "24",
        "start": str(start),
        "iss.meta": "off",
        "iss.only": "candles,candles.cursor",
        "candles.columns": ",".join(_REQUIRED_CANDLE_COLUMNS),
    }
    if query != expected_query:
        raise CnyrubHistoryError(
            "candle route does not pin the exact security, board, range, interval, and page",
            blocker="provenance_not_sufficient",
        )
    rows, columns, digest, decoded = _iss_block(
        payload,
        block_name="candles",
        required_columns=_REQUIRED_CANDLE_COLUMNS,
    )
    cursor = _cursor(decoded)
    timestamp = _utc(retrieved_at_utc)
    parsed: list[CnyrubDailyCandle] = []
    previous: date | None = None
    for row in rows:
        candle_open = _finite_number(row["open"], field="open")
        close = _finite_number(row["close"], field="close")
        high = _finite_number(row["high"], field="high")
        low = _finite_number(row["low"], field="low")
        value = _finite_nonnegative(row["value"], field="value")
        volume = _finite_nonnegative(row["volume"], field="volume")
        begin = _candle_timestamp(row["begin"], field="begin")
        end = _candle_timestamp(row["end"], field="end")
        trade_date = begin.date()
        if (
            end.date() != trade_date
            or end < begin
            or not from_date <= trade_date <= till_date
        ):
            raise CnyrubHistoryError(
                "daily candle timestamp or range is invalid",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if high < max(candle_open, close, low) or low > min(candle_open, close, high):
            raise CnyrubHistoryError(
                "daily candle OHLC values are inconsistent",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if previous is not None and trade_date <= previous:
            raise CnyrubHistoryError(
                "daily candle dates are duplicated or not chronological",
                blocker="numerical_or_chronology_integrity_failure",
            )
        previous = trade_date
        parsed.append(
            CnyrubDailyCandle(
                source_id=SOURCE_ID,
                security_id=SECURITY_ID,
                board_id=BOARD_ID,
                engine=ENGINE,
                market=MARKET,
                trade_date=trade_date,
                open=candle_open,
                high=high,
                low=low,
                close=close,
                volume=volume,
                value=value,
                candle_begin=begin,
                candle_end=end,
                source_route=route,
                retrieved_at_utc=timestamp,
                raw_payload_sha256=digest,
                source_revision_status=SOURCE_REVISION_STATUS,
                historical_model_use_status=HISTORICAL_MODEL_USE_STATUS,
            )
        )
    return parsed, cursor, columns


def load_daily_history(
    identity: CnyrubSecurityIdentity,
    *,
    from_date: date,
    till_date: date,
    transport: HttpTransport = fetch_cnyrub_bytes_with_retry,
    clock: UtcClock = utc_now,
) -> list[CnyrubDailyCandle]:
    if (
        identity.security_id != SECURITY_ID
        or identity.board_id != BOARD_ID
        or identity.engine != ENGINE
        or identity.market != MARKET
        or not identity.primary_board
        or not identity.active_board
    ):
        raise CnyrubHistoryError(
            "security identity changed before candle retrieval",
            blocker="security_identity_not_reproducible",
        )
    candles: list[CnyrubDailyCandle] = []
    start = 0
    total: int | None = None
    schema: tuple[str, ...] | None = None
    while total is None or start < total:
        route = build_candle_url(from_date, till_date, start=start)
        try:
            payload = transport(route)
        except Exception as exc:
            raise CnyrubHistoryError(
                f"official daily candle request failed: {exc}",
                blocker="official_daily_candles_not_available",
            ) from exc
        page, cursor, page_schema = parse_candle_page_response(
            payload,
            from_date=from_date,
            till_date=till_date,
            start=start,
            route=route,
            retrieved_at_utc=_utc(clock()),
        )
        index, observed_total, page_size = cursor
        if index != start or (total is not None and observed_total != total):
            raise CnyrubHistoryError(
                "official candle pagination changed during retrieval",
                blocker="official_schema_not_stable",
            )
        if schema is not None and page_schema != schema:
            raise CnyrubHistoryError(
                "official candle schema changed during retrieval",
                blocker="official_schema_not_stable",
            )
        schema = page_schema
        total = observed_total
        candles.extend(page)
        start += page_size
    dates = [item.trade_date for item in candles]
    if len(dates) != len(set(dates)) or dates != sorted(dates):
        raise CnyrubHistoryError(
            "daily candle dates are duplicated or not chronological across pages",
            blocker="numerical_or_chronology_integrity_failure",
        )
    if total and not candles:
        raise CnyrubHistoryError(
            "official daily candle history is unavailable",
            blocker="official_daily_candles_not_available",
        )
    return candles


def validate_prior_session_candle(
    candle: CnyrubDailyCandle,
    *,
    target_trade_date: date,
    prior_trade_date: date,
) -> None:
    anchor = datetime.combine(
        target_trade_date,
        datetime.strptime("06:00:00", "%H:%M:%S").time(),
        tzinfo=MOSCOW,
    )
    if candle.trade_date != prior_trade_date or candle.candle_end >= anchor:
        raise CnyrubHistoryError(
            "CNYRUB candle violates the frozen prior-session 06:00 forecast anchor",
            blocker="point_in_time_cutoff_not_provable",
        )
