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

from .models import (
    ExternalDataError,
    HttpTransport,
    fetch_bytes,
    parse_json_object,
    raw_payload_sha256,
)

Sleeper = Callable[[float], None]
UtcClock = Callable[[], datetime]

SOURCE_ID: Final[str] = "moex_cnyrub_tom_daily"
SECURITY_ID: Final[str] = "CNYRUB_TOM"
BOARD_ID: Final[str] = "CETS"
ENGINE: Final[str] = "currency"
MARKET: Final[str] = "selt"
MOEX_ISS_HOST: Final[str] = "iss.moex.com"
MOSCOW: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")
SECURITY_METADATA_ROUTE: Final[str] = (
    "https://iss.moex.com/iss/securities/CNYRUB_TOM.json"
)
CANDLE_ROUTE: Final[str] = (
    "https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/"
    "securities/CNYRUB_TOM/candles.json"
)
SOURCE_REVISION_STATUS: Final[str] = "official_iss_current_revision"
HISTORICAL_MODEL_USE_STATUS: Final[str] = "source_validation_only"
TRANSIENT_HTTP_ERROR_MESSAGE: Final[str] = "external-data request failed"
CNYRUB_HTTP_MAX_ATTEMPTS: Final[int] = 5
CNYRUB_HTTP_RETRY_DELAYS_SECONDS: Final[tuple[float, ...]] = (0.5, 1.0, 2.0, 4.0)
CNYRUB_MAX_PAGES: Final[int] = 10_000
_SHA256: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_CANDLE_COLUMNS: Final[tuple[str, ...]] = (
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
    """Fail-closed MOEX CNY/RUB source, schema, identity, or PIT error."""

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


def _utc(value: datetime) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
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


def fetch_cnyrub_bytes_with_retry(
    url: str,
    *,
    transport: HttpTransport = fetch_bytes,
    sleeper: Sleeper = time.sleep,
) -> bytes:
    """Retry only the same exact route after the canonical transient error."""

    for attempt in range(CNYRUB_HTTP_MAX_ATTEMPTS):
        try:
            return transport(url)
        except ExternalDataError as exc:
            if exc.args != (TRANSIENT_HTTP_ERROR_MESSAGE,):
                raise
            if attempt + 1 == CNYRUB_HTTP_MAX_ATTEMPTS:
                raise ExternalDataError(
                    f"{TRANSIENT_HTTP_ERROR_MESSAGE}; official route={url}; "
                    f"attempts={CNYRUB_HTTP_MAX_ATTEMPTS}"
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
        raise CnyrubHistoryError(
            "route is not the exact allowlisted official MOEX ISS route",
            blocker="provenance_not_sufficient",
        )
    return dict(parse_qsl(parsed.query, keep_blank_values=True))


def _block(
    payload: bytes,
    name: str,
    required: Sequence[str],
) -> tuple[list[dict[str, object]], tuple[str, ...], str, Mapping[str, Any]]:
    try:
        root = parse_json_object(payload)
    except ExternalDataError as exc:
        raise CnyrubHistoryError(
            "official ISS response is not valid UTF-8 JSON",
            blocker="official_schema_not_stable",
        ) from exc

    value = root.get(name)
    if not isinstance(value, Mapping):
        raise CnyrubHistoryError(
            f"official ISS response lacks {name} block",
            blocker="official_schema_not_stable",
        )
    columns = value.get("columns")
    data = value.get("data")
    if not isinstance(columns, list) or not all(
        isinstance(item, str) for item in columns
    ):
        raise CnyrubHistoryError(
            f"official ISS {name} columns are malformed",
            blocker="official_schema_not_stable",
        )
    if not set(required).issubset(columns):
        raise CnyrubHistoryError(
            f"official ISS {name} schema is missing required columns",
            blocker="official_schema_not_stable",
        )
    if not isinstance(data, list):
        raise CnyrubHistoryError(
            f"official ISS {name} data is malformed",
            blocker="official_schema_not_stable",
        )

    rows: list[dict[str, object]] = []
    for raw in data:
        if not isinstance(raw, list) or len(raw) != len(columns):
            raise CnyrubHistoryError(
                f"official ISS {name} row width mismatch",
                blocker="official_schema_not_stable",
            )
        rows.append(dict(zip(columns, raw, strict=True)))

    digest = raw_payload_sha256(payload)
    if not _SHA256.fullmatch(digest):  # pragma: no cover - hashlib invariant
        raise CnyrubHistoryError(
            "official payload digest is invalid",
            blocker="provenance_not_sufficient",
        )
    return rows, tuple(columns), digest, root


def build_security_metadata_url() -> str:
    query = urlencode({"iss.meta": "off", "iss.only": "description,boards"})
    return f"{SECURITY_METADATA_ROUTE}?{query}"


def _optional_date(value: object, field: str) -> date | None:
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
    query = _official(route, "/iss/securities/CNYRUB_TOM.json")
    if query != {"iss.meta": "off", "iss.only": "description,boards"}:
        raise CnyrubHistoryError(
            "security metadata route parameters are not exact",
            blocker="provenance_not_sufficient",
        )

    description, _, digest, root = _block(
        payload,
        "description",
        ("name", "value"),
    )
    values: dict[str, object] = {}
    for row in description:
        key = str(row["name"] or "").strip()
        if not key or key in values:
            raise CnyrubHistoryError(
                "official security description has duplicate or empty field identity",
                blocker="official_schema_not_stable",
            )
        values[key] = row["value"]

    if str(values.get("SECID") or "").strip() != SECURITY_ID:
        raise CnyrubHistoryError(
            "official SECID does not match CNYRUB_TOM",
            blocker="security_identity_not_reproducible",
        )

    boards = root.get("boards")
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
    if not isinstance(boards, Mapping):
        raise CnyrubHistoryError(
            "official security boards block is absent",
            blocker="official_schema_not_stable",
        )
    columns = boards.get("columns")
    data = boards.get("data")
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
        rows = [dict(zip(columns, raw, strict=True)) for raw in data]
    except (TypeError, ValueError) as exc:
        raise CnyrubHistoryError(
            "official security board row width mismatch",
            blocker="official_schema_not_stable",
        ) from exc

    matching = [
        row
        for row in rows
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
    try:
        primary = int(board.get("is_primary") or 0) == 1
        active = int(board.get("is_traded") or 0) == 1
    except (TypeError, ValueError) as exc:
        raise CnyrubHistoryError(
            "official CETS board activity flags are malformed",
            blocker="official_schema_not_stable",
        ) from exc
    if not primary or not active:
        raise CnyrubHistoryError(
            "official CETS board is not both primary and active",
            blocker="security_identity_not_reproducible",
        )

    history_from = _optional_date(board.get("history_from"), "history_from")
    history_till = _optional_date(board.get("history_till"), "history_till")
    if history_from is None or (
        history_till is not None and history_till < history_from
    ):
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
        primary_board=primary,
        active_board=active,
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
    _official(route, "/iss/securities/CNYRUB_TOM.json")
    try:
        payload = transport(route)
    except CnyrubHistoryError:
        raise
    except ExternalDataError as exc:
        raise CnyrubHistoryError(
            f"official exact-security metadata transport failed: {exc}",
            blocker="security_identity_not_reproducible",
        ) from exc
    except Exception as exc:
        raise CnyrubHistoryError(
            f"official exact-security metadata transport failed: {exc}",
            blocker="security_identity_not_reproducible",
        ) from exc

    retrieved_at_utc = _utc(clock())
    return parse_security_metadata_response(
        payload,
        route=route,
        retrieved_at_utc=retrieved_at_utc,
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
            "iss.only": "candles",
            "candles.columns": ",".join(_CANDLE_COLUMNS),
        }
    )
    return f"{CANDLE_ROUTE}?{query}"


def _number(value: object, field: str, *, nonnegative: bool = False) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise CnyrubHistoryError(
            f"candle {field} is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        ) from exc
    if not math.isfinite(result) or (nonnegative and result < 0):
        condition = "finite and non-negative" if nonnegative else "finite"
        raise CnyrubHistoryError(
            f"candle {field} must be {condition}",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return result


def _timestamp(value: object, field: str) -> datetime:
    try:
        parsed = datetime.strptime(
            str(value or "").strip(),
            "%Y-%m-%d %H:%M:%S",
        )
    except ValueError as exc:
        raise CnyrubHistoryError(
            f"candle {field} timestamp is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        ) from exc
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
        "/iss/engines/currency/markets/selt/boards/CETS/"
        "securities/CNYRUB_TOM/candles.json",
    )
    expected = {
        "from": from_date.isoformat(),
        "till": till_date.isoformat(),
        "interval": "24",
        "start": str(start),
        "iss.meta": "off",
        "iss.only": "candles",
        "candles.columns": ",".join(_CANDLE_COLUMNS),
    }
    if query != expected:
        raise CnyrubHistoryError(
            "candle route does not pin the exact security, board, range, interval, and page",
            blocker="provenance_not_sufficient",
        )

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
        begin = _timestamp(row["begin"], "begin")
        end = _timestamp(row["end"], "end")
        trade_date = begin.date()

        if (
            end.date() != trade_date
            or end < begin
            or not from_date <= trade_date <= till_date
        ):
            raise CnyrubHistoryError(
                "daily candle timestamp or requested chronology is invalid",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if previous is not None and trade_date <= previous:
            raise CnyrubHistoryError(
                "daily candle trade dates are duplicated or not chronological",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if high < max(open_, close, low) or low > min(open_, close, high):
            raise CnyrubHistoryError(
                "daily candle OHLC values are inconsistent",
                blocker="numerical_or_chronology_integrity_failure",
            )

        result.append(
            CnyrubDailyCandle(
                source_id=SOURCE_ID,
                security_id=SECURITY_ID,
                board_id=BOARD_ID,
                engine=ENGINE,
                market=MARKET,
                trade_date=trade_date,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
                value=value,
                candle_begin=begin,
                candle_end=end,
                source_route=route,
                retrieved_at_utc=retrieved,
                raw_payload_sha256=digest,
                source_revision_status=SOURCE_REVISION_STATUS,
                historical_model_use_status=HISTORICAL_MODEL_USE_STATUS,
            )
        )
        previous = trade_date
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
        identity.security_id != SECURITY_ID
        or identity.board_id != BOARD_ID
        or identity.engine != ENGINE
        or identity.market != MARKET
    ):
        raise CnyrubHistoryError(
            "daily history identity differs from exact CNYRUB_TOM/CETS source",
            blocker="security_identity_not_reproducible",
        )

    rows: list[CnyrubDailyCandle] = []
    expected_columns: tuple[str, ...] | None = None
    start = 0
    for _page in range(CNYRUB_MAX_PAGES):
        route = build_candle_url(from_date, till_date, start=start)
        try:
            payload = transport(route)
        except CnyrubHistoryError:
            raise
        except ExternalDataError as exc:
            raise CnyrubHistoryError(
                f"official daily candle transport failed: {exc}",
                blocker="official_daily_candles_not_available",
            ) from exc
        except Exception as exc:
            raise CnyrubHistoryError(
                f"official daily candle transport failed: {exc}",
                blocker="official_daily_candles_not_available",
            ) from exc

        page_rows, columns = parse_candle_page_response(
            payload,
            from_date=from_date,
            till_date=till_date,
            start=start,
            route=route,
            retrieved_at_utc=_utc(clock()),
        )
        if expected_columns is None:
            expected_columns = columns
        elif columns != expected_columns:
            raise CnyrubHistoryError(
                "official daily candle schema changed during pagination",
                blocker="official_schema_not_stable",
            )

        if not page_rows:
            if not rows:
                raise CnyrubHistoryError(
                    "official CNYRUB_TOM daily candles are unavailable",
                    blocker="official_daily_candles_not_available",
                )
            break

        rows.extend(page_rows)
        start += len(page_rows)
    else:
        raise CnyrubHistoryError(
            "official daily candle pagination exceeded the bounded page limit",
            blocker="official_schema_not_stable",
        )

    dates = [item.trade_date for item in rows]
    if len(dates) != len(set(dates)) or dates != sorted(dates):
        raise CnyrubHistoryError(
            "daily candle dates are duplicated or not chronological across pages",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return rows


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
            "selected CNY/RUB candle violates exact prior-session forecast-anchor policy",
            blocker="point_in_time_cutoff_not_provable",
        )
