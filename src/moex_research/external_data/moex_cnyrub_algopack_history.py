from __future__ import annotations

import hashlib
import json
import math
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from http.client import HTTPResponse
from typing import Any, Final
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .models import ExternalDataError, parse_json_object, raw_payload_sha256
from .moex_cnyrub_history import (
    BOARD_ID,
    ENGINE,
    MARKET,
    SECURITY_ID,
    CnyrubSecurityIdentity,
    build_security_metadata_url,
    load_security_identity,
)

Sleeper = Callable[[float], None]
UtcClock = Callable[[], datetime]
AlgoPackTransport = Callable[[str, str], bytes]
TokenLoader = Callable[[], str]

SOURCE_ID: Final[str] = "moex_algopack_cnyrub_tom_tradestats_5m"
ALGOPACK_HOST: Final[str] = "apim.moex.com"
ALGOPACK_TOKEN_ENV: Final[str] = "MOEX_ALGOPACK_TOKEN"
ALGOPACK_TRADESTATS_ROUTE: Final[str] = (
    "https://apim.moex.com/iss/datashop/algopack/fx/tradestats/"
    "CNYRUB_TOM.json"
)
SOURCE_REVISION_STATUS: Final[str] = "algopack_fx_tradestats_5m"
HISTORICAL_MODEL_USE_STATUS: Final[str] = "source_validation_only"
TRANSIENT_HTTP_ERROR_MESSAGE: Final[str] = "external-data request failed"
ALGOPACK_HTTP_MAX_ATTEMPTS: Final[int] = 5
ALGOPACK_HTTP_RETRY_DELAYS_SECONDS: Final[tuple[float, ...]] = (
    0.5,
    1.0,
    2.0,
    4.0,
)
ALGOPACK_MAX_PAGES: Final[int] = 10_000
ALGOPACK_BUCKET_MINUTES: Final[int] = 5
MOSCOW: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")
_TRADESTAT_COLUMNS: Final[tuple[str, ...]] = (
    "tradedate",
    "tradetime",
    "secid",
    "pr_open",
    "pr_high",
    "pr_low",
    "pr_close",
    "vol",
    "val",
    "trades",
    "trades_b",
    "trades_s",
    "val_b",
    "val_s",
    "vol_b",
    "vol_s",
)
_CURSOR_COLUMNS: Final[tuple[str, ...]] = ("INDEX", "TOTAL", "PAGESIZE")


class CnyrubAlgoPackError(ValueError):
    """Fail-closed AlgoPack source, authorization, schema, or PIT error."""

    def __init__(
        self,
        message: str,
        *,
        blocker: str = "other_fail_closed_with_exact_reason",
    ) -> None:
        super().__init__(message)
        self.blocker = blocker


@dataclass(frozen=True)
class AlgoPackTradeStat:
    trade_date: date
    bucket_begin: datetime
    security_id: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    value: float
    trades: int
    trades_buy: int
    trades_sell: int
    value_buy: float
    value_sell: float
    volume_buy: float
    volume_sell: float


@dataclass(frozen=True)
class AlgoPackCursor:
    index: int
    total: int
    page_size: int


@dataclass(frozen=True)
class CnyrubAlgoPackDailyCandle:
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
    volume_buy: float
    volume_sell: float
    volume_imbalance: float
    value: float
    value_buy: float
    value_sell: float
    trades: int
    trades_buy: int
    trades_sell: int
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
        raise CnyrubAlgoPackError(
            "retrieval timestamp must be timezone-aware",
            blocker="provenance_not_sufficient",
        )
    if value.utcoffset() != timedelta(0):
        raise CnyrubAlgoPackError(
            "retrieval timestamp must be expressed in UTC",
            blocker="provenance_not_sufficient",
        )
    return value.astimezone(timezone.utc)


def load_algopack_token() -> str:
    token = os.environ.get(ALGOPACK_TOKEN_ENV, "").strip()
    if not token:
        raise CnyrubAlgoPackError(
            f"{ALGOPACK_TOKEN_ENV} is required for subscribed AlgoPack access",
            blocker="algopack_subscription_not_available",
        )
    return token


def fetch_algopack_bytes(
    url: str,
    bearer_token: str,
    *,
    opener: Callable[..., HTTPResponse] = urlopen,
) -> bytes:
    token = str(bearer_token).strip()
    if not token:
        raise CnyrubAlgoPackError(
            "AlgoPack bearer token is empty",
            blocker="algopack_subscription_not_available",
        )
    if not url.startswith("https://"):
        raise CnyrubAlgoPackError(
            "AlgoPack route must use HTTPS",
            blocker="provenance_not_sufficient",
        )
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "moex-robot-algopack-cnyrub/1.0",
        },
    )
    try:
        with opener(request, timeout=30) as response:
            payload = response.read()
    except HTTPError as exc:
        if exc.code in (401, 403):
            raise CnyrubAlgoPackError(
                "AlgoPack subscription authorization failed",
                blocker="algopack_authorization_failed",
            ) from exc
        raise ExternalDataError(TRANSIENT_HTTP_ERROR_MESSAGE) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ExternalDataError(TRANSIENT_HTTP_ERROR_MESSAGE) from exc
    if not payload:
        raise ExternalDataError("external-data response is empty")
    return payload


def fetch_algopack_bytes_with_retry(
    url: str,
    bearer_token: str,
    *,
    transport: AlgoPackTransport = fetch_algopack_bytes,
    sleeper: Sleeper = time.sleep,
) -> bytes:
    for attempt in range(ALGOPACK_HTTP_MAX_ATTEMPTS):
        try:
            return transport(url, bearer_token)
        except CnyrubAlgoPackError:
            raise
        except ExternalDataError as exc:
            if exc.args != (TRANSIENT_HTTP_ERROR_MESSAGE,):
                raise
            if attempt + 1 == ALGOPACK_HTTP_MAX_ATTEMPTS:
                raise CnyrubAlgoPackError(
                    f"{TRANSIENT_HTTP_ERROR_MESSAGE}; official route={url}; "
                    f"attempts={ALGOPACK_HTTP_MAX_ATTEMPTS}",
                    blocker="algopack_tradestats_not_available",
                ) from exc
            sleeper(ALGOPACK_HTTP_RETRY_DELAYS_SECONDS[attempt])
    raise AssertionError("unreachable retry state")


def build_tradestats_url(
    from_date: date,
    till_date: date,
    *,
    start: int = 0,
) -> str:
    if till_date < from_date or start < 0:
        raise CnyrubAlgoPackError("AlgoPack range or pagination start is invalid")
    return (
        f"{ALGOPACK_TRADESTATS_ROUTE}?"
        + urlencode(
            {
                "from": from_date.isoformat(),
                "till": till_date.isoformat(),
                "start": start,
            }
        )
    )


def _official_algopack_route(
    url: str,
    *,
    from_date: date,
    till_date: date,
    start: int,
) -> None:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != ALGOPACK_HOST
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.path
        != "/iss/datashop/algopack/fx/tradestats/CNYRUB_TOM.json"
    ):
        raise CnyrubAlgoPackError(
            "route is not the exact allowlisted subscribed AlgoPack route",
            blocker="provenance_not_sufficient",
        )
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    expected = {
        "from": from_date.isoformat(),
        "till": till_date.isoformat(),
        "start": str(start),
    }
    if query != expected:
        raise CnyrubAlgoPackError(
            "AlgoPack route does not pin exact ticker, range, and page",
            blocker="provenance_not_sufficient",
        )


def _block(
    root: Mapping[str, Any],
    name: str,
    required: Sequence[str],
) -> tuple[list[dict[str, object]], tuple[str, ...]]:
    block = root.get(name)
    if not isinstance(block, Mapping):
        raise CnyrubAlgoPackError(
            f"AlgoPack response lacks {name} block",
            blocker="algopack_schema_not_stable",
        )
    columns = block.get("columns")
    data = block.get("data")
    if not isinstance(columns, list) or not all(
        isinstance(item, str) for item in columns
    ):
        raise CnyrubAlgoPackError(
            f"AlgoPack {name} columns are malformed",
            blocker="algopack_schema_not_stable",
        )
    if not set(required).issubset(columns):
        raise CnyrubAlgoPackError(
            f"AlgoPack {name} schema is missing required columns",
            blocker="algopack_schema_not_stable",
        )
    if not isinstance(data, list):
        raise CnyrubAlgoPackError(
            f"AlgoPack {name} data is malformed",
            blocker="algopack_schema_not_stable",
        )
    rows: list[dict[str, object]] = []
    for raw in data:
        if not isinstance(raw, list) or len(raw) != len(columns):
            raise CnyrubAlgoPackError(
                f"AlgoPack {name} row width mismatch",
                blocker="algopack_schema_not_stable",
            )
        rows.append(dict(zip(columns, raw, strict=True)))
    return rows, tuple(columns)


def _number(
    value: object,
    field: str,
    *,
    nonnegative: bool = False,
) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise CnyrubAlgoPackError(
            f"AlgoPack {field} is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        ) from exc
    if not math.isfinite(result) or (nonnegative and result < 0):
        condition = "finite and non-negative" if nonnegative else "finite"
        raise CnyrubAlgoPackError(
            f"AlgoPack {field} must be {condition}",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return result


def _integer(value: object, field: str) -> int:
    number = _number(value, field, nonnegative=True)
    if not number.is_integer():
        raise CnyrubAlgoPackError(
            f"AlgoPack {field} must be an integer",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return int(number)


def _bucket_datetime(trade_date: object, trade_time: object) -> datetime:
    try:
        parsed_date = date.fromisoformat(str(trade_date).strip())
        parsed_time = datetime.strptime(
            str(trade_time).strip(),
            "%H:%M:%S",
        ).time()
    except ValueError as exc:
        raise CnyrubAlgoPackError(
            "AlgoPack trade date or time is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        ) from exc
    return datetime.combine(parsed_date, parsed_time, tzinfo=MOSCOW)


def _validate_directional_totals(row: Mapping[str, object]) -> None:
    volume = _number(row["vol"], "vol", nonnegative=True)
    volume_buy = _number(row["vol_b"], "vol_b", nonnegative=True)
    volume_sell = _number(row["vol_s"], "vol_s", nonnegative=True)
    if not math.isclose(volume, volume_buy + volume_sell, rel_tol=0.0, abs_tol=1e-9):
        raise CnyrubAlgoPackError(
            "AlgoPack volume does not equal buy plus sell volume",
            blocker="numerical_or_chronology_integrity_failure",
        )

    value = _number(row["val"], "val", nonnegative=True)
    value_buy = _number(row["val_b"], "val_b", nonnegative=True)
    value_sell = _number(row["val_s"], "val_s", nonnegative=True)
    if not math.isclose(value, value_buy + value_sell, rel_tol=1e-6, abs_tol=1.0):
        raise CnyrubAlgoPackError(
            "AlgoPack value does not equal buy plus sell value",
            blocker="numerical_or_chronology_integrity_failure",
        )

    trades = _integer(row["trades"], "trades")
    trades_buy = _integer(row["trades_b"], "trades_b")
    trades_sell = _integer(row["trades_s"], "trades_s")
    if trades != trades_buy + trades_sell:
        raise CnyrubAlgoPackError(
            "AlgoPack trades do not equal buy plus sell trades",
            blocker="numerical_or_chronology_integrity_failure",
        )


def parse_tradestats_page_response(
    payload: bytes,
    *,
    from_date: date,
    till_date: date,
    start: int,
    route: str,
    retrieved_at_utc: datetime,
) -> tuple[
    list[AlgoPackTradeStat],
    tuple[str, ...],
    AlgoPackCursor,
    str,
]:
    _official_algopack_route(
        route,
        from_date=from_date,
        till_date=till_date,
        start=start,
    )
    try:
        root = parse_json_object(payload)
    except ExternalDataError as exc:
        raise CnyrubAlgoPackError(
            "AlgoPack response is not valid UTF-8 JSON",
            blocker="algopack_schema_not_stable",
        ) from exc

    rows, columns = _block(root, "data", _TRADESTAT_COLUMNS)
    cursor_rows, _ = _block(root, "data.cursor", _CURSOR_COLUMNS)
    if len(cursor_rows) != 1:
        raise CnyrubAlgoPackError(
            "AlgoPack cursor must contain exactly one row",
            blocker="algopack_schema_not_stable",
        )
    cursor_row = cursor_rows[0]
    cursor = AlgoPackCursor(
        index=_integer(cursor_row["INDEX"], "cursor INDEX"),
        total=_integer(cursor_row["TOTAL"], "cursor TOTAL"),
        page_size=_integer(cursor_row["PAGESIZE"], "cursor PAGESIZE"),
    )
    if cursor.index != start or cursor.page_size <= 0 or cursor.total < 0:
        raise CnyrubAlgoPackError(
            "AlgoPack cursor is inconsistent with requested page",
            blocker="algopack_schema_not_stable",
        )

    _utc(retrieved_at_utc)
    result: list[AlgoPackTradeStat] = []
    previous: datetime | None = None
    for row in rows:
        if str(row["secid"]).strip() != SECURITY_ID:
            raise CnyrubAlgoPackError(
                "AlgoPack response contains a substituted security",
                blocker="security_identity_not_reproducible",
            )
        bucket = _bucket_datetime(row["tradedate"], row["tradetime"])
        if not from_date <= bucket.date() <= till_date:
            raise CnyrubAlgoPackError(
                "AlgoPack bucket is outside requested range",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if previous is not None and bucket <= previous:
            raise CnyrubAlgoPackError(
                "AlgoPack buckets are duplicated or not chronological",
                blocker="numerical_or_chronology_integrity_failure",
            )
        _validate_directional_totals(row)
        open_ = _number(row["pr_open"], "pr_open")
        high = _number(row["pr_high"], "pr_high")
        low = _number(row["pr_low"], "pr_low")
        close = _number(row["pr_close"], "pr_close")
        if high < max(open_, close, low) or low > min(open_, close, high):
            raise CnyrubAlgoPackError(
                "AlgoPack bucket OHLC values are inconsistent",
                blocker="numerical_or_chronology_integrity_failure",
            )
        result.append(
            AlgoPackTradeStat(
                trade_date=bucket.date(),
                bucket_begin=bucket,
                security_id=SECURITY_ID,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=_number(row["vol"], "vol", nonnegative=True),
                value=_number(row["val"], "val", nonnegative=True),
                trades=_integer(row["trades"], "trades"),
                trades_buy=_integer(row["trades_b"], "trades_b"),
                trades_sell=_integer(row["trades_s"], "trades_s"),
                value_buy=_number(row["val_b"], "val_b", nonnegative=True),
                value_sell=_number(row["val_s"], "val_s", nonnegative=True),
                volume_buy=_number(row["vol_b"], "vol_b", nonnegative=True),
                volume_sell=_number(row["vol_s"], "vol_s", nonnegative=True),
            )
        )
        previous = bucket
    return result, columns, cursor, raw_payload_sha256(payload)


def _collection_digest(page_digests: Sequence[str]) -> str:
    if not page_digests:
        raise CnyrubAlgoPackError(
            "AlgoPack payload digest collection is empty",
            blocker="provenance_not_sufficient",
        )
    encoded = "\n".join(page_digests).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def aggregate_daily_tradestats(
    rows: Sequence[AlgoPackTradeStat],
    *,
    source_route: str,
    retrieved_at_utc: datetime,
    raw_payload_sha256: str,
) -> list[CnyrubAlgoPackDailyCandle]:
    grouped: dict[date, list[AlgoPackTradeStat]] = {}
    for row in rows:
        grouped.setdefault(row.trade_date, []).append(row)
    result: list[CnyrubAlgoPackDailyCandle] = []
    for trade_date in sorted(grouped):
        buckets = grouped[trade_date]
        if buckets != sorted(buckets, key=lambda item: item.bucket_begin):
            raise CnyrubAlgoPackError(
                "AlgoPack daily buckets are not chronological",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if len({item.bucket_begin for item in buckets}) != len(buckets):
            raise CnyrubAlgoPackError(
                "AlgoPack daily buckets are duplicated",
                blocker="numerical_or_chronology_integrity_failure",
            )
        volume = sum(item.volume for item in buckets)
        volume_buy = sum(item.volume_buy for item in buckets)
        volume_sell = sum(item.volume_sell for item in buckets)
        value = sum(item.value for item in buckets)
        value_buy = sum(item.value_buy for item in buckets)
        value_sell = sum(item.value_sell for item in buckets)
        trades = sum(item.trades for item in buckets)
        trades_buy = sum(item.trades_buy for item in buckets)
        trades_sell = sum(item.trades_sell for item in buckets)
        if not math.isclose(volume, volume_buy + volume_sell, rel_tol=0.0, abs_tol=1e-9):
            raise CnyrubAlgoPackError(
                "daily volume does not equal buy plus sell volume",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if not math.isclose(value, value_buy + value_sell, rel_tol=1e-6, abs_tol=1.0):
            raise CnyrubAlgoPackError(
                "daily value does not equal buy plus sell value",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if trades != trades_buy + trades_sell:
            raise CnyrubAlgoPackError(
                "daily trades do not equal buy plus sell trades",
                blocker="numerical_or_chronology_integrity_failure",
            )
        imbalance = 0.0 if volume == 0 else (volume_buy - volume_sell) / volume
        result.append(
            CnyrubAlgoPackDailyCandle(
                source_id=SOURCE_ID,
                security_id=SECURITY_ID,
                board_id=BOARD_ID,
                engine=ENGINE,
                market=MARKET,
                trade_date=trade_date,
                open=buckets[0].open,
                high=max(item.high for item in buckets),
                low=min(item.low for item in buckets),
                close=buckets[-1].close,
                volume=volume,
                volume_buy=volume_buy,
                volume_sell=volume_sell,
                volume_imbalance=imbalance,
                value=value,
                value_buy=value_buy,
                value_sell=value_sell,
                trades=trades,
                trades_buy=trades_buy,
                trades_sell=trades_sell,
                candle_begin=buckets[0].bucket_begin,
                candle_end=buckets[-1].bucket_begin
                + timedelta(minutes=ALGOPACK_BUCKET_MINUTES),
                source_route=source_route,
                retrieved_at_utc=_utc(retrieved_at_utc),
                raw_payload_sha256=raw_payload_sha256,
                source_revision_status=SOURCE_REVISION_STATUS,
                historical_model_use_status=HISTORICAL_MODEL_USE_STATUS,
            )
        )
    return result


def load_daily_history(
    identity: CnyrubSecurityIdentity,
    *,
    from_date: date,
    till_date: date,
    bearer_token: str | None = None,
    transport: AlgoPackTransport = fetch_algopack_bytes,
    token_loader: TokenLoader = load_algopack_token,
    sleeper: Sleeper = time.sleep,
    clock: UtcClock = utc_now,
) -> list[CnyrubAlgoPackDailyCandle]:
    if (
        identity.security_id != SECURITY_ID
        or identity.board_id != BOARD_ID
        or identity.engine != ENGINE
        or identity.market != MARKET
    ):
        raise CnyrubAlgoPackError(
            "AlgoPack history identity differs from exact CNYRUB_TOM/CETS source",
            blocker="security_identity_not_reproducible",
        )
    token = str(bearer_token).strip() if bearer_token is not None else token_loader()
    if not token:
        raise CnyrubAlgoPackError(
            "AlgoPack bearer token is empty",
            blocker="algopack_subscription_not_available",
        )

    all_rows: list[AlgoPackTradeStat] = []
    page_digests: list[str] = []
    expected_columns: tuple[str, ...] | None = None
    expected_total: int | None = None
    start = 0
    retrieved = _utc(clock())
    for _page in range(ALGOPACK_MAX_PAGES):
        route = build_tradestats_url(from_date, till_date, start=start)
        payload = fetch_algopack_bytes_with_retry(
            route,
            token,
            transport=transport,
            sleeper=sleeper,
        )
        page_rows, columns, cursor, digest = parse_tradestats_page_response(
            payload,
            from_date=from_date,
            till_date=till_date,
            start=start,
            route=route,
            retrieved_at_utc=retrieved,
        )
        if expected_columns is None:
            expected_columns = columns
        elif columns != expected_columns:
            raise CnyrubAlgoPackError(
                "AlgoPack schema changed during pagination",
                blocker="algopack_schema_not_stable",
            )
        if expected_total is None:
            expected_total = cursor.total
        elif cursor.total != expected_total:
            raise CnyrubAlgoPackError(
                "AlgoPack cursor total changed during pagination",
                blocker="algopack_schema_not_stable",
            )
        page_digests.append(digest)
        if not page_rows:
            if start < cursor.total:
                raise CnyrubAlgoPackError(
                    "AlgoPack pagination stopped before cursor total",
                    blocker="algopack_schema_not_stable",
                )
            break
        all_rows.extend(page_rows)
        start += len(page_rows)
        if start >= cursor.total:
            break
    else:
        raise CnyrubAlgoPackError(
            "AlgoPack pagination exceeded the bounded page limit",
            blocker="algopack_schema_not_stable",
        )

    if not all_rows:
        raise CnyrubAlgoPackError(
            "subscribed AlgoPack CNYRUB_TOM tradestats are unavailable",
            blocker="algopack_tradestats_not_available",
        )
    timestamps = [row.bucket_begin for row in all_rows]
    if timestamps != sorted(timestamps) or len(timestamps) != len(set(timestamps)):
        raise CnyrubAlgoPackError(
            "AlgoPack buckets are duplicated or not chronological across pages",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return aggregate_daily_tradestats(
        all_rows,
        source_route=build_tradestats_url(from_date, till_date, start=0),
        retrieved_at_utc=retrieved,
        raw_payload_sha256=_collection_digest(page_digests),
    )


def validate_prior_session_candle(
    candle: CnyrubAlgoPackDailyCandle,
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
        raise CnyrubAlgoPackError(
            "selected AlgoPack CNY/RUB aggregate violates exact prior-session "
            "forecast-anchor policy",
            blocker="point_in_time_cutoff_not_provable",
        )


__all__ = [
    "ALGOPACK_BUCKET_MINUTES",
    "ALGOPACK_HOST",
    "ALGOPACK_HTTP_MAX_ATTEMPTS",
    "ALGOPACK_HTTP_RETRY_DELAYS_SECONDS",
    "ALGOPACK_TOKEN_ENV",
    "ALGOPACK_TRADESTATS_ROUTE",
    "AlgoPackCursor",
    "AlgoPackTradeStat",
    "AlgoPackTransport",
    "BOARD_ID",
    "CnyrubAlgoPackDailyCandle",
    "CnyrubAlgoPackError",
    "CnyrubSecurityIdentity",
    "ENGINE",
    "HISTORICAL_MODEL_USE_STATUS",
    "MARKET",
    "SECURITY_ID",
    "SOURCE_ID",
    "SOURCE_REVISION_STATUS",
    "TRANSIENT_HTTP_ERROR_MESSAGE",
    "TokenLoader",
    "UtcClock",
    "aggregate_daily_tradestats",
    "build_security_metadata_url",
    "build_tradestats_url",
    "fetch_algopack_bytes",
    "fetch_algopack_bytes_with_retry",
    "load_algopack_token",
    "load_daily_history",
    "load_security_identity",
    "parse_tradestats_page_response",
    "utc_now",
    "validate_prior_session_candle",
]
