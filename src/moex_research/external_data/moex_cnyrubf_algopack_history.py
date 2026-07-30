from __future__ import annotations

import hashlib
import math
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from email.message import Message
from http.client import HTTPResponse
from typing import Any, Final
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from zoneinfo import ZoneInfo

from .models import ExternalDataError, parse_json_object, raw_payload_sha256


IssTransport = Callable[[str], bytes]
AlgoPackTransport = Callable[[str, str], bytes]
TokenLoader = Callable[[], str]
Sleeper = Callable[[float], None]
UtcClock = Callable[[], datetime]
HttpOpener = Callable[..., HTTPResponse]

SOURCE_ID: Final[str] = "moex_algopack_cnyrubf_fo_tradestats_5m"
SECURITY_ID: Final[str] = "CNYRUBF"
ASSET_CODE: Final[str] = "CNYRUBTOM"
BOARD_ID: Final[str] = "RFUD"
ENGINE: Final[str] = "futures"
MARKET: Final[str] = "forts"
ALGOPACK_MARKET_CODE: Final[str] = "FO"

MOEX_ISS_HOST: Final[str] = "iss.moex.com"
ALGOPACK_HOST: Final[str] = "apim.moex.com"
ALGOPACK_TOKEN_ENV: Final[str] = "MOEX_ALGOPACK_TOKEN"

SECURITY_METADATA_PATH: Final[str] = "/iss/securities/CNYRUBF.json"
SECURITY_METADATA_ROUTE: Final[str] = f"https://{MOEX_ISS_HOST}{SECURITY_METADATA_PATH}"
ALGOPACK_TRADESTATS_PATH: Final[str] = (
    "/iss/datashop/algopack/fo/tradestats/CNYRUBF.json"
)
ALGOPACK_TRADESTATS_ROUTE: Final[str] = (
    f"https://{ALGOPACK_HOST}{ALGOPACK_TRADESTATS_PATH}"
)

SOURCE_REVISION_STATUS: Final[str] = "algopack_fo_tradestats_5m"
HISTORICAL_MODEL_USE_STATUS: Final[str] = "source_validation_only"
ALGOPACK_HTTP_MAX_ATTEMPTS: Final[int] = 5
ALGOPACK_HTTP_RETRY_DELAYS_SECONDS: Final[tuple[float, ...]] = (
    0.5,
    1.0,
    2.0,
    4.0,
)
ALGOPACK_MAX_RETRY_AFTER_SECONDS: Final[float] = 60.0
ALGOPACK_MAX_PAGES: Final[int] = 10_000
ALGOPACK_BUCKET_MINUTES: Final[int] = 5
MOSCOW: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")

_TRADESTAT_COLUMNS: Final[tuple[str, ...]] = (
    "tradedate",
    "tradetime",
    "secid",
    "asset_code",
    "pr_open",
    "pr_high",
    "pr_low",
    "pr_close",
    "pr_std",
    "vol",
    "val",
    "trades",
    "pr_vwap",
    "pr_change",
    "trades_b",
    "trades_s",
    "val_b",
    "val_s",
    "vol_b",
    "vol_s",
    "disb",
    "pr_vwap_b",
    "pr_vwap_s",
    "im",
    "oi_open",
    "oi_high",
    "oi_low",
    "oi_close",
    "sec_pr_open",
    "sec_pr_high",
    "sec_pr_low",
    "sec_pr_close",
    "SYSTIME",
)
_CURSOR_COLUMNS: Final[tuple[str, ...]] = ("INDEX", "TOTAL", "PAGESIZE")


class CnyrubfAlgoPackError(ValueError):
    """Fail-closed CNYRUBF source, identity, schema, authorization, or PIT error."""

    def __init__(
        self,
        message: str,
        *,
        blocker: str = "other_fail_closed_with_exact_reason",
        retryable: bool = False,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.blocker = blocker
        self.retryable = bool(retryable)
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True)
class CnyrubfSecurityIdentity:
    source_id: str
    security_id: str
    asset_code: str
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
class AlgoPackCursor:
    index: int
    total: int
    page_size: int


@dataclass(frozen=True)
class AlgoPackTradeStat:
    trade_date: date
    bucket_begin: datetime
    bucket_end: datetime
    source_available_at: datetime
    security_id: str
    asset_code: str
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
    initial_margin: float | None
    open_interest_open: float
    open_interest_high: float
    open_interest_low: float
    open_interest_close: float


@dataclass(frozen=True)
class CnyrubfAlgoPackDailyCandle:
    source_id: str
    security_id: str
    asset_code: str
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
    initial_margin_close: float | None
    open_interest_open: float
    open_interest_high: float
    open_interest_low: float
    open_interest_close: float
    candle_begin: datetime
    candle_end: datetime
    source_available_at: datetime
    source_route: str
    retrieved_at_utc: datetime
    raw_payload_sha256: str
    source_revision_status: str
    historical_model_use_status: str

    def as_record(self) -> dict[str, object]:
        return asdict(self)


class _RejectAllRedirects(HTTPRedirectHandler):
    """Reject every redirect, including redirects carrying Authorization."""

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


def _open_without_redirects(request: Request, timeout: int) -> HTTPResponse:
    return build_opener(_RejectAllRedirects()).open(request, timeout=timeout)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc(value: datetime) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise CnyrubfAlgoPackError(
            "retrieval timestamp must be timezone-aware",
            blocker="provenance_not_sufficient",
        )
    if value.utcoffset() != timedelta(0):
        raise CnyrubfAlgoPackError(
            "retrieval timestamp must be expressed in UTC",
            blocker="provenance_not_sufficient",
        )
    return value.astimezone(timezone.utc)


def load_algopack_token() -> str:
    token = os.environ.get(ALGOPACK_TOKEN_ENV, "").strip()
    if not token:
        raise CnyrubfAlgoPackError(
            "AlgoPack token environment variable is not configured",
            blocker="token_env_not_configured",
        )
    return token


def build_security_metadata_url() -> str:
    return SECURITY_METADATA_ROUTE + "?" + urlencode(
        {"iss.meta": "off", "iss.only": "description,boards"}
    )


def build_tradestats_url(
    from_date: date,
    till_date: date,
    *,
    start: int = 0,
) -> str:
    if till_date < from_date or start < 0:
        raise CnyrubfAlgoPackError(
            "AlgoPack range or pagination start is invalid",
            blocker="provenance_not_sufficient",
        )
    return ALGOPACK_TRADESTATS_ROUTE + "?" + urlencode(
        {
            "from": from_date.isoformat(),
            "till": till_date.isoformat(),
            "start": start,
        }
    )


def _exact_metadata_route(url: str) -> None:
    parsed = urlsplit(str(url))
    if (
        parsed.scheme != "https"
        or parsed.hostname != MOEX_ISS_HOST
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.path != SECURITY_METADATA_PATH
        or parsed.fragment
    ):
        raise CnyrubfAlgoPackError(
            "route is not the exact allowlisted official MOEX ISS metadata route",
            blocker="provenance_not_sufficient",
        )
    try:
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise CnyrubfAlgoPackError(
            "security metadata route query is malformed",
            blocker="provenance_not_sufficient",
        ) from exc
    if pairs != [("iss.meta", "off"), ("iss.only", "description,boards")]:
        raise CnyrubfAlgoPackError(
            "security metadata route query is not exact",
            blocker="provenance_not_sufficient",
        )
    if str(url) != build_security_metadata_url():
        raise CnyrubfAlgoPackError(
            "security metadata route is not canonical",
            blocker="provenance_not_sufficient",
        )


def _exact_tradestats_route_values(url: str) -> tuple[date, date, int]:
    parsed = urlsplit(str(url))
    if (
        parsed.scheme != "https"
        or parsed.hostname != ALGOPACK_HOST
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.path != ALGOPACK_TRADESTATS_PATH
        or parsed.fragment
    ):
        raise CnyrubfAlgoPackError(
            "route is not the exact allowlisted subscribed AlgoPack CNYRUBF route",
            blocker="provenance_not_sufficient",
        )
    try:
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise CnyrubfAlgoPackError(
            "AlgoPack route query is malformed",
            blocker="provenance_not_sufficient",
        ) from exc
    if len(pairs) != 3 or len({key for key, _ in pairs}) != 3:
        raise CnyrubfAlgoPackError(
            "AlgoPack route query is not exact",
            blocker="provenance_not_sufficient",
        )
    query = dict(pairs)
    if set(query) != {"from", "till", "start"}:
        raise CnyrubfAlgoPackError(
            "AlgoPack route query is not exact",
            blocker="provenance_not_sufficient",
        )
    try:
        from_date = date.fromisoformat(query["from"])
        till_date = date.fromisoformat(query["till"])
        start = int(query["start"])
    except (TypeError, ValueError) as exc:
        raise CnyrubfAlgoPackError(
            "AlgoPack route query values are invalid",
            blocker="provenance_not_sufficient",
        ) from exc
    if start < 0 or till_date < from_date or str(start) != query["start"]:
        raise CnyrubfAlgoPackError(
            "AlgoPack route query values are invalid",
            blocker="provenance_not_sufficient",
        )
    if str(url) != build_tradestats_url(from_date, till_date, start=start):
        raise CnyrubfAlgoPackError(
            "AlgoPack route query is not canonical",
            blocker="provenance_not_sufficient",
        )
    return from_date, till_date, start


def _bounded_retry_after(headers: Mapping[str, Any] | Message | None) -> float | None:
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(str(value).strip())
    except ValueError:
        return None
    if (
        not math.isfinite(seconds)
        or seconds < 0
        or seconds > ALGOPACK_MAX_RETRY_AFTER_SECONDS
    ):
        return None
    return seconds


def _not_found_blocker(headers: Mapping[str, Any] | Message | None) -> str:
    marker = ""
    if headers is not None:
        marker = " ".join(
            str(headers.get(name, ""))
            for name in ("X-MOEX-Error-Code", "X-Error-Code", "X-Route-Status")
        ).lower()
    if any(word in marker for word in ("ticker", "security", "instrument", "cnyrubf")):
        return "cnyrubf_not_available"
    return "official_route_not_reproducible"


def fetch_iss_bytes(
    url: str,
    *,
    opener: HttpOpener = _open_without_redirects,
) -> bytes:
    _exact_metadata_route(url)
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "moex-robot-cnyrubf-metadata/1.0",
        },
    )
    try:
        with opener(request, timeout=30) as response:
            payload = response.read()
    except HTTPError as exc:
        code = int(exc.code)
        if 300 <= code < 400:
            raise CnyrubfAlgoPackError(
                "MOEX ISS metadata redirect was refused",
                blocker="provenance_not_sufficient",
            ) from None
        if code == 404:
            raise CnyrubfAlgoPackError(
                "official CNYRUBF metadata route is not reproducible",
                blocker="official_route_not_reproducible",
            ) from None
        if code == 429 or 500 <= code <= 599:
            raise CnyrubfAlgoPackError(
                "MOEX ISS metadata service is temporarily unavailable",
                blocker="official_schema_not_stable",
                retryable=True,
                retry_after_seconds=_bounded_retry_after(exc.headers),
            ) from None
        raise CnyrubfAlgoPackError(
            "MOEX ISS metadata HTTP response is not accepted",
            blocker="official_schema_not_stable",
        ) from None
    except (URLError, TimeoutError, OSError):
        raise CnyrubfAlgoPackError(
            "MOEX ISS metadata transport is temporarily unavailable",
            blocker="official_schema_not_stable",
            retryable=True,
        ) from None
    if not payload:
        raise CnyrubfAlgoPackError(
            "MOEX ISS metadata response is empty",
            blocker="official_schema_not_stable",
            retryable=True,
        )
    return payload


def fetch_algopack_bytes(
    url: str,
    bearer_token: str,
    *,
    opener: HttpOpener = _open_without_redirects,
) -> bytes:
    _exact_tradestats_route_values(url)
    token = str(bearer_token).strip()
    if not token:
        raise CnyrubfAlgoPackError(
            "AlgoPack token environment variable is not configured",
            blocker="token_env_not_configured",
        )
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "moex-robot-algopack-cnyrubf/1.0",
        },
    )
    try:
        with opener(request, timeout=30) as response:
            payload = response.read()
    except HTTPError as exc:
        code = int(exc.code)
        if 300 <= code < 400:
            raise CnyrubfAlgoPackError(
                "AlgoPack redirect was refused",
                blocker="provenance_not_sufficient",
            ) from None
        if code == 401:
            raise CnyrubfAlgoPackError(
                "AlgoPack authentication failed",
                blocker="algopack_authentication_failed",
            ) from None
        if code == 403:
            raise CnyrubfAlgoPackError(
                "AlgoPack subscription is not entitled for this dataset",
                blocker="algopack_subscription_not_entitled",
            ) from None
        if code == 404:
            blocker = _not_found_blocker(exc.headers)
            message = (
                "CNYRUBF AlgoPack data is not available"
                if blocker == "cnyrubf_not_available"
                else "official AlgoPack route is not reproducible"
            )
            raise CnyrubfAlgoPackError(message, blocker=blocker) from None
        if code == 429:
            raise CnyrubfAlgoPackError(
                "AlgoPack rate limit blocked the request",
                blocker="algopack_rate_limit_blocked",
                retryable=True,
                retry_after_seconds=_bounded_retry_after(exc.headers),
            ) from None
        if 500 <= code <= 599:
            raise CnyrubfAlgoPackError(
                "AlgoPack TradeStats service is temporarily unavailable",
                blocker="algopack_tradestats_not_available",
                retryable=True,
            ) from None
        raise CnyrubfAlgoPackError(
            "AlgoPack HTTP response is not accepted",
            blocker="algopack_tradestats_not_available",
        ) from None
    except (URLError, TimeoutError, OSError):
        raise CnyrubfAlgoPackError(
            "AlgoPack TradeStats transport is temporarily unavailable",
            blocker="algopack_tradestats_not_available",
            retryable=True,
        ) from None
    if not payload:
        raise CnyrubfAlgoPackError(
            "AlgoPack TradeStats response is empty",
            blocker="algopack_tradestats_not_available",
            retryable=True,
        )
    return payload


def _retry_bytes(
    call: Callable[[], bytes],
    *,
    sleeper: Sleeper,
) -> bytes:
    for attempt in range(ALGOPACK_HTTP_MAX_ATTEMPTS):
        try:
            return call()
        except CnyrubfAlgoPackError as exc:
            error = exc
        except ExternalDataError as exc:
            error = CnyrubfAlgoPackError(
                "external source transport is temporarily unavailable",
                blocker="algopack_tradestats_not_available",
                retryable=True,
            )
            error.__cause__ = exc
        if not error.retryable:
            raise error
        if attempt + 1 == ALGOPACK_HTTP_MAX_ATTEMPTS:
            raise CnyrubfAlgoPackError(
                "external source remained unavailable after bounded retries",
                blocker=error.blocker,
            ) from None
        delay = error.retry_after_seconds
        if delay is None:
            delay = ALGOPACK_HTTP_RETRY_DELAYS_SECONDS[attempt]
        sleeper(delay)
    raise AssertionError("unreachable retry state")


def fetch_iss_bytes_with_retry(
    url: str,
    *,
    transport: IssTransport = fetch_iss_bytes,
    sleeper: Sleeper = time.sleep,
) -> bytes:
    _exact_metadata_route(url)
    return _retry_bytes(lambda: transport(url), sleeper=sleeper)


def fetch_algopack_bytes_with_retry(
    url: str,
    bearer_token: str,
    *,
    transport: AlgoPackTransport = fetch_algopack_bytes,
    sleeper: Sleeper = time.sleep,
) -> bytes:
    _exact_tradestats_route_values(url)
    if not str(bearer_token).strip():
        raise CnyrubfAlgoPackError(
            "AlgoPack token environment variable is not configured",
            blocker="token_env_not_configured",
        )
    return _retry_bytes(lambda: transport(url, bearer_token), sleeper=sleeper)


def _block(
    root: Mapping[str, Any],
    name: str,
    required: Sequence[str],
    *,
    schema_blocker: str,
) -> tuple[list[dict[str, object]], tuple[str, ...]]:
    block = root.get(name)
    if not isinstance(block, Mapping):
        raise CnyrubfAlgoPackError(
            f"response lacks {name} block",
            blocker=schema_blocker,
        )
    columns = block.get("columns")
    data = block.get("data")
    if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
        raise CnyrubfAlgoPackError(
            f"{name} columns are malformed",
            blocker=schema_blocker,
        )
    if not set(required).issubset(columns):
        if name == "data" and "SYSTIME" not in columns:
            raise CnyrubfAlgoPackError(
                "AlgoPack provider availability timestamp SYSTIME is missing",
                blocker="point_in_time_cutoff_not_provable",
            )
        raise CnyrubfAlgoPackError(
            f"{name} schema is missing required columns",
            blocker=schema_blocker,
        )
    if not isinstance(data, list):
        raise CnyrubfAlgoPackError(
            f"{name} data is malformed",
            blocker=schema_blocker,
        )
    rows: list[dict[str, object]] = []
    for raw in data:
        if not isinstance(raw, list) or len(raw) != len(columns):
            raise CnyrubfAlgoPackError(
                f"{name} row width mismatch",
                blocker=schema_blocker,
            )
        rows.append(dict(zip(columns, raw, strict=True)))
    return rows, tuple(columns)


def _optional_date(value: object, field: str) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise CnyrubfAlgoPackError(
            f"official security metadata {field} is invalid",
            blocker="official_schema_not_stable",
        ) from exc


def parse_security_metadata_response(
    payload: bytes,
    *,
    route: str,
    retrieved_at_utc: datetime,
) -> CnyrubfSecurityIdentity:
    _exact_metadata_route(route)
    try:
        root = parse_json_object(payload)
    except ExternalDataError as exc:
        raise CnyrubfAlgoPackError(
            "official ISS response is not valid UTF-8 JSON",
            blocker="official_schema_not_stable",
        ) from exc

    description, _ = _block(
        root,
        "description",
        ("name", "value"),
        schema_blocker="official_schema_not_stable",
    )
    values: dict[str, object] = {}
    for row in description:
        key = str(row["name"] or "").strip()
        if not key or key in values:
            raise CnyrubfAlgoPackError(
                "official security description has duplicate or empty field identity",
                blocker="official_schema_not_stable",
            )
        values[key] = row["value"]
    if str(values.get("SECID") or "").strip() != SECURITY_ID:
        raise CnyrubfAlgoPackError(
            "official SECID does not match CNYRUBF",
            blocker="security_identity_not_reproducible",
        )

    board_rows, _ = _block(
        root,
        "boards",
        (
            "secid",
            "boardid",
            "engine",
            "market",
            "is_traded",
            "is_primary",
            "history_from",
            "history_till",
        ),
        schema_blocker="official_schema_not_stable",
    )
    matching = [
        row
        for row in board_rows
        if str(row.get("secid") or "").strip() == SECURITY_ID
        and str(row.get("boardid") or "").strip() == BOARD_ID
        and str(row.get("engine") or "").strip() == ENGINE
        and str(row.get("market") or "").strip() == MARKET
    ]
    if len(matching) != 1:
        raise CnyrubfAlgoPackError(
            "CNYRUBF does not resolve to one official futures/forts/RFUD identity",
            blocker="security_identity_not_reproducible",
        )
    board = matching[0]
    try:
        primary = int(board.get("is_primary") or 0) == 1
        active = int(board.get("is_traded") or 0) == 1
    except (TypeError, ValueError) as exc:
        raise CnyrubfAlgoPackError(
            "official RFUD board activity flags are malformed",
            blocker="official_schema_not_stable",
        ) from exc
    if not primary or not active:
        raise CnyrubfAlgoPackError(
            "official RFUD board is not both primary and active",
            blocker="security_identity_not_reproducible",
        )
    history_from = _optional_date(board.get("history_from"), "history_from")
    history_till = _optional_date(board.get("history_till"), "history_till")
    if history_from is None or (
        history_till is not None and history_till < history_from
    ):
        raise CnyrubfAlgoPackError(
            "official security history interval is invalid",
            blocker="security_identity_not_reproducible",
        )
    retrieved = _utc(retrieved_at_utc)
    return CnyrubfSecurityIdentity(
        source_id=SOURCE_ID,
        security_id=SECURITY_ID,
        asset_code=ASSET_CODE,
        board_id=BOARD_ID,
        engine=ENGINE,
        market=MARKET,
        primary_board=primary,
        active_board=active,
        history_from=history_from,
        history_till=history_till,
        metadata_route=route,
        retrieved_at_utc=retrieved,
        raw_payload_sha256=raw_payload_sha256(payload),
        source_revision_status="official_iss_current_revision",
        historical_model_use_status=HISTORICAL_MODEL_USE_STATUS,
    )


def load_security_identity(
    *,
    transport: IssTransport = fetch_iss_bytes,
    sleeper: Sleeper = time.sleep,
    clock: UtcClock = utc_now,
) -> CnyrubfSecurityIdentity:
    route = build_security_metadata_url()
    payload = fetch_iss_bytes_with_retry(route, transport=transport, sleeper=sleeper)
    return parse_security_metadata_response(
        payload,
        route=route,
        retrieved_at_utc=clock(),
    )


def _number(value: object, field: str, *, nonnegative: bool = False) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise CnyrubfAlgoPackError(
            f"AlgoPack {field} is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        ) from exc
    if not math.isfinite(result) or (nonnegative and result < 0):
        condition = "finite and non-negative" if nonnegative else "finite"
        raise CnyrubfAlgoPackError(
            f"AlgoPack {field} must be {condition}",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return result


def _optional_initial_margin(value: object) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise CnyrubfAlgoPackError(
            "AlgoPack im is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return _number(value, "im", nonnegative=True)


def _integer(value: object, field: str) -> int:
    number = _number(value, field, nonnegative=True)
    if not number.is_integer():
        raise CnyrubfAlgoPackError(
            f"AlgoPack {field} must be an integer",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return int(number)


def _provider_datetime(trade_date: object, trade_time: object) -> datetime:
    try:
        parsed_date = date.fromisoformat(str(trade_date).strip())
        parsed_time = datetime.strptime(
            str(trade_time).strip(),
            "%H:%M:%S",
        ).time()
    except ValueError as exc:
        raise CnyrubfAlgoPackError(
            "AlgoPack trade date or time is malformed",
            blocker="numerical_or_chronology_integrity_failure",
        ) from exc
    value = datetime.combine(parsed_date, parsed_time, tzinfo=MOSCOW)
    if (
        value.second != 0
        or value.microsecond != 0
        or value.minute % ALGOPACK_BUCKET_MINUTES
    ):
        raise CnyrubfAlgoPackError(
            "AlgoPack bucket end is not on the exact five-minute grid",
            blocker="numerical_or_chronology_integrity_failure",
        )
    return value


def _provider_timestamp(value: object) -> datetime:
    try:
        parsed = datetime.strptime(
            str(value or "").strip(),
            "%Y-%m-%d %H:%M:%S",
        )
    except ValueError as exc:
        raise CnyrubfAlgoPackError(
            "AlgoPack provider availability timestamp SYSTIME is malformed",
            blocker="point_in_time_cutoff_not_provable",
        ) from exc
    return parsed.replace(tzinfo=MOSCOW)


def _validate_directional_totals(row: Mapping[str, object]) -> None:
    volume = _number(row["vol"], "vol", nonnegative=True)
    volume_buy = _number(row["vol_b"], "vol_b", nonnegative=True)
    volume_sell = _number(row["vol_s"], "vol_s", nonnegative=True)
    if not math.isclose(
        volume,
        volume_buy + volume_sell,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        raise CnyrubfAlgoPackError(
            "AlgoPack volume does not equal buy plus sell volume",
            blocker="numerical_or_chronology_integrity_failure",
        )
    value = _number(row["val"], "val", nonnegative=True)
    value_buy = _number(row["val_b"], "val_b", nonnegative=True)
    value_sell = _number(row["val_s"], "val_s", nonnegative=True)
    if not math.isclose(
        value,
        value_buy + value_sell,
        rel_tol=1e-6,
        abs_tol=1.0,
    ):
        raise CnyrubfAlgoPackError(
            "AlgoPack value does not equal buy plus sell value",
            blocker="numerical_or_chronology_integrity_failure",
        )
    trades = _integer(row["trades"], "trades")
    trades_buy = _integer(row["trades_b"], "trades_b")
    trades_sell = _integer(row["trades_s"], "trades_s")
    if trades != trades_buy + trades_sell:
        raise CnyrubfAlgoPackError(
            "AlgoPack trades do not equal buy plus sell trades",
            blocker="numerical_or_chronology_integrity_failure",
        )


def _validate_ohlc(
    open_: float,
    high: float,
    low: float,
    close: float,
    *,
    name: str,
) -> None:
    if high < max(open_, close, low) or low > min(open_, close, high):
        raise CnyrubfAlgoPackError(
            f"AlgoPack {name} OHLC values are inconsistent",
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
) -> tuple[list[AlgoPackTradeStat], tuple[str, ...], AlgoPackCursor, str]:
    found_from, found_till, found_start = _exact_tradestats_route_values(route)
    if (found_from, found_till, found_start) != (from_date, till_date, start):
        raise CnyrubfAlgoPackError(
            "AlgoPack route does not pin exact ticker, range, and page",
            blocker="provenance_not_sufficient",
        )
    try:
        root = parse_json_object(payload)
    except ExternalDataError as exc:
        raise CnyrubfAlgoPackError(
            "AlgoPack response is not valid UTF-8 JSON",
            blocker="algopack_schema_not_stable",
        ) from exc
    rows, columns = _block(
        root,
        "data",
        _TRADESTAT_COLUMNS,
        schema_blocker="algopack_schema_not_stable",
    )
    cursor_rows, _ = _block(
        root,
        "data.cursor",
        _CURSOR_COLUMNS,
        schema_blocker="algopack_schema_not_stable",
    )
    if len(cursor_rows) != 1:
        raise CnyrubfAlgoPackError(
            "AlgoPack cursor must contain exactly one row",
            blocker="algopack_schema_not_stable",
        )
    cursor_row = cursor_rows[0]
    cursor = AlgoPackCursor(
        index=_integer(cursor_row["INDEX"], "cursor INDEX"),
        total=_integer(cursor_row["TOTAL"], "cursor TOTAL"),
        page_size=_integer(cursor_row["PAGESIZE"], "cursor PAGESIZE"),
    )
    if cursor.index != start or cursor.page_size <= 0 or start > cursor.total:
        raise CnyrubfAlgoPackError(
            "AlgoPack cursor is inconsistent with requested page",
            blocker="algopack_schema_not_stable",
        )
    remaining = cursor.total - start
    if len(rows) > remaining or len(rows) > cursor.page_size:
        raise CnyrubfAlgoPackError(
            "AlgoPack page row count exceeds cursor bounds",
            blocker="algopack_schema_not_stable",
        )
    _utc(retrieved_at_utc)

    result: list[AlgoPackTradeStat] = []
    previous_end: datetime | None = None
    identities: set[tuple[date, datetime, str, str]] = set()
    for row in rows:
        if str(row["secid"]).strip() != SECURITY_ID:
            raise CnyrubfAlgoPackError(
                "AlgoPack response contains a substituted security",
                blocker="security_identity_not_reproducible",
            )
        if str(row["asset_code"]).strip() != ASSET_CODE:
            raise CnyrubfAlgoPackError(
                "AlgoPack response contains a substituted asset code",
                blocker="security_identity_not_reproducible",
            )
        bucket_end = _provider_datetime(row["tradedate"], row["tradetime"])
        bucket_begin = bucket_end - timedelta(minutes=ALGOPACK_BUCKET_MINUTES)
        if not from_date <= bucket_end.date() <= till_date:
            raise CnyrubfAlgoPackError(
                "AlgoPack bucket is outside requested range",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if previous_end is not None and bucket_end <= previous_end:
            raise CnyrubfAlgoPackError(
                "AlgoPack buckets are duplicated or not chronological",
                blocker="numerical_or_chronology_integrity_failure",
            )
        identity = (
            bucket_end.date(),
            bucket_end,
            SECURITY_ID,
            ASSET_CODE,
        )
        if identity in identities:
            raise CnyrubfAlgoPackError(
                "AlgoPack provider row identity is duplicated",
                blocker="algopack_schema_not_stable",
            )
        identities.add(identity)
        source_available_at = _provider_timestamp(row["SYSTIME"])
        if source_available_at < bucket_end:
            raise CnyrubfAlgoPackError(
                "AlgoPack SYSTIME precedes its completed provider bucket",
                blocker="point_in_time_cutoff_not_provable",
            )
        _validate_directional_totals(row)
        open_ = _number(row["pr_open"], "pr_open")
        high = _number(row["pr_high"], "pr_high")
        low = _number(row["pr_low"], "pr_low")
        close = _number(row["pr_close"], "pr_close")
        _validate_ohlc(open_, high, low, close, name="price")
        oi_open = _number(row["oi_open"], "oi_open", nonnegative=True)
        oi_high = _number(row["oi_high"], "oi_high", nonnegative=True)
        oi_low = _number(row["oi_low"], "oi_low", nonnegative=True)
        oi_close = _number(row["oi_close"], "oi_close", nonnegative=True)
        _validate_ohlc(oi_open, oi_high, oi_low, oi_close, name="open interest")
        result.append(
            AlgoPackTradeStat(
                trade_date=bucket_end.date(),
                bucket_begin=bucket_begin,
                bucket_end=bucket_end,
                source_available_at=source_available_at,
                security_id=SECURITY_ID,
                asset_code=ASSET_CODE,
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
                initial_margin=_optional_initial_margin(row["im"]),
                open_interest_open=oi_open,
                open_interest_high=oi_high,
                open_interest_low=oi_low,
                open_interest_close=oi_close,
            )
        )
        previous_end = bucket_end
    return result, columns, cursor, raw_payload_sha256(payload)


def _collection_digest(page_digests: Sequence[str]) -> str:
    if not page_digests:
        raise CnyrubfAlgoPackError(
            "AlgoPack payload digest collection is empty",
            blocker="provenance_not_sufficient",
        )
    return hashlib.sha256("\n".join(page_digests).encode("ascii")).hexdigest()


def aggregate_daily_tradestats(
    rows: Sequence[AlgoPackTradeStat],
    *,
    source_route: str,
    retrieved_at_utc: datetime,
    raw_payload_sha256: str,
) -> list[CnyrubfAlgoPackDailyCandle]:
    grouped: dict[date, list[AlgoPackTradeStat]] = {}
    for row in rows:
        grouped.setdefault(row.trade_date, []).append(row)
    result: list[CnyrubfAlgoPackDailyCandle] = []
    for trade_date in sorted(grouped):
        buckets = grouped[trade_date]
        if buckets != sorted(buckets, key=lambda item: item.bucket_end):
            raise CnyrubfAlgoPackError(
                "AlgoPack daily buckets are not chronological",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if len({item.bucket_end for item in buckets}) != len(buckets):
            raise CnyrubfAlgoPackError(
                "AlgoPack daily buckets are duplicated",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if any(
            item.trade_date != trade_date or item.bucket_end.date() != trade_date
            for item in buckets
        ):
            raise CnyrubfAlgoPackError(
                "AlgoPack daily aggregate contains rows from another trade date",
                blocker="point_in_time_cutoff_not_provable",
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
        if not math.isclose(
            volume,
            volume_buy + volume_sell,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise CnyrubfAlgoPackError(
                "daily volume does not equal buy plus sell volume",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if not math.isclose(
            value,
            value_buy + value_sell,
            rel_tol=1e-6,
            abs_tol=1.0,
        ):
            raise CnyrubfAlgoPackError(
                "daily value does not equal buy plus sell value",
                blocker="numerical_or_chronology_integrity_failure",
            )
        if trades != trades_buy + trades_sell:
            raise CnyrubfAlgoPackError(
                "daily trades do not equal buy plus sell trades",
                blocker="numerical_or_chronology_integrity_failure",
            )
        source_available_at = max(item.source_available_at for item in buckets)
        candle_end = buckets[-1].bucket_end
        if source_available_at < candle_end:
            raise CnyrubfAlgoPackError(
                "daily aggregate availability precedes completion of source rows",
                blocker="point_in_time_cutoff_not_provable",
            )
        imbalance = 0.0 if volume == 0 else (volume_buy - volume_sell) / volume
        result.append(
            CnyrubfAlgoPackDailyCandle(
                source_id=SOURCE_ID,
                security_id=SECURITY_ID,
                asset_code=ASSET_CODE,
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
                initial_margin_close=buckets[-1].initial_margin,
                open_interest_open=buckets[0].open_interest_open,
                open_interest_high=max(item.open_interest_high for item in buckets),
                open_interest_low=min(item.open_interest_low for item in buckets),
                open_interest_close=buckets[-1].open_interest_close,
                candle_begin=buckets[0].bucket_begin,
                candle_end=candle_end,
                source_available_at=source_available_at,
                source_route=source_route,
                retrieved_at_utc=_utc(retrieved_at_utc),
                raw_payload_sha256=raw_payload_sha256,
                source_revision_status=SOURCE_REVISION_STATUS,
                historical_model_use_status=HISTORICAL_MODEL_USE_STATUS,
            )
        )
    return result


def load_daily_history(
    identity: CnyrubfSecurityIdentity,
    *,
    from_date: date,
    till_date: date,
    bearer_token: str | None = None,
    transport: AlgoPackTransport = fetch_algopack_bytes,
    token_loader: TokenLoader = load_algopack_token,
    sleeper: Sleeper = time.sleep,
    clock: UtcClock = utc_now,
) -> list[CnyrubfAlgoPackDailyCandle]:
    if (
        identity.security_id,
        identity.asset_code,
        identity.board_id,
        identity.engine,
        identity.market,
    ) != (SECURITY_ID, ASSET_CODE, BOARD_ID, ENGINE, MARKET):
        raise CnyrubfAlgoPackError(
            "AlgoPack history identity differs from exact CNYRUBF/FO source",
            blocker="security_identity_not_reproducible",
        )
    if not identity.primary_board or not identity.active_board:
        raise CnyrubfAlgoPackError(
            "official CNYRUBF board identity is not active and primary",
            blocker="security_identity_not_reproducible",
        )
    if from_date < identity.history_from:
        raise CnyrubfAlgoPackError(
            "requested range begins before official CNYRUBF history",
            blocker="cnyrubf_not_available",
        )
    if identity.history_till is not None and till_date > identity.history_till:
        raise CnyrubfAlgoPackError(
            "requested range ends after official CNYRUBF history",
            blocker="cnyrubf_not_available",
        )
    token = str(bearer_token).strip() if bearer_token is not None else token_loader()
    if not token:
        raise CnyrubfAlgoPackError(
            "AlgoPack token environment variable is not configured",
            blocker="token_env_not_configured",
        )

    all_rows: list[AlgoPackTradeStat] = []
    provider_ids: set[tuple[date, datetime, str, str]] = set()
    page_digests: list[str] = []
    expected_columns: tuple[str, ...] | None = None
    expected_total: int | None = None

    for _page in range(ALGOPACK_MAX_PAGES):
        start = len(all_rows)
        route = build_tradestats_url(from_date, till_date, start=start)
        payload = fetch_algopack_bytes_with_retry(
            route,
            token,
            transport=transport,
            sleeper=sleeper,
        )
        page_retrieved_at = _utc(clock())
        page_rows, columns, cursor, digest = parse_tradestats_page_response(
            payload,
            from_date=from_date,
            till_date=till_date,
            start=start,
            route=route,
            retrieved_at_utc=page_retrieved_at,
        )
        if expected_columns is None:
            expected_columns = columns
        elif columns != expected_columns:
            raise CnyrubfAlgoPackError(
                "AlgoPack schema changed during pagination",
                blocker="algopack_schema_not_stable",
            )
        if expected_total is None:
            expected_total = cursor.total
        elif cursor.total != expected_total:
            raise CnyrubfAlgoPackError(
                "AlgoPack cursor total changed during pagination",
                blocker="algopack_schema_not_stable",
            )
        if cursor.index != start or start > cursor.total:
            raise CnyrubfAlgoPackError(
                "AlgoPack pagination index is not exact",
                blocker="algopack_schema_not_stable",
            )
        remaining = cursor.total - start
        if len(page_rows) > remaining:
            raise CnyrubfAlgoPackError(
                "AlgoPack page exceeds remaining cursor total",
                blocker="algopack_schema_not_stable",
            )
        page_digests.append(digest)
        if not page_rows:
            if cursor.total == 0 and start == 0:
                raise CnyrubfAlgoPackError(
                    "CNYRUBF AlgoPack data is not available",
                    blocker="cnyrubf_not_available",
                )
            if start < cursor.total:
                raise CnyrubfAlgoPackError(
                    "AlgoPack pagination returned a premature empty page",
                    blocker="algopack_schema_not_stable",
                )
            break
        if all_rows and page_rows[0].bucket_end <= all_rows[-1].bucket_end:
            raise CnyrubfAlgoPackError(
                "AlgoPack pagination overlaps or reverses provider row order",
                blocker="algopack_schema_not_stable",
            )
        for row in page_rows:
            provider_id = (
                row.trade_date,
                row.bucket_end,
                row.security_id,
                row.asset_code,
            )
            if provider_id in provider_ids:
                raise CnyrubfAlgoPackError(
                    "AlgoPack provider row identity is duplicated across pages",
                    blocker="algopack_schema_not_stable",
                )
            provider_ids.add(provider_id)
        all_rows.extend(page_rows)
        if len(all_rows) > cursor.total:
            raise CnyrubfAlgoPackError(
                "AlgoPack accumulated rows exceed cursor total",
                blocker="algopack_schema_not_stable",
            )
        if len(all_rows) == cursor.total:
            break
    else:
        raise CnyrubfAlgoPackError(
            "AlgoPack pagination exceeded the bounded page limit",
            blocker="algopack_schema_not_stable",
        )

    if expected_total is None or len(all_rows) != expected_total:
        raise CnyrubfAlgoPackError(
            "AlgoPack pagination did not complete the exact cursor total",
            blocker="algopack_schema_not_stable",
        )
    if not all_rows:
        raise CnyrubfAlgoPackError(
            "CNYRUBF AlgoPack data is not available",
            blocker="cnyrubf_not_available",
        )
    timestamps = [row.bucket_end for row in all_rows]
    if timestamps != sorted(timestamps) or len(timestamps) != len(set(timestamps)):
        raise CnyrubfAlgoPackError(
            "AlgoPack buckets are duplicated or not chronological across pages",
            blocker="algopack_schema_not_stable",
        )
    retrieved = _utc(clock())
    return aggregate_daily_tradestats(
        all_rows,
        source_route=build_tradestats_url(from_date, till_date, start=0),
        retrieved_at_utc=retrieved,
        raw_payload_sha256=_collection_digest(page_digests),
    )


def validate_prior_session_candle(
    candle: CnyrubfAlgoPackDailyCandle,
    *,
    target_trade_date: date,
    prior_trade_date: date,
) -> None:
    anchor = datetime.combine(
        target_trade_date,
        datetime.strptime("06:00:00", "%H:%M:%S").time(),
        tzinfo=MOSCOW,
    )
    availability = candle.source_available_at
    if availability.tzinfo is None or availability.utcoffset() is None:
        raise CnyrubfAlgoPackError(
            "AlgoPack source availability timestamp is not timezone-aware",
            blocker="point_in_time_cutoff_not_provable",
        )
    exact_prior_rows = (
        candle.trade_date == prior_trade_date
        and candle.candle_begin.date() == prior_trade_date
        and candle.candle_end.date() == prior_trade_date
    )
    if (
        not exact_prior_rows
        or candle.candle_begin >= candle.candle_end
        or candle.candle_end > availability
        or candle.candle_end >= anchor
        or availability >= anchor
    ):
        raise CnyrubfAlgoPackError(
            "selected CNYRUBF aggregate cannot prove exact prior-session availability before the forecast anchor",
            blocker="point_in_time_cutoff_not_provable",
        )


__all__ = [
    "ALGOPACK_BUCKET_MINUTES",
    "ALGOPACK_HOST",
    "ALGOPACK_HTTP_MAX_ATTEMPTS",
    "ALGOPACK_HTTP_RETRY_DELAYS_SECONDS",
    "ALGOPACK_MARKET_CODE",
    "ALGOPACK_MAX_RETRY_AFTER_SECONDS",
    "ALGOPACK_TOKEN_ENV",
    "ALGOPACK_TRADESTATS_PATH",
    "ALGOPACK_TRADESTATS_ROUTE",
    "ASSET_CODE",
    "AlgoPackCursor",
    "AlgoPackTradeStat",
    "AlgoPackTransport",
    "BOARD_ID",
    "CnyrubfAlgoPackDailyCandle",
    "CnyrubfAlgoPackError",
    "CnyrubfSecurityIdentity",
    "ENGINE",
    "HISTORICAL_MODEL_USE_STATUS",
    "IssTransport",
    "MARKET",
    "MOEX_ISS_HOST",
    "MOSCOW",
    "SECURITY_ID",
    "SOURCE_ID",
    "SOURCE_REVISION_STATUS",
    "TokenLoader",
    "UtcClock",
    "aggregate_daily_tradestats",
    "build_security_metadata_url",
    "build_tradestats_url",
    "fetch_algopack_bytes",
    "fetch_algopack_bytes_with_retry",
    "fetch_iss_bytes",
    "fetch_iss_bytes_with_retry",
    "load_algopack_token",
    "load_daily_history",
    "load_security_identity",
    "parse_security_metadata_response",
    "parse_tradestats_page_response",
    "utc_now",
    "validate_prior_session_candle",
]
