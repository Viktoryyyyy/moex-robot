from __future__ import annotations

import math
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Final
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
from zoneinfo import ZoneInfo

from .models import ExternalDataError, HttpTransport, fetch_bytes, parse_json_object
from .moex_iss import (
    MOEX_ISS_HOST,
    TRANSIENT_HTTP_ERROR_MESSAGE,
    MoexIssClient,
    MoexIssClientError,
    RetryPolicy,
    Sleeper,
    UtcClock,
    parse_iss_block,
    require_utc,
    utc_now as iss_utc_now,
    validate_official_route,
)

SOURCE_ID: Final[str] = "moex_brent_futures_daily"
ASSET_CODE: Final[str] = "BR"
BOARD_ID: Final[str] = "RFUD"
MOSCOW: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")
HISTORY_ROUTE: Final[str] = (
    "https://iss.moex.com/iss/history/engines/futures/markets/forts/"
    "boards/RFUD/securities.json"
)
SECURITY_DESCRIPTION_ROUTE_TEMPLATE: Final[str] = (
    "https://iss.moex.com/iss/securities/{contract_code}.json"
)
CANDLE_ROUTE_TEMPLATE: Final[str] = (
    "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/"
    "securities/{contract_code}/candles.json"
)
SOURCE_REVISION_STATUS: Final[str] = "official_iss_current_revision"
HISTORICAL_MODEL_USE_STATUS: Final[str] = "source_validation_only"
BRENT_HTTP_MAX_ATTEMPTS: Final[int] = 5
BRENT_HTTP_RETRY_DELAYS_SECONDS: Final[tuple[float, ...]] = (
    0.5,
    1.0,
    2.0,
    4.0,
)
_EXPLICIT_CODE_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Z0-9_]{2,32}$")
_MUTABLE_TOKENS: Final[tuple[str, ...]] = (
    "CONT",
    "CONTINUOUS",
    "FRONT",
    "NEAR",
    "NEXT",
    "ACTIVE",
)


class BrentHistoryError(ValueError):
    """A fail-closed official-source, schema, identity, or PIT error."""

    def __init__(
        self,
        message: str,
        *,
        blocker: str = "other_fail_closed_with_exact_reason",
    ) -> None:
        super().__init__(message)
        self.blocker = blocker


def fetch_brent_bytes_with_retry(
    url: str,
    *,
    transport: HttpTransport = fetch_bytes,
    sleeper: Sleeper = time.sleep,
) -> bytes:
    """Fetch one Phase 8.4A MOEX Brent route with bounded transient retries."""

    client = MoexIssClient(
        retry_policy=RetryPolicy(
            maximum_total_attempts=BRENT_HTTP_MAX_ATTEMPTS,
            retry_delays_seconds=BRENT_HTTP_RETRY_DELAYS_SECONDS,
            transient_error_message=TRANSIENT_HTTP_ERROR_MESSAGE,
        ),
        transport=transport,
        sleeper=sleeper,
    )
    return client.fetch(url)


def utc_now() -> datetime:
    """Return the production retrieval clock at artifact precision."""

    return iss_utc_now()


@dataclass(frozen=True)
class EnumeratedContractIdentity:
    contract_code: str
    short_name: str
    asset_code: str
    board_id: str
    enumerated_as_of_date: date
    enumeration_route: str
    enumeration_retrieved_at_utc: datetime
    enumeration_raw_payload_sha256: str


@dataclass(frozen=True)
class BrentContract:
    source_id: str
    contract_code: str
    short_name: str
    asset_code: str
    board_id: str
    first_verified_trade_date: date
    expiration_date: date
    last_delivery_date: date
    metadata_route: str
    metadata_retrieved_at_utc: datetime
    metadata_raw_payload_sha256: str
    source_revision_status: str
    historical_model_use_status: str
    enumerated_as_of_date: date
    enumeration_route: str
    enumeration_retrieved_at_utc: datetime
    enumeration_raw_payload_sha256: str

    def as_record(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BrentDailyCandle:
    source_id: str
    contract_code: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    value: float
    candle_begin: datetime
    candle_end: datetime
    expiration_date: date
    source_route: str
    retrieved_at_utc: datetime
    raw_payload_sha256: str
    source_revision_status: str
    historical_model_use_status: str

    def as_record(self) -> dict[str, object]:
        return asdict(self)


def _utc(value: datetime) -> datetime:
    try:
        return require_utc(value)
    except MoexIssClientError as exc:
        if exc.reason == "timestamp_not_timezone_aware":
            message = "retrieval timestamp must be timezone-aware"
        else:
            message = "retrieval timestamp must be expressed in UTC"
        raise BrentHistoryError(message, blocker="provenance_not_sufficient") from exc


def _official_route(url: str, *, allowed_path_prefix: str) -> None:
    try:
        validate_official_route(
            url,
            allowed_path_prefix=allowed_path_prefix,
            expected_host=MOEX_ISS_HOST,
        )
    except MoexIssClientError as exc:
        raise BrentHistoryError(
            "route is not an allowlisted official MOEX ISS route",
            blocker="provenance_not_sufficient",
        ) from exc


def _explicit_contract_code(value: object) -> str:
    code = str(value).strip().upper()
    if (
        not _EXPLICIT_CODE_PATTERN.fullmatch(code)
        or code == ASSET_CODE
        or "!" in code
        or any(token in code for token in _MUTABLE_TOKENS)
    ):
        raise BrentHistoryError(
            "mutable, continuous, or non-explicit contract identity refused",
            blocker="expired_contract_universe_not_reproducible",
        )
    return code


def _iss_rows(
    payload: bytes,
    *,
    block_name: str,
    required_columns: Sequence[str],
) -> tuple[list[dict[str, object]], str]:
    try:
        block = parse_iss_block(
            payload,
            block_name=block_name,
            required_columns=required_columns,
        )
    except MoexIssClientError as exc:
        messages = {
            "invalid_json": "official ISS response is not valid JSON",
            "missing_block": f"official ISS response lacks {block_name} block",
            "malformed_columns": f"official ISS {block_name} columns are malformed",
            "missing_required_columns": f"official ISS {block_name} schema is missing required columns",
            "malformed_data": f"official ISS {block_name} data is malformed",
            "row_width_mismatch": f"official ISS {block_name} row width mismatch",
            "invalid_payload_digest": "official payload digest is invalid",
        }
        blocker = (
            "provenance_not_sufficient"
            if exc.reason == "invalid_payload_digest"
            else "official_schema_not_stable"
        )
        raise BrentHistoryError(messages[exc.reason], blocker=blocker) from exc
    return block.rows, block.raw_payload_sha256


def build_history_universe_url(as_of_date: date, *, start: int = 0) -> str:
    if start < 0:
        raise BrentHistoryError("history pagination start must be non-negative")
    query = urlencode(
        {
            "date": as_of_date.isoformat(),
            "assetcode": ASSET_CODE,
            "iss.meta": "off",
            "iss.only": "history,history.cursor",
            "history.columns": "BOARDID,SECID,TRADEDATE,SHORTNAME,ASSETCODE",
            "start": start,
        }
    )
    return f"{HISTORY_ROUTE}?{query}"


def parse_history_universe_response(
    payload: bytes,
    *,
    as_of_date: date,
    route: str,
    retrieved_at_utc: datetime,
) -> tuple[list[EnumeratedContractIdentity], tuple[int, int, int]]:
    _official_route(
        route,
        allowed_path_prefix=(
            "/iss/history/engines/futures/markets/forts/boards/RFUD/securities"
        ),
    )
    query = dict(parse_qsl(urlsplit(route).query, keep_blank_values=True))
    if query.get("date") != as_of_date.isoformat() or query.get("assetcode") != ASSET_CODE:
        raise BrentHistoryError(
            "history universe route does not pin the requested date and BR asset",
            blocker="expired_contract_universe_not_reproducible",
        )
    rows, digest = _iss_rows(
        payload,
        block_name="history",
        required_columns=("BOARDID", "SECID", "TRADEDATE", "SHORTNAME", "ASSETCODE"),
    )
    try:
        decoded = parse_json_object(payload)
    except ExternalDataError as exc:  # pragma: no cover - already parsed by _iss_rows
        raise BrentHistoryError(
            "official ISS response is not valid JSON",
            blocker="official_schema_not_stable",
        ) from exc
    cursor_block = decoded.get("history.cursor")
    if not isinstance(cursor_block, Mapping):
        raise BrentHistoryError(
            "official ISS history cursor is absent",
            blocker="official_schema_not_stable",
        )
    cursor_columns = cursor_block.get("columns")
    cursor_data = cursor_block.get("data")
    if not isinstance(cursor_columns, list) or not isinstance(cursor_data, list) or len(cursor_data) != 1:
        raise BrentHistoryError(
            "official ISS history cursor is malformed",
            blocker="official_schema_not_stable",
        )
    try:
        cursor = dict(zip(cursor_columns, cursor_data[0], strict=True))
        cursor_tuple = (
            int(cursor["INDEX"]),
            int(cursor["TOTAL"]),
            int(cursor["PAGESIZE"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise BrentHistoryError(
            "official ISS history cursor fields are invalid",
            blocker="official_schema_not_stable",
        ) from exc
    timestamp = _utc(retrieved_at_utc)
    identities: list[EnumeratedContractIdentity] = []
    seen: set[str] = set()
    for row in rows:
        board = str(row["BOARDID"]).strip()
        asset = str(row["ASSETCODE"] or "").strip()
        if board != BOARD_ID:
            raise BrentHistoryError(
                "non-RFUD identity entered RFUD history response",
                blocker="expired_contract_universe_not_reproducible",
            )
        if asset == "":
            continue
        if asset != ASSET_CODE:
            raise BrentHistoryError(
                "non-BR asset entered BR history response",
                blocker="expired_contract_universe_not_reproducible",
            )
        if str(row["TRADEDATE"]).strip() != as_of_date.isoformat():
            raise BrentHistoryError(
                "history response date differs from requested as-of date",
                blocker="expired_contract_universe_not_reproducible",
            )
        code = _explicit_contract_code(row["SECID"])
        if code in seen:
            raise BrentHistoryError(
                "duplicate contract identity in official history response",
                blocker="expired_contract_universe_not_reproducible",
            )
        seen.add(code)
        short_name = str(row["SHORTNAME"] or "").strip()
        if not short_name:
            raise BrentHistoryError(
                "official history contract short name is missing",
                blocker="official_schema_not_stable",
            )
        identities.append(
            EnumeratedContractIdentity(
                contract_code=code,
                short_name=short_name,
                asset_code=asset,
                board_id=board,
                enumerated_as_of_date=as_of_date,
                enumeration_route=route,
                enumeration_retrieved_at_utc=timestamp,
                enumeration_raw_payload_sha256=digest,
            )
        )
    return identities, cursor_tuple


def enumerate_brent_contract_identities(
    as_of_date: date,
    *,
    transport: HttpTransport = fetch_brent_bytes_with_retry,
    clock: UtcClock = utc_now,
) -> list[EnumeratedContractIdentity]:
    identities: list[EnumeratedContractIdentity] = []
    start = 0
    total: int | None = None
    while total is None or start < total:
        route = build_history_universe_url(as_of_date, start=start)
        _official_route(
            route,
            allowed_path_prefix=(
                "/iss/history/engines/futures/markets/forts/boards/RFUD/securities"
            ),
        )
        try:
            payload = transport(route)
        except Exception as exc:
            raise BrentHistoryError(
                "official historical contract enumeration request failed: "
                f"{exc}",
                blocker="expired_contract_universe_not_reproducible",
            ) from exc
        retrieved_at_utc = _utc(clock())
        page, (index, observed_total, page_size) = parse_history_universe_response(
            payload,
            as_of_date=as_of_date,
            route=route,
            retrieved_at_utc=retrieved_at_utc,
        )
        if index != start or observed_total < 0 or page_size <= 0:
            raise BrentHistoryError(
                "official history pagination is inconsistent",
                blocker="official_schema_not_stable",
            )
        if total is not None and observed_total != total:
            raise BrentHistoryError(
                "official history total changed during pagination",
                blocker="official_schema_not_stable",
            )
        total = observed_total
        identities.extend(page)
        start += page_size
    codes = [item.contract_code for item in identities]
    if not identities:
        raise BrentHistoryError(
            "official historical route returned no explicit BR contracts",
            blocker="expired_contract_universe_not_reproducible",
        )
    if len(codes) != len(set(codes)):
        raise BrentHistoryError(
            "duplicate contract identity across history pages",
            blocker="expired_contract_universe_not_reproducible",
        )
    return identities


def build_security_description_url(contract_code: str) -> str:
    code = _explicit_contract_code(contract_code)
    return SECURITY_DESCRIPTION_ROUTE_TEMPLATE.format(contract_code=quote(code, safe="")) + (
        "?iss.meta=off&iss.only=description,boards"
    )


def _metadata_date(values: Mapping[str, object], field: str) -> date:
    text = str(values.get(field) or "").strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise BrentHistoryError(
            f"official contract metadata {field} is missing or invalid",
            blocker="expired_contract_universe_not_reproducible",
        ) from exc


def parse_contract_metadata_response(
    payload: bytes,
    *,
    identity: EnumeratedContractIdentity,
    route: str,
    retrieved_at_utc: datetime,
) -> BrentContract:
    _official_route(route, allowed_path_prefix="/iss/securities/")
    rows, digest = _iss_rows(
        payload,
        block_name="description",
        required_columns=("name", "value"),
    )
    values: dict[str, object] = {}
    for row in rows:
        name = str(row["name"] or "").strip()
        if not name or name in values:
            raise BrentHistoryError(
                "official contract description has duplicate or empty field identity",
                blocker="official_schema_not_stable",
            )
        values[name] = row["value"]
    try:
        decoded = parse_json_object(payload)
    except ExternalDataError as exc:  # pragma: no cover - already parsed by _iss_rows
        raise BrentHistoryError(
            "official contract metadata is not valid JSON",
            blocker="official_schema_not_stable",
        ) from exc
    boards = decoded.get("boards")
    if not isinstance(boards, Mapping):
        raise BrentHistoryError(
            "official contract metadata boards block is absent",
            blocker="official_schema_not_stable",
        )
    board_columns = boards.get("columns")
    board_data = boards.get("data")
    if not isinstance(board_columns, list) or not isinstance(board_data, list):
        raise BrentHistoryError(
            "official contract metadata boards block is malformed",
            blocker="official_schema_not_stable",
        )
    try:
        board_rows = [dict(zip(board_columns, raw, strict=True)) for raw in board_data]
    except (TypeError, ValueError) as exc:
        raise BrentHistoryError(
            "official contract metadata board row width mismatch",
            blocker="official_schema_not_stable",
        ) from exc
    matching_boards = [
        row
        for row in board_rows
        if str(row.get("secid") or "").strip() == identity.contract_code
        and str(row.get("boardid") or "").strip() == BOARD_ID
    ]
    if len(matching_boards) != 1:
        raise BrentHistoryError(
            "contract does not resolve to one official RFUD board identity",
            blocker="expired_contract_universe_not_reproducible",
        )
    code = _explicit_contract_code(values.get("SECID"))
    if code != identity.contract_code:
        raise BrentHistoryError(
            "enumerated and metadata contract codes differ",
            blocker="expired_contract_universe_not_reproducible",
        )
    asset = str(values.get("ASSETCODE") or "").strip()
    if asset != ASSET_CODE:
        raise BrentHistoryError(
            "official contract metadata is not a BR asset",
            blocker="expired_contract_universe_not_reproducible",
        )
    short_name = str(values.get("SHORTNAME") or "").strip()
    if not short_name or short_name != identity.short_name:
        raise BrentHistoryError(
            "enumerated and metadata short names differ",
            blocker="expired_contract_universe_not_reproducible",
        )
    if values.get("GROUP") != "futures_forts" or values.get("TYPE") != "futures":
        raise BrentHistoryError(
            "official identity is not a FORTS futures contract",
            blocker="expired_contract_universe_not_reproducible",
        )
    first_trade = _metadata_date(values, "FRSTTRADE")
    expiration = _metadata_date(values, "LSTTRADE")
    last_delivery = _metadata_date(values, "LSTDELDATE")
    if not first_trade <= identity.enumerated_as_of_date <= expiration:
        raise BrentHistoryError(
            "historical enumeration date falls outside official contract life",
            blocker="expired_contract_universe_not_reproducible",
        )
    return BrentContract(
        source_id=SOURCE_ID,
        contract_code=code,
        short_name=short_name,
        asset_code=asset,
        board_id=BOARD_ID,
        first_verified_trade_date=first_trade,
        expiration_date=expiration,
        last_delivery_date=last_delivery,
        metadata_route=route,
        metadata_retrieved_at_utc=_utc(retrieved_at_utc),
        metadata_raw_payload_sha256=digest,
        source_revision_status=SOURCE_REVISION_STATUS,
        historical_model_use_status=HISTORICAL_MODEL_USE_STATUS,
        enumerated_as_of_date=identity.enumerated_as_of_date,
        enumeration_route=identity.enumeration_route,
        enumeration_retrieved_at_utc=identity.enumeration_retrieved_at_utc,
        enumeration_raw_payload_sha256=identity.enumeration_raw_payload_sha256,
    )


def load_contract_metadata(
    identity: EnumeratedContractIdentity,
    *,
    transport: HttpTransport = fetch_brent_bytes_with_retry,
    clock: UtcClock = utc_now,
) -> BrentContract:
    route = build_security_description_url(identity.contract_code)
    _official_route(route, allowed_path_prefix="/iss/securities/")
    try:
        payload = transport(route)
    except Exception as exc:
        raise BrentHistoryError(
            "official exact-contract metadata request failed: "
            f"{exc}",
            blocker="expired_contract_universe_not_reproducible",
        ) from exc
    retrieved_at_utc = _utc(clock())
    return parse_contract_metadata_response(
        payload,
        identity=identity,
        route=route,
        retrieved_at_utc=retrieved_at_utc,
    )


def select_nearest_contract(
    contracts: Sequence[BrentContract],
    *,
    target_trade_date: date,
) -> BrentContract:
    threshold = target_trade_date + timedelta(days=7)
    eligible = [item for item in contracts if item.expiration_date >= threshold]
    if not eligible:
        raise BrentHistoryError(
            "no explicit contract satisfies the fixed seven-calendar-day rule",
            blocker="incomplete_identity_coverage",
        )
    nearest_expiration = min(item.expiration_date for item in eligible)
    nearest = [item for item in eligible if item.expiration_date == nearest_expiration]
    if len(nearest) != 1:
        raise BrentHistoryError(
            "fixed contract rule resolves to an ambiguous official identity",
            blocker="expired_contract_universe_not_reproducible",
        )
    return nearest[0]


def build_candle_url(contract_code: str, trade_date: date) -> str:
    code = _explicit_contract_code(contract_code)
    base = CANDLE_ROUTE_TEMPLATE.format(contract_code=quote(code, safe=""))
    query = urlencode(
        {
            "from": trade_date.isoformat(),
            "till": trade_date.isoformat(),
            "interval": 24,
            "iss.meta": "off",
            "iss.only": "candles",
        }
    )
    return f"{base}?{query}"


def _finite_number(value: object, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise BrentHistoryError(
            f"candle {field} is malformed",
            blocker="official_schema_not_stable",
        ) from exc
    if not math.isfinite(number):
        raise BrentHistoryError(
            f"candle {field} must be finite",
            blocker="official_schema_not_stable",
        )
    return number


def _finite_nonnegative(value: object, *, field: str) -> float:
    number = _finite_number(value, field=field)
    if number < 0:
        raise BrentHistoryError(
            f"candle {field} must be finite and non-negative",
            blocker="official_schema_not_stable",
        )
    return number


def _candle_timestamp(value: object, *, field: str) -> datetime:
    text = str(value or "").strip()
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise BrentHistoryError(
            f"candle {field} timestamp is malformed",
            blocker="official_schema_not_stable",
        ) from exc
    return parsed.replace(tzinfo=MOSCOW)


def parse_daily_candle_response(
    payload: bytes,
    *,
    contract: BrentContract,
    trade_date: date,
    route: str,
    retrieved_at_utc: datetime,
) -> BrentDailyCandle:
    _official_route(
        route,
        allowed_path_prefix=(
            "/iss/engines/futures/markets/forts/boards/RFUD/securities/"
        ),
    )
    query = dict(parse_qsl(urlsplit(route).query, keep_blank_values=True))
    if (
        query.get("from") != trade_date.isoformat()
        or query.get("till") != trade_date.isoformat()
        or query.get("interval") != "24"
    ):
        raise BrentHistoryError(
            "explicit candle route does not pin one daily session",
            blocker="point_in_time_cutoff_not_provable",
        )
    rows, digest = _iss_rows(
        payload,
        block_name="candles",
        required_columns=("open", "close", "high", "low", "value", "volume", "begin", "end"),
    )
    if not rows:
        raise BrentHistoryError(
            "explicit expired-contract candle history is empty",
            blocker="expired_contract_candles_not_available",
        )
    parsed: list[BrentDailyCandle] = []
    for row in rows:
        candle_open = _finite_number(row["open"], field="open")
        close = _finite_number(row["close"], field="close")
        high = _finite_number(row["high"], field="high")
        low = _finite_number(row["low"], field="low")
        value = _finite_nonnegative(row["value"], field="value")
        volume = _finite_nonnegative(row["volume"], field="volume")
        begin = _candle_timestamp(row["begin"], field="begin")
        end = _candle_timestamp(row["end"], field="end")
        if begin.date() != trade_date or end.date() != trade_date or end < begin:
            raise BrentHistoryError(
                "daily candle timestamp does not belong to requested trade date",
                blocker="point_in_time_cutoff_not_provable",
            )
        if high < max(candle_open, close) or low > min(candle_open, close) or high < low:
            raise BrentHistoryError(
                "daily candle OHLC values are inconsistent",
                blocker="official_schema_not_stable",
            )
        parsed.append(
            BrentDailyCandle(
                source_id=SOURCE_ID,
                contract_code=contract.contract_code,
                trade_date=trade_date,
                open=candle_open,
                high=high,
                low=low,
                close=close,
                volume=volume,
                value=value,
                candle_begin=begin,
                candle_end=end,
                expiration_date=contract.expiration_date,
                source_route=route,
                retrieved_at_utc=_utc(retrieved_at_utc),
                raw_payload_sha256=digest,
                source_revision_status=SOURCE_REVISION_STATUS,
                historical_model_use_status=HISTORICAL_MODEL_USE_STATUS,
            )
        )
    if len(parsed) != 1:
        raise BrentHistoryError(
            "duplicate explicit contract-date daily candle",
            blocker="official_schema_not_stable",
        )
    return parsed[0]


def load_daily_candle(
    contract: BrentContract,
    trade_date: date,
    *,
    transport: HttpTransport = fetch_brent_bytes_with_retry,
    clock: UtcClock = utc_now,
) -> BrentDailyCandle:
    route = build_candle_url(contract.contract_code, trade_date)
    _official_route(
        route,
        allowed_path_prefix=(
            "/iss/engines/futures/markets/forts/boards/RFUD/securities/"
        ),
    )
    try:
        payload = transport(route)
    except Exception as exc:
        raise BrentHistoryError(
            "official explicit-contract candle request failed: "
            f"{exc}",
            blocker="expired_contract_candles_not_available",
        ) from exc
    retrieved_at_utc = _utc(clock())
    return parse_daily_candle_response(
        payload,
        contract=contract,
        trade_date=trade_date,
        route=route,
        retrieved_at_utc=retrieved_at_utc,
    )


def validate_prior_session_cutoff(
    candle: BrentDailyCandle,
    *,
    target_trade_date: date,
    prior_trade_date: date,
) -> None:
    cutoff = datetime.combine(
        target_trade_date,
        datetime.strptime("08:45:00", "%H:%M:%S").time(),
        tzinfo=MOSCOW,
    )
    if candle.trade_date != prior_trade_date or candle.candle_end >= cutoff:
        raise BrentHistoryError(
            "selected candle violates frozen prior-session decision cutoff",
            blocker="point_in_time_cutoff_not_provable",
        )
