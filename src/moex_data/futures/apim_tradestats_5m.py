from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Final, Protocol

from .raw_ohlcv_5m import Raw5mMaterializationRequest
from .validation import FuturesValidationError, guard_text

MOEX_APIM_DEFAULT_BASE_URL: Final[str] = "https://iss.moex.com"
MOEX_APIM_FO_TRADESTATS_SOURCE_ID: Final[str] = "moex_apim_algopack_fo_tradestats_5m"
MOEX_APIM_FO_TRADESTATS_NATIVE_TIMEFRAME: Final[str] = "5m"
MOEX_APIM_FO_TRADESTATS_PAGE_SIZE: Final[int] = 1000
REQUIRED_TRADESTATS_COLUMNS: Final[tuple[str, ...]] = (
    "secid",
    "tradedate",
    "tradetime",
    "pr_open",
    "pr_high",
    "pr_low",
    "pr_close",
    "vol",
    "val",
    "trades",
)


class MoexApimTradestatsSourceError(FuturesValidationError):
    pass


class MoexApimTradestatsHttpResponse(Protocol):
    status_code: int

    def json(self) -> Mapping[str, object]:
        pass


class MoexApimTradestatsHttpClient(Protocol):
    def get(self, url: str, *, params: Mapping[str, object], timeout: float) -> MoexApimTradestatsHttpResponse:
        pass


@dataclass(frozen=True)
class MoexApimFoTradestats5mPageRequest:
    url: str
    params: Mapping[str, object]


class RequestsMoexApimTradestatsHttpClient:
    def __init__(self) -> None:
        import requests

        self._session = requests.Session()

    def get(self, url: str, *, params: Mapping[str, object], timeout: float) -> MoexApimTradestatsHttpResponse:
        return self._session.get(url, params=params, timeout=timeout)


def build_moex_apim_fo_tradestats_5m_url(base_url: str) -> str:
    clean_base_url = guard_text(base_url.rstrip("/"), "apim_base_url")
    return clean_base_url + "/iss/datashop/algopack/fo/tradestats.json"


def build_moex_apim_fo_tradestats_5m_params(request: Raw5mMaterializationRequest, *, start: int) -> dict[str, object]:
    if start < 0:
        raise MoexApimTradestatsSourceError("APIM tradestats start must be non-negative")
    return {
        "date": request.partition_key.isoformat(),
        "from": request.partition_key.isoformat(),
        "till": request.partition_key.isoformat(),
        "secid": request.identity.secid,
        "start": start,
        "iss.meta": "off",
        "iss.only": "tradestats",
    }


def build_moex_apim_fo_tradestats_5m_page_request(
    request: Raw5mMaterializationRequest,
    *,
    base_url: str = MOEX_APIM_DEFAULT_BASE_URL,
    start: int = 0,
) -> MoexApimFoTradestats5mPageRequest:
    _validate_source_request(request)
    return MoexApimFoTradestats5mPageRequest(
        url=build_moex_apim_fo_tradestats_5m_url(base_url),
        params=build_moex_apim_fo_tradestats_5m_params(request, start=start),
    )


def _validate_source_request(request: Raw5mMaterializationRequest) -> None:
    if request.timeframe != MOEX_APIM_FO_TRADESTATS_NATIVE_TIMEFRAME:
        raise MoexApimTradestatsSourceError("APIM tradestats adapter requires native 5m request")
    if request.identity.market != "FORTS":
        raise MoexApimTradestatsSourceError("APIM tradestats adapter requires MARKET=FORTS")
    if request.identity.series_type != "native":
        raise MoexApimTradestatsSourceError("APIM tradestats adapter requires native SERIES_TYPE")
    if request.source_contract_ref != "contracts/datasets/futures_source_contracts.v1.yaml":
        raise MoexApimTradestatsSourceError("APIM tradestats adapter requires declared source contract ref")


def _require_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise MoexApimTradestatsSourceError(field_name + " must be a mapping")
    return value


def _require_sequence(value: object, field_name: str) -> Sequence[object]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise MoexApimTradestatsSourceError(field_name + " must be a sequence")
    return value


def _require_number(value: object, field_name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise MoexApimTradestatsSourceError(field_name + " must be numeric")
    return float(value)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MoexApimTradestatsSourceError(field_name + " is required")
    return value.strip()


def _parse_date(value: object, field_name: str) -> date:
    try:
        return date.fromisoformat(_require_text(value, field_name))
    except ValueError as exc:
        raise MoexApimTradestatsSourceError(field_name + " must be ISO date") from exc


def _parse_trade_time(value: object) -> time:
    text = _require_text(value, "tradetime")
    candidates = (text, text + ":00") if text.count(":") == 1 else (text,)
    for candidate in candidates:
        try:
            parsed = time.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.second != 0 or parsed.microsecond != 0:
            raise MoexApimTradestatsSourceError("APIM tradetime must be minute-aligned")
        if parsed.minute % 5:
            raise MoexApimTradestatsSourceError("APIM tradetime must be 5-minute aligned")
        return parsed
    raise MoexApimTradestatsSourceError("tradetime must be ISO-compatible time")


def _normalize_columns(raw_columns: Sequence[object]) -> tuple[str, ...]:
    columns = tuple(str(item).casefold() for item in raw_columns)
    if len(set(columns)) != len(columns):
        raise MoexApimTradestatsSourceError("APIM tradestats columns contain duplicates")
    missing = tuple(column for column in REQUIRED_TRADESTATS_COLUMNS if column not in columns)
    if missing:
        raise MoexApimTradestatsSourceError("APIM tradestats response missing column: " + missing[0])
    return columns


def _parse_tradestats_table(payload: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    table = _require_mapping(payload.get("tradestats"), "APIM tradestats table")
    columns = _normalize_columns(_require_sequence(table.get("columns"), "APIM tradestats columns"))
    raw_rows = _require_sequence(table.get("data"), "APIM tradestats data")
    rows: list[Mapping[str, object]] = []
    for raw_row in raw_rows:
        values = _require_sequence(raw_row, "APIM tradestats data row")
        if len(values) != len(columns):
            raise MoexApimTradestatsSourceError("APIM tradestats data row width mismatch")
        rows.append(dict(zip(columns, values)))
    return tuple(rows)


def _normalize_tradestats_row(raw_row: Mapping[str, object], request: Raw5mMaterializationRequest) -> dict[str, object]:
    tradedate = _parse_date(raw_row.get("tradedate"), "tradedate")
    if tradedate != request.partition_key:
        raise MoexApimTradestatsSourceError("APIM tradedate is outside requested partition")
    secid = _require_text(raw_row.get("secid"), "secid")
    if secid != request.identity.secid:
        raise MoexApimTradestatsSourceError("APIM secid does not match request")
    tradetime = _parse_trade_time(raw_row.get("tradetime"))
    ts = datetime.combine(tradedate, tradetime)
    return {
        "ts": ts,
        "trade_date": tradedate,
        "session_date": tradedate,
        "FAMILY": request.identity.family,
        "SECID": request.identity.secid,
        "BOARD": request.identity.board,
        "MARKET": request.identity.market,
        "SERIES_TYPE": request.identity.series_type,
        "open": _require_number(raw_row.get("pr_open"), "pr_open"),
        "high": _require_number(raw_row.get("pr_high"), "pr_high"),
        "low": _require_number(raw_row.get("pr_low"), "pr_low"),
        "close": _require_number(raw_row.get("pr_close"), "pr_close"),
        "volume": _require_number(raw_row.get("vol"), "vol"),
        "value": _require_number(raw_row.get("val"), "val"),
        "trades": _require_number(raw_row.get("trades"), "trades"),
    }


class MoexApimFoTradestats5mAdapter:
    def __init__(
        self,
        *,
        base_url: str = MOEX_APIM_DEFAULT_BASE_URL,
        timeout_seconds: float = 30.0,
        http_client: MoexApimTradestatsHttpClient | None = None,
        page_size: int = MOEX_APIM_FO_TRADESTATS_PAGE_SIZE,
    ) -> None:
        if page_size <= 0:
            raise MoexApimTradestatsSourceError("APIM tradestats page_size must be positive")
        self._base_url = guard_text(base_url.rstrip("/"), "apim_base_url")
        self._timeout_seconds = timeout_seconds
        self._http_client = http_client if http_client is not None else RequestsMoexApimTradestatsHttpClient()
        self._page_size = page_size

    @property
    def source_id(self) -> str:
        return MOEX_APIM_FO_TRADESTATS_SOURCE_ID

    def read_rows(self, request: Raw5mMaterializationRequest) -> Sequence[Mapping[str, object]]:
        _validate_source_request(request)
        output: list[Mapping[str, object]] = []
        start = 0
        while True:
            page_request = build_moex_apim_fo_tradestats_5m_page_request(request, base_url=self._base_url, start=start)
            try:
                response = self._http_client.get(
                    page_request.url,
                    params=page_request.params,
                    timeout=self._timeout_seconds,
                )
            except Exception as exc:
                raise MoexApimTradestatsSourceError("MOEX APIM tradestats request failed") from exc
            if response.status_code != 200:
                raise MoexApimTradestatsSourceError("MOEX APIM tradestats request returned HTTP " + str(response.status_code))
            try:
                payload = response.json()
            except Exception as exc:
                raise MoexApimTradestatsSourceError("MOEX APIM tradestats response is not valid JSON") from exc
            raw_rows = _parse_tradestats_table(payload)
            if not raw_rows:
                break
            output.extend(_normalize_tradestats_row(row, request) for row in raw_rows)
            if len(raw_rows) < self._page_size:
                break
            start += len(raw_rows)
        if not output:
            raise MoexApimTradestatsSourceError("MOEX APIM tradestats response returned no rows")
        return tuple(output)
