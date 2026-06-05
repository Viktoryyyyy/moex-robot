from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Protocol

from .raw_ohlcv_5m import Raw5mMaterializationRequest
from .validation import FuturesValidationError, guard_text

MOEX_ISS_DEFAULT_BASE_URL: Final[str] = "https://iss.moex.com"
MOEX_ISS_FORTS_CANDLES_SOURCE_ID: Final[str] = "moex_iss_forts_candles_5m"
MOEX_ISS_FORTS_CANDLES_NATIVE_TIMEFRAME: Final[str] = "5m"
MOEX_ISS_FORTS_CANDLES_INTERVAL: Final[str] = "5"
MOEX_ISS_FORTS_CANDLES_PAGE_SIZE: Final[int] = 100
REQUIRED_ISS_CANDLE_COLUMNS: Final[tuple[str, ...]] = (
    "begin",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
)


class MoexIssSourceError(FuturesValidationError):
    pass


class MoexIssHttpResponse(Protocol):
    status_code: int

    def json(self) -> Mapping[str, object]:
        pass


class MoexIssHttpClient(Protocol):
    def get(self, url: str, *, params: Mapping[str, object], timeout: float) -> MoexIssHttpResponse:
        pass


@dataclass(frozen=True)
class MoexIssFortsCandles5mPageRequest:
    url: str
    params: Mapping[str, object]


class RequestsMoexIssHttpClient:
    def __init__(self) -> None:
        import requests

        self._session = requests.Session()

    def get(self, url: str, *, params: Mapping[str, object], timeout: float) -> MoexIssHttpResponse:
        return self._session.get(url, params=params, timeout=timeout)


def build_moex_iss_forts_candles_5m_url(base_url: str, *, board: str, secid: str) -> str:
    clean_base_url = guard_text(base_url.rstrip("/"), "iss_base_url")
    clean_board = guard_text(board, "BOARD")
    clean_secid = guard_text(secid, "SECID")
    return (
        clean_base_url
        + "/iss/engines/futures/markets/forts/boards/"
        + clean_board.lower()
        + "/securities/"
        + clean_secid
        + "/candles.json"
    )


def build_moex_iss_forts_candles_5m_params(request: Raw5mMaterializationRequest, *, start: int) -> dict[str, object]:
    if start < 0:
        raise MoexIssSourceError("ISS start must be non-negative")
    return {
        "interval": MOEX_ISS_FORTS_CANDLES_INTERVAL,
        "from": request.partition_key.isoformat(),
        "till": request.partition_key.isoformat(),
        "start": start,
        "iss.meta": "off",
    }


def build_moex_iss_forts_candles_5m_page_request(
    request: Raw5mMaterializationRequest,
    *,
    base_url: str = MOEX_ISS_DEFAULT_BASE_URL,
    start: int = 0,
) -> MoexIssFortsCandles5mPageRequest:
    _validate_source_request(request)
    return MoexIssFortsCandles5mPageRequest(
        url=build_moex_iss_forts_candles_5m_url(base_url, board=request.identity.board, secid=request.identity.secid),
        params=build_moex_iss_forts_candles_5m_params(request, start=start),
    )


def _validate_source_request(request: Raw5mMaterializationRequest) -> None:
    if request.timeframe != MOEX_ISS_FORTS_CANDLES_NATIVE_TIMEFRAME:
        raise MoexIssSourceError("MOEX ISS FORTS candles adapter requires native 5m request")
    if request.identity.market != "FORTS":
        raise MoexIssSourceError("MOEX ISS FORTS candles adapter requires MARKET=FORTS")
    if request.identity.series_type != "native":
        raise MoexIssSourceError("MOEX ISS FORTS candles adapter requires native SERIES_TYPE")
    if request.source_contract_ref != "contracts/datasets/futures_source_contracts.v1.yaml":
        raise MoexIssSourceError("MOEX ISS FORTS candles adapter requires declared source contract ref")


def _require_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise MoexIssSourceError(field_name + " must be a mapping")
    return value


def _require_sequence(value: object, field_name: str) -> Sequence[object]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise MoexIssSourceError(field_name + " must be a sequence")
    return value


def _require_number(value: object, field_name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise MoexIssSourceError(field_name + " must be numeric")
    return float(value)


def _optional_number(value: object, field_name: str) -> float | None:
    if value is None:
        return None
    return _require_number(value, field_name)


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise MoexIssSourceError("ISS begin timestamp is required")
    try:
        return datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise MoexIssSourceError("ISS begin timestamp must be ISO-compatible") from exc


def _parse_candles_table(payload: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    table = _require_mapping(payload.get("candles"), "ISS candles table")
    raw_columns = _require_sequence(table.get("columns"), "ISS candles columns")
    columns = tuple(str(item) for item in raw_columns)
    if len(set(columns)) != len(columns):
        raise MoexIssSourceError("ISS candles columns contain duplicates")
    missing = tuple(column for column in REQUIRED_ISS_CANDLE_COLUMNS if column not in columns)
    if missing:
        raise MoexIssSourceError("ISS candles response missing column: " + missing[0])
    raw_rows = _require_sequence(table.get("data"), "ISS candles data")
    rows: list[Mapping[str, object]] = []
    for raw_row in raw_rows:
        values = _require_sequence(raw_row, "ISS candles data row")
        if len(values) != len(columns):
            raise MoexIssSourceError("ISS candles data row width mismatch")
        rows.append(dict(zip(columns, values)))
    return tuple(rows)


def _normalize_iss_candle_row(raw_row: Mapping[str, object], request: Raw5mMaterializationRequest) -> dict[str, object]:
    ts = _parse_timestamp(raw_row.get("begin"))
    if ts.date() != request.partition_key:
        raise MoexIssSourceError("ISS candle timestamp is outside requested partition")
    open_value = _require_number(raw_row.get("open"), "open")
    high_value = _require_number(raw_row.get("high"), "high")
    low_value = _require_number(raw_row.get("low"), "low")
    close_value = _require_number(raw_row.get("close"), "close")
    volume_value = _require_number(raw_row.get("volume"), "volume")
    value_value = _require_number(raw_row.get("value"), "value")
    trades_value = _optional_number(raw_row.get("trades"), "trades")
    end_value = raw_row.get("end")
    if isinstance(end_value, str) and end_value.strip():
        try:
            end_ts = datetime.fromisoformat(end_value.strip())
        except ValueError as exc:
            raise MoexIssSourceError("ISS end timestamp must be ISO-compatible") from exc
        if end_ts - ts != timedelta(minutes=5):
            raise MoexIssSourceError("ISS candle duration must be 5 minutes")
    return {
        "ts": ts,
        "trade_date": request.partition_key,
        "session_date": request.partition_key,
        "FAMILY": request.identity.family,
        "SECID": request.identity.secid,
        "BOARD": request.identity.board,
        "MARKET": request.identity.market,
        "SERIES_TYPE": request.identity.series_type,
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value,
        "volume": volume_value,
        "value": value_value,
        "trades": 0.0 if trades_value is None else trades_value,
    }


class MoexIssFortsCandles5mAdapter:
    def __init__(
        self,
        *,
        base_url: str = MOEX_ISS_DEFAULT_BASE_URL,
        timeout_seconds: float = 30.0,
        http_client: MoexIssHttpClient | None = None,
        page_size: int = MOEX_ISS_FORTS_CANDLES_PAGE_SIZE,
    ) -> None:
        if page_size <= 0:
            raise MoexIssSourceError("ISS page_size must be positive")
        self._base_url = guard_text(base_url.rstrip("/"), "iss_base_url")
        self._timeout_seconds = timeout_seconds
        self._http_client = http_client if http_client is not None else RequestsMoexIssHttpClient()
        self._page_size = page_size

    @property
    def source_id(self) -> str:
        return MOEX_ISS_FORTS_CANDLES_SOURCE_ID

    def read_rows(self, request: Raw5mMaterializationRequest) -> Sequence[Mapping[str, object]]:
        _validate_source_request(request)
        output: list[Mapping[str, object]] = []
        start = 0
        while True:
            page_request = build_moex_iss_forts_candles_5m_page_request(request, base_url=self._base_url, start=start)
            try:
                response = self._http_client.get(
                    page_request.url,
                    params=page_request.params,
                    timeout=self._timeout_seconds,
                )
            except Exception as exc:
                raise MoexIssSourceError("MOEX ISS candles request failed") from exc
            if response.status_code != 200:
                raise MoexIssSourceError("MOEX ISS candles request returned HTTP " + str(response.status_code))
            try:
                payload = response.json()
            except Exception as exc:
                raise MoexIssSourceError("MOEX ISS candles response is not valid JSON") from exc
            raw_rows = _parse_candles_table(payload)
            if not raw_rows:
                break
            output.extend(_normalize_iss_candle_row(row, request) for row in raw_rows)
            if len(raw_rows) < self._page_size:
                break
            start += len(raw_rows)
        if not output:
            raise MoexIssSourceError("MOEX ISS candles response returned no rows")
        return tuple(output)
