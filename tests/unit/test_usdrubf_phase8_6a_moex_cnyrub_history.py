from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest

from moex_research.external_data import moex_cnyrub_history as cny
from moex_research.external_data.models import ExternalDataError


RETRIEVED = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
FROM_DATE = date(2024, 8, 1)
TILL_DATE = date(2024, 8, 5)


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def _metadata_payload(
    *,
    secid: str = "CNYRUB_TOM",
    boardid: str = "CETS",
    engine: str = "currency",
    market: str = "selt",
    primary: int = 1,
    traded: int = 1,
    omit_board_column: str | None = None,
) -> bytes:
    board_columns = [
        "secid",
        "boardid",
        "engine",
        "market",
        "is_traded",
        "is_primary",
        "history_from",
        "history_till",
    ]
    board_row: list[object] = [
        secid,
        boardid,
        engine,
        market,
        traded,
        primary,
        "2010-09-27",
        None,
    ]
    if omit_board_column:
        index = board_columns.index(omit_board_column)
        board_columns.pop(index)
        board_row.pop(index)
    return _json_bytes(
        {
            "description": {
                "columns": ["name", "title", "value"],
                "data": [["SECID", "SECID", secid], ["PRIMARY_BOARDID", "board", boardid]],
            },
            "boards": {"columns": board_columns, "data": [board_row]},
        }
    )


def _identity() -> cny.CnyrubSecurityIdentity:
    return cny.parse_security_metadata_response(
        _metadata_payload(),
        route=cny.build_security_metadata_url(),
        retrieved_at_utc=RETRIEVED,
    )


def _candle_payload(
    rows: list[list[object]] | None = None,
    *,
    start: int = 0,
    total: int | None = None,
    page_size: int = 100,
    columns: list[str] | None = None,
) -> bytes:
    columns = columns or ["open", "close", "high", "low", "value", "volume", "begin", "end"]
    rows = rows if rows is not None else [
        [
            11.9,
            11.95,
            12.0,
            11.8,
            1000000.0,
            500000.0,
            "2024-08-01 07:00:00",
            "2024-08-01 23:49:59",
        ]
    ]
    return _json_bytes(
        {
            "candles": {"columns": columns, "data": rows},
            "candles.cursor": {
                "columns": ["INDEX", "TOTAL", "PAGESIZE"],
                "data": [[start, len(rows) if total is None else total, page_size]],
            },
        }
    )


def _parse(payload: bytes, *, start: int = 0) -> list[cny.CnyrubDailyCandle]:
    route = cny.build_candle_url(FROM_DATE, TILL_DATE, start=start)
    rows, _, _ = cny.parse_candle_page_response(
        payload,
        from_date=FROM_DATE,
        till_date=TILL_DATE,
        start=start,
        route=route,
        retrieved_at_utc=RETRIEVED,
    )
    return rows


def test_exact_official_host_and_route_are_allowlisted() -> None:
    route = cny.build_candle_url(FROM_DATE, TILL_DATE)
    assert urlsplit(route).hostname == "iss.moex.com"
    assert "/currency/markets/selt/boards/CETS/securities/CNYRUB_TOM/" in route
    bad = route.replace("iss.moex.com", "example.com")
    with pytest.raises(cny.CnyrubHistoryError, match="allowlisted"):
        cny.parse_candle_page_response(
            _candle_payload(),
            from_date=FROM_DATE,
            till_date=TILL_DATE,
            start=0,
            route=bad,
            retrieved_at_utc=RETRIEVED,
        )


def test_exact_security_board_engine_market_and_primary_active_identity() -> None:
    identity = _identity()
    assert (identity.security_id, identity.board_id, identity.engine, identity.market) == (
        "CNYRUB_TOM",
        "CETS",
        "currency",
        "selt",
    )
    assert identity.primary_board and identity.active_board


@pytest.mark.parametrize(
    ("keyword", "value"),
    [
        ("secid", "CNYRUBF"),
        ("boardid", "CNGD"),
        ("engine", "futures"),
        ("market", "forts"),
        ("primary", 0),
        ("traded", 0),
    ],
)
def test_no_security_board_or_market_fallback(keyword: str, value: object) -> None:
    with pytest.raises(cny.CnyrubHistoryError) as raised:
        cny.parse_security_metadata_response(
            _metadata_payload(**{keyword: value}),
            route=cny.build_security_metadata_url(),
            retrieved_at_utc=RETRIEVED,
        )
    assert raised.value.blocker == "security_identity_not_reproducible"


def test_route_pins_daily_interval_and_exact_range() -> None:
    query = parse_qs(urlsplit(cny.build_candle_url(FROM_DATE, TILL_DATE)).query)
    assert query["interval"] == ["24"]
    assert query["from"] == [FROM_DATE.isoformat()]
    assert query["till"] == [TILL_DATE.isoformat()]
    assert query["start"] == ["0"]


def test_full_pagination_uses_same_exact_route_identity() -> None:
    calls: list[str] = []
    timestamps = iter([RETRIEVED, RETRIEVED + timedelta(seconds=1)])

    def transport(url: str) -> bytes:
        calls.append(url)
        start = int(parse_qs(urlsplit(url).query)["start"][0])
        day = 1 if start == 0 else 2
        return _candle_payload(
            [[11.9, 11.95, 12.0, 11.8, 1, 2, f"2024-08-0{day} 07:00:00", f"2024-08-0{day} 23:49:59"]],
            start=start,
            total=2,
            page_size=1,
        )

    rows = cny.load_daily_history(
        _identity(),
        from_date=FROM_DATE,
        till_date=TILL_DATE,
        transport=transport,
        clock=lambda: next(timestamps),
    )
    assert [item.trade_date.isoformat() for item in rows] == ["2024-08-01", "2024-08-02"]
    assert [parse_qs(urlsplit(url).query)["start"] for url in calls] == [["0"], ["1"]]
    assert all("CNYRUB_TOM" in url and "/CETS/" in url for url in calls)


def test_bounded_retry_reuses_same_exact_route_without_jitter() -> None:
    route = cny.build_candle_url(FROM_DATE, TILL_DATE)
    calls: list[str] = []
    delays: list[float] = []

    def transport(url: str) -> bytes:
        calls.append(url)
        if len(calls) < 3:
            raise ExternalDataError("external-data request failed")
        return b"ok"

    assert cny.fetch_cnyrub_bytes_with_retry(route, transport=transport, sleeper=delays.append) == b"ok"
    assert calls == [route, route, route]
    assert delays == [0.5, 1.0]


def test_semantic_failure_is_not_retried() -> None:
    calls = 0

    def transport(_: str) -> bytes:
        nonlocal calls
        calls += 1
        raise ExternalDataError("response is not valid UTF-8 JSON")

    with pytest.raises(ExternalDataError, match="valid UTF-8 JSON"):
        cny.fetch_cnyrub_bytes_with_retry(
            cny.build_candle_url(FROM_DATE, TILL_DATE),
            transport=transport,
            sleeper=lambda _: pytest.fail("semantic failure slept"),
        )
    assert calls == 1


def test_deterministic_normalization_and_raw_payload_sha256() -> None:
    payload = _candle_payload()
    first = _parse(payload)[0]
    second = _parse(payload)[0]
    assert first == second
    assert first.raw_payload_sha256 == hashlib.sha256(payload).hexdigest()
    assert first.source_route == cny.build_candle_url(FROM_DATE, TILL_DATE)


def test_duplicate_or_nonchronological_trade_dates_fail() -> None:
    row = [11.9, 11.95, 12.0, 11.8, 1, 2, "2024-08-01 07:00:00", "2024-08-01 23:49:59"]
    with pytest.raises(cny.CnyrubHistoryError, match="duplicated|chronological"):
        _parse(_candle_payload([row, row]))


def test_missing_column_and_malformed_json_fail_closed() -> None:
    columns = ["open", "close", "high", "low", "value", "begin", "end"]
    with pytest.raises(cny.CnyrubHistoryError, match="missing required"):
        _parse(_candle_payload(columns=columns))
    with pytest.raises(cny.CnyrubHistoryError, match="valid UTF-8 JSON"):
        _parse(b"not-json")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_candle_values_fail(value: float) -> None:
    row = [11.9, 11.95, 12.0, 11.8, 1, value, "2024-08-01 07:00:00", "2024-08-01 23:49:59"]
    with pytest.raises(cny.CnyrubHistoryError, match="finite"):
        _parse(_candle_payload([row]))


def test_ohlc_inconsistency_fails() -> None:
    row = [11.9, 11.95, 11.0, 11.8, 1, 2, "2024-08-01 07:00:00", "2024-08-01 23:49:59"]
    with pytest.raises(cny.CnyrubHistoryError, match="OHLC"):
        _parse(_candle_payload([row]))


def test_exact_prior_session_cutoff_rejects_same_day() -> None:
    candle = _parse(_candle_payload())[0]
    cny.validate_prior_session_candle(
        candle,
        target_trade_date=date(2024, 8, 2),
        prior_trade_date=date(2024, 8, 1),
    )
    with pytest.raises(cny.CnyrubHistoryError, match="prior-session"):
        cny.validate_prior_session_candle(
            candle,
            target_trade_date=date(2024, 8, 1),
            prior_trade_date=date(2024, 8, 1),
        )


def test_metadata_missing_required_board_column_fails() -> None:
    with pytest.raises(cny.CnyrubHistoryError, match="required identity columns"):
        cny.parse_security_metadata_response(
            _metadata_payload(omit_board_column="engine"),
            route=cny.build_security_metadata_url(),
            retrieved_at_utc=RETRIEVED,
        )
