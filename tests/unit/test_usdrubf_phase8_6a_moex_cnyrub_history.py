from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest

from moex_research.external_data import moex_cnyrub_history as cny
from moex_research.external_data.models import ExternalDataError

NOW = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
START, END = date(2024, 8, 1), date(2024, 8, 5)
COLS = ["open", "close", "high", "low", "value", "volume", "begin", "end"]


def _bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def _metadata(**changes: object) -> bytes:
    values = {
        "secid": "CNYRUB_TOM", "boardid": "CETS", "engine": "currency",
        "market": "selt", "is_traded": 1, "is_primary": 1,
        "history_from": "2010-09-27", "history_till": None,
    }
    values.update(changes)
    columns = list(values)
    return _bytes({
        "description": {
            "columns": ["name", "title", "value"],
            "data": [["SECID", "SECID", values["secid"]]],
        },
        "boards": {"columns": columns, "data": [[values[key] for key in columns]]},
    })


def _identity() -> cny.CnyrubSecurityIdentity:
    return cny.parse_security_metadata_response(
        _metadata(), route=cny.build_security_metadata_url(), retrieved_at_utc=NOW
    )


def _payload(rows: list[list[object]] | None = None, columns: list[str] | None = None) -> bytes:
    if rows is None:
        rows = [[11.9, 11.95, 12.0, 11.8, 1, 2, "2024-08-01 07:00:00", "2024-08-01 23:49:59"]]
    return _bytes({"candles": {"columns": columns or COLS, "data": rows}})


def _parse(payload: bytes) -> cny.CnyrubDailyCandle:
    route = cny.build_candle_url(START, END)
    rows, _ = cny.parse_candle_page_response(
        payload, from_date=START, till_date=END, start=0, route=route,
        retrieved_at_utc=NOW,
    )
    return rows[0]


def test_exact_official_host_security_board_engine_market() -> None:
    identity = _identity()
    assert urlsplit(cny.build_candle_url(START, END)).hostname == "iss.moex.com"
    assert (identity.security_id, identity.board_id, identity.engine, identity.market) == (
        "CNYRUB_TOM", "CETS", "currency", "selt"
    )
    assert identity.primary_board and identity.active_board


@pytest.mark.parametrize("field,value", [
    ("secid", "CNYRUBF"), ("boardid", "CNGD"), ("engine", "futures"),
    ("market", "forts"), ("is_traded", 0), ("is_primary", 0),
])
def test_no_identity_or_board_fallback(field: str, value: object) -> None:
    with pytest.raises(cny.CnyrubHistoryError) as raised:
        cny.parse_security_metadata_response(
            _metadata(**{field: value}), route=cny.build_security_metadata_url(),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == "security_identity_not_reproducible"


def test_nonofficial_host_and_missing_metadata_column_fail() -> None:
    route = cny.build_candle_url(START, END).replace("iss.moex.com", "example.com")
    with pytest.raises(cny.CnyrubHistoryError, match="allowlisted"):
        cny.parse_candle_page_response(
            _payload(), from_date=START, till_date=END, start=0, route=route,
            retrieved_at_utc=NOW,
        )
    metadata = json.loads(_metadata())
    index = metadata["boards"]["columns"].index("engine")
    metadata["boards"]["columns"].pop(index)
    metadata["boards"]["data"][0].pop(index)
    with pytest.raises(cny.CnyrubHistoryError, match="required identity columns"):
        cny.parse_security_metadata_response(
            _bytes(metadata), route=cny.build_security_metadata_url(), retrieved_at_utc=NOW
        )


def test_route_pins_range_interval_page_and_only_candles() -> None:
    query = parse_qs(urlsplit(cny.build_candle_url(START, END, start=7)).query)
    assert query == {
        "from": ["2024-08-01"], "till": ["2024-08-05"], "interval": ["24"],
        "start": ["7"], "iss.meta": ["off"], "iss.only": ["candles"],
        "candles.columns": [",".join(COLS)],
    }


def test_pagination_advances_by_rows_until_empty_page() -> None:
    calls: list[str] = []
    clocks = iter([NOW, NOW + timedelta(seconds=1), NOW + timedelta(seconds=2)])

    def transport(url: str) -> bytes:
        calls.append(url)
        offset = int(parse_qs(urlsplit(url).query)["start"][0])
        if offset == 2:
            return _payload([])
        day = offset + 1
        return _payload([[11, 11.5, 12, 10, 1, 2, f"2024-08-0{day} 07:00:00", f"2024-08-0{day} 23:49:59"]])

    rows = cny.load_daily_history(
        _identity(), from_date=START, till_date=END, transport=transport,
        clock=lambda: next(clocks),
    )
    assert [row.trade_date.isoformat() for row in rows] == ["2024-08-01", "2024-08-02"]
    assert [parse_qs(urlsplit(url).query)["start"] for url in calls] == [["0"], ["1"], ["2"]]
    assert all("CNYRUB_TOM" in url and "/CETS/" in url for url in calls)


def test_empty_first_page_is_structured_source_unavailable() -> None:
    with pytest.raises(cny.CnyrubHistoryError, match="unavailable") as raised:
        cny.load_daily_history(
            _identity(), from_date=START, till_date=END,
            transport=lambda _url: _payload([]), clock=lambda: NOW,
        )
    assert raised.value.blocker == "official_daily_candles_not_available"


def test_bounded_retry_uses_same_route_and_no_random_jitter() -> None:
    route, calls, delays = cny.build_candle_url(START, END), [], []

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

    def transport(_url: str) -> bytes:
        nonlocal calls
        calls += 1
        raise ExternalDataError("response is not valid UTF-8 JSON")

    with pytest.raises(ExternalDataError, match="valid UTF-8 JSON"):
        cny.fetch_cnyrub_bytes_with_retry(
            cny.build_candle_url(START, END), transport=transport,
            sleeper=lambda _delay: pytest.fail("must not retry semantic failure"),
        )
    assert calls == 1


def test_deterministic_normalization_digest_and_provenance() -> None:
    payload = _payload()
    first, second = _parse(payload), _parse(payload)
    assert first == second
    assert first.raw_payload_sha256 == hashlib.sha256(payload).hexdigest()
    assert first.source_route == cny.build_candle_url(START, END)
    assert first.source_revision_status == "official_iss_current_revision"


def test_duplicate_missing_column_and_malformed_json_fail() -> None:
    row = [11, 11.5, 12, 10, 1, 2, "2024-08-01 07:00:00", "2024-08-01 23:49:59"]
    with pytest.raises(cny.CnyrubHistoryError, match="duplicated|chronological"):
        _parse(_payload([row, row]))
    with pytest.raises(cny.CnyrubHistoryError, match="missing required"):
        _parse(_payload(columns=[item for item in COLS if item != "volume"]))
    with pytest.raises(cny.CnyrubHistoryError, match="valid UTF-8 JSON"):
        _parse(b"not-json")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_value_fails(value: float) -> None:
    row = [11, 11.5, 12, 10, 1, value, "2024-08-01 07:00:00", "2024-08-01 23:49:59"]
    with pytest.raises(cny.CnyrubHistoryError, match="finite"):
        _parse(_payload([row]))


def test_ohlc_and_same_day_leakage_fail() -> None:
    row = [11, 11.5, 10, 10, 1, 2, "2024-08-01 07:00:00", "2024-08-01 23:49:59"]
    with pytest.raises(cny.CnyrubHistoryError, match="OHLC"):
        _parse(_payload([row]))
    candle = _parse(_payload())
    cny.validate_prior_session_candle(
        candle, target_trade_date=date(2024, 8, 2), prior_trade_date=date(2024, 8, 1)
    )
    with pytest.raises(cny.CnyrubHistoryError, match="prior-session"):
        cny.validate_prior_session_candle(
            candle, target_trade_date=date(2024, 8, 1), prior_trade_date=date(2024, 8, 1)
        )
