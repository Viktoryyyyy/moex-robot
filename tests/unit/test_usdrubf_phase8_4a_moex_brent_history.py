from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from functools import partial
from urllib.parse import parse_qs, urlsplit

import pytest

from moex_research.external_data import moex_brent_history as brent
from moex_research.external_data.models import ExternalDataError


RETRIEVED = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)
AS_OF = date(2024, 7, 31)


def _json_bytes(payload: object) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode()


def _history_payload(
    rows: list[list[object]] | None = None,
    *,
    columns: list[str] | None = None,
) -> bytes:
    columns = columns or ["BOARDID", "SECID", "TRADEDATE", "SHORTNAME", "ASSETCODE"]
    rows = rows if rows is not None else [["RFUD", "BRQ4", AS_OF.isoformat(), "BR-8.24", "BR"]]
    return _json_bytes(
        {
            "history": {"columns": columns, "data": rows},
            "history.cursor": {
                "columns": ["INDEX", "TOTAL", "PAGESIZE"],
                "data": [[0, len(rows), 100]],
            },
        }
    )


def _identity() -> brent.EnumeratedContractIdentity:
    route = brent.build_history_universe_url(AS_OF)
    identities, _ = brent.parse_history_universe_response(
        _history_payload(),
        as_of_date=AS_OF,
        route=route,
        retrieved_at_utc=RETRIEVED,
    )
    return identities[0]


def _metadata_payload(*, omit: str | None = None, asset: str = "BR") -> bytes:
    values = {
        "SECID": "BRQ4",
        "NAME": "Фьючерсный контракт BR-8.24",
        "SHORTNAME": "BR-8.24",
        "FRSTTRADE": "2023-07-26",
        "LSTTRADE": "2024-08-01",
        "LSTDELDATE": "2024-08-01",
        "ASSETCODE": asset,
        "GROUP": "futures_forts",
        "TYPE": "futures",
    }
    if omit:
        values.pop(omit)
    return _json_bytes(
        {
            "description": {
                "columns": ["name", "title", "value"],
                "data": [[name, name, value] for name, value in values.items()],
            },
            "boards": {
                "columns": ["secid", "boardid", "is_traded", "history_from", "history_till"],
                "data": [["BRQ4", "RFUD", 0, "2023-07-26", "2024-08-01"]],
            },
        }
    )


def _contract(**metadata: object) -> brent.BrentContract:
    return brent.parse_contract_metadata_response(
        _metadata_payload(**metadata),
        identity=_identity(),
        route=brent.build_security_description_url("BRQ4"),
        retrieved_at_utc=RETRIEVED,
    )


def _candle_payload(
    rows: list[list[object]] | None = None,
) -> bytes:
    rows = rows if rows is not None else [
        [78.66, 80.73, 80.97, 78.57, 0, 140800, "2024-07-31 00:00:00", "2024-07-31 23:59:59"]
    ]
    return _json_bytes(
        {
            "candles": {
                "columns": ["open", "close", "high", "low", "value", "volume", "begin", "end"],
                "data": rows,
            }
        }
    )


def _parse_candle(payload: bytes) -> brent.BrentDailyCandle:
    contract = _contract()
    route = brent.build_candle_url(contract.contract_code, AS_OF)
    return brent.parse_daily_candle_response(
        payload,
        contract=contract,
        trade_date=AS_OF,
        route=route,
        retrieved_at_utc=RETRIEVED,
    )


def test_official_expired_contract_universe_response_parses() -> None:
    route = brent.build_history_universe_url(AS_OF)
    rows, cursor = brent.parse_history_universe_response(
        _history_payload(),
        as_of_date=AS_OF,
        route=route,
        retrieved_at_utc=RETRIEVED,
    )
    assert cursor == (0, 1, 100)
    assert [(row.contract_code, row.asset_code, row.board_id) for row in rows] == [
        ("BRQ4", "BR", "RFUD")
    ]
    assert rows[0].enumeration_retrieved_at_utc == RETRIEVED


def test_active_only_current_route_cannot_masquerade_as_history() -> None:
    current_route = (
        "https://iss.moex.com/iss/engines/futures/markets/forts/boards/"
        "RFUD/securities.json?assetcode=BR"
    )
    with pytest.raises(brent.BrentHistoryError, match="allowlisted"):
        brent.parse_history_universe_response(
            _history_payload(),
            as_of_date=AS_OF,
            route=current_route,
            retrieved_at_utc=RETRIEVED,
        )


def test_non_BR_asset_is_rejected() -> None:
    payload = _history_payload(
        [["RFUD", "CLQ4", AS_OF.isoformat(), "CL-8.24", "CL"]]
    )
    with pytest.raises(brent.BrentHistoryError, match="non-BR"):
        brent.parse_history_universe_response(
            payload,
            as_of_date=AS_OF,
            route=brent.build_history_universe_url(AS_OF),
            retrieved_at_utc=RETRIEVED,
        )


def test_non_RFUD_board_is_rejected() -> None:
    payload = _history_payload(
        [["TQBR", "BRQ4", AS_OF.isoformat(), "BR-8.24", "BR"]]
    )
    with pytest.raises(brent.BrentHistoryError, match="non-RFUD"):
        brent.parse_history_universe_response(
            payload,
            as_of_date=AS_OF,
            route=brent.build_history_universe_url(AS_OF),
            retrieved_at_utc=RETRIEVED,
        )


def test_duplicate_contract_identity_fails() -> None:
    row = ["RFUD", "BRQ4", AS_OF.isoformat(), "BR-8.24", "BR"]
    with pytest.raises(brent.BrentHistoryError, match="duplicate"):
        brent.parse_history_universe_response(
            _history_payload([row, row]),
            as_of_date=AS_OF,
            route=brent.build_history_universe_url(AS_OF),
            retrieved_at_utc=RETRIEVED,
        )


def test_missing_expiration_metadata_fails() -> None:
    with pytest.raises(brent.BrentHistoryError, match="LSTTRADE"):
        _contract(omit="LSTTRADE")


def test_guessed_or_generated_contract_identity_is_never_accepted() -> None:
    with pytest.raises(brent.BrentHistoryError, match="non-explicit"):
        brent.build_security_description_url("BR")
    assert "month" not in brent.build_security_description_url.__name__.lower()


@pytest.mark.parametrize("alias", ["BR_CONTINUOUS", "BR_FRONT", "BR1!"])
def test_mutable_or_continuous_alias_fails(alias: str) -> None:
    with pytest.raises(brent.BrentHistoryError, match="continuous|mutable"):
        brent.build_candle_url(alias, AS_OF)


def test_explicit_expired_contract_daily_candle_parses() -> None:
    candle = _parse_candle(_candle_payload())
    assert candle.contract_code == "BRQ4"
    assert candle.trade_date == AS_OF
    assert candle.close == 80.73
    assert candle.candle_end.tzinfo is not None


def test_empty_expired_contract_candle_history_fails() -> None:
    with pytest.raises(brent.BrentHistoryError, match="empty") as raised:
        _parse_candle(_candle_payload([]))
    assert raised.value.blocker == "expired_contract_candles_not_available"


def test_malformed_candle_timestamp_fails() -> None:
    row = [78.66, 80.73, 80.97, 78.57, 0, 140800, "31/07/2024", "2024-07-31 23:59:59"]
    with pytest.raises(brent.BrentHistoryError, match="timestamp"):
        _parse_candle(_candle_payload([row]))


def test_OHLC_inconsistency_fails() -> None:
    row = [78.66, 80.73, 80.0, 78.57, 0, 140800, "2024-07-31 00:00:00", "2024-07-31 23:59:59"]
    with pytest.raises(brent.BrentHistoryError, match="OHLC"):
        _parse_candle(_candle_payload([row]))


@pytest.mark.parametrize("position", [4, 5])
def test_negative_volume_or_value_fails(position: int) -> None:
    row: list[object] = [78.66, 80.73, 80.97, 78.57, 0, 140800, "2024-07-31 00:00:00", "2024-07-31 23:59:59"]
    row[position] = -1
    with pytest.raises(brent.BrentHistoryError, match="non-negative"):
        _parse_candle(_candle_payload([row]))


def test_duplicate_contract_date_candle_fails() -> None:
    row = [78.66, 80.73, 80.97, 78.57, 0, 140800, "2024-07-31 00:00:00", "2024-07-31 23:59:59"]
    with pytest.raises(brent.BrentHistoryError, match="duplicate"):
        _parse_candle(_candle_payload([row, row]))


def test_source_provenance_is_retained_and_distinguishable() -> None:
    contract = _contract()
    candle = _parse_candle(_candle_payload())
    assert contract.metadata_route.startswith("https://iss.moex.com/iss/securities/")
    assert candle.source_route.endswith("iss.only=candles")
    assert len(contract.metadata_raw_payload_sha256) == 64
    assert len(candle.raw_payload_sha256) == 64
    assert contract.metadata_raw_payload_sha256 != candle.raw_payload_sha256


def test_history_page_timestamp_is_captured_after_transport_returns() -> None:
    events: list[str] = []

    def transport(_: str) -> bytes:
        events.append("transport_return")
        return _history_payload()

    def clock() -> datetime:
        events.append("clock")
        return RETRIEVED

    identities = brent.enumerate_brent_contract_identities(
        AS_OF, transport=transport, clock=clock
    )
    assert events == ["transport_return", "clock"]
    assert identities[0].enumeration_retrieved_at_utc == RETRIEVED


def test_separate_history_pages_capture_separate_timestamp_and_digest() -> None:
    timestamps = iter(
        [
            RETRIEVED,
            RETRIEVED + timedelta(seconds=1),
        ]
    )
    payloads: dict[int, bytes] = {}

    def transport(url: str) -> bytes:
        start = int(parse_qs(urlsplit(url).query)["start"][0])
        code = "BRQ4" if start == 0 else "BRU4"
        payload = _json_bytes(
            {
                "history": {
                    "columns": [
                        "BOARDID",
                        "SECID",
                        "TRADEDATE",
                        "SHORTNAME",
                        "ASSETCODE",
                    ],
                    "data": [
                        ["RFUD", code, AS_OF.isoformat(), f"{code}-name", "BR"]
                    ],
                },
                "history.cursor": {
                    "columns": ["INDEX", "TOTAL", "PAGESIZE"],
                    "data": [[start, 2, 1]],
                },
            }
        )
        payloads[start] = payload
        return payload

    identities = brent.enumerate_brent_contract_identities(
        AS_OF, transport=transport, clock=lambda: next(timestamps)
    )
    assert [item.enumeration_retrieved_at_utc for item in identities] == [
        RETRIEVED,
        RETRIEVED + timedelta(seconds=1),
    ]
    assert [item.enumeration_raw_payload_sha256 for item in identities] == [
        hashlib.sha256(payloads[0]).hexdigest(),
        hashlib.sha256(payloads[1]).hexdigest(),
    ]
    assert [parse_qs(urlsplit(item.enumeration_route).query)["start"] for item in identities] == [
        ["0"],
        ["1"],
    ]


def test_metadata_and_candle_each_capture_post_transport_clock() -> None:
    events: list[str] = []
    timestamps = iter([RETRIEVED, RETRIEVED + timedelta(seconds=1)])

    def clock() -> datetime:
        events.append("clock")
        return next(timestamps)

    def metadata_transport(_: str) -> bytes:
        events.append("metadata_transport_return")
        return _metadata_payload()

    contract = brent.load_contract_metadata(
        _identity(), transport=metadata_transport, clock=clock
    )

    def candle_transport(_: str) -> bytes:
        events.append("candle_transport_return")
        return _candle_payload()

    candle = brent.load_daily_candle(
        contract, AS_OF, transport=candle_transport, clock=clock
    )
    assert events == [
        "metadata_transport_return",
        "clock",
        "candle_transport_return",
        "clock",
    ]
    assert contract.metadata_retrieved_at_utc == RETRIEVED
    assert contract.enumeration_retrieved_at_utc == RETRIEVED
    assert candle.retrieved_at_utc == RETRIEVED + timedelta(seconds=1)


def test_naive_injected_clock_fails_closed() -> None:
    with pytest.raises(brent.BrentHistoryError, match="timezone-aware") as raised:
        brent.enumerate_brent_contract_identities(
            AS_OF,
            transport=lambda _: _history_payload(),
            clock=lambda: datetime(2026, 7, 16, 12, 0),
        )
    assert raised.value.blocker == "provenance_not_sufficient"


def test_non_UTC_injected_clock_fails_closed() -> None:
    non_utc = timezone(timedelta(hours=3))
    with pytest.raises(brent.BrentHistoryError, match="expressed in UTC") as raised:
        brent.load_contract_metadata(
            _identity(),
            transport=lambda _: _metadata_payload(),
            clock=lambda: datetime(2026, 7, 16, 15, 0, tzinfo=non_utc),
        )
    assert raised.value.blocker == "provenance_not_sufficient"


def test_retry_first_attempt_success_calls_once_without_sleep() -> None:
    calls: list[str] = []
    sleeps: list[float] = []
    route = brent.build_history_universe_url(AS_OF)

    def transport(url: str) -> bytes:
        calls.append(url)
        return _history_payload()

    assert brent.fetch_brent_bytes_with_retry(
        route, transport=transport, sleeper=sleeps.append
    ) == _history_payload()
    assert calls == [route]
    assert sleeps == []


def test_retry_one_transient_failure_then_success() -> None:
    calls: list[str] = []
    sleeps: list[float] = []
    route = brent.build_security_description_url("BRQ4")

    def transport(url: str) -> bytes:
        calls.append(url)
        if len(calls) == 1:
            raise ExternalDataError(brent.TRANSIENT_HTTP_ERROR_MESSAGE)
        return _metadata_payload()

    assert brent.fetch_brent_bytes_with_retry(
        route, transport=transport, sleeper=sleeps.append
    ) == _metadata_payload()
    assert calls == [route, route]
    assert sleeps == [0.5]


def test_retry_four_transient_failures_then_success_uses_exact_schedule_and_route() -> None:
    calls: list[str] = []
    sleeps: list[float] = []
    route = brent.build_candle_url("BRQ4", AS_OF)

    def transport(url: str) -> bytes:
        calls.append(url)
        if len(calls) < 5:
            raise ExternalDataError(brent.TRANSIENT_HTTP_ERROR_MESSAGE)
        return _candle_payload()

    assert brent.fetch_brent_bytes_with_retry(
        route, transport=transport, sleeper=sleeps.append
    ) == _candle_payload()
    assert calls == [route] * 5
    assert sleeps == [0.5, 1.0, 2.0, 4.0]


def test_retry_five_transient_failures_preserves_cause_route_and_attempt_count() -> None:
    calls: list[str] = []
    sleeps: list[float] = []
    errors: list[ExternalDataError] = []
    route = brent.build_security_description_url("BRQ4")

    def transport(url: str) -> bytes:
        calls.append(url)
        error = ExternalDataError(brent.TRANSIENT_HTTP_ERROR_MESSAGE)
        errors.append(error)
        raise error

    with pytest.raises(
        ExternalDataError, match=r"official route=.*BRQ4.*attempts=5"
    ) as raised:
        brent.fetch_brent_bytes_with_retry(
            route, transport=transport, sleeper=sleeps.append
        )
    assert calls == [route] * 5
    assert sleeps == [0.5, 1.0, 2.0, 4.0]
    assert raised.value.__cause__ is errors[-1]


def test_empty_response_error_is_not_retried() -> None:
    calls: list[str] = []
    sleeps: list[float] = []

    def transport(url: str) -> bytes:
        calls.append(url)
        raise ExternalDataError("external-data response is empty")

    with pytest.raises(ExternalDataError, match="response is empty"):
        brent.fetch_brent_bytes_with_retry(
            brent.build_history_universe_url(AS_OF),
            transport=transport,
            sleeper=sleeps.append,
        )
    assert len(calls) == 1
    assert sleeps == []


def test_schema_failure_after_successful_transport_is_not_retried() -> None:
    calls: list[str] = []
    sleeps: list[float] = []
    payload = _history_payload(columns=["SECID"])

    def transport(url: str) -> bytes:
        calls.append(url)
        return payload

    retrying = partial(
        brent.fetch_brent_bytes_with_retry,
        transport=transport,
        sleeper=sleeps.append,
    )
    with pytest.raises(brent.BrentHistoryError, match="missing required columns"):
        brent.enumerate_brent_contract_identities(AS_OF, transport=retrying)
    assert len(calls) == 1
    assert sleeps == []


def test_semantic_brent_history_error_is_not_retried() -> None:
    calls: list[str] = []
    sleeps: list[float] = []
    semantic_error = brent.BrentHistoryError("semantic failure")

    def transport(url: str) -> bytes:
        calls.append(url)
        raise semantic_error

    with pytest.raises(brent.BrentHistoryError) as raised:
        brent.fetch_brent_bytes_with_retry(
            brent.build_history_universe_url(AS_OF),
            transport=transport,
            sleeper=sleeps.append,
        )
    assert raised.value is semantic_error
    assert len(calls) == 1
    assert sleeps == []


def test_retry_success_retains_payload_sha_and_captures_clock_only_after_success() -> None:
    events: list[str] = []
    sleeps: list[float] = []
    calls = 0
    payload = _history_payload()

    def transport(_: str) -> bytes:
        nonlocal calls
        calls += 1
        events.append(f"transport_{calls}")
        if calls == 1:
            raise ExternalDataError(brent.TRANSIENT_HTTP_ERROR_MESSAGE)
        return payload

    def clock() -> datetime:
        events.append("clock")
        return RETRIEVED

    retrying = partial(
        brent.fetch_brent_bytes_with_retry,
        transport=transport,
        sleeper=sleeps.append,
    )
    identities = brent.enumerate_brent_contract_identities(
        AS_OF, transport=retrying, clock=clock
    )
    assert events == ["transport_1", "transport_2", "clock"]
    assert sleeps == [0.5]
    assert identities[0].enumeration_retrieved_at_utc == RETRIEVED
    assert identities[0].enumeration_raw_payload_sha256 == hashlib.sha256(
        payload
    ).hexdigest()


def test_exhausted_retry_creates_no_provenance_timestamp() -> None:
    clock_calls = 0
    sleeps: list[float] = []

    def transport(_: str) -> bytes:
        raise ExternalDataError(brent.TRANSIENT_HTTP_ERROR_MESSAGE)

    def clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        return RETRIEVED

    retrying = partial(
        brent.fetch_brent_bytes_with_retry,
        transport=transport,
        sleeper=sleeps.append,
    )
    with pytest.raises(brent.BrentHistoryError, match="attempts=5"):
        brent.enumerate_brent_contract_identities(
            AS_OF, transport=retrying, clock=clock
        )
    assert clock_calls == 0
    assert sleeps == [0.5, 1.0, 2.0, 4.0]
