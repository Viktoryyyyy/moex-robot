from __future__ import annotations

import json
from datetime import date, datetime, timezone
from email.message import Message
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request

import pytest

from moex_research.external_data import moex_cnyrubf_algopack_history as source


NOW = datetime(2026, 7, 29, 10, 0, tzinfo=timezone.utc)
START = date(2026, 6, 10)
END = date(2026, 6, 10)
TOKEN = "secret-token-value"
COLUMNS = list(source._TRADESTAT_COLUMNS)


def _metadata_payload(
    *,
    secid: str = source.SECURITY_ID,
    boardid: str = source.BOARD_ID,
    engine: str = source.ENGINE,
    market: str = source.MARKET,
    is_primary: int = 1,
    is_traded: int = 1,
) -> bytes:
    return json.dumps(
        {
            "description": {
                "columns": ["name", "title", "value"],
                "data": [["SECID", "Код ценной бумаги", secid]],
            },
            "boards": {
                "columns": [
                    "secid",
                    "boardid",
                    "engine",
                    "market",
                    "is_traded",
                    "is_primary",
                    "history_from",
                    "history_till",
                ],
                "data": [
                    [
                        secid,
                        boardid,
                        engine,
                        market,
                        is_traded,
                        is_primary,
                        "2022-06-01",
                        None,
                    ]
                ],
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")


def _row(
    *,
    day: str = "2026-06-10",
    tradetime: str = "09:05:00",
    systime: str = "2026-06-10 09:05:03",
    secid: str = source.SECURITY_ID,
    asset_code: str = source.ASSET_CODE,
    open_: float = 12.0,
    high: float = 12.2,
    low: float = 11.9,
    close: float = 12.1,
    vol: float = 10.0,
    vol_b: float = 6.0,
    vol_s: float = 4.0,
    val: float = 120.0,
    val_b: float = 72.0,
    val_s: float = 48.0,
    trades: int = 5,
    trades_b: int = 3,
    trades_s: int = 2,
    im: object = 1000.0,
    oi_open: float = 100.0,
    oi_high: float = 110.0,
    oi_low: float = 90.0,
    oi_close: float = 105.0,
) -> list[object]:
    values = {
        "tradedate": day,
        "tradetime": tradetime,
        "secid": secid,
        "asset_code": asset_code,
        "pr_open": open_,
        "pr_high": high,
        "pr_low": low,
        "pr_close": close,
        "pr_std": 0.1,
        "vol": vol,
        "val": val,
        "trades": trades,
        "pr_vwap": close,
        "pr_change": 0.0,
        "trades_b": trades_b,
        "trades_s": trades_s,
        "val_b": val_b,
        "val_s": val_s,
        "vol_b": vol_b,
        "vol_s": vol_s,
        "disb": 0.0,
        "pr_vwap_b": close,
        "pr_vwap_s": close,
        "im": im,
        "oi_open": oi_open,
        "oi_high": oi_high,
        "oi_low": oi_low,
        "oi_close": oi_close,
        "sec_pr_open": open_,
        "sec_pr_high": high,
        "sec_pr_low": low,
        "sec_pr_close": close,
        "SYSTIME": systime,
    }
    return [values[column] for column in COLUMNS]


def _page(
    rows: list[list[object]],
    *,
    start: int,
    total: int,
    page_size: int = 1000,
    columns: list[str] | None = None,
) -> bytes:
    return json.dumps(
        {
            "data": {
                "columns": columns or COLUMNS,
                "data": rows,
            },
            "data.cursor": {
                "columns": ["INDEX", "TOTAL", "PAGESIZE"],
                "data": [[start, total, page_size]],
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")


class Response:
    def __init__(self, payload: bytes = b'{"ok":true}') -> None:
        self.payload = payload

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def _identity() -> source.CnyrubfSecurityIdentity:
    return source.CnyrubfSecurityIdentity(
        source_id=source.SOURCE_ID,
        security_id=source.SECURITY_ID,
        asset_code=source.ASSET_CODE,
        board_id=source.BOARD_ID,
        engine=source.ENGINE,
        market=source.MARKET,
        primary_board=True,
        active_board=True,
        history_from=date(2022, 6, 1),
        history_till=None,
        metadata_route=source.build_security_metadata_url(),
        retrieved_at_utc=NOW,
        raw_payload_sha256="a" * 64,
        source_revision_status="official_iss_current_revision",
        historical_model_use_status=source.HISTORICAL_MODEL_USE_STATUS,
    )


def test_exact_fo_route_and_metadata_route() -> None:
    route = source.build_tradestats_url(START, END, start=17)
    parsed = urlsplit(route)
    assert parsed.scheme == "https"
    assert parsed.hostname == "apim.moex.com"
    assert parsed.path == "/iss/datashop/algopack/fo/tradestats/CNYRUBF.json"
    assert parse_qs(parsed.query) == {
        "from": ["2026-06-10"],
        "till": ["2026-06-10"],
        "start": ["17"],
    }
    metadata = urlsplit(source.build_security_metadata_url())
    assert metadata.hostname == "iss.moex.com"
    assert metadata.path == "/iss/securities/CNYRUBF.json"
    assert parse_qs(metadata.query) == {
        "iss.meta": ["off"],
        "iss.only": ["description,boards"],
    }


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/iss/datashop/algopack/fo/tradestats/CNYRUBF.json?from=2026-06-10&till=2026-06-10&start=0",
        "https://apim.moex.com/iss/datashop/algopack/fx/tradestats/CNYRUBF.json?from=2026-06-10&till=2026-06-10&start=0",
        "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/CNYRUB_TOM.json?from=2026-06-10&till=2026-06-10&start=0",
    ],
)
def test_non_allowlisted_trade_route_is_rejected_before_network(url: str) -> None:
    calls = 0

    def opener(*_args: object, **_kwargs: object) -> Response:
        nonlocal calls
        calls += 1
        return Response()

    with pytest.raises(source.CnyrubfAlgoPackError) as raised:
        source.fetch_algopack_bytes(url, TOKEN, opener=opener)
    assert raised.value.blocker == "provenance_not_sufficient"
    assert calls == 0


def test_metadata_description_columns_and_logical_secid_key() -> None:
    identity = source.parse_security_metadata_response(
        _metadata_payload(),
        route=source.build_security_metadata_url(),
        retrieved_at_utc=NOW,
    )
    assert identity.security_id == "CNYRUBF"
    assert identity.asset_code == "CNYRUBTOM"
    assert identity.board_id == "RFUD"
    assert identity.engine == "futures"
    assert identity.market == "forts"
    assert identity.primary_board is True
    assert identity.active_board is True


def test_metadata_rejects_spot_or_non_primary_identity() -> None:
    with pytest.raises(source.CnyrubfAlgoPackError) as raised:
        source.parse_security_metadata_response(
            _metadata_payload(
                secid="CNYRUB_TOM",
                boardid="CETS",
                engine="currency",
                market="selt",
            ),
            route=source.build_security_metadata_url(),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == "security_identity_not_reproducible"

    with pytest.raises(source.CnyrubfAlgoPackError) as raised:
        source.parse_security_metadata_response(
            _metadata_payload(is_primary=0),
            route=source.build_security_metadata_url(),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == "security_identity_not_reproducible"


def test_tradetime_is_bucket_end_and_systime_is_availability() -> None:
    rows, columns, cursor, digest = source.parse_tradestats_page_response(
        _page([_row()], start=0, total=1),
        from_date=START,
        till_date=END,
        start=0,
        route=source.build_tradestats_url(START, END),
        retrieved_at_utc=NOW,
    )
    assert tuple(columns) == tuple(COLUMNS)
    assert cursor == source.AlgoPackCursor(index=0, total=1, page_size=1000)
    assert len(digest) == 64
    row = rows[0]
    assert row.security_id == "CNYRUBF"
    assert row.asset_code == "CNYRUBTOM"
    assert row.bucket_end.isoformat() == "2026-06-10T09:05:00+03:00"
    assert row.bucket_begin.isoformat() == "2026-06-10T09:00:00+03:00"
    assert row.source_available_at.isoformat() == "2026-06-10T09:05:03+03:00"


@pytest.mark.parametrize(
    "row,blocker",
    [
        (_row(secid="CNYRUB_TOM"), "security_identity_not_reproducible"),
        (_row(asset_code="USDRUBTOM"), "security_identity_not_reproducible"),
        (_row(systime="2026-06-10 09:04:59"), "point_in_time_cutoff_not_provable"),
    ],
)
def test_substitution_and_precompletion_are_fail_closed(
    row: list[object],
    blocker: str,
) -> None:
    with pytest.raises(source.CnyrubfAlgoPackError) as raised:
        source.parse_tradestats_page_response(
            _page([row], start=0, total=1),
            from_date=START,
            till_date=END,
            start=0,
            route=source.build_tradestats_url(START, END),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == blocker


def test_daily_aggregation_includes_directional_and_open_interest_fields() -> None:
    payload = _page(
        [
            _row(),
            _row(
                tradetime="09:10:00",
                systime="2026-06-10 09:10:04",
                open_=12.1,
                high=12.4,
                low=12.0,
                close=12.3,
                vol=20.0,
                vol_b=8.0,
                vol_s=12.0,
                val=246.0,
                val_b=98.4,
                val_s=147.6,
                trades=8,
                trades_b=3,
                trades_s=5,
                im=1100.0,
                oi_open=105.0,
                oi_high=120.0,
                oi_low=100.0,
                oi_close=115.0,
            ),
        ],
        start=0,
        total=2,
    )

    def transport(_url: str, _token: str) -> bytes:
        return payload

    candles = source.load_daily_history(
        _identity(),
        from_date=START,
        till_date=END,
        bearer_token=TOKEN,
        transport=transport,
        sleeper=lambda _delay: None,
        clock=lambda: NOW,
    )
    assert len(candles) == 1
    candle = candles[0]
    assert candle.open == 12.0
    assert candle.close == 12.3
    assert candle.high == 12.4
    assert candle.low == 11.9
    assert candle.volume == 30.0
    assert candle.volume_buy == 14.0
    assert candle.volume_sell == 16.0
    assert candle.open_interest_open == 100.0
    assert candle.open_interest_high == 120.0
    assert candle.open_interest_low == 90.0
    assert candle.open_interest_close == 115.0
    assert candle.initial_margin_close == 1100.0
    assert candle.candle_begin.isoformat() == "2026-06-10T09:00:00+03:00"
    assert candle.candle_end.isoformat() == "2026-06-10T09:10:00+03:00"
    assert candle.source_available_at.isoformat() == "2026-06-10T09:10:04+03:00"


def test_retrieval_timestamp_is_recorded_after_final_page() -> None:
    pages = {
        0: _page([_row()], start=0, total=2, page_size=1),
        1: _page(
            [
                _row(
                    tradetime="09:10:00",
                    systime="2026-06-10 09:10:04",
                    open_=12.1,
                    high=12.4,
                    low=12.0,
                    close=12.3,
                    vol=20.0,
                    vol_b=8.0,
                    vol_s=12.0,
                    val=246.0,
                    val_b=98.4,
                    val_s=147.6,
                    trades=8,
                    trades_b=3,
                    trades_s=5,
                    im=1100.0,
                    oi_open=105.0,
                    oi_high=120.0,
                    oi_low=100.0,
                    oi_close=115.0,
                )
            ],
            start=1,
            total=2,
            page_size=1,
        ),
    }
    requested_starts: list[int] = []

    def transport(url: str, _token: str) -> bytes:
        start = int(parse_qs(urlsplit(url).query)["start"][0])
        requested_starts.append(start)
        return pages[start]

    clock_values = iter(
        [
            datetime(2026, 7, 29, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 29, 10, 1, tzinfo=timezone.utc),
            datetime(2026, 7, 29, 10, 2, tzinfo=timezone.utc),
        ]
    )
    candles = source.load_daily_history(
        _identity(),
        from_date=START,
        till_date=END,
        bearer_token=TOKEN,
        transport=transport,
        sleeper=lambda _delay: None,
        clock=lambda: next(clock_values),
    )
    assert requested_starts == [0, 1]
    assert candles[0].retrieved_at_utc == datetime(
        2026,
        7,
        29,
        10,
        2,
        tzinfo=timezone.utc,
    )


def test_prior_session_requires_exact_date_and_pre_anchor_availability() -> None:
    candle = source.CnyrubfAlgoPackDailyCandle(
        source_id=source.SOURCE_ID,
        security_id=source.SECURITY_ID,
        asset_code=source.ASSET_CODE,
        board_id=source.BOARD_ID,
        engine=source.ENGINE,
        market=source.MARKET,
        trade_date=date(2026, 6, 10),
        open=12.0,
        high=12.4,
        low=11.9,
        close=12.3,
        volume=30.0,
        volume_buy=14.0,
        volume_sell=16.0,
        volume_imbalance=-2.0 / 30.0,
        value=366.0,
        value_buy=170.4,
        value_sell=195.6,
        trades=13,
        trades_buy=6,
        trades_sell=7,
        initial_margin_close=1100.0,
        open_interest_open=100.0,
        open_interest_high=120.0,
        open_interest_low=90.0,
        open_interest_close=115.0,
        candle_begin=datetime(2026, 6, 10, 9, 0, tzinfo=source.MOSCOW),
        candle_end=datetime(2026, 6, 10, 23, 50, tzinfo=source.MOSCOW),
        source_available_at=datetime(2026, 6, 10, 23, 50, 3, tzinfo=source.MOSCOW),
        source_route=source.build_tradestats_url(START, END),
        retrieved_at_utc=NOW,
        raw_payload_sha256="b" * 64,
        source_revision_status=source.SOURCE_REVISION_STATUS,
        historical_model_use_status=source.HISTORICAL_MODEL_USE_STATUS,
    )
    source.validate_prior_session_candle(
        candle,
        target_trade_date=date(2026, 6, 11),
        prior_trade_date=date(2026, 6, 10),
    )
    with pytest.raises(source.CnyrubfAlgoPackError) as raised:
        source.validate_prior_session_candle(
            candle,
            target_trade_date=date(2026, 6, 10),
            prior_trade_date=date(2026, 6, 10),
        )
    assert raised.value.blocker == "point_in_time_cutoff_not_provable"


def test_redirect_rejection_and_token_sanitization() -> None:
    request = Request(
        source.build_tradestats_url(START, END),
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert (
        source._RejectAllRedirects().redirect_request(
            request,
            None,
            302,
            "Found",
            Message(),
            "https://evil.example/collect",
        )
        is None
    )

    headers = Message()
    error = HTTPError(
        source.build_tradestats_url(START, END),
        401,
        "sanitized",
        headers,
        None,
    )

    def opener(_request: Request, timeout: int) -> Response:
        assert timeout == 30
        raise error

    with pytest.raises(source.CnyrubfAlgoPackError) as raised:
        source.fetch_algopack_bytes(
            source.build_tradestats_url(START, END),
            TOKEN,
            opener=opener,
        )
    assert raised.value.blocker == "algopack_authentication_failed"
    assert TOKEN not in str(raised.value)
    assert TOKEN not in repr(raised.value)



@pytest.mark.parametrize("im", [None, ""])
def test_nullable_initial_margin_is_preserved_without_fill(im: object) -> None:
    payload = _page([_row(im=im)], start=0, total=1)
    rows, _, _, digest = source.parse_tradestats_page_response(
        payload,
        from_date=START,
        till_date=END,
        start=0,
        route=source.build_tradestats_url(START, END),
        retrieved_at_utc=NOW,
    )
    assert rows[0].initial_margin is None

    candles = source.aggregate_daily_tradestats(
        rows,
        source_route=source.build_tradestats_url(START, END),
        retrieved_at_utc=NOW,
        raw_payload_sha256=digest,
    )
    assert candles[0].initial_margin_close is None


@pytest.mark.parametrize("im", [-1.0, float("inf"), "not-a-number", " "])
def test_invalid_nonempty_initial_margin_remains_fail_closed(im: object) -> None:
    with pytest.raises(source.CnyrubfAlgoPackError) as raised:
        source.parse_tradestats_page_response(
            _page([_row(im=im)], start=0, total=1),
            from_date=START,
            till_date=END,
            start=0,
            route=source.build_tradestats_url(START, END),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == "numerical_or_chronology_integrity_failure"
