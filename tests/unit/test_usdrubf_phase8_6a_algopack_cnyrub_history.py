from __future__ import annotations

import json
from datetime import date, datetime, timezone
from urllib.parse import parse_qs, urlsplit

import pytest

from moex_research.external_data import moex_cnyrub_algopack_history as source

NOW = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
START = date(2024, 8, 1)
END = date(2024, 8, 2)
COLUMNS = [
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
    "sec_pr_open",
    "sec_pr_high",
    "sec_pr_low",
    "sec_pr_close",
    "SYSTIME",
]


def _identity() -> source.CnyrubSecurityIdentity:
    return source.CnyrubSecurityIdentity(
        source_id="moex_cnyrub_tom_daily",
        security_id="CNYRUB_TOM",
        board_id="CETS",
        engine="currency",
        market="selt",
        primary_board=True,
        active_board=True,
        history_from=date(2010, 9, 27),
        history_till=None,
        metadata_route=source.build_security_metadata_url(),
        retrieved_at_utc=NOW,
        raw_payload_sha256="a" * 64,
        source_revision_status="official_iss_current_revision",
        historical_model_use_status="source_validation_only",
    )


def _row(
    day: str,
    time: str,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    vol: int,
    vol_b: int,
    vol_s: int,
    val: float,
    val_b: float,
    val_s: float,
    trades: int,
    trades_b: int,
    trades_s: int,
    secid: str = "CNYRUB_TOM",
) -> list[object]:
    return [
        day,
        time,
        secid,
        open_,
        high,
        low,
        close,
        vol,
        val,
        trades,
        close,
        0,
        trades_b,
        trades_s,
        val_b,
        val_s,
        vol_b,
        vol_s,
        0,
        close,
        close,
        1,
        1,
        1,
        1,
        f"{day} {time}",
    ]


def _payload(
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


def test_exact_algopack_route_and_query() -> None:
    url = source.build_tradestats_url(START, END, start=17)
    parsed = urlsplit(url)
    assert parsed.scheme == "https"
    assert parsed.hostname == "apim.moex.com"
    assert parsed.path == (
        "/iss/datashop/algopack/fx/tradestats/CNYRUB_TOM.json"
    )
    assert parse_qs(parsed.query) == {
        "from": ["2024-08-01"],
        "till": ["2024-08-02"],
        "start": ["17"],
    }


def test_fetch_uses_bearer_header_without_token_in_url() -> None:
    captured: dict[str, object] = {}

    class Response:
        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"ok":true}'

    def opener(request: object, timeout: int) -> Response:
        captured["request"] = request
        captured["timeout"] = timeout
        return Response()

    url = source.build_tradestats_url(START, END)
    assert source.fetch_algopack_bytes(
        url,
        "secret-token",
        opener=opener,
    ) == b'{"ok":true}'
    request = captured["request"]
    assert request.get_header("Authorization") == "Bearer secret-token"
    assert "secret-token" not in request.full_url
    assert captured["timeout"] == 30


def test_unauthorized_is_structured_and_not_retried() -> None:
    calls = 0

    def transport(_url: str, _token: str) -> bytes:
        nonlocal calls
        calls += 1
        raise source.CnyrubAlgoPackError(
            "AlgoPack subscription authorization failed",
            blocker="algopack_authorization_failed",
        )

    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.fetch_algopack_bytes_with_retry(
            source.build_tradestats_url(START, END),
            "token",
            transport=transport,
            sleeper=lambda _delay: pytest.fail("authorization must not retry"),
        )
    assert raised.value.blocker == "algopack_authorization_failed"
    assert calls == 1


def test_bounded_retry_preserves_exact_route() -> None:
    calls: list[str] = []
    delays: list[float] = []

    def transport(url: str, _token: str) -> bytes:
        calls.append(url)
        if len(calls) < 3:
            raise source.ExternalDataError("external-data request failed")
        return b"ok"

    route = source.build_tradestats_url(START, END)
    assert source.fetch_algopack_bytes_with_retry(
        route,
        "token",
        transport=transport,
        sleeper=delays.append,
    ) == b"ok"
    assert calls == [route, route, route]
    assert delays == [0.5, 1.0]


def test_pagination_and_daily_directional_aggregation() -> None:
    rows = [
        _row(
            "2024-08-01",
            "10:00:00",
            open_=11.0,
            high=12.0,
            low=10.5,
            close=11.5,
            vol=10,
            vol_b=6,
            vol_s=4,
            val=110,
            val_b=66,
            val_s=44,
            trades=5,
            trades_b=3,
            trades_s=2,
        ),
        _row(
            "2024-08-01",
            "10:05:00",
            open_=11.5,
            high=12.5,
            low=11.0,
            close=12.0,
            vol=20,
            vol_b=8,
            vol_s=12,
            val=230,
            val_b=92,
            val_s=138,
            trades=9,
            trades_b=4,
            trades_s=5,
        ),
        _row(
            "2024-08-02",
            "10:00:00",
            open_=12.0,
            high=12.2,
            low=11.8,
            close=12.1,
            vol=4,
            vol_b=1,
            vol_s=3,
            val=48,
            val_b=12,
            val_s=36,
            trades=2,
            trades_b=1,
            trades_s=1,
        ),
    ]
    calls: list[int] = []

    def transport(url: str, token: str) -> bytes:
        assert token == "token"
        start = int(parse_qs(urlsplit(url).query)["start"][0])
        calls.append(start)
        if start == 0:
            return _payload(rows[:2], start=0, total=3, page_size=2)
        if start == 2:
            return _payload(rows[2:], start=2, total=3, page_size=2)
        raise AssertionError(f"unexpected start {start}")

    daily = source.load_daily_history(
        _identity(),
        from_date=START,
        till_date=END,
        bearer_token="token",
        transport=transport,
        sleeper=lambda _delay: None,
        clock=lambda: NOW,
    )
    assert calls == [0, 2]
    assert [item.trade_date for item in daily] == [START, END]
    first = daily[0]
    assert (first.open, first.high, first.low, first.close) == (
        11.0,
        12.5,
        10.5,
        12.0,
    )
    assert (first.volume, first.volume_buy, first.volume_sell) == (
        30,
        14,
        16,
    )
    assert first.volume_imbalance == pytest.approx(-2 / 30)
    assert (first.value, first.value_buy, first.value_sell) == (
        340,
        158,
        182,
    )
    assert (first.trades, first.trades_buy, first.trades_sell) == (
        14,
        7,
        7,
    )
    assert first.candle_end.isoformat() == "2024-08-01T10:10:00+03:00"
    assert first.source_revision_status == "algopack_fx_tradestats_5m"
    assert len(first.raw_payload_sha256) == 64


@pytest.mark.parametrize(
    "field,bad_value",
    [
        ("secid", "USDRUB_TOM"),
        ("vol_b", 7),
        ("val_s", 46),
        ("trades_s", 3),
    ],
)
def test_substitution_or_directional_identity_failure(
    field: str,
    bad_value: object,
) -> None:
    row = _row(
        "2024-08-01",
        "10:00:00",
        open_=11.0,
        high=12.0,
        low=10.0,
        close=11.5,
        vol=10,
        vol_b=6,
        vol_s=4,
        val=110,
        val_b=66,
        val_s=44,
        trades=5,
        trades_b=3,
        trades_s=2,
    )
    index = COLUMNS.index(field)
    row[index] = bad_value
    route = source.build_tradestats_url(START, END)
    with pytest.raises(source.CnyrubAlgoPackError):
        source.parse_tradestats_page_response(
            _payload([row], start=0, total=1),
            from_date=START,
            till_date=END,
            start=0,
            route=route,
            retrieved_at_utc=NOW,
        )


def test_missing_token_is_structured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(source.ALGOPACK_TOKEN_ENV, raising=False)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.load_algopack_token()
    assert raised.value.blocker == "algopack_subscription_not_available"


def test_prior_session_cutoff_is_fail_closed() -> None:
    row = _row(
        "2024-08-01",
        "10:00:00",
        open_=11.0,
        high=12.0,
        low=10.0,
        close=11.5,
        vol=10,
        vol_b=6,
        vol_s=4,
        val=110,
        val_b=66,
        val_s=44,
        trades=5,
        trades_b=3,
        trades_s=2,
    )
    parsed, _, _, digest = source.parse_tradestats_page_response(
        _payload([row], start=0, total=1),
        from_date=START,
        till_date=END,
        start=0,
        route=source.build_tradestats_url(START, END),
        retrieved_at_utc=NOW,
    )
    candle = source.aggregate_daily_tradestats(
        parsed,
        source_route=source.build_tradestats_url(START, END),
        retrieved_at_utc=NOW,
        raw_payload_sha256=digest,
    )[0]
    source.validate_prior_session_candle(
        candle,
        target_trade_date=date(2024, 8, 2),
        prior_trade_date=date(2024, 8, 1),
    )
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.validate_prior_session_candle(
            candle,
            target_trade_date=date(2024, 8, 1),
            prior_trade_date=date(2024, 8, 1),
        )
    assert raised.value.blocker == "point_in_time_cutoff_not_provable"
