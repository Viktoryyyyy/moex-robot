from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest

from moex_research.external_data import moex_cnyrub_algopack_history as source
from moex_research.external_data import moex_cnyrub_algopack_timestamp_policy as policy

TRADE_DATE = date(2026, 6, 10)
NOW = datetime(2026, 7, 22, 11, 0, tzinfo=timezone.utc)
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
    "trades_b",
    "trades_s",
    "val_b",
    "val_s",
    "vol_b",
    "vol_s",
    "SYSTIME",
]


def _row(*, systime: str) -> list[object]:
    return [
        "2026-06-10",
        "10:00:00",
        "CNYRUB_TOM",
        11.0,
        12.0,
        10.0,
        11.5,
        10,
        110,
        5,
        3,
        2,
        66,
        44,
        6,
        4,
        systime,
    ]


def _payload(row: list[object]) -> bytes:
    return json.dumps(
        {
            "data": {"columns": COLUMNS, "data": [row]},
            "data.cursor": {
                "columns": ["INDEX", "TOTAL", "PAGESIZE"],
                "data": [[0, 1, 1000]],
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")


def _parse(systime: str) -> list[source.AlgoPackTradeStat]:
    rows, _, _, _ = policy.parse_tradestats_page_response(
        _payload(_row(systime=systime)),
        from_date=TRADE_DATE,
        till_date=TRADE_DATE,
        start=0,
        route=source.build_tradestats_url(TRADE_DATE, TRADE_DATE),
        retrieved_at_utc=NOW,
    )
    return rows


def test_live_shaped_systime_after_tradetime_is_accepted() -> None:
    rows = _parse("2026-06-10 10:00:20")

    assert len(rows) == 1
    assert rows[0].bucket_begin.isoformat() == "2026-06-10T09:55:00+03:00"
    assert rows[0].source_available_at.isoformat() == "2026-06-10T10:00:20+03:00"

    candle = source.aggregate_daily_tradestats(
        rows,
        source_route=source.build_tradestats_url(TRADE_DATE, TRADE_DATE),
        retrieved_at_utc=NOW,
        raw_payload_sha256="a" * 64,
    )[0]
    assert candle.candle_begin.isoformat() == "2026-06-10T09:55:00+03:00"
    assert candle.candle_end.isoformat() == "2026-06-10T10:00:00+03:00"
    assert candle.source_available_at.isoformat() == "2026-06-10T10:00:20+03:00"


def test_systime_before_completed_bucket_end_is_rejected() -> None:
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        _parse("2026-06-10 09:59:59")

    assert raised.value.blocker == "point_in_time_cutoff_not_provable"
    assert "completed provider bucket" in str(raised.value)


def test_install_replaces_only_the_parser(monkeypatch) -> None:
    original_load = source.load_daily_history
    monkeypatch.setattr(
        source,
        "parse_tradestats_page_response",
        lambda *_args, **_kwargs: None,
    )

    policy.install_timestamp_policy()

    assert source.parse_tradestats_page_response is policy.parse_tradestats_page_response
    assert source.load_daily_history is original_load
