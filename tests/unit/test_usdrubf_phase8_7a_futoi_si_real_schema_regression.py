from __future__ import annotations

import json
from datetime import date, datetime, timezone

from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_source_validation as source,
)


def test_actual_moex_futoi_schema_with_dates_service_block() -> None:
    trade_date = date(2026, 7, 29)
    columns = [
        "sess_id",
        "seqnum",
        "tradedate",
        "tradetime",
        "ticker",
        "clgroup",
        "pos",
        "pos_long",
        "pos_short",
        "pos_long_num",
        "pos_short_num",
        "systime",
        "trade_session_date",
    ]
    payload = json.dumps(
        {
            "futoi": {
                "columns": columns,
                "data": [
                    [
                        1,
                        101,
                        "2026-07-29",
                        "23:50:00",
                        "Si",
                        "FIZ",
                        10.0,
                        100.0,
                        -90.0,
                        10,
                        9,
                        "2026-07-29 23:51:00",
                        "2026-07-29",
                    ],
                    [
                        1,
                        102,
                        "2026-07-29",
                        "23:50:00",
                        "Si",
                        "YUR",
                        -10.0,
                        80.0,
                        -90.0,
                        8,
                        9,
                        "2026-07-29 23:51:00",
                        "2026-07-29",
                    ],
                ],
            },
            "futoi.dates": {
                "columns": ["from", "till"],
                "data": [["2015-01-01", "2026-07-29"]],
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")

    pair, parsed_columns = source.parse_futoi_daily_response(
        payload,
        trade_date=trade_date,
        route=source.build_futoi_url(trade_date),
        retrieved_at_utc=datetime(2026, 7, 30, 3, 0, tzinfo=timezone.utc),
    )

    assert tuple(parsed_columns) == tuple(columns)
    assert pair.trade_date == trade_date
    assert pair.source_ticker == "Si"
    assert pair.moment.isoformat() == "2026-07-29T23:50:00+03:00"
    assert pair.fiz_pos == 10.0
    assert pair.yur_pos == -10.0
    assert pair.fiz_seqnum == 101
    assert pair.yur_seqnum == 102
