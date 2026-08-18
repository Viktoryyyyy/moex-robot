from __future__ import annotations

from moex_data.futures import materialize_futoi_instrument as target


def test_fetch_exact_selects_futoi_data_block_not_futoi_dates(monkeypatch) -> None:
    def fake_request_json(base_url, path, params, timeout, use_apim):
        return {
            "futoi.dates": {
                "columns": ["from", "till"],
                "data": [["2015-01-01", "2026-08-17"]],
            },
            "futoi": {
                "columns": [
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
                ],
                "data": [[
                    1,
                    101,
                    "2026-08-17",
                    "23:50:00",
                    "USDRUBF",
                    "FIZ",
                    10.0,
                    100.0,
                    -90.0,
                    10,
                    9,
                    "2026-08-17 23:51:00",
                    "2026-08-17",
                ]],
            },
        }

    monkeypatch.setattr(target.availability, "request_json", fake_request_json)

    frame, source_url = target._fetch_exact(
        "usdrubf",
        "2026-08-17",
        5.0,
        "https://iss.moex.com",
    )

    assert "tradedate" in frame.columns
    assert "tradetime" in frame.columns
    assert "from" not in frame.columns
    assert frame.iloc[0]["ticker"] == "USDRUBF"
    assert source_url.endswith("/iss/analyticalproducts/futoi/securities/usdrubf.json")
