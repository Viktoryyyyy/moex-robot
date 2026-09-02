from __future__ import annotations

from datetime import datetime, timezone

from moex_data import synchronized_live_market_oi_context as live


class _Response:
    def __init__(self, payload: dict[str, object], url: str) -> None:
        self._payload = payload
        self.url = url
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _page(index: int, securities: list[list[object]], marketdata: list[list[object]]) -> dict[str, object]:
    return {
        "securities": {
            "columns": list(live.FUTURES_SECURITY_COLUMNS),
            "data": securities,
        },
        "marketdata": {
            "columns": list(live.FUTURES_MARKETDATA_COLUMNS),
            "data": marketdata,
        },
        "securities.cursor": {
            "columns": ["INDEX", "TOTAL", "PAGESIZE"],
            "data": [[index, 2, 1]],
        },
    }


def _market_row(secid: str) -> list[object]:
    return [
        secid,
        90.0,
        92.0,
        89.0,
        91.0,
        100,
        9_100,
        10,
        1000,
        90.9,
        91.1,
        "2026-09-02 13:00:00",
    ]


def test_forts_pagination_preserves_actual_marketdata_receipt_by_secid() -> None:
    page0 = _page(
        0,
        [["A", "RFUD", "2026-09-17", 1.0, 1.0]],
        [_market_row("A")],
    )
    page1 = _page(
        1,
        [["B", "RFUD", "2026-09-17", 1.0, 1.0]],
        [_market_row("B")],
    )
    receipt_times = iter(
        [
            datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
            datetime(2026, 9, 2, 10, 0, 25, tzinfo=timezone.utc),
        ]
    )

    def http_get(url: str, *, params, timeout, headers, allow_redirects):
        assert allow_redirects is False
        return _Response(page0 if int(params.get("start", 0)) == 0 else page1, url)

    payload, _source_url, completion = live._fetch_forts_all_pages(
        url="https://example.test/forts",
        params={"iss.only": "securities,marketdata,securities.cursor"},
        headers={"Authorization": "Bearer test"},
        timeout=1.0,
        http_get=http_get,
        now_fn=lambda: next(receipt_times),
    )

    receipts = payload[live.FORTS_ROW_RECEIPTS_KEY]
    assert receipts == {
        "A": "2026-09-02T10:00:20+00:00",
        "B": "2026-09-02T10:00:25+00:00",
    }
    assert completion == datetime(2026, 9, 2, 10, 0, 25, tzinfo=timezone.utc)
