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


def _market_row(secid: str, systime: str = "2026-09-02 13:00:00") -> list[object]:
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
        systime,
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


def test_final_snapshot_keeps_row_receipts_but_uses_common_freshness_reference() -> None:
    page0 = {
        "securities": {
            "columns": list(live.FUTURES_SECURITY_COLUMNS),
            "data": [
                ["USDRUBF", "RFUD", "2099-12-31", 0.01, 10.0],
                ["SiU6", "RFUD", "2026-09-17", 1.0, 1.0],
                ["CRU6", "RFUD", "2026-09-17", 0.001, 1.0],
                ["OTHER1", "RFUD", "2026-09-17", 1.0, 1.0],
            ],
        },
        "marketdata": {
            "columns": list(live.FUTURES_MARKETDATA_COLUMNS),
            "data": [
                _market_row("USDRUBF", "2026-09-02 13:00:00"),
                _market_row("SiU6", "2026-09-02 13:00:02"),
                _market_row("CRU6", "2026-09-02 13:00:04"),
            ],
        },
        "securities.cursor": {
            "columns": ["INDEX", "TOTAL", "PAGESIZE"],
            "data": [[0, 8, 4]],
        },
    }
    page4 = {
        "securities": {
            "columns": list(live.FUTURES_SECURITY_COLUMNS),
            "data": [
                ["CNYRUBF", "RFUD", "2099-12-31", 0.001, 1.0],
                ["SiZ6", "RFUD", "2026-12-17", 1.0, 1.0],
                ["CRZ6", "RFUD", "2026-12-17", 0.001, 1.0],
                ["OTHER2", "RFUD", "2026-12-17", 1.0, 1.0],
            ],
        },
        "marketdata": {
            "columns": list(live.FUTURES_MARKETDATA_COLUMNS),
            "data": [
                _market_row("CNYRUBF", "2026-09-02 13:00:06"),
                _market_row("SiZ6", "2026-09-02 13:00:08"),
                _market_row("CRZ6", "2026-09-02 13:00:10"),
            ],
        },
        "securities.cursor": {
            "columns": ["INDEX", "TOTAL", "PAGESIZE"],
            "data": [[4, 8, 4]],
        },
    }
    receipt_times = iter(
        [
            datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
            datetime(2026, 9, 2, 10, 0, 25, tzinfo=timezone.utc),
        ]
    )

    def http_get(url: str, *, params, timeout, headers, allow_redirects):
        return _Response(page0 if int(params.get("start", 0)) == 0 else page4, url)

    forts_payload, source_url, completion = live._fetch_forts_all_pages(
        url="https://example.test/forts",
        params={"iss.only": "securities,marketdata,securities.cursor"},
        headers={"Authorization": "Bearer test"},
        timeout=1.0,
        http_get=http_get,
        now_fn=lambda: next(receipt_times),
    )
    cets_payload = {
        "marketdata": {
            "columns": list(live.CETS_MARKETDATA_COLUMNS),
            "data": [[
                "CNYRUB_TOM",
                12.0,
                12.2,
                11.9,
                12.1,
                12.05,
                1000,
                20,
                12.09,
                12.11,
                "2026-09-02 13:00:12",
            ]],
        }
    }

    snapshot = live.build_snapshot_from_payloads(
        forts_payload=forts_payload,
        cets_payload=cets_payload,
        forts_received_at_utc=completion,
        cets_received_at_utc="2026-09-02T10:00:26+00:00",
        forts_source_url=source_url,
    )

    assert snapshot["status"] == "READY"
    assert snapshot["instruments"]["usdrubf"]["received_at_utc"] == "2026-09-02T10:00:20+00:00"
    assert snapshot["instruments"]["si_front"]["received_at_utc"] == "2026-09-02T10:00:20+00:00"
    assert snapshot["instruments"]["cnyrubf"]["received_at_utc"] == "2026-09-02T10:00:25+00:00"
    assert snapshot["instruments"]["si_next"]["received_at_utc"] == "2026-09-02T10:00:25+00:00"
    assert {
        item["freshness_reference_utc"]
        for item in snapshot["instruments"].values()
    } == {"2026-09-02T10:00:26+00:00"}
