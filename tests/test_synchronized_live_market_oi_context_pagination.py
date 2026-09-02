from __future__ import annotations

from datetime import datetime, timezone

import pytest

from moex_data import synchronized_live_market_oi_context as live


class _Response:
    def __init__(
        self,
        payload: dict[str, object],
        url: str,
        *,
        status_code: int = 200,
    ) -> None:
        self._payload = payload
        self.url = url
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _market_row(secid: str, systime: str, *, oi: int, rub_per_quote_unit: float) -> list[object]:
    volume = 100
    wap = 90.5
    value_rub = int(wap * volume * rub_per_quote_unit)
    return [
        secid,
        90.0,
        92.0,
        89.0,
        91.0,
        volume,
        value_rub,
        10,
        oi,
        90.9,
        91.1,
        systime,
    ]


def _page(
    *,
    index: int,
    total: int,
    page_size: int,
    securities: list[list[object]],
    marketdata: list[list[object]],
) -> dict[str, object]:
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
            "data": [[index, total, page_size]],
        },
    }


def _cets_payload() -> dict[str, object]:
    return {
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


def test_forts_pagination_collects_required_secids_from_later_page() -> None:
    page0 = _page(
        index=0,
        total=8,
        page_size=4,
        securities=[
            ["SiU6", "RFUD", "2026-09-17", 1.0, 1.0],
            ["CRU6", "RFUD", "2026-09-17", 0.001, 1.0],
            ["OTHER1", "RFUD", "2026-09-17", 1.0, 1.0],
            ["OTHER2", "RFUD", "2026-09-17", 1.0, 1.0],
        ],
        marketdata=[
            _market_row("SiU6", "2026-09-02 13:00:04", oi=70000, rub_per_quote_unit=1),
            _market_row("CRU6", "2026-09-02 13:00:08", oi=40000, rub_per_quote_unit=1000),
        ],
    )
    page4 = _page(
        index=4,
        total=8,
        page_size=4,
        securities=[
            ["USDRUBF", "RFUD", "2099-12-31", 0.01, 10.0],
            ["CNYRUBF", "RFUD", "2099-12-31", 0.001, 1.0],
            ["SiZ6", "RFUD", "2026-12-17", 1.0, 1.0],
            ["CRZ6", "RFUD", "2026-12-17", 0.001, 1.0],
        ],
        marketdata=[
            _market_row("USDRUBF", "2026-09-02 13:00:00", oi=50000, rub_per_quote_unit=1000),
            _market_row("CNYRUBF", "2026-09-02 13:00:02", oi=60000, rub_per_quote_unit=1000),
            _market_row("SiZ6", "2026-09-02 13:00:06", oi=30000, rub_per_quote_unit=1),
            _market_row("CRZ6", "2026-09-02 13:00:10", oi=20000, rub_per_quote_unit=1000),
        ],
    )
    calls: list[dict[str, object]] = []

    def http_get(
        url: str,
        *,
        params: dict[str, object],
        timeout: float,
        headers: dict[str, str],
        allow_redirects: bool,
    ) -> _Response:
        assert allow_redirects is False
        calls.append(dict(params))
        start = int(params.get("start", 0))
        if start == 0:
            return _Response(page0, url + "?page=0")
        if start == 4:
            return _Response(page4, url + "?page=4")
        raise AssertionError(f"unexpected start={start}")

    forts_payload, source_url, received_at = live._fetch_forts_all_pages(
        url="https://example.test/forts",
        params={
            "iss.meta": "off",
            "iss.only": "securities,marketdata,securities.cursor",
            "securities.columns": ",".join(live.FUTURES_SECURITY_COLUMNS),
            "marketdata.columns": ",".join(live.FUTURES_MARKETDATA_COLUMNS),
        },
        headers={"Authorization": "Bearer test"},
        timeout=1.0,
        http_get=http_get,
        now_fn=lambda: datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
    )

    assert [call.get("start") for call in calls] == [None, 4]
    assert source_url.endswith("?page=4")
    assert received_at == datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc)
    assert {row[0] for row in forts_payload["securities"]["data"]} >= {
        "USDRUBF", "CNYRUBF", "SiU6", "SiZ6", "CRU6", "CRZ6"
    }

    snapshot = live.build_snapshot_from_payloads(
        forts_payload=forts_payload,
        cets_payload=_cets_payload(),
        forts_received_at_utc=received_at,
        cets_received_at_utc="2026-09-02T10:00:22+00:00",
    )
    assert snapshot["status"] == "READY"
    assert snapshot["bindings"] == {
        "usdrubf": "USDRUBF",
        "si_front": "SiU6",
        "si_next": "SiZ6",
        "cnyrubf": "CNYRUBF",
        "cr_front": "CRU6",
        "cr_next": "CRZ6",
        "cnyrub_tom": "CNYRUB_TOM",
    }
    assert snapshot["provenance"]["forts"]["pagination_complete"] is True


def test_forts_pagination_fails_closed_without_cursor() -> None:
    payload = {
        "securities": {"columns": list(live.FUTURES_SECURITY_COLUMNS), "data": []},
        "marketdata": {"columns": list(live.FUTURES_MARKETDATA_COLUMNS), "data": []},
    }

    def http_get(
        url: str,
        *,
        params: dict[str, object],
        timeout: float,
        headers: dict[str, str],
        allow_redirects: bool,
    ) -> _Response:
        assert allow_redirects is False
        return _Response(payload, url)

    with pytest.raises(live.SynchronizedLiveMarketOIError, match="pagination completeness is unproven"):
        live._fetch_forts_all_pages(
            url="https://example.test/forts",
            params={"iss.only": "securities,marketdata,securities.cursor"},
            headers={"Authorization": "Bearer test"},
            timeout=1.0,
            http_get=http_get,
            now_fn=lambda: datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
        )


def test_authenticated_source_redirect_is_rejected() -> None:
    def http_get(
        url: str,
        *,
        params: dict[str, object],
        timeout: float,
        headers: dict[str, str],
        allow_redirects: bool,
    ) -> _Response:
        assert allow_redirects is False
        return _Response({}, "https://iss.moex.com/public", status_code=302)

    with pytest.raises(live.SynchronizedLiveMarketOIError, match="redirect rejected"):
        live._fetch_json(
            url="https://apim.moex.com/iss/example.json",
            params={},
            headers={"Authorization": "Bearer test"},
            timeout=1.0,
            http_get=http_get,
            now_fn=lambda: datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
        )


def test_authenticated_source_route_change_is_rejected_even_on_200() -> None:
    def http_get(
        url: str,
        *,
        params: dict[str, object],
        timeout: float,
        headers: dict[str, str],
        allow_redirects: bool,
    ) -> _Response:
        assert allow_redirects is False
        return _Response({}, "https://iss.moex.com/iss/example.json")

    with pytest.raises(live.SynchronizedLiveMarketOIError, match="changed route"):
        live._fetch_json(
            url="https://apim.moex.com/iss/example.json",
            params={},
            headers={"Authorization": "Bearer test"},
            timeout=1.0,
            http_get=http_get,
            now_fn=lambda: datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
        )


def test_forts_pagination_rejects_total_change() -> None:
    page0 = _page(
        index=0,
        total=4,
        page_size=2,
        securities=[
            ["A", "RFUD", "2026-09-17", 1.0, 1.0],
            ["B", "RFUD", "2026-09-17", 1.0, 1.0],
        ],
        marketdata=[],
    )
    page2 = _page(
        index=2,
        total=5,
        page_size=2,
        securities=[
            ["C", "RFUD", "2026-09-17", 1.0, 1.0],
            ["D", "RFUD", "2026-09-17", 1.0, 1.0],
        ],
        marketdata=[],
    )

    def http_get(
        url: str,
        *,
        params: dict[str, object],
        timeout: float,
        headers: dict[str, str],
        allow_redirects: bool,
    ) -> _Response:
        return _Response(page0 if int(params.get("start", 0)) == 0 else page2, url)

    with pytest.raises(live.SynchronizedLiveMarketOIError, match="TOTAL changed"):
        live._fetch_forts_all_pages(
            url="https://example.test/forts",
            params={"iss.only": "securities,marketdata,securities.cursor"},
            headers={"Authorization": "Bearer test"},
            timeout=1.0,
            http_get=http_get,
            now_fn=lambda: datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
        )


def test_forts_pagination_rejects_truncated_page() -> None:
    truncated = _page(
        index=0,
        total=4,
        page_size=2,
        securities=[["A", "RFUD", "2026-09-17", 1.0, 1.0]],
        marketdata=[],
    )

    def http_get(
        url: str,
        *,
        params: dict[str, object],
        timeout: float,
        headers: dict[str, str],
        allow_redirects: bool,
    ) -> _Response:
        return _Response(truncated, url)

    with pytest.raises(live.SynchronizedLiveMarketOIError, match="page cardinality mismatch"):
        live._fetch_forts_all_pages(
            url="https://example.test/forts",
            params={"iss.only": "securities,marketdata,securities.cursor"},
            headers={"Authorization": "Bearer test"},
            timeout=1.0,
            http_get=http_get,
            now_fn=lambda: datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
        )
