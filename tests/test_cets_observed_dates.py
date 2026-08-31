from __future__ import annotations

import pytest

from moex_data.currency import cets_observed_dates as observed


class FakeResponse:
    def __init__(self, payload: dict[str, object], url: str) -> None:
        self._payload = payload
        self.url = url

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _payload(trade_date: str | None) -> dict[str, object]:
    columns = ["open", "high", "low", "close", "volume", "begin", "end"]
    rows = []
    if trade_date is not None:
        rows = [[1.0, 1.0, 1.0, 1.0, 1.0, trade_date + " 10:00:00", trade_date + " 10:00:59"]]
    return {"candles": {"columns": columns, "data": rows}}


def test_common_observed_dates_require_both_canonical_cets_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_get(url, *, params, timeout, headers):
        calls.append((str(url), dict(params)))
        secid = str(url).split("/securities/", 1)[1].split("/", 1)[0]
        trade_date = str(params["from"])
        if trade_date == "2026-08-30":
            return FakeResponse(_payload(None), str(url))
        if secid in {"USD000UTSTOM", "CNYRUB_TOM"}:
            return FakeResponse(_payload(trade_date), str(url))
        raise AssertionError("unexpected secid")

    monkeypatch.setattr(observed.requests, "get", fake_get)
    result = observed.observed_common_dates(["2026-08-29", "2026-08-30"], timeout=1.0)

    assert result == ["2026-08-29"]
    assert calls
    for _url, params in calls:
        assert params["from"] == params["till"]
        assert params["interval"] == 1
        assert params["start"] == 0
        assert params["iss.meta"] == "off"
        assert params["iss.only"] == "candles"


def test_observed_date_probe_fails_closed_on_wrong_returned_date(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        observed.requests,
        "get",
        lambda url, *, params, timeout, headers: FakeResponse(_payload("2026-08-29"), str(url)),
    )
    with pytest.raises(observed.CetsObservedDateError, match="outside requested date"):
        observed.observed_common_dates(["2026-08-30"], timeout=1.0)
