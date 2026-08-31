from __future__ import annotations

import pytest

from moex_data.futures import observed_tradestats_dates as observed


class FakeResponse:
    def __init__(self, payload: dict[str, object], url: str = "https://apim.moex.com/mock") -> None:
        self._payload = payload
        self.url = url

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _payload(rows: list[list[object]]) -> dict[str, object]:
    return {
        "tradestats": {
            "columns": ["SECID", "TRADEDATE"],
            "data": rows,
        }
    }


def test_observed_dates_scan_each_calendar_date_with_exact_date_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-key")
    monkeypatch.setenv("MOEX_API_URL", "https://apim.moex.com")
    monkeypatch.setattr(observed, "reference_secid", lambda *_args, **_kwargs: "SiU6")
    calls: list[dict[str, object]] = []

    data_dates = {"2026-06-12", "2026-06-15"}

    def fake_get(url, *, params, headers, timeout):
        calls.append(dict(params))
        request_date = str(params["date"])
        if int(params["start"]) > 0:
            return FakeResponse(_payload([]), str(url))
        if request_date in data_dates:
            return FakeResponse(_payload([["SiU6", request_date]]), str(url))
        return FakeResponse(_payload([]), str(url))

    monkeypatch.setattr(observed.tradestats.requests, "get", fake_get)

    result = observed.observed_dates(
        "2026-06-12",
        "2026-06-15",
        instrument_id="si_futures_family",
        timeout=1.0,
    )

    assert result == ["2026-06-12", "2026-06-15"]
    first_requests = [call for call in calls if int(call["start"]) == 0]
    assert [str(call["date"]) for call in first_requests] == [
        "2026-06-12",
        "2026-06-13",
        "2026-06-14",
        "2026-06-15",
    ]
    for call in first_requests:
        assert call["date"] == call["from"] == call["till"]
        assert call["secid"] == "SiU6"
        assert call["iss.only"] == "tradestats"


def test_observed_dates_do_not_infer_weekdays_or_weekends(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-key")
    monkeypatch.setattr(observed, "reference_secid", lambda *_args, **_kwargs: "SiU6")
    monkeypatch.setattr(
        observed,
        "_exact_date_has_secid",
        lambda trade_date, **_kwargs: trade_date.isoformat() == "2026-06-15",
    )

    result = observed.observed_dates(
        "2026-06-13",
        "2026-06-16",
        instrument_id="si_futures_family",
    )

    assert result == ["2026-06-15"]
    assert "2026-06-13" not in result
    assert "2026-06-14" not in result
    assert "2026-06-16" not in result


def test_observed_dates_fail_closed_on_exact_date_transport_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-key")
    monkeypatch.setenv("MOEX_API_URL", "https://apim.moex.com")
    monkeypatch.setattr(observed, "reference_secid", lambda *_args, **_kwargs: "SiU6")

    def fake_get(url, *, params, headers, timeout):
        return FakeResponse(_payload([["SiU6", "2026-06-14"]]), str(url))

    monkeypatch.setattr(observed.tradestats.requests, "get", fake_get)

    with pytest.raises(ValueError, match="exact-date TradeStats request returned mismatched trade date"):
        observed.observed_dates(
            "2026-06-15",
            "2026-06-15",
            instrument_id="si_futures_family",
            timeout=1.0,
        )
