from __future__ import annotations

import pytest

from moex_data import step10_rub_refresh_scheduler as step10


def test_stage10_observed_dates_use_registry_bound_si_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_observed_dates(date_start: str, date_end: str, **kwargs):
        calls.append({"date_start": date_start, "date_end": date_end, **kwargs})
        return ["2026-08-29", "2026-08-30"]

    monkeypatch.setattr(step10.observed_dates, "observed_dates", fake_observed_dates)

    result = step10._calendar_dates(
        start_date="2026-08-16",
        end_date="2026-08-30",
        timeout=60.0,
    )

    assert result == ["2026-08-29", "2026-08-30"]
    assert calls == [
        {
            "date_start": "2026-08-16",
            "date_end": "2026-08-30",
            "instrument_id": "si_futures_family",
            "timeout": 60.0,
        }
    ]
    assert step10.STAGE7_INSTRUMENTS["usdrubf_futures_family"] == "USDRUBF"
    assert step10.STAGE7_INSTRUMENTS["cnyrubf_futures_family"] == "CNYRUBF"


def test_stage10_observed_date_lookup_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args, **_kwargs):
        raise ValueError("no authoritative dates")

    monkeypatch.setattr(step10.observed_dates, "observed_dates", fail)

    with pytest.raises(step10.Step10RefreshError, match="Stage 10 observed TradeStats date source failed"):
        step10._calendar_dates(
            start_date="2026-08-16",
            end_date="2026-08-30",
            timeout=60.0,
        )
