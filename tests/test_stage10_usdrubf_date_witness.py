from __future__ import annotations

import pytest

from moex_data import step10_rub_refresh_scheduler as step10
from moex_data.futures import observed_tradestats_dates as observed_dates


def test_stage10_date_witness_is_perpetual_usdrubf() -> None:
    assert step10.OBSERVED_DATE_REFERENCE_INSTRUMENT_ID == "usdrubf_futures_family"
    assert observed_dates.reference_secid(step10.OBSERVED_DATE_REFERENCE_INSTRUMENT_ID) == "USDRUBF"
    # Regression context: the old witness remains explicitly bound to the expired Sep-2026 contract.
    assert observed_dates.reference_secid("si_futures_family") == "SiU6"


def test_stage10_calendar_dates_cross_si_expiry_using_usdrubf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, str, float]] = []

    def fake_observed_dates(
        date_start: str,
        date_end: str,
        *,
        instrument_id: str,
        timeout: float,
    ) -> list[str]:
        calls.append((date_start, date_end, instrument_id, timeout))
        return ["2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22"]

    monkeypatch.setattr(step10.observed_dates, "observed_dates", fake_observed_dates)

    result = step10._calendar_dates(
        start_date="2026-09-17",
        end_date="2026-09-22",
        timeout=12.5,
    )

    assert result == ["2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22"]
    assert calls == [
        ("2026-09-17", "2026-09-22", "usdrubf_futures_family", 12.5)
    ]


def test_stage10_calendar_dates_remains_fail_closed_on_source_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failed(*_args, **_kwargs):
        raise RuntimeError("synthetic APIM failure")

    monkeypatch.setattr(step10.observed_dates, "observed_dates", failed)

    with pytest.raises(
        step10.Step10RefreshError,
        match="Stage 10 observed TradeStats date source failed: synthetic APIM failure",
    ):
        step10._calendar_dates(
            start_date="2026-09-17",
            end_date="2026-09-22",
            timeout=1.0,
        )
