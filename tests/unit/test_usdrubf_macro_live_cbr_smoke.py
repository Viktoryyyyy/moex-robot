from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.moex_research.intelligence.usdrubf_macro_live_cbr import CbrMacroAdapterError
from src.moex_research.runners.usdrubf_macro_live_cbr_smoke import (
    run_current_cbr_macro_smoke,
)


ANCHOR = datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc)


def _ruonia_loader(start, end, *, retrieved_at_utc):
    assert start.isoformat() == "2026-07-13"
    assert end.isoformat() == "2026-08-12"
    assert retrieved_at_utc == ANCHOR
    return [
        {
            "source_id": "cbr_ruonia_daily",
            "source_route": "https://www.cbr.ru/eng/hd_base/ruonia/dynamics/?UniDbQuery.Posted=True",
            "retrieved_at_utc": ANCHOR.isoformat(),
            "historical_model_use_status": "candidate_for_phase8_2",
            "observation_date": "2026-08-10",
            "publication_date": "2026-08-11",
            "ruonia_rate_pct": 11.25,
        }
    ]


def _key_rate_loader(start, end, *, retrieved_at_utc):
    assert start.isoformat() == "2016-08-14"
    assert end.isoformat() == "2026-08-12"
    assert retrieved_at_utc == ANCHOR
    return [
        {
            "source_id": "cbr_key_rate_daily",
            "source_route": "https://www.cbr.ru/eng/hd_base/ProcStav/IR_CHG_MPO/?UniDbQuery.Posted=True",
            "retrieved_at_utc": ANCHOR.isoformat(),
            "historical_model_use_status": "candidate_for_phase8_2",
            "effective_date": "2026-07-27",
            "key_rate_pct": 14.0,
        }
    ]


def test_current_cbr_macro_smoke_builds_two_pit_safe_observations() -> None:
    observations = run_current_cbr_macro_smoke(
        now_utc=ANCHOR,
        ruonia_loader=_ruonia_loader,
        key_rate_loader=_key_rate_loader,
    )

    assert tuple(item.source_id for item in observations) == (
        "cbr_ruonia_daily",
        "cbr_key_rate_daily",
    )
    assert tuple(item.value for item in observations) == (11.25, 14.0)
    assert all(item.quality_status == "OK" for item in observations)
    assert all(item.available_at <= ANCHOR for item in observations)
    assert all(item.ingested_at <= ANCHOR for item in observations)


def test_current_cbr_macro_smoke_uses_moscow_calendar_for_request_window() -> None:
    boundary_anchor = datetime(2026, 8, 11, 21, 30, tzinfo=timezone.utc)
    seen_end_dates = []

    def boundary_ruonia_loader(start, end, *, retrieved_at_utc):
        seen_end_dates.append(end)
        return [
            {
                "source_id": "cbr_ruonia_daily",
                "source_route": "https://www.cbr.ru/eng/hd_base/ruonia/dynamics/?UniDbQuery.Posted=True",
                "retrieved_at_utc": boundary_anchor.isoformat(),
                "historical_model_use_status": "candidate_for_phase8_2",
                "observation_date": "2026-08-10",
                "publication_date": "2026-08-11",
                "ruonia_rate_pct": 11.25,
            }
        ]

    def boundary_key_rate_loader(start, end, *, retrieved_at_utc):
        seen_end_dates.append(end)
        return [
            {
                "source_id": "cbr_key_rate_daily",
                "source_route": "https://www.cbr.ru/eng/hd_base/ProcStav/IR_CHG_MPO/?UniDbQuery.Posted=True",
                "retrieved_at_utc": boundary_anchor.isoformat(),
                "historical_model_use_status": "candidate_for_phase8_2",
                "effective_date": "2026-08-12",
                "key_rate_pct": 13.0,
            }
        ]

    observations = run_current_cbr_macro_smoke(
        now_utc=boundary_anchor,
        ruonia_loader=boundary_ruonia_loader,
        key_rate_loader=boundary_key_rate_loader,
    )

    assert [item.isoformat() for item in seen_end_dates] == ["2026-08-12", "2026-08-12"]
    assert observations[1].observed_or_effective_at.isoformat() == "2026-08-12T00:00:00+03:00"
    assert observations[1].value == 13.0


def test_current_cbr_macro_smoke_fails_closed_on_same_day_ruonia() -> None:
    def same_day_ruonia_loader(start, end, *, retrieved_at_utc):
        return [
            {
                "source_id": "cbr_ruonia_daily",
                "source_route": "https://www.cbr.ru/eng/hd_base/ruonia/dynamics/?UniDbQuery.Posted=True",
                "retrieved_at_utc": ANCHOR.isoformat(),
                "historical_model_use_status": "candidate_for_phase8_2",
                "observation_date": "2026-08-11",
                "publication_date": "2026-08-12",
                "ruonia_rate_pct": 99.0,
            }
        ]

    with pytest.raises(CbrMacroAdapterError, match="no causally eligible RUONIA"):
        run_current_cbr_macro_smoke(
            now_utc=ANCHOR,
            ruonia_loader=same_day_ruonia_loader,
            key_rate_loader=_key_rate_loader,
        )


def test_current_cbr_macro_smoke_rejects_nonpositive_lookback() -> None:
    with pytest.raises(ValueError, match="lookback days must be positive"):
        run_current_cbr_macro_smoke(
            now_utc=ANCHOR,
            ruonia_lookback_days=0,
            ruonia_loader=_ruonia_loader,
            key_rate_loader=_key_rate_loader,
        )
