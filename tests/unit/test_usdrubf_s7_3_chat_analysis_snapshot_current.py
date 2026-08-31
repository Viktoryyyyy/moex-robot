from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current as current


NOW = datetime(2026, 8, 31, 10, 51, 23, tzinfo=timezone.utc)


def _bindings():
    return [
        {
            "root": "CR",
            "as_of_date": "2026-08-31",
            "role": "front",
            "instrument_id": "cr_front_contract",
            "secid": "CRU6",
            "last_trade_date": "2026-09-17",
            "source_id": "moex_iss_forts_securities_reference",
        },
        {
            "root": "CR",
            "as_of_date": "2026-08-31",
            "role": "next",
            "instrument_id": "cr_next_contract",
            "secid": "CRZ6",
            "last_trade_date": "2026-12-17",
            "source_id": "moex_iss_forts_securities_reference",
        },
    ]


def test_cnyrubf_live_uses_explicit_current_front_and_closed_bars(monkeypatch) -> None:
    binding_calls = []
    bar_calls = []

    def discover_front_next(*, root: str, as_of_date: str):
        binding_calls.append((root, as_of_date))
        return _bindings()

    def load_bars(secid: str, trade_date):
        bar_calls.append((secid, trade_date.isoformat()))
        return [
            {"end": datetime(2026, 8, 31, 13, 40, tzinfo=current.base.MOSCOW), "open": 11.10, "high": 11.12, "low": 11.09, "close": 11.11, "volume": 100.0},
            {"end": datetime(2026, 8, 31, 13, 45, tzinfo=current.base.MOSCOW), "open": 11.11, "high": 11.15, "low": 11.10, "close": 11.14, "volume": 150.0},
            {"end": datetime(2026, 8, 31, 13, 55, tzinfo=current.base.MOSCOW), "open": 11.14, "high": 11.16, "low": 11.13, "close": 11.15, "volume": 200.0},
        ]

    monkeypatch.setattr(current.front_next_binding, "discover_front_next", discover_front_next)
    monkeypatch.setattr(current.live, "_load_bars", load_bars)

    result = current._cnyrubf_live_component(NOW)
    data = result.data
    observation = data["observation"]

    assert binding_calls == [("CR", "2026-08-31")]
    assert bar_calls == [("CRU6", "2026-08-31")]
    assert data["mode"] == "LIVE_ALGOPACK_FO_TRADESTATS_5M_PARTIAL_DAY_CONTEXT"
    assert data["implicit_latest_used"] is False
    assert data["partial_day"] is True
    assert data["action_authority"] is False
    assert observation["source_id"] == "moex_algopack_fo_tradestats_5m"
    assert observation["secid"] == "CRU6"
    assert observation["trade_date"] == "2026-08-31"
    assert observation["open"] == pytest.approx(11.10)
    assert observation["high"] == pytest.approx(11.15)
    assert observation["low"] == pytest.approx(11.09)
    assert observation["close"] == pytest.approx(11.14)
    assert observation["volume"] == pytest.approx(250.0)
    assert observation["closed_5m_bar_count"] == 2
    assert result.data_as_of == datetime(2026, 8, 31, 13, 45, tzinfo=current.base.MOSCOW)


def test_cnyrubf_live_fails_closed_when_front_binding_date_mismatches(monkeypatch) -> None:
    bad = _bindings()
    bad[0] = {**bad[0], "as_of_date": "2026-08-30"}
    monkeypatch.setattr(
        current.front_next_binding,
        "discover_front_next",
        lambda **_kwargs: bad,
    )

    with pytest.raises(current.CurrentChatSnapshotError, match="binding date mismatch"):
        current._cnyrubf_live_component(NOW)


def test_current_producers_replace_only_cnyrubf_live() -> None:
    baseline = current.base.default_producers()
    selected = current.current_producers()

    assert set(selected) == set(baseline)
    assert selected["cnyrubf_live"] is current._cnyrubf_live_component
    assert selected["live_market_structure"] is current.base._live_market_component
    assert selected["cbr_macro"] is current.base._macro_component
    assert selected["official_news"] is current.base._news_component
    assert selected["cnyrub_spot_live"] is current.base._cny_spot_component
    assert callable(selected["stage9_daily"])
    assert callable(selected["stage9_weekly"])
