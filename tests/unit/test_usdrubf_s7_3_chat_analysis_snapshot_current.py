from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current as current


NOW = datetime(2026, 8, 31, 10, 51, 23, tzinfo=timezone.utc)


def _bar(end: datetime, open_: float, high: float, low: float, close: float, volume: float = 100.0):
    return {
        "end": end,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


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


def _structure_sessions():
    prior_day = date(2026, 8, 28)
    current_day = date(2026, 8, 31)
    prior = [
        _bar(datetime(2026, 8, 28, 13, 30, tzinfo=current.base.MOSCOW), 79.80, 80.00, 79.70, 79.90),
        _bar(datetime(2026, 8, 28, 13, 35, tzinfo=current.base.MOSCOW), 79.90, 80.20, 79.80, 80.10),
        _bar(datetime(2026, 8, 28, 13, 40, tzinfo=current.base.MOSCOW), 80.10, 80.30, 79.60, 79.70),
    ]
    current_bars = [
        _bar(datetime(2026, 8, 31, 13, 30, tzinfo=current.base.MOSCOW), 79.90, 80.00, 79.80, 79.90),
        _bar(datetime(2026, 8, 31, 13, 35, tzinfo=current.base.MOSCOW), 79.90, 80.10, 79.90, 80.00),
        _bar(datetime(2026, 8, 31, 13, 40, tzinfo=current.base.MOSCOW), 80.00, 80.40, 80.00, 80.35),
        _bar(datetime(2026, 8, 31, 13, 55, tzinfo=current.base.MOSCOW), 90.00, 90.50, 89.50, 90.25),
    ]
    return prior_day, current_day, prior, current_bars


def test_usdrubf_structural_component_is_exact_source_causal_and_compact(monkeypatch) -> None:
    prior_day, current_day, prior, current_bars = _structure_sessions()
    calls = []

    def load_bars(secid: str, trade_date: date):
        calls.append((secid, trade_date))
        assert secid == "USDRUBF"
        if trade_date == current_day:
            return current_bars
        if trade_date == prior_day:
            return prior
        return []

    monkeypatch.setattr(current.live, "_load_bars", load_bars)

    result = current._usdrubf_live_market_structure_component(NOW)
    data = result.data
    block = data["structural_levels"]

    assert calls == [
        ("USDRUBF", date(2026, 8, 31)),
        ("USDRUBF", date(2026, 8, 30)),
        ("USDRUBF", date(2026, 8, 29)),
        ("USDRUBF", date(2026, 8, 28)),
    ]
    assert data["instrument"] == "USDRUBF"
    assert data["requested_secid"] == "USDRUBF"
    assert data["source_id"] == "moex_algopack_fo_tradestats_5m"
    assert data["trade_date"] == "2026-08-31"
    assert data["prior_trade_date"] == "2026-08-28"
    assert data["price"] == pytest.approx(80.35)
    assert data["current_closed_5m_bar_count"] == 3
    assert result.data_as_of == datetime(2026, 8, 31, 13, 40, tzinfo=current.base.MOSCOW)

    assert data["trend"] in {"BULLISH_USD", "NEUTRAL", "BEARISH_USD"}
    assert isinstance(data["market_regime"], str) and data["market_regime"]
    assert data["ema_3_19"]["standalone_directional_authority"] is False
    assert data["ema_3_19"]["s7_2_verdict"] == "REJECT_AS_STANDALONE_DIRECTIONAL_SIGNAL"
    assert data["futoi"]["action_authority"] is False

    assert block["schema_version"] == "usdrubf_structural_levels_snapshot.v1"
    assert block["instrument"] == "USDRUBF"
    assert block["status"] == "FRESH"
    assert block["price_context"]["price"] == pytest.approx(80.35)
    assert block["price_context"]["source_timestamp"] == result.data_as_of
    assert block["price_context"]["freshness"]["age_seconds"] == 683
    assert block["price_context"]["closed_bar_only"] is True

    levels = block["active_levels"]
    interactions = block["level_interactions"]
    assert [item["level_id"] for item in levels] == [
        "previous_session_high_20260828",
        "previous_session_low_20260828",
    ]
    assert {item["level_type"] for item in levels} == {
        "PREVIOUS_SESSION_HIGH",
        "PREVIOUS_SESSION_LOW",
    }
    assert all(item["status"] == "ACTIVE" for item in levels)
    assert all(item["source_timeframe"] == "5m" for item in levels)
    assert all(item["structural_quality"] >= 0.0 for item in levels)
    assert all(item["provenance"]["requested_secid"] == "USDRUBF" for item in levels)
    assert {item["level_id"] for item in interactions} == {item["level_id"] for item in levels}
    assert all("previous_state" in item and "event_timestamp" in item for item in interactions)
    assert all(item["provenance"]["source_data_as_of"] == result.data_as_of for item in interactions)

    extrema = block["observed_extrema"]
    assert extrema["prior_completed_session"]["high"] == pytest.approx(80.30)
    assert extrema["prior_completed_session"]["low"] == pytest.approx(79.60)
    assert extrema["current_observed_session"]["high"] == pytest.approx(80.40)
    assert extrema["current_observed_session"]["low"] == pytest.approx(79.80)
    assert extrema["current_observed_session"]["partial_session"] is True

    methodology = block["methodology"]
    assert methodology["ranking_applied"] is False
    assert methodology["all_active_levels_emitted"] is True
    assert methodology["closed_bars_only"] is True
    assert methodology["lookahead_forbidden"] is True
    assert methodology["calendar_dependency"] is False
    assert methodology["weekday_weekend_inference"] is False
    assert block["unsupported_facts"]["swing_hh_hl_lh_ll"].startswith("NOT_EMITTED")

    authority = block["authority"]
    assert authority == {
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }
    assert "targets" not in block
    assert "stops" not in block
    assert "scenario_probabilities" not in block


def test_usdrubf_structural_component_is_reproducible_for_same_input(monkeypatch) -> None:
    prior_day, current_day, prior, current_bars = _structure_sessions()

    def load_bars(secid: str, trade_date: date):
        assert secid == "USDRUBF"
        if trade_date == current_day:
            return list(current_bars)
        if trade_date == prior_day:
            return list(prior)
        return []

    monkeypatch.setattr(current.live, "_load_bars", load_bars)
    first = current._usdrubf_live_market_structure_component(NOW)
    second = current._usdrubf_live_market_structure_component(NOW)

    assert first.data_as_of == second.data_as_of
    assert first.data["structural_levels"] == second.data["structural_levels"]
    assert first.data["active_levels"] == second.data["active_levels"]
    assert first.data["level_interactions"] == second.data["level_interactions"]
    assert first.data["market_regime"] == second.data["market_regime"]
    assert first.data["ema_3_19"] == second.data["ema_3_19"]


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


def test_current_producers_replace_structure_and_cnyrubf_only() -> None:
    baseline = current.base.default_producers()
    selected = current.current_producers()

    assert set(selected) == set(baseline)
    assert selected["cnyrubf_live"] is current._cnyrubf_live_component
    assert selected["live_market_structure"] is current._usdrubf_live_market_structure_component
    assert selected["cbr_macro"] is current.base._macro_component
    assert selected["official_news"] is current.base._news_component
    assert selected["cnyrub_spot_live"] is current.base._cny_spot_component
    assert callable(selected["stage9_daily"])
    assert callable(selected["stage9_weekly"])