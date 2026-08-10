from __future__ import annotations

import argparse
import builtins
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest

from src.moex_research.intelligence.usdrubf_decision_engine import build_market_state
from src.moex_research.intelligence.usdrubf_live_shadow_bridge import (
    LiveShadowBridgeError,
    MOSCOW,
    blocked_futoi_context,
    build_closed_15m_bars,
    build_ema_context,
    build_live_decision_input,
    build_previous_session_zones,
    closed_bars,
    find_prior_session,
    futoi_context_from_pair,
    safe_wait_decision_agent,
)
from src.moex_research.runners import usdrubf_live_shadow_smoke as runner


def _bar(end: datetime, open_: float, high: float, low: float, close: float, volume: float = 100.0):
    return {
        "end": end,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _sessions():
    prior_day = date(2026, 8, 7)
    current_day = date(2026, 8, 10)
    prior = (
        _bar(datetime(2026, 8, 7, 10, 0, tzinfo=MOSCOW), 79.8, 80.0, 79.7, 79.9),
        _bar(datetime(2026, 8, 7, 10, 5, tzinfo=MOSCOW), 79.9, 80.2, 79.8, 80.1),
        _bar(datetime(2026, 8, 7, 10, 10, tzinfo=MOSCOW), 80.1, 80.3, 79.6, 79.7),
    )
    current = (
        _bar(datetime(2026, 8, 10, 10, 0, tzinfo=MOSCOW), 79.9, 80.0, 79.8, 79.9),
        _bar(datetime(2026, 8, 10, 10, 5, tzinfo=MOSCOW), 79.9, 80.1, 79.9, 80.0),
        _bar(datetime(2026, 8, 10, 10, 10, tzinfo=MOSCOW), 80.0, 80.4, 80.0, 80.35),
    )
    return prior_day, current_day, prior, current


def test_previous_session_zones_are_causal_and_nonzero() -> None:
    prior_day, _current_day, prior, _current = _sessions()
    high, low = build_previous_session_zones(prior)

    assert high.level_id == "previous_session_high_20260807"
    assert high.level_type == "PREVIOUS_SESSION_HIGH"
    assert high.center_price == 80.3
    assert high.lower_bound < high.center_price < high.upper_bound
    assert low.level_id == "previous_session_low_20260807"
    assert low.level_type == "PREVIOUS_SESSION_LOW"
    assert low.center_price == 79.6
    assert high.created_at == prior[-1]["end"].isoformat()
    assert low.created_at == prior[-1]["end"].isoformat()
    assert prior[-1]["end"].astimezone(MOSCOW).date() == prior_day


def test_closed_bars_excludes_future_rows() -> None:
    _prior_day, _current_day, _prior, current = _sessions()
    as_of = datetime(2026, 8, 10, 10, 6, tzinfo=MOSCOW)
    result = closed_bars(current, as_of_timestamp=as_of)
    assert [item["end"] for item in result] == [current[0]["end"], current[1]["end"]]


def test_ema_context_uses_only_complete_aligned_15m_bars() -> None:
    _prior_day, _current_day, _prior, current = _sessions()
    incomplete_next = _bar(
        datetime(2026, 8, 10, 10, 15, tzinfo=MOSCOW), 80.35, 80.5, 80.3, 80.45
    )
    bars = (*current, incomplete_next)

    aggregated = build_closed_15m_bars(bars)
    context = build_ema_context(bars)

    assert len(aggregated) == 1
    assert aggregated[0]["end"] == datetime(2026, 8, 10, 10, 0, tzinfo=MOSCOW).isoformat()
    assert aggregated[0]["open"] == current[0]["open"]
    assert aggregated[0]["close"] == current[2]["close"]
    assert aggregated[0]["high"] == max(item["high"] for item in current)
    assert aggregated[0]["low"] == min(item["low"] for item in current)
    assert aggregated[0]["volume"] == sum(item["volume"] for item in current)
    assert context.available_at == current[2]["end"]
    assert context.details["bar_count"] == 1
    assert context.details["source"] == "closed_15m_bar_replay_from_5m"
    assert context.direction == "NEUTRAL"


def test_live_decision_input_uses_last_closed_bar_and_blocked_futoi_by_default() -> None:
    _prior_day, _current_day, prior, current = _sessions()
    inputs = build_live_decision_input(
        current_session_bars=current,
        prior_session_bars=prior,
        wall_clock_as_of=datetime(2026, 8, 10, 10, 11, tzinfo=MOSCOW),
    )

    assert inputs.as_of_timestamp == current[2]["end"]
    assert inputs.price == current[2]["close"]
    assert inputs.futoi.source_id == "futoi"
    assert inputs.futoi.quality_status == "BLOCKED"
    assert inputs.ema_3_19_ai.source_id == "ema_3_19_ai"
    assert inputs.ema_3_19_ai.available_at == current[2]["end"]
    assert len(inputs.active_levels) == 2
    assert {item.level_id for item in inputs.level_interactions} == {
        item.level_id for item in inputs.active_levels
    }
    assert inputs.news_events == ()
    assert inputs.macro_state.observations == ()


def test_safe_wait_agent_builds_valid_non_actionable_market_state() -> None:
    _prior_day, _current_day, prior, current = _sessions()
    inputs = build_live_decision_input(
        current_session_bars=current,
        prior_session_bars=prior,
        wall_clock_as_of=datetime(2026, 8, 10, 10, 11, tzinfo=MOSCOW),
    )
    state = build_market_state(inputs, decision_agent=safe_wait_decision_agent)

    assert state.trade_state == "WAIT"
    assert state.targets == ()
    assert state.invalidation is None
    assert state.confidence == 0.25
    assert set(state.evidence_refs).issubset(
        {f"level:{item.level_id}" for item in state.active_levels}
    )


def test_find_prior_session_skips_empty_calendar_days() -> None:
    current_day = date(2026, 8, 10)
    calls = []

    def loader(secid: str, trade_date: date):
        calls.append((secid, trade_date))
        if trade_date == date(2026, 8, 7):
            return (_bar(datetime(2026, 8, 7, 10, 0, tzinfo=MOSCOW), 80.0, 80.1, 79.9, 80.0),)
        return ()

    prior_date, bars = find_prior_session(current_day, loader=loader, max_lookback_days=4)
    assert prior_date == date(2026, 8, 7)
    assert len(bars) == 1
    assert calls == [
        ("Si", date(2026, 8, 9)),
        ("Si", date(2026, 8, 8)),
        ("Si", date(2026, 8, 7)),
    ]


def test_futoi_pair_is_preserved_without_inventing_directional_rule() -> None:
    pair = SimpleNamespace(
        source_available_at=datetime(2026, 8, 8, 0, 0, tzinfo=MOSCOW),
        trade_date=date(2026, 8, 7),
        moment=datetime(2026, 8, 7, 23, 50, tzinfo=MOSCOW),
        sess_id="1",
        fiz_pos=100.0,
        fiz_pos_long=150.0,
        fiz_pos_short=-50.0,
        fiz_pos_long_num=10,
        fiz_pos_short_num=5,
        yur_pos=-100.0,
        yur_pos_long=30.0,
        yur_pos_short=-130.0,
        yur_pos_long_num=3,
        yur_pos_short_num=13,
    )
    context = futoi_context_from_pair(pair)

    assert context.quality_status == "OK"
    assert context.direction == "MIXED"
    assert context.confidence == 0.0
    assert context.details["interpretation"] == "participant_positioning_only_no_directional_rule_frozen"
    assert context.details["fiz_pos"] == 100.0
    assert context.details["yur_pos"] == -100.0


def test_blocked_futoi_context_is_explicit() -> None:
    context = blocked_futoi_context(
        available_at=datetime(2026, 8, 10, 10, 5, tzinfo=MOSCOW),
        reason="not enabled",
    )
    assert context.quality_status == "BLOCKED"
    assert context.details == {"reason": "not enabled"}


def test_runner_loads_project_dotenv_before_moex_feed_import(monkeypatch) -> None:
    events = []
    trade_day = date(2026, 8, 10)

    def fake_load_dotenv(path, override=False):
        events.append(("dotenv", path, override))
        return True

    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "src.api.futures.fo_feed_intraday":
            events.append(("import", name))
            return SimpleNamespace(
                load_fo_5m_day=lambda secid, trade_date: (secid, trade_date)
            )
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(runner, "load_dotenv", fake_load_dotenv)
    monkeypatch.setattr(builtins, "__import__", fake_import)

    result = runner._load_bars("Si", trade_day)

    assert events[0] == ("dotenv", runner.PROJECT_ENV_PATH, False)
    assert events[1] == ("import", "src.api.futures.fo_feed_intraday")
    assert result == ("Si", trade_day)


def test_runner_requires_explicit_flowise_config_without_safe_wait() -> None:
    args = argparse.Namespace(
        safe_wait_agent=False,
        flowise_endpoint=None,
        flowise_request_field=None,
        flowise_response_field=None,
        flowise_timeout_seconds=20.0,
    )
    with pytest.raises(RuntimeError, match="explicit Flowise"):
        runner._decision_agent(args)


def test_runner_one_shot_safe_wait_uses_explicit_state_root(monkeypatch, tmp_path) -> None:
    now = datetime.now(MOSCOW).replace(microsecond=0)
    current_day = now.date()
    prior_day = current_day - timedelta(days=1)
    current = (
        _bar(
            datetime.combine(current_day, datetime.min.time(), tzinfo=MOSCOW) + timedelta(hours=10),
            80.0,
            80.1,
            79.9,
            80.0,
        ),
        _bar(
            datetime.combine(current_day, datetime.min.time(), tzinfo=MOSCOW)
            + timedelta(hours=10, minutes=5),
            80.0,
            80.2,
            80.0,
            80.1,
        ),
        _bar(
            datetime.combine(current_day, datetime.min.time(), tzinfo=MOSCOW)
            + timedelta(hours=10, minutes=10),
            80.1,
            80.3,
            80.05,
            80.25,
        ),
    )
    prior = (
        _bar(
            datetime.combine(prior_day, datetime.min.time(), tzinfo=MOSCOW) + timedelta(hours=10),
            79.8,
            80.0,
            79.7,
            79.9,
        ),
        _bar(
            datetime.combine(prior_day, datetime.min.time(), tzinfo=MOSCOW)
            + timedelta(hours=10, minutes=5),
            79.9,
            80.2,
            79.6,
            80.0,
        ),
    )

    def fake_load(_secid: str, trade_date: date):
        if trade_date == current_day:
            return current
        if trade_date == prior_day:
            return prior
        return ()

    monkeypatch.setattr(runner, "_load_bars", fake_load)
    args = argparse.Namespace(
        state_root=str(tmp_path),
        max_prior_lookback_days=7,
        enable_futoi=False,
        safe_wait_agent=True,
        flowise_endpoint=None,
        flowise_request_field=None,
        flowise_response_field=None,
        flowise_timeout_seconds=20.0,
    )
    result = runner.run_once(args)

    assert result["status"] == "COMPLETED"
    assert result["decision_agent_mode"] == "SAFE_WAIT"
    assert result["trade_state"] == "WAIT"
    assert result["futoi_quality"] == "BLOCKED"
    assert result["news_event_count"] == 0
    assert result["macro_observation_count"] == 0
    assert str(result["market_state_path"]).startswith(str(tmp_path))
    assert (tmp_path / "current_cycle.json").is_file()


def test_previous_session_requires_observed_increment() -> None:
    flat = (
        _bar(datetime(2026, 8, 7, 10, 0, tzinfo=MOSCOW), 80.0, 80.0, 80.0, 80.0),
    )
    with pytest.raises(LiveShadowBridgeError, match="price increment"):
        build_previous_session_zones(flat)
