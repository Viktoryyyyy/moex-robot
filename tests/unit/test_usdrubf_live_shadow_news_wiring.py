from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

import pytest

from src.moex_research.intelligence.usdrubf_live_shadow_bridge import MOSCOW, build_live_decision_input
from src.moex_research.intelligence.usdrubf_news_macro import NewsEvent
from src.moex_research.runners import usdrubf_live_shadow_smoke as runner


WALL_CLOCK = datetime(2026, 8, 13, 12, 0, tzinfo=MOSCOW)
NEWS_AS_OF = WALL_CLOCK + timedelta(seconds=2)
CURRENT_DAY = date(2026, 8, 13)
PRIOR_DAY = date(2026, 8, 12)


def _bar(end: datetime, open_: float, high: float, low: float, close: float):
    return {
        "end": end,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": 100.0,
    }


def _sessions():
    prior = (
        _bar(datetime(2026, 8, 12, 10, 0, tzinfo=MOSCOW), 79.8, 80.0, 79.7, 79.9),
        _bar(datetime(2026, 8, 12, 10, 5, tzinfo=MOSCOW), 79.9, 80.2, 79.6, 80.0),
    )
    current = (
        _bar(datetime(2026, 8, 13, 10, 0, tzinfo=MOSCOW), 80.0, 80.1, 79.9, 80.0),
        _bar(datetime(2026, 8, 13, 10, 5, tzinfo=MOSCOW), 80.0, 80.2, 80.0, 80.1),
        _bar(datetime(2026, 8, 13, 10, 10, tzinfo=MOSCOW), 80.1, 80.3, 80.05, 80.25),
    )
    return prior, current


def _event(*, ingested_at: datetime = NEWS_AS_OF) -> NewsEvent:
    available = WALL_CLOCK - timedelta(minutes=5)
    return NewsEvent(
        event_id="event_test",
        cluster_id="cluster_test",
        source_id="official_test",
        source_tier="OFFICIAL_PRIMARY",
        source_reference="https://official.example/event",
        published_at=available.isoformat(),
        available_at=available.isoformat(),
        ingested_at=ingested_at.isoformat(),
        content_hash="a" * 64,
        event_type="OFFICIAL_COMMUNICATION",
        entities=(),
        rub_relevance=0.0,
        direction="NEUTRAL",
        importance="LOW",
        novelty="NEW",
        horizon="SHORT_TERM",
        confidence=0.0,
        mechanism="Deterministic neutral classification; no directional effect is inferred.",
        quality_status="OK",
    )


def test_wall_clock_composition_includes_news_and_rejects_future_ingestion() -> None:
    prior, current = _sessions()
    market_input = build_live_decision_input(
        current_session_bars=current,
        prior_session_bars=prior,
        wall_clock_as_of=WALL_CLOCK,
    )

    combined = runner._compose_wall_clock_decision_input(
        market_input=market_input,
        wall_clock=NEWS_AS_OF,
        macro_state=market_input.macro_state,
        news_events=(_event(),),
    )
    assert combined.as_of_timestamp == NEWS_AS_OF
    assert len(combined.news_events) == 1
    assert combined.news_events[0].direction == "NEUTRAL"
    assert combined.news_events[0].confidence == 0.0

    with pytest.raises(RuntimeError, match="ingested after decision wall clock"):
        runner._compose_wall_clock_decision_input(
            market_input=market_input,
            wall_clock=NEWS_AS_OF,
            macro_state=market_input.macro_state,
            news_events=(_event(ingested_at=NEWS_AS_OF + timedelta(seconds=1)),),
        )


def test_runner_wires_bounded_live_news_and_exposes_source_quality(monkeypatch, tmp_path) -> None:
    prior, current = _sessions()

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return WALL_CLOCK.replace(tzinfo=None)
            return WALL_CLOCK.astimezone(tz)

    def fake_load(_secid: str, trade_date: date):
        if trade_date == CURRENT_DAY:
            return current
        if trade_date == PRIOR_DAY:
            return prior
        return ()

    captured = {}

    def fake_news(*, timeout_seconds: float, max_events: int):
        captured["timeout_seconds"] = timeout_seconds
        captured["max_events"] = max_events
        return (
            (_event(),),
            NEWS_AS_OF,
            {
                "source_count": 3,
                "ok_source_count": 2,
                "failed_source_count": 1,
                "failed_source_ids": "blocked_source",
                "source_quality": "one:OK,two:OK,blocked_source:SOURCE_UNAVAILABLE",
                "acquired_record_count": 5,
                "pipeline_event_count": 4,
                "events_dropped_by_bound": 3,
            },
        )

    monkeypatch.setattr(runner, "datetime", FixedDateTime)
    monkeypatch.setattr(runner, "_load_bars", fake_load)
    monkeypatch.setattr(runner, "_load_current_live_news", fake_news)

    args = argparse.Namespace(
        state_root=str(tmp_path),
        max_prior_lookback_days=7,
        enable_futoi=False,
        safe_wait_agent=True,
        cbr_macro=False,
        live_news=True,
        news_timeout_seconds=7.5,
        news_max_events=1,
        flowise_endpoint=None,
        flowise_request_field=None,
        flowise_response_field=None,
        flowise_timeout_seconds=20.0,
    )
    result = runner.run_once(args)

    assert captured == {"timeout_seconds": 7.5, "max_events": 1}
    assert result["status"] == "COMPLETED"
    assert result["as_of_timestamp"] == NEWS_AS_OF.isoformat()
    assert result["news_mode"] == "LIVE_RSS_DETERMINISTIC_NEUTRAL"
    assert result["news_source_count"] == 3
    assert result["news_ok_source_count"] == 2
    assert result["news_failed_source_count"] == 1
    assert result["news_failed_source_ids"] == "blocked_source"
    assert result["news_acquired_record_count"] == 5
    assert result["news_pipeline_event_count"] == 4
    assert result["news_event_count"] == 1
    assert result["news_events_dropped_by_bound"] == 3
    assert result["macro_mode"] == "DISABLED"
    assert result["decision_agent_mode"] == "SAFE_WAIT"
    assert result["trade_state"] == "WAIT"
    assert (tmp_path / "current_cycle.json").is_file()
