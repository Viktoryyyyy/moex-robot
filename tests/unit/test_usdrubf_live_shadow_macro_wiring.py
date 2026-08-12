from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta

from src.moex_research.intelligence.usdrubf_live_shadow_bridge import (
    MOSCOW,
    build_live_decision_input,
)
from src.moex_research.intelligence.usdrubf_news_macro import MacroObservation, MacroState
from src.moex_research.runners import usdrubf_live_shadow_smoke as runner


WALL_CLOCK = datetime(2026, 8, 12, 15, 0, tzinfo=MOSCOW)
CURRENT_DAY = date(2026, 8, 12)
PRIOR_DAY = date(2026, 8, 11)


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
        _bar(datetime(2026, 8, 11, 10, 0, tzinfo=MOSCOW), 79.8, 80.0, 79.7, 79.9),
        _bar(datetime(2026, 8, 11, 10, 5, tzinfo=MOSCOW), 79.9, 80.2, 79.6, 80.0),
    )
    current = (
        _bar(datetime(2026, 8, 12, 10, 0, tzinfo=MOSCOW), 80.0, 80.1, 79.9, 80.0),
        _bar(datetime(2026, 8, 12, 10, 5, tzinfo=MOSCOW), 80.0, 80.2, 80.0, 80.1),
        _bar(datetime(2026, 8, 12, 10, 10, tzinfo=MOSCOW), 80.1, 80.3, 80.05, 80.25),
    )
    return prior, current


def _macro_state(as_of: datetime) -> MacroState:
    local_day_start = datetime.combine(as_of.astimezone(MOSCOW).date(), time.min, tzinfo=MOSCOW)
    ruonia_available = local_day_start
    ruonia_published = ruonia_available - timedelta(microseconds=1)
    key_effective = local_day_start - timedelta(days=10)
    observations = (
        MacroObservation(
            metric_id="cbr_ruonia_rate_pct",
            source_id="cbr_ruonia_daily",
            source_reference="https://www.cbr.ru/eng/hd_base/ruonia/dynamics/?test=1",
            value=13.57,
            unit="PERCENT_PER_ANNUM",
            observed_or_effective_at=local_day_start - timedelta(days=2),
            published_at=ruonia_published,
            available_at=ruonia_available,
            ingested_at=as_of,
            quality_status="OK",
        ),
        MacroObservation(
            metric_id="cbr_key_rate_pct",
            source_id="cbr_key_rate_daily",
            source_reference="https://www.cbr.ru/eng/hd_base/ProcStav/IR_CHG_MPO/?test=1",
            value=14.0,
            unit="PERCENT_PER_ANNUM",
            observed_or_effective_at=key_effective,
            published_at=key_effective,
            available_at=key_effective,
            ingested_at=as_of,
            quality_status="OK",
        ),
    )
    return MacroState(
        as_of_timestamp=as_of.isoformat(),
        observations=observations,
        overall_direction="NEUTRAL",
        confidence=0.0,
        dominant_drivers=(),
    )


def test_wall_clock_composition_does_not_backdate_cbr_retrieval() -> None:
    prior, current = _sessions()
    market_input = build_live_decision_input(
        current_session_bars=current,
        prior_session_bars=prior,
        wall_clock_as_of=WALL_CLOCK,
    )
    assert market_input.as_of_timestamp == current[-1]["end"]

    combined = runner._compose_wall_clock_decision_input(
        market_input=market_input,
        wall_clock=WALL_CLOCK,
        macro_state=_macro_state(WALL_CLOCK),
    )

    assert combined.as_of_timestamp == WALL_CLOCK
    assert combined.price == market_input.price
    assert combined.ema_3_19_ai.available_at == current[-1]["end"]
    assert tuple(item.metric_id for item in combined.macro_state.observations) == (
        "cbr_ruonia_rate_pct",
        "cbr_key_rate_pct",
    )
    assert all(item.ingested_at == WALL_CLOCK for item in combined.macro_state.observations)


def test_runner_wires_current_cbr_macrostate_and_reports_separate_market_timestamp(
    monkeypatch,
    tmp_path,
) -> None:
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

    def fake_macro_state(*, as_of_timestamp: datetime):
        captured["as_of"] = as_of_timestamp
        return _macro_state(as_of_timestamp)

    monkeypatch.setattr(runner, "datetime", FixedDateTime)
    monkeypatch.setattr(runner, "_load_bars", fake_load)
    monkeypatch.setattr(runner, "_load_current_cbr_macro_state", fake_macro_state)

    args = argparse.Namespace(
        state_root=str(tmp_path),
        max_prior_lookback_days=7,
        enable_futoi=False,
        safe_wait_agent=True,
        cbr_macro=True,
        flowise_endpoint=None,
        flowise_request_field=None,
        flowise_response_field=None,
        flowise_timeout_seconds=20.0,
    )
    result = runner.run_once(args)

    assert captured["as_of"] == WALL_CLOCK
    assert result["status"] == "COMPLETED"
    assert result["market_data_as_of_timestamp"] == current[-1]["end"].isoformat()
    assert result["as_of_timestamp"] == WALL_CLOCK.isoformat()
    assert result["macro_mode"] == "LIVE_CBR"
    assert result["macro_observation_count"] == 2
    assert result["macro_metric_ids"] == "cbr_ruonia_rate_pct,cbr_key_rate_pct"
    assert result["macro_direction"] == "NEUTRAL"
    assert result["macro_confidence"] == 0.0
    assert result["news_event_count"] == 0
    assert result["futoi_quality"] == "BLOCKED"
    assert result["decision_agent_mode"] == "SAFE_WAIT"
    assert result["trade_state"] == "WAIT"
    assert (tmp_path / "current_cycle.json").is_file()
