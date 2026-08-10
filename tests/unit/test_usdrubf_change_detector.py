from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

import pytest

from src.moex_research.intelligence.usdrubf_change_detector import (
    ChangeDetectorError,
    detect_market_state_changes,
)
from src.moex_research.intelligence.usdrubf_decision_engine import (
    DecisionMarketState,
    DirectionalContext,
    ResolvedLevelReference,
)
from src.moex_research.intelligence.usdrubf_level_structure import (
    InteractionSnapshot,
    LevelZone,
)
from src.moex_research.intelligence.usdrubf_news_macro import (
    MacroObservation,
    MacroState,
    NewsEvent,
)


T0 = datetime(2026, 8, 10, 7, 0, tzinfo=timezone.utc)
T1 = T0 + timedelta(minutes=5)
T2 = T1 + timedelta(minutes=5)


def _support() -> LevelZone:
    return LevelZone(
        level_id="support_80",
        level_type="SUPPORT",
        center_price=80.0,
        lower_bound=79.8,
        upper_bound=80.2,
        created_at=T0.isoformat(),
        source_timeframe="15m",
        status="ACTIVE",
    )


def _resistance() -> LevelZone:
    return LevelZone(
        level_id="resistance_82",
        level_type="RESISTANCE",
        center_price=82.0,
        lower_bound=81.8,
        upper_bound=82.2,
        created_at=T0.isoformat(),
        source_timeframe="15m",
        status="ACTIVE",
    )


def _interaction(
    level_id: str,
    state: str,
    *,
    previous_state: str | None = None,
) -> InteractionSnapshot:
    return InteractionSnapshot(
        level_id=level_id,
        state=state,
        direction="FROM_BELOW",
        event_timestamp=T1.isoformat(),
        previous_state=previous_state,
        structural_quality=0.75,
        touch_count=2,
        breakout_side=(
            "ABOVE"
            if state
            in {
                "BREAKOUT",
                "RETEST_PENDING",
                "RETEST",
                "RETEST_HOLD",
                "RETEST_FAIL",
                "ACCEPTANCE",
                "FALSE_BREAKOUT",
            }
            else None
        ),
        as_of_timestamp=T1.isoformat(),
    )


def _signal(
    source_id: str,
    direction: str = "BULLISH_USD",
    quality_status: str = "OK",
) -> DirectionalContext:
    return DirectionalContext(
        source_id=source_id,
        available_at=T1,
        direction=direction,
        confidence=0.75,
        quality_status=quality_status,
    )


def _macro(direction: str = "NEUTRAL") -> MacroState:
    observation = MacroObservation(
        metric_id="cbr_rate",
        source_id="cbr_key_rate_daily",
        source_reference="https://example.test/cbr",
        value=18.0,
        unit="percent",
        observed_or_effective_at=T0,
        published_at=T0,
        available_at=T1,
        ingested_at=T1,
        quality_status="OK",
    )
    return MacroState(
        as_of_timestamp=T1.isoformat(),
        observations=(observation,),
        overall_direction=direction,
        confidence=0.7,
        dominant_drivers=("cbr_rate",),
    )


def _news(
    event_id: str = "news_1",
    *,
    importance: str = "MEDIUM",
    rub_relevance: float = 0.8,
    confidence: float = 0.8,
) -> NewsEvent:
    return NewsEvent(
        event_id=event_id,
        cluster_id=f"cluster_{event_id}",
        source_id="cbr",
        source_tier="OFFICIAL_PRIMARY",
        source_reference=f"https://example.test/{event_id}",
        published_at=T0.isoformat(),
        available_at=T1.isoformat(),
        ingested_at=T1.isoformat(),
        content_hash=("a" if event_id == "news_1" else "b") * 64,
        event_type="MONETARY_POLICY",
        entities=("CBR",),
        rub_relevance=rub_relevance,
        direction="USD_BEARISH",
        importance=importance,
        novelty="NEW",
        horizon="SHORT_TERM",
        confidence=confidence,
        mechanism="Official policy information changes RUB context.",
        quality_status="OK",
    )


def _target() -> ResolvedLevelReference:
    return ResolvedLevelReference(
        level_id="resistance_82",
        price_anchor="CENTER",
        price=82.0,
    )


def _invalidation(anchor: str = "LOWER_BOUND") -> ResolvedLevelReference:
    price = 79.8 if anchor == "LOWER_BOUND" else 80.0
    return ResolvedLevelReference(
        level_id="support_80",
        price_anchor=anchor,
        price=price,
    )


def _state(
    *,
    as_of: datetime = T1,
    price: float = 81.0,
    regime: str = "RANGE",
    interactions=None,
    levels=None,
    ema_direction: str = "BULLISH_USD",
    futoi_direction: str = "BULLISH_USD",
    news=None,
    macro_direction: str = "NEUTRAL",
    bias: str = "NEUTRAL",
    trade_state: str = "WAIT",
    confidence: float = 0.5,
    targets=(),
    invalidation=None,
    evidence_refs=("level:support_80",),
) -> DecisionMarketState:
    active_levels = tuple(levels) if levels is not None else (_support(), _resistance())
    level_interactions = (
        tuple(interactions)
        if interactions is not None
        else (
            _interaction("support_80", "AWAY", previous_state="RANGE_RETURN"),
            _interaction("resistance_82", "AWAY", previous_state="RANGE_RETURN"),
        )
    )
    return DecisionMarketState(
        instrument="USDRUBF",
        as_of_timestamp=as_of.isoformat(),
        price=price,
        trend="NEUTRAL",
        market_regime=regime,
        active_levels=active_levels,
        level_interaction=level_interactions,
        ema_3_19_ai=_signal("ema_3_19_ai", ema_direction),
        futoi=_signal("futoi", futoi_direction),
        news_state=tuple(news) if news is not None else (_news(),),
        macro_state=_macro(macro_direction),
        final_bias=bias,
        trade_state=trade_state,
        confidence=confidence,
        targets=tuple(targets),
        invalidation=invalidation,
        scenario="scenario",
        reason="reason",
        evidence_refs=tuple(evidence_refs),
    )


def _codes(result) -> set[str]:
    return {item.code for item in result.events}


def test_semantically_unchanged_states_create_no_change_events() -> None:
    previous = _state(as_of=T1)
    current = replace(previous, as_of_timestamp=T2.isoformat())

    result = detect_market_state_changes(previous, current)

    assert result.events == ()
    assert result.highest_severity is None
    assert result.significant_change is False
    assert result.action_alert is False
    json.dumps(result.to_dict())


def test_price_only_movement_is_not_a_significant_change() -> None:
    previous = _state(as_of=T1, price=81.0)
    current = replace(previous, as_of_timestamp=T2.isoformat(), price=81.4)

    result = detect_market_state_changes(previous, current)

    assert result.events == ()


def test_wait_to_enter_is_action_and_risk_change() -> None:
    previous = _state(as_of=T1)
    current = _state(
        as_of=T2,
        trade_state="ENTER",
        bias="BULLISH_USD",
        confidence=0.8,
        targets=(_target(),),
        invalidation=_invalidation(),
        interactions=(
            _interaction("support_80", "REJECTION", previous_state="TEST"),
            _interaction("resistance_82", "RETEST_HOLD", previous_state="RETEST"),
        ),
        evidence_refs=("level:support_80", "level:resistance_82"),
    )

    result = detect_market_state_changes(previous, current)

    assert result.highest_severity == "ACTION"
    assert result.action_alert is True
    assert "TRADE_STATE_CHANGED" in _codes(result)
    assert "INVALIDATION_CHANGED" in _codes(result)


def test_breakout_to_retest_hold_is_action_structure_change() -> None:
    previous = _state(
        as_of=T1,
        interactions=(
            _interaction("support_80", "AWAY", previous_state="RANGE_RETURN"),
            _interaction("resistance_82", "BREAKOUT", previous_state="BREAKOUT_ATTEMPT"),
        ),
    )
    current = _state(
        as_of=T2,
        interactions=(
            _interaction("support_80", "AWAY", previous_state="RANGE_RETURN"),
            _interaction("resistance_82", "RETEST_HOLD", previous_state="RETEST"),
        ),
    )

    result = detect_market_state_changes(previous, current)

    event = next(item for item in result.events if item.code == "RETEST_HOLD_CONFIRMED")
    assert event.event_type == "MARKET_STRUCTURE_CHANGED"
    assert event.severity == "ACTION"
    assert event.level_id == "resistance_82"


def test_retest_hold_to_failure_is_action_structure_change() -> None:
    previous = _state(
        as_of=T1,
        interactions=(
            _interaction("support_80", "AWAY", previous_state="RANGE_RETURN"),
            _interaction("resistance_82", "RETEST_HOLD", previous_state="RETEST"),
        ),
    )
    current = _state(
        as_of=T2,
        interactions=(
            _interaction("support_80", "AWAY", previous_state="RANGE_RETURN"),
            _interaction("resistance_82", "RETEST_FAIL", previous_state="RETEST"),
        ),
    )

    result = detect_market_state_changes(previous, current)

    event = next(item for item in result.events if item.code == "RETEST_HOLD_FAILED")
    assert event.severity == "ACTION"


def test_invalidation_change_is_action_for_active_trade() -> None:
    previous = _state(
        as_of=T1,
        trade_state="HOLD",
        targets=(_target(),),
        invalidation=_invalidation("LOWER_BOUND"),
    )
    current = _state(
        as_of=T2,
        trade_state="HOLD",
        targets=(_target(),),
        invalidation=_invalidation("CENTER"),
    )

    result = detect_market_state_changes(previous, current)

    event = next(item for item in result.events if item.code == "INVALIDATION_CHANGED")
    assert event.severity == "ACTION"


def test_ema_and_futoi_changes_keep_separate_change_types() -> None:
    previous = _state(as_of=T1)
    current = _state(
        as_of=T2,
        ema_direction="BEARISH_USD",
        futoi_direction="BEARISH_USD",
    )

    result = detect_market_state_changes(previous, current)

    by_code = {item.code: item for item in result.events}
    assert by_code["EMA_3_19_AI_CHANGED"].event_type == "MODEL_SIGNAL_CHANGED"
    assert by_code["FUTOI_CHANGED"].event_type == "POSITIONING_CHANGED"
    assert by_code["EMA_3_19_AI_CHANGED"].severity == "IMPORTANT"
    assert by_code["FUTOI_CHANGED"].severity == "IMPORTANT"


def test_new_critical_rub_news_is_action_without_recounting_old_event() -> None:
    previous = _state(as_of=T1, news=(_news("news_1"),))
    current = _state(
        as_of=T2,
        news=(
            _news("news_1"),
            _news("news_2", importance="CRITICAL", rub_relevance=0.9, confidence=0.85),
        ),
    )

    result = detect_market_state_changes(previous, current)

    news_events = [item for item in result.events if item.code == "NEW_NEWS_EVENT"]
    assert len(news_events) == 1
    assert news_events[0].severity == "ACTION"
    assert news_events[0].evidence_refs == ("news:news_2",)


def test_support_leaving_active_set_does_not_invent_break_reason() -> None:
    previous = _state(as_of=T1)
    current = _state(
        as_of=T2,
        levels=(_resistance(),),
        interactions=(
            _interaction("resistance_82", "AWAY", previous_state="RANGE_RETURN"),
        ),
        evidence_refs=("level:resistance_82",),
    )

    result = detect_market_state_changes(previous, current)

    event = next(item for item in result.events if item.code == "LEVEL_LEFT_ACTIVE_SET")
    assert event.level_id == "support_80"
    assert event.severity == "IMPORTANT"
    assert "no break cause is inferred" in event.reason


def test_current_snapshot_must_be_strictly_later() -> None:
    previous = _state(as_of=T1)
    current = _state(as_of=T1)

    with pytest.raises(ChangeDetectorError, match="later than previous"):
        detect_market_state_changes(previous, current)
