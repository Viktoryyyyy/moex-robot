from datetime import datetime, timedelta, timezone
import json

import pytest

from src.moex_research.intelligence.usdrubf_decision_engine import (
    DecisionEngineError,
    DecisionInput,
    DirectionalContext,
    build_decision_payload,
    build_market_state,
    ema_context_from_target_position,
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


T0 = datetime(2026, 8, 9, 10, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 8, 9, 10, 5, tzinfo=timezone.utc)
T2 = datetime(2026, 8, 9, 10, 10, tzinfo=timezone.utc)


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
    *,
    state: str = "AWAY",
    previous_state: str | None = "RANGE_RETURN",
    direction: str = "FROM_BELOW",
    quality: float = 0.7,
) -> InteractionSnapshot:
    return InteractionSnapshot(
        level_id=level_id,
        state=state,
        direction=direction,
        event_timestamp=T1.isoformat(),
        previous_state=previous_state,
        structural_quality=quality,
        touch_count=2,
        breakout_side=(
            "ABOVE"
            if state
            in {"BREAKOUT", "RETEST_PENDING", "RETEST", "RETEST_HOLD", "ACCEPTANCE"}
            else None
        ),
        as_of_timestamp=T1.isoformat(),
    )


def _news(*, available_at: datetime = T1, quality_status: str = "OK") -> NewsEvent:
    return NewsEvent(
        event_id="news_1",
        cluster_id="cluster_1",
        source_id="cbr",
        source_tier="OFFICIAL_PRIMARY",
        source_reference="https://example.test/cbr/1",
        published_at=T0.isoformat(),
        available_at=available_at.isoformat(),
        ingested_at=available_at.isoformat(),
        content_hash="a" * 64,
        event_type="MONETARY_POLICY",
        entities=("CBR",),
        rub_relevance=0.9,
        direction="USD_BEARISH",
        importance="HIGH",
        novelty="NEW",
        horizon="SHORT_TERM",
        confidence=0.8,
        mechanism="Rate expectations affect RUB carry.",
        quality_status=quality_status,
    )


def _macro(*, available_at: datetime = T1, quality_status: str = "OK") -> MacroState:
    observation = MacroObservation(
        metric_id="cbr_rate",
        source_id="cbr_key_rate_daily",
        source_reference="https://example.test/cbr-rate",
        value=18.0 if quality_status == "OK" else None,
        unit="percent",
        observed_or_effective_at=T0,
        published_at=T0,
        available_at=available_at,
        ingested_at=available_at,
        quality_status=quality_status,
    )
    return MacroState(
        as_of_timestamp=available_at.isoformat(),
        observations=(observation,),
        overall_direction="USD_BEARISH",
        confidence=0.7,
        dominant_drivers=("cbr_rate",) if quality_status == "OK" else (),
    )


def _ema_context(source_id: str = "ema_3_19_ai") -> DirectionalContext:
    return DirectionalContext(
        source_id=source_id,
        available_at=T1,
        direction="BULLISH_USD",
        confidence=0.8,
        details={"target_position": 1},
    )


def _futoi_context(
    source_id: str = "futoi",
    quality_status: str = "OK",
) -> DirectionalContext:
    return DirectionalContext(
        source_id=source_id,
        available_at=T1,
        direction="BULLISH_USD",
        confidence=0.6,
        quality_status=quality_status,
        details={"interpretation": "participant_positioning_only"},
    )


def _inputs(
    *,
    price: float = 81.0,
    futoi_quality: str = "OK",
    news_event: NewsEvent | None = None,
    macro_state: MacroState | None = None,
    interactions=None,
    ema_context: DirectionalContext | None = None,
    futoi_context: DirectionalContext | None = None,
) -> DecisionInput:
    levels = (_support(), _resistance())
    return DecisionInput(
        as_of_timestamp=T2,
        price=price,
        trend="BULLISH_USD",
        market_regime="RANGE_TO_TREND",
        active_levels=levels,
        level_interactions=(
            interactions
            if interactions is not None
            else (
                _interaction("support_80"),
                _interaction(
                    "resistance_82",
                    state="RETEST_HOLD",
                    previous_state="RETEST",
                ),
            )
        ),
        ema_3_19_ai=ema_context or _ema_context(),
        futoi=futoi_context or _futoi_context(quality_status=futoi_quality),
        news_events=(news_event or _news(),),
        macro_state=macro_state or _macro(),
    )


def _decision(**overrides):
    output = {
        "final_bias": "BULLISH_USD",
        "trade_state": "ENTER",
        "confidence": 0.82,
        "target_references": [
            {"level_id": "resistance_82", "price_anchor": "CENTER"},
        ],
        "invalidation_reference": {
            "level_id": "support_80",
            "price_anchor": "LOWER_BOUND",
        },
        "scenario": "Bullish continuation after confirmed structure holds.",
        "reason": "Structure, EMA and positioning support the bullish scenario.",
        "evidence_refs": [
            "level:support_80",
            "level:resistance_82",
            "signal:ema_3_19_ai",
            "signal:futoi",
            "news:news_1",
            "macro:cbr_rate",
        ],
    }
    output.update(overrides)
    return output


def test_market_state_resolves_numeric_prices_only_from_active_levels() -> None:
    state = build_market_state(_inputs(), decision_agent=lambda _payload: _decision())

    assert state.final_bias == "BULLISH_USD"
    assert state.trade_state == "ENTER"
    assert state.targets[0].level_id == "resistance_82"
    assert state.targets[0].price == 82.0
    assert state.invalidation is not None
    assert state.invalidation.level_id == "support_80"
    assert state.invalidation.price == 79.8
    serialized = state.to_dict()
    assert serialized["targets"][0]["price"] == 82.0
    assert serialized["invalidation"]["price"] == 79.8
    json.dumps(serialized)


def test_agent_cannot_create_numeric_target_or_other_factual_fields() -> None:
    output = _decision(target_price=83.5)
    with pytest.raises(DecisionEngineError, match="undeclared fields"):
        build_market_state(_inputs(), decision_agent=lambda _payload: output)


def test_agent_cannot_reference_unknown_level() -> None:
    output = _decision(
        target_references=[{"level_id": "invented_90", "price_anchor": "CENTER"}]
    )
    with pytest.raises(DecisionEngineError, match="unknown active level_id"):
        build_market_state(_inputs(), decision_agent=lambda _payload: output)


def test_enter_requires_target_and_invalidation_references() -> None:
    with pytest.raises(DecisionEngineError, match="ENTER requires at least one target"):
        build_market_state(
            _inputs(),
            decision_agent=lambda _payload: _decision(
                target_references=[],
                invalidation_reference=None,
            ),
        )


def test_future_news_is_rejected_before_decision_agent() -> None:
    calls = []
    with pytest.raises(DecisionEngineError, match="news event is not yet available"):
        inputs = _inputs(news_event=_news(available_at=T2 + timedelta(minutes=1)))
        build_market_state(
            inputs,
            decision_agent=lambda payload: calls.append(payload) or _decision(),
        )
    assert calls == []


def test_level_interactions_must_cover_each_active_level_exactly_once() -> None:
    with pytest.raises(DecisionEngineError, match="exactly one record"):
        _inputs(interactions=(_interaction("support_80"),))


def test_blocked_futoi_cannot_be_cited_as_usable_evidence() -> None:
    inputs = _inputs(futoi_quality="BLOCKED")
    payload = build_decision_payload(inputs)
    assert payload["futoi"]["usable"] is False
    assert "signal:futoi" not in payload["output_contract"]["allowed_evidence_refs"]
    with pytest.raises(DecisionEngineError, match="supplied usable facts"):
        build_market_state(inputs, decision_agent=lambda _payload: _decision())


def test_payload_contains_all_five_decision_domains() -> None:
    payload = build_decision_payload(_inputs())

    assert payload["market_facts"]["level_interactions"][1]["state"] == "RETEST_HOLD"
    assert payload["ema_3_19_ai"]["direction"] == "BULLISH_USD"
    assert payload["futoi"]["details"]["interpretation"] == "participant_positioning_only"
    assert payload["news_state"]["events"][0]["event_id"] == "news_1"
    assert payload["macro_state"]["observations"][0]["metric_id"] == "cbr_rate"
    assert payload["output_contract"]["numeric_level_creation_forbidden"] is True
    assert payload["output_contract"]["actionable_entry_requires_structural_confirmation"] is True


def test_breakout_alone_does_not_automatically_create_long_trade_state() -> None:
    interactions = (
        _interaction("support_80"),
        _interaction(
            "resistance_82",
            state="BREAKOUT",
            previous_state="BREAKOUT_ATTEMPT",
            quality=0.65,
        ),
    )
    inputs = _inputs(interactions=interactions)
    state = build_market_state(
        inputs,
        decision_agent=lambda _payload: _decision(
            final_bias="NEUTRAL",
            trade_state="WAIT",
            confidence=0.45,
            target_references=[],
            invalidation_reference=None,
            scenario="Wait for retest or acceptance.",
            reason="A breakout exists but confirmation is not yet sufficient.",
            evidence_refs=["level:resistance_82"],
        ),
    )

    assert state.level_interaction[1].state == "BREAKOUT"
    assert state.trade_state == "WAIT"
    assert state.final_bias == "NEUTRAL"


def test_breakout_only_enter_is_rejected_by_validator() -> None:
    interactions = (
        _interaction("support_80"),
        _interaction(
            "resistance_82",
            state="BREAKOUT",
            previous_state="BREAKOUT_ATTEMPT",
            quality=0.65,
        ),
    )
    inputs = _inputs(interactions=interactions)
    with pytest.raises(DecisionEngineError, match="unconfirmed breakout or retest"):
        build_market_state(
            inputs,
            decision_agent=lambda _payload: _decision(
                evidence_refs=["level:resistance_82"],
            ),
        )


def test_signal_contexts_are_bound_to_expected_sources() -> None:
    with pytest.raises(DecisionEngineError, match="ema_3_19_ai context source_id mismatch"):
        _inputs(ema_context=_ema_context(source_id="futoi"))
    with pytest.raises(DecisionEngineError, match="futoi context source_id mismatch"):
        _inputs(futoi_context=_futoi_context(source_id="ema_3_19_ai"))


def test_ema_target_position_helper_maps_only_explicit_positions() -> None:
    assert ema_context_from_target_position(1, available_at=T1).direction == "BULLISH_USD"
    assert ema_context_from_target_position(-1, available_at=T1).direction == "BEARISH_USD"
    assert ema_context_from_target_position(0, available_at=T1).direction == "NEUTRAL"
    with pytest.raises(DecisionEngineError, match="target_position"):
        ema_context_from_target_position(None, available_at=T1)  # type: ignore[arg-type]
    with pytest.raises(DecisionEngineError, match="target_position"):
        ema_context_from_target_position(2, available_at=T1)
    with pytest.raises(DecisionEngineError, match="target_position"):
        ema_context_from_target_position(True, available_at=T1)  # type: ignore[arg-type]
    with pytest.raises(DecisionEngineError, match="target_position"):
        ema_context_from_target_position(False, available_at=T1)  # type: ignore[arg-type]


@pytest.mark.parametrize("price", [float("nan"), float("inf")])
def test_non_finite_market_price_is_rejected(price: float) -> None:
    with pytest.raises(DecisionEngineError, match="finite positive"):
        _inputs(price=price)


def test_future_macro_observation_is_rejected() -> None:
    future = _macro(available_at=T2 + timedelta(minutes=1))
    with pytest.raises(DecisionEngineError, match="macro_state is from the future"):
        _inputs(macro_state=future)
