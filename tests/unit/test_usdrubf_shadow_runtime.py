from datetime import datetime, timedelta, timezone
import json

import pytest

from src.moex_research.intelligence.usdrubf_decision_engine import (
    DecisionInput,
    DirectionalContext,
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
from src.moex_research.intelligence.usdrubf_shadow_runtime import (
    ShadowJsonStore,
    ShadowRuntime,
    ShadowRuntimeError,
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
    event_timestamp: datetime,
    previous_state: str | None,
) -> InteractionSnapshot:
    return InteractionSnapshot(
        level_id=level_id,
        state=state,
        direction="FROM_BELOW",
        event_timestamp=event_timestamp.isoformat(),
        previous_state=previous_state,
        structural_quality=0.8,
        touch_count=2,
        breakout_side=(
            "ABOVE"
            if state in {"BREAKOUT", "RETEST_PENDING", "RETEST", "RETEST_HOLD", "ACCEPTANCE"}
            else None
        ),
        as_of_timestamp=event_timestamp.isoformat(),
    )


def _news(available_at: datetime) -> NewsEvent:
    return NewsEvent(
        event_id="news_1",
        cluster_id="cluster_1",
        source_id="cbr",
        source_tier="OFFICIAL_PRIMARY",
        source_reference="https://example.test/cbr",
        published_at=T0.isoformat(),
        available_at=available_at.isoformat(),
        ingested_at=available_at.isoformat(),
        content_hash="a" * 64,
        event_type="MONETARY_POLICY",
        entities=("CBR",),
        rub_relevance=0.8,
        direction="USD_BEARISH",
        importance="MEDIUM",
        novelty="NEW",
        horizon="SHORT_TERM",
        confidence=0.7,
        mechanism="Policy context.",
        quality_status="OK",
    )


def _macro(available_at: datetime) -> MacroState:
    observation = MacroObservation(
        metric_id="cbr_rate",
        source_id="cbr_key_rate_daily",
        source_reference="https://example.test/cbr-rate",
        value=18.0,
        unit="percent",
        observed_or_effective_at=T0,
        published_at=T0,
        available_at=available_at,
        ingested_at=available_at,
        quality_status="OK",
    )
    return MacroState(
        as_of_timestamp=available_at.isoformat(),
        observations=(observation,),
        overall_direction="NEUTRAL",
        confidence=0.6,
        dominant_drivers=("cbr_rate",),
    )


def _inputs(
    as_of: datetime,
    *,
    resistance_state: str = "BREAKOUT",
    resistance_previous_state: str = "BREAKOUT_ATTEMPT",
    signal_details: bool = True,
) -> DecisionInput:
    return DecisionInput(
        as_of_timestamp=as_of,
        price=81.0,
        trend="BULLISH_USD",
        market_regime="RANGE_TO_TREND",
        active_levels=(_support(), _resistance()),
        level_interactions=(
            _interaction(
                "support_80",
                "AWAY",
                event_timestamp=as_of,
                previous_state="RANGE_RETURN",
            ),
            _interaction(
                "resistance_82",
                resistance_state,
                event_timestamp=as_of,
                previous_state=resistance_previous_state,
            ),
        ),
        ema_3_19_ai=DirectionalContext(
            source_id="ema_3_19_ai",
            available_at=as_of,
            direction="BULLISH_USD",
            confidence=0.8,
            details={"target_position": 1} if signal_details else None,
        ),
        futoi=DirectionalContext(
            source_id="futoi",
            available_at=as_of,
            direction="BULLISH_USD",
            confidence=0.6,
            details={"interpretation": "participant_positioning_only"}
            if signal_details
            else None,
        ),
        news_events=(_news(as_of),),
        macro_state=_macro(as_of),
    )


def _wait_decision():
    return {
        "final_bias": "NEUTRAL",
        "trade_state": "WAIT",
        "confidence": 0.45,
        "target_references": [],
        "invalidation_reference": None,
        "scenario": "Wait for structural confirmation.",
        "reason": "Breakout is not yet confirmed by a held retest.",
        "evidence_refs": ["level:resistance_82"],
    }


def _enter_decision():
    return {
        "final_bias": "BULLISH_USD",
        "trade_state": "ENTER",
        "confidence": 0.82,
        "target_references": [
            {"level_id": "resistance_82", "price_anchor": "UPPER_BOUND"},
        ],
        "invalidation_reference": {
            "level_id": "support_80",
            "price_anchor": "LOWER_BOUND",
        },
        "scenario": "Confirmed retest hold supports bullish continuation.",
        "reason": "Structure is confirmed and risk is bounded by active levels.",
        "evidence_refs": [
            "level:support_80",
            "level:resistance_82",
            "signal:ema_3_19_ai",
            "signal:futoi",
        ],
    }


def test_first_shadow_cycle_commits_generation_without_change_alert(tmp_path) -> None:
    store = ShadowJsonStore(tmp_path)
    runtime = ShadowRuntime(store)

    result = runtime.run_cycle(
        _inputs(T1),
        decision_agent=lambda _payload: _wait_decision(),
    )

    assert result.change_detection is None
    assert result.significant_change is False
    assert result.action_candidate is False
    assert result.market_state_path.parent == tmp_path
    assert result.change_detection_path.parent == tmp_path
    assert result.market_state_path.name.startswith("market_state.")
    assert result.change_detection_path.name.startswith("change_detection.")
    assert (tmp_path / "current_cycle.json").is_file()
    assert json.loads(result.change_detection_path.read_text(encoding="utf-8")) is None
    persisted = json.loads(result.market_state_path.read_text(encoding="utf-8"))
    assert persisted["trade_state"] == "WAIT"
    assert store.load_change_detection_raw() is None


def test_second_cycle_loads_previous_state_and_emits_action_candidate(tmp_path) -> None:
    runtime = ShadowRuntime(ShadowJsonStore(tmp_path))
    runtime.run_cycle(
        _inputs(T1),
        decision_agent=lambda _payload: _wait_decision(),
    )

    second = runtime.run_cycle(
        _inputs(
            T2,
            resistance_state="RETEST_HOLD",
            resistance_previous_state="RETEST",
        ),
        decision_agent=lambda _payload: _enter_decision(),
    )

    assert second.change_detection is not None
    assert second.change_detection.highest_severity == "ACTION"
    assert second.action_candidate is True
    codes = {item.code for item in second.change_detection.events}
    assert "RETEST_HOLD_CONFIRMED" in codes
    assert "TRADE_STATE_CHANGED" in codes
    persisted_change = json.loads(second.change_detection_path.read_text(encoding="utf-8"))
    assert persisted_change["highest_severity"] == "ACTION"


def test_store_restores_market_state_across_runtime_instance_restart(tmp_path) -> None:
    first_runtime = ShadowRuntime(ShadowJsonStore(tmp_path))
    first_runtime.run_cycle(
        _inputs(T1),
        decision_agent=lambda _payload: _wait_decision(),
    )

    restarted = ShadowRuntime(ShadowJsonStore(tmp_path))
    second = restarted.run_cycle(
        _inputs(
            T2,
            resistance_state="RETEST_HOLD",
            resistance_previous_state="RETEST",
        ),
        decision_agent=lambda _payload: _enter_decision(),
    )

    assert second.change_detection is not None
    assert second.change_detection.previous_as_of_timestamp == T1.isoformat()
    assert second.change_detection.current_as_of_timestamp == T2.isoformat()


def test_directional_context_none_details_survive_restart_restore(tmp_path) -> None:
    first_runtime = ShadowRuntime(ShadowJsonStore(tmp_path))
    first_runtime.run_cycle(
        _inputs(T1, signal_details=False),
        decision_agent=lambda _payload: _wait_decision(),
    )

    restored = ShadowJsonStore(tmp_path).load_market_state()

    assert restored is not None
    assert restored.ema_3_19_ai.details is None
    assert restored.futoi.details is None


def test_tampered_numeric_target_price_fails_closed_on_restore(tmp_path) -> None:
    store = ShadowJsonStore(tmp_path)
    result = ShadowRuntime(store).run_cycle(
        _inputs(
            T1,
            resistance_state="RETEST_HOLD",
            resistance_previous_state="RETEST",
        ),
        decision_agent=lambda _payload: _enter_decision(),
    )

    payload = json.loads(result.market_state_path.read_text(encoding="utf-8"))
    payload["targets"][0]["price"] = 99.0
    result.market_state_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ShadowRuntimeError, match="deterministic level anchor"):
        store.load_market_state()


def test_tampered_enter_without_risk_references_fails_closed_on_restore(tmp_path) -> None:
    store = ShadowJsonStore(tmp_path)
    result = ShadowRuntime(store).run_cycle(
        _inputs(T1),
        decision_agent=lambda _payload: _wait_decision(),
    )

    payload = json.loads(result.market_state_path.read_text(encoding="utf-8"))
    payload["trade_state"] = "ENTER"
    payload["final_bias"] = "BULLISH_USD"
    payload["targets"] = []
    payload["invalidation"] = None
    result.market_state_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ShadowRuntimeError, match="persisted ENTER requires target"):
        store.load_market_state()


def test_tampered_evidence_reference_fails_closed_on_restore(tmp_path) -> None:
    store = ShadowJsonStore(tmp_path)
    result = ShadowRuntime(store).run_cycle(
        _inputs(T1),
        decision_agent=lambda _payload: _wait_decision(),
    )

    payload = json.loads(result.market_state_path.read_text(encoding="utf-8"))
    payload["evidence_refs"] = ["level:invented"]
    result.market_state_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ShadowRuntimeError, match="unavailable facts"):
        store.load_market_state()


def test_failed_pointer_commit_keeps_previous_cycle_authoritative(tmp_path) -> None:
    store = ShadowJsonStore(tmp_path)
    runtime = ShadowRuntime(store)
    first = runtime.run_cycle(
        _inputs(T1),
        decision_agent=lambda _payload: _wait_decision(),
    )
    pointer_path = tmp_path / "current_cycle.json"
    prior_pointer = pointer_path.read_bytes()

    original_write = store._write_atomic

    def fail_pointer(filename, payload):
        if filename == store.pointer_filename:
            raise OSError("simulated pointer commit failure")
        return original_write(filename, payload)

    store._write_atomic = fail_pointer  # type: ignore[method-assign]
    with pytest.raises(OSError, match="pointer commit failure"):
        runtime.run_cycle(
            _inputs(
                T2,
                resistance_state="RETEST_HOLD",
                resistance_previous_state="RETEST",
            ),
            decision_agent=lambda _payload: _enter_decision(),
        )

    assert pointer_path.read_bytes() == prior_pointer
    restored = ShadowJsonStore(tmp_path).load_market_state()
    assert restored is not None
    assert restored.as_of_timestamp == T1.isoformat()
    assert restored.trade_state == "WAIT"
    assert first.market_state_path.is_file()


def test_shadow_store_uses_only_explicit_root_and_distinct_basenames(tmp_path) -> None:
    store = ShadowJsonStore(
        tmp_path,
        market_state_filename="current.json",
        change_filename="changes.json",
        pointer_filename="pointer.json",
    )
    assert store.root == tmp_path

    with pytest.raises(ShadowRuntimeError, match="distinct"):
        ShadowJsonStore(
            tmp_path,
            market_state_filename="same.json",
            change_filename="same.json",
        )
    with pytest.raises(ShadowRuntimeError, match="distinct"):
        ShadowJsonStore(
            tmp_path,
            market_state_filename="same.json",
            pointer_filename="same.json",
        )
    with pytest.raises(ShadowRuntimeError, match="basename"):
        ShadowJsonStore(tmp_path, market_state_filename="../state.json")


def test_shadow_cycle_rejects_non_monotonic_restart_sequence(tmp_path) -> None:
    runtime = ShadowRuntime(ShadowJsonStore(tmp_path))
    runtime.run_cycle(
        _inputs(T1),
        decision_agent=lambda _payload: _wait_decision(),
    )

    with pytest.raises(ValueError, match="later than previous"):
        runtime.run_cycle(
            _inputs(T1),
            decision_agent=lambda _payload: _wait_decision(),
        )


def test_atomic_shadow_snapshots_leave_no_temp_files(tmp_path) -> None:
    runtime = ShadowRuntime(ShadowJsonStore(tmp_path))
    runtime.run_cycle(
        _inputs(T1),
        decision_agent=lambda _payload: _wait_decision(),
    )

    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []
