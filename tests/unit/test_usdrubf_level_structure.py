from dataclasses import asdict

from src.moex_research.intelligence.usdrubf_level_structure import (
    EngineConfig,
    LevelZone,
    classify_level_history,
)


def _bar(end: str, high: float, low: float, close: float) -> dict:
    return {"end": end, "high": high, "low": low, "close": close}


def _zone(
    level_id: str = "r1",
    level_type: str = "RESISTANCE",
    center: float = 82.70,
    lower: float = 82.65,
    upper: float = 82.75,
    created_at: str = "2026-08-09T09:00:00+03:00",
) -> LevelZone:
    return LevelZone(
        level_id=level_id,
        level_type=level_type,
        center_price=center,
        lower_bound=lower,
        upper_bound=upper,
        created_at=created_at,
        source_timeframe="5m",
        status="ACTIVE",
    )


def _resistance_breakout_prefix() -> list[dict]:
    return [
        _bar("2026-08-09T10:00:00+03:00", 82.60, 82.50, 82.60),
        _bar("2026-08-09T10:05:00+03:00", 82.72, 82.64, 82.68),
        _bar("2026-08-09T10:10:00+03:00", 82.86, 82.76, 82.82),
        _bar("2026-08-09T10:15:00+03:00", 82.96, 82.80, 82.91),
    ]


def test_empty_history_is_untouched() -> None:
    result = classify_level_history(_zone(), [])
    assert result.state == "UNTOUCHED"
    assert result.direction == "NONE"
    assert result.event_timestamp is None
    assert result.previous_state is None


def test_prefix_history_follows_frozen_transition_sequence() -> None:
    zone = _zone()
    bars = _resistance_breakout_prefix() + [
        _bar("2026-08-09T10:20:00+03:00", 82.87, 82.70, 82.78),
        _bar("2026-08-09T10:25:00+03:00", 82.88, 82.70, 82.79),
        _bar("2026-08-09T10:30:00+03:00", 82.94, 82.77, 82.88),
    ]
    expected = [
        ("APPROACH", "UNTOUCHED"),
        ("TEST", "APPROACH"),
        ("BREAKOUT_ATTEMPT", "TEST"),
        ("BREAKOUT", "BREAKOUT_ATTEMPT"),
        ("RETEST_PENDING", "BREAKOUT"),
        ("RETEST", "RETEST_PENDING"),
        ("RETEST_HOLD", "RETEST"),
    ]
    actual = []
    for index in range(1, len(bars) + 1):
        result = classify_level_history(zone, bars[:index])
        actual.append((result.state, result.previous_state))
        assert result.event_timestamp is not None
    assert actual == expected


def test_resistance_rejection_is_not_downside_breakout() -> None:
    bars = [
        _bar("2026-08-09T10:00:00+03:00", 82.60, 82.50, 82.60),
        _bar("2026-08-09T10:05:00+03:00", 82.72, 82.64, 82.68),
        _bar("2026-08-09T10:10:00+03:00", 82.64, 82.45, 82.50),
    ]
    result = classify_level_history(_zone(), bars)
    assert result.state == "REJECTION"
    assert result.breakout_side is None


def test_support_rejection_and_breakout_are_directional() -> None:
    zone = _zone("s1", "SUPPORT", 80.00, 79.95, 80.05)
    rejection = [
        _bar("2026-08-09T10:00:00+03:00", 80.20, 80.10, 80.10),
        _bar("2026-08-09T10:05:00+03:00", 80.04, 79.96, 80.01),
        _bar("2026-08-09T10:10:00+03:00", 80.25, 80.10, 80.20),
    ]
    assert classify_level_history(zone, rejection).state == "REJECTION"

    breakout = [
        _bar("2026-08-09T10:00:00+03:00", 80.20, 80.10, 80.10),
        _bar("2026-08-09T10:05:00+03:00", 80.04, 79.96, 80.01),
        _bar("2026-08-09T10:10:00+03:00", 79.94, 79.78, 79.88),
        _bar("2026-08-09T10:15:00+03:00", 79.92, 79.72, 79.82),
    ]
    result = classify_level_history(zone, breakout)
    assert result.state == "BREAKOUT"
    assert result.breakout_side == "BELOW"


def test_direction_neutral_range_boundary_can_break_out() -> None:
    zone = _zone("range1", "RANGE_BOUNDARY")
    bars = [
        _bar("2026-08-09T10:00:00+03:00", 82.72, 82.66, 82.70),
        _bar("2026-08-09T10:05:00+03:00", 82.73, 82.67, 82.71),
        _bar("2026-08-09T10:10:00+03:00", 82.88, 82.80, 82.84),
        _bar("2026-08-09T10:15:00+03:00", 82.94, 82.82, 82.90),
    ]
    result = classify_level_history(zone, bars)
    assert result.state == "BREAKOUT"
    assert result.breakout_side == "ABOVE"


def test_successful_breakout_retest_hold_path() -> None:
    bars = _resistance_breakout_prefix() + [
        _bar("2026-08-09T10:20:00+03:00", 82.87, 82.70, 82.78),
        _bar("2026-08-09T10:25:00+03:00", 82.88, 82.70, 82.79),
        _bar("2026-08-09T10:30:00+03:00", 82.94, 82.77, 82.88),
    ]
    result = classify_level_history(_zone(), bars)
    assert result.state == "RETEST_HOLD"
    assert result.previous_state == "RETEST"
    assert result.breakout_side == "ABOVE"
    assert result.structural_quality > 0.5


def test_failed_retest_becomes_false_breakout() -> None:
    bars = _resistance_breakout_prefix() + [
        _bar("2026-08-09T10:20:00+03:00", 82.87, 82.70, 82.73),
        _bar("2026-08-09T10:25:00+03:00", 82.86, 82.69, 82.72),
        _bar("2026-08-09T10:30:00+03:00", 82.64, 82.55, 82.60),
        _bar("2026-08-09T10:35:00+03:00", 82.60, 82.48, 82.55),
    ]
    result = classify_level_history(_zone(), bars)
    assert result.state == "FALSE_BREAKOUT"
    assert result.previous_state == "RETEST_FAIL"
    assert result.breakout_side == "ABOVE"


def test_acceptance_requires_consecutive_qualifying_closes() -> None:
    cfg = EngineConfig(acceptance_distance_widths=0.5, acceptance_close_count=2)
    qualifying = _resistance_breakout_prefix() + [
        _bar("2026-08-09T10:20:00+03:00", 83.02, 82.90, 82.96),
        _bar("2026-08-09T10:25:00+03:00", 83.07, 82.95, 83.00),
    ]
    assert classify_level_history(_zone(), qualifying, cfg).state == "ACCEPTANCE"

    interrupted = _resistance_breakout_prefix() + [
        _bar("2026-08-09T10:20:00+03:00", 83.02, 82.90, 82.96),
        _bar("2026-08-09T10:25:00+03:00", 82.81, 82.76, 82.79),
        _bar("2026-08-09T10:30:00+03:00", 83.03, 82.91, 82.97),
    ]
    result = classify_level_history(_zone(), interrupted, cfg)
    assert result.state == "RETEST_PENDING"


def test_retest_resets_pre_retest_acceptance_counter() -> None:
    cfg = EngineConfig(acceptance_distance_widths=0.5, acceptance_close_count=2)
    bars = _resistance_breakout_prefix() + [
        _bar("2026-08-09T10:20:00+03:00", 83.02, 82.90, 82.96),
        _bar("2026-08-09T10:25:00+03:00", 82.87, 82.70, 82.78),
        _bar("2026-08-09T10:30:00+03:00", 82.94, 82.77, 82.88),
        _bar("2026-08-09T10:35:00+03:00", 83.02, 82.90, 82.97),
    ]
    result = classify_level_history(_zone(), bars, cfg)
    assert result.state == "RETEST_HOLD"


def test_level_zone_and_interaction_expose_contract_metadata() -> None:
    zone = _zone()
    zone_payload = asdict(zone)
    assert {"created_at", "source_timeframe", "status"}.issubset(zone_payload)

    result = classify_level_history(zone, _resistance_breakout_prefix())
    payload = asdict(result)
    assert payload["level_id"] == zone.level_id
    assert payload["event_timestamp"] == "2026-08-09T10:15:00+03:00"
    assert payload["previous_state"] == "BREAKOUT_ATTEMPT"
    assert payload["as_of_timestamp"] == "2026-08-09T10:15:00+03:00"


def test_same_input_is_reproducible_and_quality_context_is_bounded() -> None:
    zone = _zone()
    bars = _resistance_breakout_prefix()
    first = classify_level_history(zone, bars, volume_confirmation=0.8, volatility_context=0.6)
    second = classify_level_history(zone, bars, volume_confirmation=0.8, volatility_context=0.6)
    assert first == second
    assert 0.0 <= first.structural_quality <= 1.0


def test_rejects_non_increasing_time_and_level_lookahead() -> None:
    zone = _zone()
    duplicated = [
        _bar("2026-08-09T10:05:00+03:00", 82.72, 82.60, 82.68),
        _bar("2026-08-09T10:05:00+03:00", 82.80, 82.67, 82.76),
    ]
    try:
        classify_level_history(zone, duplicated)
    except ValueError as exc:
        assert "strictly increasing" in str(exc)
    else:
        raise AssertionError("expected ValueError")

    future_zone = _zone(created_at="2026-08-09T11:00:00+03:00")
    try:
        classify_level_history(future_zone, _resistance_breakout_prefix())
    except ValueError as exc:
        assert "created_at" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_rejects_unknown_level_type_or_status() -> None:
    try:
        _zone(level_type="UNKNOWN")
    except ValueError as exc:
        assert "level_type" in str(exc)
    else:
        raise AssertionError("expected ValueError")

    try:
        LevelZone("x", "RESISTANCE", 82.70, 82.65, 82.75, "2026-08-09T09:00:00+03:00", "5m", "UNKNOWN")
    except ValueError as exc:
        assert "status" in str(exc)
    else:
        raise AssertionError("expected ValueError")
