from src.moex_research.intelligence.usdrubf_level_structure import (
    EngineConfig,
    LevelZone,
    classify_level_history,
)


def _bar(end: str, high: float, low: float, close: float) -> dict:
    return {"end": end, "high": high, "low": low, "close": close}


def test_empty_history_is_untouched() -> None:
    zone = LevelZone("r1", "RESISTANCE", 82.70, 82.65, 82.75)
    result = classify_level_history(zone, [])
    assert result.level_id == "r1"
    assert result.state == "UNTOUCHED"
    assert result.direction == "NONE"


def test_single_tick_or_single_close_does_not_confirm_breakout() -> None:
    zone = LevelZone("r1", "RESISTANCE", 82.70, 82.65, 82.75)
    bars = [
        _bar("2026-08-09T10:00:00+03:00", 82.72, 82.60, 82.68),
        _bar("2026-08-09T10:05:00+03:00", 82.86, 82.68, 82.82),
    ]
    result = classify_level_history(zone, bars)
    assert result.state == "BREAKOUT_ATTEMPT"
    assert result.breakout_side == "ABOVE"


def test_successful_breakout_retest_hold_path() -> None:
    zone = LevelZone("r1", "RESISTANCE", 82.70, 82.65, 82.75)
    bars = [
        _bar("2026-08-09T10:00:00+03:00", 82.70, 82.58, 82.66),
        _bar("2026-08-09T10:05:00+03:00", 82.88, 82.68, 82.82),
        _bar("2026-08-09T10:10:00+03:00", 82.96, 82.80, 82.91),
        _bar("2026-08-09T10:15:00+03:00", 82.87, 82.70, 82.78),
        _bar("2026-08-09T10:20:00+03:00", 82.94, 82.77, 82.88),
    ]
    result = classify_level_history(zone, bars)
    assert result.state == "RETEST_HOLD"
    assert result.breakout_side == "ABOVE"
    assert result.touch_count >= 2
    assert result.structural_quality > 0.5


def test_failed_retest_becomes_false_breakout() -> None:
    zone = LevelZone("r1", "RESISTANCE", 82.70, 82.65, 82.75)
    bars = [
        _bar("2026-08-09T10:00:00+03:00", 82.71, 82.60, 82.67),
        _bar("2026-08-09T10:05:00+03:00", 82.88, 82.68, 82.83),
        _bar("2026-08-09T10:10:00+03:00", 82.96, 82.81, 82.91),
        _bar("2026-08-09T10:15:00+03:00", 82.86, 82.69, 82.73),
        _bar("2026-08-09T10:20:00+03:00", 82.74, 82.55, 82.60),
        _bar("2026-08-09T10:25:00+03:00", 82.66, 82.48, 82.55),
    ]
    result = classify_level_history(zone, bars)
    assert result.state == "FALSE_BREAKOUT"
    assert result.breakout_side == "ABOVE"


def test_acceptance_requires_multiple_closed_bars_away_from_zone() -> None:
    zone = LevelZone("r1", "RESISTANCE", 82.70, 82.65, 82.75)
    cfg = EngineConfig(acceptance_distance_widths=0.5, acceptance_close_count=2)
    bars = [
        _bar("2026-08-09T10:00:00+03:00", 82.72, 82.61, 82.67),
        _bar("2026-08-09T10:05:00+03:00", 82.88, 82.69, 82.83),
        _bar("2026-08-09T10:10:00+03:00", 82.98, 82.82, 82.92),
        _bar("2026-08-09T10:15:00+03:00", 83.02, 82.88, 82.96),
        _bar("2026-08-09T10:20:00+03:00", 83.07, 82.91, 83.00),
    ]
    result = classify_level_history(zone, bars, cfg)
    assert result.state == "ACCEPTANCE"
    assert result.structural_quality > 0.5


def test_same_input_is_reproducible_and_causal_prefix_differs() -> None:
    zone = LevelZone("r1", "RESISTANCE", 82.70, 82.65, 82.75)
    bars = [
        _bar("2026-08-09T10:00:00+03:00", 82.71, 82.60, 82.67),
        _bar("2026-08-09T10:05:00+03:00", 82.87, 82.68, 82.82),
        _bar("2026-08-09T10:10:00+03:00", 82.95, 82.80, 82.90),
    ]
    first = classify_level_history(zone, bars)
    second = classify_level_history(zone, bars)
    prefix = classify_level_history(zone, bars[:2])
    assert first == second
    assert prefix.state == "BREAKOUT_ATTEMPT"
    assert first.state in {"BREAKOUT", "RETEST_PENDING"}


def test_rejects_non_increasing_bar_time() -> None:
    zone = LevelZone("r1", "RESISTANCE", 82.70, 82.65, 82.75)
    bars = [
        _bar("2026-08-09T10:05:00+03:00", 82.72, 82.60, 82.68),
        _bar("2026-08-09T10:05:00+03:00", 82.80, 82.67, 82.76),
    ]
    try:
        classify_level_history(zone, bars)
    except ValueError as exc:
        assert "strictly increasing" in str(exc)
    else:
        raise AssertionError("expected ValueError")
