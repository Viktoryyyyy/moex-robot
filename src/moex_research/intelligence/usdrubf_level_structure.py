from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping


_ALLOWED_STATES = {
    "UNTOUCHED",
    "APPROACH",
    "TEST",
    "REPEATED_TEST",
    "REJECTION",
    "BREAKOUT_ATTEMPT",
    "BREAKOUT",
    "RETEST_PENDING",
    "RETEST",
    "RETEST_HOLD",
    "RETEST_FAIL",
    "ACCEPTANCE",
    "FALSE_BREAKOUT",
    "RANGE_RETURN",
    "AWAY",
}


@dataclass(frozen=True)
class LevelZone:
    level_id: str
    level_type: str
    center_price: float
    lower_bound: float
    upper_bound: float

    def __post_init__(self) -> None:
        if not self.level_id.strip():
            raise ValueError("level_id must be non-empty")
        if not (0 < self.lower_bound <= self.center_price <= self.upper_bound):
            raise ValueError("invalid level zone bounds")
        if self.upper_bound == self.lower_bound:
            raise ValueError("level zone must have non-zero width")

    @property
    def width(self) -> float:
        return self.upper_bound - self.lower_bound


@dataclass(frozen=True)
class InteractionSnapshot:
    level_id: str
    state: str
    direction: str
    touch_count: int
    breakout_side: str | None
    structural_quality: float
    as_of_timestamp: str | None

    def __post_init__(self) -> None:
        if self.state not in _ALLOWED_STATES:
            raise ValueError("invalid interaction state")
        if self.direction not in {"FROM_BELOW", "FROM_ABOVE", "INSIDE_ZONE", "NONE"}:
            raise ValueError("invalid interaction direction")
        if self.breakout_side not in {None, "ABOVE", "BELOW"}:
            raise ValueError("invalid breakout_side")
        if self.touch_count < 0:
            raise ValueError("touch_count must be non-negative")
        if not 0.0 <= self.structural_quality <= 1.0:
            raise ValueError("structural_quality out of range")


@dataclass(frozen=True)
class EngineConfig:
    approach_distance_widths: float = 1.0
    rejection_distance_widths: float = 1.0
    acceptance_distance_widths: float = 0.5
    acceptance_close_count: int = 2

    def __post_init__(self) -> None:
        if self.approach_distance_widths <= 0:
            raise ValueError("approach_distance_widths must be positive")
        if self.rejection_distance_widths <= 0:
            raise ValueError("rejection_distance_widths must be positive")
        if self.acceptance_distance_widths < 0:
            raise ValueError("acceptance_distance_widths must be non-negative")
        if self.acceptance_close_count < 2:
            raise ValueError("acceptance_close_count must be >= 2")


def _bar_timestamp(bar: Mapping[str, object]) -> datetime:
    raw = bar.get("end")
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str) and raw.strip():
        return datetime.fromisoformat(raw)
    raise ValueError("bar.end must be datetime or ISO datetime string")


def _number(bar: Mapping[str, object], key: str) -> float:
    raw = bar.get(key)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"bar.{key} must be numeric")
    value = float(raw)
    if value <= 0:
        raise ValueError(f"bar.{key} must be positive")
    return value


def _validate_bar(bar: Mapping[str, object]) -> tuple[datetime, float, float, float]:
    ts = _bar_timestamp(bar)
    high = _number(bar, "high")
    low = _number(bar, "low")
    close = _number(bar, "close")
    if low > high or not low <= close <= high:
        raise ValueError("invalid OHLC bar")
    return ts, high, low, close


def _touches(zone: LevelZone, high: float, low: float) -> bool:
    return low <= zone.upper_bound and high >= zone.lower_bound


def _close_side(zone: LevelZone, close: float) -> str:
    if close > zone.upper_bound:
        return "ABOVE"
    if close < zone.lower_bound:
        return "BELOW"
    return "INSIDE"


def _direction(zone: LevelZone, close: float) -> str:
    side = _close_side(zone, close)
    if side == "ABOVE":
        return "FROM_ABOVE"
    if side == "BELOW":
        return "FROM_BELOW"
    return "INSIDE_ZONE"


def _distance_to_zone(zone: LevelZone, close: float) -> float:
    if close < zone.lower_bound:
        return zone.lower_bound - close
    if close > zone.upper_bound:
        return close - zone.upper_bound
    return 0.0


def _quality(touch_count: int, confirmed: bool, reaction_widths: float) -> float:
    score = 0.25 + min(touch_count, 3) * 0.15
    if confirmed:
        score += 0.2
    score += min(max(reaction_widths, 0.0), 1.0) * 0.1
    return round(min(score, 1.0), 6)


def classify_level_history(
    zone: LevelZone,
    bars: Iterable[Mapping[str, object]],
    config: EngineConfig | None = None,
) -> InteractionSnapshot:
    """Classify one level against closed bars only.

    The function is deterministic and causal: state after bar N is based only on
    bars <= N. A breakout requires two consecutive closes beyond the zone; a
    retest requires a later touch from the breakout side.
    """

    cfg = config or EngineConfig()
    validated = [_validate_bar(bar) for bar in bars]
    if not validated:
        return InteractionSnapshot(zone.level_id, "UNTOUCHED", "NONE", 0, None, 0.0, None)

    for index in range(1, len(validated)):
        if validated[index][0] <= validated[index - 1][0]:
            raise ValueError("bars must be strictly increasing by end timestamp")

    state = "UNTOUCHED"
    touch_count = 0
    direction = "NONE"
    breakout_side: str | None = None
    breakout_close_count = 0
    acceptance_close_count = 0
    reaction_widths = 0.0

    for ts, high, low, close in validated:
        side = _close_side(zone, close)
        touched = _touches(zone, high, low)
        distance = _distance_to_zone(zone, close)

        if state in {"UNTOUCHED", "AWAY", "RANGE_RETURN", "REJECTION", "ACCEPTANCE", "FALSE_BREAKOUT"}:
            if touched:
                direction = _direction(zone, close)
                touch_count += 1
                state = "TEST" if touch_count == 1 else "REPEATED_TEST"
            elif distance <= zone.width * cfg.approach_distance_widths:
                direction = _direction(zone, close)
                state = "APPROACH"
            else:
                state = "AWAY"
            continue

        if state == "APPROACH":
            if touched:
                direction = _direction(zone, close)
                touch_count += 1
                state = "TEST" if touch_count == 1 else "REPEATED_TEST"
            elif distance > zone.width * cfg.approach_distance_widths:
                state = "AWAY"
            continue

        if state in {"TEST", "REPEATED_TEST"}:
            if side in {"ABOVE", "BELOW"}:
                breakout_side = side
                breakout_close_count = 1
                state = "BREAKOUT_ATTEMPT"
            elif touched:
                touch_count += 1
                state = "REPEATED_TEST"
            elif distance >= zone.width * cfg.rejection_distance_widths:
                reaction_widths = distance / zone.width
                state = "REJECTION"
            continue

        if state == "BREAKOUT_ATTEMPT":
            if side == breakout_side:
                breakout_close_count += 1
                if breakout_close_count >= 2:
                    state = "BREAKOUT"
                    acceptance_close_count = 0
            elif side == "INSIDE":
                state = "REJECTION"
                breakout_side = None
                breakout_close_count = 0
            else:
                state = "REJECTION"
                breakout_side = None
                breakout_close_count = 0
            continue

        if state == "BREAKOUT":
            if breakout_side is None:
                raise RuntimeError("BREAKOUT without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            if side == opposite:
                state = "FALSE_BREAKOUT"
                continue
            if touched:
                touch_count += 1
                state = "RETEST"
                continue
            if side == breakout_side:
                away = _distance_to_zone(zone, close) / zone.width
                if away >= cfg.acceptance_distance_widths:
                    acceptance_close_count += 1
                else:
                    acceptance_close_count = 0
                if acceptance_close_count >= cfg.acceptance_close_count:
                    reaction_widths = away
                    state = "ACCEPTANCE"
                else:
                    state = "RETEST_PENDING"
            continue

        if state == "RETEST_PENDING":
            if breakout_side is None:
                raise RuntimeError("RETEST_PENDING without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            if side == opposite:
                state = "FALSE_BREAKOUT"
            elif touched:
                touch_count += 1
                state = "RETEST"
            elif side == breakout_side:
                away = _distance_to_zone(zone, close) / zone.width
                if away >= cfg.acceptance_distance_widths:
                    acceptance_close_count += 1
                    if acceptance_close_count >= cfg.acceptance_close_count:
                        reaction_widths = away
                        state = "ACCEPTANCE"
            continue

        if state == "RETEST":
            if breakout_side is None:
                raise RuntimeError("RETEST without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            if side == breakout_side:
                reaction_widths = _distance_to_zone(zone, close) / zone.width
                state = "RETEST_HOLD"
            elif side == opposite:
                state = "RETEST_FAIL"
            continue

        if state == "RETEST_HOLD":
            if breakout_side is None:
                raise RuntimeError("RETEST_HOLD without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            if side == opposite:
                state = "RETEST_FAIL"
            elif touched:
                touch_count += 1
                state = "RETEST"
            elif side == breakout_side:
                away = _distance_to_zone(zone, close) / zone.width
                if away >= cfg.acceptance_distance_widths:
                    acceptance_close_count += 1
                    if acceptance_close_count >= cfg.acceptance_close_count:
                        reaction_widths = away
                        state = "ACCEPTANCE"
            continue

        if state == "RETEST_FAIL":
            if breakout_side is None:
                raise RuntimeError("RETEST_FAIL without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            state = "FALSE_BREAKOUT" if side == opposite else "RANGE_RETURN"

    confirmed = state in {"BREAKOUT", "RETEST_HOLD", "ACCEPTANCE"}
    quality = _quality(touch_count, confirmed, reaction_widths)
    return InteractionSnapshot(
        level_id=zone.level_id,
        state=state,
        direction=direction,
        touch_count=touch_count,
        breakout_side=breakout_side,
        structural_quality=quality,
        as_of_timestamp=validated[-1][0].isoformat(),
    )
