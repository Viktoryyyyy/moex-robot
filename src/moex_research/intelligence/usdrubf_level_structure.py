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

_TRANSITION_MAP = {
    "UNTOUCHED": {"APPROACH"},
    "APPROACH": {"TEST", "AWAY"},
    "TEST": {"REPEATED_TEST", "REJECTION", "BREAKOUT_ATTEMPT"},
    "REPEATED_TEST": {"REJECTION", "BREAKOUT_ATTEMPT"},
    "BREAKOUT_ATTEMPT": {"BREAKOUT", "REJECTION"},
    "BREAKOUT": {"RETEST_PENDING", "ACCEPTANCE", "FALSE_BREAKOUT"},
    "RETEST_PENDING": {"RETEST", "ACCEPTANCE", "FALSE_BREAKOUT"},
    "RETEST": {"RETEST_HOLD", "RETEST_FAIL"},
    "RETEST_HOLD": {"ACCEPTANCE", "RETEST"},
    "RETEST_FAIL": {"FALSE_BREAKOUT", "RANGE_RETURN"},
    "REJECTION": {"APPROACH", "RANGE_RETURN", "AWAY"},
    "ACCEPTANCE": {"APPROACH", "AWAY"},
    "FALSE_BREAKOUT": {"RANGE_RETURN", "AWAY"},
    "RANGE_RETURN": {"APPROACH", "AWAY"},
    "AWAY": {"APPROACH"},
}

_ALLOWED_LEVEL_TYPES = {
    "SUPPORT",
    "RESISTANCE",
    "RANGE_BOUNDARY",
    "PREVIOUS_SESSION_HIGH",
    "PREVIOUS_SESSION_LOW",
    "CURRENT_SESSION_HIGH",
    "CURRENT_SESSION_LOW",
    "BREAKOUT_ORIGIN",
    "VOLUME_LIQUIDITY_ZONE",
    "EMA_CONTEXT_ZONE",
}
_ALLOWED_LEVEL_STATUS = {"ACTIVE", "BROKEN", "INVALIDATED", "RETIRED"}


def _parse_datetime(value: str, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty ISO datetime string")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO datetime string") from exc


@dataclass(frozen=True)
class LevelZone:
    level_id: str
    level_type: str
    center_price: float
    lower_bound: float
    upper_bound: float
    created_at: str
    source_timeframe: str
    status: str

    def __post_init__(self) -> None:
        if not self.level_id.strip():
            raise ValueError("level_id must be non-empty")
        if self.level_type not in _ALLOWED_LEVEL_TYPES:
            raise ValueError("invalid level_type")
        if not (0 < self.lower_bound <= self.center_price <= self.upper_bound):
            raise ValueError("invalid level zone bounds")
        if self.upper_bound == self.lower_bound:
            raise ValueError("level zone must have non-zero width")
        _parse_datetime(self.created_at, "created_at")
        if not self.source_timeframe.strip():
            raise ValueError("source_timeframe must be non-empty")
        if self.status not in _ALLOWED_LEVEL_STATUS:
            raise ValueError("invalid level status")

    @property
    def width(self) -> float:
        return self.upper_bound - self.lower_bound


@dataclass(frozen=True)
class InteractionSnapshot:
    level_id: str
    state: str
    direction: str
    event_timestamp: str | None
    previous_state: str | None
    structural_quality: float
    touch_count: int
    breakout_side: str | None
    as_of_timestamp: str | None

    def __post_init__(self) -> None:
        if not self.level_id.strip():
            raise ValueError("level_id must be non-empty")
        if self.state not in _ALLOWED_STATES:
            raise ValueError("invalid interaction state")
        if self.direction not in {"FROM_BELOW", "FROM_ABOVE", "INSIDE_ZONE", "NONE"}:
            raise ValueError("invalid interaction direction")
        if self.previous_state is not None and self.previous_state not in _ALLOWED_STATES:
            raise ValueError("invalid previous_state")
        if self.event_timestamp is not None:
            _parse_datetime(self.event_timestamp, "event_timestamp")
        if self.as_of_timestamp is not None:
            _parse_datetime(self.as_of_timestamp, "as_of_timestamp")
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
        try:
            return datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError("bar.end must be datetime or ISO datetime string") from exc
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


def _direction_from_side(side: str) -> str:
    if side == "ABOVE":
        return "FROM_ABOVE"
    if side == "BELOW":
        return "FROM_BELOW"
    return "INSIDE_ZONE"


def _expected_breakout_side(zone: LevelZone, direction: str) -> str | None:
    if direction == "FROM_BELOW":
        return "ABOVE"
    if direction == "FROM_ABOVE":
        return "BELOW"
    if zone.level_type in {"RESISTANCE", "PREVIOUS_SESSION_HIGH", "CURRENT_SESSION_HIGH"}:
        return "ABOVE"
    if zone.level_type in {"SUPPORT", "PREVIOUS_SESSION_LOW", "CURRENT_SESSION_LOW"}:
        return "BELOW"
    return None


def _distance_to_zone(zone: LevelZone, close: float) -> float:
    if close < zone.lower_bound:
        return zone.lower_bound - close
    if close > zone.upper_bound:
        return close - zone.upper_bound
    return 0.0


def _bounded_optional(value: float | None, name: str) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    numeric = float(value)
    if not 0.0 <= numeric <= 1.0:
        raise ValueError(f"{name} out of range")
    return numeric


def _quality(
    touch_count: int,
    confirmed: bool,
    reaction_widths: float,
    dwell_bars: int,
    close_confirmation_count: int,
    volume_confirmation: float | None,
    volatility_context: float | None,
) -> float:
    score = 0.15
    score += min(touch_count, 3) * 0.12
    score += min(dwell_bars, 3) * 0.05
    score += min(close_confirmation_count, 3) * 0.08
    score += min(max(reaction_widths, 0.0), 1.0) * 0.10
    score += _bounded_optional(volume_confirmation, "volume_confirmation") * 0.05
    score += _bounded_optional(volatility_context, "volatility_context") * 0.05
    if confirmed:
        score += 0.10
    return round(min(score, 1.0), 6)


def classify_level_history(
    zone: LevelZone,
    bars: Iterable[Mapping[str, object]],
    config: EngineConfig | None = None,
    *,
    volume_confirmation: float | None = None,
    volatility_context: float | None = None,
) -> InteractionSnapshot:
    """Classify one level against closed bars using the frozen Stage 2 transition map."""

    cfg = config or EngineConfig()
    _bounded_optional(volume_confirmation, "volume_confirmation")
    _bounded_optional(volatility_context, "volatility_context")
    validated = [_validate_bar(bar) for bar in bars]
    if not validated:
        return InteractionSnapshot(
            level_id=zone.level_id,
            state="UNTOUCHED",
            direction="NONE",
            event_timestamp=None,
            previous_state=None,
            structural_quality=0.0,
            touch_count=0,
            breakout_side=None,
            as_of_timestamp=None,
        )

    for index in range(1, len(validated)):
        if validated[index][0] <= validated[index - 1][0]:
            raise ValueError("bars must be strictly increasing by end timestamp")

    created_at = _parse_datetime(zone.created_at, "created_at")
    try:
        if created_at > validated[-1][0]:
            raise ValueError("level created_at is after as_of_timestamp")
    except TypeError as exc:
        raise ValueError("level created_at and bar.end must use compatible timezone semantics") from exc

    state = "UNTOUCHED"
    previous_state: str | None = None
    event_timestamp: str | None = None
    touch_count = 0
    direction = "NONE"
    approach_side: str | None = None
    breakout_side: str | None = None
    breakout_close_count = 0
    acceptance_close_count = 0
    reaction_widths = 0.0
    dwell_bars = 0
    close_confirmation_count = 0
    pending_retest_touch = False

    def transition(new_state: str, ts: datetime) -> None:
        nonlocal state, previous_state, event_timestamp
        if new_state == state:
            return
        if new_state not in _TRANSITION_MAP[state]:
            raise RuntimeError(f"invalid contracted transition: {state}->{new_state}")
        previous_state = state
        state = new_state
        event_timestamp = ts.isoformat()

    for ts, high, low, close in validated:
        side = _close_side(zone, close)
        touched = _touches(zone, high, low)
        distance = _distance_to_zone(zone, close)
        near = distance <= zone.width * cfg.approach_distance_widths
        if touched or near:
            dwell_bars += 1

        if state == "UNTOUCHED":
            if touched or near:
                if side in {"ABOVE", "BELOW"}:
                    approach_side = side
                direction = _direction_from_side(approach_side or side)
                if touched:
                    touch_count += 1
                transition("APPROACH", ts)

        elif state == "APPROACH":
            if touched:
                if approach_side in {"ABOVE", "BELOW"}:
                    direction = _direction_from_side(approach_side)
                elif side in {"ABOVE", "BELOW"}:
                    direction = _direction_from_side(side)
                touch_count += 1
                transition("TEST", ts)
            elif not near:
                if side in {"ABOVE", "BELOW"}:
                    approach_side = side
                    direction = _direction_from_side(side)
                transition("AWAY", ts)

        elif state in {"TEST", "REPEATED_TEST"}:
            expected_breakout_side = _expected_breakout_side(zone, direction)
            valid_breakout_side = (
                side in {"ABOVE", "BELOW"}
                and (expected_breakout_side is None or side == expected_breakout_side)
            )
            rejection_side = (
                side in {"ABOVE", "BELOW"}
                and expected_breakout_side is not None
                and side != expected_breakout_side
            )
            if valid_breakout_side and not touched:
                breakout_side = side
                breakout_close_count = 1
                close_confirmation_count = max(close_confirmation_count, 1)
                transition("BREAKOUT_ATTEMPT", ts)
            elif touched:
                touch_count += 1
                if state == "TEST":
                    transition("REPEATED_TEST", ts)
            elif rejection_side and distance >= zone.width * cfg.rejection_distance_widths:
                reaction_widths = distance / zone.width
                breakout_side = None
                breakout_close_count = 0
                transition("REJECTION", ts)

        elif state == "BREAKOUT_ATTEMPT":
            if side == breakout_side and not touched:
                breakout_close_count += 1
                close_confirmation_count = max(close_confirmation_count, breakout_close_count)
                if breakout_close_count >= 2:
                    acceptance_close_count = 0
                    transition("BREAKOUT", ts)
            else:
                breakout_side = None
                breakout_close_count = 0
                acceptance_close_count = 0
                transition("REJECTION", ts)

        elif state == "BREAKOUT":
            if breakout_side is None:
                raise RuntimeError("BREAKOUT without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            if side == opposite and not touched:
                pending_retest_touch = False
                acceptance_close_count = 0
                transition("FALSE_BREAKOUT", ts)
            elif touched:
                touch_count += 1
                pending_retest_touch = True
                acceptance_close_count = 0
                transition("RETEST_PENDING", ts)
            elif side == breakout_side:
                away = distance / zone.width
                if away >= cfg.acceptance_distance_widths:
                    acceptance_close_count += 1
                else:
                    acceptance_close_count = 0
                if acceptance_close_count >= cfg.acceptance_close_count:
                    reaction_widths = away
                    close_confirmation_count = max(close_confirmation_count, acceptance_close_count)
                    transition("ACCEPTANCE", ts)
                else:
                    transition("RETEST_PENDING", ts)

        elif state == "RETEST_PENDING":
            if breakout_side is None:
                raise RuntimeError("RETEST_PENDING without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            if side == opposite and not touched:
                pending_retest_touch = False
                acceptance_close_count = 0
                transition("FALSE_BREAKOUT", ts)
            elif pending_retest_touch:
                if touched:
                    touch_count += 1
                pending_retest_touch = False
                acceptance_close_count = 0
                transition("RETEST", ts)
            elif touched:
                touch_count += 1
                acceptance_close_count = 0
                transition("RETEST", ts)
            elif side == breakout_side:
                away = distance / zone.width
                if away >= cfg.acceptance_distance_widths:
                    acceptance_close_count += 1
                    if acceptance_close_count >= cfg.acceptance_close_count:
                        reaction_widths = away
                        close_confirmation_count = max(close_confirmation_count, acceptance_close_count)
                        transition("ACCEPTANCE", ts)
                else:
                    acceptance_close_count = 0

        elif state == "RETEST":
            if breakout_side is None:
                raise RuntimeError("RETEST without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            if side == opposite and not touched:
                acceptance_close_count = 0
                transition("RETEST_FAIL", ts)
            elif side == breakout_side and not touched:
                reaction_widths = distance / zone.width
                acceptance_close_count = 0
                transition("RETEST_HOLD", ts)
            elif touched:
                touch_count += 1

        elif state == "RETEST_HOLD":
            if breakout_side is None:
                raise RuntimeError("RETEST_HOLD without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            if touched or side == opposite:
                if touched:
                    touch_count += 1
                acceptance_close_count = 0
                transition("RETEST", ts)
            elif side == breakout_side:
                away = distance / zone.width
                if away >= cfg.acceptance_distance_widths:
                    acceptance_close_count += 1
                    if acceptance_close_count >= cfg.acceptance_close_count:
                        reaction_widths = away
                        close_confirmation_count = max(close_confirmation_count, acceptance_close_count)
                        transition("ACCEPTANCE", ts)
                else:
                    acceptance_close_count = 0

        elif state == "RETEST_FAIL":
            if breakout_side is None:
                raise RuntimeError("RETEST_FAIL without breakout_side")
            opposite = "BELOW" if breakout_side == "ABOVE" else "ABOVE"
            if side == opposite and not touched:
                transition("FALSE_BREAKOUT", ts)
            else:
                transition("RANGE_RETURN", ts)

        elif state == "REJECTION":
            breakout_side = None
            breakout_close_count = 0
            acceptance_close_count = 0
            if touched:
                touch_count += 1
                transition("RANGE_RETURN", ts)
            elif near:
                if side in {"ABOVE", "BELOW"}:
                    approach_side = side
                    direction = _direction_from_side(side)
                transition("APPROACH", ts)
            else:
                transition("AWAY", ts)

        elif state == "ACCEPTANCE":
            acceptance_close_count = 0
            if touched or near:
                if side in {"ABOVE", "BELOW"}:
                    approach_side = side
                    direction = _direction_from_side(side)
                breakout_side = None
                transition("APPROACH", ts)
            else:
                transition("AWAY", ts)

        elif state == "FALSE_BREAKOUT":
            acceptance_close_count = 0
            if touched or side == "INSIDE" or near:
                transition("RANGE_RETURN", ts)
            else:
                transition("AWAY", ts)

        elif state == "RANGE_RETURN":
            breakout_side = None
            if touched or near:
                if side in {"ABOVE", "BELOW"}:
                    approach_side = side
                    direction = _direction_from_side(side)
                transition("APPROACH", ts)
            else:
                transition("AWAY", ts)

        elif state == "AWAY":
            breakout_side = None
            breakout_close_count = 0
            acceptance_close_count = 0
            if touched or near:
                if side in {"ABOVE", "BELOW"}:
                    approach_side = side
                    direction = _direction_from_side(side)
                if touched:
                    touch_count += 1
                transition("APPROACH", ts)

    confirmed = state in {"BREAKOUT", "RETEST_HOLD", "ACCEPTANCE"}
    quality = _quality(
        touch_count=touch_count,
        confirmed=confirmed,
        reaction_widths=reaction_widths,
        dwell_bars=dwell_bars,
        close_confirmation_count=close_confirmation_count,
        volume_confirmation=volume_confirmation,
        volatility_context=volatility_context,
    )
    return InteractionSnapshot(
        level_id=zone.level_id,
        state=state,
        direction=direction,
        event_timestamp=event_timestamp,
        previous_state=previous_state,
        structural_quality=quality,
        touch_count=touch_count,
        breakout_side=breakout_side,
        as_of_timestamp=validated[-1][0].isoformat(),
    )
