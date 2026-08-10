from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Iterable, Mapping

from .usdrubf_decision_engine import DecisionMarketState, ResolvedLevelReference


_CHANGE_TYPES = {
    "MARKET_STRUCTURE_CHANGED",
    "NEWS_STRUCTURE_CHANGED",
    "POSITIONING_CHANGED",
    "MODEL_SIGNAL_CHANGED",
    "RISK_CHANGED",
}
_SEVERITIES = {"INFO", "IMPORTANT", "ACTION"}
_SEVERITY_RANK = {"INFO": 1, "IMPORTANT": 2, "ACTION": 3}
_ACTIVE_TRADE_STATES = {"ENTER", "HOLD", "ADD", "REDUCE"}
_ACTION_TRADE_STATES = {"ENTER", "ADD", "REDUCE", "EXIT"}
_PENDING_BREAKOUT_STATES = {"BREAKOUT", "RETEST_PENDING", "RETEST"}
_RETEST_FAILURE_STATES = {"RETEST_FAIL", "FALSE_BREAKOUT", "RANGE_RETURN"}


class ChangeDetectorError(ValueError):
    """Raised when two MarketState snapshots cannot be compared safely."""


def _dt(value: str, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ChangeDetectorError(f"{field} must be a non-empty ISO datetime string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ChangeDetectorError(f"{field} must be ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ChangeDetectorError(f"{field} must be timezone-aware")
    return parsed


def _jsonable(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


@dataclass(frozen=True)
class ChangeEvent:
    event_type: str
    severity: str
    code: str
    reason: str
    previous_value: object | None = None
    current_value: object | None = None
    level_id: str | None = None
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.event_type not in _CHANGE_TYPES:
            raise ChangeDetectorError("invalid change event_type")
        if self.severity not in _SEVERITIES:
            raise ChangeDetectorError("invalid change severity")
        if not isinstance(self.code, str) or not self.code.strip():
            raise ChangeDetectorError("change code must be non-empty")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ChangeDetectorError("change reason must be non-empty")
        if self.level_id is not None and not self.level_id.strip():
            raise ChangeDetectorError("level_id must be non-empty when supplied")
        if len(self.evidence_refs) != len(set(self.evidence_refs)):
            raise ChangeDetectorError("evidence_refs must be unique")


@dataclass(frozen=True)
class ChangeDetectionResult:
    instrument: str
    previous_as_of_timestamp: str
    current_as_of_timestamp: str
    events: tuple[ChangeEvent, ...]
    highest_severity: str | None
    significant_change: bool
    action_alert: bool

    def to_dict(self) -> dict[str, object]:
        return _jsonable(asdict(self))  # type: ignore[return-value]


def _event(
    event_type: str,
    severity: str,
    code: str,
    reason: str,
    *,
    previous_value: object | None = None,
    current_value: object | None = None,
    level_id: str | None = None,
    evidence_refs: Iterable[str] = (),
) -> ChangeEvent:
    return ChangeEvent(
        event_type=event_type,
        severity=severity,
        code=code,
        reason=reason,
        previous_value=previous_value,
        current_value=current_value,
        level_id=level_id,
        evidence_refs=tuple(evidence_refs),
    )


def _reference_key(value: ResolvedLevelReference | None) -> tuple[str, str, float] | None:
    if value is None:
        return None
    return (value.level_id, value.price_anchor, float(value.price))


def _target_keys(state: DecisionMarketState) -> tuple[tuple[str, str, float], ...]:
    return tuple(_reference_key(item) for item in state.targets if item is not None)  # type: ignore[misc]


def _opposite_bias(left: str, right: str) -> bool:
    return {left, right} == {"BULLISH_USD", "BEARISH_USD"}


def _append_decision_changes(
    events: list[ChangeEvent],
    previous: DecisionMarketState,
    current: DecisionMarketState,
) -> None:
    if previous.trade_state != current.trade_state:
        severity = "ACTION" if current.trade_state in _ACTION_TRADE_STATES else "IMPORTANT"
        events.append(
            _event(
                "RISK_CHANGED",
                severity,
                "TRADE_STATE_CHANGED",
                "Trade-state recommendation changed.",
                previous_value=previous.trade_state,
                current_value=current.trade_state,
                evidence_refs=current.evidence_refs,
            )
        )

    if previous.final_bias != current.final_bias:
        severity = "ACTION" if _opposite_bias(previous.final_bias, current.final_bias) else "IMPORTANT"
        events.append(
            _event(
                "RISK_CHANGED",
                severity,
                "FINAL_BIAS_CHANGED",
                "Directional decision bias changed.",
                previous_value=previous.final_bias,
                current_value=current.final_bias,
                evidence_refs=current.evidence_refs,
            )
        )

    previous_invalidation = _reference_key(previous.invalidation)
    current_invalidation = _reference_key(current.invalidation)
    if previous_invalidation != current_invalidation:
        severity = "ACTION" if current.trade_state in _ACTIVE_TRADE_STATES else "IMPORTANT"
        events.append(
            _event(
                "RISK_CHANGED",
                severity,
                "INVALIDATION_CHANGED",
                "Deterministic invalidation reference changed.",
                previous_value=previous_invalidation,
                current_value=current_invalidation,
                evidence_refs=current.evidence_refs,
            )
        )

    previous_targets = _target_keys(previous)
    current_targets = _target_keys(current)
    if previous_targets != current_targets:
        events.append(
            _event(
                "RISK_CHANGED",
                "IMPORTANT",
                "TARGETS_CHANGED",
                "Deterministic target references changed.",
                previous_value=previous_targets,
                current_value=current_targets,
                evidence_refs=current.evidence_refs,
            )
        )

    confidence_delta = current.confidence - previous.confidence
    if abs(confidence_delta) >= 0.15:
        events.append(
            _event(
                "RISK_CHANGED",
                "IMPORTANT",
                "CONFIDENCE_CHANGED_MATERIALLY",
                "Decision confidence changed by at least 0.15.",
                previous_value=previous.confidence,
                current_value=current.confidence,
                evidence_refs=current.evidence_refs,
            )
        )


def _append_market_structure_changes(
    events: list[ChangeEvent],
    previous: DecisionMarketState,
    current: DecisionMarketState,
) -> None:
    if previous.market_regime != current.market_regime:
        events.append(
            _event(
                "MARKET_STRUCTURE_CHANGED",
                "IMPORTANT",
                "MARKET_REGIME_CHANGED",
                "Market regime changed.",
                previous_value=previous.market_regime,
                current_value=current.market_regime,
            )
        )

    previous_levels = {item.level_id: item for item in previous.active_levels}
    current_levels = {item.level_id: item for item in current.active_levels}
    for level_id in sorted(previous_levels.keys() - current_levels.keys()):
        level = previous_levels[level_id]
        severity = "IMPORTANT" if level.level_type in {"SUPPORT", "RESISTANCE"} else "INFO"
        events.append(
            _event(
                "MARKET_STRUCTURE_CHANGED",
                severity,
                "LEVEL_LEFT_ACTIVE_SET",
                "A previously active level is no longer present in the active level set; no break cause is inferred.",
                previous_value=level.level_type,
                current_value=None,
                level_id=level_id,
                evidence_refs=(f"level:{level_id}",),
            )
        )
    for level_id in sorted(current_levels.keys() - previous_levels.keys()):
        level = current_levels[level_id]
        events.append(
            _event(
                "MARKET_STRUCTURE_CHANGED",
                "INFO",
                "LEVEL_ENTERED_ACTIVE_SET",
                "A new active level entered MarketState.",
                previous_value=None,
                current_value=level.level_type,
                level_id=level_id,
                evidence_refs=(f"level:{level_id}",),
            )
        )

    previous_interactions = {item.level_id: item for item in previous.level_interaction}
    current_interactions = {item.level_id: item for item in current.level_interaction}
    for level_id in sorted(previous_interactions.keys() & current_interactions.keys()):
        before = previous_interactions[level_id]
        after = current_interactions[level_id]
        if before.state == after.state:
            continue

        if before.state in _PENDING_BREAKOUT_STATES and after.state == "RETEST_HOLD":
            severity = "ACTION"
            code = "RETEST_HOLD_CONFIRMED"
            reason = "Previously unconfirmed breakout/retest structure advanced to RETEST_HOLD."
        elif before.state == "RETEST_HOLD" and after.state in _RETEST_FAILURE_STATES:
            severity = "ACTION"
            code = "RETEST_HOLD_FAILED"
            reason = "A previously held retest failed or returned to the prior range."
        elif after.state in {"FALSE_BREAKOUT", "RETEST_FAIL"}:
            severity = "ACTION" if current.trade_state in _ACTIVE_TRADE_STATES else "IMPORTANT"
            code = "STRUCTURE_FAILURE_CONFIRMED"
            reason = "The current level interaction confirms failed breakout/retest structure."
        elif after.state in {"BREAKOUT", "RETEST_PENDING", "RETEST"}:
            severity = "IMPORTANT"
            code = "STRUCTURE_CONFIRMATION_PENDING"
            reason = "Level interaction changed into a breakout/retest state that still requires confirmation."
        elif after.state in {"ACCEPTANCE", "REJECTION"}:
            severity = "IMPORTANT"
            code = "STRUCTURE_CONFIRMATION_CHANGED"
            reason = "Level interaction reached a confirmed acceptance or rejection state."
        else:
            severity = "INFO"
            code = "LEVEL_INTERACTION_CHANGED"
            reason = "Level interaction state changed."

        events.append(
            _event(
                "MARKET_STRUCTURE_CHANGED",
                severity,
                code,
                reason,
                previous_value=before.state,
                current_value=after.state,
                level_id=level_id,
                evidence_refs=(f"level:{level_id}",),
            )
        )


def _append_signal_changes(
    events: list[ChangeEvent],
    previous: DecisionMarketState,
    current: DecisionMarketState,
) -> None:
    if (
        previous.ema_3_19_ai.direction != current.ema_3_19_ai.direction
        or previous.ema_3_19_ai.quality_status != current.ema_3_19_ai.quality_status
    ):
        severity = (
            "IMPORTANT"
            if previous.ema_3_19_ai.direction != current.ema_3_19_ai.direction
            else "INFO"
        )
        events.append(
            _event(
                "MODEL_SIGNAL_CHANGED",
                severity,
                "EMA_3_19_AI_CHANGED",
                "EMA 3/19 AI directional or quality context changed.",
                previous_value=(
                    previous.ema_3_19_ai.direction,
                    previous.ema_3_19_ai.quality_status,
                ),
                current_value=(
                    current.ema_3_19_ai.direction,
                    current.ema_3_19_ai.quality_status,
                ),
                evidence_refs=("signal:ema_3_19_ai",) if current.ema_3_19_ai.usable else (),
            )
        )

    if (
        previous.futoi.direction != current.futoi.direction
        or previous.futoi.quality_status != current.futoi.quality_status
    ):
        severity = "IMPORTANT" if previous.futoi.direction != current.futoi.direction else "INFO"
        events.append(
            _event(
                "POSITIONING_CHANGED",
                severity,
                "FUTOI_CHANGED",
                "FUTOI participant-positioning directional or quality context changed.",
                previous_value=(previous.futoi.direction, previous.futoi.quality_status),
                current_value=(current.futoi.direction, current.futoi.quality_status),
                evidence_refs=("signal:futoi",) if current.futoi.usable else (),
            )
        )


def _append_news_macro_changes(
    events: list[ChangeEvent],
    previous: DecisionMarketState,
    current: DecisionMarketState,
) -> None:
    previous_event_ids = {item.event_id for item in previous.news_state}
    for item in current.news_state:
        if item.event_id in previous_event_ids or item.quality_status != "OK":
            continue
        if item.importance == "CRITICAL" and item.rub_relevance >= 0.7 and item.confidence >= 0.6:
            severity = "ACTION"
        elif item.importance in {"HIGH", "CRITICAL"}:
            severity = "IMPORTANT"
        else:
            severity = "INFO"
        events.append(
            _event(
                "NEWS_STRUCTURE_CHANGED",
                severity,
                "NEW_NEWS_EVENT",
                "A new usable NewsEvent entered MarketState.",
                previous_value=None,
                current_value=(item.event_type, item.direction, item.importance, item.novelty),
                evidence_refs=(f"news:{item.event_id}",),
            )
        )

    if previous.macro_state.overall_direction != current.macro_state.overall_direction:
        events.append(
            _event(
                "RISK_CHANGED",
                "IMPORTANT",
                "MACRO_DIRECTION_CHANGED",
                "MacroState overall directional interpretation changed.",
                previous_value=previous.macro_state.overall_direction,
                current_value=current.macro_state.overall_direction,
                evidence_refs=tuple(
                    f"macro:{item.metric_id}"
                    for item in current.macro_state.observations
                    if item.quality_status == "OK"
                ),
            )
        )


def _sort_events(events: Iterable[ChangeEvent]) -> tuple[ChangeEvent, ...]:
    return tuple(
        sorted(
            events,
            key=lambda item: (
                -_SEVERITY_RANK[item.severity],
                item.event_type,
                item.code,
                item.level_id or "",
            ),
        )
    )


def detect_market_state_changes(
    previous: DecisionMarketState,
    current: DecisionMarketState,
) -> ChangeDetectionResult:
    """Compare two ordered MarketState snapshots without creating new market facts."""

    if previous.instrument != current.instrument:
        raise ChangeDetectorError("MarketState instruments must match")
    previous_ts = _dt(previous.as_of_timestamp, "previous.as_of_timestamp")
    current_ts = _dt(current.as_of_timestamp, "current.as_of_timestamp")
    if current_ts <= previous_ts:
        raise ChangeDetectorError("current MarketState must be later than previous MarketState")

    events: list[ChangeEvent] = []
    _append_market_structure_changes(events, previous, current)
    _append_signal_changes(events, previous, current)
    _append_news_macro_changes(events, previous, current)
    _append_decision_changes(events, previous, current)

    ordered = _sort_events(events)
    highest = ordered[0].severity if ordered else None
    return ChangeDetectionResult(
        instrument=current.instrument,
        previous_as_of_timestamp=previous.as_of_timestamp,
        current_as_of_timestamp=current.as_of_timestamp,
        events=ordered,
        highest_severity=highest,
        significant_change=any(item.severity in {"IMPORTANT", "ACTION"} for item in ordered),
        action_alert=any(item.severity == "ACTION" for item in ordered),
    )
