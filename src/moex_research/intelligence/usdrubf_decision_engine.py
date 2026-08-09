from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Callable, Mapping, Sequence

from .usdrubf_level_structure import InteractionSnapshot, LevelZone
from .usdrubf_news_macro import MacroState, NewsEvent


_ALLOWED_DIRECTIONS = {"BULLISH_USD", "NEUTRAL", "BEARISH_USD", "MIXED"}
_ALLOWED_FINAL_BIAS = {"BULLISH_USD", "NEUTRAL", "BEARISH_USD"}
_ALLOWED_TRADE_STATES = {"WAIT", "ENTER", "HOLD", "ADD", "REDUCE", "EXIT"}
_ALLOWED_SIGNAL_QUALITY = {"OK", "MISSING", "STALE", "BLOCKED"}
_ALLOWED_PRICE_ANCHORS = {"LOWER_BOUND", "CENTER", "UPPER_BOUND"}
_DECISION_OUTPUT_FIELDS = {
    "final_bias",
    "trade_state",
    "confidence",
    "target_references",
    "invalidation_reference",
    "scenario",
    "reason",
    "evidence_refs",
}


class DecisionEngineError(ValueError):
    """Raised when decision inputs or bounded agent output violate the contract."""


def _dt(value: datetime | str, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise DecisionEngineError(f"{field} must be ISO datetime") from exc
    if not isinstance(value, datetime):
        raise DecisionEngineError(f"{field} must be datetime or ISO datetime string")
    if value.tzinfo is None or value.utcoffset() is None:
        raise DecisionEngineError(f"{field} must be timezone-aware")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DecisionEngineError(f"{field} must be non-empty")
    return value.strip()


def _probability(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DecisionEngineError(f"{field} must be numeric")
    numeric = float(value)
    if not 0.0 <= numeric <= 1.0:
        raise DecisionEngineError(f"{field} must be within 0..1")
    return numeric


def _jsonable(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


@dataclass(frozen=True)
class DirectionalContext:
    source_id: str
    available_at: datetime | str
    direction: str
    confidence: float
    quality_status: str = "OK"
    details: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _text(self.source_id, "source_id"))
        available = _dt(self.available_at, "available_at")
        object.__setattr__(self, "available_at", available)
        if self.direction not in _ALLOWED_DIRECTIONS:
            raise DecisionEngineError("invalid directional context direction")
        object.__setattr__(self, "confidence", _probability(self.confidence, "confidence"))
        if self.quality_status not in _ALLOWED_SIGNAL_QUALITY:
            raise DecisionEngineError("invalid directional context quality_status")
        if self.details is not None and not isinstance(self.details, Mapping):
            raise DecisionEngineError("details must be a mapping or None")

    @property
    def usable(self) -> bool:
        return self.quality_status == "OK"


def ema_context_from_target_position(
    target_position: int | None,
    *,
    available_at: datetime | str,
    confidence: float = 1.0,
    details: Mapping[str, object] | None = None,
) -> DirectionalContext:
    if target_position not in {-1, 0, 1, None}:
        raise DecisionEngineError("EMA target_position must be one of -1, 0, 1 or None")
    direction = {
        1: "BULLISH_USD",
        -1: "BEARISH_USD",
        0: "NEUTRAL",
        None: "NEUTRAL",
    }[target_position]
    return DirectionalContext(
        source_id="ema_3_19_ai",
        available_at=available_at,
        direction=direction,
        confidence=confidence,
        quality_status="OK",
        details=details,
    )


@dataclass(frozen=True)
class DecisionInput:
    as_of_timestamp: datetime | str
    price: float
    trend: str
    market_regime: str
    active_levels: Sequence[LevelZone]
    level_interactions: Sequence[InteractionSnapshot]
    ema_3_19_ai: DirectionalContext
    futoi: DirectionalContext
    news_events: Sequence[NewsEvent]
    macro_state: MacroState
    instrument: str = "USDRUBF"

    def __post_init__(self) -> None:
        if self.instrument != "USDRUBF":
            raise DecisionEngineError("instrument must be USDRUBF")
        as_of = _dt(self.as_of_timestamp, "as_of_timestamp")
        object.__setattr__(self, "as_of_timestamp", as_of)
        if isinstance(self.price, bool) or not isinstance(self.price, (int, float)):
            raise DecisionEngineError("price must be numeric")
        if float(self.price) <= 0:
            raise DecisionEngineError("price must be positive")
        object.__setattr__(self, "price", float(self.price))
        if self.trend not in _ALLOWED_FINAL_BIAS:
            raise DecisionEngineError("invalid trend")
        object.__setattr__(self, "market_regime", _text(self.market_regime, "market_regime"))

        levels = tuple(self.active_levels)
        interactions = tuple(self.level_interactions)
        news = tuple(self.news_events)
        object.__setattr__(self, "active_levels", levels)
        object.__setattr__(self, "level_interactions", interactions)
        object.__setattr__(self, "news_events", news)

        level_ids = [level.level_id for level in levels]
        if len(level_ids) != len(set(level_ids)):
            raise DecisionEngineError("active level_id values must be unique")
        if any(level.status != "ACTIVE" for level in levels):
            raise DecisionEngineError("active_levels may contain only ACTIVE levels")
        for level in levels:
            if _dt(level.created_at, f"level {level.level_id} created_at") > as_of:
                raise DecisionEngineError("active level created after decision as_of_timestamp")

        interaction_ids = [item.level_id for item in interactions]
        if len(interaction_ids) != len(set(interaction_ids)):
            raise DecisionEngineError("level interaction records must be unique by level_id")
        if set(interaction_ids) != set(level_ids):
            raise DecisionEngineError(
                "level_interactions must contain exactly one record for every active level_id"
            )
        for item in interactions:
            if item.event_timestamp is not None and _dt(
                item.event_timestamp, f"interaction {item.level_id} event_timestamp"
            ) > as_of:
                raise DecisionEngineError("level interaction event is in the future")
            if item.as_of_timestamp is not None and _dt(
                item.as_of_timestamp, f"interaction {item.level_id} as_of_timestamp"
            ) > as_of:
                raise DecisionEngineError("level interaction snapshot is in the future")

        for signal, label in (
            (self.ema_3_19_ai, "ema_3_19_ai"),
            (self.futoi, "futoi"),
        ):
            if signal.available_at > as_of:
                raise DecisionEngineError(f"{label} context is not yet available")

        for event in news:
            if _dt(event.available_at, f"news {event.event_id} available_at") > as_of:
                raise DecisionEngineError("news event is not yet available")

        macro_as_of = _dt(self.macro_state.as_of_timestamp, "macro_state.as_of_timestamp")
        if macro_as_of > as_of:
            raise DecisionEngineError("macro_state is from the future")
        for observation in self.macro_state.observations:
            if observation.available_at > as_of:
                raise DecisionEngineError("macro observation is not yet available")


@dataclass(frozen=True)
class ResolvedLevelReference:
    level_id: str
    price_anchor: str
    price: float


@dataclass(frozen=True)
class DecisionMarketState:
    instrument: str
    as_of_timestamp: str
    price: float
    trend: str
    market_regime: str
    active_levels: tuple[LevelZone, ...]
    level_interaction: tuple[InteractionSnapshot, ...]
    ema_3_19_ai: DirectionalContext
    futoi: DirectionalContext
    news_state: tuple[NewsEvent, ...]
    macro_state: MacroState
    final_bias: str
    trade_state: str
    confidence: float
    targets: tuple[ResolvedLevelReference, ...]
    invalidation: ResolvedLevelReference | None
    scenario: str
    reason: str
    evidence_refs: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return _jsonable(asdict(self))  # type: ignore[return-value]


def _level_payload(level: LevelZone) -> dict[str, object]:
    return {
        "level_id": level.level_id,
        "level_type": level.level_type,
        "center_price": level.center_price,
        "lower_bound": level.lower_bound,
        "upper_bound": level.upper_bound,
        "created_at": level.created_at,
        "source_timeframe": level.source_timeframe,
        "status": level.status,
    }


def _interaction_payload(item: InteractionSnapshot) -> dict[str, object]:
    return {
        "level_id": item.level_id,
        "state": item.state,
        "direction": item.direction,
        "event_timestamp": item.event_timestamp,
        "previous_state": item.previous_state,
        "structural_quality": item.structural_quality,
        "touch_count": item.touch_count,
        "breakout_side": item.breakout_side,
        "as_of_timestamp": item.as_of_timestamp,
    }


def _signal_payload(signal: DirectionalContext) -> dict[str, object]:
    return {
        "source_id": signal.source_id,
        "available_at": signal.available_at.isoformat(),
        "direction": signal.direction,
        "confidence": signal.confidence,
        "quality_status": signal.quality_status,
        "usable": signal.usable,
        "details": _jsonable(signal.details or {}),
    }


def _news_payload(event: NewsEvent) -> dict[str, object]:
    return _jsonable(asdict(event))  # type: ignore[return-value]


def _macro_payload(state: MacroState) -> dict[str, object]:
    return _jsonable(asdict(state))  # type: ignore[return-value]


def _allowed_evidence_refs(inputs: DecisionInput) -> set[str]:
    refs = {f"level:{level.level_id}" for level in inputs.active_levels}
    refs.update(
        f"news:{event.event_id}"
        for event in inputs.news_events
        if event.quality_status == "OK"
    )
    refs.update(
        f"macro:{observation.metric_id}"
        for observation in inputs.macro_state.observations
        if observation.quality_status == "OK"
    )
    if inputs.ema_3_19_ai.usable:
        refs.add("signal:ema_3_19_ai")
    if inputs.futoi.usable:
        refs.add("signal:futoi")
    return refs


def build_decision_payload(inputs: DecisionInput) -> dict[str, object]:
    return {
        "instrument": inputs.instrument,
        "as_of_timestamp": inputs.as_of_timestamp.isoformat(),
        "market_facts": {
            "price": inputs.price,
            "trend": inputs.trend,
            "market_regime": inputs.market_regime,
            "active_levels": tuple(_level_payload(level) for level in inputs.active_levels),
            "level_interactions": tuple(
                _interaction_payload(item) for item in inputs.level_interactions
            ),
        },
        "ema_3_19_ai": _signal_payload(inputs.ema_3_19_ai),
        "futoi": _signal_payload(inputs.futoi),
        "news_state": {
            "events": tuple(_news_payload(event) for event in inputs.news_events),
        },
        "macro_state": _macro_payload(inputs.macro_state),
        "output_contract": {
            "final_bias_allowed": tuple(sorted(_ALLOWED_FINAL_BIAS)),
            "trade_state_allowed": tuple(sorted(_ALLOWED_TRADE_STATES)),
            "price_anchor_allowed": tuple(sorted(_ALLOWED_PRICE_ANCHORS)),
            "numeric_level_creation_forbidden": True,
            "targets_and_invalidation_must_reference_active_level_id": True,
            "technical_level_events_are_read_only": True,
            "allowed_evidence_refs": tuple(sorted(_allowed_evidence_refs(inputs))),
        },
    }


def _parse_reference(
    value: object,
    *,
    field: str,
    levels_by_id: Mapping[str, LevelZone],
) -> ResolvedLevelReference:
    if not isinstance(value, Mapping):
        raise DecisionEngineError(f"{field} must be a mapping")
    if set(value) != {"level_id", "price_anchor"}:
        raise DecisionEngineError(f"{field} may contain only level_id and price_anchor")
    level_id = _text(value.get("level_id"), f"{field}.level_id")
    price_anchor = _text(value.get("price_anchor"), f"{field}.price_anchor")
    if price_anchor not in _ALLOWED_PRICE_ANCHORS:
        raise DecisionEngineError(f"invalid {field}.price_anchor")
    try:
        level = levels_by_id[level_id]
    except KeyError as exc:
        raise DecisionEngineError(f"{field} references unknown active level_id") from exc
    price = {
        "LOWER_BOUND": level.lower_bound,
        "CENTER": level.center_price,
        "UPPER_BOUND": level.upper_bound,
    }[price_anchor]
    return ResolvedLevelReference(
        level_id=level_id,
        price_anchor=price_anchor,
        price=float(price),
    )


def _validate_agent_output(
    output: Mapping[str, object],
    *,
    inputs: DecisionInput,
) -> tuple[
    str,
    str,
    float,
    tuple[ResolvedLevelReference, ...],
    ResolvedLevelReference | None,
    str,
    str,
    tuple[str, ...],
]:
    if not isinstance(output, Mapping):
        raise DecisionEngineError("decision agent output must be a mapping")
    extra = set(output) - _DECISION_OUTPUT_FIELDS
    missing = _DECISION_OUTPUT_FIELDS - set(output)
    if extra:
        raise DecisionEngineError(
            f"decision agent may not output factual or undeclared fields: {sorted(extra)}"
        )
    if missing:
        raise DecisionEngineError(f"decision agent missing fields: {sorted(missing)}")

    final_bias = _text(output["final_bias"], "final_bias")
    if final_bias not in _ALLOWED_FINAL_BIAS:
        raise DecisionEngineError("invalid final_bias")
    trade_state = _text(output["trade_state"], "trade_state")
    if trade_state not in _ALLOWED_TRADE_STATES:
        raise DecisionEngineError("invalid trade_state")
    confidence = _probability(output["confidence"], "confidence")

    levels_by_id = {level.level_id: level for level in inputs.active_levels}
    targets_raw = output["target_references"]
    if not isinstance(targets_raw, Sequence) or isinstance(targets_raw, (str, bytes)):
        raise DecisionEngineError("target_references must be a sequence")
    targets = tuple(
        _parse_reference(item, field=f"target_references[{index}]", levels_by_id=levels_by_id)
        for index, item in enumerate(targets_raw)
    )
    target_keys = [(item.level_id, item.price_anchor) for item in targets]
    if len(target_keys) != len(set(target_keys)):
        raise DecisionEngineError("target_references must be unique")

    invalidation_raw = output["invalidation_reference"]
    invalidation = (
        None
        if invalidation_raw is None
        else _parse_reference(
            invalidation_raw,
            field="invalidation_reference",
            levels_by_id=levels_by_id,
        )
    )

    if trade_state in {"ENTER", "ADD"} and not targets:
        raise DecisionEngineError(f"{trade_state} requires at least one target reference")
    if trade_state in {"ENTER", "ADD", "HOLD"} and invalidation is None:
        raise DecisionEngineError(f"{trade_state} requires an invalidation reference")

    scenario = _text(output["scenario"], "scenario")
    reason = _text(output["reason"], "reason")
    evidence_raw = output["evidence_refs"]
    if not isinstance(evidence_raw, Sequence) or isinstance(evidence_raw, (str, bytes)):
        raise DecisionEngineError("evidence_refs must be a sequence")
    evidence_refs = tuple(_text(item, "evidence_ref") for item in evidence_raw)
    if not evidence_refs:
        raise DecisionEngineError("evidence_refs must not be empty")
    if len(evidence_refs) != len(set(evidence_refs)):
        raise DecisionEngineError("evidence_refs must be unique")
    allowed_refs = _allowed_evidence_refs(inputs)
    if not set(evidence_refs).issubset(allowed_refs):
        raise DecisionEngineError("evidence_refs must reference supplied usable facts")

    return (
        final_bias,
        trade_state,
        confidence,
        targets,
        invalidation,
        scenario,
        reason,
        evidence_refs,
    )


def build_market_state(
    inputs: DecisionInput,
    *,
    decision_agent: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> DecisionMarketState:
    """Build one bounded MarketState from deterministic/source-bound inputs.

    The decision agent may interpret supplied facts and select existing level
    references, but it cannot create numeric levels, mutate technical events, or
    emit undeclared factual fields.
    """

    payload = build_decision_payload(inputs)
    output = decision_agent(payload)
    (
        final_bias,
        trade_state,
        confidence,
        targets,
        invalidation,
        scenario,
        reason,
        evidence_refs,
    ) = _validate_agent_output(output, inputs=inputs)

    return DecisionMarketState(
        instrument=inputs.instrument,
        as_of_timestamp=inputs.as_of_timestamp.isoformat(),
        price=inputs.price,
        trend=inputs.trend,
        market_regime=inputs.market_regime,
        active_levels=tuple(inputs.active_levels),
        level_interaction=tuple(inputs.level_interactions),
        ema_3_19_ai=inputs.ema_3_19_ai,
        futoi=inputs.futoi,
        news_state=tuple(inputs.news_events),
        macro_state=inputs.macro_state,
        final_bias=final_bias,
        trade_state=trade_state,
        confidence=confidence,
        targets=targets,
        invalidation=invalidation,
        scenario=scenario,
        reason=reason,
        evidence_refs=evidence_refs,
    )
