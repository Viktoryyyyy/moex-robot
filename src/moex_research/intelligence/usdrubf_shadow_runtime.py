from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
from math import isclose, isfinite
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable, Mapping, Sequence

from .usdrubf_change_detector import ChangeDetectionResult, detect_market_state_changes
from .usdrubf_decision_engine import (
    DecisionEngineError,
    DecisionInput,
    DecisionMarketState,
    DirectionalContext,
    ResolvedLevelReference,
    build_market_state,
)
from .usdrubf_level_structure import InteractionSnapshot, LevelZone
from .usdrubf_news_macro import MacroObservation, MacroState, NewsEvent


_ALLOWED_FINAL_BIAS = {"BULLISH_USD", "NEUTRAL", "BEARISH_USD"}
_ALLOWED_TRADE_STATES = {"WAIT", "ENTER", "HOLD", "ADD", "REDUCE", "EXIT"}
_ALLOWED_PRICE_ANCHORS = {"LOWER_BOUND", "CENTER", "UPPER_BOUND"}
_PENDING_CONFIRMATION_STATES = {"BREAKOUT", "RETEST_PENDING", "RETEST"}


class ShadowRuntimeError(ValueError):
    """Raised when persisted shadow state is malformed or violates runtime boundaries."""


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ShadowRuntimeError(f"{field} must be a mapping")
    return value


def _sequence(value: object, field: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ShadowRuntimeError(f"{field} must be a sequence")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ShadowRuntimeError(f"{field} must be non-empty")
    return value.strip()


def _number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ShadowRuntimeError(f"{field} must be numeric")
    numeric = float(value)
    if not isfinite(numeric):
        raise ShadowRuntimeError(f"{field} must be finite")
    return numeric


def _level_reference_from_mapping(
    raw: object,
    *,
    field: str,
    levels_by_id: Mapping[str, LevelZone],
) -> ResolvedLevelReference:
    item = _mapping(raw, field)
    if set(item) != {"level_id", "price_anchor", "price"}:
        raise ShadowRuntimeError(f"{field} has unexpected fields")
    level_id = _text(item.get("level_id"), f"{field}.level_id")
    anchor = _text(item.get("price_anchor"), f"{field}.price_anchor")
    if anchor not in _ALLOWED_PRICE_ANCHORS:
        raise ShadowRuntimeError(f"invalid {field}.price_anchor")
    try:
        level = levels_by_id[level_id]
    except KeyError as exc:
        raise ShadowRuntimeError(f"{field} references unknown active level") from exc
    expected = {
        "LOWER_BOUND": level.lower_bound,
        "CENTER": level.center_price,
        "UPPER_BOUND": level.upper_bound,
    }[anchor]
    observed = _number(item.get("price"), f"{field}.price")
    if not isclose(observed, float(expected), rel_tol=0.0, abs_tol=1e-12):
        raise ShadowRuntimeError(f"{field}.price does not match deterministic level anchor")
    return ResolvedLevelReference(level_id=level_id, price_anchor=anchor, price=observed)


def _directional_context(raw: object, field: str) -> DirectionalContext:
    item = _mapping(raw, field)
    raw_details = item.get("details")
    details = None if raw_details is None else _mapping(raw_details, f"{field}.details")
    return DirectionalContext(
        source_id=_text(item.get("source_id"), f"{field}.source_id"),
        available_at=item.get("available_at"),
        direction=_text(item.get("direction"), f"{field}.direction"),
        confidence=_number(item.get("confidence"), f"{field}.confidence"),
        quality_status=_text(item.get("quality_status"), f"{field}.quality_status"),
        details=details,
    )


def _news_event(raw: object, index: int) -> NewsEvent:
    item = _mapping(raw, f"news_state[{index}]")
    entities = tuple(
        _text(value, f"news_state[{index}].entities")
        for value in _sequence(item.get("entities", ()), f"news_state[{index}].entities")
    )
    return NewsEvent(
        event_id=_text(item.get("event_id"), "event_id"),
        cluster_id=_text(item.get("cluster_id"), "cluster_id"),
        source_id=_text(item.get("source_id"), "source_id"),
        source_tier=_text(item.get("source_tier"), "source_tier"),
        source_reference=_text(item.get("source_reference"), "source_reference"),
        published_at=_text(item.get("published_at"), "published_at"),
        available_at=_text(item.get("available_at"), "available_at"),
        ingested_at=_text(item.get("ingested_at"), "ingested_at"),
        content_hash=_text(item.get("content_hash"), "content_hash"),
        event_type=_text(item.get("event_type"), "event_type"),
        entities=entities,
        rub_relevance=_number(item.get("rub_relevance"), "rub_relevance"),
        direction=_text(item.get("direction"), "direction"),
        importance=_text(item.get("importance"), "importance"),
        novelty=_text(item.get("novelty"), "novelty"),
        horizon=_text(item.get("horizon"), "horizon"),
        confidence=_number(item.get("confidence"), "confidence"),
        mechanism=_text(item.get("mechanism"), "mechanism"),
        quality_status=_text(item.get("quality_status"), "quality_status"),
    )


def _macro_state(raw: object) -> MacroState:
    item = _mapping(raw, "macro_state")
    observations: list[MacroObservation] = []
    for index, raw_observation in enumerate(
        _sequence(item.get("observations", ()), "macro_state.observations")
    ):
        observation = _mapping(raw_observation, f"macro_state.observations[{index}]")
        observations.append(
            MacroObservation(
                metric_id=_text(observation.get("metric_id"), "metric_id"),
                source_id=_text(observation.get("source_id"), "source_id"),
                source_reference=_text(observation.get("source_reference"), "source_reference"),
                value=(
                    None
                    if observation.get("value") is None
                    else _number(observation.get("value"), "value")
                ),
                unit=_text(observation.get("unit"), "unit"),
                observed_or_effective_at=observation.get("observed_or_effective_at"),
                published_at=observation.get("published_at"),
                available_at=observation.get("available_at"),
                ingested_at=observation.get("ingested_at"),
                quality_status=_text(observation.get("quality_status"), "quality_status"),
            )
        )
    dominant_drivers = tuple(
        _text(value, "macro_state.dominant_drivers")
        for value in _sequence(item.get("dominant_drivers", ()), "macro_state.dominant_drivers")
    )
    return MacroState(
        as_of_timestamp=_text(item.get("as_of_timestamp"), "macro_state.as_of_timestamp"),
        observations=tuple(observations),
        overall_direction=_text(item.get("overall_direction"), "macro_state.overall_direction"),
        confidence=_number(item.get("confidence"), "macro_state.confidence"),
        dominant_drivers=dominant_drivers,
    )


def _allowed_evidence_refs(inputs: DecisionInput) -> set[str]:
    refs = {f"level:{item.level_id}" for item in inputs.active_levels}
    refs.update(
        f"news:{item.event_id}"
        for item in inputs.news_events
        if item.quality_status == "OK"
    )
    refs.update(
        f"macro:{item.metric_id}"
        for item in inputs.macro_state.observations
        if item.quality_status == "OK"
    )
    if inputs.ema_3_19_ai.usable:
        refs.add("signal:ema_3_19_ai")
    if inputs.futoi.usable:
        refs.add("signal:futoi")
    return refs


def _validate_persisted_decision_fields(
    *,
    inputs: DecisionInput,
    trade_state: str,
    targets: tuple[ResolvedLevelReference, ...],
    invalidation: ResolvedLevelReference | None,
    evidence_refs: tuple[str, ...],
) -> None:
    if trade_state in {"ENTER", "ADD"} and not targets:
        raise ShadowRuntimeError(f"persisted {trade_state} requires target references")
    if trade_state in {"ENTER", "ADD", "HOLD"} and invalidation is None:
        raise ShadowRuntimeError(f"persisted {trade_state} requires invalidation")
    if not evidence_refs:
        raise ShadowRuntimeError("persisted evidence_refs must not be empty")
    if len(evidence_refs) != len(set(evidence_refs)):
        raise ShadowRuntimeError("persisted evidence_refs must be unique")
    if not set(evidence_refs).issubset(_allowed_evidence_refs(inputs)):
        raise ShadowRuntimeError("persisted evidence_refs reference unavailable facts")
    if trade_state in {"ENTER", "ADD"}:
        interactions = {item.level_id: item for item in inputs.level_interactions}
        structural_ids = tuple(
            value.removeprefix("level:")
            for value in evidence_refs
            if value.startswith("level:")
        )
        if not structural_ids:
            raise ShadowRuntimeError(
                f"persisted {trade_state} requires structural level evidence"
            )
        if all(
            interactions[level_id].state in _PENDING_CONFIRMATION_STATES
            for level_id in structural_ids
        ):
            raise ShadowRuntimeError(
                f"persisted {trade_state} relies only on unconfirmed breakout/retest"
            )


def market_state_from_dict(raw: object) -> DecisionMarketState:
    """Restore one persisted state and revalidate deterministic and bounded decision fields."""

    item = _mapping(raw, "market_state")
    required = {
        "instrument",
        "as_of_timestamp",
        "price",
        "trend",
        "market_regime",
        "active_levels",
        "level_interaction",
        "ema_3_19_ai",
        "futoi",
        "news_state",
        "macro_state",
        "final_bias",
        "trade_state",
        "confidence",
        "targets",
        "invalidation",
        "scenario",
        "reason",
        "evidence_refs",
    }
    if set(item) != required:
        raise ShadowRuntimeError("persisted MarketState field set mismatch")

    try:
        levels = tuple(
            LevelZone(**dict(_mapping(value, f"active_levels[{index}]")))
            for index, value in enumerate(_sequence(item["active_levels"], "active_levels"))
        )
        interactions = tuple(
            InteractionSnapshot(**dict(_mapping(value, f"level_interaction[{index}]")))
            for index, value in enumerate(
                _sequence(item["level_interaction"], "level_interaction")
            )
        )
        ema = _directional_context(item["ema_3_19_ai"], "ema_3_19_ai")
        futoi = _directional_context(item["futoi"], "futoi")
        news = tuple(
            _news_event(value, index)
            for index, value in enumerate(_sequence(item["news_state"], "news_state"))
        )
        macro = _macro_state(item["macro_state"])
        validated_inputs = DecisionInput(
            instrument=_text(item["instrument"], "instrument"),
            as_of_timestamp=_text(item["as_of_timestamp"], "as_of_timestamp"),
            price=_number(item["price"], "price"),
            trend=_text(item["trend"], "trend"),
            market_regime=_text(item["market_regime"], "market_regime"),
            active_levels=levels,
            level_interactions=interactions,
            ema_3_19_ai=ema,
            futoi=futoi,
            news_events=news,
            macro_state=macro,
        )
    except (DecisionEngineError, TypeError, ValueError) as exc:
        raise ShadowRuntimeError("persisted MarketState factual inputs are invalid") from exc

    levels_by_id = {level.level_id: level for level in levels}
    targets = tuple(
        _level_reference_from_mapping(
            value,
            field=f"targets[{index}]",
            levels_by_id=levels_by_id,
        )
        for index, value in enumerate(_sequence(item["targets"], "targets"))
    )
    invalidation = (
        None
        if item["invalidation"] is None
        else _level_reference_from_mapping(
            item["invalidation"],
            field="invalidation",
            levels_by_id=levels_by_id,
        )
    )
    final_bias = _text(item["final_bias"], "final_bias")
    trade_state = _text(item["trade_state"], "trade_state")
    confidence = _number(item["confidence"], "confidence")
    if final_bias not in _ALLOWED_FINAL_BIAS:
        raise ShadowRuntimeError("invalid persisted final_bias")
    if trade_state not in _ALLOWED_TRADE_STATES:
        raise ShadowRuntimeError("invalid persisted trade_state")
    if not 0.0 <= confidence <= 1.0:
        raise ShadowRuntimeError("invalid persisted confidence")
    evidence_refs = tuple(
        _text(value, "evidence_ref")
        for value in _sequence(item["evidence_refs"], "evidence_refs")
    )
    _validate_persisted_decision_fields(
        inputs=validated_inputs,
        trade_state=trade_state,
        targets=targets,
        invalidation=invalidation,
        evidence_refs=evidence_refs,
    )

    return DecisionMarketState(
        instrument=validated_inputs.instrument,
        as_of_timestamp=validated_inputs.as_of_timestamp.isoformat(),
        price=validated_inputs.price,
        trend=validated_inputs.trend,
        market_regime=validated_inputs.market_regime,
        active_levels=tuple(validated_inputs.active_levels),
        level_interaction=tuple(validated_inputs.level_interactions),
        ema_3_19_ai=validated_inputs.ema_3_19_ai,
        futoi=validated_inputs.futoi,
        news_state=tuple(validated_inputs.news_events),
        macro_state=validated_inputs.macro_state,
        final_bias=final_bias,
        trade_state=trade_state,
        confidence=confidence,
        targets=targets,
        invalidation=invalidation,
        scenario=_text(item["scenario"], "scenario"),
        reason=_text(item["reason"], "reason"),
        evidence_refs=evidence_refs,
    )


class ShadowJsonStore:
    """Restart-safe snapshots under an explicit caller root."""

    def __init__(
        self,
        root: Path | str,
        *,
        market_state_filename: str = "market_state.json",
        change_filename: str = "change_detection.json",
    ) -> None:
        self.root = Path(root)
        self.market_state_filename = self._basename(market_state_filename)
        self.change_filename = self._basename(change_filename)
        if self.market_state_filename == self.change_filename:
            raise ShadowRuntimeError("shadow snapshot filenames must be distinct")

    @staticmethod
    def _basename(value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ShadowRuntimeError("snapshot filename must be a plain basename")
        path = Path(value)
        if path.name != value or value in {".", ".."}:
            raise ShadowRuntimeError("snapshot filename must be a plain basename")
        return value

    def _path(self, filename: str) -> Path:
        return self.root / filename

    def _write_atomic(self, filename: str, payload: object) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        target = self._path(filename)
        serialized = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        temp_path: Path | None = None
        try:
            with NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.root,
                prefix=f".{filename}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                handle.write(serialized)
                handle.flush()
                temp_path = Path(handle.name)
            temp_path.replace(target)
        except Exception:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
            raise
        return target

    def load_market_state(self) -> DecisionMarketState | None:
        path = self._path(self.market_state_filename)
        if not path.exists():
            return None
        if path.is_symlink() or not path.is_file():
            raise ShadowRuntimeError("market state snapshot is not a regular non-symlink file")
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ShadowRuntimeError("market state snapshot is unreadable or invalid JSON") from exc
        return market_state_from_dict(raw)

    def save_market_state(self, state: DecisionMarketState) -> Path:
        return self._write_atomic(self.market_state_filename, state.to_dict())

    def save_change_detection(self, result: ChangeDetectionResult | None) -> Path:
        payload = None if result is None else result.to_dict()
        return self._write_atomic(self.change_filename, payload)


@dataclass(frozen=True)
class ShadowCycleResult:
    market_state: DecisionMarketState
    change_detection: ChangeDetectionResult | None
    significant_change: bool
    action_candidate: bool
    market_state_path: Path
    change_detection_path: Path


class ShadowRuntime:
    def __init__(self, store: ShadowJsonStore) -> None:
        self._store = store

    def run_cycle(
        self,
        inputs: DecisionInput,
        *,
        decision_agent: Callable[[Mapping[str, object]], Mapping[str, object]],
    ) -> ShadowCycleResult:
        previous = self._store.load_market_state()
        current = build_market_state(inputs, decision_agent=decision_agent)
        changes = None if previous is None else detect_market_state_changes(previous, current)

        # The MarketState snapshot is the restart authority and is committed last.
        # If the process fails between these writes, restart still reads the prior state.
        change_detection_path = self._store.save_change_detection(changes)
        market_state_path = self._store.save_market_state(current)
        return ShadowCycleResult(
            market_state=current,
            change_detection=changes,
            significant_change=False if changes is None else changes.significant_change,
            action_candidate=False if changes is None else changes.action_alert,
            market_state_path=market_state_path,
            change_detection_path=change_detection_path,
        )
