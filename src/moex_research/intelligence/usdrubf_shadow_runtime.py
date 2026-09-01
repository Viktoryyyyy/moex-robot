from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
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
from .usdrubf_news_macro import (
    MacroObservation,
    MacroState,
    NewsEvent,
    NewsSourceProvenance,
)


_ALLOWED_FINAL_BIAS = {"BULLISH_USD", "NEUTRAL", "BEARISH_USD"}
_ALLOWED_TRADE_STATES = {"WAIT", "ENTER", "HOLD", "ADD", "REDUCE", "EXIT"}
_ALLOWED_PRICE_ANCHORS = {"LOWER_BOUND", "CENTER", "UPPER_BOUND"}
_PENDING_CONFIRMATION_STATES = {"BREAKOUT", "RETEST_PENDING", "RETEST"}
_ALLOWED_NEWS_TIERS = {
    "OFFICIAL_PRIMARY",
    "OFFICIAL_SECONDARY",
    "MAJOR_AGENCY_OR_FINANCIAL_MEDIA",
}
_ALLOWED_NEWS_DIRECTIONS = {"USD_BULLISH", "USD_BEARISH", "NEUTRAL", "MIXED"}
_ALLOWED_NEWS_IMPORTANCE = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
_ALLOWED_NEWS_NOVELTY = {"NEW", "UPDATE", "REPEAT", "STALE"}
_ALLOWED_NEWS_HORIZONS = {"INTRADAY", "SHORT_TERM", "MEDIUM_TERM", "LONG_TERM"}
_ALLOWED_NEWS_QUALITY = {
    "OK",
    "SOURCE_UNAVAILABLE",
    "TIMESTAMP_UNPROVABLE",
    "DUPLICATE",
    "CLASSIFICATION_FAILED",
    "STALE",
}
_ALLOWED_MACRO_DIRECTIONS = _ALLOWED_NEWS_DIRECTIONS
_NEWS_PROVENANCE_FIELDS = {
    "source_id",
    "source_tier",
    "source_reference",
    "published_at",
    "available_at",
    "ingested_at",
    "content_hash",
}
_MAX_NEWS_SOURCE_PROVENANCE = 16
_POINTER_VERSION = 1


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


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ShadowRuntimeError(f"{field} must be a non-negative integer")
    return value


def _probability(value: object, field: str) -> float:
    numeric = _number(value, field)
    if not 0.0 <= numeric <= 1.0:
        raise ShadowRuntimeError(f"{field} must be within 0..1")
    return numeric


def _aware_datetime(value: object, field: str) -> datetime:
    raw = _text(value, field)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ShadowRuntimeError(f"{field} must be ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowRuntimeError(f"{field} must be timezone-aware")
    return parsed


def _plain_json_basename(value: object, field: str) -> str:
    name = _text(value, field)
    path = Path(name)
    if path.name != name or name in {".", ".."} or path.suffix != ".json":
        raise ShadowRuntimeError(f"{field} must be a plain .json basename")
    return name


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
        confidence=_probability(item.get("confidence"), f"{field}.confidence"),
        quality_status=_text(item.get("quality_status"), f"{field}.quality_status"),
        details=details,
    )


def _news_source_provenance(raw: object, field: str) -> NewsSourceProvenance:
    item = _mapping(raw, field)
    if set(item) != _NEWS_PROVENANCE_FIELDS:
        raise ShadowRuntimeError(f"{field} has unexpected fields")
    source_tier = _text(item.get("source_tier"), f"{field}.source_tier")
    if source_tier not in _ALLOWED_NEWS_TIERS:
        raise ShadowRuntimeError(f"invalid {field}.source_tier")
    published = _aware_datetime(item.get("published_at"), f"{field}.published_at")
    available = _aware_datetime(item.get("available_at"), f"{field}.available_at")
    ingested = _aware_datetime(item.get("ingested_at"), f"{field}.ingested_at")
    if published > available or available > ingested:
        raise ShadowRuntimeError(f"invalid {field} timestamp ordering")
    content_hash = _text(item.get("content_hash"), f"{field}.content_hash")
    if len(content_hash) != 64 or any(ch not in "0123456789abcdef" for ch in content_hash):
        raise ShadowRuntimeError(f"invalid {field}.content_hash")
    return NewsSourceProvenance(
        source_id=_text(item.get("source_id"), f"{field}.source_id"),
        source_tier=source_tier,
        source_reference=_text(item.get("source_reference"), f"{field}.source_reference"),
        published_at=published.isoformat(),
        available_at=available.isoformat(),
        ingested_at=ingested.isoformat(),
        content_hash=content_hash,
    )


def _news_event(raw: object, index: int) -> NewsEvent:
    field = f"news_state[{index}]"
    item = _mapping(raw, field)
    source_tier = _text(item.get("source_tier"), f"{field}.source_tier")
    direction = _text(item.get("direction"), f"{field}.direction")
    importance = _text(item.get("importance"), f"{field}.importance")
    novelty = _text(item.get("novelty"), f"{field}.novelty")
    horizon = _text(item.get("horizon"), f"{field}.horizon")
    quality = _text(item.get("quality_status"), f"{field}.quality_status")
    if source_tier not in _ALLOWED_NEWS_TIERS:
        raise ShadowRuntimeError(f"invalid {field}.source_tier")
    if direction not in _ALLOWED_NEWS_DIRECTIONS:
        raise ShadowRuntimeError(f"invalid {field}.direction")
    if importance not in _ALLOWED_NEWS_IMPORTANCE:
        raise ShadowRuntimeError(f"invalid {field}.importance")
    if novelty not in _ALLOWED_NEWS_NOVELTY:
        raise ShadowRuntimeError(f"invalid {field}.novelty")
    if horizon not in _ALLOWED_NEWS_HORIZONS:
        raise ShadowRuntimeError(f"invalid {field}.horizon")
    if quality not in _ALLOWED_NEWS_QUALITY:
        raise ShadowRuntimeError(f"invalid {field}.quality_status")
    published = _aware_datetime(item.get("published_at"), f"{field}.published_at")
    available = _aware_datetime(item.get("available_at"), f"{field}.available_at")
    ingested = _aware_datetime(item.get("ingested_at"), f"{field}.ingested_at")
    if published > available or available > ingested:
        raise ShadowRuntimeError(f"invalid {field} timestamp ordering")
    content_hash = _text(item.get("content_hash"), f"{field}.content_hash")
    if len(content_hash) != 64 or any(ch not in "0123456789abcdef" for ch in content_hash):
        raise ShadowRuntimeError(f"invalid {field}.content_hash")
    entities = tuple(
        _text(value, f"{field}.entities")
        for value in _sequence(item.get("entities", ()), f"{field}.entities")
    )

    provenance_keys = {
        "source_provenance",
        "source_provenance_total_count",
        "source_provenance_truncated",
    }
    present_provenance_keys = provenance_keys.intersection(item)
    if present_provenance_keys and present_provenance_keys != provenance_keys:
        raise ShadowRuntimeError(f"{field} provenance metadata field set is incomplete")
    if present_provenance_keys:
        source_provenance = tuple(
            _news_source_provenance(value, f"{field}.source_provenance[{prov_index}]")
            for prov_index, value in enumerate(
                _sequence(item.get("source_provenance"), f"{field}.source_provenance")
            )
        )
        if len(source_provenance) > _MAX_NEWS_SOURCE_PROVENANCE:
            raise ShadowRuntimeError(f"{field}.source_provenance exceeds persisted bound")
        source_provenance_total_count = _nonnegative_int(
            item.get("source_provenance_total_count"),
            f"{field}.source_provenance_total_count",
        )
        source_provenance_truncated = item.get("source_provenance_truncated")
        if not isinstance(source_provenance_truncated, bool):
            raise ShadowRuntimeError(f"{field}.source_provenance_truncated must be boolean")
        if source_provenance_total_count < len(source_provenance):
            raise ShadowRuntimeError(f"{field}.source_provenance_total_count is inconsistent")
        if source_provenance_truncated != (
            source_provenance_total_count > len(source_provenance)
        ):
            raise ShadowRuntimeError(f"{field}.source_provenance_truncated is inconsistent")
    else:
        source_provenance = ()
        source_provenance_total_count = 0
        source_provenance_truncated = False

    return NewsEvent(
        event_id=_text(item.get("event_id"), f"{field}.event_id"),
        cluster_id=_text(item.get("cluster_id"), f"{field}.cluster_id"),
        source_id=_text(item.get("source_id"), f"{field}.source_id"),
        source_tier=source_tier,
        source_reference=_text(item.get("source_reference"), f"{field}.source_reference"),
        published_at=published.isoformat(),
        available_at=available.isoformat(),
        ingested_at=ingested.isoformat(),
        content_hash=content_hash,
        event_type=_text(item.get("event_type"), f"{field}.event_type"),
        entities=entities,
        rub_relevance=_probability(item.get("rub_relevance"), f"{field}.rub_relevance"),
        direction=direction,
        importance=importance,
        novelty=novelty,
        horizon=horizon,
        confidence=_probability(item.get("confidence"), f"{field}.confidence"),
        mechanism=_text(item.get("mechanism"), f"{field}.mechanism"),
        quality_status=quality,
        source_provenance=source_provenance,
        source_provenance_total_count=source_provenance_total_count,
        source_provenance_truncated=source_provenance_truncated,
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
    overall_direction = _text(item.get("overall_direction"), "macro_state.overall_direction")
    if overall_direction not in _ALLOWED_MACRO_DIRECTIONS:
        raise ShadowRuntimeError("invalid macro_state.overall_direction")
    dominant_drivers = tuple(
        _text(value, "macro_state.dominant_drivers")
        for value in _sequence(item.get("dominant_drivers", ()), "macro_state.dominant_drivers")
    )
    usable_metrics = {obs.metric_id for obs in observations if obs.quality_status == "OK"}
    if not set(dominant_drivers).issubset(usable_metrics):
        raise ShadowRuntimeError("macro_state.dominant_drivers reference unavailable metrics")
    return MacroState(
        as_of_timestamp=_aware_datetime(
            item.get("as_of_timestamp"), "macro_state.as_of_timestamp"
        ).isoformat(),
        observations=tuple(observations),
        overall_direction=overall_direction,
        confidence=_probability(item.get("confidence"), "macro_state.confidence"),
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
    target_keys = tuple((item.level_id, item.price_anchor) for item in targets)
    if len(target_keys) != len(set(target_keys)):
        raise ShadowRuntimeError("persisted target references must be unique")
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
    """Restore a persisted state and revalidate factual and decision invariants."""

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
    except (DecisionEngineError, ShadowRuntimeError, TypeError, ValueError) as exc:
        if isinstance(exc, ShadowRuntimeError):
            raise
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
    confidence = _probability(item["confidence"], "confidence")
    if final_bias not in _ALLOWED_FINAL_BIAS:
        raise ShadowRuntimeError("invalid persisted final_bias")
    if trade_state not in _ALLOWED_TRADE_STATES:
        raise ShadowRuntimeError("invalid persisted trade_state")
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


@dataclass(frozen=True)
class _CyclePointer:
    current_as_of_timestamp: str
    market_state_file: str
    change_detection_file: str


class ShadowJsonStore:
    """Restart-safe generation snapshots committed by one atomic pointer update."""

    def __init__(
        self,
        root: Path | str,
        *,
        market_state_filename: str = "market_state.json",
        change_filename: str = "change_detection.json",
        pointer_filename: str = "current_cycle.json",
    ) -> None:
        self.root = Path(root)
        self.market_state_filename = _plain_json_basename(
            market_state_filename, "market_state_filename"
        )
        self.change_filename = _plain_json_basename(change_filename, "change_filename")
        self.pointer_filename = _plain_json_basename(pointer_filename, "pointer_filename")
        if len({self.market_state_filename, self.change_filename, self.pointer_filename}) != 3:
            raise ShadowRuntimeError("shadow snapshot basenames must be distinct")

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

    def _read_json_file(self, filename: str, field: str) -> object:
        safe_name = _plain_json_basename(filename, field)
        path = self._path(safe_name)
        if path.is_symlink() or not path.is_file():
            raise ShadowRuntimeError(f"{field} is not a regular non-symlink file")
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ShadowRuntimeError(f"{field} is unreadable or invalid JSON") from exc

    @staticmethod
    def _generation_filename(base: str, cycle_id: str) -> str:
        path = Path(base)
        return f"{path.stem}.{cycle_id}{path.suffix}"

    @staticmethod
    def _cycle_id(as_of_timestamp: str) -> str:
        return sha256(as_of_timestamp.encode("utf-8")).hexdigest()[:20]

    def _load_pointer(self) -> _CyclePointer | None:
        path = self._path(self.pointer_filename)
        if not path.exists():
            return None
        raw = _mapping(
            self._read_json_file(self.pointer_filename, "current cycle pointer"),
            "current cycle pointer",
        )
        required = {
            "version",
            "current_as_of_timestamp",
            "market_state_file",
            "change_detection_file",
        }
        if set(raw) != required or raw.get("version") != _POINTER_VERSION:
            raise ShadowRuntimeError("current cycle pointer field set or version mismatch")
        current_as_of = _aware_datetime(
            raw.get("current_as_of_timestamp"), "current cycle pointer timestamp"
        ).isoformat()
        market_file = _plain_json_basename(
            raw.get("market_state_file"), "current cycle market_state_file"
        )
        change_file = _plain_json_basename(
            raw.get("change_detection_file"), "current cycle change_detection_file"
        )
        if self.pointer_filename in {market_file, change_file} or market_file == change_file:
            raise ShadowRuntimeError("current cycle pointer references invalid snapshot files")
        return _CyclePointer(
            current_as_of_timestamp=current_as_of,
            market_state_file=market_file,
            change_detection_file=change_file,
        )

    def load_market_state(self) -> DecisionMarketState | None:
        pointer = self._load_pointer()
        if pointer is None:
            return None
        raw = self._read_json_file(pointer.market_state_file, "market state generation")
        state = market_state_from_dict(raw)
        if state.as_of_timestamp != pointer.current_as_of_timestamp:
            raise ShadowRuntimeError("market state generation timestamp does not match pointer")
        return state

    def load_change_detection_raw(self) -> object | None:
        pointer = self._load_pointer()
        if pointer is None:
            return None
        return self._read_json_file(
            pointer.change_detection_file, "change detection generation"
        )

    def commit_cycle(
        self,
        state: DecisionMarketState,
        change_detection: ChangeDetectionResult | None,
    ) -> tuple[Path, Path]:
        cycle_id = self._cycle_id(state.as_of_timestamp)
        state_name = self._generation_filename(self.market_state_filename, cycle_id)
        change_name = self._generation_filename(self.change_filename, cycle_id)

        state_path = self._write_atomic(state_name, state.to_dict())
        change_path = self._write_atomic(
            change_name,
            None if change_detection is None else change_detection.to_dict(),
        )

        # Commit point: both immutable generation files exist before pointer replacement.
        self._write_atomic(
            self.pointer_filename,
            {
                "version": _POINTER_VERSION,
                "current_as_of_timestamp": state.as_of_timestamp,
                "market_state_file": state_name,
                "change_detection_file": change_name,
            },
        )
        return state_path, change_path


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
        market_state_path, change_detection_path = self._store.commit_cycle(current, changes)
        return ShadowCycleResult(
            market_state=current,
            change_detection=changes,
            significant_change=False if changes is None else changes.significant_change,
            action_candidate=False if changes is None else changes.action_alert,
            market_state_path=market_state_path,
            change_detection_path=change_detection_path,
        )
