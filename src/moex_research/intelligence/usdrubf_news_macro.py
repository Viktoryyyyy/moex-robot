from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import re
from typing import Callable, Iterable, Mapping, Sequence


_ALLOWED_SOURCE_TIERS = {
    "OFFICIAL_PRIMARY",
    "OFFICIAL_SECONDARY",
    "MAJOR_AGENCY_OR_FINANCIAL_MEDIA",
}
_ALLOWED_DIRECTIONS = {"USD_BULLISH", "USD_BEARISH", "NEUTRAL", "MIXED"}
_ALLOWED_IMPORTANCE = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
_ALLOWED_NOVELTY = {"NEW", "UPDATE", "REPEAT", "STALE"}
_ALLOWED_HORIZONS = {"INTRADAY", "SHORT_TERM", "MEDIUM_TERM", "LONG_TERM"}
_ALLOWED_NEWS_QUALITY = {
    "OK",
    "SOURCE_UNAVAILABLE",
    "TIMESTAMP_UNPROVABLE",
    "DUPLICATE",
    "CLASSIFICATION_FAILED",
    "STALE",
}
_ALLOWED_MACRO_QUALITY = {
    "OK",
    "MISSING",
    "LATE_PUBLICATION",
    "SOURCE_UNAVAILABLE",
    "STALE",
    "BLOCKED_BY_SOURCE_POLICY",
}
_CLASSIFIER_OUTPUT_FIELDS = {
    "event_type",
    "entities",
    "rub_relevance",
    "direction",
    "importance",
    "novelty",
    "horizon",
    "confidence",
    "mechanism",
}
_MACRO_INTERPRETER_OUTPUT_FIELDS = {
    "overall_direction",
    "confidence",
    "dominant_drivers",
}
_TOKEN_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё]+")


class ClassifierOutputError(ValueError):
    """Raised when interpretive output violates the bounded contract."""


def _dt(value: datetime | str, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field} must be ISO datetime") from exc
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be datetime or ISO datetime string")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


def _required_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be non-empty")
    return value.strip()


def normalize_text(value: str) -> str:
    value = _required_text(value, "text")
    return " ".join(value.casefold().split())


def _tokens(value: str) -> frozenset[str]:
    return frozenset(token.casefold() for token in _TOKEN_RE.findall(value) if len(token) > 1)


def _similarity(left: str, right: str) -> float:
    a = _tokens(left)
    b = _tokens(right)
    if not a or not b:
        return 1.0 if a == b else 0.0
    return len(a & b) / len(a | b)


def _stable_id(prefix: str, *parts: str) -> str:
    digest = sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


@dataclass(frozen=True)
class NewsSourceRecord:
    source_id: str
    source_tier: str
    source_reference: str
    published_at: datetime | str
    available_at: datetime | str
    ingested_at: datetime | str
    headline: str
    body: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _required_text(self.source_id, "source_id"))
        object.__setattr__(self, "source_reference", _required_text(self.source_reference, "source_reference"))
        object.__setattr__(self, "headline", _required_text(self.headline, "headline"))
        if self.source_tier not in _ALLOWED_SOURCE_TIERS:
            raise ValueError("invalid source_tier")
        if not isinstance(self.body, str):
            raise ValueError("body must be a string")
        published = _dt(self.published_at, "published_at")
        available = _dt(self.available_at, "available_at")
        ingested = _dt(self.ingested_at, "ingested_at")
        if published > available:
            raise ValueError("published_at must be <= available_at")
        if available > ingested:
            raise ValueError("available_at must be <= ingested_at")
        object.__setattr__(self, "published_at", published)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "ingested_at", ingested)

    @property
    def normalized_text(self) -> str:
        return normalize_text(f"{self.headline} {self.body}".strip())

    @property
    def content_hash(self) -> str:
        return sha256(self.normalized_text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class NewsEvent:
    event_id: str
    cluster_id: str
    source_id: str
    source_tier: str
    source_reference: str
    published_at: str
    available_at: str
    ingested_at: str
    content_hash: str
    event_type: str
    entities: tuple[str, ...]
    rub_relevance: float
    direction: str
    importance: str
    novelty: str
    horizon: str
    confidence: float
    mechanism: str
    quality_status: str = "OK"

    def __post_init__(self) -> None:
        if self.quality_status not in _ALLOWED_NEWS_QUALITY:
            raise ValueError("invalid news quality_status")


@dataclass(frozen=True)
class NewsPipelineResult:
    events: tuple[NewsEvent, ...]
    exact_duplicates_removed: int
    future_records_filtered: int
    clusters_classified: int


@dataclass(frozen=True)
class MacroObservation:
    metric_id: str
    source_id: str
    source_reference: str
    value: float | None
    unit: str
    observed_or_effective_at: datetime | str
    published_at: datetime | str
    available_at: datetime | str
    ingested_at: datetime | str
    quality_status: str = "OK"

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric_id", _required_text(self.metric_id, "metric_id"))
        object.__setattr__(self, "source_id", _required_text(self.source_id, "source_id"))
        object.__setattr__(self, "source_reference", _required_text(self.source_reference, "source_reference"))
        object.__setattr__(self, "unit", _required_text(self.unit, "unit"))
        if self.quality_status not in _ALLOWED_MACRO_QUALITY:
            raise ValueError("invalid macro quality_status")
        if self.value is not None and (isinstance(self.value, bool) or not isinstance(self.value, (int, float))):
            raise ValueError("value must be numeric or None")
        if self.quality_status == "OK" and self.value is None:
            raise ValueError("OK macro observation requires numeric value")
        observed = _dt(self.observed_or_effective_at, "observed_or_effective_at")
        published = _dt(self.published_at, "published_at")
        available = _dt(self.available_at, "available_at")
        ingested = _dt(self.ingested_at, "ingested_at")
        if published > available:
            raise ValueError("published_at must be <= available_at")
        if available > ingested:
            raise ValueError("available_at must be <= ingested_at")
        object.__setattr__(self, "observed_or_effective_at", observed)
        object.__setattr__(self, "published_at", published)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "ingested_at", ingested)
        if self.value is not None:
            object.__setattr__(self, "value", float(self.value))


@dataclass(frozen=True)
class MacroState:
    as_of_timestamp: str
    observations: tuple[MacroObservation, ...]
    overall_direction: str
    confidence: float
    dominant_drivers: tuple[str, ...]


def _validate_probability(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ClassifierOutputError(f"{field} must be numeric")
    result = float(value)
    if not 0.0 <= result <= 1.0:
        raise ClassifierOutputError(f"{field} must be within 0..1")
    return result


def _validate_news_output(output: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(output, Mapping):
        raise ClassifierOutputError("classifier output must be a mapping")
    extra = set(output) - _CLASSIFIER_OUTPUT_FIELDS
    missing = _CLASSIFIER_OUTPUT_FIELDS - set(output)
    if extra:
        raise ClassifierOutputError(f"classifier may not output source-bound fields: {sorted(extra)}")
    if missing:
        raise ClassifierOutputError(f"classifier missing fields: {sorted(missing)}")

    event_type = _required_text(output["event_type"], "event_type")  # type: ignore[arg-type]
    entities_raw = output["entities"]
    if not isinstance(entities_raw, Sequence) or isinstance(entities_raw, (str, bytes)):
        raise ClassifierOutputError("entities must be a sequence of strings")
    entities = tuple(_required_text(entity, "entity") for entity in entities_raw)  # type: ignore[arg-type]
    direction = output["direction"]
    importance = output["importance"]
    novelty = output["novelty"]
    horizon = output["horizon"]
    if direction not in _ALLOWED_DIRECTIONS:
        raise ClassifierOutputError("invalid direction")
    if importance not in _ALLOWED_IMPORTANCE:
        raise ClassifierOutputError("invalid importance")
    if novelty not in _ALLOWED_NOVELTY:
        raise ClassifierOutputError("invalid novelty")
    if horizon not in _ALLOWED_HORIZONS:
        raise ClassifierOutputError("invalid horizon")
    mechanism = _required_text(output["mechanism"], "mechanism")  # type: ignore[arg-type]
    return {
        "event_type": event_type,
        "entities": entities,
        "rub_relevance": _validate_probability(output["rub_relevance"], "rub_relevance"),
        "direction": direction,
        "importance": importance,
        "novelty": novelty,
        "horizon": horizon,
        "confidence": _validate_probability(output["confidence"], "confidence"),
        "mechanism": mechanism,
    }


def _source_rank(tier: str) -> int:
    return {
        "OFFICIAL_PRIMARY": 0,
        "OFFICIAL_SECONDARY": 1,
        "MAJOR_AGENCY_OR_FINANCIAL_MEDIA": 2,
    }[tier]


def _assign_clusters(
    records: Sequence[NewsSourceRecord],
    prior_clusters: Mapping[str, Sequence[str]],
    similarity_threshold: float,
) -> dict[str, list[NewsSourceRecord]]:
    if not 0.0 < similarity_threshold <= 1.0:
        raise ValueError("similarity_threshold must be within (0, 1]")
    normalized_prior = {
        cluster_id: tuple(normalize_text(text) for text in texts)
        for cluster_id, texts in prior_clusters.items()
    }
    clusters: dict[str, list[NewsSourceRecord]] = {}
    cluster_texts: dict[str, list[str]] = {key: list(value) for key, value in normalized_prior.items()}

    for record in records:
        headline = normalize_text(record.headline)
        best_cluster: str | None = None
        best_score = 0.0
        for cluster_id, texts in cluster_texts.items():
            score = max((_similarity(headline, text) for text in texts), default=0.0)
            if score >= similarity_threshold and (
                score > best_score
                or (score == best_score and (best_cluster is None or cluster_id < best_cluster))
            ):
                best_score = score
                best_cluster = cluster_id
        if best_cluster is None:
            best_cluster = _stable_id("cluster", headline)
            cluster_texts.setdefault(best_cluster, [])
        clusters.setdefault(best_cluster, []).append(record)
        cluster_texts[best_cluster].append(headline)
    return clusters


def process_news_batch(
    records: Iterable[NewsSourceRecord],
    *,
    as_of_timestamp: datetime | str,
    classifier: Callable[[Mapping[str, object]], Mapping[str, object]],
    prior_clusters: Mapping[str, Sequence[str]] | None = None,
    prior_event_history: Mapping[str, Sequence[Mapping[str, object]]] | None = None,
    similarity_threshold: float = 0.72,
) -> NewsPipelineResult:
    """Normalize, PIT-filter, deduplicate, cluster, then classify one representative per cluster.

    Raw `body` content is transient input. Persistable `NewsEvent` records contain only
    source metadata, hashes and bounded classification outputs.
    """

    as_of = _dt(as_of_timestamp, "as_of_timestamp")
    ordered = sorted(
        tuple(records),
        key=lambda item: (item.available_at, _source_rank(item.source_tier), item.source_id, item.source_reference),
    )
    eligible: list[NewsSourceRecord] = []
    future_filtered = 0
    seen_hashes: set[str] = set()
    duplicate_count = 0
    for record in ordered:
        if record.available_at > as_of:
            future_filtered += 1
            continue
        if record.content_hash in seen_hashes:
            duplicate_count += 1
            continue
        seen_hashes.add(record.content_hash)
        eligible.append(record)

    clusters = _assign_clusters(eligible, prior_clusters or {}, similarity_threshold)
    history = prior_event_history or {}
    events: list[NewsEvent] = []
    for cluster_id in sorted(clusters):
        members = clusters[cluster_id]
        representative = min(
            members,
            key=lambda item: (
                _source_rank(item.source_tier),
                -item.available_at.timestamp(),
                item.source_id,
                item.source_reference,
            ),
        )
        evidence = tuple(
            {
                "source_id": member.source_id,
                "source_tier": member.source_tier,
                "source_reference": member.source_reference,
                "published_at": member.published_at.isoformat(),
                "available_at": member.available_at.isoformat(),
                "content_hash": member.content_hash,
            }
            for member in sorted(
                members,
                key=lambda item: (_source_rank(item.source_tier), item.available_at, item.source_id),
            )
        )
        payload = {
            "instrument": "USDRUBF",
            "cluster_id": cluster_id,
            "headline": representative.headline,
            "normalized_text": representative.normalized_text,
            "cluster_evidence": evidence,
            "cluster_history": tuple(history.get(cluster_id, ())),
            "as_of_timestamp": as_of.isoformat(),
        }
        bounded = _validate_news_output(classifier(payload))
        event_id = _stable_id(
            "event",
            representative.source_id,
            representative.source_reference,
            representative.published_at.isoformat(),
            representative.content_hash,
        )
        events.append(
            NewsEvent(
                event_id=event_id,
                cluster_id=cluster_id,
                source_id=representative.source_id,
                source_tier=representative.source_tier,
                source_reference=representative.source_reference,
                published_at=representative.published_at.isoformat(),
                available_at=representative.available_at.isoformat(),
                ingested_at=representative.ingested_at.isoformat(),
                content_hash=representative.content_hash,
                event_type=bounded["event_type"],  # type: ignore[arg-type]
                entities=bounded["entities"],  # type: ignore[arg-type]
                rub_relevance=bounded["rub_relevance"],  # type: ignore[arg-type]
                direction=bounded["direction"],  # type: ignore[arg-type]
                importance=bounded["importance"],  # type: ignore[arg-type]
                novelty=bounded["novelty"],  # type: ignore[arg-type]
                horizon=bounded["horizon"],  # type: ignore[arg-type]
                confidence=bounded["confidence"],  # type: ignore[arg-type]
                mechanism=bounded["mechanism"],  # type: ignore[arg-type]
            )
        )
    return NewsPipelineResult(
        events=tuple(events),
        exact_duplicates_removed=duplicate_count,
        future_records_filtered=future_filtered,
        clusters_classified=len(clusters),
    )


def build_macro_state(
    observations: Iterable[MacroObservation],
    *,
    as_of_timestamp: datetime | str,
    interpreter: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None,
) -> MacroState:
    """Build a PIT-safe macro state; the interpreter may only assess direction/confidence/drivers."""

    as_of = _dt(as_of_timestamp, "as_of_timestamp")
    eligible = tuple(
        sorted(
            (item for item in observations if item.available_at <= as_of),
            key=lambda item: (item.metric_id, item.available_at, item.source_id),
        )
    )
    if interpreter is None:
        return MacroState(
            as_of_timestamp=as_of.isoformat(),
            observations=eligible,
            overall_direction="NEUTRAL",
            confidence=0.0,
            dominant_drivers=(),
        )

    payload_observations = tuple(
        {
            "metric_id": item.metric_id,
            "source_id": item.source_id,
            "source_reference": item.source_reference,
            "value": item.value,
            "unit": item.unit,
            "observed_or_effective_at": item.observed_or_effective_at.isoformat(),
            "published_at": item.published_at.isoformat(),
            "available_at": item.available_at.isoformat(),
            "quality_status": item.quality_status,
        }
        for item in eligible
    )
    output = interpreter(
        {
            "instrument": "USDRUBF",
            "as_of_timestamp": as_of.isoformat(),
            "observations": payload_observations,
        }
    )
    if not isinstance(output, Mapping):
        raise ClassifierOutputError("macro interpreter output must be a mapping")
    extra = set(output) - _MACRO_INTERPRETER_OUTPUT_FIELDS
    missing = _MACRO_INTERPRETER_OUTPUT_FIELDS - set(output)
    if extra:
        raise ClassifierOutputError(f"macro interpreter may not output factual fields: {sorted(extra)}")
    if missing:
        raise ClassifierOutputError(f"macro interpreter missing fields: {sorted(missing)}")
    direction = output["overall_direction"]
    if direction not in _ALLOWED_DIRECTIONS:
        raise ClassifierOutputError("invalid macro overall_direction")
    drivers_raw = output["dominant_drivers"]
    if not isinstance(drivers_raw, Sequence) or isinstance(drivers_raw, (str, bytes)):
        raise ClassifierOutputError("dominant_drivers must be a sequence")
    drivers = tuple(_required_text(driver, "dominant_driver") for driver in drivers_raw)  # type: ignore[arg-type]
    usable_metric_ids = {item.metric_id for item in eligible if item.quality_status == "OK"}
    if not set(drivers).issubset(usable_metric_ids):
        raise ClassifierOutputError("dominant_drivers must reference eligible OK observations")
    return MacroState(
        as_of_timestamp=as_of.isoformat(),
        observations=eligible,
        overall_direction=direction,  # type: ignore[arg-type]
        confidence=_validate_probability(output["confidence"], "confidence"),
        dominant_drivers=drivers,
    )
