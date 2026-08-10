from __future__ import annotations

from datetime import datetime
import re
from typing import Callable, Mapping, Sequence

from .usdrubf_news_macro import ClassifierOutputError


STAGE12B3_INPUT_FIELDS = frozenset(
    {
        "instrument",
        "cluster_id",
        "headline",
        "normalized_text",
        "cluster_evidence",
        "cluster_history",
        "as_of_timestamp",
    }
)
STAGE12B3_EVIDENCE_FIELDS = frozenset(
    {
        "source_id",
        "source_tier",
        "source_reference",
        "published_at",
        "available_at",
        "content_hash",
        "normalized_text",
    }
)
STAGE12B3_OUTPUT_FIELDS = frozenset(
    {
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
)
STAGE12B3_EVENT_TYPES = frozenset(
    {
        "CBR_MONETARY_POLICY",
        "CBR_FX_POLICY",
        "CBR_REGULATORY_POLICY",
        "MOEX_FX_MARKET_STRUCTURE",
        "MOEX_MARKET_OPERATION",
        "FED_MONETARY_POLICY",
        "US_INFLATION",
        "US_LABOR_MARKET",
        "SANCTIONS",
        "GEOPOLITICS",
        "ENERGY_OIL",
        "OFFICIAL_COMMUNICATION",
        "OTHER_RUB_RELEVANT",
        "OTHER_LOW_RELEVANCE",
    }
)
STAGE12B3_DIRECTIONS = frozenset({"USD_BULLISH", "USD_BEARISH", "NEUTRAL", "MIXED"})
STAGE12B3_IMPORTANCE = frozenset({"LOW", "MEDIUM", "HIGH", "CRITICAL"})
STAGE12B3_NOVELTY = frozenset({"NEW", "UPDATE", "REPEAT", "STALE"})
STAGE12B3_HORIZONS = frozenset({"INTRADAY", "SHORT_TERM", "MEDIUM_TERM", "LONG_TERM"})
STAGE12B3_SOURCE_TIERS = frozenset(
    {"OFFICIAL_PRIMARY", "OFFICIAL_SECONDARY", "MAJOR_AGENCY_OR_FINANCIAL_MEDIA"}
)
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


ClassifierAgent = Callable[[Mapping[str, object]], Mapping[str, object]]


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ClassifierOutputError(f"{field} must be non-empty string")
    return value.strip()


def _aware_iso(value: object, field: str) -> datetime:
    text = _required_text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ClassifierOutputError(f"{field} must be ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ClassifierOutputError(f"{field} must be timezone-aware")
    return parsed


def _probability(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ClassifierOutputError(f"{field} must be numeric")
    result = float(value)
    if not 0.0 <= result <= 1.0:
        raise ClassifierOutputError(f"{field} must be within 0..1")
    return result


def _enum(value: object, allowed: frozenset[str], field: str) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ClassifierOutputError(f"invalid {field}")
    return value


def _mapping_sequence(value: object, field: str, *, allow_empty: bool) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ClassifierOutputError(f"{field} must be a sequence of mappings")
    items = tuple(value)
    if not allow_empty and not items:
        raise ClassifierOutputError(f"{field} must be non-empty")
    if any(not isinstance(item, Mapping) for item in items):
        raise ClassifierOutputError(f"{field} must contain mappings only")
    return items


def _validate_classifier_payload(payload: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(payload, Mapping):
        raise ClassifierOutputError("classifier payload must be a mapping")
    fields = set(payload)
    extra = fields - STAGE12B3_INPUT_FIELDS
    missing = STAGE12B3_INPUT_FIELDS - fields
    if extra:
        raise ClassifierOutputError(f"classifier payload has extra fields: {sorted(extra)}")
    if missing:
        raise ClassifierOutputError(f"classifier payload missing fields: {sorted(missing)}")
    if payload["instrument"] != "USDRUBF":
        raise ClassifierOutputError("instrument must be USDRUBF")

    as_of = _aware_iso(payload["as_of_timestamp"], "as_of_timestamp")
    cluster_id = _required_text(payload["cluster_id"], "cluster_id")
    headline = _required_text(payload["headline"], "headline")
    normalized_text = _required_text(payload["normalized_text"], "normalized_text")

    evidence = _mapping_sequence(payload["cluster_evidence"], "cluster_evidence", allow_empty=False)
    bounded_evidence: list[dict[str, object]] = []
    for index, item in enumerate(evidence):
        fields = set(item)
        extra = fields - STAGE12B3_EVIDENCE_FIELDS
        missing = STAGE12B3_EVIDENCE_FIELDS - fields
        if extra or missing:
            raise ClassifierOutputError(
                f"cluster_evidence[{index}] fields mismatch: extra={sorted(extra)} missing={sorted(missing)}"
            )
        tier = _enum(item["source_tier"], STAGE12B3_SOURCE_TIERS, f"cluster_evidence[{index}].source_tier")
        published = _aware_iso(item["published_at"], f"cluster_evidence[{index}].published_at")
        available = _aware_iso(item["available_at"], f"cluster_evidence[{index}].available_at")
        if published > available:
            raise ClassifierOutputError("cluster evidence published_at must be <= available_at")
        if available > as_of:
            raise ClassifierOutputError("cluster evidence may not be available after as_of_timestamp")
        content_hash = _required_text(item["content_hash"], f"cluster_evidence[{index}].content_hash")
        if not _HASH_RE.fullmatch(content_hash):
            raise ClassifierOutputError("cluster evidence content_hash must be lowercase sha256 hex")
        bounded_evidence.append(
            {
                "source_id": _required_text(item["source_id"], f"cluster_evidence[{index}].source_id"),
                "source_tier": tier,
                "source_reference": _required_text(
                    item["source_reference"], f"cluster_evidence[{index}].source_reference"
                ),
                "published_at": published.isoformat(),
                "available_at": available.isoformat(),
                "content_hash": content_hash,
                "normalized_text": _required_text(
                    item["normalized_text"], f"cluster_evidence[{index}].normalized_text"
                ),
            }
        )

    history = _mapping_sequence(payload["cluster_history"], "cluster_history", allow_empty=True)
    bounded_history: list[dict[str, object]] = []
    for index, item in enumerate(history):
        if "available_at" not in item:
            raise ClassifierOutputError(
                f"cluster_history[{index}].available_at is required for PIT validation"
            )
        available = _aware_iso(item["available_at"], f"cluster_history[{index}].available_at")
        if available > as_of:
            raise ClassifierOutputError("cluster history may not be available after as_of_timestamp")
        bounded_item = dict(item)
        bounded_item["available_at"] = available.isoformat()
        bounded_history.append(bounded_item)

    return {
        "instrument": "USDRUBF",
        "cluster_id": cluster_id,
        "headline": headline,
        "normalized_text": normalized_text,
        "cluster_evidence": tuple(bounded_evidence),
        "cluster_history": tuple(bounded_history),
        "as_of_timestamp": as_of.isoformat(),
    }


def _validate_classifier_output(output: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(output, Mapping):
        raise ClassifierOutputError("classifier output must be a mapping")
    fields = set(output)
    extra = fields - STAGE12B3_OUTPUT_FIELDS
    missing = STAGE12B3_OUTPUT_FIELDS - fields
    if extra:
        raise ClassifierOutputError(f"classifier output has extra fields: {sorted(extra)}")
    if missing:
        raise ClassifierOutputError(f"classifier output missing fields: {sorted(missing)}")

    entities_raw = output["entities"]
    if not isinstance(entities_raw, Sequence) or isinstance(entities_raw, (str, bytes, bytearray)):
        raise ClassifierOutputError("entities must be a sequence of strings")
    entities = tuple(_required_text(item, "entity") for item in entities_raw)
    if len(set(entities)) != len(entities):
        raise ClassifierOutputError("entities must be unique")

    return {
        "event_type": _enum(output["event_type"], STAGE12B3_EVENT_TYPES, "event_type"),
        "entities": entities,
        "rub_relevance": _probability(output["rub_relevance"], "rub_relevance"),
        "direction": _enum(output["direction"], STAGE12B3_DIRECTIONS, "direction"),
        "importance": _enum(output["importance"], STAGE12B3_IMPORTANCE, "importance"),
        "novelty": _enum(output["novelty"], STAGE12B3_NOVELTY, "novelty"),
        "horizon": _enum(output["horizon"], STAGE12B3_HORIZONS, "horizon"),
        "confidence": _probability(output["confidence"], "confidence"),
        "mechanism": _required_text(output["mechanism"], "mechanism"),
    }


class Stage12B3NewsClassifier:
    """Fail-closed boundary around a future LLM/Flowise news classifier transport.

    This guard validates the source-bound cluster payload before the model call and
    validates the bounded nine-field interpretation after the call. It performs no
    network access and does not provide an endpoint, model, memory or tool access.
    """

    def __init__(self, agent: ClassifierAgent) -> None:
        if not callable(agent):
            raise ValueError("agent must be callable")
        self._agent = agent

    def __call__(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        bounded_payload = _validate_classifier_payload(payload)
        try:
            response = self._agent(bounded_payload)
        except ClassifierOutputError:
            raise
        except Exception as exc:
            raise ClassifierOutputError("news classifier agent call failed") from exc
        return _validate_classifier_output(response)


def stage12b3_news_classifier(agent: ClassifierAgent) -> Stage12B3NewsClassifier:
    return Stage12B3NewsClassifier(agent)
