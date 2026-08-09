from datetime import datetime, timezone

import pytest

from src.moex_research.intelligence.usdrubf_news_macro import (
    ClassifierOutputError,
    MacroObservation,
    NewsSourceRecord,
    build_macro_state,
    process_news_batch,
)


T0 = datetime(2026, 8, 9, 10, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 8, 9, 10, 1, tzinfo=timezone.utc)
T2 = datetime(2026, 8, 9, 10, 2, tzinfo=timezone.utc)
T3 = datetime(2026, 8, 9, 10, 3, tzinfo=timezone.utc)


def _record(
    reference: str,
    headline: str,
    *,
    source_id: str = "cbr",
    source_tier: str = "OFFICIAL_PRIMARY",
    published_at=T0,
    available_at=T1,
    ingested_at=T2,
    body: str = "",
) -> NewsSourceRecord:
    return NewsSourceRecord(
        source_id=source_id,
        source_tier=source_tier,
        source_reference=reference,
        published_at=published_at,
        available_at=available_at,
        ingested_at=ingested_at,
        headline=headline,
        body=body,
    )


def _classification(**overrides):
    result = {
        "event_type": "MONETARY_POLICY",
        "entities": ["CBR"],
        "rub_relevance": 0.9,
        "direction": "USD_BEARISH",
        "importance": "HIGH",
        "novelty": "NEW",
        "horizon": "SHORT_TERM",
        "confidence": 0.8,
        "mechanism": "Rate expectations affect RUB carry.",
    }
    result.update(overrides)
    return result


def test_exact_duplicates_are_removed_before_classifier() -> None:
    calls = []

    def classifier(payload):
        calls.append(payload)
        return _classification()

    first = _record("a", "CBR keeps policy rate unchanged")
    duplicate = _record(
        "b",
        "CBR keeps policy rate unchanged",
        source_id="agency",
        source_tier="MAJOR_AGENCY_OR_FINANCIAL_MEDIA",
    )
    result = process_news_batch([first, duplicate], as_of_timestamp=T3, classifier=classifier)

    assert result.exact_duplicates_removed == 1
    assert result.clusters_classified == 1
    assert len(calls) == 1
    assert len(result.events) == 1
    assert result.events[0].source_id == "cbr"


def test_semantically_similar_headlines_cluster_before_classifier() -> None:
    calls = []

    def classifier(payload):
        calls.append(payload)
        return _classification()

    records = [
        _record("a", "CBR keeps key rate unchanged at meeting"),
        _record(
            "b",
            "CBR keeps key rate unchanged after meeting",
            source_id="agency",
            source_tier="MAJOR_AGENCY_OR_FINANCIAL_MEDIA",
        ),
    ]
    result = process_news_batch(records, as_of_timestamp=T3, classifier=classifier, similarity_threshold=0.6)

    assert result.clusters_classified == 1
    assert len(calls[0]["cluster_evidence"]) == 2


def test_future_news_is_filtered_before_classifier() -> None:
    calls = []

    def classifier(payload):
        calls.append(payload)
        return _classification()

    future = _record("future", "Future release", published_at=T2, available_at=T3, ingested_at=T3)
    result = process_news_batch([future], as_of_timestamp=T2, classifier=classifier)

    assert result.future_records_filtered == 1
    assert result.events == ()
    assert calls == []


def test_classifier_cannot_override_source_bound_facts() -> None:
    def classifier(_payload):
        return _classification(source_id="invented")

    with pytest.raises(ClassifierOutputError, match="source-bound"):
        process_news_batch([_record("a", "CBR decision")], as_of_timestamp=T3, classifier=classifier)


def test_classifier_outputs_are_bounded() -> None:
    def classifier(_payload):
        return _classification(confidence=1.5)

    with pytest.raises(ClassifierOutputError, match="confidence"):
        process_news_batch([_record("a", "CBR decision")], as_of_timestamp=T3, classifier=classifier)


def test_prior_cluster_history_is_supplied_to_classifier_not_model_memory() -> None:
    probe = {}

    def first_classifier(payload):
        probe["cluster_id"] = payload["cluster_id"]
        return _classification()

    first = process_news_batch(
        [_record("a", "New sanctions package announced")],
        as_of_timestamp=T3,
        classifier=first_classifier,
    )
    cluster_id = first.events[0].cluster_id
    seen = {}

    def second_classifier(payload):
        seen["history"] = payload["cluster_history"]
        return _classification(novelty="UPDATE")

    process_news_batch(
        [_record("b", "New sanctions package officially announced")],
        as_of_timestamp=T3,
        classifier=second_classifier,
        prior_clusters={cluster_id: ["New sanctions package announced"]},
        prior_event_history={cluster_id: [{"novelty": "NEW", "event_id": first.events[0].event_id}]},
        similarity_threshold=0.5,
    )
    assert seen["history"] == ({"novelty": "NEW", "event_id": first.events[0].event_id},)


def test_persistable_news_event_does_not_retain_raw_body() -> None:
    result = process_news_batch(
        [_record("a", "CBR decision", body="raw licensed article text")],
        as_of_timestamp=T3,
        classifier=lambda _: _classification(),
    )
    event = result.events[0]
    assert not hasattr(event, "body")
    assert not hasattr(event, "normalized_text")
    assert len(event.content_hash) == 64


def _macro(metric_id: str, value, *, available_at=T1, quality_status="OK") -> MacroObservation:
    return MacroObservation(
        metric_id=metric_id,
        source_id="official",
        source_reference=f"https://example/{metric_id}",
        value=value,
        unit="index",
        observed_or_effective_at=T0,
        published_at=T0,
        available_at=available_at,
        ingested_at=max(available_at, T2),
        quality_status=quality_status,
    )


def test_macro_state_filters_future_observations_before_interpreter() -> None:
    captured = {}

    def interpreter(payload):
        captured["observations"] = payload["observations"]
        return {
            "overall_direction": "USD_BULLISH",
            "confidence": 0.7,
            "dominant_drivers": ["oil"],
        }

    state = build_macro_state(
        [_macro("oil", 70.0), _macro("future", 1.0, available_at=T3)],
        as_of_timestamp=T2,
        interpreter=interpreter,
    )

    assert [item.metric_id for item in state.observations] == ["oil"]
    assert [item["metric_id"] for item in captured["observations"]] == ["oil"]


def test_macro_missing_data_remains_explicit_and_cannot_be_dominant_driver() -> None:
    missing = _macro("inflation", None, quality_status="MISSING")

    def interpreter(_payload):
        return {
            "overall_direction": "NEUTRAL",
            "confidence": 0.2,
            "dominant_drivers": ["inflation"],
        }

    with pytest.raises(ClassifierOutputError, match="eligible OK"):
        build_macro_state([missing], as_of_timestamp=T2, interpreter=interpreter)


def test_macro_interpreter_cannot_rewrite_numeric_observations() -> None:
    def interpreter(_payload):
        return {
            "overall_direction": "USD_BULLISH",
            "confidence": 0.7,
            "dominant_drivers": ["oil"],
            "observations": [{"metric_id": "oil", "value": 999.0}],
        }

    with pytest.raises(ClassifierOutputError, match="factual fields"):
        build_macro_state([_macro("oil", 70.0)], as_of_timestamp=T2, interpreter=interpreter)


def test_naive_timestamps_are_rejected_for_point_in_time_safety() -> None:
    naive = datetime(2026, 8, 9, 10, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        _record("a", "CBR decision", published_at=naive)
