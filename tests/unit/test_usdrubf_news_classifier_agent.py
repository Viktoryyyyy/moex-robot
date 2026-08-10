from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.moex_research.intelligence.usdrubf_news_classifier_agent import (
    STAGE12B3_EVENT_TYPES,
    stage12b3_news_classifier,
)
from src.moex_research.intelligence.usdrubf_news_macro import (
    ClassifierOutputError,
    NewsSourceRecord,
    process_news_batch,
)


AS_OF = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
CONTENT_HASH = "a" * 64


def _payload() -> dict[str, object]:
    return {
        "instrument": "USDRUBF",
        "cluster_id": "cluster_test",
        "headline": "Bank of Russia policy update",
        "normalized_text": "bank of russia policy update",
        "cluster_evidence": (
            {
                "source_id": "cbr_press_rss",
                "source_tier": "OFFICIAL_PRIMARY",
                "source_reference": "https://www.cbr.ru/press/event/",
                "published_at": "2026-08-10T13:30:00+00:00",
                "available_at": "2026-08-10T13:30:00+00:00",
                "content_hash": CONTENT_HASH,
                "normalized_text": "bank of russia policy update",
            },
        ),
        "cluster_history": (),
        "as_of_timestamp": AS_OF.isoformat(),
    }


def _output(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "event_type": "CBR_MONETARY_POLICY",
        "entities": ["Bank of Russia"],
        "rub_relevance": 0.9,
        "direction": "NEUTRAL",
        "importance": "HIGH",
        "novelty": "NEW",
        "horizon": "SHORT_TERM",
        "confidence": 0.7,
        "mechanism": "Из переданного текста направление для RUB не подтверждено без дополнительного контекста.",
    }
    value.update(overrides)
    return value


def test_guard_passes_only_bounded_payload_and_normalizes_output() -> None:
    seen: dict[str, object] = {}

    def agent(payload):
        seen.update(payload)
        return _output()

    classifier = stage12b3_news_classifier(agent)
    result = classifier(_payload())

    assert set(seen) == {
        "instrument",
        "cluster_id",
        "headline",
        "normalized_text",
        "cluster_evidence",
        "cluster_history",
        "as_of_timestamp",
    }
    assert seen["instrument"] == "USDRUBF"
    assert result["event_type"] == "CBR_MONETARY_POLICY"
    assert result["direction"] == "NEUTRAL"
    assert result["entities"] == ("Bank of Russia",)
    assert result["rub_relevance"] == 0.9


def test_guard_rejects_future_evidence_before_agent_call() -> None:
    payload = _payload()
    evidence = dict(payload["cluster_evidence"][0])
    evidence["available_at"] = (AS_OF + timedelta(seconds=1)).isoformat()
    payload["cluster_evidence"] = (evidence,)
    called = False

    def agent(payload):
        nonlocal called
        called = True
        return _output()

    with pytest.raises(ClassifierOutputError, match="may not be available after"):
        stage12b3_news_classifier(agent)(payload)
    assert called is False


def test_guard_rejects_future_cluster_history_before_agent_call() -> None:
    payload = _payload()
    payload["cluster_history"] = (
        {
            "event_id": "future_event",
            "available_at": (AS_OF + timedelta(seconds=1)).isoformat(),
            "novelty": "NEW",
        },
    )
    called = False

    def agent(payload):
        nonlocal called
        called = True
        return _output()

    with pytest.raises(ClassifierOutputError, match="cluster history may not be available after"):
        stage12b3_news_classifier(agent)(payload)
    assert called is False


def test_guard_requires_cluster_history_available_at_for_pit_validation() -> None:
    payload = _payload()
    payload["cluster_history"] = ({"event_id": "old_event", "novelty": "NEW"},)
    called = False

    def agent(payload):
        nonlocal called
        called = True
        return _output()

    with pytest.raises(ClassifierOutputError, match="available_at is required"):
        stage12b3_news_classifier(agent)(payload)
    assert called is False


def test_guard_accepts_pit_safe_cluster_history() -> None:
    payload = _payload()
    payload["cluster_history"] = (
        {
            "event_id": "old_event",
            "available_at": (AS_OF - timedelta(minutes=5)).isoformat(),
            "novelty": "NEW",
        },
    )
    seen: dict[str, object] = {}

    def agent(bounded_payload):
        seen.update(bounded_payload)
        return _output(novelty="UPDATE")

    result = stage12b3_news_classifier(agent)(payload)

    history = seen["cluster_history"]
    assert history[0]["available_at"] == (AS_OF - timedelta(minutes=5)).isoformat()
    assert result["novelty"] == "UPDATE"


def test_guard_rejects_extra_input_field() -> None:
    payload = _payload()
    payload["market_price"] = 99999.0
    with pytest.raises(ClassifierOutputError, match="extra fields"):
        stage12b3_news_classifier(lambda payload: _output())(payload)


def test_guard_rejects_source_bound_output_field() -> None:
    with pytest.raises(ClassifierOutputError, match="extra fields"):
        stage12b3_news_classifier(
            lambda payload: _output(source_id="invented")
        )(_payload())


def test_guard_rejects_unfrozen_event_type_and_bad_probability() -> None:
    assert "RANDOM_EVENT" not in STAGE12B3_EVENT_TYPES
    with pytest.raises(ClassifierOutputError, match="invalid event_type"):
        stage12b3_news_classifier(
            lambda payload: _output(event_type="RANDOM_EVENT")
        )(_payload())
    with pytest.raises(ClassifierOutputError, match="rub_relevance must be within"):
        stage12b3_news_classifier(
            lambda payload: _output(rub_relevance=1.1)
        )(_payload())


def test_guard_rejects_duplicate_entities() -> None:
    with pytest.raises(ClassifierOutputError, match="entities must be unique"):
        stage12b3_news_classifier(
            lambda payload: _output(entities=["CBR", "CBR"])
        )(_payload())


def test_guard_wraps_transport_failure_fail_closed() -> None:
    def broken_agent(payload):
        raise OSError("transport down")

    with pytest.raises(ClassifierOutputError, match="agent call failed"):
        stage12b3_news_classifier(broken_agent)(_payload())


def test_guard_is_compatible_with_existing_process_news_batch() -> None:
    record = NewsSourceRecord(
        source_id="cbr_press_rss",
        source_tier="OFFICIAL_PRIMARY",
        source_reference="https://www.cbr.ru/press/event/",
        published_at=datetime(2026, 8, 10, 13, 30, tzinfo=timezone.utc),
        available_at=datetime(2026, 8, 10, 13, 30, tzinfo=timezone.utc),
        ingested_at=AS_OF,
        headline="Bank of Russia policy update",
        body="Official policy details.",
    )
    classifier = stage12b3_news_classifier(lambda payload: _output())

    result = process_news_batch(
        [record],
        as_of_timestamp=AS_OF,
        classifier=classifier,
    )

    assert len(result.events) == 1
    event = result.events[0]
    assert event.source_id == "cbr_press_rss"
    assert event.event_type == "CBR_MONETARY_POLICY"
    assert event.direction == "NEUTRAL"
    assert event.importance == "HIGH"
