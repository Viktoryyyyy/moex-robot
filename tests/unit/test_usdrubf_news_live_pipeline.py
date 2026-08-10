from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from src.moex_research.intelligence.usdrubf_news_live_pipeline import (
    run_live_official_news_pipeline,
)
from src.moex_research.intelligence.usdrubf_news_macro import ClassifierOutputError


class _Response:
    def __init__(self, payload: bytes, url: str) -> None:
        self.payload = payload
        self.url = url

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.url


def _rss(*items: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel><title>test</title>'
        + "".join(items)
        + "</channel></rss>"
    ).encode("utf-8")


def _item(*, host: str, title: str = "Policy update", pub_date: str) -> str:
    return (
        "<item>"
        f"<title>{title}</title>"
        f"<link>https://{host}/event</link>"
        f"<pubDate>{pub_date}</pubDate>"
        "<description>Official details</description>"
        "</item>"
    )


def _source(source_id: str, host: str) -> dict[str, object]:
    return {
        "source_id": source_id,
        "tier": "OFFICIAL_PRIMARY",
        "transport": "RSS",
        "references": [f"https://{host}/feed.xml"],
        "stage12b_status": "READY_CANDIDATE",
    }


def _registry(tmp_path: Path, *sources: dict[str, object]) -> Path:
    path = tmp_path / "registry.json"
    path.write_text(json.dumps({"primary_sources": list(sources)}), encoding="utf-8")
    return path


def _classifier_agent(payload: dict[str, object]) -> dict[str, object]:
    assert payload["instrument"] == "USDRUBF"
    assert payload["cluster_evidence"]
    return {
        "event_type": "OFFICIAL_COMMUNICATION",
        "entities": ["official_source"],
        "rub_relevance": 0.7,
        "direction": "NEUTRAL",
        "importance": "MEDIUM",
        "novelty": "NEW",
        "horizon": "SHORT_TERM",
        "confidence": 0.8,
        "mechanism": "Переданное официальное сообщение не подтверждает направленный эффект для USDRUBF.",
    }


def test_live_pipeline_preserves_source_failure_and_classifies_healthy_records(tmp_path: Path) -> None:
    registry = _registry(
        tmp_path,
        _source("one", "one.example"),
        _source("two", "two.example"),
    )
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    payload = _rss(
        _item(
            host="one.example",
            pub_date="Mon, 10 Aug 2026 13:30:00 +0000",
        )
    )

    def opener(request, timeout):
        if request.full_url.startswith("https://two.example/"):
            raise OSError("network down")
        return _Response(payload, request.full_url)

    result = run_live_official_news_pipeline(
        classifier_agent=_classifier_agent,
        registry_path=registry,
        source_ids=("one", "two"),
        opener=opener,
        now_fn=lambda: now,
    )

    assert result.as_of_timestamp == now.isoformat()
    assert result.acquired_record_count == 1
    assert result.source_failure_count == 1
    assert result.acquisition.failures[0].source_id == "two"
    assert result.acquisition.failures[0].quality_status == "SOURCE_UNAVAILABLE"
    assert len(result.news.events) == 1
    event = result.news.events[0]
    assert event.source_id == "one"
    assert event.event_type == "OFFICIAL_COMMUNICATION"
    assert event.direction == "NEUTRAL"
    assert event.quality_status == "OK"


def test_live_pipeline_does_not_classify_future_rss_items(tmp_path: Path) -> None:
    registry = _registry(tmp_path, _source("one", "one.example"))
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    payload = _rss(
        _item(
            host="one.example",
            pub_date="Mon, 10 Aug 2026 14:05:00 +0000",
        )
    )
    calls = 0

    def classifier_agent(payload):
        nonlocal calls
        calls += 1
        return _classifier_agent(payload)

    result = run_live_official_news_pipeline(
        classifier_agent=classifier_agent,
        registry_path=registry,
        source_ids=("one",),
        opener=lambda request, timeout: _Response(payload, request.full_url),
        now_fn=lambda: now,
    )

    assert result.acquisition.source_results[0].future_items_skipped == 1
    assert result.acquired_record_count == 0
    assert result.news.events == ()
    assert calls == 0


def test_live_pipeline_rejects_future_as_of_timestamp_before_acquisition(tmp_path: Path) -> None:
    registry = _registry(tmp_path, _source("one", "one.example"))
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="later than acquisition time"):
        run_live_official_news_pipeline(
            classifier_agent=_classifier_agent,
            registry_path=registry,
            source_ids=("one",),
            opener=lambda request, timeout: pytest.fail("network must not be called"),
            now_fn=lambda: now,
            as_of_timestamp=now + timedelta(seconds=1),
        )


def test_live_pipeline_enforces_stage12b3_output_contract(tmp_path: Path) -> None:
    registry = _registry(tmp_path, _source("one", "one.example"))
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    payload = _rss(
        _item(
            host="one.example",
            pub_date="Mon, 10 Aug 2026 13:30:00 +0000",
        )
    )

    def invalid_agent(payload):
        return {**_classifier_agent(payload), "event_type": "UNFROZEN_EVENT_TYPE"}

    with pytest.raises(ClassifierOutputError, match="invalid event_type"):
        run_live_official_news_pipeline(
            classifier_agent=invalid_agent,
            registry_path=registry,
            source_ids=("one",),
            opener=lambda request, timeout: _Response(payload, request.full_url),
            now_fn=lambda: now,
        )


def test_live_pipeline_enforces_stage12b3_source_bound_output_guard(tmp_path: Path) -> None:
    registry = _registry(tmp_path, _source("one", "one.example"))
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    payload = _rss(
        _item(
            host="one.example",
            pub_date="Mon, 10 Aug 2026 13:30:00 +0000",
        )
    )

    def invalid_agent(payload):
        return {**_classifier_agent(payload), "source_id": "invented"}

    with pytest.raises(ClassifierOutputError, match="extra fields"):
        run_live_official_news_pipeline(
            classifier_agent=invalid_agent,
            registry_path=registry,
            source_ids=("one",),
            opener=lambda request, timeout: _Response(payload, request.full_url),
            now_fn=lambda: now,
        )


def test_live_pipeline_rejects_future_cluster_history_before_agent_call(tmp_path: Path) -> None:
    registry = _registry(tmp_path, _source("one", "one.example"))
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    payload = _rss(
        _item(
            host="one.example",
            pub_date="Mon, 10 Aug 2026 13:30:00 +0000",
        )
    )
    called = False

    def classifier_agent(payload):
        nonlocal called
        called = True
        return _classifier_agent(payload)

    with pytest.raises(ClassifierOutputError, match="cluster_history may not contain future entries"):
        run_live_official_news_pipeline(
            classifier_agent=classifier_agent,
            registry_path=registry,
            source_ids=("one",),
            opener=lambda request, timeout: _Response(payload, request.full_url),
            now_fn=lambda: now,
            prior_clusters={"cluster_existing": ("Policy update",)},
            prior_event_history={
                "cluster_existing": (
                    {"available_at": (now + timedelta(seconds=1)).isoformat(), "novelty": "NEW"},
                )
            },
        )
    assert called is False
