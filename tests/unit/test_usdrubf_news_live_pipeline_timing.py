from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from src.moex_research.intelligence.usdrubf_news_live_pipeline import (
    deterministic_neutral_news_classifier,
    run_live_official_news_pipeline,
)


class _Response:
    def __init__(self, payload: bytes, url: str) -> None:
        self.payload = payload
        self.url = url

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.url


def _registry(tmp_path: Path) -> Path:
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "primary_sources": [
                    {
                        "source_id": "official",
                        "tier": "OFFICIAL_PRIMARY",
                        "transport": "RSS",
                        "references": ["https://official.example/feed.xml"],
                        "stage12b_status": "READY_CANDIDATE",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def _rss() -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel><title>test</title>'
        '<item><title>Official policy update</title>'
        '<link>https://official.example/event</link>'
        '<pubDate>Thu, 13 Aug 2026 08:59:00 +0000</pubDate>'
        '<description>Official details</description></item>'
        '</channel></rss>'
    ).encode("utf-8")


def test_live_pipeline_restamps_ingestion_at_batch_completion(tmp_path: Path) -> None:
    started = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)
    source_clock = started + timedelta(seconds=1)
    completed = started + timedelta(seconds=4)
    values = iter((started, source_clock, completed))

    result = run_live_official_news_pipeline(
        classifier_agent=deterministic_neutral_news_classifier,
        registry_path=_registry(tmp_path),
        source_ids=("official",),
        opener=lambda request, timeout: _Response(_rss(), request.full_url),
        now_fn=lambda: next(values),
    )

    assert result.as_of_timestamp == completed.isoformat()
    assert result.acquired_record_count == 1
    assert result.acquisition.records[0].ingested_at == completed
    assert len(result.news.events) == 1
    event = result.news.events[0]
    assert event.ingested_at == completed.isoformat()
    assert event.event_type == "OFFICIAL_COMMUNICATION"
    assert event.direction == "NEUTRAL"
    assert event.rub_relevance == 0.0
    assert event.confidence == 0.0
    assert event.importance == "LOW"
    assert event.novelty == "NEW"


def test_neutral_classifier_marks_existing_cluster_as_update() -> None:
    output = deterministic_neutral_news_classifier(
        {
            "cluster_history": ({"available_at": "2026-08-12T09:00:00+00:00"},),
        }
    )
    assert output["novelty"] == "UPDATE"
    assert output["direction"] == "NEUTRAL"
    assert output["confidence"] == 0.0
