from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import src.moex_research.intelligence.usdrubf_news_live_pipeline as pipeline
from src.moex_research.intelligence.usdrubf_news_live_eia import (
    SOURCE_ID as EIA_ID,
    SUMMARY_PDF_URL,
    EiaAcquisitionError,
    EiaSourceResult,
)
from src.moex_research.intelligence.usdrubf_news_macro import NewsSourceRecord


RSS_ID = "healthy_rss"
RSS_URL = "https://healthy.example/feed.xml"
NOW = datetime(2026, 9, 2, 15, 0, tzinfo=timezone.utc)


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
                        "source_id": RSS_ID,
                        "tier": "OFFICIAL_PRIMARY",
                        "transport": "RSS",
                        "references": [RSS_URL],
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
        '<rss version="2.0"><channel><title>test</title><item>'
        '<title>Healthy official item</title>'
        '<link>https://healthy.example/item</link>'
        '<pubDate>Wed, 02 Sep 2026 14:00:00 +0000</pubDate>'
        '<description>Healthy body</description>'
        '</item></channel></rss>'
    ).encode("utf-8")


def _opener(request, timeout):
    assert request.full_url == RSS_URL
    return _Response(_rss(), RSS_URL)


def test_default_live_set_includes_eia_as_thirteenth_source() -> None:
    assert EIA_ID in pipeline.LIVE_OFFICIAL_SOURCE_IDS
    assert len(pipeline.LIVE_OFFICIAL_SOURCE_IDS) == 13


def test_eia_failure_is_isolated_from_existing_healthy_source(tmp_path: Path, monkeypatch) -> None:
    def fail_eia(**kwargs):
        raise EiaAcquisitionError("SOURCE_UNAVAILABLE", "EIA unavailable")

    monkeypatch.setattr(pipeline, "fetch_eia_wpsr", fail_eia)
    result = pipeline._acquire_official_sources(
        registry_path=_registry(tmp_path),
        source_ids=(RSS_ID, EIA_ID),
        opener=_opener,
        now_fn=lambda: NOW,
        timeout_seconds=10.0,
    )

    assert [item.source_id for item in result.source_results] == [RSS_ID, EIA_ID]
    assert result.source_results[0].quality_status == "OK"
    assert len(result.source_results[0].records) == 1
    assert result.source_results[1].quality_status == "SOURCE_UNAVAILABLE"
    assert result.source_results[1].records == ()
    assert len(result.failures) == 1


def test_eia_success_is_composed_without_directional_semantic_changes(tmp_path: Path, monkeypatch) -> None:
    record = NewsSourceRecord(
        source_id=EIA_ID,
        source_tier="OFFICIAL_PRIMARY",
        source_reference=SUMMARY_PDF_URL,
        published_at=datetime(2026, 9, 2, 10, 30, tzinfo=timezone.utc),
        available_at=NOW,
        ingested_at=NOW,
        headline="EIA Weekly Petroleum Status Report",
        body="Crude oil inventories decreased.",
    )

    def ok_eia(**kwargs):
        return EiaSourceResult(source_id=EIA_ID, quality_status="OK", records=(record,))

    monkeypatch.setattr(pipeline, "fetch_eia_wpsr", ok_eia)
    result = pipeline._acquire_official_sources(
        registry_path=_registry(tmp_path),
        source_ids=(RSS_ID, EIA_ID),
        opener=_opener,
        now_fn=lambda: NOW,
        timeout_seconds=10.0,
    )

    assert [item.source_id for item in result.source_results] == [RSS_ID, EIA_ID]
    assert result.ok_source_count == 2
    assert result.failures == ()
    assert result.source_results[1].records[0].source_reference == SUMMARY_PDF_URL
