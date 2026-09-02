from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from src.moex_research.intelligence.usdrubf_news_live_pipeline import (
    LIVE_OFFICIAL_SOURCE_IDS,
    _acquire_official_sources,
)


TREASURY_ID = "us_treasury_press_releases"
OFAC_ID = "ofac_recent_actions"
RSS_ID = "healthy_rss"
RSS_URL = "https://healthy.example/feed.xml"
TREASURY_INDEX = "https://home.treasury.gov/news/press-releases"
TREASURY_DETAIL = "https://home.treasury.gov/news/press-releases/sb0620"
NOW = datetime(2026, 9, 2, 7, 30, tzinfo=timezone.utc)


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
                    },
                    {
                        "source_id": TREASURY_ID,
                        "group": "US_TREASURY_OFAC",
                        "tier": "OFFICIAL_PRIMARY",
                        "transport": "HTML_INDEX",
                        "references": [TREASURY_INDEX],
                        "stage12b_status": "READY_CANDIDATE",
                    },
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
        '<pubDate>Wed, 02 Sep 2026 07:00:00 +0000</pubDate>'
        '<description>Healthy body</description>'
        '</item></channel></rss>'
    ).encode("utf-8")


def _treasury_index() -> bytes:
    return f'<html><body><a href="{TREASURY_DETAIL}">release</a></body></html>'.encode("utf-8")


def _treasury_detail(*, include_timestamp: bool = True) -> bytes:
    timestamp = (
        '<div class="field field--name-field-news-publication-date field--type-datetime">'
        '<time datetime="2026-09-01T06:20:00-04:00" class="datetime">September 1, 2026</time>'
        '</div>'
        if include_timestamp
        else ""
    )
    return (
        '<html><head><meta property="og:title" content="Treasury official release" /></head><body>'
        + timestamp
        + '<div class="field field--name-field-news-body field--type-text-long">'
        '<p>Official Treasury body.</p></div></body></html>'
    ).encode("utf-8")


def test_default_live_set_adds_treasury_but_not_timestamp_unproven_ofac() -> None:
    assert TREASURY_ID in LIVE_OFFICIAL_SOURCE_IDS
    assert OFAC_ID not in LIVE_OFFICIAL_SOURCE_IDS
    assert len(LIVE_OFFICIAL_SOURCE_IDS) == 12


def test_treasury_success_is_composed_with_existing_healthy_source(tmp_path: Path) -> None:
    payloads = {
        RSS_URL: _rss(),
        TREASURY_INDEX: _treasury_index(),
        TREASURY_DETAIL: _treasury_detail(),
    }

    def opener(request, timeout):
        return _Response(payloads[request.full_url], request.full_url)

    result = _acquire_official_sources(
        registry_path=_registry(tmp_path),
        source_ids=(RSS_ID, TREASURY_ID),
        opener=opener,
        now_fn=lambda: NOW,
        timeout_seconds=10.0,
    )

    assert [item.source_id for item in result.source_results] == [RSS_ID, TREASURY_ID]
    assert result.ok_source_count == 2
    assert result.failures == ()
    treasury = result.source_results[1]
    assert len(treasury.records) == 1
    record = treasury.records[0]
    assert record.source_reference == TREASURY_DETAIL
    assert record.published_at.isoformat() == "2026-09-01T06:20:00-04:00"
    assert record.available_at == record.published_at
    assert record.ingested_at == NOW


def test_treasury_timestamp_failure_does_not_poison_existing_healthy_source(tmp_path: Path) -> None:
    payloads = {
        RSS_URL: _rss(),
        TREASURY_INDEX: _treasury_index(),
        TREASURY_DETAIL: _treasury_detail(include_timestamp=False),
    }

    def opener(request, timeout):
        return _Response(payloads[request.full_url], request.full_url)

    result = _acquire_official_sources(
        registry_path=_registry(tmp_path),
        source_ids=(RSS_ID, TREASURY_ID),
        opener=opener,
        now_fn=lambda: NOW,
        timeout_seconds=10.0,
    )

    assert result.source_results[0].source_id == RSS_ID
    assert result.source_results[0].quality_status == "OK"
    assert len(result.source_results[0].records) == 1
    treasury = result.source_results[1]
    assert treasury.source_id == TREASURY_ID
    assert treasury.quality_status == "TIMESTAMP_UNPROVABLE"
    assert treasury.records == ()
    assert len(result.failures) == 1
