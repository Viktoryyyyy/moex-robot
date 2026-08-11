from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlparse

from src.moex_research.intelligence.usdrubf_news_live_rss import (
    FIRST_SLICE_SOURCE_IDS,
    LIVE_RSS_SOURCE_IDS,
    fetch_official_rss_batch,
    load_first_slice_bindings,
)


SOURCE_ID = "whitehouse_releases"
FEED_URL = "https://www.whitehouse.gov/releases/feed/"
ITEM_URL = "https://www.whitehouse.gov/releases/example-release/"
NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)


class _Response:
    def __init__(self, payload: bytes, url: str) -> None:
        self.payload = payload
        self.url = url

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.url


def _rss_payload(*, item_url: str, title: str = "Example release") -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Official publisher feed</title>
    <link>{item_url}</link>
    <description>Official publisher feed</description>
    <item>
      <title>{title}</title>
      <link>{item_url}</link>
      <pubDate>Mon, 10 Aug 2026 18:06:42 +0000</pubDate>
      <description>Official release summary.</description>
    </item>
  </channel>
</rss>
""".encode("utf-8")


def test_whitehouse_releases_uses_registered_official_rss_binding() -> None:
    binding = load_first_slice_bindings(source_ids=(SOURCE_ID,))[0]
    assert binding.source_id == SOURCE_ID
    assert binding.source_tier == "OFFICIAL_PRIMARY"
    assert binding.feed_url == FEED_URL
    assert binding.allowed_host == "www.whitehouse.gov"


def test_whitehouse_releases_reuses_generic_rss_adapter() -> None:
    def opener(request, timeout):
        assert request.full_url == FEED_URL
        return _Response(_rss_payload(item_url=ITEM_URL), FEED_URL)

    result = fetch_official_rss_batch(
        source_ids=(SOURCE_ID,),
        opener=opener,
        now_fn=lambda: NOW,
    )

    assert result.ok_source_count == 1
    assert result.failures == ()
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source_id == SOURCE_ID
    assert record.source_reference == ITEM_URL
    assert record.published_at.isoformat() == "2026-08-10T18:06:42+00:00"
    assert record.available_at == record.published_at
    assert record.headline == "Example release"
    assert record.body == "Official release summary."


def test_default_live_rss_batch_includes_whitehouse_without_changing_frozen_first_slice() -> None:
    assert LIVE_RSS_SOURCE_IDS == FIRST_SLICE_SOURCE_IDS + (SOURCE_ID,)

    def opener(request, timeout):
        parsed = urlparse(request.full_url)
        assert parsed.hostname
        item_url = f"https://{parsed.hostname}/example-release"
        return _Response(_rss_payload(item_url=item_url, title=parsed.hostname), request.full_url)

    result = fetch_official_rss_batch(
        opener=opener,
        now_fn=lambda: NOW,
    )

    assert tuple(item.source_id for item in result.source_results) == LIVE_RSS_SOURCE_IDS
    assert result.ok_source_count == len(LIVE_RSS_SOURCE_IDS)
    assert result.failures == ()
    assert any(record.source_id == SOURCE_ID for record in result.records)
