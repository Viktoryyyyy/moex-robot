from __future__ import annotations

from datetime import datetime, timezone

from src.moex_research.intelligence.usdrubf_news_live_rss import (
    fetch_official_rss_batch,
    load_first_slice_bindings,
)


SOURCE_ID = "whitehouse_releases"
FEED_URL = "https://www.whitehouse.gov/releases/feed/"
ITEM_URL = "https://www.whitehouse.gov/releases/example-release/"


class _Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return FEED_URL


def test_whitehouse_releases_uses_registered_official_rss_binding() -> None:
    binding = load_first_slice_bindings(source_ids=(SOURCE_ID,))[0]
    assert binding.source_id == SOURCE_ID
    assert binding.source_tier == "OFFICIAL_PRIMARY"
    assert binding.feed_url == FEED_URL
    assert binding.allowed_host == "www.whitehouse.gov"


def test_whitehouse_releases_reuses_generic_rss_adapter() -> None:
    payload = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>The White House Releases</title>
    <link>https://www.whitehouse.gov/releases/</link>
    <description>Official White House releases</description>
    <item>
      <title>Example release</title>
      <link>{ITEM_URL}</link>
      <pubDate>Mon, 10 Aug 2026 18:06:42 +0000</pubDate>
      <description>Official release summary.</description>
    </item>
  </channel>
</rss>
""".encode("utf-8")

    def opener(request, timeout):
        assert request.full_url == FEED_URL
        return _Response(payload)

    result = fetch_official_rss_batch(
        source_ids=(SOURCE_ID,),
        opener=opener,
        now_fn=lambda: datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc),
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
