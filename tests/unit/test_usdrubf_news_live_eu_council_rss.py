from __future__ import annotations

from datetime import datetime, timezone

from src.moex_research.intelligence.usdrubf_news_live_rss import (
    FIRST_SLICE_SOURCE_IDS,
    LIVE_RSS_SOURCE_IDS,
    fetch_official_rss_batch,
    load_first_slice_bindings,
)


SOURCE_ID = "eu_council_press_releases"
FEED_URL = "https://www.consilium.europa.eu/en/rss/pressreleases.ashx"
ITEM_URL = (
    "https://www.consilium.europa.eu/en/press/press-releases/2026/08/11/"
    "example-eu-council-release/"
)
NOW = datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc)


class _Response:
    def __init__(self, payload: bytes, url: str) -> None:
        self.payload = payload
        self.url = url

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.url


def _rss_payload() -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>European Council press releases</title>
    <link>https://www.consilium.europa.eu/en/press/press-releases/</link>
    <description>European Council press releases</description>
    <item>
      <guid>150141</guid>
      <link>{ITEM_URL}</link>
      <title>Example EU Council release</title>
      <description>Official EU Council release summary.</description>
      <atom:updated>2026-08-11T13:40:00Z</atom:updated>
      <category>Foreign affairs</category>
    </item>
  </channel>
</rss>
""".encode("utf-8")


def test_eu_council_uses_registered_official_rss_binding() -> None:
    binding = load_first_slice_bindings(source_ids=(SOURCE_ID,))[0]
    assert binding.source_id == SOURCE_ID
    assert binding.source_tier == "OFFICIAL_PRIMARY"
    assert binding.feed_url == FEED_URL
    assert binding.allowed_host == "www.consilium.europa.eu"


def test_eu_council_atom_updated_reuses_generic_rss_adapter() -> None:
    def opener(request, timeout):
        assert request.full_url == FEED_URL
        return _Response(_rss_payload(), FEED_URL)

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
    assert record.published_at.isoformat() == "2026-08-11T13:40:00+00:00"
    assert record.available_at == record.published_at
    assert record.headline == "Example EU Council release"
    assert record.body == "Official EU Council release summary."


def test_default_live_rss_set_includes_eu_council_after_whitehouse() -> None:
    assert LIVE_RSS_SOURCE_IDS[: len(FIRST_SLICE_SOURCE_IDS)] == FIRST_SLICE_SOURCE_IDS
    assert "whitehouse_releases" in LIVE_RSS_SOURCE_IDS
    assert SOURCE_ID in LIVE_RSS_SOURCE_IDS
    assert LIVE_RSS_SOURCE_IDS.index(SOURCE_ID) > LIVE_RSS_SOURCE_IDS.index("whitehouse_releases")
