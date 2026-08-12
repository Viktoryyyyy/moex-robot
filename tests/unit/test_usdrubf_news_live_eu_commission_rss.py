from __future__ import annotations

from datetime import datetime, timezone

from src.moex_research.intelligence.usdrubf_news_live_rss import (
    FIRST_SLICE_SOURCE_IDS,
    LIVE_RSS_SOURCE_IDS,
    fetch_official_rss_batch,
    load_first_slice_bindings,
)


SOURCE_ID = "eu_commission_news"
FEED_URL = "https://commission.europa.eu/node/29665/rss_en"
COMMISSION_URL = (
    "https://commission.europa.eu/news-and-media/news/"
    "new-packaging-rules-less-waste-and-easier-recycling-2026-08-12_en"
)
PRESSCORNER_URL = "https://ec.europa.eu/commission/presscorner/detail/en/ip_26_1680"
CIVIL_PROTECTION_URL = (
    "https://civil-protection-humanitarian-aid.ec.europa.eu/news-stories/news/"
    "eu-deploys-emergency-assistance-2026-06-26_en"
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


def _item(title: str, link: str) -> str:
    return (
        "<item>"
        f"<title>{title}</title>"
        f"<link>{link}</link>"
        "<description>Official European Commission publication.</description>"
        "<pubDate>Wed, 12 Aug 2026 10:06:47 +0200</pubDate>"
        "</item>"
    )


def _rss_payload(*items: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel>'
        '<title>European Commission highlighted news</title>'
        '<link>https://commission.europa.eu/news-and-media/highlighted-news_en</link>'
        '<description>European Commission highlighted news</description>'
        + "".join(items)
        + "</channel></rss>"
    ).encode("utf-8")


def test_eu_commission_uses_registered_official_rss_binding_and_explicit_item_hosts() -> None:
    binding = load_first_slice_bindings(source_ids=(SOURCE_ID,))[0]
    assert binding.source_id == SOURCE_ID
    assert binding.source_tier == "OFFICIAL_SECONDARY"
    assert binding.feed_url == FEED_URL
    assert binding.allowed_host == "commission.europa.eu"
    assert binding.additional_allowed_hosts == (
        "ec.europa.eu",
        "civil-protection-humanitarian-aid.ec.europa.eu",
    )
    assert binding.item_allowed_hosts == (
        "commission.europa.eu",
        "ec.europa.eu",
        "civil-protection-humanitarian-aid.ec.europa.eu",
    )


def test_eu_commission_generic_rss_adapter_accepts_only_registered_official_item_hosts() -> None:
    payload = _rss_payload(
        _item("Commission news", COMMISSION_URL),
        _item("Press corner release", PRESSCORNER_URL),
        _item("Civil protection release", CIVIL_PROTECTION_URL),
    )

    def opener(request, timeout):
        assert request.full_url == FEED_URL
        return _Response(payload, FEED_URL)

    result = fetch_official_rss_batch(
        source_ids=(SOURCE_ID,),
        opener=opener,
        now_fn=lambda: NOW,
    )

    assert result.ok_source_count == 1
    assert result.failures == ()
    assert len(result.records) == 3
    assert {record.source_reference for record in result.records} == {
        COMMISSION_URL,
        PRESSCORNER_URL,
        CIVIL_PROTECTION_URL,
    }
    assert all(record.source_id == SOURCE_ID for record in result.records)
    assert all(record.source_tier == "OFFICIAL_SECONDARY" for record in result.records)
    assert all(record.available_at == record.published_at for record in result.records)
    assert all(record.published_at.isoformat() == "2026-08-12T10:06:47+02:00" for record in result.records)


def test_eu_commission_unregistered_item_host_remains_fail_closed() -> None:
    payload = _rss_payload(_item("Untrusted mirror", "https://evil.example/eu-release"))

    result = fetch_official_rss_batch(
        source_ids=(SOURCE_ID,),
        opener=lambda request, timeout: _Response(payload, FEED_URL),
        now_fn=lambda: NOW,
    )

    assert result.ok_source_count == 0
    assert len(result.failures) == 1
    failure = result.failures[0]
    assert failure.source_id == SOURCE_ID
    assert failure.quality_status == "SOURCE_INVALID"
    assert "registered publisher allowlist" in (failure.error or "")


def test_default_live_rss_set_includes_eu_commission_after_eu_council() -> None:
    assert LIVE_RSS_SOURCE_IDS[: len(FIRST_SLICE_SOURCE_IDS)] == FIRST_SLICE_SOURCE_IDS
    assert "whitehouse_releases" in LIVE_RSS_SOURCE_IDS
    assert "eu_council_press_releases" in LIVE_RSS_SOURCE_IDS
    assert SOURCE_ID in LIVE_RSS_SOURCE_IDS
    assert LIVE_RSS_SOURCE_IDS.index(SOURCE_ID) > LIVE_RSS_SOURCE_IDS.index(
        "eu_council_press_releases"
    )
