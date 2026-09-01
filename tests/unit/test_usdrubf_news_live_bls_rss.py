from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.moex_research.intelligence.usdrubf_news_live_rss import (
    RssAcquisitionError,
    fetch_official_rss_batch,
    fetch_rss_source,
    load_first_slice_bindings,
)


BLS_SOURCE_IDS = (
    "bls_employment_situation_rss",
    "bls_cpi_rss",
)
BLS_FEED_URLS = {
    "bls_employment_situation_rss": "https://www.bls.gov/feed/empsit.rss",
    "bls_cpi_rss": "https://www.bls.gov/feed/cpi.rss",
}


class _Response:
    def __init__(self, payload: bytes, url: str) -> None:
        self.payload = payload
        self.url = url

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.url


def _atom_entry(
    *,
    title: str = "Employment Situation",
    link: str = "https://www.bls.gov/news.release/empsit.nr0.htm",
    published: str | None = "2026-08-07T08:30:00-04:00",
) -> bytes:
    timestamp = f"<updated>{published}</updated>" if published is not None else ""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        '<title>BLS News Release</title>'
        '<id>https://www.bls.gov/</id>'
        '<updated>2026-08-07T08:30:00-04:00</updated>'
        '<entry>'
        f'<title>{title}</title>'
        f'<link href="{link}" />'
        f'{timestamp}'
        '<summary>Official BLS release.</summary>'
        '</entry>'
        '</feed>'
    ).encode("utf-8")


def _bindings():
    return {
        binding.source_id: binding
        for binding in load_first_slice_bindings(source_ids=BLS_SOURCE_IDS)
    }


def test_bls_registry_keeps_documented_official_feed_routes() -> None:
    bindings = _bindings()
    assert set(bindings) == set(BLS_SOURCE_IDS)
    for source_id, expected_url in BLS_FEED_URLS.items():
        binding = bindings[source_id]
        assert binding.feed_url == expected_url
        assert binding.allowed_host == "www.bls.gov"
        assert binding.source_tier == "OFFICIAL_PRIMARY"


@pytest.mark.parametrize("source_id", BLS_SOURCE_IDS)
def test_bls_request_is_contact_identified_and_preserves_factual_times(source_id: str) -> None:
    binding = _bindings()[source_id]
    now = datetime(2026, 8, 7, 13, 0, tzinfo=timezone.utc)
    seen_user_agent: list[str | None] = []

    def opener(request, timeout):
        seen_user_agent.append(request.get_header("User-agent"))
        return _Response(_atom_entry(), request.full_url)

    result = fetch_rss_source(binding, opener=opener, now_fn=lambda: now)

    assert result.quality_status == "OK"
    assert len(result.records) == 1
    assert seen_user_agent == [
        "MOEX_Bot/rub-intelligence-stage12b1 (+https://github.com/Viktoryyyyy/moex-robot)"
    ]
    record = result.records[0]
    assert record.source_id == source_id
    assert record.source_tier == "OFFICIAL_PRIMARY"
    assert record.source_reference.startswith("https://www.bls.gov/news.release/")
    assert record.published_at.isoformat() == "2026-08-07T08:30:00-04:00"
    assert record.available_at == record.published_at
    assert record.ingested_at == now


def test_bls_missing_item_timestamp_fails_closed() -> None:
    binding = _bindings()["bls_employment_situation_rss"]
    now = datetime(2026, 8, 7, 13, 0, tzinfo=timezone.utc)

    with pytest.raises(RssAcquisitionError) as exc_info:
        fetch_rss_source(
            binding,
            opener=lambda request, timeout: _Response(_atom_entry(published=None), request.full_url),
            now_fn=lambda: now,
        )

    assert exc_info.value.code == "TIMESTAMP_UNPROVABLE"


def test_bls_future_publication_is_not_exposed_as_news() -> None:
    binding = _bindings()["bls_cpi_rss"]
    now = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
    payload = _atom_entry(
        title="Consumer Price Index",
        link="https://www.bls.gov/news.release/cpi.nr0.htm",
        published="2026-08-07T08:30:00-04:00",
    )

    result = fetch_rss_source(
        binding,
        opener=lambda request, timeout: _Response(payload, request.full_url),
        now_fn=lambda: now,
    )

    assert result.records == ()
    assert result.future_items_skipped == 1


def test_bls_source_failure_is_isolated_and_provenance_is_preserved() -> None:
    now = datetime(2026, 8, 7, 13, 0, tzinfo=timezone.utc)

    def opener(request, timeout):
        if request.full_url.endswith("/cpi.rss"):
            return _Response(_atom_entry(), "https://evil.example/cpi.rss")
        return _Response(_atom_entry(), request.full_url)

    result = fetch_official_rss_batch(
        source_ids=BLS_SOURCE_IDS,
        opener=opener,
        now_fn=lambda: now,
    )

    assert result.ok_source_count == 1
    assert len(result.failures) == 1
    assert result.failures[0].source_id == "bls_cpi_rss"
    assert result.failures[0].quality_status == "SOURCE_INVALID"
    assert len(result.records) == 1
    assert result.records[0].source_id == "bls_employment_situation_rss"
