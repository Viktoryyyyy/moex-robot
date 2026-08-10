from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from src.moex_research.intelligence.usdrubf_news_live_rss import (
    FIRST_SLICE_SOURCE_IDS,
    RssFeedBinding,
    fetch_official_rss_batch,
    fetch_rss_source,
    load_first_slice_bindings,
)


class _Response:
    def __init__(self, payload: bytes, url: str = "https://www.cbr.ru/rss/RssPress") -> None:
        self.payload = payload
        self.url = url

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.url


def _rss(*items: str) -> bytes:
    joined = "".join(items)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel><title>test</title>'
        f"{joined}</channel></rss>"
    ).encode("utf-8")


def _item(
    *,
    title: str = "Policy update",
    link: str = "https://www.cbr.ru/press/event/",
    pub_date: str = "Mon, 10 Aug 2026 10:00:00 +0000",
    description: str = "<b>Details</b> &amp; context",
) -> str:
    return (
        "<item>"
        f"<title>{title}</title>"
        f"<link>{link}</link>"
        f"<pubDate>{pub_date}</pubDate>"
        f"<description><![CDATA[{description}]]></description>"
        "</item>"
    )


def _registry(tmp_path: Path, *, sources: list[dict]) -> Path:
    path = tmp_path / "registry.json"
    path.write_text(json.dumps({"primary_sources": sources}), encoding="utf-8")
    return path


def _source(source_id: str, url: str) -> dict:
    return {
        "source_id": source_id,
        "tier": "OFFICIAL_PRIMARY",
        "transport": "RSS",
        "references": [url],
        "stage12b_status": "READY_CANDIDATE",
    }


def test_canonical_registry_selects_exact_first_slice() -> None:
    bindings = load_first_slice_bindings()
    assert tuple(item.source_id for item in bindings) == FIRST_SLICE_SOURCE_IDS
    assert all(item.source_tier == "OFFICIAL_PRIMARY" for item in bindings)
    assert all(item.feed_url.startswith("https://") for item in bindings)


def test_registry_rejects_non_ready_source(tmp_path: Path) -> None:
    path = _registry(
        tmp_path,
        sources=[
            {
                **_source("candidate", "https://example.com/feed.xml"),
                "stage12b_status": "BLOCKED_PENDING_ROUTE",
            }
        ],
    )
    with pytest.raises(ValueError, match="not Stage 12B ready"):
        load_first_slice_bindings(registry_path=path, source_ids=("candidate",))


def test_fetch_rss_source_builds_source_bound_records_and_strips_html() -> None:
    binding = RssFeedBinding(
        source_id="cbr_press_rss",
        source_tier="OFFICIAL_PRIMARY",
        feed_url="https://www.cbr.ru/rss/RssPress",
        allowed_host="www.cbr.ru",
    )
    payload = _rss(_item())
    now = datetime(2026, 8, 10, 11, 0, tzinfo=timezone.utc)

    result = fetch_rss_source(
        binding,
        opener=lambda request, timeout: _Response(payload, request.full_url),
        now_fn=lambda: now,
    )

    assert result.quality_status == "OK"
    assert result.future_items_skipped == 0
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source_id == "cbr_press_rss"
    assert record.source_reference == "https://www.cbr.ru/press/event/"
    assert record.published_at.isoformat() == "2026-08-10T10:00:00+00:00"
    assert record.available_at == record.published_at
    assert record.ingested_at == now
    assert record.body == "Details & context"


def test_fetch_rss_source_rejects_cross_host_redirect() -> None:
    binding = RssFeedBinding(
        source_id="cbr_press_rss",
        source_tier="OFFICIAL_PRIMARY",
        feed_url="https://www.cbr.ru/rss/RssPress",
        allowed_host="www.cbr.ru",
    )
    payload = _rss(_item())
    now = datetime(2026, 8, 10, 11, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="final response host does not match"):
        fetch_rss_source(
            binding,
            opener=lambda request, timeout: _Response(payload, "https://evil.example/feed.xml"),
            now_fn=lambda: now,
        )


def test_fetch_rss_source_rejects_https_to_http_redirect() -> None:
    binding = RssFeedBinding(
        source_id="cbr_press_rss",
        source_tier="OFFICIAL_PRIMARY",
        feed_url="https://www.cbr.ru/rss/RssPress",
        allowed_host="www.cbr.ru",
    )
    payload = _rss(_item())
    now = datetime(2026, 8, 10, 11, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="final response URL must be HTTPS"):
        fetch_rss_source(
            binding,
            opener=lambda request, timeout: _Response(payload, "http://www.cbr.ru/rss/RssPress"),
            now_fn=lambda: now,
        )


def test_fetch_rss_source_rejects_off_domain_item_link() -> None:
    binding = RssFeedBinding(
        source_id="cbr_press_rss",
        source_tier="OFFICIAL_PRIMARY",
        feed_url="https://www.cbr.ru/rss/RssPress",
        allowed_host="www.cbr.ru",
    )
    payload = _rss(_item(link="https://example.com/not-cbr"))
    now = datetime(2026, 8, 10, 11, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="host does not match"):
        fetch_rss_source(
            binding,
            opener=lambda request, timeout: _Response(payload, request.full_url),
            now_fn=lambda: now,
        )


def test_fetch_rss_source_requires_provable_publication_timestamp() -> None:
    binding = RssFeedBinding(
        source_id="cbr_press_rss",
        source_tier="OFFICIAL_PRIMARY",
        feed_url="https://www.cbr.ru/rss/RssPress",
        allowed_host="www.cbr.ru",
    )
    payload = _rss(
        "<item><title>Update</title><link>https://www.cbr.ru/press/event/</link></item>"
    )
    now = datetime(2026, 8, 10, 11, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="missing publication timestamp"):
        fetch_rss_source(
            binding,
            opener=lambda request, timeout: _Response(payload, request.full_url),
            now_fn=lambda: now,
        )


def test_future_feed_item_is_not_exposed_as_record() -> None:
    binding = RssFeedBinding(
        source_id="cbr_press_rss",
        source_tier="OFFICIAL_PRIMARY",
        feed_url="https://www.cbr.ru/rss/RssPress",
        allowed_host="www.cbr.ru",
    )
    payload = _rss(_item(pub_date="Mon, 10 Aug 2026 12:00:00 +0000"))
    now = datetime(2026, 8, 10, 11, 0, tzinfo=timezone.utc)

    result = fetch_rss_source(
        binding,
        opener=lambda request, timeout: _Response(payload, request.full_url),
        now_fn=lambda: now,
    )
    assert result.records == ()
    assert result.future_items_skipped == 1


def test_batch_keeps_other_sources_when_one_source_is_unavailable(tmp_path: Path) -> None:
    path = _registry(
        tmp_path,
        sources=[
            _source("one", "https://one.example/feed.xml"),
            _source("two", "https://two.example/feed.xml"),
        ],
    )
    payload = _rss(
        _item(
            link="https://one.example/event",
            pub_date="Mon, 10 Aug 2026 10:00:00 +0000",
        )
    )
    now = datetime(2026, 8, 10, 11, 0, tzinfo=timezone.utc)

    def opener(request, timeout):
        if request.full_url.startswith("https://two.example/"):
            raise OSError("network down")
        return _Response(payload, request.full_url)

    result = fetch_official_rss_batch(
        registry_path=path,
        source_ids=("one", "two"),
        opener=opener,
        now_fn=lambda: now,
    )

    assert result.ok_source_count == 1
    assert len(result.records) == 1
    assert len(result.failures) == 1
    assert result.failures[0].source_id == "two"
    assert result.failures[0].quality_status == "SOURCE_UNAVAILABLE"


def test_batch_does_not_accept_x_source(tmp_path: Path) -> None:
    path = _registry(
        tmp_path,
        sources=[
            {
                "source_id": "x_reuters",
                "tier": "X_WIRE_DISCOVERY",
                "transport": "X",
                "references": ["https://x.com/Reuters"],
                "stage12b_status": "READY_CANDIDATE",
            }
        ],
    )
    with pytest.raises(ValueError):
        load_first_slice_bindings(registry_path=path, source_ids=("x_reuters",))
