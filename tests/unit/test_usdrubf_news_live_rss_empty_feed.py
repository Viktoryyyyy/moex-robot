from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.moex_research.intelligence.usdrubf_news_live_rss import (
    RssFeedBinding,
    fetch_rss_source,
)


class _Response:
    def __init__(self, payload: bytes, url: str) -> None:
        self._payload = payload
        self._url = url

    def read(self, size: int = -1) -> bytes:
        return self._payload if size < 0 else self._payload[:size]

    def geturl(self) -> str:
        return self._url


def _binding() -> RssFeedBinding:
    return RssFeedBinding(
        source_id="moex_fx_news_rss",
        source_tier="OFFICIAL_PRIMARY",
        feed_url="https://www.moex.com/export/news.aspx?cat=210",
        allowed_host="www.moex.com",
    )


def _fetch(payload: bytes):
    return fetch_rss_source(
        _binding(),
        opener=lambda request, timeout: _Response(payload, request.full_url),
        now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
    )


def test_structurally_valid_empty_rss_is_ok_with_zero_records() -> None:
    payload = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<rss version="2.0"><channel>'
        b'<title>Moscow Exchange FX news</title>'
        b'<link>https://www.moex.com/</link>'
        b'<description>Official FX market news</description>'
        b'</channel></rss>'
    )
    result = _fetch(payload)

    assert result.quality_status == "OK"
    assert result.records == ()
    assert result.future_items_skipped == 0


def test_structurally_valid_empty_atom_is_ok_with_zero_records() -> None:
    payload = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<feed xmlns="http://www.w3.org/2005/Atom">'
        b'<title>Official feed</title>'
        b'<id>https://www.moex.com/feed</id>'
        b'<updated>2026-08-11T08:00:00Z</updated>'
        b'</feed>'
    )
    result = _fetch(payload)

    assert result.quality_status == "OK"
    assert result.records == ()


@pytest.mark.parametrize(
    "payload",
    (
        b'<?xml version="1.0" encoding="utf-8"?><root />',
        b'<?xml version="1.0" encoding="utf-8"?><rss><channel /></rss>',
        b'<?xml version="1.0" encoding="utf-8"?><feed />',
        b'<?xml version="1.0" encoding="utf-8"?><feed xmlns="https://evil.example/ns"><title>x</title><id>y</id><updated>2026-08-11T08:00:00Z</updated></feed>',
    ),
)
def test_malformed_or_non_feed_empty_xml_remains_invalid(payload: bytes) -> None:
    with pytest.raises(ValueError, match="contains no items"):
        _fetch(payload)
