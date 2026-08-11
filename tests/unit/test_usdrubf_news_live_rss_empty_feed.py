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


def test_structurally_valid_empty_rss_is_ok_with_zero_records() -> None:
    payload = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<rss version="2.0"><channel><title>Moscow Exchange FX news</title></channel></rss>'
    )
    result = fetch_rss_source(
        _binding(),
        opener=lambda request, timeout: _Response(payload, request.full_url),
        now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
    )

    assert result.quality_status == "OK"
    assert result.records == ()
    assert result.future_items_skipped == 0


def test_arbitrary_empty_xml_remains_invalid() -> None:
    payload = b'<?xml version="1.0" encoding="utf-8"?><root />'

    with pytest.raises(ValueError, match="contains no items"):
        fetch_rss_source(
            _binding(),
            opener=lambda request, timeout: _Response(payload, request.full_url),
            now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
        )
