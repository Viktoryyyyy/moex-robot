from __future__ import annotations

from datetime import datetime, timezone
import threading
import time

from src.moex_research.intelligence import usdrubf_news_live_treasury as treasury


def test_production_treasury_detail_pages_overlap_and_preserve_freshness_sort(monkeypatch):
    binding = treasury.TreasuryBinding(
        source_id=treasury.SOURCE_ID,
        source_tier="OFFICIAL_PRIMARY",
        index_url="https://home.treasury.gov/news/press-releases",
        allowed_host="home.treasury.gov",
    )
    monkeypatch.setattr(treasury, "load_treasury_binding", lambda **_kwargs: binding)

    detail_urls = [
        f"https://home.treasury.gov/news/press-releases/jy{i}"
        for i in range(1, 5)
    ]
    index_html = "<html><body>" + "".join(
        f'<a href="{url}">release</a>' for url in detail_urls
    ) + "</body></html>"

    active = 0
    max_active = 0
    lock = threading.Lock()

    def fake_request_html(url, **_kwargs):
        nonlocal active, max_active
        if url == binding.index_url:
            return index_html.encode("utf-8")
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.05)
            idx = detail_urls.index(url) + 1
            return (
                '<html><head><meta property="og:title" content="Release '
                + str(idx)
                + '"></head><body><div class="field--name-field-news-publication-date">'
                + '<time datetime="2026-09-0'
                + str(idx)
                + 'T12:00:00+00:00"></time></div>'
                + '<div class="field--name-field-news-body">Body '
                + str(idx)
                + '</div></body></html>'
            ).encode("utf-8")
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(treasury, "_request_html", fake_request_html)

    result = treasury.fetch_treasury_press_releases(
        opener=treasury.urlopen,
        now_fn=lambda: datetime(2026, 9, 5, tzinfo=timezone.utc),
        max_detail_pages=3,
        max_candidate_pages=4,
    )

    assert max_active > 1
    assert result.candidate_count == 4
    assert [record.headline for record in result.records] == [
        "Release 4",
        "Release 3",
        "Release 2",
    ]
    assert result.future_items_skipped == 0


def test_custom_opener_keeps_sequential_detail_scan(monkeypatch):
    binding = treasury.TreasuryBinding(
        source_id=treasury.SOURCE_ID,
        source_tier="OFFICIAL_PRIMARY",
        index_url="https://home.treasury.gov/news/press-releases",
        allowed_host="home.treasury.gov",
    )
    monkeypatch.setattr(treasury, "load_treasury_binding", lambda **_kwargs: binding)
    detail_urls = [
        "https://home.treasury.gov/news/press-releases/jy1",
        "https://home.treasury.gov/news/press-releases/jy2",
    ]
    index_html = "<html><body>" + "".join(
        f'<a href="{url}">release</a>' for url in detail_urls
    ) + "</body></html>"
    call_order = []

    def fake_request_html(url, **_kwargs):
        call_order.append(url)
        if url == binding.index_url:
            return index_html.encode("utf-8")
        idx = detail_urls.index(url) + 1
        return (
            '<html><head><meta property="og:title" content="Release '
            + str(idx)
            + '"></head><body><div class="field--name-field-news-publication-date">'
            + '<time datetime="2026-09-0'
            + str(idx)
            + 'T12:00:00+00:00"></time></div>'
            + '<div class="field--name-field-news-body">Body</div></body></html>'
        ).encode("utf-8")

    monkeypatch.setattr(treasury, "_request_html", fake_request_html)
    custom_opener = object()
    result = treasury.fetch_treasury_press_releases(
        opener=custom_opener,
        now_fn=lambda: datetime(2026, 9, 5, tzinfo=timezone.utc),
        max_detail_pages=2,
        max_candidate_pages=2,
    )

    assert result.quality_status == "OK"
    assert call_order == [binding.index_url] + detail_urls
