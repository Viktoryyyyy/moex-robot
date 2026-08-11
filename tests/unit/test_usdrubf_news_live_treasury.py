from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from src.moex_research.intelligence.usdrubf_news_live_treasury import (
    TreasuryAcquisitionError,
    fetch_treasury_press_releases,
    load_treasury_binding,
)


INDEX_URL = "https://home.treasury.gov/news/press-releases"
DETAIL_URL = "https://home.treasury.gov/news/press-releases/sb0599"
FEATURED_URL = "https://home.treasury.gov/news/press-releases/jy2400"


class _Response:
    def __init__(self, payload: bytes, url: str) -> None:
        self.payload = payload
        self.url = url

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.url


def _registry(tmp_path: Path, *, status: str = "READY_CANDIDATE") -> Path:
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "primary_sources": [
                    {
                        "source_id": "us_treasury_press_releases",
                        "group": "US_TREASURY_OFAC",
                        "tier": "OFFICIAL_PRIMARY",
                        "transport": "HTML_INDEX",
                        "references": [INDEX_URL],
                        "topics": ["SANCTIONS"],
                        "stage12b_status": status,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def _index(detail_url: str = DETAIL_URL, nav_detail_url: str = FEATURED_URL) -> bytes:
    return (
        '<html><body>'
        '<nav><h2>Featured Stories</h2>'
        f'<a href="{nav_detail_url}">stale navigation release</a>'
        '<h2>Press Releases</h2>'
        f'<a href="{nav_detail_url}">mega-menu release</a></nav>'
        '<main><h1>Press Releases</h1>'
        f'<a href="{detail_url}">Treasury Releases CFIUS Annual Report for 2025</a>'
        f'<a href="{detail_url}">duplicate list link</a>'
        '</main></body></html>'
    ).encode("utf-8")


def _detail(timestamp: str = "2026-08-07T20:39:09Z") -> bytes:
    return (
        '<html><body>'
        '<nav><time datetime="2026-06-23T14:30:00Z">June 23</time></nav>'
        '<main><h1>Treasury Releases CFIUS Annual Report for 2025</h1>'
        f'<time datetime="{timestamp}">August 7, 2026</time>'
        '<p>WASHINGTON, D.C. – Today, Treasury released its annual report.</p>'
        '<p>The report highlights key indicators.</p>'
        '</main></body></html>'
    ).encode("utf-8")


def test_registry_binding_is_loaded_from_stage12a_contract(tmp_path: Path) -> None:
    binding = load_treasury_binding(registry_path=_registry(tmp_path))
    assert binding.source_id == "us_treasury_press_releases"
    assert binding.source_tier == "OFFICIAL_PRIMARY"
    assert binding.index_url == INDEX_URL
    assert binding.allowed_host == "home.treasury.gov"


def test_registry_rejects_non_ready_treasury_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not Stage 12B ready"):
        load_treasury_binding(
            registry_path=_registry(tmp_path, status="BLOCKED_PENDING_TIMESTAMP")
        )


def test_live_acquisition_binds_first_time_after_main_h1(tmp_path: Path) -> None:
    responses = {INDEX_URL: _index(), DETAIL_URL: _detail()}

    def opener(request, timeout):
        return _Response(responses[request.full_url], request.full_url)

    now = datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc)
    result = fetch_treasury_press_releases(
        registry_path=_registry(tmp_path),
        opener=opener,
        now_fn=lambda: now,
        max_detail_pages=1,
    )

    assert result.quality_status == "OK"
    assert result.future_items_skipped == 0
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source_id == "us_treasury_press_releases"
    assert record.source_reference == DETAIL_URL
    assert record.published_at.isoformat() == "2026-08-07T20:39:09+00:00"
    assert record.available_at == record.published_at
    assert record.ingested_at == now
    assert record.headline == "Treasury Releases CFIUS Annual Report for 2025"
    assert record.body.startswith("WASHINGTON, D.C.")


def test_navigation_release_links_are_ignored_before_primary_listing(tmp_path: Path) -> None:
    calls: list[str] = []
    responses = {INDEX_URL: _index(), DETAIL_URL: _detail()}

    def opener(request, timeout):
        calls.append(request.full_url)
        return _Response(responses[request.full_url], request.full_url)

    result = fetch_treasury_press_releases(
        registry_path=_registry(tmp_path),
        opener=opener,
        now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
        max_detail_pages=1,
    )
    assert calls == [INDEX_URL, DETAIL_URL]
    assert result.records[0].source_reference == DETAIL_URL


def test_duplicate_primary_listing_links_are_fetched_once(tmp_path: Path) -> None:
    calls: list[str] = []
    responses = {INDEX_URL: _index(), DETAIL_URL: _detail()}

    def opener(request, timeout):
        calls.append(request.full_url)
        return _Response(responses[request.full_url], request.full_url)

    fetch_treasury_press_releases(
        registry_path=_registry(tmp_path),
        opener=opener,
        now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
        max_detail_pages=10,
    )
    assert calls == [INDEX_URL, DETAIL_URL]


def test_index_without_main_press_releases_h1_is_invalid(tmp_path: Path) -> None:
    bad_index = (
        '<html><body><nav><h2>Press Releases</h2>'
        f'<a href="{FEATURED_URL}">navigation only</a></nav></body></html>'
    ).encode("utf-8")

    def opener(request, timeout):
        return _Response(bad_index, request.full_url)

    with pytest.raises(TreasuryAcquisitionError) as exc_info:
        fetch_treasury_press_releases(
            registry_path=_registry(tmp_path),
            opener=opener,
            now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
            max_detail_pages=1,
        )
    assert exc_info.value.code == "SOURCE_INVALID"


def test_detail_without_main_timestamp_is_timestamp_unprovable(tmp_path: Path) -> None:
    bad_detail = (
        b'<html><body><h1>Treasury Release</h1><p>No publication time.</p></body></html>'
    )
    responses = {INDEX_URL: _index(), DETAIL_URL: bad_detail}

    def opener(request, timeout):
        return _Response(responses[request.full_url], request.full_url)

    with pytest.raises(TreasuryAcquisitionError) as exc_info:
        fetch_treasury_press_releases(
            registry_path=_registry(tmp_path),
            opener=opener,
            now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
            max_detail_pages=1,
        )
    assert exc_info.value.code == "TIMESTAMP_UNPROVABLE"


def test_future_detail_is_not_exposed(tmp_path: Path) -> None:
    responses = {
        INDEX_URL: _index(),
        DETAIL_URL: _detail("2026-08-12T20:39:09Z"),
    }

    def opener(request, timeout):
        return _Response(responses[request.full_url], request.full_url)

    result = fetch_treasury_press_releases(
        registry_path=_registry(tmp_path),
        opener=opener,
        now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
        max_detail_pages=1,
    )
    assert result.records == ()
    assert result.future_items_skipped == 1


def test_cross_host_release_link_is_rejected(tmp_path: Path) -> None:
    malicious = _index("https://example.com/news/press-releases/sb0599")

    def opener(request, timeout):
        return _Response(malicious, request.full_url)

    with pytest.raises(TreasuryAcquisitionError) as exc_info:
        fetch_treasury_press_releases(
            registry_path=_registry(tmp_path),
            opener=opener,
            now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
            max_detail_pages=1,
        )
    assert exc_info.value.code == "SOURCE_INVALID"
