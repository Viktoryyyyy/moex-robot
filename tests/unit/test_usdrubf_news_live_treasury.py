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
LATEST_URL = "https://home.treasury.gov/news/press-releases/sb0599"
STALE_URL = "https://home.treasury.gov/news/press-releases/jy2400"


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


def _index(*, include_cross_host: bool = False) -> bytes:
    malicious = (
        '<a href="https://example.com/news/press-releases/evil">malicious release</a>'
        if include_cross_host
        else ""
    )
    return (
        '<html><body>'
        '<nav><a href="/news/press-releases/readouts">Readouts</a>'
        f'<a href="{STALE_URL}">Featured stale release</a>'
        f'<a href="{STALE_URL}">duplicate stale release</a></nav>'
        f'{malicious}'
        '<section><a href="/news/press-releases/statements-remarks">Statements</a>'
        f'<a href="{LATEST_URL}">Treasury Releases CFIUS Annual Report for 2025</a>'
        f'<a href="{LATEST_URL}">duplicate current release</a></section>'
        '</body></html>'
    ).encode("utf-8")


def _detail(title: str, timestamp: str) -> bytes:
    return (
        '<html><body>'
        '<nav><time datetime="2025-01-01T00:00:00Z">navigation</time></nav>'
        f'<main><h1>{title}</h1><time datetime="{timestamp}">published</time>'
        '<p>WASHINGTON, D.C. – Treasury publication body.</p>'
        '<p>Additional official detail.</p></main>'
        '</body></html>'
    ).encode("utf-8")


def _responses() -> dict[str, _Response]:
    return {
        INDEX_URL: _Response(_index(), INDEX_URL),
        STALE_URL: _Response(_detail("Featured stale release", "2026-06-23T14:30:00Z"), STALE_URL),
        LATEST_URL: _Response(
            _detail("Treasury Releases CFIUS Annual Report for 2025", "2026-08-07T20:39:09Z"),
            LATEST_URL,
        ),
    }


def test_registry_binding_is_loaded_from_stage12a_contract(tmp_path: Path) -> None:
    binding = load_treasury_binding(registry_path=_registry(tmp_path))
    assert binding.source_id == "us_treasury_press_releases"
    assert binding.source_tier == "OFFICIAL_PRIMARY"
    assert binding.index_url == INDEX_URL
    assert binding.allowed_host == "home.treasury.gov"


def test_registry_rejects_non_ready_treasury_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not Stage 12B ready"):
        load_treasury_binding(registry_path=_registry(tmp_path, status="BLOCKED_PENDING_TIMESTAMP"))


def test_upstream_listing_pattern_dedupes_skips_hubs_and_sorts_by_detail_timestamp(
    tmp_path: Path,
) -> None:
    responses = _responses()
    calls: list[str] = []

    def opener(request, timeout):
        calls.append(request.full_url)
        return responses[request.full_url]

    now = datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc)
    result = fetch_treasury_press_releases(
        registry_path=_registry(tmp_path),
        opener=opener,
        now_fn=lambda: now,
        max_detail_pages=1,
        max_candidate_pages=10,
    )

    assert result.quality_status == "OK"
    assert result.candidate_count == 2
    assert result.future_items_skipped == 0
    assert calls == [INDEX_URL, STALE_URL, LATEST_URL]
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source_reference == LATEST_URL
    assert record.published_at.isoformat() == "2026-08-07T20:39:09+00:00"
    assert record.available_at == record.published_at
    assert record.ingested_at == now
    assert record.headline == "Treasury Releases CFIUS Annual Report for 2025"
    assert record.body.startswith("WASHINGTON, D.C.")


def test_all_candidates_are_returned_newest_first_when_within_record_limit(tmp_path: Path) -> None:
    responses = _responses()

    def opener(request, timeout):
        return responses[request.full_url]

    result = fetch_treasury_press_releases(
        registry_path=_registry(tmp_path),
        opener=opener,
        now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
        max_detail_pages=2,
        max_candidate_pages=10,
    )
    assert [record.source_reference for record in result.records] == [LATEST_URL, STALE_URL]


def test_candidate_set_over_bound_aborts_parser_immediately(tmp_path: Path) -> None:
    links = (
        "".join(f'<a href="/news/press-releases/item-{i}">release {i}</a>' for i in range(3))
        + '<a href="https://example.com/news/press-releases/evil">later malicious link</a>'
    ).encode("utf-8")
    calls: list[str] = []

    def opener(request, timeout):
        calls.append(request.full_url)
        return _Response(links, request.full_url)

    with pytest.raises(
        TreasuryAcquisitionError,
        match="candidate set exceeds bounded detail scan limit",
    ) as exc_info:
        fetch_treasury_press_releases(
            registry_path=_registry(tmp_path),
            opener=opener,
            now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
            max_detail_pages=1,
            max_candidate_pages=2,
        )
    assert exc_info.value.code == "SOURCE_INVALID"
    assert calls == [INDEX_URL]


def test_index_without_release_candidates_is_invalid(tmp_path: Path) -> None:
    def opener(request, timeout):
        return _Response(b'<html><body><a href="/news/press-releases/readouts">Readouts</a></body></html>', request.full_url)

    with pytest.raises(TreasuryAcquisitionError) as exc_info:
        fetch_treasury_press_releases(
            registry_path=_registry(tmp_path),
            opener=opener,
            now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
            max_detail_pages=1,
        )
    assert exc_info.value.code == "SOURCE_INVALID"


def test_detail_without_main_timestamp_is_timestamp_unprovable(tmp_path: Path) -> None:
    index = f'<a href="{LATEST_URL}">release</a>'.encode("utf-8")
    responses = {
        INDEX_URL: _Response(index, INDEX_URL),
        LATEST_URL: _Response(b'<html><body><h1>Treasury Release</h1><p>No timestamp.</p></body></html>', LATEST_URL),
    }

    def opener(request, timeout):
        return responses[request.full_url]

    with pytest.raises(TreasuryAcquisitionError) as exc_info:
        fetch_treasury_press_releases(
            registry_path=_registry(tmp_path),
            opener=opener,
            now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
            max_detail_pages=1,
        )
    assert exc_info.value.code == "TIMESTAMP_UNPROVABLE"


def test_future_detail_is_not_exposed(tmp_path: Path) -> None:
    index = f'<a href="{LATEST_URL}">release</a>'.encode("utf-8")
    responses = {
        INDEX_URL: _Response(index, INDEX_URL),
        LATEST_URL: _Response(_detail("Future release", "2026-08-12T20:39:09Z"), LATEST_URL),
    }

    def opener(request, timeout):
        return responses[request.full_url]

    result = fetch_treasury_press_releases(
        registry_path=_registry(tmp_path),
        opener=opener,
        now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
        max_detail_pages=1,
    )
    assert result.records == ()
    assert result.future_items_skipped == 1
    assert result.candidate_count == 1


def test_cross_host_release_link_is_rejected(tmp_path: Path) -> None:
    def opener(request, timeout):
        return _Response(_index(include_cross_host=True), request.full_url)

    with pytest.raises(TreasuryAcquisitionError) as exc_info:
        fetch_treasury_press_releases(
            registry_path=_registry(tmp_path),
            opener=opener,
            now_fn=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=timezone.utc),
            max_detail_pages=1,
        )
    assert exc_info.value.code == "SOURCE_INVALID"
