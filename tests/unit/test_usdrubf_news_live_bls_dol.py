from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from src.moex_research.intelligence.usdrubf_news_live_bls_dol import (
    BLS_DOL_SOURCE_IDS,
    DOL_ECONOMIC_DATA_INDEX_URL,
    fetch_bls_dol_mirror_batch,
)
from src.moex_research.intelligence.usdrubf_news_live_pipeline import (
    LIVE_OFFICIAL_SOURCE_IDS,
)
from src.moex_research.intelligence.usdrubf_news_live_rss import LIVE_RSS_SOURCE_IDS


EMPLOYMENT_ID = "bls_employment_situation_dol_mirror"
CPI_ID = "bls_cpi_dol_mirror"
EMPLOYMENT_URL = "https://www.dol.gov/newsroom/economicdata/empsit_08072026.pdf"
CPI_URL = "https://www.dol.gov/newsroom/economicdata/cpi_08122026.pdf"


class _Response:
    def __init__(self, payload: bytes, url: str, content_type: str) -> None:
        self.payload = payload
        self.url = url
        self.headers = {"Content-Type": content_type}

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.url


class _Page:
    def __init__(self, text: str) -> None:
        self.text = text

    def extract_text(self) -> str:
        return self.text


class _Reader:
    def __init__(self, stream) -> None:
        raw = stream.getvalue()
        if b"EMPLOYMENT" in raw:
            text = (
                "Transmission of material in this news release is embargoed until "
                "8:30 a.m. (ET) Friday, August 7, 2026. "
                "THE EMPLOYMENT SITUATION - JULY 2026. "
                "Total nonfarm payroll employment changed."
            )
        else:
            text = (
                "Transmission of material in this release is embargoed until "
                "8:30 a.m. (ET) Wednesday, August 12, 2026. "
                "CONSUMER PRICE INDEX - JULY 2026. "
                "The Consumer Price Index changed."
            )
        self.pages = [_Page(text)]


def _index(*links: str) -> bytes:
    body = "".join(f'<a href="{link}">release</a>' for link in links)
    return f"<html><body>{body}</body></html>".encode("utf-8")


def _healthy_opener(request, timeout):
    if request.full_url == DOL_ECONOMIC_DATA_INDEX_URL:
        return _Response(
            _index(
                "/newsroom/economicdata/empsit_08072026.pdf",
                "/newsroom/economicdata/cpi_08122026.pdf",
            ),
            DOL_ECONOMIC_DATA_INDEX_URL,
            "text/html; charset=UTF-8",
        )
    if request.full_url == EMPLOYMENT_URL:
        return _Response(b"%PDF-FAKE-EMPLOYMENT", EMPLOYMENT_URL, "application/pdf")
    if request.full_url == CPI_URL:
        return _Response(b"%PDF-FAKE-CPI", CPI_URL, "application/pdf")
    raise AssertionError(f"unexpected URL: {request.full_url}")


def test_default_live_source_set_replaces_bls_rss_routes_and_adds_treasury_and_eia() -> None:
    legacy_bls = {"bls_employment_situation_rss", "bls_cpi_rss"}
    expected = (
        (set(LIVE_RSS_SOURCE_IDS) - legacy_bls)
        | set(BLS_DOL_SOURCE_IDS)
        | {"us_treasury_press_releases", "eia_weekly_petroleum_status_report"}
    )

    assert len(LIVE_OFFICIAL_SOURCE_IDS) == 13
    assert set(LIVE_OFFICIAL_SOURCE_IDS) == expected
    assert legacy_bls.isdisjoint(LIVE_OFFICIAL_SOURCE_IDS)
    assert set(BLS_DOL_SOURCE_IDS).issubset(LIVE_OFFICIAL_SOURCE_IDS)


def test_dol_mirror_contract_is_explicitly_official_secondary_and_non_directional() -> None:
    root = Path(__file__).resolve().parents[2]
    contract = json.loads(
        (
            root
            / "contracts"
            / "intelligence"
            / "usdrubf_news_bls_dol_mirror_source_contract_v1.json"
        ).read_text(encoding="utf-8")
    )
    assert contract["governance"]["directional_action_authority"] is False
    assert contract["governance"]["pdf_embargo_timestamp_is_required"] is True
    assert contract["governance"]["published_at_comes_from_bls_release_timestamp"] is True
    assert contract["governance"]["available_at_is_first_successful_dol_acquisition"] is True
    by_id = {item["source_id"]: item for item in contract["sources"]}
    assert set(by_id) == set(BLS_DOL_SOURCE_IDS)
    assert all(item["tier"] == "OFFICIAL_SECONDARY" for item in by_id.values())
    assert by_id[EMPLOYMENT_ID]["replaces_live_route"] == "bls_employment_situation_rss"
    assert by_id[CPI_ID]["replaces_live_route"] == "bls_cpi_rss"


def test_dol_mirror_acquires_both_releases_with_pdf_publication_time_and_provenance() -> None:
    now = datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc)

    result = fetch_bls_dol_mirror_batch(
        opener=_healthy_opener,
        now_fn=lambda: now,
        pdf_reader_factory=_Reader,
    )

    assert result.ok_source_count == 2
    assert result.failures == ()
    assert len(result.records) == 2
    by_id = {record.source_id: record for record in result.records}

    employment = by_id[EMPLOYMENT_ID]
    assert employment.source_tier == "OFFICIAL_SECONDARY"
    assert employment.source_reference == EMPLOYMENT_URL
    assert employment.published_at.isoformat() == "2026-08-07T08:30:00-04:00"
    assert employment.available_at == now
    assert employment.ingested_at == now

    cpi = by_id[CPI_ID]
    assert cpi.source_tier == "OFFICIAL_SECONDARY"
    assert cpi.source_reference == CPI_URL
    assert cpi.published_at.isoformat() == "2026-08-12T08:30:00-04:00"
    assert cpi.available_at == now
    assert cpi.ingested_at == now


def test_dol_index_selects_latest_discovered_pdf_without_constructing_url_from_clock() -> None:
    now = datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc)
    older = "https://www.dol.gov/newsroom/economicdata/empsit_07022026.pdf"

    def opener(request, timeout):
        if request.full_url == DOL_ECONOMIC_DATA_INDEX_URL:
            return _Response(
                _index(
                    older,
                    "https://evil.example/newsroom/economicdata/empsit_09012026.pdf",
                    EMPLOYMENT_URL,
                ),
                DOL_ECONOMIC_DATA_INDEX_URL,
                "text/html",
            )
        assert request.full_url == EMPLOYMENT_URL
        return _Response(b"%PDF-FAKE-EMPLOYMENT", EMPLOYMENT_URL, "application/pdf")

    result = fetch_bls_dol_mirror_batch(
        source_ids=(EMPLOYMENT_ID,),
        opener=opener,
        now_fn=lambda: now,
        pdf_reader_factory=_Reader,
    )

    assert result.ok_source_count == 1
    assert result.records[0].source_reference == EMPLOYMENT_URL


def test_dol_mirror_future_release_is_not_exposed() -> None:
    now = datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc)

    result = fetch_bls_dol_mirror_batch(
        source_ids=(CPI_ID,),
        opener=_healthy_opener,
        now_fn=lambda: now,
        pdf_reader_factory=_Reader,
    )

    assert result.ok_source_count == 1
    assert result.records == ()
    assert result.source_results[0].future_items_skipped == 1


def test_dol_mirror_timestamp_failure_is_isolated() -> None:
    now = datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc)

    class BadCpiReader:
        def __init__(self, stream) -> None:
            raw = stream.getvalue()
            text = (
                "Transmission of material in this news release is embargoed until "
                "8:30 a.m. (ET) Friday, August 7, 2026. THE EMPLOYMENT SITUATION."
                if b"EMPLOYMENT" in raw
                else "CONSUMER PRICE INDEX - JULY 2026. No timestamp."
            )
            self.pages = [_Page(text)]

    result = fetch_bls_dol_mirror_batch(
        opener=_healthy_opener,
        now_fn=lambda: now,
        pdf_reader_factory=BadCpiReader,
    )

    assert result.ok_source_count == 1
    assert len(result.failures) == 1
    assert result.failures[0].source_id == CPI_ID
    assert result.failures[0].quality_status == "TIMESTAMP_UNPROVABLE"
    assert result.records[0].source_id == EMPLOYMENT_ID


def test_dol_mirror_rejects_cross_host_pdf_redirect_without_poisoning_other_source() -> None:
    now = datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc)

    def opener(request, timeout):
        if request.full_url == DOL_ECONOMIC_DATA_INDEX_URL:
            return _Response(
                _index(
                    "/newsroom/economicdata/empsit_08072026.pdf",
                    "/newsroom/economicdata/cpi_08122026.pdf",
                ),
                DOL_ECONOMIC_DATA_INDEX_URL,
                "text/html",
            )
        if request.full_url == EMPLOYMENT_URL:
            return _Response(b"%PDF-FAKE-EMPLOYMENT", EMPLOYMENT_URL, "application/pdf")
        if request.full_url == CPI_URL:
            return _Response(
                b"%PDF-FAKE-CPI",
                "https://evil.example/cpi_08122026.pdf",
                "application/pdf",
            )
        raise AssertionError(f"unexpected URL: {request.full_url}")

    result = fetch_bls_dol_mirror_batch(
        opener=opener,
        now_fn=lambda: now,
        pdf_reader_factory=_Reader,
    )

    assert result.ok_source_count == 1
    assert result.records[0].source_id == EMPLOYMENT_ID
    assert len(result.failures) == 1
    assert result.failures[0].source_id == CPI_ID
    assert result.failures[0].quality_status == "SOURCE_INVALID"
