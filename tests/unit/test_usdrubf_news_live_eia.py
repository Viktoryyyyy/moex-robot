from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from src.moex_research.intelligence.usdrubf_news_live_eia import (
    INDEX_URL,
    SCHEDULE_URL,
    SOURCE_ID,
    SUMMARY_PDF_URL,
    EiaAcquisitionError,
    fetch_eia_wpsr,
)


class _Response:
    def __init__(self, payload: bytes, url: str, content_type: str) -> None:
        self.payload = payload
        self.url = url
        self.headers = {"Content-Type": content_type}

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.url


def _index(*, week: str = "August 21, 2026", release: str = "August 26, 2026") -> bytes:
    return (
        "<html><body>"
        f"<span>Data for week ending {week}</span> "
        f"<span>Release Date:</span> <span class='date'>{release}</span> "
        "<span>Next Release Date:</span> <span>September 2, 2026</span>"
        "</body></html>"
    ).encode("utf-8")


def _schedule(*, holiday_row: str = "") -> bytes:
    return (
        "<html><body>"
        "<p>The wpsrsummary.pdf, overview.pdf, and Tables 1-14 in CSV and XLS formats, "
        "are released to the web site after 10:30 a.m. eastern time on Wednesday.</p>"
        "<table>"
        "<tr><th>Data for the week ending</th><th>Alternate release date</th>"
        "<th>Release day</th><th>Release time</th><th>Holiday</th></tr>"
        f"{holiday_row}"
        "</table></body></html>"
    ).encode("utf-8")


def _opener(index: bytes, schedule: bytes, pdf: bytes = b"%PDF-FAKE"):
    def opener(request, timeout):
        if request.full_url == INDEX_URL:
            return _Response(index, INDEX_URL, "text/html; charset=UTF-8")
        if request.full_url == SCHEDULE_URL:
            return _Response(schedule, SCHEDULE_URL, "text/html; charset=UTF-8")
        if request.full_url == SUMMARY_PDF_URL:
            return _Response(
                pdf,
                "https://ir.eia.gov/secure/wpsr/wpsrsummary.pdf?Policy=test&Signature=test",
                "application/pdf",
            )
        raise AssertionError(f"unexpected URL: {request.full_url}")

    return opener


def _pdf_text(week: str = "August 21, 2026") -> str:
    return (
        f"Summary of Weekly Petroleum Data for the week ending {week}. "
        "U.S. commercial crude oil inventories decreased by 2.4 million barrels. "
        "Gasoline inventories increased and distillate fuel inventories decreased."
    )


def test_regular_wpsr_uses_official_1030_et_release_and_acquisition_availability() -> None:
    acquired_at = datetime(2026, 8, 26, 17, 0, tzinfo=timezone.utc)
    result = fetch_eia_wpsr(
        opener=_opener(_index(), _schedule()),
        now_fn=lambda: acquired_at,
        pdf_text_extractor=lambda raw: _pdf_text(),
    )

    assert result.quality_status == "OK"
    assert result.future_items_skipped == 0
    assert len(result.records) == 1
    record = result.records[0]
    assert record.source_id == SOURCE_ID
    assert record.source_tier == "OFFICIAL_PRIMARY"
    assert record.source_reference == SUMMARY_PDF_URL
    assert record.published_at.isoformat() == "2026-08-26T10:30:00-04:00"
    assert record.available_at == acquired_at
    assert record.ingested_at == acquired_at
    assert "August 21, 2026" in record.headline
    assert "crude oil inventories" in record.body


def test_holiday_release_uses_exact_schedule_exception() -> None:
    holiday = (
        "<tr><td>September 4, 2026</td><td>September 10, 2026</td>"
        "<td>Thursday</td><td>12:00 p.m.</td><td>Labor Day</td></tr>"
    )
    acquired_at = datetime(2026, 9, 10, 18, 0, tzinfo=timezone.utc)
    result = fetch_eia_wpsr(
        opener=_opener(
            _index(week="September 4, 2026", release="September 10, 2026"),
            _schedule(holiday_row=holiday),
        ),
        now_fn=lambda: acquired_at,
        pdf_text_extractor=lambda raw: _pdf_text("September 4, 2026"),
    )

    assert result.quality_status == "OK"
    assert result.records[0].published_at.isoformat() == "2026-09-10T12:00:00-04:00"


def test_non_wednesday_release_without_holiday_row_fails_timestamp_closed() -> None:
    with pytest.raises(EiaAcquisitionError) as exc_info:
        fetch_eia_wpsr(
            opener=_opener(
                _index(week="September 4, 2026", release="September 10, 2026"),
                _schedule(),
            ),
            now_fn=lambda: datetime(2026, 9, 10, 18, 0, tzinfo=timezone.utc),
            pdf_text_extractor=lambda raw: _pdf_text("September 4, 2026"),
        )
    assert exc_info.value.code == "TIMESTAMP_UNPROVABLE"


def test_index_pdf_week_mismatch_fails_closed() -> None:
    with pytest.raises(EiaAcquisitionError) as exc_info:
        fetch_eia_wpsr(
            opener=_opener(_index(), _schedule()),
            now_fn=lambda: datetime(2026, 8, 26, 17, 0, tzinfo=timezone.utc),
            pdf_text_extractor=lambda raw: _pdf_text("August 14, 2026"),
        )
    assert exc_info.value.code == "SOURCE_INVALID"


def test_future_release_is_skipped_without_fabricating_availability() -> None:
    result = fetch_eia_wpsr(
        opener=_opener(_index(), _schedule()),
        now_fn=lambda: datetime(2026, 8, 26, 14, 0, tzinfo=timezone.utc),
        pdf_text_extractor=lambda raw: _pdf_text(),
    )
    assert result.quality_status == "OK"
    assert result.records == ()
    assert result.future_items_skipped == 1


def test_eia_contract_preserves_no_action_authority_and_no_header_timestamp_fallback() -> None:
    root = Path(__file__).resolve().parents[2]
    contract = json.loads(
        (
            root
            / "contracts"
            / "intelligence"
            / "usdrubf_news_eia_wpsr_live_source_contract_v1.json"
        ).read_text(encoding="utf-8")
    )
    assert contract["source"]["source_id"] == SOURCE_ID
    assert contract["source"]["tier"] == "OFFICIAL_PRIMARY"
    assert contract["governance"]["directional_action_authority"] is False
    assert contract["governance"]["global_news_authority"] is False
    policy = contract["publication_time_policy"]
    assert policy["http_date_is_publication_time"] is False
    assert policy["last_modified_is_publication_time"] is False
    assert policy["pdf_creation_metadata_is_publication_time"] is False
    assert policy["calendar_or_schedule_alone_proves_content_availability"] is False


@pytest.mark.parametrize("week,release", [
    ("August 28, 2026", "September 2, 2026"),
    ("Aug. 28, 2026", "Sept. 2, 2026"),
    ("Aug 28, 2026", "Sep 2, 2026"),
])
def test_official_index_month_formats_preserve_proven_publication(week, release) -> None:
    index = _index(week=week, release=release).replace(
        b"<span>September 2, 2026</span>", b"<span>Sept. 10, 2026</span>"
    )
    acquired = datetime(2026, 9, 8, 21, 39, tzinfo=timezone.utc)
    result = fetch_eia_wpsr(
        opener=_opener(index, _schedule()), now_fn=lambda: acquired,
        pdf_text_extractor=lambda raw: _pdf_text("August 28, 2026"),
    )
    assert result.records[0].published_at.isoformat() == "2026-09-02T10:30:00-04:00"
    assert result.records[0].available_at == acquired
    assert result.records[0].source_reference == SUMMARY_PDF_URL
    assert week in result.records[0].headline


@pytest.mark.parametrize("index", [
    b"<p>Next Release Date: Sept. 2, 2026</p>",
    b"<p>Data for week ending Aug. 28, 2026 Next Release Date: Sept. 2, 2026</p>",
    _index(week="Bogus. 28, 2026", release="Sept. 2, 2026"),
    _index(week="Aug. 32, 2026", release="Sept. 2, 2026"),
    _index() + _index(),
    _index() + _index(week="August 28, 2026", release="September 2, 2026"),
    _index() + b"<p>Previous issues: Data for week ending unknown</p>",
])
def test_missing_unknown_or_ambiguous_index_dates_fail_closed(index) -> None:
    with pytest.raises(EiaAcquisitionError) as caught:
        fetch_eia_wpsr(
            opener=_opener(index, _schedule()),
            now_fn=lambda: datetime(2026, 9, 8, 22, tzinfo=timezone.utc),
            pdf_text_extractor=lambda raw: _pdf_text(),
        )
    assert caught.value.code == "TIMESTAMP_UNPROVABLE"


def test_dotted_index_retains_pdf_week_crosscheck() -> None:
    with pytest.raises(EiaAcquisitionError) as caught:
        fetch_eia_wpsr(
            opener=_opener(_index(week="Aug. 28, 2026", release="Sept. 2, 2026"), _schedule()),
            now_fn=lambda: datetime(2026, 9, 8, 22, tzinfo=timezone.utc),
            pdf_text_extractor=lambda raw: _pdf_text("August 21, 2026"),
        )
    assert caught.value.code == "SOURCE_INVALID"


def test_dotted_index_retains_holiday_week_crosscheck() -> None:
    holiday = (
        "<tr><td>September 4, 2026</td><td>September 10, 2026</td>"
        "<td>Thursday</td><td>12:00 p.m.</td><td>Labor Day</td></tr>"
    )
    with pytest.raises(EiaAcquisitionError) as caught:
        fetch_eia_wpsr(
            opener=_opener(_index(week="Aug. 28, 2026", release="Sept. 10, 2026"),
                           _schedule(holiday_row=holiday)),
            now_fn=lambda: datetime(2026, 9, 10, 22, tzinfo=timezone.utc),
            pdf_text_extractor=lambda raw: _pdf_text("August 28, 2026"),
        )
    assert caught.value.code == "TIMESTAMP_UNPROVABLE"


def test_dotted_index_future_publication_still_excluded() -> None:
    result = fetch_eia_wpsr(
        opener=_opener(_index(week="Aug. 28, 2026", release="Sept. 2, 2026"), _schedule()),
        now_fn=lambda: datetime(2026, 9, 2, 14, 29, tzinfo=timezone.utc),
        pdf_text_extractor=lambda raw: _pdf_text("August 28, 2026"),
    )
    assert result.records == ()
    assert result.future_items_skipped == 1
