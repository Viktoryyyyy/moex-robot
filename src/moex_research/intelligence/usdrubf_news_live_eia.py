from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from html import unescape
from html.parser import HTMLParser
from io import BytesIO
import re
from typing import Callable
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from pypdf import PdfReader

from .usdrubf_news_macro import NewsSourceRecord


SOURCE_ID = "eia_weekly_petroleum_status_report"
SOURCE_TIER = "OFFICIAL_PRIMARY"
INDEX_URL = "https://www.eia.gov/petroleum/supply/weekly/"
SCHEDULE_URL = "https://www.eia.gov/petroleum/supply/weekly/schedule.php"
SUMMARY_PDF_URL = "https://ir.eia.gov/wpsr/wpsrsummary.pdf"

_EIA_HOST = "www.eia.gov"
_EIA_PDF_HOST = "ir.eia.gov"
_USER_AGENT = (
    "MOEX_Bot/rub-intelligence-eia-wpsr-v1 "
    "(+https://github.com/Viktoryyyyy/moex-robot)"
)
_ET = ZoneInfo("America/New_York")
_SPACE_RE = re.compile(r"\s+")
_INDEX_RE = re.compile(
    r"Data\s+for\s+week\s+ending\s+"
    r"(?P<week>[A-Za-z]+\s+\d{1,2},\s+\d{4})\s+"
    r"Release\s+Date:\s*"
    r"(?P<release>[A-Za-z]+\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE,
)
_PDF_WEEK_RE = re.compile(
    r"Summary\s+of\s+Weekly\s+Petroleum\s+Data\s+for\s+the\s+week\s+ending\s+"
    r"(?P<week>[A-Za-z]+\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE,
)
_STANDARD_TIME_RE = re.compile(
    r"wpsrsummary\.pdf.*?released\s+to\s+the\s+web\s+site\s+after\s+"
    r"(?P<clock>\d{1,2}:\d{2})\s+(?P<meridiem>a\.m\.|p\.m\.).*?"
    r"eastern\s+time\s+on\s+Wednesday",
    re.IGNORECASE,
)
_ENGLISH_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_ALLOWED_QUALITY = {"OK", "SOURCE_UNAVAILABLE", "SOURCE_INVALID", "TIMESTAMP_UNPROVABLE"}


class EiaAcquisitionError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        if code not in _ALLOWED_QUALITY - {"OK"}:
            raise ValueError("invalid EIA acquisition error code")
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class EiaSourceResult:
    source_id: str
    quality_status: str
    records: tuple[NewsSourceRecord, ...]
    future_items_skipped: int = 0
    error: str | None = None

    def __post_init__(self) -> None:
        if self.quality_status not in _ALLOWED_QUALITY:
            raise ValueError("invalid EIA quality_status")
        if self.quality_status == "OK" and self.error is not None:
            raise ValueError("OK EIA result may not include error")
        if self.quality_status != "OK" and self.records:
            raise ValueError("failed EIA result may not include records")


class _VisibleAndTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.rows: list[tuple[str, ...]] = []
        self._in_row = False
        self._in_cell = False
        self._cell_parts: list[str] = []
        self._row_cells: list[str] = []

    @property
    def text(self) -> str:
        return _collapse(" ".join(self.parts))

    def handle_starttag(self, tag: str, attrs) -> None:
        name = tag.casefold()
        if name == "tr":
            self._in_row = True
            self._row_cells = []
        elif name in {"td", "th"} and self._in_row:
            self._in_cell = True
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(data)
        if self._in_cell:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        name = tag.casefold()
        if name in {"td", "th"} and self._in_cell:
            self._row_cells.append(_collapse(" ".join(self._cell_parts)))
            self._in_cell = False
            self._cell_parts = []
        elif name == "tr" and self._in_row:
            if self._row_cells:
                self.rows.append(tuple(self._row_cells))
            self._in_row = False
            self._row_cells = []


def _collapse(value: str) -> str:
    return _SPACE_RE.sub(" ", unescape(value).replace("\xa0", " ")).strip()


def _parse_english_date(value: str) -> date:
    match = re.fullmatch(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", _collapse(value))
    if match is None:
        raise EiaAcquisitionError("TIMESTAMP_UNPROVABLE", "EIA release date is not parseable")
    month = _ENGLISH_MONTHS.get(match.group(1).casefold())
    if month is None:
        raise EiaAcquisitionError("TIMESTAMP_UNPROVABLE", "EIA release month is not recognized")
    try:
        return date(int(match.group(3)), month, int(match.group(2)))
    except ValueError as exc:
        raise EiaAcquisitionError("TIMESTAMP_UNPROVABLE", "EIA release date is invalid") from exc


def _parse_clock(clock: str, meridiem: str) -> time:
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", clock.strip())
    if match is None:
        raise EiaAcquisitionError("TIMESTAMP_UNPROVABLE", "EIA release clock is not parseable")
    hour = int(match.group(1))
    minute = int(match.group(2))
    marker = meridiem.casefold()
    if not 1 <= hour <= 12 or not 0 <= minute <= 59 or marker not in {"a.m.", "p.m."}:
        raise EiaAcquisitionError("TIMESTAMP_UNPROVABLE", "EIA release clock is invalid")
    if marker == "a.m.":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12
    return time(hour, minute)


def _parse_clock_cell(value: str) -> time:
    match = re.fullmatch(r"(\d{1,2}:\d{2})\s+(a\.m\.|p\.m\.)", _collapse(value), re.I)
    if match is None:
        raise EiaAcquisitionError("TIMESTAMP_UNPROVABLE", "EIA holiday release time is not parseable")
    return _parse_clock(match.group(1), match.group(2))


def _normalize_host(host: str) -> str:
    value = host.casefold().rstrip(".")
    return value[4:] if value.startswith("www.") else value


def _validate_final_url(response: object, *, kind: str) -> str:
    geturl = getattr(response, "geturl", None)
    if not callable(geturl):
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA response has no final URL")
    try:
        final_url = geturl()
    except Exception as exc:
        raise EiaAcquisitionError("SOURCE_UNAVAILABLE", "EIA final URL could not be resolved") from exc
    if not isinstance(final_url, str):
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA final URL must be a string")
    parsed = urlparse(final_url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA final URL must be HTTPS")
    if kind == "pdf":
        if _normalize_host(parsed.hostname) != _normalize_host(_EIA_PDF_HOST):
            raise EiaAcquisitionError("SOURCE_INVALID", "EIA PDF final host is not allowed")
        if parsed.path not in {"/wpsr/wpsrsummary.pdf", "/secure/wpsr/wpsrsummary.pdf"}:
            raise EiaAcquisitionError("SOURCE_INVALID", "EIA PDF final path is not allowed")
    else:
        if _normalize_host(parsed.hostname) != _normalize_host(_EIA_HOST):
            raise EiaAcquisitionError("SOURCE_INVALID", "EIA HTML final host is not allowed")
    return final_url


def _content_type(response: object) -> str:
    headers = getattr(response, "headers", None)
    if headers is None:
        return ""
    get = getattr(headers, "get", None)
    if not callable(get):
        return ""
    value = get("Content-Type")
    return value.casefold() if isinstance(value, str) else ""


def _read_response(response: object, *, max_bytes: int) -> bytes:
    read = getattr(response, "read", None)
    if not callable(read):
        raise EiaAcquisitionError("SOURCE_UNAVAILABLE", "EIA response is not readable")
    try:
        raw = read(max_bytes + 1)
    except Exception as exc:
        raise EiaAcquisitionError("SOURCE_UNAVAILABLE", "EIA response read failed") from exc
    if not isinstance(raw, (bytes, bytearray)):
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA response body must be bytes")
    if len(raw) > max_bytes:
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA response exceeded maximum size")
    return bytes(raw)


def _request_bytes(
    url: str,
    *,
    kind: str,
    opener: Callable[..., object],
    timeout_seconds: float,
    max_bytes: int,
) -> bytes:
    accept = "application/pdf,*/*;q=0.1" if kind == "pdf" else "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1"
    request = Request(url, headers={"User-Agent": _USER_AGENT, "Accept": accept}, method="GET")
    try:
        response = opener(request, timeout=timeout_seconds)
        _validate_final_url(response, kind=kind)
        content_type = _content_type(response)
        if kind == "pdf":
            if "application/pdf" not in content_type:
                raise EiaAcquisitionError("SOURCE_INVALID", "EIA summary response is not PDF")
        elif "text/html" not in content_type and "application/xhtml+xml" not in content_type:
            raise EiaAcquisitionError("SOURCE_INVALID", "EIA index or schedule response is not HTML")
        raw = _read_response(response, max_bytes=max_bytes)
        if kind == "pdf" and not raw.startswith(b"%PDF"):
            raise EiaAcquisitionError("SOURCE_INVALID", "EIA summary response lacks PDF magic")
        return raw
    except EiaAcquisitionError:
        raise
    except Exception as exc:
        raise EiaAcquisitionError("SOURCE_UNAVAILABLE", "EIA request failed") from exc


def _parse_html(raw: bytes, *, label: str) -> _VisibleAndTableParser:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EiaAcquisitionError("SOURCE_INVALID", f"EIA {label} is not UTF-8") from exc
    parser = _VisibleAndTableParser()
    try:
        parser.feed(text)
        parser.close()
    except Exception as exc:
        raise EiaAcquisitionError("SOURCE_INVALID", f"EIA {label} HTML is malformed") from exc
    return parser


def _index_dates(parser: _VisibleAndTableParser) -> tuple[date, date, str]:
    match = _INDEX_RE.search(parser.text)
    if match is None:
        raise EiaAcquisitionError("TIMESTAMP_UNPROVABLE", "EIA index release date fields are missing")
    week_text = _collapse(match.group("week"))
    release_text = _collapse(match.group("release"))
    return _parse_english_date(week_text), _parse_english_date(release_text), week_text


def _scheduled_publication_time(
    parser: _VisibleAndTableParser,
    *,
    week_ending: date,
    release_date: date,
) -> datetime:
    standard = _STANDARD_TIME_RE.search(parser.text)
    if standard is None:
        raise EiaAcquisitionError("TIMESTAMP_UNPROVABLE", "EIA standard WPSR release time is missing")
    standard_clock = _parse_clock(standard.group("clock"), standard.group("meridiem"))

    holiday_match: tuple[date, time] | None = None
    for row in parser.rows:
        if len(row) < 4:
            continue
        try:
            row_week = _parse_english_date(row[0])
            row_release = _parse_english_date(row[1])
        except EiaAcquisitionError:
            continue
        if row_release != release_date:
            continue
        if row_week != week_ending:
            raise EiaAcquisitionError(
                "TIMESTAMP_UNPROVABLE",
                "EIA holiday schedule release date conflicts with week-ending date",
            )
        holiday_match = (row_release, _parse_clock_cell(row[3]))
        break

    if holiday_match is not None:
        release_day, release_clock = holiday_match
    else:
        if release_date.weekday() != 2:
            raise EiaAcquisitionError(
                "TIMESTAMP_UNPROVABLE",
                "non-Wednesday EIA release has no matching holiday schedule exception",
            )
        release_day, release_clock = release_date, standard_clock
    return datetime.combine(release_day, release_clock, tzinfo=_ET)


def _extract_pdf_text(raw: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(raw))
        if not reader.pages:
            raise ValueError("empty PDF")
        text = reader.pages[0].extract_text()
    except Exception as exc:
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA WPSR PDF text extraction failed") from exc
    if not isinstance(text, str) or not text.strip():
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA WPSR PDF first page is empty")
    return _collapse(text)


def fetch_eia_wpsr(
    *,
    opener: Callable[..., object] = urlopen,
    now_fn: Callable[[], datetime] | None = None,
    timeout_seconds: float = 10.0,
    max_html_bytes: int = 1_000_000,
    max_pdf_bytes: int = 5_000_000,
    pdf_text_extractor: Callable[[bytes], str] = _extract_pdf_text,
) -> EiaSourceResult:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if max_html_bytes <= 0 or max_pdf_bytes <= 0:
        raise ValueError("EIA response limits must be positive")
    if not callable(pdf_text_extractor):
        raise ValueError("pdf_text_extractor must be callable")

    index_raw = _request_bytes(
        INDEX_URL,
        kind="html",
        opener=opener,
        timeout_seconds=timeout_seconds,
        max_bytes=max_html_bytes,
    )
    schedule_raw = _request_bytes(
        SCHEDULE_URL,
        kind="html",
        opener=opener,
        timeout_seconds=timeout_seconds,
        max_bytes=max_html_bytes,
    )
    pdf_raw = _request_bytes(
        SUMMARY_PDF_URL,
        kind="pdf",
        opener=opener,
        timeout_seconds=timeout_seconds,
        max_bytes=max_pdf_bytes,
    )

    index = _parse_html(index_raw, label="WPSR index")
    schedule = _parse_html(schedule_raw, label="WPSR schedule")
    week_ending, release_date, week_text = _index_dates(index)
    published_at = _scheduled_publication_time(
        schedule,
        week_ending=week_ending,
        release_date=release_date,
    )

    try:
        pdf_text = _collapse(pdf_text_extractor(pdf_raw))
    except EiaAcquisitionError:
        raise
    except Exception as exc:
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA WPSR PDF text extraction failed") from exc
    if not pdf_text:
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA WPSR PDF text is empty")
    pdf_week_match = _PDF_WEEK_RE.search(pdf_text)
    if pdf_week_match is None:
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA WPSR PDF week-ending field is missing")
    if _parse_english_date(pdf_week_match.group("week")) != week_ending:
        raise EiaAcquisitionError("SOURCE_INVALID", "EIA WPSR index and PDF week-ending dates disagree")

    acquired_at = (now_fn or (lambda: datetime.now(timezone.utc)))()
    if not isinstance(acquired_at, datetime) or acquired_at.tzinfo is None or acquired_at.utcoffset() is None:
        raise ValueError("now_fn must return timezone-aware datetime")
    if published_at > acquired_at:
        return EiaSourceResult(
            source_id=SOURCE_ID,
            quality_status="OK",
            records=(),
            future_items_skipped=1,
        )

    body = pdf_text[:4000]
    record = NewsSourceRecord(
        source_id=SOURCE_ID,
        source_tier=SOURCE_TIER,
        source_reference=SUMMARY_PDF_URL,
        published_at=published_at,
        available_at=acquired_at,
        ingested_at=acquired_at,
        headline=f"EIA Weekly Petroleum Status Report — week ending {week_text}",
        body=body,
    )
    return EiaSourceResult(
        source_id=SOURCE_ID,
        quality_status="OK",
        records=(record,),
    )
