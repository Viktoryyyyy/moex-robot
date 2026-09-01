from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from io import BytesIO
import re
from typing import Callable, Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from pypdf import PdfReader

from .usdrubf_news_live_rss import (
    RssAcquisitionError,
    RssBatchResult,
    RssSourceResult,
)
from .usdrubf_news_macro import NewsSourceRecord


DOL_ECONOMIC_DATA_INDEX_URL = "https://www.dol.gov/newsroom/economicdata"
DOL_ALLOWED_HOST = "www.dol.gov"
BLS_DOL_SOURCE_IDS = (
    "bls_employment_situation_dol_mirror",
    "bls_cpi_dol_mirror",
)

_USER_AGENT = (
    "MOEX_Bot/rub-intelligence-stage12b1 "
    "(+https://github.com/Viktoryyyyy/moex-robot)"
)
_RELEASE_TIME_RE = re.compile(
    r"embargoed\s+until\s+"
    r"(?P<clock>\d{1,2}:\d{2})\s+"
    r"(?P<meridiem>a\.m\.|p\.m\.)\s+"
    r"\(ET\)\s+"
    r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"(?P<month>[A-Za-z]+)\s+"
    r"(?P<day>\d{1,2}),\s+"
    r"(?P<year>\d{4})",
    re.IGNORECASE,
)
_SPACE_RE = re.compile(r"\s+")
_PDF_PATH_RE = re.compile(
    r"^/newsroom/economicdata/(?P<prefix>empsit|cpi)_(?P<date>\d{8})\.pdf$",
    re.IGNORECASE,
)
_ET = ZoneInfo("America/New_York")
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
_ENGLISH_WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


@dataclass(frozen=True)
class _MirrorSpec:
    source_id: str
    prefix: str
    headline: str


_SPECS = {
    "bls_employment_situation_dol_mirror": _MirrorSpec(
        source_id="bls_employment_situation_dol_mirror",
        prefix="empsit",
        headline="BLS Employment Situation release",
    ),
    "bls_cpi_dol_mirror": _MirrorSpec(
        source_id="bls_cpi_dol_mirror",
        prefix="cpi",
        headline="BLS Consumer Price Index release",
    ),
}


class _HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.casefold() != "a":
            return
        for name, value in attrs:
            if name.casefold() == "href" and isinstance(value, str) and value.strip():
                self.hrefs.append(value.strip())


def _normalize_host(host: str) -> str:
    value = host.casefold().rstrip(".")
    return value[4:] if value.startswith("www.") else value


def _validate_final_url(response: object, *, allowed_host: str) -> str:
    geturl = getattr(response, "geturl", None)
    if not callable(geturl):
        raise RssAcquisitionError("SOURCE_INVALID", "DOL response does not expose final URL")
    try:
        final_url = geturl()
    except Exception as exc:
        raise RssAcquisitionError(
            "SOURCE_UNAVAILABLE",
            "DOL final response URL could not be resolved",
        ) from exc
    if not isinstance(final_url, str):
        raise RssAcquisitionError("SOURCE_INVALID", "DOL final response URL must be a string")
    parsed = urlparse(final_url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise RssAcquisitionError("SOURCE_INVALID", "DOL final response URL must be HTTPS")
    if _normalize_host(parsed.hostname) != _normalize_host(allowed_host):
        raise RssAcquisitionError(
            "SOURCE_INVALID",
            "DOL final response host does not match the registered publisher",
        )
    return final_url


def _read_bounded(response: object, *, max_bytes: int, label: str) -> bytes:
    read = getattr(response, "read", None)
    if not callable(read):
        raise RssAcquisitionError("SOURCE_UNAVAILABLE", f"{label} response has no readable body")
    try:
        raw = read(max_bytes + 1)
    except Exception as exc:
        raise RssAcquisitionError("SOURCE_UNAVAILABLE", f"{label} response read failed") from exc
    if not isinstance(raw, (bytes, bytearray)):
        raise RssAcquisitionError("SOURCE_INVALID", f"{label} response body must be bytes")
    if len(raw) > max_bytes:
        raise RssAcquisitionError("SOURCE_INVALID", f"{label} response exceeded maximum size")
    return bytes(raw)


def _response_content_type(response: object) -> str:
    headers = getattr(response, "headers", None)
    if headers is None:
        return ""
    get = getattr(headers, "get", None)
    if not callable(get):
        return ""
    value = get("Content-Type")
    return "" if value is None else str(value)


def _open(
    url: str,
    *,
    opener: Callable[..., object],
    timeout_seconds: float,
    accept: str,
) -> object:
    request = Request(
        url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": accept,
        },
        method="GET",
    )
    try:
        return opener(request, timeout=timeout_seconds)
    except RssAcquisitionError:
        raise
    except Exception as exc:
        raise RssAcquisitionError("SOURCE_UNAVAILABLE", "DOL request failed") from exc


def _discover_latest_release_urls(index_html: bytes) -> dict[str, str]:
    try:
        text = index_html.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RssAcquisitionError("SOURCE_INVALID", "DOL index is not valid UTF-8") from exc

    parser = _HrefParser()
    try:
        parser.feed(text)
        parser.close()
    except Exception as exc:
        raise RssAcquisitionError("SOURCE_INVALID", "DOL index HTML could not be parsed") from exc

    candidates: dict[str, list[tuple[datetime, str]]] = {
        source_id: [] for source_id in BLS_DOL_SOURCE_IDS
    }
    by_prefix = {spec.prefix: spec for spec in _SPECS.values()}
    for href in parser.hrefs:
        absolute = urljoin(DOL_ECONOMIC_DATA_INDEX_URL, href)
        parsed = urlparse(absolute)
        if parsed.scheme != "https" or not parsed.hostname:
            continue
        if _normalize_host(parsed.hostname) != _normalize_host(DOL_ALLOWED_HOST):
            continue
        match = _PDF_PATH_RE.fullmatch(parsed.path)
        if match is None or parsed.query or parsed.fragment:
            continue
        spec = by_prefix.get(match.group("prefix").casefold())
        if spec is None:
            continue
        raw_date = match.group("date")
        try:
            release_date = datetime(
                int(raw_date[4:8]),
                int(raw_date[0:2]),
                int(raw_date[2:4]),
            )
        except ValueError:
            continue
        candidates[spec.source_id].append((release_date, absolute))

    selected: dict[str, str] = {}
    for source_id, rows in candidates.items():
        if not rows:
            continue
        rows.sort(key=lambda item: (item[0], item[1]))
        selected[source_id] = rows[-1][1]
    return selected


def _extract_first_page_text(
    raw_pdf: bytes,
    *,
    pdf_reader_factory: Callable[[BytesIO], object],
    max_text_chars: int,
) -> str:
    if not raw_pdf.startswith(b"%PDF"):
        raise RssAcquisitionError("SOURCE_INVALID", "DOL release body is not a PDF")
    try:
        reader = pdf_reader_factory(BytesIO(raw_pdf))
        pages = getattr(reader, "pages")
        if len(pages) < 1:
            raise ValueError("PDF has no pages")
        extract_text = getattr(pages[0], "extract_text")
        text = extract_text()
    except RssAcquisitionError:
        raise
    except Exception as exc:
        raise RssAcquisitionError("SOURCE_INVALID", "DOL release PDF text extraction failed") from exc
    if not isinstance(text, str) or not text.strip():
        raise RssAcquisitionError("SOURCE_INVALID", "DOL release PDF first page has no text")
    normalized = _SPACE_RE.sub(" ", text).strip()
    if len(normalized) > max_text_chars:
        normalized = normalized[:max_text_chars]
    return normalized


def _release_timestamp(first_page_text: str) -> datetime:
    match = _RELEASE_TIME_RE.search(first_page_text)
    if match is None:
        raise RssAcquisitionError(
            "TIMESTAMP_UNPROVABLE",
            "BLS release embargo/publication timestamp is not present in DOL mirror PDF",
        )
    month = _ENGLISH_MONTHS.get(match.group("month").casefold())
    weekday = _ENGLISH_WEEKDAYS.get(match.group("weekday").casefold())
    if month is None or weekday is None:
        raise RssAcquisitionError(
            "TIMESTAMP_UNPROVABLE",
            "BLS release embargo/publication timestamp contains an unknown English date token",
        )
    try:
        hour_text, minute_text = match.group("clock").split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
        if not 1 <= hour <= 12 or not 0 <= minute <= 59:
            raise ValueError("invalid 12-hour clock")
        meridiem = match.group("meridiem").replace(".", "").casefold()
        if meridiem not in {"am", "pm"}:
            raise ValueError("invalid meridiem")
        hour_24 = hour % 12 + (12 if meridiem == "pm" else 0)
        local = datetime(
            int(match.group("year")),
            month,
            int(match.group("day")),
            hour_24,
            minute,
            tzinfo=_ET,
        )
    except (TypeError, ValueError) as exc:
        raise RssAcquisitionError(
            "TIMESTAMP_UNPROVABLE",
            "BLS release embargo/publication timestamp is malformed",
        ) from exc
    if local.weekday() != weekday:
        raise RssAcquisitionError(
            "TIMESTAMP_UNPROVABLE",
            "BLS release weekday does not match release date",
        )
    return local


def _fetch_one_release(
    spec: _MirrorSpec,
    release_url: str,
    *,
    opener: Callable[..., object],
    now: datetime,
    timeout_seconds: float,
    max_pdf_bytes: int,
    max_text_chars: int,
    max_body_chars: int,
    pdf_reader_factory: Callable[[BytesIO], object],
) -> RssSourceResult:
    response = _open(
        release_url,
        opener=opener,
        timeout_seconds=timeout_seconds,
        accept="application/pdf,*/*;q=0.1",
    )
    final_url = _validate_final_url(response, allowed_host=DOL_ALLOWED_HOST)
    if final_url != release_url:
        raise RssAcquisitionError(
            "SOURCE_INVALID",
            "DOL release final URL differs from the discovered immutable PDF URL",
        )
    content_type = _response_content_type(response).casefold()
    if "application/pdf" not in content_type:
        raise RssAcquisitionError("SOURCE_INVALID", "DOL release Content-Type is not application/pdf")
    raw = _read_bounded(response, max_bytes=max_pdf_bytes, label="DOL release")
    first_page_text = _extract_first_page_text(
        raw,
        pdf_reader_factory=pdf_reader_factory,
        max_text_chars=max_text_chars,
    )
    published_at = _release_timestamp(first_page_text)
    if published_at > now:
        return RssSourceResult(
            source_id=spec.source_id,
            quality_status="OK",
            records=(),
            future_items_skipped=1,
        )
    record = NewsSourceRecord(
        source_id=spec.source_id,
        source_tier="OFFICIAL_SECONDARY",
        source_reference=release_url,
        published_at=published_at,
        available_at=now,
        ingested_at=now,
        headline=f"{spec.headline} - {published_at.date().isoformat()}",
        body=first_page_text[:max_body_chars],
    )
    return RssSourceResult(
        source_id=spec.source_id,
        quality_status="OK",
        records=(record,),
    )


def fetch_bls_dol_mirror_batch(
    *,
    source_ids: Iterable[str] = BLS_DOL_SOURCE_IDS,
    opener: Callable[..., object] = urlopen,
    now_fn: Callable[[], datetime] | None = None,
    timeout_seconds: float = 10.0,
    max_index_bytes: int = 1_000_000,
    max_pdf_bytes: int = 5_000_000,
    max_text_chars: int = 50_000,
    max_body_chars: int = 2_000,
    pdf_reader_factory: Callable[[BytesIO], object] = PdfReader,
) -> RssBatchResult:
    requested = tuple(source_ids)
    if len(requested) != len(set(requested)):
        raise ValueError("duplicate BLS DOL mirror source_id requested")
    unknown = [source_id for source_id in requested if source_id not in _SPECS]
    if unknown:
        raise ValueError(f"unsupported BLS DOL mirror source_id: {unknown[0]}")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if min(max_index_bytes, max_pdf_bytes, max_text_chars) <= 0 or max_body_chars < 0:
        raise ValueError("DOL acquisition bounds must be positive")

    now = (now_fn or (lambda: datetime.now(timezone.utc)))()
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now_fn must return timezone-aware datetime")
    if not requested:
        return RssBatchResult(())

    try:
        response = _open(
            DOL_ECONOMIC_DATA_INDEX_URL,
            opener=opener,
            timeout_seconds=timeout_seconds,
            accept="text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
        )
        _validate_final_url(response, allowed_host=DOL_ALLOWED_HOST)
        index_html = _read_bounded(response, max_bytes=max_index_bytes, label="DOL index")
        discovered = _discover_latest_release_urls(index_html)
    except RssAcquisitionError as exc:
        return RssBatchResult(
            tuple(
                RssSourceResult(
                    source_id=source_id,
                    quality_status=exc.code,
                    records=(),
                    error=str(exc),
                )
                for source_id in requested
            )
        )

    results: list[RssSourceResult] = []
    for source_id in requested:
        spec = _SPECS[source_id]
        release_url = discovered.get(source_id)
        if release_url is None:
            results.append(
                RssSourceResult(
                    source_id=source_id,
                    quality_status="SOURCE_INVALID",
                    records=(),
                    error="DOL index contains no matching current BLS release PDF",
                )
            )
            continue
        try:
            result = _fetch_one_release(
                spec,
                release_url,
                opener=opener,
                now=now,
                timeout_seconds=timeout_seconds,
                max_pdf_bytes=max_pdf_bytes,
                max_text_chars=max_text_chars,
                max_body_chars=max_body_chars,
                pdf_reader_factory=pdf_reader_factory,
            )
        except RssAcquisitionError as exc:
            result = RssSourceResult(
                source_id=source_id,
                quality_status=exc.code,
                records=(),
                error=str(exc),
            )
        results.append(result)
    return RssBatchResult(tuple(results))
