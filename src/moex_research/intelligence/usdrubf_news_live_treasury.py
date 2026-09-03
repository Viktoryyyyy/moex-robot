from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from .usdrubf_news_live_rss import SOURCE_REGISTRY_PATH
from .usdrubf_news_macro import NewsSourceRecord


SOURCE_ID = "us_treasury_press_releases"
_ALLOWED_TIERS = {"OFFICIAL_PRIMARY", "OFFICIAL_SECONDARY"}
_ALLOWED_QUALITY = {"OK", "SOURCE_UNAVAILABLE", "SOURCE_INVALID", "TIMESTAMP_UNPROVABLE"}
_RELEASE_PATH_PREFIX = "/news/press-releases/"
_HUB_SLUGS = {"press-releases", "readouts", "statements-remarks", "testimonies"}
_PUBLICATION_DATE_CLASS = "field--name-field-news-publication-date"
_BODY_CLASS = "field--name-field-news-body"
_DEFAULT_DETAIL_MAX_WORKERS = 8


class TreasuryAcquisitionError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        if code not in _ALLOWED_QUALITY - {"OK"}:
            raise ValueError("invalid Treasury acquisition error code")
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class TreasuryBinding:
    source_id: str
    source_tier: str
    index_url: str
    allowed_host: str


@dataclass(frozen=True)
class TreasurySourceResult:
    source_id: str
    quality_status: str
    records: tuple[NewsSourceRecord, ...]
    future_items_skipped: int = 0
    candidate_count: int = 0
    error: str | None = None

    def __post_init__(self) -> None:
        if self.quality_status not in _ALLOWED_QUALITY:
            raise ValueError("invalid Treasury quality_status")
        if self.quality_status == "OK" and self.error is not None:
            raise ValueError("OK Treasury result may not include error")
        if self.quality_status != "OK" and self.records:
            raise ValueError("failed Treasury result may not include records")
        if self.candidate_count < 0:
            raise ValueError("candidate_count must be non-negative")


def _normalize_host(host: str) -> str:
    value = host.casefold().rstrip(".")
    return value[4:] if value.startswith("www.") else value


def load_treasury_binding(
    *, registry_path: Path | str = SOURCE_REGISTRY_PATH,
) -> TreasuryBinding:
    try:
        contract = json.loads(Path(registry_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("unable to load Stage 12A source registry") from exc
    if not isinstance(contract, Mapping):
        raise ValueError("Stage 12A source registry must be a mapping")
    sources = contract.get("primary_sources")
    if not isinstance(sources, list):
        raise ValueError("source registry primary_sources must be a list")
    item = next(
        (
            value
            for value in sources
            if isinstance(value, Mapping) and value.get("source_id") == SOURCE_ID
        ),
        None,
    )
    if item is None:
        raise ValueError("Treasury source is not present in Stage 12A registry")
    if item.get("stage12b_status") != "READY_CANDIDATE":
        raise ValueError("Treasury source is not Stage 12B ready")
    if item.get("transport") != "HTML_INDEX":
        raise ValueError("Treasury source must use HTML_INDEX transport")
    tier = item.get("tier")
    if tier not in _ALLOWED_TIERS:
        raise ValueError("Treasury source must be official")
    refs = item.get("references")
    if not isinstance(refs, list) or len(refs) != 1 or not isinstance(refs[0], str):
        raise ValueError("Treasury source must have one explicit index reference")
    index_url = refs[0]
    parsed = urlparse(index_url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("Treasury index URL must be HTTPS")
    return TreasuryBinding(
        source_id=SOURCE_ID,
        source_tier=str(tier),
        index_url=index_url,
        allowed_host=parsed.hostname,
    )


def _validate_final_url(response: object, *, allowed_host: str) -> str:
    geturl = getattr(response, "geturl", None)
    if not callable(geturl):
        raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury response has no final URL")
    try:
        final_url = geturl()
    except Exception as exc:
        raise TreasuryAcquisitionError(
            "SOURCE_UNAVAILABLE", "Treasury final response URL could not be resolved"
        ) from exc
    if not isinstance(final_url, str):
        raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury final URL must be a string")
    parsed = urlparse(final_url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury final URL must be HTTPS")
    if _normalize_host(parsed.hostname) != _normalize_host(allowed_host):
        raise TreasuryAcquisitionError(
            "SOURCE_INVALID", "Treasury final response host does not match registry"
        )
    return final_url


def _read_response(response: object, *, max_bytes: int) -> bytes:
    read = getattr(response, "read", None)
    if not callable(read):
        raise TreasuryAcquisitionError("SOURCE_UNAVAILABLE", "Treasury response is not readable")
    try:
        raw = read(max_bytes + 1)
    except Exception as exc:
        raise TreasuryAcquisitionError("SOURCE_UNAVAILABLE", "Treasury response read failed") from exc
    if not isinstance(raw, (bytes, bytearray)):
        raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury response body must be bytes")
    if len(raw) > max_bytes:
        raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury response exceeded maximum size")
    return bytes(raw)


class _TreasuryIndexParser(HTMLParser):
    """Collect same-host Treasury release candidates without DOM-layout assumptions.

    The extraction rule mirrors the maintained us-legal-mcp Treasury pattern:
    accept anchors under /news/press-releases/, exclude known section subtrees,
    and deduplicate by URL. Freshness is resolved later from each detail page's
    authoritative publication timestamp rather than from index ordering.
    """

    def __init__(
        self,
        *,
        base_url: str,
        allowed_host: str,
        max_candidate_pages: int,
    ) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.allowed_host = allowed_host
        self.max_candidate_pages = max_candidate_pages
        self.links: list[str] = []
        self._seen: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a":
            return
        href = dict(attrs).get("href")
        if not href:
            return
        absolute = urljoin(self.base_url, href.strip())
        parsed = urlparse(absolute)
        if not parsed.path.startswith(_RELEASE_PATH_PREFIX):
            return
        relative_path = parsed.path[len(_RELEASE_PATH_PREFIX) :].strip("/")
        if not relative_path:
            return
        first_segment = relative_path.split("/", 1)[0].casefold()
        if first_segment in _HUB_SLUGS:
            return
        if parsed.scheme != "https" or not parsed.hostname:
            raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury release link must be HTTPS")
        if _normalize_host(parsed.hostname) != _normalize_host(self.allowed_host):
            raise TreasuryAcquisitionError(
                "SOURCE_INVALID", "Treasury release link host does not match registry"
            )
        clean = parsed._replace(fragment="").geturl()
        if clean in self._seen:
            return
        self._seen.add(clean)
        if len(self._seen) > self.max_candidate_pages:
            raise TreasuryAcquisitionError(
                "SOURCE_INVALID",
                "Treasury release candidate set exceeds bounded detail scan limit",
            )
        self.links.append(clean)


class _TreasuryDetailParser(HTMLParser):
    """Bind detail facts only to Treasury's semantic article markup.

    Treasury detail pages contain global navigation H1/time elements before the
    article. The release title is exposed by OpenGraph `og:title`, while the
    authoritative timestamp and body are exposed by Drupal field classes
    `field--name-field-news-publication-date` and `field--name-field-news-body`.
    Positional H1/time elements are intentionally ignored.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._title: str | None = None
        self._publication_depth = 0
        self._body_depth = 0
        self._body_parts: list[str] = []
        self.primary_datetime: str | None = None

    @property
    def title(self) -> str:
        return " ".join((self._title or "").split())

    @property
    def body(self) -> str:
        return " ".join(" ".join(self._body_parts).split())[:2000]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        name = tag.casefold()
        attr_map = {key.casefold(): value for key, value in attrs}

        if name == "meta":
            property_name = (attr_map.get("property") or "").strip().casefold()
            if property_name == "og:title":
                content = (attr_map.get("content") or "").strip()
                if content:
                    if self._title is not None and self._title != content:
                        raise TreasuryAcquisitionError(
                            "SOURCE_INVALID", "Treasury detail has conflicting og:title values"
                        )
                    self._title = content
            return

        if name == "div":
            classes = set((attr_map.get("class") or "").split())
            if self._publication_depth:
                self._publication_depth += 1
            elif _PUBLICATION_DATE_CLASS in classes:
                self._publication_depth = 1

            if self._body_depth:
                self._body_depth += 1
            elif _BODY_CLASS in classes:
                self._body_depth = 1
            return

        if name == "time" and self._publication_depth:
            value = (attr_map.get("datetime") or "").strip()
            if value:
                if self.primary_datetime is not None and self.primary_datetime != value:
                    raise TreasuryAcquisitionError(
                        "TIMESTAMP_UNPROVABLE",
                        "Treasury publication field contains conflicting timestamps",
                    )
                self.primary_datetime = value

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() != "div":
            return
        if self._publication_depth:
            self._publication_depth -= 1
        if self._body_depth:
            self._body_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._body_depth:
            self._body_parts.append(data)


def _parse_iso_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TreasuryAcquisitionError(
            "TIMESTAMP_UNPROVABLE", "Treasury publication timestamp is not parseable"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TreasuryAcquisitionError(
            "TIMESTAMP_UNPROVABLE", "Treasury publication timestamp is timezone-naive"
        )
    return parsed


def _request_html(
    url: str,
    *,
    opener: Callable[..., object],
    timeout_seconds: float,
    max_bytes: int,
    allowed_host: str,
) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": "MOEX_Bot/rub-intelligence-treasury-html-v1",
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
        },
        method="GET",
    )
    try:
        response = opener(request, timeout=timeout_seconds)
        _validate_final_url(response, allowed_host=allowed_host)
        return _read_response(response, max_bytes=max_bytes)
    except TreasuryAcquisitionError:
        raise
    except Exception as exc:
        raise TreasuryAcquisitionError("SOURCE_UNAVAILABLE", "Treasury request failed") from exc


def fetch_treasury_press_releases(
    *,
    registry_path: Path | str = SOURCE_REGISTRY_PATH,
    opener: Callable[..., object] = urlopen,
    now_fn: Callable[[], datetime] | None = None,
    timeout_seconds: float = 10.0,
    max_bytes: int = 2_000_000,
    max_detail_pages: int = 10,
    max_candidate_pages: int = 40,
) -> TreasurySourceResult:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if max_bytes <= 0 or max_detail_pages <= 0 or max_candidate_pages <= 0:
        raise ValueError("Treasury limits must be positive")
    if max_detail_pages > max_candidate_pages:
        raise ValueError("max_detail_pages may not exceed max_candidate_pages")
    now = (now_fn or (lambda: datetime.now(timezone.utc)))()
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now_fn must return timezone-aware datetime")

    binding = load_treasury_binding(registry_path=registry_path)
    index_raw = _request_html(
        binding.index_url,
        opener=opener,
        timeout_seconds=timeout_seconds,
        max_bytes=max_bytes,
        allowed_host=binding.allowed_host,
    )
    try:
        index_text = index_raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury index is not UTF-8") from exc

    index_parser = _TreasuryIndexParser(
        base_url=binding.index_url,
        allowed_host=binding.allowed_host,
        max_candidate_pages=max_candidate_pages,
    )
    index_parser.feed(index_text)
    index_parser.close()
    if not index_parser.links:
        raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury index contains no release candidates")

    def load_detail(detail_url: str) -> tuple[NewsSourceRecord | None, int]:
        raw = _request_html(
            detail_url,
            opener=opener,
            timeout_seconds=timeout_seconds,
            max_bytes=max_bytes,
            allowed_host=binding.allowed_host,
        )
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury detail is not UTF-8") from exc
        parser = _TreasuryDetailParser()
        parser.feed(text)
        parser.close()
        if not parser.title:
            raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury release is missing og:title")
        if parser.primary_datetime is None:
            raise TreasuryAcquisitionError(
                "TIMESTAMP_UNPROVABLE",
                "Treasury release semantic publication timestamp is missing",
            )
        published_at = _parse_iso_timestamp(parser.primary_datetime)
        if published_at > now:
            return None, 1
        return (
            NewsSourceRecord(
                source_id=binding.source_id,
                source_tier=binding.source_tier,
                source_reference=detail_url,
                published_at=published_at,
                available_at=published_at,
                ingested_at=now,
                headline=parser.title,
                body=parser.body,
            ),
            0,
        )

    detail_results: list[tuple[NewsSourceRecord | None, int]] = []
    production_urlopen = opener is urlopen
    if production_urlopen and len(index_parser.links) > 1:
        worker_count = min(_DEFAULT_DETAIL_MAX_WORKERS, len(index_parser.links))
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="treasury-detail",
        ) as executor:
            futures = {
                detail_url: executor.submit(load_detail, detail_url)
                for detail_url in index_parser.links
            }
            detail_results = [
                futures[detail_url].result()
                for detail_url in index_parser.links
            ]
    else:
        detail_results = [load_detail(detail_url) for detail_url in index_parser.links]

    records = [record for record, _future in detail_results if record is not None]
    future_items = sum(future for _record, future in detail_results)
    records.sort(key=lambda record: (record.published_at, record.source_reference), reverse=True)
    return TreasurySourceResult(
        source_id=binding.source_id,
        quality_status="OK",
        records=tuple(records[:max_detail_pages]),
        future_items_skipped=future_items,
        candidate_count=len(index_parser.links),
    )