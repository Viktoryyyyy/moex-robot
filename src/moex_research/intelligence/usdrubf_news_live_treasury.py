from __future__ import annotations

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
    error: str | None = None

    def __post_init__(self) -> None:
        if self.quality_status not in _ALLOWED_QUALITY:
            raise ValueError("invalid Treasury quality_status")
        if self.quality_status == "OK" and self.error is not None:
            raise ValueError("OK Treasury result may not include error")
        if self.quality_status != "OK" and self.records:
            raise ValueError("failed Treasury result may not include records")


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
    """Extract release links only from the primary chronological listing.

    Treasury renders mega-menu/featured release links before the page's actual
    <main><h1>Press Releases</h1> listing. Treating every matching href as an
    index item can therefore fill a bounded detail-page budget with stale
    navigation entries. The parser remains fail-closed and starts collecting
    only after the main-page H1 has been proven to be the Press Releases list.
    """

    def __init__(self, *, base_url: str, allowed_host: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.allowed_host = allowed_host
        self.links: list[str] = []
        self._in_main = False
        self._in_h1 = False
        self._h1_parts: list[str] = []
        self._listing_started = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        name = tag.casefold()
        if name == "main":
            self._in_main = True
            return
        if not self._in_main:
            return
        if name == "h1" and not self._listing_started:
            self._in_h1 = True
            self._h1_parts = []
            return
        if not self._listing_started or name != "a":
            return
        href = dict(attrs).get("href")
        if not href:
            return
        absolute = urljoin(self.base_url, href.strip())
        parsed = urlparse(absolute)
        if not parsed.path.startswith(_RELEASE_PATH_PREFIX):
            return
        if parsed.path.rstrip("/") == _RELEASE_PATH_PREFIX.rstrip("/"):
            return
        if parsed.scheme != "https" or not parsed.hostname:
            raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury release link must be HTTPS")
        if _normalize_host(parsed.hostname) != _normalize_host(self.allowed_host):
            raise TreasuryAcquisitionError(
                "SOURCE_INVALID", "Treasury release link host does not match registry"
            )
        clean = parsed._replace(fragment="").geturl()
        if clean not in self.links:
            self.links.append(clean)

    def handle_endtag(self, tag: str) -> None:
        name = tag.casefold()
        if name == "h1" and self._in_h1:
            title = " ".join(" ".join(self._h1_parts).split())
            self._in_h1 = False
            self._h1_parts = []
            if title.casefold() == "press releases":
                self._listing_started = True
            return
        if name == "main" and self._in_main:
            self._in_main = False
            self._in_h1 = False
            self._h1_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_h1:
            self._h1_parts.append(data)


class _TreasuryDetailParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._in_h1 = False
        self._h1_seen = False
        self._title_parts: list[str] = []
        self._in_p = False
        self._p_parts: list[str] = []
        self._paragraphs: list[str] = []
        self.primary_datetime: str | None = None

    @property
    def title(self) -> str:
        return " ".join(" ".join(self._title_parts).split())

    @property
    def body(self) -> str:
        return " ".join(self._paragraphs)[:2000]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        name = tag.casefold()
        if name == "h1" and not self._h1_seen:
            self._in_h1 = True
            return
        if self._h1_seen and self.primary_datetime is None and name == "time":
            value = dict(attrs).get("datetime")
            if value and value.strip():
                self.primary_datetime = value.strip()
            return
        if self._h1_seen and self.primary_datetime is not None and name == "p" and len(self._paragraphs) < 3:
            self._in_p = True
            self._p_parts = []

    def handle_endtag(self, tag: str) -> None:
        name = tag.casefold()
        if name == "h1" and self._in_h1:
            self._in_h1 = False
            self._h1_seen = True
            return
        if name == "p" and self._in_p:
            value = " ".join(" ".join(self._p_parts).split())
            if value:
                self._paragraphs.append(value)
            self._p_parts = []
            self._in_p = False

    def handle_data(self, data: str) -> None:
        if self._in_h1:
            self._title_parts.append(data)
        elif self._in_p:
            self._p_parts.append(data)


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
) -> TreasurySourceResult:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if max_bytes <= 0 or max_detail_pages <= 0:
        raise ValueError("Treasury limits must be positive")
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
    )
    index_parser.feed(index_text)
    index_parser.close()
    if not index_parser.links:
        raise TreasuryAcquisitionError(
            "SOURCE_INVALID", "Treasury primary Press Releases listing contains no release links"
        )

    records: list[NewsSourceRecord] = []
    future_items = 0
    for detail_url in index_parser.links[:max_detail_pages]:
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
            raise TreasuryAcquisitionError("SOURCE_INVALID", "Treasury release is missing main title")
        if parser.primary_datetime is None:
            raise TreasuryAcquisitionError(
                "TIMESTAMP_UNPROVABLE", "Treasury release main publication timestamp is missing"
            )
        published_at = _parse_iso_timestamp(parser.primary_datetime)
        if published_at > now:
            future_items += 1
            continue
        records.append(
            NewsSourceRecord(
                source_id=binding.source_id,
                source_tier=binding.source_tier,
                source_reference=detail_url,
                published_at=published_at,
                available_at=published_at,
                ingested_at=now,
                headline=parser.title,
                body=parser.body,
            )
        )

    return TreasurySourceResult(
        source_id=binding.source_id,
        quality_status="OK",
        records=tuple(records),
        future_items_skipped=future_items,
    )
