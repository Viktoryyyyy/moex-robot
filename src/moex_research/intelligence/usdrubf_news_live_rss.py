from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
import json
from pathlib import Path
import re
from typing import Callable, Iterable, Mapping
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from .usdrubf_news_macro import NewsSourceRecord


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SOURCE_REGISTRY_PATH = (
    PROJECT_ROOT / "contracts" / "intelligence" / "usdrubf_news_macro_source_registry_v1.json"
)

FIRST_SLICE_SOURCE_IDS = (
    "cbr_press_rss",
    "cbr_events_rss",
    "moex_all_news_rss",
    "moex_fx_news_rss",
    "fed_press_all_rss",
    "fed_monetary_policy_rss",
    "bls_employment_situation_rss",
    "bls_cpi_rss",
)
LIVE_RSS_SOURCE_IDS = FIRST_SLICE_SOURCE_IDS + (
    "whitehouse_releases",
    "eu_council_press_releases",
    "eu_commission_news",
)

_ALLOWED_TIERS = {"OFFICIAL_PRIMARY", "OFFICIAL_SECONDARY"}
_ALLOWED_STATUS = {"OK", "SOURCE_UNAVAILABLE", "SOURCE_INVALID", "TIMESTAMP_UNPROVABLE"}
_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")
_ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"


class RssAcquisitionError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        if code not in _ALLOWED_STATUS - {"OK"}:
            raise ValueError("invalid RSS acquisition error code")
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class RssFeedBinding:
    source_id: str
    source_tier: str
    feed_url: str
    allowed_host: str
    additional_allowed_hosts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_id:
            raise ValueError("source_id must be non-empty")
        if self.source_tier not in _ALLOWED_TIERS:
            raise ValueError("RSS live slice accepts official sources only")
        parsed = urlparse(self.feed_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("feed_url must be explicit HTTPS URL")
        primary_host = _normalize_host(self.allowed_host)
        if _normalize_host(parsed.hostname) != primary_host:
            raise ValueError("allowed_host must match feed_url host")
        seen = {primary_host}
        for host in self.additional_allowed_hosts:
            normalized = _normalize_host(host)
            if not normalized:
                raise ValueError("additional_allowed_hosts must contain non-empty hostnames")
            if normalized in seen:
                raise ValueError("additional_allowed_hosts must be unique and exclude allowed_host")
            seen.add(normalized)

    @property
    def item_allowed_hosts(self) -> tuple[str, ...]:
        return (self.allowed_host,) + self.additional_allowed_hosts


@dataclass(frozen=True)
class RssSourceResult:
    source_id: str
    quality_status: str
    records: tuple[NewsSourceRecord, ...]
    future_items_skipped: int = 0
    error: str | None = None

    def __post_init__(self) -> None:
        if self.quality_status not in _ALLOWED_STATUS:
            raise ValueError("invalid quality_status")
        if self.quality_status == "OK" and self.error is not None:
            raise ValueError("OK source result may not include error")
        if self.quality_status != "OK" and self.records:
            raise ValueError("failed source result may not include records")


@dataclass(frozen=True)
class RssBatchResult:
    source_results: tuple[RssSourceResult, ...]

    @property
    def records(self) -> tuple[NewsSourceRecord, ...]:
        return tuple(
            record
            for source_result in self.source_results
            if source_result.quality_status == "OK"
            for record in source_result.records
        )

    @property
    def failures(self) -> tuple[RssSourceResult, ...]:
        return tuple(item for item in self.source_results if item.quality_status != "OK")

    @property
    def ok_source_count(self) -> int:
        return sum(item.quality_status == "OK" for item in self.source_results)


def _normalize_host(host: str) -> str:
    value = host.casefold().rstrip(".")
    return value[4:] if value.startswith("www.") else value


def _load_registry(path: Path | str) -> Mapping[str, object]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("unable to load Stage 12A source registry") from exc
    if not isinstance(value, Mapping):
        raise ValueError("Stage 12A source registry must be a mapping")
    return value


def load_first_slice_bindings(
    *,
    registry_path: Path | str = SOURCE_REGISTRY_PATH,
    source_ids: Iterable[str] = FIRST_SLICE_SOURCE_IDS,
) -> tuple[RssFeedBinding, ...]:
    contract = _load_registry(registry_path)
    sources_raw = contract.get("primary_sources")
    if not isinstance(sources_raw, list):
        raise ValueError("source registry primary_sources must be a list")
    by_id = {
        item.get("source_id"): item
        for item in sources_raw
        if isinstance(item, Mapping) and isinstance(item.get("source_id"), str)
    }

    bindings: list[RssFeedBinding] = []
    seen: set[str] = set()
    for source_id in source_ids:
        if source_id in seen:
            raise ValueError(f"duplicate source_id requested: {source_id}")
        seen.add(source_id)
        try:
            item = by_id[source_id]
        except KeyError as exc:
            raise ValueError(f"source not present in Stage 12A registry: {source_id}") from exc
        if item.get("stage12b_status") != "READY_CANDIDATE":
            raise ValueError(f"source is not Stage 12B ready: {source_id}")
        if item.get("transport") != "RSS":
            raise ValueError(f"source is not an RSS source: {source_id}")
        tier = item.get("tier")
        if tier not in _ALLOWED_TIERS:
            raise ValueError(f"source is not an official source: {source_id}")
        refs = item.get("references")
        if not isinstance(refs, list) or len(refs) != 1 or not isinstance(refs[0], str):
            raise ValueError(f"RSS source must have one explicit reference: {source_id}")
        feed_url = refs[0]
        parsed = urlparse(feed_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError(f"RSS source must use HTTPS: {source_id}")
        primary_host = parsed.hostname
        allowed_item_hosts = item.get("item_link_allowed_hosts")
        additional_allowed_hosts: tuple[str, ...] = ()
        if allowed_item_hosts is not None:
            if (
                not isinstance(allowed_item_hosts, list)
                or not allowed_item_hosts
                or not all(isinstance(host, str) and host for host in allowed_item_hosts)
            ):
                raise ValueError(
                    f"item_link_allowed_hosts must be a non-empty hostname list: {source_id}"
                )
            normalized_hosts = [_normalize_host(host) for host in allowed_item_hosts]
            if len(normalized_hosts) != len(set(normalized_hosts)):
                raise ValueError(f"item_link_allowed_hosts contains duplicates: {source_id}")
            normalized_primary = _normalize_host(primary_host)
            if normalized_primary not in normalized_hosts:
                raise ValueError(
                    f"item_link_allowed_hosts must include the RSS feed host: {source_id}"
                )
            additional_allowed_hosts = tuple(
                host
                for host in allowed_item_hosts
                if _normalize_host(host) != normalized_primary
            )
        bindings.append(
            RssFeedBinding(
                source_id=source_id,
                source_tier=tier,
                feed_url=feed_url,
                allowed_host=primary_host,
                additional_allowed_hosts=additional_allowed_hosts,
            )
        )
    return tuple(bindings)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].casefold()


def _child_text(item: ET.Element, names: tuple[str, ...]) -> str | None:
    wanted = {name.casefold() for name in names}
    for child in item:
        if _local_name(child.tag) in wanted:
            text = "".join(child.itertext()).strip()
            if text:
                return text
    return None


def _item_link(item: ET.Element) -> str | None:
    for child in item:
        if _local_name(child.tag) != "link":
            continue
        href = child.attrib.get("href")
        if href and href.strip():
            return href.strip()
        text = "".join(child.itertext()).strip()
        if text:
            return text
    return None


def _parse_timestamp(value: str) -> datetime:
    text = value.strip()
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RssAcquisitionError(
                "TIMESTAMP_UNPROVABLE",
                "RSS item publication timestamp is not parseable",
            ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RssAcquisitionError(
            "TIMESTAMP_UNPROVABLE",
            "RSS item publication timestamp is timezone-naive",
        )
    return parsed


def _bounded_description(value: str | None, *, max_chars: int) -> str:
    if not value:
        return ""
    cleaned = _SPACE_RE.sub(" ", unescape(_TAG_RE.sub(" ", value))).strip()
    return cleaned[:max_chars]


def _validate_item_link(url: str, *, allowed_hosts: Iterable[str]) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise RssAcquisitionError("SOURCE_INVALID", "RSS item link must be HTTPS")
    normalized_allowed = {_normalize_host(host) for host in allowed_hosts}
    if _normalize_host(parsed.hostname) not in normalized_allowed:
        raise RssAcquisitionError(
            "SOURCE_INVALID",
            "RSS item link host does not match the registered publisher allowlist",
        )
    return url


def _validate_final_response_url(response: object, *, allowed_host: str) -> str:
    geturl = getattr(response, "geturl", None)
    if not callable(geturl):
        raise RssAcquisitionError(
            "SOURCE_INVALID",
            "RSS response does not expose final URL",
        )
    try:
        final_url = geturl()
    except Exception as exc:
        raise RssAcquisitionError(
            "SOURCE_UNAVAILABLE",
            "RSS final response URL could not be resolved",
        ) from exc
    if not isinstance(final_url, str):
        raise RssAcquisitionError("SOURCE_INVALID", "RSS final response URL must be a string")
    parsed = urlparse(final_url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise RssAcquisitionError("SOURCE_INVALID", "RSS final response URL must be HTTPS")
    if _normalize_host(parsed.hostname) != _normalize_host(allowed_host):
        raise RssAcquisitionError(
            "SOURCE_INVALID",
            "RSS final response host does not match the registered publisher",
        )
    return final_url


def _read_response(response: object, *, max_bytes: int) -> bytes:
    read = getattr(response, "read", None)
    if not callable(read):
        raise RssAcquisitionError("SOURCE_UNAVAILABLE", "RSS response has no readable body")
    try:
        raw = read(max_bytes + 1)
    except Exception as exc:
        raise RssAcquisitionError("SOURCE_UNAVAILABLE", "RSS response read failed") from exc
    if not isinstance(raw, (bytes, bytearray)):
        raise RssAcquisitionError("SOURCE_INVALID", "RSS response body must be bytes")
    if len(raw) > max_bytes:
        raise RssAcquisitionError("SOURCE_INVALID", "RSS response exceeded maximum size")
    return bytes(raw)


def _valid_empty_feed_structure(root: ET.Element) -> bool:
    root_name = _local_name(root.tag)
    if root_name == "rss" and not root.tag.startswith("{"):
        channels = [child for child in root if _local_name(child.tag) == "channel"]
        if len(channels) != 1:
            return False
        channel = channels[0]
        return all(
            _child_text(channel, (field,)) is not None
            for field in ("title", "link", "description")
        )
    if root.tag == f"{{{_ATOM_NAMESPACE}}}feed":
        return all(
            _child_text(root, (field,)) is not None
            for field in ("title", "id", "updated")
        )
    return False


def _parse_rss_records(
    binding: RssFeedBinding,
    raw: bytes,
    *,
    ingested_at: datetime,
    max_description_chars: int,
) -> tuple[tuple[NewsSourceRecord, ...], int]:
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise RssAcquisitionError("SOURCE_INVALID", "RSS response is not valid XML") from exc

    items = [element for element in root.iter() if _local_name(element.tag) in {"item", "entry"}]
    if not items:
        if _valid_empty_feed_structure(root):
            return (), 0
        raise RssAcquisitionError("SOURCE_INVALID", "RSS feed contains no items")

    records: list[NewsSourceRecord] = []
    future_items = 0
    for item in items:
        title = _child_text(item, ("title",))
        link = _item_link(item)
        published_text = _child_text(item, ("pubDate", "published", "updated", "date"))
        if not title or not link:
            raise RssAcquisitionError(
                "SOURCE_INVALID",
                "RSS item is missing required title or link",
            )
        if not published_text:
            raise RssAcquisitionError(
                "TIMESTAMP_UNPROVABLE",
                "RSS item is missing publication timestamp",
            )
        published_at = _parse_timestamp(published_text)
        if published_at > ingested_at:
            future_items += 1
            continue
        source_reference = _validate_item_link(
            link,
            allowed_hosts=binding.item_allowed_hosts,
        )
        body = _bounded_description(
            _child_text(item, ("description", "summary", "content")),
            max_chars=max_description_chars,
        )
        records.append(
            NewsSourceRecord(
                source_id=binding.source_id,
                source_tier=binding.source_tier,
                source_reference=source_reference,
                published_at=published_at,
                available_at=published_at,
                ingested_at=ingested_at,
                headline=title,
                body=body,
            )
        )
    return tuple(records), future_items


def fetch_rss_source(
    binding: RssFeedBinding,
    *,
    opener: Callable[..., object] = urlopen,
    now_fn: Callable[[], datetime] | None = None,
    timeout_seconds: float = 10.0,
    max_bytes: int = 2_000_000,
    max_description_chars: int = 2_000,
) -> RssSourceResult:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if max_bytes <= 0 or max_description_chars < 0:
        raise ValueError("RSS size limits must be non-negative and max_bytes positive")
    now = (now_fn or (lambda: datetime.now(timezone.utc)))()
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now_fn must return timezone-aware datetime")

    request = Request(
        binding.feed_url,
        headers={
            "User-Agent": "MOEX_Bot/rub-intelligence-stage12b1",
            "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.1",
        },
        method="GET",
    )
    try:
        response = opener(request, timeout=timeout_seconds)
        _validate_final_response_url(response, allowed_host=binding.allowed_host)
        raw = _read_response(response, max_bytes=max_bytes)
        records, future_items = _parse_rss_records(
            binding,
            raw,
            ingested_at=now,
            max_description_chars=max_description_chars,
        )
    except RssAcquisitionError:
        raise
    except Exception as exc:
        raise RssAcquisitionError("SOURCE_UNAVAILABLE", "RSS request failed") from exc

    return RssSourceResult(
        source_id=binding.source_id,
        quality_status="OK",
        records=records,
        future_items_skipped=future_items,
    )


def fetch_official_rss_batch(
    *,
    registry_path: Path | str = SOURCE_REGISTRY_PATH,
    source_ids: Iterable[str] = LIVE_RSS_SOURCE_IDS,
    opener: Callable[..., object] = urlopen,
    now_fn: Callable[[], datetime] | None = None,
    timeout_seconds: float = 10.0,
) -> RssBatchResult:
    bindings = load_first_slice_bindings(
        registry_path=registry_path,
        source_ids=source_ids,
    )
    results: list[RssSourceResult] = []
    for binding in bindings:
        try:
            result = fetch_rss_source(
                binding,
                opener=opener,
                now_fn=now_fn,
                timeout_seconds=timeout_seconds,
            )
        except RssAcquisitionError as exc:
            result = RssSourceResult(
                source_id=binding.source_id,
                quality_status=exc.code,
                records=(),
                error=str(exc),
            )
        results.append(result)
    return RssBatchResult(tuple(results))
