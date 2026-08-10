from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence
from urllib.request import urlopen

from .usdrubf_news_live_rss import (
    FIRST_SLICE_SOURCE_IDS,
    SOURCE_REGISTRY_PATH,
    RssBatchResult,
    fetch_official_rss_batch,
)
from .usdrubf_news_macro import NewsPipelineResult, process_news_batch


Classifier = Callable[[Mapping[str, object]], Mapping[str, object]]


@dataclass(frozen=True)
class LiveNewsPipelineResult:
    acquisition: RssBatchResult
    news: NewsPipelineResult
    as_of_timestamp: str

    @property
    def source_failure_count(self) -> int:
        return len(self.acquisition.failures)

    @property
    def acquired_record_count(self) -> int:
        return len(self.acquisition.records)


def _aware_datetime(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


def run_live_official_news_pipeline(
    *,
    classifier: Classifier,
    registry_path: Path | str = SOURCE_REGISTRY_PATH,
    source_ids: Iterable[str] = FIRST_SLICE_SOURCE_IDS,
    opener: Callable[..., object] = urlopen,
    now_fn: Callable[[], datetime] | None = None,
    as_of_timestamp: datetime | None = None,
    timeout_seconds: float = 10.0,
    prior_clusters: Mapping[str, Sequence[str]] | None = None,
    prior_event_history: Mapping[str, Sequence[Mapping[str, object]]] | None = None,
    similarity_threshold: float = 0.72,
) -> LiveNewsPipelineResult:
    """Acquire official RSS records and feed healthy records into News Pipeline.

    The classifier is an explicit caller-supplied boundary. This function does
    not provide an LLM, Flowise endpoint, heuristic news interpretation, or
    fallback classifier. Acquisition failures remain visible in the returned
    RssBatchResult while healthy records continue through the existing bounded
    process_news_batch() path.
    """

    if not callable(classifier):
        raise ValueError("classifier must be callable")

    now = _aware_datetime(
        (now_fn or (lambda: datetime.now(timezone.utc)))(),
        "now_fn result",
    )
    as_of = now if as_of_timestamp is None else _aware_datetime(as_of_timestamp, "as_of_timestamp")
    if as_of > now:
        raise ValueError("as_of_timestamp may not be later than acquisition time")

    acquisition = fetch_official_rss_batch(
        registry_path=registry_path,
        source_ids=source_ids,
        opener=opener,
        now_fn=lambda: now,
        timeout_seconds=timeout_seconds,
    )

    news = process_news_batch(
        acquisition.records,
        as_of_timestamp=as_of,
        classifier=classifier,
        prior_clusters=prior_clusters,
        prior_event_history=prior_event_history,
        similarity_threshold=similarity_threshold,
    )

    return LiveNewsPipelineResult(
        acquisition=acquisition,
        news=news,
        as_of_timestamp=as_of.isoformat(),
    )
