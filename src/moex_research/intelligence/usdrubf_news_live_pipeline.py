from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence
from urllib.request import urlopen

from .usdrubf_news_classifier_agent import (
    ClassifierAgent,
    stage12b3_news_classifier,
)
from .usdrubf_news_live_bls_dol import (
    BLS_DOL_SOURCE_IDS,
    fetch_bls_dol_mirror_batch,
)
from .usdrubf_news_live_rss import (
    LIVE_RSS_SOURCE_IDS,
    SOURCE_REGISTRY_PATH,
    RssBatchResult,
    RssSourceResult,
    fetch_official_rss_batch,
)
from .usdrubf_news_live_treasury import (
    SOURCE_ID as TREASURY_SOURCE_ID,
    TreasuryAcquisitionError,
    fetch_treasury_press_releases,
)
from .usdrubf_news_macro import NewsPipelineResult, process_news_batch


_BLS_RSS_SOURCE_IDS = frozenset(
    {
        "bls_employment_situation_rss",
        "bls_cpi_rss",
    }
)
_TREASURY_LIVE_TIMEOUT_SECONDS = 30.0
LIVE_OFFICIAL_SOURCE_IDS = (
    tuple(source_id for source_id in LIVE_RSS_SOURCE_IDS if source_id not in _BLS_RSS_SOURCE_IDS)
    + BLS_DOL_SOURCE_IDS
    + (TREASURY_SOURCE_ID,)
)


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


def deterministic_neutral_news_classifier(payload: Mapping[str, object]) -> Mapping[str, object]:
    """Conservative no-LLM classifier for factual live wiring.

    It deliberately assigns no directional or confidence authority. Mandatory
    Stage 12B.3 fields receive bounded neutral values only, while source-bound
    facts continue to come exclusively from deterministic acquisition.
    """

    history = payload.get("cluster_history")
    has_history = isinstance(history, Sequence) and not isinstance(
        history, (str, bytes, bytearray)
    ) and bool(history)
    return {
        "event_type": "OFFICIAL_COMMUNICATION",
        "entities": (),
        "rub_relevance": 0.0,
        "direction": "NEUTRAL",
        "importance": "LOW",
        "novelty": "UPDATE" if has_history else "NEW",
        "horizon": "SHORT_TERM",
        "confidence": 0.0,
        "mechanism": "Deterministic neutral classification; no directional effect is inferred.",
    }


def _restamp_live_acquisition(
    acquisition: RssBatchResult,
    *,
    ingested_at: datetime,
) -> RssBatchResult:
    """Conservatively stamp successful records no earlier than batch completion."""

    stamped_results = []
    for source_result in acquisition.source_results:
        records = tuple(
            replace(record, ingested_at=ingested_at)
            for record in source_result.records
        )
        stamped_results.append(replace(source_result, records=records))
    return RssBatchResult(tuple(stamped_results))


def _filter_acquisition_by_ingestion(
    acquisition: RssBatchResult,
    *,
    as_of: datetime,
) -> RssBatchResult:
    """Exclude records not yet ingested at an explicit historical PIT cutoff."""

    filtered_results = []
    for source_result in acquisition.source_results:
        records = tuple(
            record
            for record in source_result.records
            if record.ingested_at <= as_of
        )
        filtered_results.append(replace(source_result, records=records))
    return RssBatchResult(tuple(filtered_results))


def _acquire_treasury_source(
    *,
    registry_path: Path | str,
    opener: Callable[..., object],
    now_fn: Callable[[], datetime],
    timeout_seconds: float,
) -> RssBatchResult:
    try:
        result = fetch_treasury_press_releases(
            registry_path=registry_path,
            opener=opener,
            now_fn=now_fn,
            timeout_seconds=timeout_seconds,
        )
    except TreasuryAcquisitionError as exc:
        return RssBatchResult(
            (
                RssSourceResult(
                    source_id=TREASURY_SOURCE_ID,
                    quality_status=exc.code,
                    records=(),
                    error=str(exc),
                ),
            )
        )
    return RssBatchResult(
        (
            RssSourceResult(
                source_id=result.source_id,
                quality_status=result.quality_status,
                records=result.records,
                future_items_skipped=result.future_items_skipped,
                error=result.error,
            ),
        )
    )


def _acquire_official_sources(
    *,
    registry_path: Path | str,
    source_ids: Iterable[str],
    opener: Callable[..., object],
    now_fn: Callable[[], datetime],
    timeout_seconds: float,
) -> RssBatchResult:
    requested = tuple(source_ids)
    if len(requested) != len(set(requested)):
        raise ValueError("duplicate live News source_id requested")

    dol_ids = tuple(source_id for source_id in requested if source_id in BLS_DOL_SOURCE_IDS)
    treasury_requested = TREASURY_SOURCE_ID in requested
    rss_ids = tuple(
        source_id
        for source_id in requested
        if source_id not in BLS_DOL_SOURCE_IDS and source_id != TREASURY_SOURCE_ID
    )

    rss = (
        fetch_official_rss_batch(
            registry_path=registry_path,
            source_ids=rss_ids,
            opener=opener,
            now_fn=now_fn,
            timeout_seconds=timeout_seconds,
        )
        if rss_ids
        else RssBatchResult(())
    )
    dol = (
        fetch_bls_dol_mirror_batch(
            source_ids=dol_ids,
            opener=opener,
            now_fn=now_fn,
            timeout_seconds=timeout_seconds,
        )
        if dol_ids
        else RssBatchResult(())
    )
    treasury = (
        _acquire_treasury_source(
            registry_path=registry_path,
            opener=opener,
            now_fn=now_fn,
            timeout_seconds=max(timeout_seconds, _TREASURY_LIVE_TIMEOUT_SECONDS),
        )
        if treasury_requested
        else RssBatchResult(())
    )

    by_id = {
        item.source_id: item
        for item in rss.source_results + dol.source_results + treasury.source_results
    }
    if set(by_id) != set(requested):
        raise RuntimeError("live News acquisition did not return exactly the requested source set")
    return RssBatchResult(tuple(by_id[source_id] for source_id in requested))


def run_live_official_news_pipeline(
    *,
    classifier_agent: ClassifierAgent,
    registry_path: Path | str = SOURCE_REGISTRY_PATH,
    source_ids: Iterable[str] = LIVE_OFFICIAL_SOURCE_IDS,
    opener: Callable[..., object] = urlopen,
    now_fn: Callable[[], datetime] | None = None,
    as_of_timestamp: datetime | None = None,
    timeout_seconds: float = 10.0,
    prior_clusters: Mapping[str, Sequence[str]] | None = None,
    prior_event_history: Mapping[str, Sequence[Mapping[str, object]]] | None = None,
    similarity_threshold: float = 0.72,
) -> LiveNewsPipelineResult:
    """Acquire official records and run the bounded Stage 12B.3 classifier path.

    The default composition keeps healthy official RSS sources, replaces the
    production-blocked BLS RSS routes with bounded official DOL-hosted BLS
    release mirrors, and adds the bounded Treasury press-release HTML adapter.
    Treasury uses a source-specific 30-second live timeout floor because the
    official index has demonstrated response latency close to the common
    10-second source timeout; all other source timeout semantics remain unchanged.
    The caller supplies only the external classifier transport/callable. This
    live composition always wraps it with stage12b3_news_classifier() before any
    cluster reaches process_news_batch(), so callers cannot accidentally bypass
    the Stage 12B.3 PIT/input/output guard through this API.

    Acquisition failures remain visible while healthy records continue through
    the deterministic News Pipeline. For current-live use, successful source
    records are conservatively restamped at batch completion so HTTP latency can
    never be represented as pre-response ingestion. An explicit historical
    as-of additionally excludes records whose conservative ingestion stamp is
    later than that cutoff.
    """

    if not callable(classifier_agent):
        raise ValueError("classifier_agent must be callable")

    clock = now_fn or (lambda: datetime.now(timezone.utc))
    started_at = _aware_datetime(clock(), "now_fn result")
    explicit_as_of = (
        None
        if as_of_timestamp is None
        else _aware_datetime(as_of_timestamp, "as_of_timestamp")
    )
    if explicit_as_of is not None and explicit_as_of > started_at:
        raise ValueError("as_of_timestamp may not be later than acquisition time")

    acquisition = _acquire_official_sources(
        registry_path=registry_path,
        source_ids=source_ids,
        opener=opener,
        now_fn=clock,
        timeout_seconds=timeout_seconds,
    )

    completed_at = _aware_datetime(clock(), "now_fn result")
    if completed_at < started_at:
        raise ValueError("now_fn moved backwards during acquisition")
    acquisition = _restamp_live_acquisition(acquisition, ingested_at=completed_at)

    as_of = completed_at if explicit_as_of is None else explicit_as_of
    if as_of > completed_at:
        raise ValueError("as_of_timestamp may not be later than acquisition completion")
    if explicit_as_of is not None:
        acquisition = _filter_acquisition_by_ingestion(acquisition, as_of=as_of)

    bounded_classifier = stage12b3_news_classifier(classifier_agent)
    news = process_news_batch(
        acquisition.records,
        as_of_timestamp=as_of,
        classifier=bounded_classifier,
        prior_clusters=prior_clusters,
        prior_event_history=prior_event_history,
        similarity_threshold=similarity_threshold,
    )

    return LiveNewsPipelineResult(
        acquisition=acquisition,
        news=news,
        as_of_timestamp=as_of.isoformat(),
    )
