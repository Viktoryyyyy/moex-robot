from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base


DEFAULT_MAX_WORKERS = 4


@dataclass(frozen=True)
class _CapturedProducerResult:
    value: base.ProducedComponent | None
    error: Exception | None


def _capture(
    producer: base.ComponentProducer,
    now: datetime,
) -> _CapturedProducerResult:
    try:
        return _CapturedProducerResult(value=producer(now), error=None)
    except Exception as exc:
        return _CapturedProducerResult(value=None, error=exc)


def _cached_producer(captured: _CapturedProducerResult) -> base.ComponentProducer:
    def produce(_now: datetime) -> base.ProducedComponent:
        if captured.error is not None:
            raise captured.error
        if captured.value is None:
            raise base.ChatAnalysisSnapshotError("prefetched producer returned no value")
        return captured.value

    return produce


def prefetch_producers(
    producers: Mapping[str, base.ComponentProducer],
    *,
    now: datetime,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> dict[str, base.ComponentProducer]:
    if not isinstance(producers, Mapping):
        raise base.ChatAnalysisSnapshotError("component producers must be a mapping")
    names = sorted(str(name) for name in producers)
    if not names:
        return {}
    if len(names) != len(producers) or set(names) != set(producers):
        raise base.ChatAnalysisSnapshotError("component producer names must be unique strings")
    if isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers <= 0:
        raise base.ChatAnalysisSnapshotError("max_workers must be a positive integer")

    now_utc = base._aware(now, "parallel_component_prefetch.now")
    worker_count = min(max_workers, len(names))
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="s7_3_component",
    ) as pool:
        futures = {
            name: pool.submit(_capture, producers[name], now_utc)
            for name in names
        }
        captured = {name: futures[name].result() for name in names}

    return {
        name: _cached_producer(captured[name])
        for name in names
    }
