from __future__ import annotations

from datetime import datetime, timezone
from threading import Barrier

import pytest

from src.moex_research.runners import s7_3_parallel_component_prefetch as prefetch
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base


NOW = datetime(2026, 9, 3, 6, 0, 0, tzinfo=timezone.utc)


def test_prefetch_runs_independent_producers_concurrently() -> None:
    barrier = Barrier(2, timeout=1.0)

    def producer(name: str):
        def run(now: datetime) -> base.ProducedComponent:
            assert now == NOW
            barrier.wait()
            return base.ProducedComponent(data={"name": name}, data_as_of=now)

        return run

    cached = prefetch.prefetch_producers(
        {"a": producer("a"), "b": producer("b")},
        now=NOW,
        max_workers=2,
    )

    assert cached["a"](NOW).data == {"name": "a"}
    assert cached["b"](NOW).data == {"name": "b"}


def test_prefetch_preserves_component_failure_for_existing_fail_closed_wrapper() -> None:
    def failing(_now: datetime) -> base.ProducedComponent:
        raise RuntimeError("source failed")

    cached = prefetch.prefetch_producers({"failing": failing}, now=NOW)

    with pytest.raises(RuntimeError, match="source failed"):
        cached["failing"](NOW)


def test_prefetch_returns_deterministic_name_order() -> None:
    def producer(name: str):
        return lambda now: base.ProducedComponent(data={"name": name}, data_as_of=now)

    cached = prefetch.prefetch_producers(
        {"z": producer("z"), "a": producer("a"), "m": producer("m")},
        now=NOW,
        max_workers=3,
    )

    assert list(cached) == ["a", "m", "z"]
