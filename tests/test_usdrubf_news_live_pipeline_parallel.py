from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from threading import Barrier

from src.moex_research.intelligence import usdrubf_news_live_pipeline as pipeline
from src.moex_research.intelligence.usdrubf_news_live_rss import (
    RssBatchResult,
    RssSourceResult,
)


NOW = datetime(2026, 9, 3, 10, 0, 0, tzinfo=timezone.utc)


def _ok(source_id: str) -> RssSourceResult:
    return RssSourceResult(
        source_id=source_id,
        quality_status="OK",
        records=(),
    )


def test_parallel_rss_sources_overlap_and_preserve_requested_order(monkeypatch) -> None:
    source_ids = ("rss_c", "rss_a", "rss_b")
    bindings = tuple(SimpleNamespace(source_id=source_id) for source_id in source_ids)
    barrier = Barrier(len(bindings))

    monkeypatch.setattr(
        pipeline,
        "load_first_slice_bindings",
        lambda **kwargs: bindings,
    )

    def fake_fetch(binding, **kwargs):
        barrier.wait(timeout=2.0)
        return _ok(binding.source_id)

    monkeypatch.setattr(pipeline, "fetch_rss_source", fake_fetch)

    result = pipeline._acquire_rss_sources_parallel(
        registry_path="unused.json",
        source_ids=source_ids,
        opener=lambda *args, **kwargs: None,
        now_fn=lambda: NOW,
        timeout_seconds=10.0,
    )

    assert tuple(item.source_id for item in result.source_results) == source_ids


def test_parallel_source_groups_overlap_and_preserve_requested_order(monkeypatch) -> None:
    rss_id = "rss_test"
    dol_id = pipeline.BLS_DOL_SOURCE_IDS[0]
    requested = (pipeline.EIA_SOURCE_ID, rss_id, pipeline.TREASURY_SOURCE_ID, dol_id)
    barrier = Barrier(4)

    def rss(**kwargs):
        barrier.wait(timeout=2.0)
        return RssBatchResult((_ok(rss_id),))

    def dol(**kwargs):
        barrier.wait(timeout=2.0)
        return RssBatchResult((_ok(dol_id),))

    def treasury(**kwargs):
        barrier.wait(timeout=2.0)
        return RssBatchResult((_ok(pipeline.TREASURY_SOURCE_ID),))

    def eia(**kwargs):
        barrier.wait(timeout=2.0)
        return RssBatchResult((_ok(pipeline.EIA_SOURCE_ID),))

    monkeypatch.setattr(pipeline, "_acquire_rss_sources_parallel", rss)
    monkeypatch.setattr(pipeline, "fetch_bls_dol_mirror_batch", dol)
    monkeypatch.setattr(pipeline, "_acquire_treasury_source", treasury)
    monkeypatch.setattr(pipeline, "_acquire_eia_source", eia)

    result = pipeline._acquire_official_sources(
        registry_path="unused.json",
        source_ids=requested,
        opener=lambda *args, **kwargs: None,
        now_fn=lambda: NOW,
        timeout_seconds=10.0,
        parallel=True,
    )

    assert tuple(item.source_id for item in result.source_results) == requested
