from __future__ import annotations

from dataclasses import asdict

import pytest

from src.moex_research.intelligence.usdrubf_news_macro import (
    NewsEvent,
    NewsSourceProvenance,
)
from src.moex_research.intelligence.usdrubf_shadow_runtime import (
    ShadowRuntimeError,
    _news_event,
)


def _event() -> NewsEvent:
    provenance = (
        NewsSourceProvenance(
            source_id="official_a",
            source_tier="OFFICIAL_PRIMARY",
            source_reference="https://a.example/release",
            published_at="2026-09-01T10:00:00+00:00",
            available_at="2026-09-01T10:01:00+00:00",
            ingested_at="2026-09-01T10:02:00+00:00",
            content_hash="a" * 64,
        ),
        NewsSourceProvenance(
            source_id="official_b",
            source_tier="OFFICIAL_SECONDARY",
            source_reference="https://b.example/release",
            published_at="2026-09-01T10:00:00+00:00",
            available_at="2026-09-01T10:01:30+00:00",
            ingested_at="2026-09-01T10:02:00+00:00",
            content_hash="a" * 64,
        ),
    )
    return NewsEvent(
        event_id="event_1",
        cluster_id="cluster_1",
        source_id="official_b",
        source_tier="OFFICIAL_SECONDARY",
        source_reference="https://b.example/release",
        published_at="2026-09-01T10:00:00+00:00",
        available_at="2026-09-01T10:01:30+00:00",
        ingested_at="2026-09-01T10:02:00+00:00",
        content_hash="a" * 64,
        event_type="OFFICIAL_COMMUNICATION",
        entities=("official_source",),
        rub_relevance=0.0,
        direction="NEUTRAL",
        importance="LOW",
        novelty="NEW",
        horizon="SHORT_TERM",
        confidence=0.0,
        mechanism="No directional effect is inferred.",
        quality_status="OK",
        source_provenance=provenance,
        source_provenance_total_count=2,
        source_provenance_truncated=False,
    )


def test_shadow_news_restore_preserves_durable_source_provenance() -> None:
    original = _event()

    restored = _news_event(asdict(original), 0)

    assert restored.source_provenance == original.source_provenance
    assert restored.source_provenance_total_count == 2
    assert restored.source_provenance_truncated is False


def test_shadow_news_restore_keeps_legacy_event_compatible() -> None:
    payload = asdict(_event())
    payload.pop("source_provenance")
    payload.pop("source_provenance_total_count")
    payload.pop("source_provenance_truncated")

    restored = _news_event(payload, 0)

    assert restored.source_provenance == ()
    assert restored.source_provenance_total_count == 0
    assert restored.source_provenance_truncated is False


def test_shadow_news_restore_rejects_partial_provenance_metadata() -> None:
    payload = asdict(_event())
    payload.pop("source_provenance_total_count")

    with pytest.raises(ShadowRuntimeError, match="provenance metadata field set"):
        _news_event(payload, 0)


def test_shadow_news_restore_rejects_malformed_provenance_timestamp_order() -> None:
    payload = asdict(_event())
    payload["source_provenance"][0]["available_at"] = "2026-09-01T09:59:00+00:00"

    with pytest.raises(ShadowRuntimeError, match="provenance.*timestamp ordering"):
        _news_event(payload, 0)
