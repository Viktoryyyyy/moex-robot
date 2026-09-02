from __future__ import annotations

from datetime import datetime, timezone
import json

from src.moex_research.intelligence.usdrubf_news_macro import (
    NewsSourceRecord,
    process_news_batch,
)
from src.moex_research.intelligence.usdrubf_news_macro_runtime import JsonSnapshotStore


T0 = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 9, 1, 10, 1, tzinfo=timezone.utc)
T2 = datetime(2026, 9, 1, 10, 2, tzinfo=timezone.utc)


def _record(source_id: str, reference: str, headline: str, *, available_at=T1) -> NewsSourceRecord:
    return NewsSourceRecord(
        source_id=source_id,
        source_tier="OFFICIAL_PRIMARY",
        source_reference=reference,
        published_at=T0,
        available_at=available_at,
        ingested_at=max(available_at, T2),
        headline=headline,
    )


def _classification() -> dict[str, object]:
    return {
        "event_type": "OFFICIAL_COMMUNICATION",
        "entities": ["official_source"],
        "rub_relevance": 0.0,
        "direction": "NEUTRAL",
        "importance": "LOW",
        "novelty": "NEW",
        "horizon": "SHORT_TERM",
        "confidence": 0.0,
        "mechanism": "No directional effect is inferred.",
    }


def test_exact_duplicate_sources_collapse_to_one_event_but_keep_durable_provenance() -> None:
    calls: list[dict[str, object]] = []
    first = _record("official_a", "https://a.example/release", "Same official release")
    second = _record("official_b", "https://b.example/release", "Same official release")

    result = process_news_batch(
        [second, first],
        as_of_timestamp=T2,
        classifier=lambda payload: calls.append(dict(payload)) or _classification(),
    )

    assert result.exact_duplicates_removed == 1
    assert result.clusters_classified == 1
    assert len(result.events) == 1
    assert len(calls) == 1
    assert len(calls[0]["cluster_evidence"]) == 1
    event = result.events[0]
    assert [item.source_id for item in event.source_provenance] == ["official_a", "official_b"]
    assert [item.source_reference for item in event.source_provenance] == [
        "https://a.example/release",
        "https://b.example/release",
    ]
    assert event.source_provenance_total_count == 2
    assert event.source_provenance_truncated is False


def test_near_duplicate_cluster_persists_all_source_provenance_without_changing_classifier_evidence() -> None:
    calls: list[dict[str, object]] = []
    records = [
        _record("official_a", "https://a.example/release", "Policy decision announced"),
        _record(
            "official_b",
            "https://b.example/release",
            "Policy decision officially announced",
            available_at=T2,
        ),
    ]

    result = process_news_batch(
        records,
        as_of_timestamp=T2,
        classifier=lambda payload: calls.append(dict(payload)) or _classification(),
        similarity_threshold=0.5,
    )

    assert len(result.events) == 1
    assert len(calls[0]["cluster_evidence"]) == 2
    assert {item.source_id for item in result.events[0].source_provenance} == {
        "official_a",
        "official_b",
    }


def test_provenance_order_is_deterministic_and_bound_is_explicit() -> None:
    records = [
        _record(f"source_{index:02d}", f"https://example.test/{index:02d}", "Same bounded release")
        for index in range(18)
    ]

    forward = process_news_batch(
        records,
        as_of_timestamp=T2,
        classifier=lambda _payload: _classification(),
    ).events[0]
    reverse = process_news_batch(
        reversed(records),
        as_of_timestamp=T2,
        classifier=lambda _payload: _classification(),
    ).events[0]

    expected_ids = [f"source_{index:02d}" for index in range(16)]
    assert [item.source_id for item in forward.source_provenance] == expected_ids
    assert forward.source_provenance == reverse.source_provenance
    assert forward.source_provenance_total_count == 18
    assert forward.source_provenance_truncated is True


def test_truncated_provenance_preserves_distinct_source_before_filling_extra_records() -> None:
    dominant = [
        _record(
            "official_a",
            f"https://a.example/release/{index:02d}",
            "Same crowded release",
        )
        for index in range(20)
    ]
    secondary = _record(
        "official_b",
        "https://b.example/release",
        "Same crowded release",
        available_at=T2,
    )

    forward = process_news_batch(
        [*dominant, secondary],
        as_of_timestamp=T2,
        classifier=lambda _payload: _classification(),
    ).events[0]
    reverse = process_news_batch(
        reversed([*dominant, secondary]),
        as_of_timestamp=T2,
        classifier=lambda _payload: _classification(),
    ).events[0]

    assert len(forward.source_provenance) == 16
    assert {item.source_id for item in forward.source_provenance} == {
        "official_a",
        "official_b",
    }
    assert sum(item.source_id == "official_b" for item in forward.source_provenance) == 1
    assert forward.source_provenance == reverse.source_provenance
    assert forward.source_provenance_total_count == 21
    assert forward.source_provenance_truncated is True


def test_snapshot_serializes_provenance_without_raw_content(tmp_path) -> None:
    event = process_news_batch(
        [
            _record("official_a", "https://a.example/release", "Same persisted release"),
            _record("official_b", "https://b.example/release", "Same persisted release"),
        ],
        as_of_timestamp=T2,
        classifier=lambda _payload: _classification(),
    ).events[0]

    path = JsonSnapshotStore(tmp_path).save_news_events((event,))
    payload = json.loads(path.read_text(encoding="utf-8"))[0]

    assert [item["source_id"] for item in payload["source_provenance"]] == [
        "official_a",
        "official_b",
    ]
    assert payload["source_provenance_total_count"] == 2
    assert payload["source_provenance_truncated"] is False
    assert "body" not in payload
    assert "normalized_text" not in payload
