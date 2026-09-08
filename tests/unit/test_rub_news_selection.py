from dataclasses import replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import pytest
from moex_research.intelligence.usdrubf_news_macro import NewsEvent
from moex_research.intelligence.rub_news_selection import select, freeze_audit

NOW = datetime(2026, 9, 8, 4, 0, tzinfo=timezone.utc)


def event(i, source='busy'):
    stamp = (NOW - timedelta(minutes=i)).isoformat()
    return NewsEvent(event_id=f'e{i}', cluster_id=f'c{i}', source_id=source,
        source_tier='OFFICIAL_PRIMARY', source_reference=f'https://example.org/{i}',
        published_at=stamp, available_at=stamp, ingested_at=NOW.isoformat(),
        content_hash=sha256(str(i).encode()).hexdigest(), event_type='OFFICIAL_COMMUNICATION',
        entities=(), rub_relevance=0., direction='NEUTRAL', importance='LOW',
        novelty='NEW', horizon='SHORT_TERM', confidence=0., mechanism='placeholder')


def test_busy_feed_cannot_displace_other_sources_and_selection_replays(tmp_path):
    events = [event(i) for i in range(100)] + [event(100, 'cbr'), event(101, 'treasury')]
    selected, audit = select(events, limit=20, as_of=NOW)
    assert len(selected) == 20
    assert {e.source_id for e in selected} == {'busy', 'cbr', 'treasury'}
    assert audit['candidate_count'] == 102
    assert audit['relevance_validation_complete'] is False
    reverse, again = select(reversed(events), limit=20, as_of=NOW)
    assert reverse == selected and again == audit
    evidence = freeze_audit(audit, root=tmp_path)
    raw = Path(evidence['path']).read_bytes()
    assert sha256(raw).hexdigest() == evidence['sha256']
    assert len(json.loads(raw)['candidates']) == 102
    assert b'headline' not in raw and b'body' not in raw


def test_one_event_per_cluster_and_no_bad_quality():
    latest = event(1)
    older = replace(event(2), cluster_id=latest.cluster_id)
    bad = replace(event(3), quality_status='TIMESTAMP_UNPROVABLE')
    selected, audit = select([latest, latest, older, bad], limit=20, as_of=NOW)
    assert selected == (latest,)
    assert set(audit['excluded_quality_or_cluster_ids']) == {'e2', 'e3'}


def test_conflicting_identity_and_future_candidates_fail_even_outside_limit():
    first = event(1)
    with pytest.raises(ValueError, match='identity'):
        select([first, replace(first, content_hash='b' * 64)], limit=1, as_of=NOW)
    future = replace(event(99), ingested_at=(NOW + timedelta(seconds=1)).isoformat())
    with pytest.raises(ValueError, match='causal'):
        select([first, future], limit=1, as_of=NOW)


def test_empty_candidates_and_insufficient_source_budget_are_explicit():
    selected, audit = select([], limit=20, as_of=NOW)
    assert selected == () and audit['selected_source_count'] == 0
    selected, audit = select([event(1, 'a'), event(2, 'b')], limit=1, as_of=NOW)
    assert audit['eligible_source_count'] == 2 and audit['selected_source_count'] == 1


def test_macro_collection_is_not_full_coverage_or_neutral_analysis(monkeypatch):
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
    state = {'observations': [{'metric_id': 'cbr_key_rate_pct', 'value': 10}],
        'overall_direction': 'NEUTRAL', 'confidence': 0., 'dominant_drivers': []}
    monkeypatch.setattr(runner.live, '_load_current_cbr_macro_state', lambda: (state, NOW))
    view = runner._macro_component(NOW).data
    assert state['overall_direction'] == 'NEUTRAL'
    assert view['state']['overall_direction'] == 'UNKNOWN'
    assert view['state']['observations'] == state['observations']
    assert view['full_macro_complete'] is False
    assert set(view['missing_required_blocks']) == {'minfin_fx_operations', 'rosstat_macro', 'event_calendar'}
    assert view['numeric_release_surprise'] is None
