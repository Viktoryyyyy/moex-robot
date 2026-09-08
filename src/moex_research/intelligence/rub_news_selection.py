"""Deterministic source coverage and replayable metadata for bounded news views."""
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path

POLICY = 'rub_news_source_coverage.v1'


def _time(value):
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.utcoffset() is None:
        raise ValueError('news selection requires aware timestamps')
    return parsed


def select(events, *, limit, as_of):
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError('positive news limit required')
    as_of = _time(as_of)
    unique = {}
    for event in events:
        if not _time(event.published_at) <= _time(event.available_at) <= _time(event.ingested_at) <= as_of:
            raise ValueError('news candidate violates causal order')
        if event.event_id in unique and unique[event.event_id] != event:
            raise ValueError('conflicting news event identity')
        unique[event.event_id] = event
    order = lambda e: (_time(e.available_at), e.source_id, e.event_id)
    candidates = sorted(unique.values(), key=order, reverse=True)
    clusters, groups = set(), defaultdict(list)
    rejected = []
    for event in candidates:
        if event.quality_status != 'OK' or event.cluster_id in clusters:
            rejected.append(event.event_id)
            continue
        clusters.add(event.cluster_id)
        groups[event.source_id].append(event)
    # One newest event per source per round; a prolific feed cannot consume
    # all slots when the budget can represent every eligible source.
    selected = []
    while groups and len(selected) < limit:
        sources = sorted(groups, key=lambda source: order(groups[source][0]), reverse=True)
        for source in sources:
            if len(selected) == limit:
                break
            selected.append(groups[source].pop(0))
            if not groups[source]:
                del groups[source]
    selected.sort(key=order)
    audit = {'policy': POLICY, 'as_of': as_of.isoformat(), 'limit': limit,
        'candidate_count': len(candidates), 'selected_ids': [e.event_id for e in selected],
        'eligible_event_count': len(clusters),
        'events_dropped_by_bound': len(clusters) - len(selected),
        'excluded_quality_or_cluster_ids': rejected,
        'eligible_source_count': len({e.source_id for e in candidates if e.event_id not in rejected}),
        'selected_source_count': len({e.source_id for e in selected}),
        'relevance_validation_complete': False,
        'selection_semantics': 'source_coverage_then_recency_not_rub_importance',
        'candidates': [asdict(e) for e in candidates]}
    return tuple(selected), audit


def freeze_audit(audit, *, root):
    root = Path(root).resolve()
    directory = root / 'raw/news_selection'
    if not directory.resolve().is_relative_to(root):
        raise ValueError('news audit escapes data root')
    directory.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(audit, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
    digest = sha256(raw).hexdigest()
    path = directory / (digest + '.json')
    if path.is_symlink():
        raise ValueError('news audit symlink refused')
    try:
        with path.open('xb') as stream:
            stream.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise ValueError('news audit collision')
    return {'path': str(path), 'sha256': digest, **{k: v for k, v in audit.items() if k != 'candidates'}}
