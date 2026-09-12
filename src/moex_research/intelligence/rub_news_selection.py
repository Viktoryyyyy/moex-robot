"""Deterministic source coverage and replayable metadata for bounded news views."""
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path

POLICY = 'rub_news_source_coverage.v1'
CURRENT_POLICY = 'rub_news_publication_windows.v2'
FRESH_SECONDS = 86400
BACKGROUND_SECONDS = 7 * FRESH_SECONDS


def _time(value):
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.utcoffset() is None:
        raise ValueError('news selection requires aware timestamps')
    return parsed


def select_v1(events, *, limit, as_of):
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


def select(events, *, limit, as_of, policy=CURRENT_POLICY):
    if policy == POLICY:
        return select_v1(events, limit=limit, as_of=as_of)
    if policy != CURRENT_POLICY:
        raise ValueError('unsupported news selection policy')
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError('positive news limit required')
    from .usdrubf_news_macro import publication_identity
    as_of = _time(as_of); limit = min(limit, 20)
    unique = {}
    for event in events:
        if not _time(event.published_at) <= _time(event.available_at) <= _time(event.ingested_at) <= as_of:
            raise ValueError('news candidate violates causal order')
        if event.event_id in unique and unique[event.event_id] != event:
            raise ValueError('conflicting news event identity')
        unique[event.event_id] = event
    order = lambda e: (_time(e.published_at), e.source_id, e.event_id)
    candidates = sorted(unique.values(), key=order, reverse=True)
    pools = {'fresh': defaultdict(list), 'background': defaultdict(list)}
    identities = set(); rejected = []; bands = {}
    for event in candidates:
        try:
            identity = publication_identity(event.source_reference, event.published_at, event.content_hash)
        except ValueError:
            # Legacy missing version proof cannot establish equivalence to another event.
            identity = ('legacy_unproven_event', event.event_id)
        seconds = (as_of - _time(event.published_at)).total_seconds()
        if event.quality_status != 'OK' or identity in identities or seconds > BACKGROUND_SECONDS:
            rejected.append(event.event_id); continue
        identities.add(identity)
        band = 'fresh' if seconds <= FRESH_SECONDS else 'background'
        bands[event.event_id] = band
        pools[band][event.source_id].append(event)
    selected = []
    for band, bound in (('fresh', limit), ('background', min(4, limit - len(selected)))):
        # Calculate the remaining background budget after the fresh pool is drained.
        if band == 'background': bound = min(4, limit - len(selected))
        groups = pools[band]; count = 0
        while groups and count < bound:
            for source in sorted(groups, key=lambda source: order(groups[source][0]), reverse=True):
                if count == bound: break
                selected.append(groups[source].pop(0)); count += 1
                if not groups[source]: del groups[source]
    selected.sort(key=order)
    audit = {'policy': CURRENT_POLICY, 'as_of': as_of.isoformat(), 'limit': limit,
        'candidate_count': len(candidates), 'selected_ids': [e.event_id for e in selected],
        'eligible_event_count': len(identities), 'events_dropped_by_bound': len(identities) - len(selected),
        'excluded_quality_or_cluster_ids': rejected,
        'eligible_source_count': len({e.source_id for e in candidates if e.event_id in bands}),
        'selected_source_count': len({e.source_id for e in selected}),
        'relevance_validation_complete': False,
        'selection_semantics': 'publication_fresh_first_then_bounded_source_background_not_predicted_importance',
        'selected_bands': {e.event_id: bands[e.event_id] for e in selected},
        'fresh_horizon_seconds': FRESH_SECONDS, 'background_horizon_seconds': BACKGROUND_SECONDS,
        'background_limit': 4, 'candidates': [asdict(e) if hasattr(e, '__dataclass_fields__') else vars(e) for e in candidates]}
    return tuple(selected), audit


def freeze_audit_v1(audit, *, root):
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


def freeze_audit(audit, *, root, records=(), source_results=()):
    if audit.get('policy') == POLICY:
        return freeze_audit_v1(audit, root=root)
    from .rub_news_audit import freeze
    return freeze(audit, root=root, records=records, source_results=source_results)


def events_from_dicts(candidates):
    """Restore existing dataclasses without extending legacy persisted shapes."""
    from .usdrubf_news_macro import NewsEvent, NewsSourceProvenance
    result = []
    for candidate in candidates:
        value = dict(candidate)
        value['entities'] = tuple(value.get('entities', ()))
        value['source_provenance'] = tuple(NewsSourceProvenance(**item) for item in value.get('source_provenance', ()))
        result.append(NewsEvent(**value))
    return tuple(result)


def replay(audit):
    """Pure selection replay; v1 retains the original candidate JSON field shapes."""
    selected, rebuilt = select(events_from_dicts(audit['candidates']), limit=audit['limit'],
                               as_of=audit['as_of'], policy=audit['policy'])
    original = {item['event_id']: item for item in audit['candidates']}
    rebuilt['candidates'] = [original[item['event_id']] for item in rebuilt['candidates']]
    return selected, rebuilt
