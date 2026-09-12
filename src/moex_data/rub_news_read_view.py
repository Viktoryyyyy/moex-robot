"""Publication-window selection from retained snapshot metadata, never live fetches."""
from copy import deepcopy
from types import SimpleNamespace

from moex_research.intelligence.rub_news_selection import select, _time, CURRENT_POLICY

PRIMARY = ('source_id', 'source_tier', 'source_reference', 'published_at', 'available_at', 'ingested_at', 'content_hash')


def compact_primary(event, *, identity_proven, audit_ref):
    value = deepcopy(event)
    value.pop('source_provenance', None)
    value.pop('source_provenance_total_count', None)
    value.pop('source_provenance_truncated', None)
    value['primary_provenance'] = {key: event.get(key) for key in PRIMARY}
    value['publication_identity_policy'] = 'exact_reference_publication_utc_content_hash.v2' if identity_proven else 'legacy_primary_only_mixed_cluster_identity_unproven'
    value['audit_ref'] = deepcopy(audit_ref)
    return value


def project(component, *, now):
    component = component if isinstance(component, dict) else {}
    data = component.get('data'); data = data if isinstance(data, dict) else {}
    retained = data.get('retained_event_pool')
    scope = 'retained_eligible_publications_at_capture' if isinstance(retained, list) else 'legacy_selected_records_only_no_unselected_candidates'
    pool = retained if isinstance(retained, list) else data.get('legacy_selected_event_pool', data.get('events', []))
    pool = pool if isinstance(pool, list) else []
    candidates = []; rejected = 0
    if component.get('status') in {'READY', 'PARTIAL', 'RETAINED_PREVIOUS'}:
        for event in pool:
            try:
                if not isinstance(event, dict): raise ValueError('invalid event')
                stamps = [_time(event[key]) for key in ('published_at', 'available_at', 'ingested_at')]
                if not stamps[0] <= stamps[1] <= stamps[2] <= now: raise ValueError('noncausal event')
                if not all(isinstance(event.get(key), str) and event[key] for key in ('event_id', 'source_id', 'source_reference')):
                    raise ValueError('missing event identity')
                value = deepcopy(event); value.setdefault('quality_status', 'OK')
                value.setdefault('content_hash', None)
                if value.get('publication_identity_policy') == 'exact_reference_publication_utc_content_hash.v2':
                    from moex_research.intelligence.usdrubf_news_macro import publication_identity
                    publication_identity(value['source_reference'], value['published_at'], value['content_hash'])
                candidates.append(SimpleNamespace(**value))
            except (KeyError, ValueError, TypeError, OverflowError): rejected += 1
    by_id = {}; conflicts = set()
    for candidate in candidates:
        prior = by_id.get(candidate.event_id)
        if prior is not None and vars(prior) != vars(candidate): conflicts.add(candidate.event_id)
        by_id[candidate.event_id] = candidate
    rejected += sum(candidate.event_id in conflicts for candidate in candidates)
    selected, selection = select([candidate for key, candidate in by_id.items() if key not in conflicts], limit=20, as_of=now)
    summary = data.get('summary'); summary = summary if isinstance(summary, dict) else {}
    capture = summary.get('selection_audit'); capture = capture if isinstance(capture, dict) else {}
    audit_ref = capture.get('audit_ref') or ({key: capture[key] for key in ('path', 'sha256') if key in capture} or None)
    events = []
    for candidate in selected:
        raw = vars(candidate)
        proven = raw.get('publication_identity_policy') == 'exact_reference_publication_utc_content_hash.v2'
        item = compact_primary(raw, identity_proven=proven, audit_ref=audit_ref)
        seconds = (now - _time(item['published_at'])).total_seconds()
        band = selection['selected_bands'][item['event_id']]
        item.update(publication_age_seconds_at_as_of=seconds, age_reference_utc=now.isoformat(),
                    selection_band=band, retention_reason='published_within_24h' if band == 'fresh' else 'latest_eligible_source_background_within_7d')
        events.append(item)
    return {'events': events, 'selection_at_read': {key: value for key, value in selection.items() if key != 'candidates'},
            'selection_scope': scope, 'invalid_causal_or_identity_count': rejected,
            'pool_candidate_count': len(pool), 'selection_policy': CURRENT_POLICY}


def apply(snapshot, *, now):
    components = snapshot.get('components')
    component = components.get('official_news') if isinstance(components, dict) else None
    if not isinstance(component, dict) or not isinstance(component.get('data'), dict): return
    data = component['data']
    # Freeze the finite legacy input pool before the first read, retaining no raw bodies.
    if 'retained_event_pool' not in data:
        data['legacy_selected_event_pool'] = deepcopy(data.get('legacy_selected_event_pool', data.get('events', [])))
        temporary = deepcopy(component); temporary['data']['events'] = data['legacy_selected_event_pool']
    else: temporary = component
    view = project(temporary, now=now)
    data['events'] = view.pop('events')
    data['consumption_selection'] = view
