"""Compact consumer presentation of the existing admitted factual release."""
from copy import deepcopy
from datetime import datetime, timedelta
from hashlib import sha256
import json
import re
from zoneinfo import ZoneInfo

SCHEMA = 'rub_factual_package.v1'
MOSCOW = ZoneInfo('Europe/Moscow')
MARKETS = ('usdrubf', 'si_front', 'si_next', 'cnyrubf', 'cr_front', 'cr_next', 'cnyrub_tom')
AUDIT_KEYS = {'snapshot_path', 'evidence_path', 'verified_evidence_path', 'liquidity_evidence_path',
    'manifest_path', 'document_manifest_path', 'index_manifest_path', 'raw_path', 'path',
    'pointer_ref', 'partition_ref', 'manifest_ref', 'quality_report_ref', 'source_registry_ref',
    'source_contract_ref', 'contract_ref', 'runtime_contract_ref', 'level_structure_contract_ref',
    'level_structure_engine_ref', 'source_adapter_ref', 'retained_provenance', 'upstream_placeholder'}


def compact_values(value):
    """Remove named audit locations/digests, never arbitrary 'ref' substrings."""
    if isinstance(value, dict):
        return {key: compact_values(item) for key, item in value.items()
            if key not in AUDIT_KEYS and not key.endswith('_sha256') and key != 'sha256'}
    if isinstance(value, list): return [compact_values(item) for item in value]
    return deepcopy(value)


def _dict(value):
    return value if isinstance(value, dict) else {}


def compact_news_context(news):
    """Keep content-addressed news references without exposing audit locations."""
    def reference(value):
        value = _dict(value)
        digest = value.get('sha256')
        if not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest):
            return None
        result = {'sha256': digest}
        if value.get('schema_version') == 'rub_news_capture.v2':
            result['schema_version'] = value['schema_version']
        return result
    result = compact_values(news)
    for original, event in zip(news['events'], result['events']):
        event['audit_ref'] = reference(original.get('audit_ref'))
    original = _dict(_dict(news.get('summary')).get('selection_audit'))
    target = _dict(_dict(result.get('summary')).get('selection_audit'))
    if 'audit_ref' in original or 'sha256' in original:
        target['audit_ref'] = reference(original.get('audit_ref', original))
    return result


def coverage(release):
    """Coverage of the approved engineering minimum, independent of model gates."""
    facts = {item['factor']: item for item in release['facts']}
    macro = release['macro_evidence_inventory']
    rows = []
    def add(identity, available, reason, *, scope, status=None):
        rows.append({'requirement_id': identity, 'required': True,
            'status': status or ('AVAILABLE' if available else 'UNAVAILABLE'),
            'usable': bool(available), 'scope': scope, 'reason': None if available else reason})
    for key in MARKETS:
        item = release['market_usability'][key]
        add(key, key in facts, item['missing_reason'], scope='current_price_oi' if key != 'cnyrub_tom' else 'current_spot_price')
        rows[-1].update(quote_usable=item['quote_usable'],
            cross_market_comparison_usable=item['cross_market_comparison_usable'],
            missing_metadata=item['missing_metadata'])
        add(key + '_metadata', not item['missing_metadata'], 'missing_' + '_and_'.join(item['missing_metadata']), scope='accepted_source_units_and_applicable_expiry')
    add('basis_carry', 'basis_carry' in facts, 'no_individually_admitted_metrics', scope='individual_READY_metrics_only')
    for key in ('futoi_live', 'futoi_live_cr'):
        context = release['futoi_context'][key]
        add(key, key in facts and context['current_usable'], context['reason'], scope=context['scope'])
        rows[-1]['previous_dated_observation_available'] = context['previous_observation'] is not None
    add('market_structure', release['market_structure']['status'] == 'AVAILABLE', release['market_structure']['reason'], scope='accepted_dated_USDRUBF_structure')
    for timeframe in ('1H', '1D', '1W'):
        add('timeframe_' + timeframe, any(item['values'].get('timeframe') == timeframe for item in release['timeframe_context']),
            'accepted_dated_timeframe_missing', scope='dated_aggregate_not_report_horizon')
    for item in macro['requirements_coverage']:
        identity = item['requirement_id']
        blocked = identity == 'minfin_fx_operations_plan'
        add(identity, item['current_evidence_present'],
            'accepted_latest_Minfin_plan_missing_external_source_evidence_required'
            if blocked else 'fresh_replayable_required_evidence_missing', scope='CURRENT_RECEIVED_ENGINEERING_MINIMUM',
            status='EXTERNAL_BLOCKER' if blocked and not item['current_evidence_present'] else None)
    for key in ('oil', 'external_cny'):
        add(key, key in facts, 'accepted_dated_reference_missing', scope='latest_published_dated_reference_not_live')
    news = release['news_context']; summary = _dict(news.get('summary'))
    events = news['events']
    selected = _dict(news.get('selection_at_read'))
    counts_valid = (all(type(summary.get(key)) is int and summary[key] >= 0
        for key in ('source_count', 'ok_source_count', 'failed_source_count'))
        and summary['source_count'] > 0
        and summary['ok_source_count'] + summary['failed_source_count'] == summary['source_count'])
    all_sources_ok = counts_valid and summary['failed_source_count'] == 0 and summary.get('failed_source_ids') == ''
    news_ok = (news.get('source_status') == 'READY' and news.get('acquisition_fresh') is True
        and all_sources_ok and bool(selected)
        and selected.get('selected_ids') == [event.get('event_id') for event in events]
        and (bool(events) or selected.get('candidate_count') == 0)
        and all(item.get('content_status') == 'AVAILABLE' for item in events)
        and news.get('excluded_event_count') == 0)
    news_reason = ('configured_source_counts_missing_or_inconsistent' if not counts_valid else
        'configured_news_sources_failed_or_failure_ids_inconsistent' if not all_sources_ok else
        'source_refresh_selection_or_original_headlines_missing')
    add('news_content_and_selection', news_ok, news_reason, scope='bounded_raw_news_NOT_ANALYZED',
        status='PARTIAL' if events and not news_ok else None)
    rows[-1].update(event_count=len(events), source_count=summary.get('source_count'),
        ok_source_count=summary.get('ok_source_count'), failed_source_count=summary.get('failed_source_count'),
        failed_source_ids=summary.get('failed_source_ids'), events_dropped_by_bound=selected.get('events_dropped_by_bound'))
    position = release['user_position_context']
    add('explicit_position_state', position.get('status') == 'AVAILABLE' or position.get('availability') == 'NO_EXPLICIT_USER_INPUT',
        position.get('availability'), scope='explicit_user_input_or_explicit_absence')
    missing = [row['requirement_id'] for row in rows if not row['usable']]
    dated = release.get('dated_context', {}).get('observations', {})
    for row in rows:
        key = row['requirement_id']
        row['dated_preparation_available'] = ('market:' + key in dated if key in MARKETS else
            any(item.startswith('basis:') for item in dated) if key == 'basis_carry' else
            'structure' in dated if key == 'market_structure' else
            any(item.startswith('timeframe:') for item in dated) if key == 'timeframe_1H' else False)
    return {'status': 'COMPLETE' if not missing else 'PARTIAL', 'requirements': rows,
        'missing_required': missing, 'model_ready': False,
        'scope': 'current_received_facts_and_admitted_dated_context',
        'external_blockers': [row['requirement_id'] for row in rows if row['status'] == 'EXTERNAL_BLOCKER']}


def review_horizons(snapshot, release, *, now):
    local = now.astimezone(MOSCOW)
    today = local.date(); monday = today - timedelta(days=today.weekday())
    next_monday = monday + timedelta(days=7); next_sunday = next_monday + timedelta(days=6)
    component = _dict(_dict(snapshot.get('components')).get('futures_calendar'))
    data = _dict(component.get('data'))
    # The input to this function is already reconciled at the same read time.
    calendar_usable = component.get('status') == 'READY' and data.get('calendar_plan_usable') is True
    days = data.get('days', []) if calendar_usable else []
    by_date = {item['civil_date']: item for item in days}
    wanted = [(next_monday + timedelta(days=i)).isoformat() for i in range(7)]
    covered = all(day in by_date for day in wanted)
    planned = sorted({item['trading_date'] for item in days if item.get('trading_date')
        and next_monday.isoformat() <= item['trading_date'] <= next_sunday.isoformat()})
    mappings = [{key: item.get(key) for key in ('civil_date', 'is_traded', 'trading_date', 'reason')}
        for item in days if item.get('civil_date') in wanted or item.get('trading_date') in planned]
    return {
        'D1': {'kind': 'DAILY_REVIEW', 'timezone': 'Europe/Moscow',
            'report_start': today.isoformat() + 'T00:00:00+03:00', 'report_end': local.isoformat(),
            'dated_aggregate_available': any(item['values'].get('timeframe') == '1D' for item in release['timeframe_context']),
            'session_completion_proven': False},
        'W1': {'kind': 'WEEKLY_REVIEW_AND_NEXT_WEEK_PREPARATION', 'timezone': 'Europe/Moscow',
            'report_start': monday.isoformat() + 'T00:00:00+03:00', 'report_end': local.isoformat(),
            'regular_preparation_day': 'SUNDAY', 'is_regular_preparation_day': today.weekday() == 6,
            'prospective_start_date': next_monday.isoformat(), 'prospective_end_date': next_sunday.isoformat(),
            'dated_aggregate_available': any(item['values'].get('timeframe') == '1W' for item in release['timeframe_context']),
            'planned_calendar_coverage': 'COMPLETE' if covered else 'PARTIAL' if planned else 'UNAVAILABLE',
            'coverage_semantics': 'CIVIL_DATE_PLAN_ONLY_NOT_COMPLETE_SESSION_ENUMERATION',
            'planned_trading_dates': planned, 'uncovered_civil_dates': [day for day in wanted if day not in by_date],
            'planned_civil_date_mappings': mappings,
            'planned_dates_are_actual_sessions': False, 'session_completion_proven': False}}


def build_package(snapshot, release, *, now):
    """Called with the same reconciled view as the release, never fetches sources."""
    freshness = _dict(snapshot.get('live_read_freshness'))
    if freshness.get('read_at_utc') != now.isoformat():
        raise ValueError('package and snapshot consumption times differ')
    encoded = lambda value: json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
    components = _dict(snapshot.get('components'))
    versions = {key: {'status': value.get('status'), 'source_as_of': value.get('data_as_of'),
        'refresh_attempted_at': value.get('refresh_attempted_at'), 'last_success_at': value.get('last_success_at'),
        'read_view_sha256': sha256(encoded(value)).hexdigest()}
        for key, value in sorted(components.items()) if isinstance(value, dict)}
    chosen = {key: release[key] for key in ('facts', 'market_usability', 'market_structure',
        'timeframe_context', 'futoi_context', 'news_context', 'user_position_context', 'dated_context')}
    macro = release['macro_evidence_inventory']
    chosen['macro_context'] = {key: macro[key] for key in ('facts', 'scheduled_events', 'calendar_coverage')}
    chosen = compact_values(chosen)
    chosen['news_context'] = compact_news_context(release['news_context'])
    readiness = coverage(release)
    return {'project': 'MOEX_Bot', 'schema_version': SCHEMA, 'as_of_utc': now.isoformat(),
        'code_revision': release['code_revision'], 'status': readiness['status'],
        'presentation_integrity': {'status': 'VALIDATED_PROJECTION', 'factual_only': True},
        'factual_coverage': readiness, 'review_horizons': review_horizons(snapshot, release, now=now),
        'generations': {'slow_snapshot_generated_at_utc': snapshot['identity']['generated_at_utc'],
            'fast_market': deepcopy(snapshot.get('fast_market_read', {'status': 'NOT_ENABLED'})),
            'components': versions}, **chosen,
        'authority': {'model_ready': False, 'forecast_generated': False, 'training_authorized': False,
            'directional_authority': False, 'action_authority': False, 'broker_execution': False},
        'reading_notes': ['Prices and OI are current only where their own usability is true.',
            'Dated prior FUTOI observations and timeframe aggregates are not completed-session proof.',
            'Brent is published CLOSE; external CNY is DEXCHUS, not live CNH.',
            'News is original source content with UNKNOWN impact, not neutral analysis.',
            'News content_status describes preserved headlines; the audit retains bounded acquired parser bodies, which may be truncated by source adapters.',
            'News available_at is the adapter eligibility timestamp (publication time or acquisition time, depending on source); ingested_at records receipt. Neither proves first historical market availability.',
            'An announced Minfin plan would not prove executed operations.',
            'The report period, dated bar aggregate and prospective horizon are different.'],
        'deferred_scope': ['broader_global_event_calendar', 'historical_model_vintages', 'training_and_model_evaluation'],
        'audit_replay': 'Original snapshot, manifests and evidence files are needed only for strict replay.'}
