"""Read-only inventory of admitted macro evidence and distinct scheduled events."""
from copy import deepcopy
from datetime import datetime, timezone

from moex_research.external_data import cbr_rates_factual as cbr
from moex_research.external_data import rosstat_cpi_factual as rosstat
from moex_research.external_data import cbr_meeting_calendar
from moex_research.external_data import rosstat_monthly_cpi, cbr_liquidity_factual
from moex_data.rub_macro_requirements import describe as describe_requirements
from moex_data.rub_data_uncertainty import describe as describe_uncertainty

SCHEMA = 'rub_macro_evidence_inventory.v1'
PROVIDERS = {'cbr_rates_verified': cbr, 'rosstat_cpi': rosstat,
    'rosstat_monthly_cpi': rosstat_monthly_cpi, 'cbr_liquidity_verified': cbr_liquidity_factual}
DENIED = ('required_series_policy_complete', 'full_macro_accepted', 'full_calendar_accepted',
          'horizon_alignment_accepted', 'model_accepted', 'action_authority')


def _now(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('aware consumption time required')
    return value.astimezone(timezone.utc)


def _blocked(component, reason):
    result = deepcopy(component) if isinstance(component, dict) else {}
    data = result.get('data')
    if not isinstance(data, dict):
        data = result['data'] = {}
    result['status'] = 'UNAVAILABLE'
    for key in ('factual_authority', 'consumer_factual_use_allowed', 'historical_pit_acceptance',
                'forecast_use_allowed', 'action_authority', 'full_macro_complete',
                'latest_publication_verified', 'weekly_release_calendar_accepted', 'calendar_accepted',
                'full_rosstat_macro_accepted', 'forecast_alignment_accepted'):
        data[key] = False
    previous_reason = data.get('read_freshness_reason')
    data['read_freshness_reason'] = previous_reason if isinstance(previous_reason, str) and previous_reason else reason
    return result


def reconcile_components(snapshot, *, now):
    """Copy the snapshot/components and replay only present known macro providers."""
    result = dict(snapshot) if isinstance(snapshot, dict) else {}
    original = result.get('components')
    components = result['components'] = dict(original) if isinstance(original, dict) else {}
    try:
        reference = _now(now)
    except (ValueError, TypeError, OverflowError):
        reference = None
    for name, provider in PROVIDERS.items():
        if name not in components:
            continue
        value = components[name]
        if reference is None:
            components[name] = _blocked(value, 'invalid_macro_consumption_time')
        elif not isinstance(value, dict) or not isinstance(value.get('data'), dict):
            components[name] = _blocked(value, 'malformed_macro_component')
        else:
            try:
                view = provider.reconcile(value, now=reference)
                if not isinstance(view, dict) or not isinstance(view.get('data'), dict):
                    raise ValueError('malformed reconciled component')
                if (view.get('status') != 'READY' or view['data'].get('factual_authority') is not True
                        or view['data'].get('consumer_factual_use_allowed') is not True):
                    view = _blocked(view, 'macro_evidence_not_admitted')
                components[name] = view
            except (ValueError, TypeError, KeyError, OSError, OverflowError, AttributeError):
                # Source readers may encounter structurally invalid on-disk JSON;
                # isolate that provider without hiding errors in other application code.
                components[name] = _blocked(value, 'macro_evidence_replay_failed')
    if 'cbr_meeting_calendar' in components:
        components['cbr_meeting_calendar'] = cbr_meeting_calendar.reconcile(
            components['cbr_meeting_calendar'], now=reference)
    return result


def _usable(component):
    return (isinstance(component, dict) and component.get('status') == 'READY'
        and isinstance(component.get('data'), dict)
        and component['data'].get('factual_authority') is True
        and component['data'].get('consumer_factual_use_allowed') is True)


def describe(snapshot, *, now):
    try:
        reference = _now(now)
    except (ValueError, TypeError, OverflowError):
        reference = None
    components = reconcile_components(snapshot, now=reference)['components']
    blocks = [
        {'block_id': 'cbr_rates', 'component': 'cbr_rates_verified',
         'policy_gaps': ['required_cbr_series_not_finalized', 'banking_liquidity_requirement_and_vintage_policy_pending'],
         'missing_evidence': ['accepted_cbr_decision_release_calendar']},
        {'block_id': 'minfin_fx_operations', 'component': None,
         'policy_gaps': ['required_minfin_series_and_revision_policy_pending'],
         'missing_evidence': ['accepted_latest_minfin_fx_component']},
        {'block_id': 'rosstat_macro', 'component': 'rosstat_cpi',
         'policy_gaps': ['required_rosstat_series_not_finalized', 'monthly_cpi_and_other_series_acceptance_pending'],
         'missing_evidence': []},
        {'block_id': 'event_calendar', 'component': None,
         'policy_gaps': ['required_event_set_and_availability_policy_pending'],
         'missing_evidence': ['accepted_global_macro_calendar', 'accepted_cbr_decision_release_calendar']},
    ]
    by_block = {row['block_id']: row for row in blocks}
    facts, events = [], []
    cbr_component = components.get('cbr_rates_verified')
    if _usable(cbr_component):
        data = cbr_component['data']
        for name in ('key_rate', 'ruonia'):
            observation, receipt = data['observations'][name], data['receipts'][name]
            facts.append({'fact_id': observation['metric_id'], 'block_id': 'cbr_rates',
                'component': 'cbr_rates_verified', 'series_id': observation['metric_id'],
                'value': observation['value'], 'units': observation['unit'],
                'observation_date': observation.get('observation_date'),
                'effective_date': observation.get('effective_date'),
                'source_publication_date': observation['source_publication_date'],
                'publication_date_kind': 'SOURCE_DATE' if name == 'ruonia' else 'UNKNOWN',
                'source_publication_time': None, 'system_available_at': observation['system_available_at'],
                'received_at': receipt['received_at'], 'scope': observation['scope'],
                'evidence': {'source_url': receipt['source_url'], 'raw_sha256': receipt['raw_sha256'],
                    'manifest_sha256': data['manifest_sha256']},
                'consensus': None, 'surprise': None, 'historical_pit_acceptance': False,
                'horizon_alignment_accepted': False})
    else:
        by_block['cbr_rates']['missing_evidence'].append('fresh_replayable_key_rate_and_ruonia')
    rosstat_component = components.get('rosstat_cpi')
    if _usable(rosstat_component):
        data = rosstat_component['data']
        facts.append({'fact_id': data['series_id'], 'block_id': 'rosstat_macro', 'component': 'rosstat_cpi',
            'series_id': data['series_id'], 'units': data['units'], 'indices': deepcopy(data['indices']),
            'weekly_change_percent': data['weekly_change_percent'],
            'document_format': data['document_format'], 'observation_start': data['observation_start'],
            'observation_end': data['observation_end'], 'effective_date': None,
            'source_publication_date': data['listed_publication_date'],
            'publication_date_kind': 'ARCHIVE_LISTED_DATE', 'source_publication_time': None,
            'system_available_at': data['system_available_at'], 'received_at': data['received_at'], 'scope': data['scope'],
            'evidence': {'source_url': data['source_url'], 'raw_sha256': data['raw_sha256'],
                'document_manifest_sha256': data['document_manifest_sha256'],
                'index_manifest_sha256': data['index_manifest_sha256']},
            'consensus': None, 'surprise': None, 'monthly_final': False,
            'historical_pit_acceptance': False, 'horizon_alignment_accepted': False})
        if data.get('weekly_release_calendar_accepted') is True:
            upcoming = data['next_scheduled_release']
            events.append({'event_id': 'rosstat_weekly_cpi:' + upcoming['observation_end'],
                'block_id': 'event_calendar', 'component': 'rosstat_cpi', 'series_id': data['series_id'],
                'event_family': 'rosstat_weekly_cpi',
                'event_status': 'SCHEDULED', 'scheduled_date': upcoming['scheduled_publication_date'],
                'scheduled_time': None, 'timezone': data['calendar_timezone'],
                'observation_start': upcoming['observation_start'], 'observation_end': upcoming['observation_end'],
                'source_publication_time': None, 'actual_event_time': None,
                'system_available_at': data['index_received_at'],
                'scope': data['weekly_calendar_scope'], 'source_url': rosstat.INDEX_URL,
                'weekly_calendar_coverage_end': data['weekly_calendar_coverage_end'],
                'calendar_overdue_policy': data['calendar_overdue_policy'],
                'index_manifest_sha256': data['index_manifest_sha256'],
                'event_occurred_proven': False, 'full_calendar_accepted': False,
                'consensus': None, 'surprise': None})
    else:
        by_block['rosstat_macro']['missing_evidence'].append('fresh_replayable_weekly_cpi')
    if not events:
        by_block['event_calendar']['missing_evidence'].append('fresh_replayable_weekly_cpi_schedule')
    calendar = components.get('cbr_meeting_calendar', {})
    calendar_data = calendar.get('data') if isinstance(calendar, dict) else None
    if (isinstance(calendar_data, dict) and calendar.get('status') == 'READY'
            and calendar_data.get('calendar_schedule_usable') is True):
        for planned in calendar_data['upcoming_events']:
            events.append({**deepcopy(planned), 'block_id': 'event_calendar',
                'event_family': 'rates.cbr_key_rate_calendar', 'component': 'cbr_meeting_calendar',
                'scope': calendar_data['scope'], 'system_available_at': calendar_data['system_available_at'],
                'source_url': calendar_data['source_url'], 'raw_sha256': calendar_data['raw_sha256'],
                'manifest_sha256': calendar_data['manifest_sha256'],
                'consensus': None, 'surprise': None, 'event_occurred_proven': False,
                'full_calendar_accepted': False})
        if calendar_data['upcoming_events']:
            for block_id in ('cbr_rates', 'event_calendar'):
                by_block[block_id]['missing_evidence'].remove('accepted_cbr_decision_release_calendar')
    monthly = components.get('rosstat_monthly_cpi')
    if _usable(monthly):
        data = monthly['data']
        fact = {'fact_id': data['series_id'], 'series_id': data['series_id'],
            'block_id': 'rosstat_macro', 'component': 'rosstat_monthly_cpi',
            'units': data['units'], 'observation_month': data['observation_month'],
            'indices': deepcopy(data['indices']), 'changes_percent': deepcopy(data['changes_percent']),
            'source_publication_date': data.get('listed_publication_date'),
            'source_publication_time': None, 'received_at': data['received_at'],
            'system_available_at': data['system_available_at'], 'scope': data['scope'],
            'quality_status': 'USABLE_WITH_LIMITATIONS',
            'evidence': {key: data[key] for key in ('source_url', 'raw_sha256', 'index_manifest_sha256', 'document_manifest_sha256')},
            'consensus': None, 'surprise': None, 'historical_pit_acceptance': False,
            'horizon_alignment_accepted': False}
        fact['uncertainty'] = describe_uncertainty(fact, source_policy={
            'evidence_verified': True, 'decimal_places': data.get('decimal_places'),
            'revision_kind': 'immutable_received_version'})
        facts.append(fact)
        by_block['rosstat_macro']['policy_gaps'].remove('monthly_cpi_and_other_series_acceptance_pending')
        by_block['rosstat_macro']['policy_gaps'].append('other_rosstat_series_and_historical_vintages_pending')
    else:
        by_block['rosstat_macro']['missing_evidence'].append('fresh_replayable_monthly_cpi')
    liquidity = components.get('cbr_liquidity_verified')
    if _usable(liquidity):
        data = liquidity['data']
        for observation in data['observations']:
            fact = {**deepcopy(observation), 'fact_id': observation['metric_id'],
                'series_id': observation['metric_id'], 'block_id': 'cbr_rates',
                'component': 'cbr_liquidity_verified', 'received_at': data['received_at'],
                'scope': data['scope'], 'quality_status': 'USABLE_WITH_LIMITATIONS',
                'source_publication_date': None, 'consensus': None, 'surprise': None,
                'evidence': {key: data[key] for key in ('source_url', 'raw_sha256', 'manifest_sha256')},
                'historical_pit_acceptance': False, 'horizon_alignment_accepted': False,
                'arithmetic_residual': deepcopy(data.get('arithmetic_residual'))}
            fact['uncertainty'] = describe_uncertainty(fact, source_policy={
                'evidence_verified': True, 'decimal_places': observation.get('displayed_decimal_places'),
                'revision_kind': 'latest_revised'})
            facts.append(fact)
        by_block['cbr_rates']['policy_gaps'].remove('banking_liquidity_requirement_and_vintage_policy_pending')
        by_block['cbr_rates']['policy_gaps'].append('banking_liquidity_historical_vintage_policy_pending')
    else:
        by_block['cbr_rates']['missing_evidence'].append('fresh_replayable_banking_liquidity')
    for block in blocks:
        block.update(required=True, full_block_accepted=False,
            admitted_fact_ids=[fact['fact_id'] for fact in facts if fact['block_id'] == block['block_id']],
            scheduled_event_ids=[event['event_id'] for event in events if event['block_id'] == block['block_id']])
    requirements = describe_requirements()
    coverage = []
    for requirement in requirements['requirements']:
        metric_id, event_family = requirement['metric_id'], requirement['event_family']
        metric_ids = requirement.get('metric_ids', [metric_id] if metric_id is not None else [])
        matched_facts = [fact['fact_id'] for fact in facts if fact['series_id'] in metric_ids]
        matched_events = [event['event_id'] for event in events if event_family is not None and event.get('event_family') == event_family]
        coverage.append({'requirement_id': requirement['requirement_id'],
            'current_evidence_present': (bool(metric_ids) and set(metric_ids) <= set(matched_facts)) or bool(matched_events),
            'admitted_fact_ids': matched_facts, 'scheduled_event_ids': matched_events,
            'full_requirement_accepted': False})
    return {'schema_version': SCHEMA, 'as_of_utc': reference.isoformat() if reference else None,
        'reference_time_valid': reference is not None, 'status': 'INCOMPLETE',
        'scope': 'RECEIVED_MACRO_EVIDENCE_INVENTORY_ONLY', 'required_blocks': blocks,
        'facts': facts, 'scheduled_events': events,
        'requirements_policy': requirements, 'requirements_coverage': coverage,
        'missing_evidence': sorted({gap for block in blocks for gap in block['missing_evidence']}),
        'policy_gaps': sorted({gap for block in blocks for gap in block['policy_gaps']}),
        **dict.fromkeys(DENIED, False)}
