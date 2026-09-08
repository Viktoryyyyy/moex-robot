"""Versioned engineering minimum; requirement presence never admits evidence."""
from copy import deepcopy

from moex_research.external_data.registry import SOURCE_REGISTRY

SCHEMA = 'rub_macro_requirements.v1'
REGISTRY = 'src/moex_research/external_data/registry.py'
NEWS_REGISTRY = 'contracts/intelligence/usdrubf_news_macro_source_registry_v1.json'
DENIED = ('full_policy_complete', 'required_series_policy_complete',
          'full_macro_accepted', 'full_calendar_accepted',
          'horizon_alignment_accepted', 'model_accepted', 'action_authority')
REQUIRED_BLOCKS = ('cbr_rates', 'minfin_fx_operations', 'rosstat_macro', 'event_calendar')


def _requirement(identity, block, kind, source, registry, *, metric=None,
                 event=None, scope, unresolved=()):
    return {'requirement_id': identity, 'block_id': block, 'kind': kind,
            'source_id': source, 'source_registry_ref': registry,
            'metric_id': metric, 'event_family': event, 'required': True,
            'scope': scope, 'unresolved': list(unresolved),
            'availability_rule': 'Replay archived evidence at aware consumption time; '
                'system availability must not exceed consumption time. Keep observation, '
                'effective, publication, schedule and receipt times separate; unknown '
                'publication times stay null. Reject stale, retained and failed evidence.',
            'revision_rule': 'Preserve immutable raw and receipt versions; do not apply '
                'a later version before its availability. Current receipt does not '
                'establish historical publication vintages.',
            'admitted': False}


_REQUIREMENTS = [
    _requirement('cbr_key_rate', 'cbr_rates', 'fact', 'cbr_key_rate_daily', REGISTRY,
                 metric='cbr_key_rate_pct', scope='latest_received_published_rate'),
    _requirement('cbr_ruonia', 'cbr_rates', 'fact', 'cbr_ruonia_daily', REGISTRY,
                 metric='cbr_ruonia_rate_pct', scope='latest_received_published_rate'),
    _requirement('rosstat_weekly_cpi', 'rosstat_macro', 'fact', 'rosstat_official_releases', NEWS_REGISTRY,
                 metric='ROSSTAT_WEEKLY_CPI_ESTIMATE', scope='weekly_estimate_not_monthly_final'),
    _requirement('cbr_banking_liquidity', 'cbr_rates', 'unresolved_series',
                 'cbr_banking_liquidity_daily', REGISTRY, scope='registered_source_only',
                 unresolved=('metric_selection', 'row_level_publication_vintages')),
    _requirement('minfin_fx_operations_plan', 'minfin_fx_operations', 'announcement_plan',
                 'minfin_ru_press_center', NEWS_REGISTRY, scope='ANNOUNCED_PLAN_NOT_EXECUTION',
                 unresolved=('accepted_latest_document', 'series_identity', 'revision_policy')),
    _requirement('rosstat_weekly_cpi_schedule', 'event_calendar', 'scheduled_event',
                 'rosstat_official_releases', NEWS_REGISTRY, event='rosstat_weekly_cpi',
                 scope='finite_weekly_release_schedule_not_actual_publication'),
    _requirement('cbr_key_rate_meeting_schedule', 'event_calendar', 'scheduled_event',
                 None, 'contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml',
                 event='rates.cbr_key_rate_calendar', scope='planned_meeting_not_realized_decision',
                 unresolved=('historical_schedule_vintages', 'actual_decision_publication')),
]
_REQUIREMENTS[6].update(
    design_reference='contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml',
    source_adapter_ref='src/moex_research/external_data/cbr_meeting_calendar.py',
    availability_scope='current_receipt_not_historical')
_REQUIREMENTS[4]['value_mapping'] = {
    'document_schema': 'minfin_fx_document.v1', 'asset_scope': 'FX_AND_GOLD',
    'amount_unit': 'RUB_BILLION', 'fields': ['total_amount', 'daily_amount'],
    'period_fields': ['operation_start', 'operation_end'],
    'direction_field': 'direction', 'latest_selection_required': True,
    'executed_operations_proven': False}

_POLICY = {'schema_version': SCHEMA, 'status': 'ENGINEERING_MINIMUM',
           'scope': 'CURRENT_RECEIVED_RUB_MACRO_EVIDENCE_CHECKLIST',
           'required_blocks': list(REQUIRED_BLOCKS), 'requirements': _REQUIREMENTS,
           'unresolved_gaps': ['monthly_cpi_and_other_series', 'tax_cycle',
                               'global_macro_calendar', 'h10_release_calendar',
                               'consensus_surprise', 'banking_liquidity_vintage',
                               'exhaustive_product_series_and_event_definition'],
           **dict.fromkeys(DENIED, False)}


def describe():
    """Return an isolated checklist, without fetching or admitting any source."""
    for requirement in _REQUIREMENTS:
        if requirement['source_registry_ref'] == REGISTRY:
            source = SOURCE_REGISTRY.get(requirement['source_id'])
            if source is None or source.source_id != requirement['source_id']:
                raise ValueError('required macro source missing from existing registry')
    return deepcopy(_POLICY)
