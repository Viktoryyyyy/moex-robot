"""Qualitative limitations for evidence already verified by a source adapter.

This helper does not replay evidence, infer precision from floats, or estimate
forecast error. Callers must pass the result of source verification explicitly.
"""
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
import re

SCHEMA = 'rub_data_uncertainty.v1'
DENIED = ('forecast_use_allowed', 'historical_pit_acceptance', 'action_authority')


def describe(fact, *, source_policy=None):
    """Describe a received fact; omitted/failed verification stays blocked.

    source_policy: evidence_verified (strict bool from successful source replay,
    never an unchecked stored flag), decimal_places (printed precision, optional),
    revision_kind (latest_revised,
    immutable_received_version, or unknown). Decimal places describe display
    granularity in source units, never a proven rounding rule, error bound or
    measurement accuracy. An old observation date does not itself imply stale
    evidence. Freshness and causal admission remain the caller's responsibility.
    """
    value = fact if isinstance(fact, dict) else {}
    policy = source_policy if isinstance(source_policy, dict) else {}
    blocked = not isinstance(fact, dict) or not value or policy.get('evidence_verified') is not True
    if value.get('status') not in (None, 'READY') or value.get('refresh_error'):
        blocked = True
    for flag in ('factual_authority', 'consumer_factual_use_allowed'):
        if flag in value and value[flag] is not True:
            blocked = True
    limitations = ['source_verification_and_freshness_remain_owned_by_adapter',
                   'fresh_receipt_does_not_make_observation_current',
                   'no_numeric_accuracy_or_forecast_error_estimate']
    temporal = {}
    month = value.get('observation_month')
    if month is not None:
        try:
            if not isinstance(month, str) or re.fullmatch(r'[0-9]{4}-[0-9]{2}', month) is None:
                raise ValueError('canonical month required')
            date.fromisoformat(month + '-01')
        except (ValueError, TypeError):
            blocked = True
            limitations.append('invalid_observation_month')
            month = None
    temporal['observation_month'] = month
    for field in ('observation_date', 'observation_start', 'observation_end',
                  'effective_date', 'source_publication_date'):
        item = value.get(field)
        if item is not None:
            try:
                if not isinstance(item, str) or date.fromisoformat(item).isoformat() != item:
                    raise ValueError('invalid date')
            except (ValueError, TypeError):
                blocked = True
                limitations.append('invalid_' + field)
                item = None
        temporal[field] = item
    for field in ('source_publication_time', 'system_available_at', 'received_at'):
        item = value.get(field)
        if item is not None:
            try:
                if not isinstance(item, str) or datetime.fromisoformat(item).utcoffset() is None:
                    raise ValueError('aware timestamp required')
            except (ValueError, TypeError):
                blocked = True
                limitations.append('invalid_' + field)
                item = None
        temporal[field] = item
    if temporal['source_publication_time'] is None:
        limitations.append('publication_date_known_hour_unknown' if temporal['source_publication_date']
                            else 'source_publication_time_unknown')
    precision = policy.get('decimal_places')
    granularity = None
    if precision is not None:
        if type(precision) is not int or not 0 <= precision <= 12:
            blocked = True
            limitations.append('invalid_source_precision_policy')
            precision = None
        else:
            granularity = format(Decimal(1).scaleb(-precision), 'f')
            limitations.append('reported_granularity_is_not_accuracy_or_error_bound')
    else:
        limitations.append('source_printed_granularity_unknown')
    revision = policy.get('revision_kind', 'unknown')
    if revision not in ('unknown', 'latest_revised', 'immutable_received_version'):
        blocked = True
        revision = 'unknown'
        limitations.append('invalid_revision_policy')
    limitations.append({'latest_revised': 'latest_revised_view_not_original_publication_vintage',
                        'immutable_received_version': 'immutable_receipt_not_historical_vintage_acceptance',
                        'unknown': 'source_revision_policy_unknown'}[revision])
    if blocked:
        limitations.append('source_fact_not_admitted')
    return {'schema_version': SCHEMA, 'status': 'BLOCKED' if blocked else 'DATED_CONTEXT_ONLY',
            'usable_as_dated_context': not blocked, 'source_verification_performed_here': False,
            'temporal_metadata': temporal,
            'rounding': {'decimal_places': precision, 'reported_granularity': granularity,
                         'units': deepcopy(value.get('units')), 'accuracy_bound': None},
            'revision_kind': revision, 'limitations': limitations,
            'numeric_error_bounds': None, 'probabilities': None, **dict.fromkeys(DENIED, False)}
