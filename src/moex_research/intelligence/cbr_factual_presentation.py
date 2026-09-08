"""Separate legacy policy boundaries from observed CBR publication evidence."""
from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo


def _aware(value):
    result = datetime.fromisoformat(value)
    if result.utcoffset() is None:
        raise ValueError('CBR timestamp must be aware')
    return result


def present(state):
    result = deepcopy(state)
    for observation in result.get('observations', []):
        source = observation['source_id']
        if source not in ('cbr_ruonia_daily', 'cbr_key_rate_daily'):
            raise ValueError('unexpected canonical CBR source')
        received = _aware(observation['ingested_at'])
        boundary = _aware(observation['available_at'])
        legacy_publication = _aware(observation['published_at'])
        if not legacy_publication <= boundary <= received:
            raise ValueError('CBR policy boundary follows receipt')
        observation['policy_available_not_before'] = observation['available_at']
        observation['publication_time_status'] = 'UNKNOWN'
        observation['source_publication_time'] = None
        observation['source_publication_date'] = (
            legacy_publication.astimezone(ZoneInfo('Europe/Moscow')).date().isoformat()
            if source == 'cbr_ruonia_daily' else None)
        observation['published_at'] = None
        observation['available_at'] = observation['ingested_at']
        observation['system_available_at'] = observation['ingested_at']
        observation['availability_semantics'] = 'actual_receipt_not_legacy_policy_boundary'
        observation['historical_pit_acceptance'] = False
        observation['immutable_evidence_accepted'] = False
    return result
