from copy import deepcopy

import pytest

from moex_research.intelligence.cbr_factual_presentation import present


def state(source='cbr_ruonia_daily'):
    return {'observations': [{'source_id': source, 'value': 17.2,
        'observed_or_effective_at': '2026-09-04T00:00:00+03:00',
        'published_at': '2026-09-07T23:59:59.999999+03:00',
        'available_at': '2026-09-08T00:00:00+03:00',
        'ingested_at': '2026-09-08T07:00:00+00:00'}]}


@pytest.mark.parametrize('source', ['cbr_ruonia_daily', 'cbr_key_rate_daily'])
def test_policy_time_is_not_presented_as_actual_publication(source):
    original = state(source)
    before = deepcopy(original)
    item = present(original)['observations'][0]
    assert original == before
    assert item['published_at'] is item['source_publication_time'] is None
    assert item['available_at'] == item['system_available_at'] == before['observations'][0]['ingested_at']
    assert item['policy_available_not_before'] == before['observations'][0]['available_at']
    assert item['source_publication_date'] == ('2026-09-07' if source == 'cbr_ruonia_daily' else None)
    assert item['value'] == 17.2
    assert item['historical_pit_acceptance'] is False


@pytest.mark.parametrize('field,value', [('ingested_at', '2026-09-01T00:00:00+00:00'),
    ('ingested_at', '2026-09-08T00:00:00'), ('published_at', 'invalid'), ('source_id', 'other')])
def test_unproven_or_inconsistent_receipt_cannot_be_presented(field, value):
    payload = state()
    payload['observations'][0][field] = value
    with pytest.raises(ValueError): present(payload)
