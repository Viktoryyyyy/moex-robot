from copy import deepcopy

import pytest

from moex_data.rub_data_uncertainty import DENIED, describe


def fact():
    return {'value': 7.12, 'units': 'percent', 'observation_date': '2026-08-01',
            'source_publication_date': '2026-08-03', 'source_publication_time': None,
            'system_available_at': '2026-09-08T07:00:00+00:00',
            'received_at': '2026-09-08T07:00:00+00:00'}


def test_limited_verified_fact_is_dated_context_without_fabricated_accuracy():
    value = fact()
    before = deepcopy(value)
    result = describe(value, source_policy={'evidence_verified': True,
                      'decimal_places': 2, 'revision_kind': 'latest_revised'})
    assert result['usable_as_dated_context'] is True
    assert result['rounding']['reported_granularity'] == '0.01'
    assert result['rounding']['accuracy_bound'] is None
    assert result['numeric_error_bounds'] is result['probabilities'] is None
    assert all(result[key] is False for key in DENIED)
    assert 'latest_revised_view_not_original_publication_vintage' in result['limitations']
    assert 'publication_date_known_hour_unknown' in result['limitations']
    assert result['temporal_metadata']['observation_date'] == '2026-08-01'
    assert 'fresh_receipt_does_not_make_observation_current' in result['limitations']
    assert value == before


@pytest.mark.parametrize('verified', [None, False, 1, 'true'])
def test_unverified_fact_never_becomes_context(verified):
    assert describe(fact(), source_policy={'evidence_verified': verified})['status'] == 'BLOCKED'


@pytest.mark.parametrize('override', [{'status': 'RETAINED_PREVIOUS'},
    {'status': 'UNAVAILABLE'}, {'refresh_error': 'failed'}, {'factual_authority': False},
    {'consumer_factual_use_allowed': False}, {'received_at': '2026-09-08T07:00:00'},
    {'observation_date': '2026-99-99'}])
def test_explicit_failure_or_invalid_time_cannot_be_promoted(override):
    assert describe({**fact(), **override}, source_policy={'evidence_verified': True})['usable_as_dated_context'] is False


def test_no_float_precision_inference_and_deepcopy():
    value = fact()
    result = describe(value, source_policy={'evidence_verified': True})
    assert result['rounding']['decimal_places'] is None
    assert result['rounding']['reported_granularity'] is None
    result['temporal_metadata']['observation_date'] = 'forged'
    assert value['observation_date'] == '2026-08-01'


@pytest.mark.parametrize('precision', [True, -1, 13, '2', 2.0])
def test_invalid_precision_fails_closed(precision):
    result = describe(fact(), source_policy={'evidence_verified': True, 'decimal_places': precision})
    assert result['status'] == 'BLOCKED'
    assert result['rounding']['reported_granularity'] is None


def test_month_does_not_invent_publication_date():
    result = describe({'observation_month': '2026-08', 'source_url': 'https://example.org/cpi_20260901.htm'},
                      source_policy={'evidence_verified': True})
    assert result['status'] == 'DATED_CONTEXT_ONLY'
    assert result['temporal_metadata']['observation_month'] == '2026-08'
    assert result['temporal_metadata']['source_publication_date'] is None
    assert result['temporal_metadata']['source_publication_time'] is None


@pytest.mark.parametrize('month', ['2026-8', '2026-00', '2026-13', '0000-01',
    '202608', '2026-08-01', '2026-08 ', 202608, True])
def test_invalid_observation_month_stays_blocked(month):
    result = describe({'observation_month': month}, source_policy={'evidence_verified': True})
    assert result['status'] == 'BLOCKED'
    assert result['temporal_metadata']['observation_month'] is None
