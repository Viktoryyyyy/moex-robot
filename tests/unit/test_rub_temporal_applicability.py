from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest
from moex_data.rub_temporal_applicability import apply

NOW = datetime(2026, 9, 7, 21, 5, tzinfo=timezone.utc)


def snapshot():
    stamp = (NOW - timedelta(minutes=5)).isoformat()
    pair = dict(snapshot_ts=stamp, source_publication_time=stamp,
        availability_ts_utc=stamp, ingest_ts_utc=stamp)
    record = dict(status='FRESH', last_success_at=stamp, factual=pair)
    data = dict(instrument_id='si_futures_family', factual_authority=True, consumer_factual_use_allowed=True, current_intraday=record,
        previous_completed_session=dict(status='FRESH', last_success_at=stamp, refresh_attempted_at=stamp,
            trade_date='2026-09-06', expected_trade_date='2026-09-06',
            factual=dict(trade_date='2026-09-06', snapshot_ts='2026-09-06T12:00:00+00:00',
                source_publication_time='2026-09-06T12:00:00+00:00', availability_ts_utc=stamp, ingest_ts_utc=stamp)),
        context_refresh=dict(observed_trade_dates=['2026-09-06', '2026-09-07'],
            through_date='2026-09-07', refresh_attempted_at=stamp,
            observed_current_trade_date='2026-09-07', previous_observed_trade_date='2026-09-06'))
    return {'components': {'futoi_live': {'status': 'READY', 'data': data}},
        'authority': {'futoi_factual_authority': True,
            'futoi_by_instrument': {'si_futures_family': {'factual_authority': True}}}}


def test_fresh_previous_date_is_not_proof_of_completed_session():
    value = snapshot()
    apply(value, now=NOW)
    view = value['temporal_applicability']['components']['futoi_live']
    assert view['current']['usable'] is True
    assert view['previous']['dated_observation_available'] is True
    assert view['previous']['session_completion_proven'] is False
    assert view['previous']['current_use_allowed'] is False
    assert value['temporal_applicability']['session_state'] == 'UNKNOWN'


@pytest.mark.parametrize('defect', ['expired', 'future', 'retained', 'failed', 'missing', 'blocked'])
def test_bad_current_cannot_borrow_fresh_previous_date(defect):
    value = snapshot()
    data = value['components']['futoi_live']['data']
    record = data['current_intraday']
    if defect == 'expired':
        record['factual']['snapshot_ts'] = (NOW - timedelta(seconds=1201)).isoformat()
    elif defect == 'future':
        record['last_success_at'] = (NOW + timedelta(seconds=1)).isoformat()
    elif defect == 'retained':
        record['status'] = 'RETAINED_STALE'
    elif defect == 'failed':
        record['failed_attempt_at'] = NOW.isoformat()
    elif defect == 'missing':
        data['current_intraday'] = None
    else:
        data['consumer_factual_use_allowed'] = False
    old_fact = deepcopy(data['previous_completed_session'])
    apply(value, now=NOW)
    assert data['factual_authority'] is False
    assert value['authority']['futoi_factual_authority'] is False
    assert value['authority']['futoi_by_instrument']['si_futures_family']['factual_authority'] is False
    assert data['previous_completed_session'] == old_fact


@pytest.mark.parametrize('defect', ['receipt', 'date', 'witness'])
def test_previous_observation_requires_receipt_date_and_witness(defect):
    value = snapshot()
    data = value['components']['futoi_live']['data']
    if defect == 'receipt':
        data['previous_completed_session']['last_success_at'] = (NOW - timedelta(seconds=1201)).isoformat()
    elif defect == 'date':
        data['previous_completed_session']['factual']['trade_date'] = '2026-09-05'
    else:
        data['context_refresh']['observed_trade_dates'] = []
    apply(value, now=NOW)
    assert value['temporal_applicability']['components']['futoi_live']['previous']['dated_observation_available'] is False


def test_slow_daily_price_keeps_its_date_not_receipt_as_event_time():
    value = snapshot()
    value['components']['oil'] = {'status': 'READY', 'data': {
        'source_trade_date': '2026-09-04', 'received_at': NOW.isoformat(),
        'source_event_time': None, 'consumer_factual_use_allowed': True}}
    apply(value, now=NOW)
    oil = value['temporal_applicability']['components']['oil']
    assert oil['dated_price_use_allowed'] is True
    assert oil['source_trade_date'] == '2026-09-04'
    assert oil['price_event_time'] is None
    assert oil['intraday_use_allowed'] is False


@pytest.mark.parametrize('defect', ['future_event', 'source_order', 'receipt_order', 'missing_time',
    'naive_time', 'failed_attempt', 'refresh_error', 'refresh_error_class', 'wrong_instrument',
    'record_date', 'expected_date', 'request_after_receipt', 'expired_witness', 'future_witness',
    'witness_after_request', 'through_date', 'current_witness_date'])
def test_previous_metadata_failures_are_not_available_or_completed(defect):
    value = snapshot()
    data = value['components']['futoi_live']['data']
    previous = data['previous_completed_session']
    fact = previous['factual']
    context = data['context_refresh']
    if defect == 'future_event': fact['snapshot_ts'] = (NOW + timedelta(days=1)).isoformat()
    elif defect == 'source_order': fact['source_publication_time'] = '2026-09-05T12:00:00+00:00'
    elif defect == 'receipt_order': fact['ingest_ts_utc'] = NOW.isoformat()
    elif defect == 'missing_time': del fact['availability_ts_utc']
    elif defect == 'naive_time': fact['snapshot_ts'] = '2026-09-06T12:00:00'
    elif defect in ('failed_attempt', 'refresh_error', 'refresh_error_class'):
        previous['failed_attempt_at' if defect == 'failed_attempt' else defect] = 'failed'
    elif defect == 'wrong_instrument': data['instrument_id'] = 'cr_futures_family'
    elif defect == 'record_date': previous['trade_date'] = '2026-09-05'
    elif defect == 'expected_date': previous['expected_trade_date'] = '2026-09-05'
    elif defect == 'request_after_receipt': previous['refresh_attempted_at'] = NOW.isoformat()
    elif defect == 'expired_witness': context['refresh_attempted_at'] = (NOW - timedelta(seconds=1201)).isoformat()
    elif defect == 'future_witness': context['refresh_attempted_at'] = (NOW + timedelta(seconds=1)).isoformat()
    elif defect == 'witness_after_request': context['refresh_attempted_at'] = (NOW - timedelta(seconds=1)).isoformat()
    elif defect == 'through_date': context['through_date'] = '2026-09-05'
    else: context['observed_current_trade_date'] = '2026-09-06'
    original = deepcopy(previous)
    apply(value, now=NOW)
    result = value['temporal_applicability']['components']['futoi_live']['previous']
    assert result['dated_observation_available'] is False
    assert result['reason']
    assert result['session_completion_proven'] is False
    assert result['authority_granted_by_this_view'] is False
    assert result['current_use_allowed'] is False
    assert previous == original


@pytest.mark.parametrize('instrument,name', [('si_futures_family', 'futoi_live'), ('cr_futures_family', 'futoi_live_cr')])
def test_previous_old_source_event_with_fresh_receipt_is_descriptive_only(instrument, name):
    value = snapshot()
    component = value['components'].pop('futoi_live')
    value['components'][name] = component
    component['data']['instrument_id'] = instrument
    component['data']['previous_completed_session']['consumer_factual_use_allowed'] = False
    apply(value, now=NOW)
    result = value['temporal_applicability']['components'][name]['previous']
    assert result['dated_observation_available'] is True
    assert result['reason'] is None
    assert result['source_event_age_limited'] is False
    assert component['data']['previous_completed_session']['consumer_factual_use_allowed'] is False
    assert result['session_completion_state'] == 'UNKNOWN'


def test_previous_witness_does_not_infer_current_from_civil_day():
    value = snapshot()
    context = value['components']['futoi_live']['data']['context_refresh']
    context.update(observed_trade_dates=['2026-09-06'], observed_current_trade_date=None)
    apply(value, now=NOW)
    result = value['temporal_applicability']['components']['futoi_live']['previous']
    assert result['dated_observation_available'] is True
    assert result['session_completion_proven'] is False
