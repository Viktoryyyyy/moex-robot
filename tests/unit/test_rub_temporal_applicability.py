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
    data = dict(factual_authority=True, consumer_factual_use_allowed=True, current_intraday=record,
        previous_completed_session=dict(status='FRESH', last_success_at=stamp,
            factual={'trade_date': '2026-09-06'}),
        context_refresh=dict(observed_trade_dates=['2026-09-06', '2026-09-07'],
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
