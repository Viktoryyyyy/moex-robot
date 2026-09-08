from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest
from moex_data import rub_factual_release as release
from moex_data.rub_factual_release_acceptance import projection_completeness
from moex_data.rub_snapshot_read_freshness import apply_read_freshness

NOW = datetime(2026, 9, 8, 13, tzinfo=timezone.utc)
COMMIT = 'a' * 40


def snapshot(status='PARTIAL'):
    stamp = NOW.isoformat()
    instruments = {key: {'secid': key, 'last': 12.8, 'oi': 0, 'timestamp': stamp,
        'stale': False, 'price_oi_usable': key != 'cnyrub_tom'} for key in ('cnyrub_tom', 'cr_front')}
    metric = {'metric_id': 'cny.basis', 'pair_id': 'CNY/RUB', 'status': 'READY', 'value': -0.2,
        'units': 'RUB_per_CNY', 'legs': ['cr_front', 'cnyrub_tom'], 'formula': 'a-b',
        'source_refs': {'cr_front': 'official'}, 'source_timestamps': {'cr_front': stamp},
        'freshness': {'status': 'READY'}, 'synchronized': True, 'data_as_of': stamp,
        'action_authority': False}
    return {'identity': {'generated_at_utc': stamp}, 'components': {
        'synchronized_live_market_oi': {'status': 'READY', 'data': {'instruments': instruments,
            'quality': {'spot_price_usable': True}, 'synchronization': {}}},
        'live_basis_carry': {'status': status, 'data': {'status': status, 'current_live_scope_status': status,
            'pairs': {'cny': {'pair_id': 'CNY/RUB', 'metrics': [metric]}}}}}}


def facts(value):
    return {item['factor']: item for item in value['facts']}


@pytest.mark.parametrize('status', ['READY', 'PARTIAL'])
def test_complete_projection_same_asof_and_immutable(status, tmp_path):
    original = snapshot(status); before = deepcopy(original)
    built = release.build(original, now=NOW, code_revision=COMMIT)
    direct = release.describe(original)
    canonical = release.describe(apply_read_freshness(original, now=NOW))
    for value in (built, direct, canonical):
        projection_completeness(original, value, now=NOW)
        assert facts(value)['cnyrub_tom']['values'] == {'last': 12.8}
        assert facts(value)['basis_carry'] == facts(built)['basis_carry']
        assert value['horizons']['D1']['target_date_proven'] is False
    directory = release.export(original, now=NOW, code_revision=COMMIT, output=tmp_path)
    replay = release.build(json.loads((directory/'input_snapshot.json').read_text()), now=NOW, code_revision=COMMIT)
    assert replay == built
    assert original == before


@pytest.mark.parametrize('defect', ['item_false', 'quality_false', 'quality_missing', 'stale', 'expired',
    'missing', 'none', 'nan', 'bool', 'negative', 'future'])
def test_spot_refusal_and_no_usd_tom_invention(defect):
    original = snapshot(); data = original['components']['synchronized_live_market_oi']['data']
    spot = data['instruments']['cnyrub_tom']
    if defect == 'item_false': spot['spot_price_usable'] = False
    elif defect == 'quality_false': data['quality']['spot_price_usable'] = False
    elif defect == 'quality_missing': del data['quality']['spot_price_usable']
    elif defect == 'stale': spot['stale'] = True
    elif defect == 'expired': spot['timestamp'] = (NOW-timedelta(seconds=61)).isoformat()
    elif defect == 'future': spot['timestamp'] = (NOW+timedelta(seconds=6)).isoformat()
    elif defect == 'missing': del data['instruments']['cnyrub_tom']
    else: spot['last'] = {'none': None, 'nan': float('nan'), 'bool': True, 'negative': -1}[defect]
    value = release.describe(original)
    assert 'cnyrub_tom' not in facts(value)
    assert not any(factor in facts(value) for factor in ('usd_tom', 'usd_spot'))
    projection_completeness(original, value, now=NOW)


@pytest.mark.parametrize('defect', ['expired', 'missing'])
def test_only_dependent_metric_expires(defect):
    original = snapshot(); components = original['components']
    metrics = components['live_basis_carry']['data']['pairs']['cny']['metrics']
    other = deepcopy(metrics[0]); other.update(metric_id='cny.independent', legs=['cr_front'], value=0)
    metrics.append(other)
    instruments = components['synchronized_live_market_oi']['data']['instruments']
    if defect == 'expired': instruments['cnyrub_tom']['timestamp'] = (NOW-timedelta(seconds=61)).isoformat()
    else: del instruments['cnyrub_tom']
    value = release.build(original, now=NOW, code_revision=COMMIT)
    projection_completeness(original, value, now=NOW)
    assert [m['values']['metric_id'] for m in facts(value)['basis_carry']['values']['metrics']] == ['cny.independent']


@pytest.mark.parametrize('conflict', [False, True])
def test_duplicates_deterministic_or_conflicts_excluded(conflict):
    original = snapshot(); metrics = original['components']['live_basis_carry']['data']['pairs']['cny']['metrics']
    metrics.append(deepcopy(metrics[0]))
    if conflict: metrics[1]['value'] = 1
    value = release.build(original, now=NOW, code_revision=COMMIT)
    projection_completeness(original, value, now=NOW)
    assert ('basis_carry' in facts(value)) is not conflict
    if not conflict:
        entries = facts(value)['basis_carry']['values']['metrics']
        assert len(entries) == 1 and entries[0]['snapshot_path'].endswith('.metrics.0')


@pytest.mark.parametrize('value', [None, True, float('nan'), float('inf')])
def test_invalid_metric_values_never_exported(value):
    original = snapshot()
    original['components']['live_basis_carry']['data']['pairs']['cny']['metrics'][0]['value'] = value
    result = release.describe(original)
    assert 'basis_carry' not in facts(result)
    projection_completeness(original, result, now=NOW)


@pytest.mark.parametrize('factor', ['cnyrub_tom', 'basis_carry'])
def test_reverse_oracle_detects_silent_omission(factor):
    original = snapshot(); value = release.build(original, now=NOW, code_revision=COMMIT)
    value['facts'] = [fact for fact in value['facts'] if fact['factor'] != factor]
    with pytest.raises(AssertionError): projection_completeness(original, value, now=NOW)


def core_snapshot():
    original = snapshot(); components = original['components']; stamp = NOW.isoformat()
    item = components['synchronized_live_market_oi']['data']['instruments']['cr_front']
    item.update(open=12.7, high=12.9, low=12.6, bid=12.79, ask=12.81, spread=.02,
        quote_usable=True, units='source_unit', expiry_date='2026-09-17', wap=None)
    components['live_market_structure'] = {'status': 'READY', 'data': {'structural_levels': {
        'status': 'FRESH', 'data_as_of': stamp, 'price_context': {'price': 80, 'source_timestamp': stamp},
        'active_levels': [{'level_id': 'one', 'low': 79, 'high': 81}],
        'level_interactions': [{'level_id': 'one', 'state': 'TEST'}],
        'methodology': {'lookahead_forbidden': True}, 'observed_extrema': {
            'prior_completed_session': {'high': 82, 'low': 78, 'partial_session': False, 'trade_date': '2026-09-07'}}}}}
    components['stage9_daily'] = {'status': 'READY', 'data': {'server_core': {'blocks': [
        {'block_id': tf, 'stage': 7, 'timeframe': tf, 'status': 'ready',
            'selected_causal_ts_utc': stamp, 'selected_observation': {'close': 80}} for tf in ('1H', '1D', '1W')]}}}
    components['official_news'] = {'status': 'READY', 'data_as_of': stamp, 'data': {'events': [{
        'event_id': 'one', 'headline': 'Published source fact', 'source_reference': 'https://example.org/1',
        'published_at': stamp, 'available_at': stamp, 'ingested_at': stamp, 'direction': 'NEUTRAL'}],
        'summary': {'source_count': 1}}}
    original['user_position_context'] = {'status': 'AVAILABLE', 'explicit_user_input': True,
        'instrument': 'USDRUBF', 'direction': 'LONG', 'average_entry_price': 79, 'user_input_updated_at': stamp,
        'contracts': 100, 'capital': 999999}
    return original


def test_core_fact_values_dates_and_exclusions_are_preserved():
    original = core_snapshot(); before = deepcopy(original)
    value = release.build(original, now=NOW, code_revision=COMMIT)
    projection_completeness(original, value, now=NOW)
    market = facts(value)['cr_front']['values']
    assert market['open'] == 12.7 and market['bid'] == 12.79 and market['wap'] is None
    assert market['expiry_date'] == '2026-09-17'
    assert value['market_structure']['values']['observed_extrema']['prior_observed_date']['session_completion_proven'] is False
    assert len(value['timeframe_context']) == 3
    assert value['news_context']['events'][0]['headline'] == 'Published source fact'
    assert value['news_context']['events'][0]['direction'] == 'UNKNOWN'
    assert 'capital' not in value['user_position_context'] and 'contracts' not in value['user_position_context']
    assert original == before


@pytest.mark.parametrize('defect', ['expired_structure', 'future_structure', 'blocked_structure', 'future_bar', 'blocked_bar', 'future_news', 'invalid_news_order', 'position_absent', 'position_future', 'quote_blocked'])
def test_core_refusals_do_not_erase_independent_facts(defect):
    original = core_snapshot(); components = original['components']
    if defect.endswith('structure'):
        levels = components['live_market_structure']['data']['structural_levels']
        if defect == 'blocked_structure': levels['status'] = 'STALE'
        else: levels['data_as_of'] = (NOW + timedelta(seconds=1) if defect == 'future_structure' else NOW - timedelta(seconds=1201)).isoformat()
    elif defect.endswith('bar'):
        block = components['stage9_daily']['data']['server_core']['blocks'][0]
        if defect == 'future_bar': block['selected_causal_ts_utc'] = (NOW + timedelta(seconds=1)).isoformat()
        else: block['status'] = 'blocked'
    elif defect.endswith('news') or defect == 'invalid_news_order':
        event = components['official_news']['data']['events'][0]
        event['available_at'] = (NOW + timedelta(seconds=1) if defect == 'future_news' else NOW - timedelta(seconds=1)).isoformat()
    elif defect == 'position_absent': original.pop('user_position_context')
    elif defect == 'position_future': original['user_position_context']['user_input_updated_at'] = (NOW + timedelta(seconds=1)).isoformat()
    else: components['synchronized_live_market_oi']['data']['instruments']['cr_front']['quote_usable'] = False
    value = release.build(original, now=NOW, code_revision=COMMIT)
    projection_completeness(original, value, now=NOW)
    assert facts(value)['cr_front']['values']['last'] == 12.8
    if defect.endswith('structure'): assert value['market_structure']['status'] == 'UNAVAILABLE'
    if defect.endswith('bar'): assert len(value['timeframe_context']) == 2
    if 'news' in defect: assert value['news_context']['events'] == []
    if defect.startswith('position'): assert value['user_position_context']['direction'] is None
    if defect == 'quote_blocked': assert 'bid' not in facts(value)['cr_front']['values']


def futoi_snapshot():
    original = snapshot(); stamp = NOW.isoformat(); old = '2026-09-07T12:00:00+00:00'
    fact = dict(trade_date='2026-09-08', snapshot_ts=stamp, source_publication_time=stamp,
        availability_ts_utc=stamp, ingest_ts_utc=stamp, total_open_interest=100)
    fact.update(fiz=dict(long=60, short=40, net=20, long_participants=10, short_participants=20, net_share_of_oi=.2),
        yur=dict(long=40, short=60, net=-20, long_participants=5, short_participants=8, net_share_of_oi=-.2))
    previous = dict(fact, trade_date='2026-09-07', snapshot_ts=old, source_publication_time=old)
    data = {'instrument_id': 'si_futures_family', 'factual_authority': True,
        'consumer_factual_use_allowed': True, 'governance': {'factual_use_allowed': True},
        'current_intraday': {'status': 'FRESH', 'last_success_at': stamp, 'factual': fact},
        'previous_completed_session': {'status': 'FRESH', 'last_success_at': stamp,
            'refresh_attempted_at': stamp, 'trade_date': '2026-09-07', 'expected_trade_date': '2026-09-07', 'factual': previous},
        'context_refresh': {'observed_trade_dates': ['2026-09-07', '2026-09-08'], 'through_date': '2026-09-08',
            'refresh_attempted_at': stamp, 'observed_current_trade_date': '2026-09-08', 'previous_observed_trade_date': '2026-09-07'},
        'delta_statistics': {'instrument_id': 'si_futures_family', 'current': {'status': 'AVAILABLE', 'factual': deepcopy(fact)},
            'previous_observed_session': {'status': 'AVAILABLE', 'factual': deepcopy(previous)},
            'observed_date_witness': {'status': 'PASS', 'current_observed_trade_date': '2026-09-08'}, 'deltas': {
                'delta_1d': {'status': 'AVAILABLE', 'values': {'total_open_interest': 1}, 'target_trade_date': '2026-09-07'},
                'delta_20d': {'status': 'UNAVAILABLE', 'values': {'total_open_interest': 999}, 'reason': 'baseline_missing'}},
            'statistics': {'status': 'UNAVAILABLE', 'variables': {'bad': 999}, 'reason': 'history_missing'}}}
    original['components']['futoi_live'] = {'status': 'READY', 'data': data}
    original['components']['futoi_live_cr'] = {'status': 'READY', 'data': deepcopy(data)}
    return original


@pytest.mark.parametrize('defect', ['none', 'bad_current', 'identity', 'blocked_history', 'previous_date', 'witness', 'participants', 'instrument', 'previous_denied'])
def test_si_history_admission_and_cr_scope_are_independent(defect):
    original = futoi_snapshot(); data = original['components']['futoi_live']['data']
    if defect == 'bad_current': data['current_intraday']['status'] = 'RETAINED_STALE'
    elif defect == 'identity': data['delta_statistics']['current']['factual']['snapshot_ts'] = '2026-09-08T12:59:00+00:00'
    elif defect == 'blocked_history': data['delta_statistics']['consumer_factual_use_allowed'] = False
    elif defect == 'previous_date': data['previous_completed_session']['factual']['trade_date'] = '2026-09-06'
    elif defect == 'witness': data['delta_statistics']['observed_date_witness']['status'] = 'FAIL'
    elif defect == 'participants': data['delta_statistics']['current']['factual']['fiz']['long_participants'] += 1
    elif defect == 'instrument': data['delta_statistics']['instrument_id'] = 'cr_futures_family'
    elif defect == 'previous_denied': data['previous_completed_session']['consumer_factual_use_allowed'] = False
    value = release.build(original, now=NOW, code_revision=COMMIT)
    si = value['futoi_context']['futoi_live']; cr = value['futoi_context']['futoi_live_cr']
    projection_completeness(original, value, now=NOW)
    assert cr['previous_observation'] is None and cr['comparisons'] is None
    if defect in ('bad_current', 'identity', 'blocked_history', 'witness', 'participants', 'instrument'): assert si['comparisons'] is None
    else:
        if defect in ('previous_date', 'previous_denied'): assert si['comparisons']['deltas']['delta_1d']['values'] is None
        else: assert si['comparisons']['deltas']['delta_1d']['values']['total_open_interest'] == 1
        assert si['comparisons']['deltas']['delta_20d']['values'] is None
        assert si['comparisons']['statistics']['variables'] is None
    assert (si['previous_observation'] is None) == (defect in ('previous_date', 'previous_denied'))
    assert si['session_completion_proven'] is False


@pytest.mark.parametrize('defect', ['none', 'timestamp', 'secid', 'value', 'source', 'receipt', 'quote_only'])
def test_same_generation_contract_metadata_and_independent_quotes(defect):
    original = core_snapshot(); components = original['components']
    row = components['synchronized_live_market_oi']['data']['instruments']['cr_front']
    row.update(source_id='official', received_at_utc=NOW.isoformat())
    leg = dict(status='READY', secid=row['secid'], timestamp=row['timestamp'], source_id=row['source_id'],
        received_at_utc=row['received_at_utc'], raw_value=row['last'], raw_unit='source_unit',
        normalized_unit='normalized_source_unit', normalization_divisor=1000, expiry_date='2026-09-17')
    components['live_basis_carry']['data']['pairs']['cny']['legs'] = {'cr_front': leg}
    if defect == 'timestamp': leg['timestamp'] = (NOW - timedelta(seconds=1)).isoformat()
    elif defect == 'secid': leg['secid'] = 'other'
    elif defect == 'value': leg['raw_value'] = 100
    elif defect == 'source': leg['source_id'] = 'other'
    elif defect == 'receipt': leg['received_at_utc'] = (NOW - timedelta(seconds=1)).isoformat()
    elif defect == 'quote_only': row['price_oi_usable'] = False; row['oi'] = None
    value = release.build(original, now=NOW, code_revision=COMMIT)
    projection_completeness(original, value, now=NOW)
    context = value['market_usability']['cr_front']
    assert (context['contract_metadata'] is not None) == (defect in ('none', 'quote_only'))
    assert context['quote']['values']['bid'] == 12.79
    if defect == 'quote_only': assert 'cr_front' not in facts(value)
    else: assert facts(value)['cr_front']['values']['last'] == 12.8


@pytest.mark.parametrize('block', ['market_structure', 'timeframe_context', 'news_context', 'previous', 'comparisons', 'metadata'])
def test_reverse_oracle_rejects_core_omissions(block):
    original = core_snapshot()
    original['components']['futoi_live'] = futoi_snapshot()['components']['futoi_live']
    value = release.build(original, now=NOW, code_revision=COMMIT)
    if block == 'market_structure': value[block] = {'status': 'UNAVAILABLE'}
    elif block == 'timeframe_context': value[block] = []
    elif block == 'news_context': value[block]['events'] = []
    elif block == 'previous': value['futoi_context']['futoi_live']['previous_observation'] = None
    elif block == 'comparisons': value['futoi_context']['futoi_live']['comparisons'] = None
    else: value['market_usability']['cr_front']['quote'] = None
    with pytest.raises(AssertionError): projection_completeness(original, value, now=NOW)


@pytest.mark.parametrize('entrypoint', ['describe', 'build'])
def test_malformed_basis_cannot_restore_expired_futoi_admission(entrypoint):
    original = futoi_snapshot()
    later = NOW + timedelta(days=1)
    original['live_read_freshness'] = {'read_at_utc': later.isoformat()}
    original['components']['live_basis_carry']['data']['pairs']['cny']['metrics'] = [None]
    before = deepcopy(original)
    # Full read-view validation fails before it can return the downgrade. A
    # consumer must not then publish the persisted Si/CR authority flags.
    with pytest.raises(AttributeError):
        if entrypoint == 'describe': release.describe(original)
        else: release.build(original, now=later, code_revision=COMMIT)
    assert original == before


@pytest.mark.parametrize('corruption', ['extrema_empty', 'extrema_high', 'extrema_date', 'extrema_completion', 'position_absent', 'position_unavailable', 'position_price', 'position_direction', 'position_time', 'position_extra'])
def test_reverse_oracle_rejects_extrema_and_position_loss_or_corruption(corruption):
    original = core_snapshot(); value = release.build(original, now=NOW, code_revision=COMMIT)
    extrema = value['market_structure']['values']['observed_extrema']
    position = value['user_position_context']
    if corruption == 'extrema_empty': extrema.clear()
    elif corruption == 'extrema_high': extrema['prior_observed_date']['high'] = 999
    elif corruption == 'extrema_date': extrema['prior_observed_date']['trade_date'] = '2026-09-01'
    elif corruption == 'extrema_completion': extrema['prior_observed_date']['session_completion_proven'] = True
    elif corruption == 'position_absent': value.pop('user_position_context')
    elif corruption == 'position_unavailable': value['user_position_context'] = {'status': 'UNAVAILABLE', 'availability': 'NO_EXPLICIT_USER_INPUT'}
    elif corruption == 'position_price': position['average_entry_price'] = 999
    elif corruption == 'position_direction': position['direction'] = 'SHORT'
    elif corruption == 'position_time': position['user_input_updated_at'] = '2026-09-01T12:00:00+00:00'
    else: position['capital'] = 100000
    with pytest.raises(AssertionError): projection_completeness(original, value, now=NOW)


@pytest.mark.parametrize('state', ['absent', 'invalid', 'future', 'flat', 'short'])
def test_position_oracle_proves_missing_invalid_and_explicit_states(state):
    original = core_snapshot(); position = original['user_position_context']
    if state == 'absent': original.pop('user_position_context')
    elif state == 'invalid': position['average_entry_price'] = True
    elif state == 'future': position['user_input_updated_at'] = (NOW + timedelta(seconds=1)).isoformat()
    elif state == 'flat': position.update(direction='FLAT', average_entry_price=None)
    else: position['direction'] = 'SHORT'
    value = release.build(original, now=NOW, code_revision=COMMIT)
    projection_completeness(original, value, now=NOW)
    value['user_position_context']['availability'] = 'FABRICATED'
    with pytest.raises(AssertionError): projection_completeness(original, value, now=NOW)
