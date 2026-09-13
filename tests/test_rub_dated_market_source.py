"""Synthetic native ISS tables; expected arithmetic is independent of replay."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest

from moex_data import rub_dated_context as dated, rub_dated_market_source as source
from moex_data import rub_dated_basis_source as basis_source
from moex_data import synchronized_live_market_oi_context as core

NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)


def payloads():
    ids = ['USDRUBF', 'SiU6', 'SiZ6', 'CNYRUBF', 'CRU6', 'CRZ6']
    prices = [80, 81000, 82000, 11, 11.1, 11.2]
    securities = {'columns': ['SECID', 'BOARDID', 'LASTTRADEDATE', 'MINSTEP', 'STEPPRICE'],
                  'data': [[s, 'RFUD', '2026-09-17' if s.endswith('U6') else '2026-12-17', 1, 1] for s in ids]}
    columns = ['SECID', 'LAST', 'OPEN', 'HIGH', 'LOW', 'VOLTODAY', 'VALTODAY', 'NUMTRADES', 'OPENPOSITION', 'BID', 'OFFER', 'SYSTIME']
    rows = [[s, p, p, p + 1, p - 1, 100, 1000, 10, 2000, p - .1, p + .1, '2026-09-11 23:50:01'] for s, p in zip(ids, prices)]
    return ({'securities': securities, 'marketdata': {'columns': columns, 'data': rows}},
            {'marketdata': {'columns': ['SECID', 'LAST', 'OPEN', 'HIGH', 'LOW', 'VOLTODAY', 'NUMTRADES', 'WAPRICE', 'BID', 'OFFER', 'SYSTIME'],
                            'data': [['CNYRUB_TOM', 10.9, 10.9, 11, 10, 100, 10, 10.9, 10.8, 10.95, '2026-09-11 23:50:02']]}})


def acquisitions(now=NOW, spot=True):
    forts, cets = payloads(); target = {}
    source.collect(target, payload=forts, source_url='https://apim.moex.com' + core.FORTS_ENDPOINT,
                   requested=now - timedelta(seconds=2), received=now - timedelta(seconds=1), future=True)
    if spot:
        source.collect(target, payload=cets, source_url='https://apim.moex.com' + core.CETS_ENDPOINT,
                       requested=now - timedelta(seconds=2), received=now - timedelta(seconds=1), future=False)
    return target


def test_cold_start_all_seven_and_same_acquisition_basis_with_no_live_admission():
    store = source.capture(None, acquisitions(), now=NOW)
    admitted, errors = dated.validated(store, NOW)
    assert errors == {}
    assert {k for k in admitted if k.startswith('market:')} == {'market:' + k for k in core.LOGICAL_ORDER}
    assert admitted['market:si_front'][2]['last'] == 81000
    assert admitted['market:si_front'][2]['stale'] is True
    assert admitted['market:si_front'][2]['price_oi_usable'] is False
    assert admitted['basis:usd_rub.front_next_spread_abs'][2]['value'] == 1
    assert admitted['basis:usd_rub.front_next_term_carry_annualized'][2]['value'] == pytest.approx((82 / 81 - 1) * 365 / 91)
    view = dated.describe({'accepted_dated_market': store}, now=NOW)
    assert len(view['observations']) == 29  # seven instruments, 7 USD + 15 CNY metrics
    assert len(json.dumps(store)) < 200000
    for item in view['observations'].values():
        assert item['current_usable'] is item['historical_pit_usable'] is item['model_usable'] is False
        assert item['raw_source_digest']
        if 'original_legs' in item:
            assert item['values']['freshness']['status'] == 'DATED_96H'
            assert all(leg['current_usable'] is False for leg in item['original_legs'].values())
            assert 'FRESH' not in json.dumps(item)


def test_repeat_next_day_does_not_renew_acceptance_or_revision():
    first = source.capture(None, acquisitions(), now=NOW)
    later = NOW + timedelta(days=1)
    assert source.capture(first, acquisitions(later), now=later) == first


def test_unrelated_registry_and_cny_revision_do_not_renew_usd():
    first = source.capture(None, acquisitions(), now=NOW)
    later = NOW + timedelta(seconds=30)
    evidence = acquisitions(later)
    for row in evidence.values():
        row['binding_reference']['securities']['data'].append(['CRH7', 'RFUD', '2027-03-18', 1, 1])
    evidence['cnyrub_tom']['raw_source_payload']['marketdata_row']['LAST'] = 10.95
    second = source.capture(first, evidence, now=later)
    for key in ('market:si_front', 'market:usdrubf', 'basis:usd_rub.front_next_spread_abs'):
        assert second['selections'][key] == first['selections'][key]
    assert second['selections']['market:cnyrub_tom'] != first['selections']['market:cnyrub_tom']


def test_missing_cr_family_does_not_block_fixed_or_si():
    forts, _ = payloads()
    forts['securities']['data'] = [r for r in forts['securities']['data'] if not r[0].startswith('CR')]
    evidence = {}
    source.collect(evidence, payload=forts, source_url='https://apim.moex.com' + core.FORTS_ENDPOINT,
                   requested=NOW - timedelta(seconds=2), received=NOW - timedelta(seconds=1), future=True)
    assert {'usdrubf', 'cnyrubf', 'si_front', 'si_next'} == set(evidence)
    assert len(source.capture(None, evidence, now=NOW)['selections']) > 4


def test_native_observation_age_and_generation_cannot_be_relabelled():
    frame = source.make_frame('usdrubf', acquisitions()['usdrubf'], now=NOW)
    frame['generation_at_utc'] = (NOW - timedelta(days=1)).isoformat()
    with pytest.raises(ValueError, match='generation'): source.replay(frame)


@pytest.mark.parametrize('store', [['corrupt'], 'corrupt', 42])
def test_malformed_store_refuses_without_breaking_other_description(store):
    result = dated.describe({'accepted_dated_market': store}, now=NOW)
    assert result['observations'] == {} and result['refusals']


def test_first_live_capture_can_supersede_prior_b_basis():
    from test_rub_fast_market import market
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live
    previous = source.capture(None, acquisitions(), now=NOW)
    raw = market()
    for item in raw['instruments'].values():
        item.update(timestamp=NOW.isoformat(), received_at_utc=NOW.isoformat(), stale=False,
                    source_update_timestamp_utc=NOW.isoformat())
    witness = {'components': {}, 'authority': {}, 'analysis_views': {}, 'analysis_workflow': {}}
    live.attach_live_market_oi_context(witness, raw, attempted_at_utc=NOW.isoformat())
    live.attach_live_basis_carry_context(witness, raw, attempted_at_utc=NOW.isoformat())
    new = dated.capture(previous, components=witness['components'], now=NOW, kind='market')
    assert any(f.get('origin') is None for f in new['frames'].values())


def test_compact_and_reverse_acceptance_support_b_basis_without_live_components():
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    from test_rub_fast_market import snapshot
    view = snapshot()
    view['identity']['generated_at_utc'] = NOW.isoformat()
    view['accepted_dated_market'] = source.capture(None, acquisitions(), now=NOW)
    result = release.compact(view, now=NOW, code_revision='a' * 40)
    assert len(result['dated_context']['observations']) == 29
    projection_completeness(view, result, now=NOW)


def test_missing_spot_and_invalid_instrument_do_not_erase_other_futures():
    evidence = acquisitions(spot=False)
    evidence['si_front']['raw_source_payload']['marketdata_row']['LAST'] = True
    store = source.capture(None, evidence, now=NOW)
    admitted, _ = dated.validated(store, NOW)
    assert 'market:cnyrub_tom' not in admitted and 'market:si_front' not in admitted
    assert {'market:usdrubf', 'market:si_next', 'market:cnyrubf', 'market:cr_front', 'market:cr_next'} <= admitted.keys()
    assert 'basis:cny_rub.front_next_spread_abs' in admitted


@pytest.mark.parametrize('field,value', [('LAST', True), ('LAST', -1), ('OPENPOSITION', -1),
    ('OPENPOSITION', 1.5), ('OPENPOSITION', False), ('SYSTIME', '2026-09-13 16:00:00'),
    ('SECID', 'ALIEN'), ('HIGH', 2), ('VOLTODAY', -1)])
def test_rehashed_native_corruption_rejected(field, value):
    evidence = acquisitions()['si_front']
    frame = source.make_frame('si_front', evidence, now=NOW)
    frame['raw_source_payload']['marketdata_row'][field] = value
    frame['raw_source_digest'] = dated.digest(frame['raw_source_payload'])
    frame['revision_id'] = source._revision(frame)
    with pytest.raises((ValueError, core.SynchronizedLiveMarketOIError)):
        source.replay(frame)


@pytest.mark.parametrize('mutation', ['unit', 'source', 'board', 'expiry', 'step', 'binding', 'request', 'digest'])
def test_rehashed_metadata_and_custody_rejected(mutation):
    frame = source.make_frame('si_front', acquisitions()['si_front'], now=NOW)
    if mutation == 'unit': frame['units']['last'] = 'RUB_per_USD'
    if mutation == 'source': frame['identity']['source_url'] = 'https://iss.moex.com' + core.FORTS_ENDPOINT
    if mutation == 'board': frame['identity']['boardid'] = 'OTHER'
    if mutation == 'expiry': frame['raw_source_payload']['securities']['data'][0][2] = '2026-09-10'
    if mutation == 'step': frame['raw_source_payload']['securities']['data'][0][3] = True
    if mutation == 'binding': frame['identity']['logical_id'] = 'si_next'; frame['purpose'] = 'market:si_next'
    if mutation == 'request': frame['request_started_at_utc'] = (NOW + timedelta(seconds=1)).isoformat()
    frame['raw_source_digest'] = dated.digest(frame['raw_source_payload'])
    frame['revision_id'] = source._revision(frame)
    if mutation == 'digest': frame['raw_source_digest'] = '0' * 64
    with pytest.raises(ValueError): source.replay(frame)


def test_basis_mixed_generation_rejected_and_consumer_expiry():
    legs = {k: source.make_frame(k, v, now=NOW) for k, v in acquisitions().items()}
    frame = basis_source.make_frame(legs, now=NOW)
    frame['leg_evidence']['si_front']['generation_at_utc'] = (NOW - timedelta(seconds=1)).isoformat()
    with pytest.raises(ValueError, match='mixed_acquisitions'): basis_source.replay(frame)
    store = source.capture(None, acquisitions(), now=NOW)
    admitted, rejected = dated.validated(store, NOW + timedelta(hours=96))
    assert not admitted and rejected


def test_fast_default_adapter_sink_persists_dated_even_when_live_fetch_fails(tmp_path, monkeypatch):
    from moex_data import rub_fast_market as fast
    def fetch(**kwargs):
        kwargs['dated_evidence_sink'].update(acquisitions(spot=False))
        raise TimeoutError('spot missing')
    monkeypatch.setattr('moex_data.synchronized_live_market_oi_context_partial.fetch_live_snapshot', fetch)
    saved = fast.refresh(tmp_path, clock=lambda: NOW)
    assert saved['status'] == 'FAILED' and saved['market'] is None
    assert len([k for k in saved['accepted_dated_market']['selections'] if k.startswith('market:')]) == 6


def test_authenticated_adapter_captures_successful_forts_before_spot_failure():
    from moex_data import synchronized_live_market_oi_context_apim as apim
    forts, _ = payloads()
    class Response:
        status_code = 200
        def __init__(self, url): self.url = url
        def raise_for_status(self): pass
        def json(self): return deepcopy(forts)
    def get(url, **kwargs):
        assert kwargs['headers']['Authorization'] == 'Bearer synthetic-test'
        assert kwargs['allow_redirects'] is False
        if '/CETS/' in url: raise TimeoutError('synthetic spot failure')
        return Response(url)
    evidence = {}
    with pytest.raises(TimeoutError):
        apim.fetch_live_snapshot(http_get=get, now_fn=lambda: NOW,
                                 env={'MOEX_API_KEY': 'synthetic-test'}, dated_evidence_sink=evidence)
    assert set(evidence) == set(core.FUTURES_LOGICAL_ORDER)
    assert 'synthetic-test' not in json.dumps(evidence)
    result = source.capture(None, evidence, now=NOW)
    assert 'market:usdrubf' in result['selections']
    assert result['last_source_admission_refusals']['market:cnyrub_tom']


def test_cli_summary_does_not_duplicate_raw_store():
    from moex_data.rub_fast_market import collection_summary
    store = source.capture(None, acquisitions(), now=NOW)
    result = collection_summary({'status': 'COLLECTED', 'market': {'large': True}, 'accepted_dated_market': store})
    assert result['accepted_dated_market']['frame_count'] == len(store['frames'])
    assert 'raw_source_payload' not in json.dumps(result)
    assert 'market' not in result and len(json.dumps(result)) < 2500
