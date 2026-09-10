"""Labelled acceptance simulations; these are not archived production replay."""
from copy import deepcopy
from datetime import timedelta
import json

import pytest
from moex_data import rub_dated_context as dated, rub_fast_market as fast
from moex_data import rub_factual_release as release
from moex_data.rub_snapshot_read_freshness import apply_read_freshness
from test_rub_fast_market import NOW, market, snapshot, publish, read


def package(root, now):
    view = read(root, now)
    return release.compact(view, now=now, code_revision='a' * 40)


def test_last_accepted_simulation_retains_values_but_never_live_after_failure(tmp_path):
    original = publish(tmp_path)
    assert original['accepted_dated_market']['selections']
    later = NOW + timedelta(hours=12)
    def failed(): raise TimeoutError()
    fast.refresh(tmp_path, loader=failed, clock=lambda: later)
    value = package(tmp_path, later)
    assert not any(item['factor'] in dated.MARKETS for item in value['facts'])
    assert value['generations']['fast_market']['error']
    prior = value['dated_context']['observations']['market:si_front']
    assert prior['accepted_at_utc'] == NOW.isoformat()
    assert prior['checked_at_utc'] == later.isoformat()
    assert prior['values']['last'] == original['market']['instruments']['si_front']['last']
    assert prior['current_usable'] is False
    assert not any('futoi' in key for key in value['dated_context']['observations'])


def test_partial_new_frame_preserves_old_spot_and_same_frame_basis(tmp_path):
    initial = publish(tmp_path)
    later = NOW + timedelta(seconds=30)
    current = market()
    for key, item in current['instruments'].items():
        if key != 'cnyrub_tom':
            item.update(timestamp=later.isoformat(), received_at_utc=later.isoformat())
    current['instruments']['cnyrub_tom']['stale'] = True
    second = fast.refresh(tmp_path, loader=lambda: current, clock=lambda: later)
    selectors = second['accepted_dated_market']['selections']
    assert selectors['market:cnyrub_tom'] == initial['accepted_dated_market']['selections']['market:cnyrub_tom']
    assert selectors['market:si_front'] != initial['accepted_dated_market']['selections']['market:si_front']
    assert set(second['accepted_dated_market']['frames']) == set(selectors.values())
    result = dated.describe(read(tmp_path, later), now=later)
    for key, item in result['observations'].items():
        if key.startswith('basis:'):
            assert set(item['original_bindings']) == set(item['values']['legs'])
            assert all(item['original_legs'][leg]['secid'] == secid for leg, secid in item['original_bindings'].items())


@pytest.mark.parametrize('defect', ['digest', 'missing', 'quality_rehashed', 'identity_rehashed', 'basis_rehashed', 'future_rehashed', 'same_row_rehashed', 'fractional_oi_rehashed'])
def test_corrupt_acceptance_evidence_refuses_even_recomputed_digest(tmp_path, defect):
    value = publish(tmp_path); store = value['accepted_dated_market']
    old = store['selections']['market:si_front']; frame = store['frames'][old]
    if defect == 'missing': del store['frames'][old]
    else:
        item = frame['components']['synchronized_live_market_oi']['data']['instruments']['si_front']
        if defect == 'identity_rehashed': item['secid'] = 'ALIEN'
        elif defect == 'same_row_rehashed': item['price_oi_same_source_row'] = False
        elif defect == 'fractional_oi_rehashed': item['oi'] = 1.5
        elif defect == 'quality_rehashed': item['price_oi_usable'] = False
        elif defect == 'future_rehashed': item['received_at_utc'] = (NOW + timedelta(seconds=1)).isoformat()
        elif defect == 'basis_rehashed': frame['components']['live_basis_carry']['data']['ready_metric_count'] = 999
        else: item['last'] = 999
        if defect.endswith('rehashed'):
            new = dated.digest(frame); store['frames'][new] = store['frames'].pop(old)
            store['selections'] = {key: new if ref == old else ref for key, ref in store['selections'].items()}
    result = dated.describe({'accepted_dated_market': store}, now=NOW)
    assert 'market:si_front' not in result['observations']
    assert result['refusals']


def test_exact_source_age_bound_and_no_reingest_extension(tmp_path):
    value = publish(tmp_path)
    source = value['market']['instruments']['si_front']['timestamp']
    edge = dated.stamp(source) + timedelta(seconds=dated.MAX_AGE_SECONDS)
    assert 'market:si_front' in dated.describe({'accepted_dated_market': value['accepted_dated_market']}, now=edge)['observations']
    assert 'market:si_front' not in dated.describe({'accepted_dated_market': value['accepted_dated_market']}, now=edge + timedelta(microseconds=1))['observations']


def test_stale_unaccepted_initial_cache_does_not_become_dated(tmp_path):
    old = market()
    for item in old['instruments'].values(): item['timestamp'] = (NOW - timedelta(hours=3)).isoformat()
    value = publish(tmp_path, old)
    assert not value['accepted_dated_market']['selections']


def test_missing_spot_evidence_does_not_erase_independent_futures(tmp_path):
    data = market()
    data['instruments']['cnyrub_tom'].update(timestamp=None, received_at_utc=None, source_id=None, last=None)
    value = publish(tmp_path, data)
    assert 'market:si_front' in value['accepted_dated_market']['selections']
    assert 'market:cnyrub_tom' not in value['accepted_dated_market']['selections']


def test_identical_source_does_not_renew_original_acceptance(tmp_path):
    initial = publish(tmp_path)
    later = fast.refresh(tmp_path, loader=market, clock=lambda: NOW + timedelta(seconds=1))
    assert later['accepted_dated_market']['selections'] == initial['accepted_dated_market']['selections']


def test_reingested_same_publication_preserves_first_acceptance_receipt(tmp_path):
    initial = publish(tmp_path)
    data = market()
    for item in data['instruments'].values(): item['received_at_utc'] = (NOW + timedelta(seconds=1)).isoformat()
    later = fast.refresh(tmp_path, loader=lambda: data, clock=lambda: NOW + timedelta(seconds=1))
    assert later['accepted_dated_market']['selections'] == initial['accepted_dated_market']['selections']


@pytest.mark.parametrize('defect', ['component', 'data_status', 'quality_status', 'factual_denied', 'quality_missing', 'row_denied', 'basis_denied'])
def test_original_component_and_quality_denials_cannot_be_minted_or_rehashed(tmp_path, defect):
    value = publish(tmp_path); store = value['accepted_dated_market']
    old = store['selections']['market:si_front']; frame = deepcopy(store['frames'][old])
    component = frame['components']['synchronized_live_market_oi']; raw = component['data']
    if defect == 'component': component['status'] = 'UNAVAILABLE'
    elif defect == 'data_status': raw['status'] = 'UNAVAILABLE'
    elif defect == 'quality_status': raw['quality']['status'] = 'FAIL'
    elif defect == 'factual_denied': raw['quality']['factual_context_usable'] = False
    elif defect == 'quality_missing': raw.pop('quality')
    elif defect == 'row_denied': raw['quality']['price_oi_usable_by_instrument']['si_front'] = False
    else: frame['components']['live_basis_carry']['status'] = 'UNAVAILABLE'
    captured = dated.capture(None, components=frame['components'], now=NOW, kind='market')
    ref = dated.digest(frame)
    rehashed = {'schema_version': dated.SCHEMA, 'frames': {ref: frame},
                'selections': {key: ref for key in store['selections']}}
    for result in (captured, rehashed):
        admitted, _ = dated.validated(result, NOW)
        if defect == 'row_denied':
            assert 'market:si_front' not in admitted and 'market:cr_front' in admitted
            assert all('si_front' not in item[2]['legs'] for key, item in admitted.items() if key.startswith('basis:'))
        elif defect == 'basis_denied':
            assert 'market:si_front' in admitted
            assert not any(key.startswith('basis:') for key in admitted)
        else: assert not admitted


def test_explicit_partial_original_admission_keeps_valid_futures_subset(tmp_path):
    data = market()
    data['status'] = 'PARTIAL'; data['quality'].update(status='PARTIAL', analysis_usable=False, spot_price_usable=False)
    data['instruments']['cnyrub_tom']['stale'] = True
    value = publish(tmp_path, data)
    admitted, _ = dated.validated(value['accepted_dated_market'], NOW)
    assert all('market:' + key in admitted for key in dated.MARKETS if key != 'cnyrub_tom')
    assert 'market:cnyrub_tom' not in admitted
    assert any(key.startswith('basis:') for key in admitted)


def test_rehashed_unknown_source_is_not_admitted_despite_matching_derived_basis(tmp_path):
    from src.moex_research.runners.usdrubf_s7_3_chat_analysis_snapshot_live_market_oi import _load_basis_carry_or_unavailable
    value = publish(tmp_path); store = value['accepted_dated_market']; old = store['selections']['market:si_front']
    frame = deepcopy(store['frames'][old]); raw = frame['components']['synchronized_live_market_oi']['data']
    raw['instruments']['si_front']['source_id'] = 'unknown_source'
    frame['components']['live_basis_carry']['data'] = _load_basis_carry_or_unavailable(raw)
    ref = dated.digest(frame)
    changed = {'schema_version': dated.SCHEMA, 'frames': {ref: frame}, 'selections': {key: ref for key in store['selections']}}
    admitted, _ = dated.validated(changed, NOW)
    assert 'market:si_front' not in admitted and 'market:cr_front' in admitted


@pytest.mark.parametrize('schema', [None, 'unknown_market.v99'])
def test_missing_or_unknown_schema_cannot_mint_or_rehash_market_witness(tmp_path, schema):
    from src.moex_research.runners.usdrubf_s7_3_chat_analysis_snapshot_live_market_oi import _load_basis_carry_or_unavailable
    value = publish(tmp_path); store = value['accepted_dated_market']
    frame = deepcopy(store['frames'][store['selections']['market:si_front']])
    raw = frame['components']['synchronized_live_market_oi']['data']
    if schema is None: raw.pop('schema_version')
    else: raw['schema_version'] = schema
    # Rebuild the matching failed basis shell: equality alone must not admit this schema.
    frame['components']['live_basis_carry']['data'] = _load_basis_carry_or_unavailable(raw)
    minted = dated.capture(None, components=frame['components'], now=NOW, kind='market')
    assert minted['selections'] == {}
    ref = dated.digest(frame)
    rehashed = {'schema_version': dated.SCHEMA, 'frames': {ref: frame}, 'selections': {key: ref for key in store['selections']}}
    admitted, refused = dated.validated(rehashed, NOW)
    assert admitted == {} and set(refused.values()) == {'unsupported_original_market_schema'}


@pytest.mark.parametrize('defect', ['none', 'spread', 'crossed', 'quote_map', 'source_value', 'quote_coherence'])
def test_rehashed_quote_gate_preserves_price_oi_but_excludes_invalid_book(tmp_path, defect):
    value = publish(tmp_path); store = value['accepted_dated_market']
    frame = deepcopy(store['frames'][store['selections']['market:si_front']])
    raw = frame['components']['synchronized_live_market_oi']['data']; row = raw['instruments']['si_front']
    assert row['quote_usable'] is True
    if defect == 'spread': row['spread'] += 1
    elif defect == 'crossed': row['bid'] = row['ask'] + 1; row['spread'] = -1
    elif defect == 'quote_map': raw['quality']['quote_usable_by_instrument']['si_front'] = False
    elif defect == 'source_value': row['bid_source_value'] = row['bid'] + 1
    elif defect == 'quote_coherence': row['quote_temporal_coherence'] = 'UNKNOWN'
    ref = dated.digest(frame)
    rehashed = {'schema_version': dated.SCHEMA, 'frames': {ref: frame}, 'selections': {key: ref for key in store['selections']}}
    result = dated.describe({'accepted_dated_market': rehashed}, now=NOW)
    observed = result['observations']['market:si_front']
    assert observed['values']['last'] == row['last'] and observed['values']['oi'] == row['oi']
    assert ('bid' in observed['values']) == (defect == 'none')
    if defect != 'none':
        assert observed['quote_refusal']
        for key, item in result['observations'].items():
            if key.startswith('basis:') and 'si_front' in item['original_legs']:
                assert 'bid' not in item['original_legs']['si_front']


def test_metadata_survives_price_staleness_without_moving_to_new_contract(tmp_path):
    publish(tmp_path)
    value = package(tmp_path, NOW + timedelta(seconds=61))
    # Generation expiry prevents the entire current frame overlay; its own dated metadata remains scoped.
    dated_item = value['dated_context']['observations']['market:si_front']
    assert dated_item['contract_metadata']['secid'] == dated_item['source_identity']['secid']
    assert not value['market_usability']['si_front']['price_oi_usable']


def test_frozen_envelope_replays_without_network_and_input_mutation(tmp_path):
    publish(tmp_path)
    frozen = read(tmp_path, NOW); before = deepcopy(frozen)
    result = release.compact(frozen, now=NOW, code_revision='a' * 40)
    assert result == release.compact(json.loads(json.dumps(frozen)), now=NOW, code_revision='a' * 40)
    assert frozen == before
