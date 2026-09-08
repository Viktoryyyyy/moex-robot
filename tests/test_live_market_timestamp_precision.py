"""Preserve microseconds through source normalization and consumer time gates."""
from datetime import datetime, timedelta, timezone

import pytest

from moex_data import live_basis_carry_context as basis
from moex_data import synchronized_live_market_oi_context as live
from moex_data import synchronized_live_market_oi_context_partial as partial
from moex_data.rub_snapshot_read_freshness import apply_read_freshness

EARLY = datetime(2026, 9, 8, 7, 0, 0, 100000, tzinfo=timezone.utc)
RECEIPT = datetime(2026, 9, 8, 7, 1, 0, 50000, tzinfo=timezone.utc)


def payloads(late):
    specs = [('USDRUBF', '2099-12-31'), ('CNYRUBF', '2099-12-31'),
             ('SiU6', '2026-09-17'), ('SiZ6', '2026-12-17'),
             ('CRU6', '2026-09-17'), ('CRZ6', '2026-12-17')]
    securities, market = [], []
    for secid, expiry in specs:
        security = dict(SECID=secid, BOARDID='RFUD', LASTTRADEDATE=expiry,
                        MINSTEP=1, STEPPRICE=1)
        securities.append([security.get(key) for key in live.FUTURES_SECURITY_COLUMNS])
        row = dict(SECID=secid, OPEN=10, HIGH=12, LOW=9, LAST=11,
                   OPENPOSITION=100, BID=10, OFFER=11,
                   SYSTIME=(EARLY if secid == 'SiU6' else late).isoformat())
        market.append([row.get(key) for key in live.FUTURES_MARKETDATA_COLUMNS])
    spot = dict(SECID='CNYRUB_TOM', OPEN=10, HIGH=12, LOW=9, LAST=11,
                BID=10, OFFER=11, SYSTIME=late.isoformat())
    return ({'securities': {'columns': list(live.FUTURES_SECURITY_COLUMNS), 'data': securities},
             'marketdata': {'columns': list(live.FUTURES_MARKETDATA_COLUMNS), 'data': market}},
            {'marketdata': {'columns': list(live.CETS_MARKETDATA_COLUMNS),
                            'data': [[spot.get(key) for key in live.CETS_MARKETDATA_COLUMNS]]}})


def normalized(late, *, receipt=RECEIPT):
    forts, cets = payloads(late)
    return live.build_snapshot_from_payloads(forts_payload=forts, cets_payload=cets,
        forts_received_at_utc=receipt, cets_received_at_utc=receipt)


def front_metric(context):
    return next(metric for metric in context['pairs']['usd_rub']['metrics']
                if metric['stage4_metric_id'] == 'front_perpetual_basis_abs')


@pytest.mark.parametrize('microseconds,expected', [(0, True), (1, False), (800000, False)])
def test_exact_skew_boundary_survives_normalization_partial_and_basis(microseconds, expected):
    late = EARLY + timedelta(seconds=60, microseconds=microseconds)
    market = normalized(late)
    assert all(row['stale'] is False for row in market['instruments'].values())
    assert market['instruments']['si_front']['timestamp'] == EARLY.isoformat()
    assert market['instruments']['usdrubf']['timestamp'] == late.isoformat()
    assert market['snapshot_received_at_utc'] == RECEIPT.isoformat()
    assert market['synchronization']['synchronized'] is expected
    partial_market = partial._reclassify(market)
    assert partial_market['synchronization']['futures_synchronized'] is expected
    metric = front_metric(basis.build_context(partial_market))
    assert metric['synchronized'] is expected
    assert metric['status'] == ('READY' if expected else 'UNAVAILABLE')
    assert metric['source_timestamps']['si_front'] == EARLY.isoformat()
    assert metric['source_timestamps']['usdrubf'] == late.isoformat()
    if not expected:
        assert metric['value'] is None
        assert metric['unavailable_reason'] == 'source_timestamp_skew_exceeds_threshold'


def test_basis_direct_input_does_not_round_subsecond_skew():
    market = normalized(EARLY + timedelta(seconds=60))
    # Test the second serialization boundary independently from source parsing.
    market['instruments']['usdrubf']['timestamp'] = (EARLY + timedelta(seconds=60, microseconds=1)).isoformat()
    metric = front_metric(basis.build_context(market))
    assert metric['status'] == 'UNAVAILABLE'
    assert metric['synchronized'] is False


def test_equivalent_offsets_preserve_the_same_precise_instant():
    market = normalized(EARLY + timedelta(seconds=60))
    source = market['instruments']['usdrubf']
    stamp = datetime.fromisoformat(source['timestamp'])
    source['timestamp'] = stamp.astimezone(timezone(timedelta(hours=3))).isoformat()
    metric = front_metric(basis.build_context(market))
    assert metric['status'] == 'READY'
    assert metric['source_timestamps']['usdrubf'] == stamp.isoformat()
    assert live._iso(stamp.astimezone(timezone(timedelta(hours=3)))) == stamp.isoformat()


@pytest.mark.parametrize('microseconds,expected', [(0, True), (1, False)])
def test_read_freshness_uses_original_source_instant(microseconds, expected):
    market = partial._reclassify(normalized(EARLY + timedelta(seconds=30)))
    context = basis.build_context(market)
    context['current_live_scope_status'] = 'READY'
    snapshot = {'components': {
        'synchronized_live_market_oi': {'status': 'READY', 'data': market},
        'live_basis_carry': {'status': 'READY', 'data': context}},
        'authority': {'live_market_oi_factual_authority': True,
                      'live_basis_carry_factual_authority': True}}
    read = apply_read_freshness(snapshot, now=EARLY + timedelta(seconds=60, microseconds=microseconds))
    metric = front_metric(read['components']['live_basis_carry']['data'])
    assert (metric['status'] == 'READY') is expected
    assert read['components']['synchronized_live_market_oi']['data']['instruments']['si_front']['stale'] is not expected


def test_whole_second_output_remains_compatible():
    stamp = EARLY.replace(microsecond=0)
    assert live._iso(stamp) == basis._iso(stamp) == '2026-09-08T07:00:00+00:00'
