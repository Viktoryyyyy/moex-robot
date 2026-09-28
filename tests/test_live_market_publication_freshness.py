"""Synthetic delayed refresh; real fast envelope, TTL, basis and saved reader."""
from copy import deepcopy
from datetime import timedelta
import json

import pytest

from moex_data import rub_fast_market as fast
from moex_data.rub_snapshot_read_freshness import apply_read_freshness
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as runner
from test_rub_fast_market import NOW, market, snapshot


def shifted_market(now):
    from datetime import datetime
    value = market()
    shift = now - NOW
    for row in value['instruments'].values():
        for key in ('timestamp', 'received_at_utc', 'source_update_timestamp_utc', 'freshness_reference_utc'):
            if row.get(key):
                row[key] = (datetime.fromisoformat(row[key]) + shift).isoformat()
    value['snapshot_received_at_utc'] = now.isoformat()
    for key in ('as_of_utc', 'oldest_timestamp_utc', 'futures_as_of_utc', 'futures_oldest_timestamp_utc'):
        if value['synchronization'].get(key):
            value['synchronization'][key] = (datetime.fromisoformat(value['synchronization'][key]) + shift).isoformat()
    return value


@pytest.mark.parametrize('cache', ['fresh', 'expired', 'failed', 'corrupt', 'absent', 'disabled'])
def test_real_refresh_publishes_rechecked_market_and_basis_after_slow_history(tmp_path, monkeypatch, cache):
    clock = [NOW]
    calls = []
    seed = snapshot()
    seed['identity']['generated_at_utc'] = NOW.isoformat()
    slow = deepcopy(seed['components'])
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    monkeypatch.setattr(runner.base, 'load_dotenv', lambda *a, **k: None)
    monkeypatch.setattr(runner.base, 'install_timestamp_policy', lambda: None)
    monkeypatch.setattr(runner.current_context.current, 'current_producers', lambda: {})
    monkeypatch.setattr(runner.current_context.context, 'run_refresh_all', lambda **k: {'instrument_results': {}})
    monkeypatch.setattr(runner.current_context.delta_context, 'build_all', lambda **k: {})
    monkeypatch.setattr(runner.futoi, 'build_snapshot', lambda **k: deepcopy(seed))
    monkeypatch.setattr(runner.current_context, '_attach_futoi_context', lambda *a, **k: None)
    monkeypatch.setattr('moex_data.rub_dated_hour_source.acquire', lambda **k: {'latest_attempts': {}})
    for module in ('rub_si_futoi_dated_context', 'rub_si_futoi_observed_statistics',
                   'rub_cr_futoi_dated_context', 'rub_cr_futoi_observed_statistics',
                   'rub_contract_price_market_oi_observed'):
        monkeypatch.setattr('moex_data.'+module+'.capture_snapshot', lambda *a, **k: clock[0])

    def history(*args, **kwargs):
        clock[0] += timedelta(minutes=4)
        if cache != 'disabled':
            folder = fast.state_path(tmp_path)
            folder.mkdir(parents=True, exist_ok=True)
            (folder / 'enabled').write_text(fast.SCHEMA)
            if cache != 'absent':
                captured = NOW if cache == 'expired' else clock[0]
                def loader():
                    if cache == 'failed':
                        raise TimeoutError('synthetic refusal')
                    return shifted_market(captured)
                value = fast.refresh(tmp_path, loader=loader, clock=lambda: captured)
                if cache == 'corrupt':
                    value['market']['instruments']['si_front']['last'] += 1
                    (folder / 'current.json').write_text(json.dumps(value))
        return clock[0]
    monkeypatch.setattr('moex_data.rub_historical_basis_carry_context.capture_snapshot', history)

    def live_loader():
        calls.append(clock[0])
        return shifted_market(clock[0])
    saved, path = runner.refresh_snapshot(now_fn=lambda: clock[0], live_loader=live_loader)
    persisted = json.loads(path.read_bytes())
    assert persisted == saved
    assert calls == [NOW]  # No extra quote request at the publication boundary.
    assert persisted['live_publication_freshness']['checked_at_utc'] == clock[0].isoformat()
    assert persisted['live_publication_freshness']['additional_live_fetch_performed'] is False
    for key, value in slow.items():
        assert persisted['components'][key]['data'] == value['data']
    before = path.read_bytes()
    read, _ = runner.base.read_current_snapshot(now_fn=lambda: clock[0])
    assert path.read_bytes() == before
    for view in (persisted, read):
        live = view['components'][runner.COMPONENT]['data']
        basis = view['components'][runner.BASIS_CARRY_COMPONENT]['data']
        if cache == 'fresh':
            assert all(row['price_oi_usable'] for key, row in live['instruments'].items() if key != 'cnyrub_tom')
            assert basis['ready_metric_count'] > 0
            for pair in basis['pairs'].values():
                for metric in pair['metrics']:
                    if metric['status'] == 'READY':
                        assert all(metric['source_timestamps'][key] == live['instruments'][key]['timestamp'] for key in metric['legs'])
                        assert max(metric['freshness']['age_seconds_by_leg'].values()) <= 60
        else:
            assert view['components'][runner.COMPONENT]['status'] == 'UNAVAILABLE'
            assert basis['ready_metric_count'] == 0
            assert view['authority']['live_market_oi_factual_authority'] is False
            assert view['authority']['live_basis_carry_factual_authority'] is False
    if cache == 'fresh':
        expired = apply_read_freshness(persisted, now=clock[0]+timedelta(seconds=61))
        assert expired['components'][runner.BASIS_CARRY_COMPONENT]['data']['ready_metric_count'] == 0
        assert expired['authority']['live_market_oi_factual_authority'] is False


def test_old_spot_does_not_discard_independent_futures_basis(tmp_path):
    value = snapshot()
    data = shifted_market(NOW)
    data['instruments']['cnyrub_tom']['timestamp'] = (NOW-timedelta(hours=2)).isoformat()
    fast.refresh(tmp_path, loader=lambda: data, clock=lambda: NOW)
    (fast.state_path(tmp_path)/'enabled').write_text(fast.SCHEMA)
    result = runner._live_context_at_publication(value, root=tmp_path, now=NOW)
    live = result['components'][runner.COMPONENT]['data']['instruments']
    assert live['cnyrub_tom']['stale'] and not live['cnyrub_tom']['price_oi_usable']
    assert all(row['price_oi_usable'] for key, row in live.items() if key != 'cnyrub_tom')
    metrics = [m for p in result['components'][runner.BASIS_CARRY_COMPONENT]['data']['pairs'].values() for m in p['metrics']]
    assert any(m['status'] == 'READY' and 'cnyrub_tom' not in m['legs'] for m in metrics)
    assert all(m['status'] == 'UNAVAILABLE' for m in metrics if 'cnyrub_tom' in m['legs'])
