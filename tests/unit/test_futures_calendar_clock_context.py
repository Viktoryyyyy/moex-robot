from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import runpy

import pytest
from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
from moex_data import rub_futures_calendar as calendar

NOW = datetime(2026,9,8,10,tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def isolate_unrelated_cny_timestamp_policy(monkeypatch):
    # Clock tests use injected non-CNY producers. Installing the process-global
    # CNY adapter patch here would leak into independent source contract tests.
    monkeypatch.setattr(runner, 'install_timestamp_policy', lambda: None)


class Clock:
    def __init__(self, offsets):
        self.values = iter(NOW + timedelta(seconds=n) for n in offsets)
        self.readings = []
    def __call__(self):
        value = next(self.values); self.readings.append(value)
        return value


def raw():
    helpers = runpy.run_path(str(Path(__file__).with_name('test_rub_futures_calendar.py')))
    return helpers['encoded'](helpers['payload']())


def producers():
    helpers = runpy.run_path(str(Path(__file__).with_name('test_usdrubf_s7_3_chat_analysis_snapshot.py')))
    return {**helpers['_producers'](), 'futures_calendar': runner._futures_calendar_component}


@pytest.mark.parametrize('mode', ['LIVE', 'TEST'])
def test_advancing_clock_request_receipt_completion_and_immutable_manifest(tmp_path, monkeypatch, mode):
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    clock = Clock([0, 1, 9, 10])
    calls = []
    def transport(url, *, env):
        calls.append(url); return raw()
    if mode == 'LIVE':
        monkeypatch.setattr(runner, '_live_now', lambda: clock.readings[-1])
        monkeypatch.setattr(calendar, '_fetch', transport)
    context = runner.CalendarClockContext(mode=mode, now_fn=clock,
        fetch=transport if mode == 'TEST' else None)
    value, path = runner.refresh_snapshot(producers=producers(), calendar_context=context)
    data = value['components']['futures_calendar']['data']
    assert value['components']['futures_calendar']['status'] == 'READY'
    assert data['requested_at'] == (NOW+timedelta(seconds=1)).isoformat()
    assert data['received_at'] == data['system_available_at'] == (NOW+timedelta(seconds=9)).isoformat()
    assert datetime.fromisoformat(value['identity']['generated_at_utc']) == NOW+timedelta(seconds=10)
    assert len(clock.readings) == 4 and len(calls) == 1
    assert calls[0] == calendar.source_url(*calendar.interval(NOW+timedelta(seconds=1)))
    manifest = json.loads(Path(data['manifest_path']).read_text())
    assert manifest['requested_at'] == data['requested_at']
    assert manifest['received_at'] == data['received_at']
    assert json.loads(path.read_text()) == value
    assert all(data[key] is False for key in calendar.DENIED)


def test_default_live_context_shares_clock_after_slow_preceding_collector(tmp_path, monkeypatch):
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    clock = Clock([0, 8, 9, 17, 18])
    monkeypatch.setattr(runner, '_live_now', lambda: clock.readings[-1])
    selected = producers()
    previous = selected['cbr_macro']
    def slow_collector(now):
        assert clock() - now == timedelta(seconds=8)
        return previous(now)
    selected['cbr_macro'] = slow_collector
    requests = []
    def transport(url, *, env):
        requests.append((url, clock.readings[-1]))
        return raw()
    monkeypatch.setattr(calendar, '_fetch', transport)
    # Exercise automatic LIVE context creation, as production overlays do.
    value, _ = runner.refresh_snapshot(now_fn=clock, producers=selected)
    component = value['components']['futures_calendar']
    assert component['status'] == 'READY'
    data = component['data']
    requested = datetime.fromisoformat(data['requested_at'])
    received = datetime.fromisoformat(data['received_at'])
    completed = datetime.fromisoformat(value['identity']['generated_at_utc'])
    assert requested == NOW + timedelta(seconds=9)
    assert received == NOW + timedelta(seconds=17)
    assert completed == NOW + timedelta(seconds=18)
    assert NOW < requested < received < completed
    assert requests == [(calendar.source_url(*calendar.interval(requested)), requested)]
    assert len(clock.readings) == 5
    assert data['system_available_at'] == data['received_at']
    assert all(data[key] is False for key in calendar.DENIED)


@pytest.mark.parametrize('entry', ['refresh', 'build', 'explicit_live_build', 'producer'])
def test_historical_clock_without_context_rejected_before_http(tmp_path, monkeypatch, entry):
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    monkeypatch.setattr(runner, '_live_now', lambda: NOW+timedelta(days=1))
    def forbidden(*args, **kwargs): pytest.fail('HTTP must not run')
    monkeypatch.setattr(calendar, '_fetch', forbidden)
    with pytest.raises(runner.ChatAnalysisSnapshotError, match='clock|anchor'):
        if entry == 'refresh': runner.refresh_snapshot(now_fn=Clock([0]), producers=producers())
        elif entry == 'build': runner.build_snapshot(now=NOW, producers=producers())
        elif entry == 'explicit_live_build': runner.build_snapshot(now=NOW, producers=producers(), calendar_context=runner.CalendarClockContext())
        else: runner._futures_calendar_component(NOW)


@pytest.mark.parametrize('defect', ['before_receipt', 'expired', 'raw_changed', 'manifest_changed', 'valid'])
def test_replay_uses_original_receipts_without_fetch(tmp_path, monkeypatch, defect):
    data = calendar.load(root=tmp_path, env={}, now_fn=Clock([0,2]), fetch=lambda url,env: raw())
    original = {'status':'READY', 'data':data}; before = deepcopy(original)
    consumed = NOW+timedelta(seconds=3)
    if defect == 'before_receipt': consumed = NOW+timedelta(seconds=1)
    elif defect == 'expired': consumed = NOW+timedelta(seconds=1203)
    elif defect == 'raw_changed': Path(data['manifest_path']).with_name(data['raw_sha256']+'.json').write_bytes(b'{}')
    elif defect == 'manifest_changed': Path(data['manifest_path']).write_bytes(b'{}')
    def forbidden(*args, **kwargs): pytest.fail('Replay must not fetch or load')
    monkeypatch.setattr(calendar, 'load', forbidden)
    monkeypatch.setattr(calendar, '_fetch', forbidden)
    context = runner.CalendarClockContext(mode='REPLAY', archived_component=original)
    if defect == 'valid':
        result = context.produce(consumed)
        assert result.data['received_at'] == data['received_at']
        assert result.data_as_of == data['received_at']
        assert result.data['manifest_sha256'] == data['manifest_sha256']
    else:
        with pytest.raises(runner.ChatAnalysisSnapshotError): context.produce(consumed)
    assert original == before


def test_test_clock_requires_transport_and_shared_refresh_clock(tmp_path, monkeypatch):
    with pytest.raises(runner.ChatAnalysisSnapshotError): runner.CalendarClockContext(mode='TEST', now_fn=Clock([0]))
    with pytest.raises(runner.ChatAnalysisSnapshotError): runner.CalendarClockContext(mode='REPLAY', fetch=lambda: None)
    context = runner.CalendarClockContext(mode='TEST', now_fn=Clock([0]), fetch=lambda url,env: raw())
    with pytest.raises(runner.ChatAnalysisSnapshotError, match='share one clock'):
        runner.refresh_snapshot(producers=producers(), now_fn=Clock([0]), calendar_context=context)


@pytest.mark.parametrize('route', ['futoi', 'current_context', 'live_market'])
def test_overlay_binds_clock_before_slow_prework_and_real_prefetch(tmp_path, monkeypatch, route):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as futoi
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current_context as current_context
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live_market
    base = futoi.base
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    monkeypatch.setattr(base, 'install_timestamp_policy', lambda: None)
    monkeypatch.setattr(futoi, '_load_governance', lambda: {})
    monkeypatch.setattr(futoi, '_futoi_component', lambda **kwargs: {'status': 'UNAVAILABLE', 'data': {}})
    selected = producers(); selected['futures_calendar'] = base._futures_calendar_component
    clock = Clock([0, 8, 9, 17, 18])
    monkeypatch.setattr(base, '_live_now', lambda: clock.readings[-1])
    requests = []
    def fetch(url, *, env):
        requests.append(clock.readings[-1]); return raw()
    monkeypatch.setattr(calendar, '_fetch', fetch)
    def prework(**kwargs):
        assert clock() == NOW+timedelta(seconds=8)
        return {}
    if route == 'futoi':
        prior = selected['cbr_macro']
        def preceding(now):
            prework(); return prior(now)
        selected['cbr_macro'] = preceding
        value, _ = futoi.refresh_snapshot(now_fn=clock, producers=selected)
    else:
        monkeypatch.setattr(current_context.current, 'current_producers', lambda: selected)
        monkeypatch.setattr(current_context.context, 'run_refresh_all', prework)
        monkeypatch.setattr(current_context.delta_context, 'build_all', lambda **kwargs: {})
        monkeypatch.setattr(current_context, '_attach_futoi_context', lambda *args: None)
        if route == 'current_context':
            value, _ = current_context.refresh_snapshot(now_fn=clock)
        else:
            # Keep real prefetch and real futoi/base builders; unrelated attachments
            # are isolated so this regression exercises only calendar clock wiring.
            monkeypatch.setattr(live_market, 'attach_live_market_oi_context', lambda *args, **kwargs: None)
            monkeypatch.setattr(live_market, 'attach_live_basis_carry_context', lambda *args, **kwargs: None)
            monkeypatch.setattr(live_market.user_position, 'attach_user_position_context', lambda *args, **kwargs: None)
            value, _ = live_market.refresh_snapshot(now_fn=clock, live_loader=lambda: {})
    component = value['components']['futures_calendar']; data = component['data']
    assert component['status'] == 'READY'
    assert requests == [NOW+timedelta(seconds=9)]
    assert data['received_at'] == (NOW+timedelta(seconds=17)).isoformat()
    assert datetime.fromisoformat(value['identity']['refresh_started_at_utc']) == NOW
    assert datetime.fromisoformat(value['identity']['generated_at_utc']) == NOW+timedelta(seconds=18)
    assert len(clock.readings) == 5


@pytest.mark.parametrize('route', ['futoi', 'current_context', 'live_market'])
def test_overlay_historical_clock_rejected_before_prework_or_http(monkeypatch, tmp_path, route):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as futoi
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current_context as current_context
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live_market
    base = futoi.base
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    monkeypatch.setattr(base, 'install_timestamp_policy', lambda: None)
    monkeypatch.setattr(base, '_live_now', lambda: NOW+timedelta(days=1))
    selected = producers(); selected['futures_calendar'] = base._futures_calendar_component
    monkeypatch.setattr(current_context.current, 'current_producers', lambda: selected)
    def forbidden(*args, **kwargs): pytest.fail('historical clock must fail before prework or HTTP')
    monkeypatch.setattr(current_context.context, 'run_refresh_all', forbidden)
    monkeypatch.setattr(calendar, '_fetch', forbidden)
    with pytest.raises(base.ChatAnalysisSnapshotError):
        if route == 'futoi': futoi.refresh_snapshot(now_fn=Clock([0]), producers=selected)
        elif route == 'current_context': current_context.refresh_snapshot(now_fn=Clock([0]))
        else: live_market.refresh_snapshot(now_fn=Clock([0]), live_loader=forbidden)
