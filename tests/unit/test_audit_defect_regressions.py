"""Offline regressions for the September 2026 audit; no market/API calls."""
from __future__ import annotations

import ast
import base64
import csv
import hmac
import importlib
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from moex_backtest.engine.canonical import CanonicalBacktestEngine, CanonicalBacktestInput, ExecutionConfig
from src.moex_backtest.engine.run_registered_backtest import _execute_canonical_backtest
from src.moex_features.intraday.si_15m_ohlc_from_5m import materialize_feature_frame
from src.strategies.ema_3_19_15m.config import validate_config
from src.strategies.ema_3_19_15m.signal_engine import generate_signals

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 4, 12, tzinfo=timezone.utc)


def materialize(tmp_path, minutes):
    path = tmp_path / 'bars.csv'
    pd.DataFrame([dict(end=NOW + timedelta(minutes=m), open=100., high=102., low=99., close=101., volume=1)
                  for m in minutes]).to_csv(path, index=False)
    return materialize_feature_frame(dataset_artifact_path=path, instrument_id='si', timezone_name='Europe/Moscow')


@pytest.mark.parametrize('minutes', [[5, 10], [5, 15], [10, 15], [5, 10, 10]])
def test_incomplete_or_duplicate_15m_is_not_finalized(tmp_path, minutes):
    with pytest.raises(ValueError):
        materialize(tmp_path, minutes)


def test_complete_intervals_survive_partial_tail_and_session_gap(tmp_path):
    frame = materialize(tmp_path, [5, 10, 15, 20, 25, 65, 70, 75])
    assert list(frame.end) == [pd.Timestamp(NOW + timedelta(minutes=m)).tz_convert('Europe/Moscow') for m in [15, 75]]
    assert list(frame.volume) == [3, 3]


@pytest.mark.parametrize('terminal_close', [True, False])
def test_backtest_marks_holding_bars_and_terminal_position(terminal_close):
    result = CanonicalBacktestEngine().run(CanonicalBacktestInput(
        bars=[dict(timestamp=i, open=p, close=p) for i, p in enumerate([100., 100., 50., 150.])],
        signals=[dict(timestamp=0, target_position=1)],
        execution_config=ExecutionConfig(terminal_close=terminal_close)))
    assert result.metrics['max_drawdown'] == 50.
    assert result.metrics['ending_equity'] == 50.
    assert result.metrics['total_pnl'] == 50.
    assert len(result.artifacts['marked_equity_path']) == 4


def test_registered_backtest_counts_first_loss_from_zero():
    ends = pd.date_range(NOW, periods=3, freq='15min')
    frame = pd.DataFrame(dict(instrument_id=['si']*3, end=ends, open=[100., 100., 90.], close=[100., 100., 90.]))
    result = _execute_canonical_backtest(feature_frame=frame,
        normalized_signals=({'decision_ts': ends[0], 'desired_position': 1.},), commission_points=0.)
    assert result.iloc[0].pnl_day == -10.
    assert result.iloc[0].max_dd_day == 10.


def test_terminal_liquidation_cost_counts_as_drawdown_from_last_close():
    from moex_backtest.engine.canonical import CostConfig
    result = CanonicalBacktestEngine().run(CanonicalBacktestInput(
        bars=[dict(timestamp=i, open=p, close=p) for i,p in enumerate([100.,100.,200.])],
        signals=[dict(timestamp=0, target_position=1)], cost_config=CostConfig(commission_bps=100.)))
    assert result.metrics['ending_equity'] == 97.
    assert result.metrics['max_drawdown'] == 2.


@pytest.mark.parametrize('warmup', [19, 25])
def test_ema_warmup_is_honored_and_boundary_cross_can_fire(warmup):
    config = validate_config({'warmup_bars': warmup})
    early = tuple(dict(end=NOW + timedelta(minutes=15*i), close=100. + i) for i in range(warmup-1))
    assert generate_signals(inputs=early, config=config) == ()
    rows = tuple(dict(end=NOW + timedelta(minutes=15*i), close=100. if i < warmup-1 else 101.) for i in range(warmup))
    assert len(generate_signals(inputs=rows, config=config)) == 1


@pytest.mark.parametrize('before,target', [(0., 1.), (1., -1.), (-1., 0.)])
def test_runtime_checkpoint_failure_retries_without_duplicate(tmp_path, monkeypatch, before, target):
    engine = importlib.import_module('src.moex_runtime.engine.run_registered_runtime_boundary')
    store = importlib.import_module('src.moex_runtime.state_store.file_backed_runtime_session_store')
    from src.moex_strategy_sdk.interfaces import LiveAdapterDecision

    state_path, log_path = tmp_path/'state.json', tmp_path/'trades.csv'
    store.save_runtime_state(state_path, dict(current_position=before, last_desired_position=before, last_trade_seq=1))
    store.append_trade_log_row(log_path, dict(trade_date='2026-09-04', seq=1, bar_end=NOW.isoformat(), action='OPEN_LONG',
        prev_pos=0., new_pos=before, price=99., reason_code='previous'))
    seen = []
    def decide(**kwargs):
        seen.append(dict(kwargs['inputs'].state))
        return LiveAdapterDecision('test', '1', 'Si', NOW, target, 'test', True,
                                   {'last_desired_position': target, 'adapter_checkpoint': 'restored'})
    resolved = SimpleNamespace(instrument_record={'instrument_id':'Si','timezone':'Europe/Moscow'},
        dataset_contract={'locator_ref':'data'}, environment_record={}, runtime_state_contract=SimpleNamespace(locator_ref='state'),
        runtime_trade_log_contract=SimpleNamespace(locator_ref='log'),
        runtime_feature_builder=lambda **_: pd.DataFrame([dict(end=pd.Timestamp(NOW), close=100.)]),
        runtime_signal_builder=lambda **_: (), runtime_live_decision_builder=decide,
        strategy_config=None, manifest=SimpleNamespace(version='1'))
    monkeypatch.setattr(engine, 'load_registered_runtime_boundary', lambda **_: resolved)
    paths={'data':tmp_path/'unused.csv','state':state_path,'log':log_path}
    monkeypatch.setattr(engine, 'resolve_external_pattern_artifact_path', lambda **kw: paths[kw['locator_ref']])
    def fail(*args, **kwargs):
        raise OSError('simulated checkpoint failure')
    monkeypatch.setattr(engine, 'save_runtime_state', fail)
    with pytest.raises(OSError):
        engine.run_registered_runtime_boundary(strategy_id='test', portfolio_id='p', environment_id='e')
    monkeypatch.setattr(engine, 'save_runtime_state', store.save_runtime_state)
    result = engine.run_registered_runtime_boundary(strategy_id='test', portfolio_id='p', environment_id='e')
    assert not result['position_changed']
    assert seen[-1]['current_position'] == target
    assert seen[-1]['last_desired_position'] == target
    assert seen[-1]['adapter_checkpoint'] == 'restored'
    with log_path.open(newline='') as handle:
        assert len(list(csv.DictReader(handle))) == 2
    assert store.load_runtime_state(state_path)['last_trade_seq'] == 2
    assert not store.transition_journal_path(state_path).exists()


def test_newer_flat_checkpoint_is_not_overridden_by_older_log():
    from src.moex_runtime.execution.runtime_position_transition import recover_position_state
    state = recover_position_state({'current_position': 0., 'last_trade_seq': 3}, {'seq':'2','new_pos':'1.0'})
    assert state['current_position'] == 0.


def test_recovery_of_old_log_without_journal_and_uncommitted_pending(tmp_path):
    from src.moex_runtime.state_store.file_backed_runtime_session_store import prepare_runtime_transition, recover_runtime_state
    checkpoint = {'current_position':1., 'last_trade_seq':1}
    committed = {'seq':'2', 'new_pos':'-1.0'}
    recovered = recover_runtime_state(tmp_path/'state.json', checkpoint, committed)
    assert recovered['current_position'] == recovered['last_desired_position'] == -1.
    assert recovered['last_trade_seq'] == 2
    prepare_runtime_transition(tmp_path/'state.json', {'seq':3}, {'current_position':0., 'last_trade_seq':3})
    # An event not committed to the log must not alter the recovered position.
    assert recover_runtime_state(tmp_path/'state.json', checkpoint, committed) == recovered


def isolated_definitions(relative, names, namespace):
    """Run exact definitions on Windows without unrelated POSIX pipeline imports.

    Integration coverage remains in the existing Linux snapshot/HTTP suites.
    """
    tree = ast.parse((ROOT / relative).read_text(encoding='utf-8'))
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
    future = ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *nodes], type_ignores=[])), relative, 'exec'), namespace)
    return namespace


@pytest.mark.parametrize('offset,status', [(1,None), (0,'FRESH'), (-1201,'STALE')])
def test_snapshot_rejects_future_clock(offset, status):
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    snapshot = {'identity': {'generated_at_utc': (NOW + timedelta(seconds=offset)).isoformat()}}
    ns = isolated_definitions('src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot.py',
        {'_aware','_iso','read_current_snapshot'}, dict(datetime=datetime, timezone=timezone, Path=Path,
        ChatAnalysisSnapshotError=RuntimeError, STALE_AFTER_SECONDS=1200, _data_root=lambda: Path('.'),
        current_snapshot_path=lambda _:Path('fixture'), _load_previous=lambda _:snapshot,
        apply_read_freshness=apply_read_freshness))
    if status is None:
        with pytest.raises(RuntimeError, match='future'):
            ns['read_current_snapshot'](now_fn=lambda:NOW)
    else:
        assert ns['read_current_snapshot'](now_fn=lambda:NOW)[0]['read_freshness']['status'] == status


@pytest.mark.parametrize('token,expected', [('café',False), ('test-token',True), ('invalid',False)])
def test_bearer_auth_handles_non_ascii(token, expected):
    ns = isolated_definitions('src/misc/rub_factual_snapshot_http_server.py', {'SnapshotRequestHandler'},
                              dict(BaseHTTPRequestHandler=BaseHTTPRequestHandler, hmac=hmac))
    handler = SimpleNamespace(headers={'Authorization':'Bearer '+token}, server=SimpleNamespace(api_token='test-token'))
    assert ns['SnapshotRequestHandler']._authorized(handler) is expected


@pytest.mark.parametrize('password,expected', [('пароль',True), ('неверно',False)])
def test_web_basic_auth_utf8(password, expected):
    from src.misc.moex_analyst_web_chat import AnalystRequestHandler
    credential = base64.b64encode(('пользователь:'+password).encode()).decode('ascii')
    handler = SimpleNamespace(headers={'Authorization':'Basic '+credential},
                              server=SimpleNamespace(web_user='пользователь', web_password='пароль'))
    assert AnalystRequestHandler._authorized(handler) is expected


def test_legacy_loop_warms_up_executes_next_bar_and_survives_reload(tmp_path, monkeypatch):
    from src.cli.loop_ema_5_12_realtime import process_closed_bar
    from src.strategy.realtime.ema_5_12 import session_state as ss
    from src.infra import trade_logger
    monkeypatch.setattr(ss, 'STATE_DIR', tmp_path)
    monkeypatch.setattr(trade_logger, 'SIGNALS_DIR', tmp_path)
    state = ss.SessionState(trade_date=NOW.date())
    trade_logger.ensure_ema_5_12_file(state.trade_date)
    for i in range(12):
        state, trade = process_closed_bar(bar=dict(end=NOW+timedelta(minutes=5*i), close=100.+i), state=state, now=NOW+timedelta(days=1))
        assert trade is None
    assert state.pending_target_pos == 1
    ss.save_session_state(state)
    state = ss.load_session_state(state.trade_date)
    bar = dict(end=NOW+timedelta(minutes=60), close=112.)
    state, trade = process_closed_bar(bar=bar, state=state, now=NOW+timedelta(days=1))
    assert trade.pos_before == 0 and trade.pos_after == state.pos == 1
    assert trade.bar_end_signal < trade.bar_end_exec
    trade_logger.append_trade_ema_5_12(state.trade_date, trade)
    state, duplicate = process_closed_bar(bar=bar, state=state, now=NOW+timedelta(days=1))
    assert duplicate is None and state.ema_bars_seen == 13
    state, future = process_closed_bar(bar=dict(end=NOW+timedelta(days=2), close=1.), state=state, now=NOW)
    assert future is None and state.ema_bars_seen == 13
    # A later reversal realizes the long's PnL and preserves the previous log row.
    state.pending_target_pos = -1
    state.pending_signal_bar_end = bar['end']
    state, trade = process_closed_bar(bar=dict(end=NOW+timedelta(minutes=65), close=110.), state=state, now=NOW+timedelta(days=1))
    assert trade.pos_before == 1 and trade.pos_after == state.pos == -1
    assert trade.pnl < -2.
    trade_logger.append_trade_ema_5_12(state.trade_date, trade)
    with trade_logger._file_path_ema_5_12(state.trade_date).open(newline='') as handle:
        rows = list(csv.DictReader(handle))
    assert [row['seq_no'] for row in rows] == ['1','2']


@pytest.mark.parametrize('module', ['src.cli.loop_futoi_5m','src.cli.loop_marketdata_5m','src.cli.loop_oi_5m',
    'src.cli.loop_orderbook_5m','src.cli.loop_trades_5m','src.misc.diag_orderbook_once','src.misc.online_signal_mr1','src.bot.cmd_bot'])
def test_legacy_entrypoint_imports(module, monkeypatch):
    import requests
    def no_network(*args, **kwargs):
        raise AssertionError('imports must not perform HTTP requests')
    monkeypatch.setattr(requests.sessions.Session, 'request', no_network)
    importlib.import_module(module)


@pytest.mark.parametrize('exists', [False, True])
def test_bot_status_is_sent(tmp_path, monkeypatch, exists):
    from src.bot import cmd_bot as bot
    marker = tmp_path/'marker'
    if exists:
        marker.touch()
    monkeypatch.setattr(bot, 'LOCK_FILE', marker)
    monkeypatch.setattr(bot, 'STOP_FILE', marker)
    monkeypatch.setattr(bot, 'load_dotenv', lambda:None)
    monkeypatch.setenv('BOT_TOKEN', 'fixture')
    monkeypatch.setenv('ADMIN_USER_ID', '1')
    monkeypatch.setattr(bot, 'load_offset', lambda:None)
    monkeypatch.setattr(bot, 'save_offset', lambda _:None)
    calls = []
    def get(*args, **kwargs):
        if calls:
            raise KeyboardInterrupt
        return SimpleNamespace(raise_for_status=lambda:None, json=lambda:{'result':[{
            'update_id':1, 'message':{'text':'/status','from':{'id':1},'chat':{'id':1}}}]})
    monkeypatch.setattr(bot.requests, 'get', get)
    monkeypatch.setattr(bot, 'send_message', lambda token,chat,text:calls.append(text))
    bot.main()
    label = 'есть' if exists else 'нет'
    assert calls == [f'ℹ️ Статус: loop lock: {label}; stop file: {label}']


@pytest.mark.parametrize('field', ['pnl_day','EMA_EDGE_DAY','date','metrics_date','gate_state'])
def test_gate_bad_ema_input_reports_validation_error(tmp_path, monkeypatch, field):
    import sys
    from src.research.ema import build_gate_conditioned_ema_day as gate
    rows = [dict(date='2026-09-04', pnl_day=1., EMA_EDGE_DAY=1.)]
    if field == 'date':
        rows *= 2
    elif field in ('pnl_day','EMA_EDGE_DAY'):
        rows[0][field] = 'bad'
    pd.DataFrame(rows).to_csv(tmp_path/'ema.csv', index=False)
    metrics_row = '2026-09-04,1,1\n'
    (tmp_path/'metrics.csv').write_text('date,trend_ratio,rel_range\n' + metrics_row*(2 if field=='metrics_date' else 1))
    if field == 'gate_state':
        monkeypatch.setattr(gate, 'build_yday_features', lambda frame, column:frame)
        monkeypatch.setattr(gate, 'compute_phase_transition_risk', lambda frame, config:float('nan'))
    (tmp_path/'config.json').write_text('{}')
    monkeypatch.setattr(sys, 'argv', ['gate','--ema-day-csv',str(tmp_path/'ema.csv'), '--day-metrics-csv',str(tmp_path/'metrics.csv'),
        '--config',str(tmp_path/'config.json'),'--out-joined-csv',str(tmp_path/'joined.csv'),'--out-agg-csv',str(tmp_path/'agg.csv')])
    with pytest.raises(SystemExit):
        gate.main()
