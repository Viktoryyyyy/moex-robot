from __future__ import annotations
import json
import shutil
import tempfile
from pathlib import Path
from src.realtime.gate_preflight import preflight

def _must_fail(msg, fn):
    try:
        fn()
    except Exception:
        return
    raise AssertionError('EXPECTED_FAIL: ' + msg)

def _write(p, text):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding='utf-8')

def run():
    base = Path('data/gate')
    gate = base / 'phase_transition_risk.json'
    stamp = base / 'phase_transition_gate_daily.last_success.json'
    hist = base / 'rel_range_history.csv'

    if not gate.exists():
        raise RuntimeError('missing gate json')
    if not stamp.exists():
        raise RuntimeError('missing stamp')
    if not hist.exists():
        raise RuntimeError('missing history')

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        tmp_gate = td / 'phase_transition_risk.json'
        shutil.copy2(gate, tmp_gate)

        tmp_gate.unlink()
        _must_fail('deleted gate json', lambda: preflight(path=str(tmp_gate)))

        shutil.copy2(gate, tmp_gate)

        _write(tmp_gate, '{ not_json ')
        _must_fail('corrupt gate json', lambda: preflight(path=str(tmp_gate)))

        shutil.copy2(gate, tmp_gate)
        obj = json.loads(tmp_gate.read_text(encoding='utf-8'))

        obj['phase_transition_risk'] = 2
        _write(tmp_gate, json.dumps(obj))
        _must_fail('risk=2', lambda: preflight(path=str(tmp_gate)))

        obj = json.loads(gate.read_text(encoding='utf-8'))
        obj['asof_date'] = '1999-01-01'
        _write(tmp_gate, json.dumps(obj))
        _must_fail('stale asof_date', lambda: preflight(path=str(tmp_gate)))

    print('TESTS_OK')

run()
