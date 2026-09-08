from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import pytest

from moex_data import rub_futures_calendar as m

NOW = datetime(2026, 9, 8, 10, tzinfo=timezone.utc)


def payload(start=None, end=None):
    start, end = (start, end) if start else m.interval(NOW)
    rows = []
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        traded, session, reason, updated = 1, None, 'N', None
        if day.isoformat() in ('2026-09-12', '2026-09-13'):
            traded = 0  # Real source marks these with N, not W.
        if day.isoformat() in ('2026-09-19', '2026-09-20'):
            session, reason, updated = '2026-09-21', 'W', '2025-12-19 14:16:30'
        rows.append([day.isoformat(), traded, session, reason, updated])
    return {'off_days': {'columns': m.COLUMNS, 'data': rows}}


def encoded(value):
    return json.dumps(value).encode()


def parse(value):
    start, end = m.interval(NOW)
    return m.parse(encoded(value), start=start, end=end, received_at=NOW)


def load(tmp_path):
    return m.load(root=tmp_path, env={}, now_fn=lambda: NOW, fetch=lambda url, env: encoded(payload()))


def test_source_flags_and_weekend_mapping_without_completion():
    days = {x['civil_date']: x for x in parse(payload())}
    assert days['2026-09-12']['is_traded'] == 0
    assert days['2026-09-12']['reason'] == 'N'
    assert days['2026-09-12']['trading_date'] is None
    assert days['2026-09-19']['trading_date'] == '2026-09-21'
    assert days['2026-09-19']['source_update_time'] == '2025-12-19 14:16:30'
    assert days['2026-09-21']['trading_date'] == '2026-09-21'


@pytest.mark.parametrize('value', [None, True, False, '1', 1.0, -1, 2])
def test_strict_integer_traded(value):
    item = payload()
    item['off_days']['data'][0][1] = value
    with pytest.raises(ValueError): parse(item)


@pytest.mark.parametrize('reason,traded', [('X', 1), ('H', 1), ('T', 0), (None, 1), ([], 1)])
def test_unknown_or_conflicting_reason_rejected(reason, traded):
    item = payload()
    item['off_days']['data'][0][1] = traded
    item['off_days']['data'][0][3] = reason
    with pytest.raises(ValueError): parse(item)


@pytest.mark.parametrize('reason,traded', [('H', 0), ('T', 1), ('N', 0), ('N', 1)])
def test_known_normal_reason_preserves_flag(reason, traded):
    item = payload()
    item['off_days']['data'][0][1] = traded
    item['off_days']['data'][0][3] = reason
    row = parse(item)[0]
    assert row['is_traded'] == traded
    assert row['trading_date'] == (row['civil_date'] if traded else None)


@pytest.mark.parametrize('defect', ['missing', 'duplicate', 'outside', 'compactdate', 'width', 'rowdict', 'columns', 'rowsdict', 'blocklist', 'futureupdate', 'badupdate'])
def test_schema_dates_and_coverage_fail_closed(defect):
    item = payload()
    rows = item['off_days']['data']
    if defect == 'missing': rows.pop()
    elif defect == 'duplicate': rows.append(rows[0])
    elif defect == 'outside': rows[0][0] = '2026-08-31'
    elif defect == 'compactdate': rows[0][0] = '20260901'
    elif defect == 'width': rows[0].pop()
    elif defect == 'rowdict': rows[0] = {}
    elif defect == 'columns': item['off_days']['columns'] = list(reversed(m.COLUMNS))
    elif defect == 'rowsdict': item['off_days']['data'] = {}
    elif defect == 'blocklist': item['off_days'] = []
    elif defect == 'futureupdate': rows[0][4] = '2026-09-09 10:00:00'
    else: rows[0][4] = '2026-09-08T10:00:00'
    with pytest.raises(ValueError): parse(item)


@pytest.mark.parametrize('defect', ['null', 'backward', 'outside', 'normalmapping', 'nottraded', 'destclosed', 'destweekend'])
def test_mapping_validation(defect):
    item = payload()
    rows = {row[0]: row for row in item['off_days']['data']}
    row = rows['2026-09-19']
    if defect == 'null': row[2] = None
    elif defect == 'backward': row[2] = '2026-09-18'
    elif defect == 'outside': row[2] = '2026-09-23'
    elif defect == 'normalmapping': row[3] = 'N'
    elif defect == 'nottraded': row[1] = 0
    elif defect == 'destclosed': rows['2026-09-21'][1] = 0
    else: row[2] = '2026-09-20'
    with pytest.raises(ValueError): parse(item)


def test_load_replay_no_authority(tmp_path):
    data = load(tmp_path)
    assert data['scope'] == 'PUBLISHED_CALENDAR_ONLY'
    assert data['calendar_plan_usable'] is True
    assert data['source_publication_time'] is None
    assert all(data[k] is False for k in m.DENIED)
    assert m.reconcile({'status': 'READY', 'data': data}, now=NOW)['status'] == 'READY'
    assert len(list(Path(data['manifest_path']).parent.glob('*.json'))) == 2


@pytest.mark.parametrize('seconds,expected', [(1200, 'READY'), (1201, 'UNAVAILABLE'), (-1, 'UNAVAILABLE')])
def test_freshness_and_causality(tmp_path, seconds, expected):
    assert m.reconcile({'status': 'READY', 'data': load(tmp_path)}, now=NOW + timedelta(seconds=seconds))['status'] == expected


@pytest.mark.parametrize('defect', ['manifest', 'raw', 'fact', 'authority', 'url', 'receipt', 'refresh', 'priorfailure'])
def test_tampering_and_no_promotion(tmp_path, defect):
    data = load(tmp_path)
    component = {'status': 'READY', 'data': data}
    if defect in ('manifest', 'raw'):
        path = Path(data['manifest_path'])
        if defect == 'raw': path = path.parent / (data['raw_sha256'] + '.json')
        path.write_bytes(path.read_bytes() + b'changed')
    elif defect == 'fact': data['days'][0]['is_traded'] = 0
    elif defect == 'authority': data['session_completion_proven'] = True
    elif defect == 'url': data['source_url'] = 'https://evil.example/'
    elif defect == 'receipt': data['requested_at'] = (NOW + timedelta(seconds=1)).isoformat()
    elif defect == 'refresh': component['refresh_error'] = 'failed'
    else: component['status'] = 'UNAVAILABLE'
    result = m.reconcile(component, now=NOW)
    assert result['status'] == 'UNAVAILABLE'
    assert result['data']['calendar_plan_usable'] is False
    assert all(result['data'][k] is False for k in m.DENIED)
    assert m.reconcile(result, now=NOW)['status'] == 'UNAVAILABLE'


def test_current_year_capping():
    assert m.interval(datetime(2026, 1, 2, tzinfo=timezone.utc))[0] == date(2026, 1, 1)
    assert m.interval(datetime(2026, 12, 30, tzinfo=timezone.utc))[1] == date(2026, 12, 31)


def test_cross_midnight_rejected(tmp_path):
    clock = iter([NOW, NOW + timedelta(days=1)])
    with pytest.raises(ValueError):
        m.load(root=tmp_path, env={}, now_fn=lambda: next(clock), fetch=lambda url, env: encoded(payload()))


def test_json_duplicate_key_rejected():
    start, end = m.interval(NOW)
    with pytest.raises(ValueError):
        m.parse(b'{"off_days":{},"off_days":{}}', start=start, end=end, received_at=NOW)


def test_apply_no_other_component_changes(tmp_path):
    snapshot = {'components': {'futures_calendar': {'status': 'READY', 'data': load(tmp_path)}, 'cbr_macro': {'data': {'x': 1}}}}
    before = deepcopy(snapshot['components']['cbr_macro'])
    m.apply(snapshot, now=NOW + timedelta(seconds=1201))
    assert snapshot['components']['futures_calendar']['status'] == 'UNAVAILABLE'
    assert snapshot['components']['cbr_macro'] == before


def test_transport_preserves_tls_and_disables_redirects(monkeypatch):
    import requests
    captured = {}
    class Response:
        status_code = 200
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def iter_content(self, chunk_size): yield encoded(payload())
    def get(url, **kwargs):
        captured.update(kwargs)
        response = Response()
        response.url = url
        return response
    monkeypatch.setattr(requests, 'get', get)
    m._fetch(m.source_url(*m.interval(NOW)), env={'MOEX_API_KEY': 'test-only', 'REQUESTS_CA_BUNDLE': '/configured/public-ca.pem'})
    assert captured['verify'] == '/configured/public-ca.pem'
    assert captured['allow_redirects'] is False
    with pytest.raises(ValueError): m._fetch('https://evil.example/', env={'MOEX_API_KEY': 'test-only'})
