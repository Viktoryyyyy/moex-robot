from copy import deepcopy
from datetime import datetime, date, timedelta
import pytest
from moex_data import rub_trading_target_plan as target


def calendar_view():
    days = []
    for n in range(22):
        day = (date(2026, 9, 1) + timedelta(days=n)).isoformat()
        closed = day in {'2026-09-12', '2026-09-13'}
        mapped = '2026-09-21' if day in {'2026-09-19', '2026-09-20'} else day
        days.append({'civil_date': day, 'is_traded': 0 if closed else 1,
            'trading_date': None if closed else mapped})
    return {'status': 'READY', 'data': {'calendar_plan_usable': True,
        'coverage_start': '2026-09-01', 'days': days, 'manifest_sha256': 'a' * 64,
        'received_at': '2026-09-08T10:35:54+00:00'}}


@pytest.mark.parametrize('stamp,day,start', [
    ('2026-09-08T13:40:00+03:00', '2026-09-09', '2026-09-09T06:50:00+03:00'),
    ('2026-09-11T20:00:00+03:00', '2026-09-14', '2026-09-14T06:50:00+03:00'),
    ('2026-09-18T20:00:00+03:00', '2026-09-21', '2026-09-19T09:50:00+03:00'),
    ('2026-09-19T09:49:59+03:00', '2026-09-21', '2026-09-19T09:50:00+03:00'),
])
def test_next_not_started_candidate_uses_weekend_start(monkeypatch, stamp, day, start):
    monkeypatch.setattr(target.calendar, 'reconcile', lambda *_a, **_k: calendar_view())
    result = target.describe({}, now=datetime.fromisoformat(stamp))
    assert result['D1']['candidate_trading_date'] == day
    assert result['D1']['first_published_start'] == start
    assert result['D1']['status'] == 'CANDIDATE_ONLY'
    assert result['D1']['horizon_definition_accepted'] is False
    assert result['W1']['candidate_trading_date'] is None
    assert result['forecast_trading_targets_accepted'] is False
    assert result['session_completion_proven'] is False


@pytest.mark.parametrize('stamp', ['2026-09-19T09:50:00+03:00',
    '2026-09-20T12:00:00+03:00', '2026-09-21T06:00:00+03:00'])
def test_monday_is_already_started_and_later_unknown_plan_cannot_fill_gap(monkeypatch, stamp):
    monkeypatch.setattr(target.calendar, 'reconcile', lambda *_a, **_k: calendar_view())
    result = target.describe({}, now=datetime.fromisoformat(stamp))
    assert result['D1']['candidate_trading_date'] is None
    assert result['D1']['status'] == 'UNAVAILABLE'


def test_calendar_schedule_contradiction_cannot_skip_to_later_good_date(monkeypatch):
    view = calendar_view()
    row = next(row for row in view['data']['days'] if row['civil_date'] == '2026-09-09')
    row.update(is_traded=0, trading_date=None)
    monkeypatch.setattr(target.calendar, 'reconcile', lambda *_a, **_k: deepcopy(view))
    result = target.describe({}, now=datetime.fromisoformat('2026-09-08T13:40:00+03:00'))
    assert result['D1']['candidate_trading_date'] is None
    assert result['D1']['reason'] == 'calendar_and_schedule_coverage_or_rules_disagree'


def test_expired_calendar_and_pre_review_time_are_not_candidates(monkeypatch):
    monkeypatch.setattr(target.calendar, 'reconcile', lambda *_a, **_k: {'status': 'UNAVAILABLE'})
    assert target.describe({}, now=datetime.fromisoformat('2026-09-08T13:40:00+03:00'))['D1']['candidate_trading_date'] is None
    monkeypatch.setattr(target.calendar, 'reconcile', lambda *_a, **_k: calendar_view())
    result = target.describe({}, now=datetime.fromisoformat('2026-09-08T06:00:00+03:00'))
    assert result['D1']['candidate_trading_date'] is None


@pytest.mark.parametrize('component', [None, [], {'status': 'READY', 'data': [1]},
    {'status': 'READY', 'data': 'invalid'}])
def test_malformed_calendar_component_is_unavailable(component):
    result = target.describe(component, now=datetime.fromisoformat('2026-09-08T13:40:00+03:00'))
    assert result['D1']['candidate_trading_date'] is None
    assert result['forecast_trading_targets_accepted'] is False


def test_real_manifest_pipeline_retention_and_factual_release(tmp_path, monkeypatch):
    import json
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    from moex_data.rub_factual_release import describe
    now = datetime.fromisoformat('2026-09-08T13:40:00+03:00')
    rows = []
    for row in calendar_view()['data']['days']:
        mapped = row['trading_date'] if row['trading_date'] != row['civil_date'] else None
        rows.append([row['civil_date'], row['is_traded'], mapped,
            'W' if mapped else 'N', None])
    raw = json.dumps({'off_days': {'columns': target.calendar.COLUMNS, 'data': rows}}).encode()
    data = target.calendar.load(root=tmp_path, env={}, now_fn=lambda: now,
        fetch=lambda url, env: raw)
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    replay = runner.CalendarClockContext(mode='REPLAY', archived_component={'status': 'READY', 'data': data})
    assert replay.produce(now).data['manifest_sha256'] == data['manifest_sha256']
    snapshot = {'identity': {'generated_at_utc': now.isoformat()},
        'components': {'futures_calendar': {'status': 'READY', 'data': data}}}
    before = deepcopy(snapshot)
    view = apply_read_freshness(snapshot, now=now)
    assert snapshot == before
    assert view['trading_target_plan']['D1']['candidate_trading_date'] == '2026-09-09'
    release = describe(view)
    assert release['horizons']['D1']['planning_candidate']['candidate_trading_date'] == '2026-09-09'
    assert release['horizons']['D1']['target_trading_date'] is None
    assert release['horizons']['D1']['target_date_proven'] is False
    def fail(now): raise ValueError('calendar capture failed')
    retained = runner._component_payload('futures_calendar', fail, now=now, previous=snapshot)
    assert retained['status'] == 'UNAVAILABLE'
    assert retained['data']['calendar_plan_usable'] is False
    expired = apply_read_freshness(snapshot, now=now + timedelta(seconds=1201))
    assert expired['trading_target_plan']['D1']['candidate_trading_date'] is None
    assert describe(expired)['horizons']['D1']['planning_candidate']['candidate_trading_date'] is None


def test_live_calendar_boundary_extension_preserves_saturday_candidate(tmp_path):
    import json
    from urllib.parse import urlsplit, parse_qs
    now = datetime.fromisoformat('2026-09-19T09:49:59+03:00')
    calls = []
    mappings = {'2026-09-19': '2026-09-21', '2026-09-20': '2026-09-21',
        '2026-09-26': '2026-09-28', '2026-09-27': '2026-09-28',
        '2026-10-03': '2026-10-05', '2026-10-04': '2026-10-05'}
    def fetch(url, *, env):
        calls.append(url)
        query = parse_qs(urlsplit(url).query)
        start, end = date.fromisoformat(query['from'][0]), date.fromisoformat(query['till'][0])
        rows = []
        for offset in range((end - start).days + 1):
            civil = (start + timedelta(days=offset)).isoformat()
            traded = 0 if civil in {'2026-09-12', '2026-09-13'} else 1
            rows.append([civil, traded, mappings.get(civil), 'W' if civil in mappings else 'N', None])
        return json.dumps({'off_days': {'columns': target.calendar.COLUMNS, 'data': rows}}).encode()
    data = target.calendar.load(root=tmp_path, env={}, now_fn=lambda: now, fetch=fetch)
    assert len(calls) == 2
    assert data['coverage_end'] == '2026-10-05'
    result = target.describe({'status': 'READY', 'data': data}, now=now)
    assert result['D1']['candidate_trading_date'] == '2026-09-21'
    assert result['D1']['first_published_start'] == '2026-09-19T09:50:00+03:00'
    assert result['forecast_trading_targets_accepted'] is False
