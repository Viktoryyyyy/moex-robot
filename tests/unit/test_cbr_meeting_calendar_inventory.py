"""Calendar wiring preserves scheduled-only scope and immutable source replay."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import runpy

import pytest

from moex_data import rub_macro_evidence_inventory as inventory
from moex_research.external_data import cbr_meeting_calendar as calendar
from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner

NOW = datetime(2026, 9, 8, 7, tzinfo=timezone.utc)
FAMILY = 'rates.cbr_key_rate_calendar'


@pytest.fixture
def snapshot(tmp_path):
    helpers = runpy.run_path(str(Path(__file__).with_name('test_cbr_meeting_calendar.py')))
    data = calendar.load(root=tmp_path / 'cbr', now_fn=lambda: NOW,
        fetch=lambda url: helpers['raw']())
    rosstat = runpy.run_path(str(Path(__file__).with_name('test_rosstat_cpi_factual.py')))
    rosstat_root = tmp_path / 'rosstat'
    rosstat_root.mkdir()
    return {'schema_version': runner.SCHEMA_VERSION,
        'identity': {'project': runner.PROJECT, 'generated_at_utc': NOW.isoformat()},
        'components': {'cbr_meeting_calendar': {'status': 'READY', 'data': data},
            'rosstat_cpi': rosstat['component'](rosstat_root)}}


def cbr_events(value):
    return [event for event in value['scheduled_events'] if event.get('event_family') == FAMILY]


def assert_no_grants(value):
    assert value['status'] == 'INCOMPLETE'
    assert all(value[key] is False for key in inventory.DENIED)
    assert not any(fact.get('component') == 'cbr_meeting_calendar' for fact in value['facts'])
    assert all(row['full_requirement_accepted'] is False for row in value['requirements_coverage'])


def test_default_producer_uses_calendar_loader(monkeypatch, tmp_path, snapshot):
    calls = []
    data = snapshot['components']['cbr_meeting_calendar']['data']
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    def load(**kwargs):
        calls.append(kwargs)
        return deepcopy(data)
    monkeypatch.setattr(calendar, 'load', load)
    produced = runner.default_producers()['cbr_meeting_calendar'](NOW)
    assert calls == [{'root': tmp_path}]
    assert produced.data == data
    assert produced.data_as_of == data['received_at']


def test_failed_canonical_producer_revokes_previous_schedule(snapshot):
    before = deepcopy(snapshot)
    def fail(now):
        raise OSError('source unreachable')
    result = runner._component_payload('cbr_meeting_calendar', fail, now=NOW, previous=snapshot)
    assert result['status'] == 'UNAVAILABLE'
    assert result['data']['calendar_schedule_usable'] is False
    assert all(result['data'][key] is False for key in calendar.DENIED)
    assert snapshot == before


def test_inventory_projects_only_upcoming_schedule_and_preserves_unknowns(snapshot):
    before = deepcopy(snapshot)
    value = inventory.describe(snapshot, now=NOW)
    events = cbr_events(value)
    original = snapshot['components']['cbr_meeting_calendar']['data']
    assert {e['event_id'] for e in events} == {e['event_id'] for e in original['upcoming_events']}
    assert events
    for event in events:
        assert event['event_status'] == 'SCHEDULED'
        assert event['scheduled_time'] is None
        assert event['actual_event_time'] is event['source_publication_time'] is None
        assert event['planned_times_kind'] == 'TENTATIVE_SOURCE_SCHEDULE'
        assert event['consensus'] is event['surprise'] is None
        assert event['event_occurred_proven'] is event['full_calendar_accepted'] is False
        assert event['system_available_at'] == original['received_at']
        assert event['raw_sha256'] == original['raw_sha256']
        assert event['manifest_sha256'] == original['manifest_sha256']
    assert any(e.get('event_family') == 'rosstat_weekly_cpi' for e in value['scheduled_events'])
    assert 'accepted_cbr_decision_release_calendar' not in value['missing_evidence']
    assert 'accepted_global_macro_calendar' in value['missing_evidence']
    assert_no_grants(value)
    assert snapshot == before


def test_requirements_coverage_tracks_calendar_presence_without_acceptance(snapshot):
    value = inventory.describe(snapshot, now=NOW)
    requirement = next(r for r in value['requirements_policy']['requirements'] if r['event_family'] == FAMILY)
    coverage = next(r for r in value['requirements_coverage'] if r['requirement_id'] == requirement['requirement_id'])
    assert coverage['current_evidence_present'] is True
    assert coverage['admitted_fact_ids'] == []
    assert set(coverage['scheduled_event_ids']) == {e['event_id'] for e in cbr_events(value)}
    assert coverage['full_requirement_accepted'] is False
    assert_no_grants(value)


@pytest.mark.parametrize('defect', ['expired', 'raw', 'manifest', 'missing', 'retained', 'refresh', 'invented_event'])
def test_failed_calendar_disappears_without_removing_independent_rosstat(snapshot, defect):
    data = snapshot['components']['cbr_meeting_calendar']['data']
    manifest = Path(data['manifest_path'])
    if defect == 'expired':
        # Re-freeze the calendar at an older receipt; keep independent Rosstat fresh.
        helpers = runpy.run_path(str(Path(__file__).with_name('test_cbr_meeting_calendar.py')))
        stale = calendar.load(root=manifest.parent / 'older',
            now_fn=lambda: NOW - timedelta(seconds=1201), fetch=lambda url: helpers['raw']())
        snapshot['components']['cbr_meeting_calendar']['data'] = stale
    elif defect == 'raw': manifest.with_name(data['raw_sha256'] + '.html').write_bytes(b'changed')
    elif defect == 'manifest': manifest.write_bytes(b'{}')
    elif defect == 'missing': manifest.unlink()
    elif defect == 'retained': snapshot['components']['cbr_meeting_calendar']['status'] = 'RETAINED_PREVIOUS'
    elif defect == 'refresh': snapshot['components']['cbr_meeting_calendar']['refresh_error'] = 'failed'
    elif defect == 'invented_event': data['events'][0]['actual_event_time'] = NOW.isoformat()
    before = deepcopy(snapshot)
    value = inventory.describe(snapshot, now=NOW)
    assert cbr_events(value) == []
    assert any(e.get('event_family') == 'rosstat_weekly_cpi' for e in value['scheduled_events'])
    assert any(f['component'] == 'rosstat_cpi' for f in value['facts'])
    assert 'accepted_cbr_decision_release_calendar' in value['missing_evidence']
    requirement = next(r for r in value['requirements_policy']['requirements'] if r['event_family'] == FAMILY)
    coverage = next(r for r in value['requirements_coverage'] if r['requirement_id'] == requirement['requirement_id'])
    assert coverage['current_evidence_present'] is False
    assert coverage['scheduled_event_ids'] == []
    assert_no_grants(value)
    assert snapshot == before


def test_forged_derived_upcoming_events_are_recomputed_without_mutating_snapshot(snapshot):
    data = snapshot['components']['cbr_meeting_calendar']['data']
    expected_ids = {event['event_id'] for event in data['upcoming_events']}
    data['upcoming_events'][0]['actual_event_time'] = NOW.isoformat()
    data['upcoming_events'].append({'event_id': 'forged', 'actual_event_time': NOW.isoformat()})
    before = deepcopy(snapshot)
    view = calendar.reconcile(snapshot['components']['cbr_meeting_calendar'], now=NOW)
    assert view['status'] == 'READY'
    assert view['data']['calendar_schedule_usable'] is True
    assert {event['event_id'] for event in view['data']['upcoming_events']} == expected_ids
    assert all(event['actual_event_time'] is None for event in view['data']['upcoming_events'])
    value = inventory.describe(snapshot, now=NOW)
    assert {event['event_id'] for event in cbr_events(value)} == expected_ids
    assert all(event['actual_event_time'] is None for event in cbr_events(value))
    assert 'forged' not in json.dumps(value)
    assert_no_grants(value)
    assert snapshot == before


@pytest.mark.parametrize('expired', [False, True])
def test_canonical_reader_rebuilds_fresh_projection_without_reusing_forged_cache(monkeypatch, tmp_path, snapshot, expired):
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    snapshot['factual_release'] = {'macro_evidence_inventory': {'scheduled_events': [{'event_id': 'forged'}]}}
    path = runner.current_snapshot_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(snapshot).encode()
    path.write_bytes(raw)
    now = NOW + timedelta(seconds=1201) if expired else NOW
    read, returned = runner.read_current_snapshot(now_fn=lambda: now)
    projection = read['factual_release']['macro_evidence_inventory']
    assert returned == path
    assert bool(cbr_events(projection)) is (not expired)
    assert 'forged' not in json.dumps(projection)
    assert read['components']['cbr_meeting_calendar']['data']['calendar_schedule_usable'] is (not expired)
    assert_no_grants(projection)
    assert path.read_bytes() == raw
