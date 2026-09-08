"""Release integration must replay macro evidence even outside the API reader."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import runpy

import pytest

from moex_data import rub_factual_release as release
from moex_data import rub_macro_evidence_inventory as inventory
from moex_research.external_data import cbr_rates_factual as cbr_rates

NOW = datetime(2026, 9, 8, 7, tzinfo=timezone.utc)
MACRO_KEYS = {'cbr_rates_verified', 'rosstat_cpi'}


@pytest.fixture
def snapshot(tmp_path):
    # Reuse source fixtures without test-module import-path assumptions. No
    # source reconciliation is mocked: HTML, receipt and manifest hashes replay.
    folder = Path(__file__).parent
    cbr_helper = runpy.run_path(str(folder / 'test_cbr_rates_factual.py'))
    rosstat_helper = runpy.run_path(str(folder / 'test_rosstat_cpi_factual.py'))
    cbr_root, rosstat_root = tmp_path / 'cbr', tmp_path / 'rosstat'
    rosstat_root.mkdir()
    cbr_data = cbr_rates.load(root=cbr_root, now_fn=lambda: NOW,
        fetch=lambda url: cbr_helper['raw']('ruonia' if 'ruonia' in url else 'key_rate'))
    return {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {
        'cbr_rates_verified': {'status': 'READY', 'data': cbr_data},
        'rosstat_cpi': rosstat_helper['component'](rosstat_root)}}


def factors(result):
    return {fact['factor'] for fact in result['facts']}


def assert_projection(result, source, now):
    assert result['macro_evidence_inventory'] == inventory.describe(source, now=now)
    assert all(result['macro_evidence_inventory'][key] is False for key in inventory.DENIED)
    assert result['status'] == 'INCOMPLETE'
    assert result['model_validated'] is False
    assert result['training_authorized'] is False
    assert result['broker_execution'] is False


def test_direct_describe_replays_fresh_macro_without_mutating_input(snapshot):
    before = deepcopy(snapshot)
    result = release.describe(snapshot)
    assert MACRO_KEYS <= factors(result)
    assert_projection(result, snapshot, NOW)
    assert snapshot == before


def test_direct_describe_expired_macro_not_in_facts_or_inventory(snapshot):
    read = NOW + timedelta(seconds=1201)
    snapshot['live_read_freshness'] = {'read_at_utc': read.isoformat()}
    before = deepcopy(snapshot)
    result = release.describe(snapshot)
    assert not factors(result).intersection(MACRO_KEYS)
    assert result['macro_evidence_inventory']['facts'] == []
    assert result['macro_evidence_inventory']['scheduled_events'] == []
    assert_projection(result, snapshot, read)
    assert snapshot == before


@pytest.mark.parametrize('component', sorted(MACRO_KEYS))
@pytest.mark.parametrize('defect', ['raw', 'manifest', 'retained', 'refresh_error'])
def test_direct_macro_replay_failure_removes_only_failed_source(snapshot, component, defect):
    block = snapshot['components'][component]
    data = block['data']
    if defect == 'retained':
        block['status'] = 'RETAINED_PREVIOUS'
    elif defect == 'refresh_error':
        block['refresh_error'] = 'latest request failed'
    else:
        if component == 'cbr_rates_verified':
            path = Path(data['manifest_path'])
            if defect == 'raw': path = path.parent / (data['receipts']['ruonia']['raw_sha256'] + '.html')
        else:
            path = Path(data['document_manifest_path'])
            if defect == 'raw': path = path.parent / (json.loads(path.read_bytes())['raw_sha256'] + '.html')
        path.write_bytes(path.read_bytes() + b'changed')
    before = deepcopy(snapshot)
    result = release.describe(snapshot)
    assert component not in factors(result)
    assert MACRO_KEYS - {component} <= factors(result)
    assert_projection(result, snapshot, NOW)
    assert snapshot == before


@pytest.mark.parametrize('reference', [None, [], {}, {'read_at_utc': None},
    {'read_at_utc': '2026-09-08T07:00:00'}, {'read_at_utc': 'invalid'}])
def test_present_invalid_read_reference_never_falls_back_to_fresh_generation(snapshot, reference):
    snapshot['live_read_freshness'] = reference
    result = release.describe(snapshot)
    assert not factors(result).intersection(MACRO_KEYS)
    assert result['macro_evidence_inventory']['facts'] == []
    assert result['macro_evidence_inventory']['reference_time_valid'] is False
    assert result['status'] == 'INCOMPLETE'


@pytest.mark.parametrize('component', sorted(MACRO_KEYS))
@pytest.mark.parametrize('value', [None, [], {'status': 'READY', 'data': []},
    {'status': 'READY', 'data': {'consumer_factual_use_allowed': True}}])
def test_malformed_macro_component_cannot_appear_as_accepted_fact(snapshot, component, value):
    snapshot['components'][component] = deepcopy(value)
    result = release.describe(snapshot)
    assert component not in factors(result)
    assert_projection(result, snapshot, NOW)


def test_cached_forged_inventory_is_not_reused(snapshot):
    forged = {'untrusted_old_projection': True, 'all_accepted': True}
    snapshot['macro_evidence_inventory'] = deepcopy(forged)
    snapshot['factual_release'] = {'macro_evidence_inventory': deepcopy(forged)}
    snapshot['live_read_freshness'] = {'read_at_utc': (NOW + timedelta(seconds=1201)).isoformat()}
    result = release.describe(snapshot)
    assert not factors(result).intersection(MACRO_KEYS)
    assert 'untrusted_old_projection' not in json.dumps(result['macro_evidence_inventory'])
    assert result['macro_evidence_inventory'] != forged


def test_frozen_export_is_deterministic_and_uses_same_macro_decision(snapshot, tmp_path):
    original = deepcopy(snapshot)
    first = release.export(snapshot, now=NOW, code_revision='a' * 40, output=tmp_path / 'first')
    second = release.export(snapshot, now=NOW, code_revision='a' * 40, output=tmp_path / 'second')
    assert first.name == second.name
    assert (first / 'release.json').read_bytes() == (second / 'release.json').read_bytes()
    exported = json.loads((first / 'release.json').read_bytes())
    direct = release.describe(snapshot)
    assert exported['macro_evidence_inventory'] == direct['macro_evidence_inventory']
    assert factors(exported) == factors(direct)
    assert json.loads((first / 'input_snapshot.json').read_bytes()) == original
    assert snapshot == original


def test_current_reader_rebuilds_cached_inventory_after_macro_expiry(snapshot, tmp_path, monkeypatch):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner

    snapshot['schema_version'] = runner.SCHEMA_VERSION
    snapshot['identity']['project'] = runner.PROJECT
    cached = release.describe(snapshot)
    assert MACRO_KEYS <= factors(cached)
    assert cached['macro_evidence_inventory']['facts']
    snapshot['factual_release'] = cached
    path = runner.current_snapshot_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot), encoding='utf-8')
    original = path.read_bytes()
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))

    # No fast generation exists: exercise the real HTTP snapshot read pipeline,
    # including its freshness reconciliation, without starting a refresh.
    read = NOW + timedelta(seconds=1201)
    result, read_path = runner.read_current_snapshot(now_fn=lambda: read)
    rebuilt = result['factual_release']
    assert read_path == path
    assert not factors(rebuilt).intersection(MACRO_KEYS)
    assert rebuilt['macro_evidence_inventory']['facts'] == []
    assert rebuilt['macro_evidence_inventory']['scheduled_events'] == []
    assert rebuilt['macro_evidence_inventory']['as_of_utc'] == read.isoformat()
    assert rebuilt['macro_evidence_inventory'] != cached['macro_evidence_inventory']
    assert_projection(rebuilt, result, read)
    assert path.read_bytes() == original
