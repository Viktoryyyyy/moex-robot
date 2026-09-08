"""Actual frozen source replay through current reader and factual projections."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import runpy

import pytest

from moex_data import rub_factual_release as release
from moex_data.rub_snapshot_read_freshness import apply_read_freshness

KEYS = {'rosstat_monthly_cpi', 'cbr_liquidity_verified'}


@pytest.fixture
def frozen(tmp_path):
    folder = Path(__file__).parent
    liquidity = runpy.run_path(str(folder / 'test_cbr_liquidity_factual.py'))
    monthly = runpy.run_path(str(folder / 'test_rosstat_monthly_cpi.py'))
    roots = [tmp_path / 'liquidity', tmp_path / 'monthly']
    for root in roots:
        root.mkdir()
    now = max(liquidity['NOW'], monthly['NOW'])
    snapshot = {'identity': {'generated_at_utc': now.isoformat()}, 'components': {
        'cbr_liquidity_verified': liquidity['component'](roots[0]),
        'rosstat_monthly_cpi': monthly['component'](roots[1], now=now)}}
    return snapshot, now


def projected(result):
    return [row for row in result['macro_evidence_inventory']['facts'] if row['component'] in KEYS]


def test_both_limited_sources_remain_usable_without_publication_hour(frozen):
    snapshot, now = frozen
    original = deepcopy(snapshot)
    result = release.describe(snapshot)
    rows = projected(result)
    assert len(rows) == 5
    assert {r['component'] for r in rows} == KEYS
    assert KEYS <= {r['factor'] for r in result['facts']}
    for row in rows:
        detail = row['uncertainty']
        assert detail['usable_as_dated_context'] is True
        assert detail['temporal_metadata']['source_publication_time'] is None
        assert detail['numeric_error_bounds'] is detail['probabilities'] is None
        assert detail['forecast_use_allowed'] is detail['historical_pit_acceptance'] is detail['action_authority'] is False
    monthly = next(r for r in rows if r['component'] == 'rosstat_monthly_cpi')
    assert monthly['uncertainty']['temporal_metadata']['observation_month'] == monthly['observation_month']
    assert monthly['uncertainty']['temporal_metadata']['source_publication_date'] is None
    for row in rows:
        if row['component'] == 'cbr_liquidity_verified':
            assert row['uncertainty']['revision_kind'] == 'latest_revised'
            assert row['uncertainty']['rounding']['reported_granularity'] == '0.1'
    assert result['status'] == 'INCOMPLETE'
    assert snapshot == original


@pytest.mark.parametrize('key', sorted(KEYS))
def test_raw_tamper_excludes_only_failed_source_from_reader_and_release(frozen, key):
    snapshot, now = frozen
    data = snapshot['components'][key]['data']
    manifest = Path(data['manifest_path'] if key == 'cbr_liquidity_verified' else data['document_manifest_path'])
    raw = manifest.parent / (data['raw_sha256'] + '.html')
    raw.write_bytes(raw.read_bytes() + b'changed')
    checked = apply_read_freshness(snapshot, now=now)
    assert checked['components'][key]['status'] == 'UNAVAILABLE'
    result = release.describe(checked)
    assert key not in {r['factor'] for r in result['facts']}
    assert {r['component'] for r in projected(result)} == KEYS - {key}


def test_expired_current_file_rebuilds_cached_release_without_mutating_disk(frozen, tmp_path, monkeypatch):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
    snapshot, now = frozen
    snapshot['schema_version'] = runner.SCHEMA_VERSION
    snapshot['identity']['project'] = runner.PROJECT
    snapshot['factual_release'] = release.describe(snapshot)
    assert len(projected(snapshot['factual_release'])) == 5
    path = runner.current_snapshot_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot), encoding='utf-8')
    before = path.read_bytes()
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    result, _ = runner.read_current_snapshot(now_fn=lambda: now + timedelta(seconds=1201))
    assert all(result['components'][key]['status'] == 'UNAVAILABLE' for key in KEYS)
    assert projected(result['factual_release']) == []
    assert not KEYS.intersection(r['factor'] for r in result['factual_release']['facts'])
    assert path.read_bytes() == before


def test_frozen_export_preserves_limited_macro_decision(frozen, tmp_path):
    snapshot, now = frozen
    first = release.export(snapshot, now=now, code_revision='a' * 40, output=tmp_path / 'one')
    second = release.export(snapshot, now=now, code_revision='a' * 40, output=tmp_path / 'two')
    assert (first / 'release.json').read_bytes() == (second / 'release.json').read_bytes()
    assert projected(json.loads((first / 'release.json').read_bytes())) == projected(release.describe(snapshot))
