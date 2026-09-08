"""Offline acceptance profile: frozen monthly CPI and four liquidity metrics."""
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
import json
from pathlib import Path
import runpy

import pytest

from moex_data import rub_factual_release as release
from moex_data import rub_factual_release_acceptance as acceptance

REVISION = 'a' * 40
FACTORS = {'rosstat_monthly_cpi', 'cbr_liquidity_verified'}


@pytest.fixture
def frozen(tmp_path):
    folder = Path(__file__).resolve().parents[1] / 'unit'
    liquidity = runpy.run_path(str(folder / 'test_cbr_liquidity_factual.py'))
    monthly = runpy.run_path(str(folder / 'test_rosstat_monthly_cpi.py'))
    now = max(liquidity['NOW'], monthly['NOW'])
    snapshot = {'identity': {'generated_at_utc': now.isoformat()}, 'components': {
        'cbr_liquidity_verified': liquidity['component'](tmp_path / 'source-liquidity'),
        'rosstat_monthly_cpi': monthly['component'](tmp_path / 'source-monthly', now=now)}}
    return snapshot, now


def factors(result):
    return {row['factor'] for row in result['facts']}


def assert_no_authority(result):
    assert result['status'] == 'INCOMPLETE'
    for key in ('model_validated', 'training_authorized', 'broker_execution',
                'historical_dataset_accepted', 'session_completion_proven'):
        assert result[key] is False
    for horizon in ('D1', 'W1'):
        item = result['horizons'][horizon]
        assert item['forecast_generated'] is item['target_date_proven'] is False
        assert item['model_probability'] is item['target_trading_date'] is None


def test_factual_release_end_to_end_acceptance(frozen, tmp_path):
    snapshot, now = frozen
    before = deepcopy(snapshot)
    direct = release.describe(snapshot)
    built = release.build(snapshot, now=now, code_revision=REVISION)
    assert factors(direct) == factors(built) == FACTORS
    assert direct['macro_evidence_inventory'] == built['macro_evidence_inventory']
    assert_no_authority(built)
    facts = built['macro_evidence_inventory']['facts']
    assert len(facts) == 5
    for fact in facts:
        uncertainty = fact['uncertainty']
        assert uncertainty['usable_as_dated_context'] is True
        assert uncertainty['numeric_error_bounds'] is uncertainty['probabilities'] is None
        assert uncertainty['temporal_metadata']['source_publication_time'] is None
        assert uncertainty['forecast_use_allowed'] is uncertainty['historical_pit_acceptance'] is uncertainty['action_authority'] is False
    first = release.export(snapshot, now=now, code_revision=REVISION, output=tmp_path / 'first')
    second = release.export(snapshot, now=now, code_revision=REVISION, output=tmp_path / 'second')
    assert first.name == second.name
    raw = (first / 'release.json').read_bytes()
    assert raw == (second / 'release.json').read_bytes()
    manifest = json.loads((first / 'manifest.json').read_bytes())
    assert sha256(raw).hexdigest() == manifest['release_sha256'] == first.name
    assert manifest['input_snapshot_sha256'] == built['input_snapshot_sha256']
    assert json.loads(raw) == built
    archived = json.loads((first / 'input_snapshot.json').read_bytes())
    assert archived == snapshot
    assert release.build(archived, now=now, code_revision=REVISION) == built

    report = acceptance.run(snapshot, now=now, code_revision=REVISION, output=tmp_path / 'acceptance')
    assert report['status'] == 'PASS'
    assert report['checks'] and all(check['status'] == 'PASS' for check in report['checks'])
    assert report['failures'] == []
    assert report['release_status'] == 'INCOMPLETE'
    assert report['fact_count'] == len(built['facts'])
    assert Path(report['artifact_directory']).is_dir()

    expired = release.build(snapshot, now=now + timedelta(seconds=1201), code_revision=REVISION)
    assert not factors(expired).intersection(FACTORS)
    assert expired['macro_evidence_inventory']['facts'] == []
    assert_no_authority(expired)
    for key in sorted(FACTORS):
        data = snapshot['components'][key]['data']
        manifest_path = Path(data['manifest_path'] if key == 'cbr_liquidity_verified' else data['document_manifest_path'])
        raw_path = manifest_path.parent / (data['raw_sha256'] + '.html')
        original_raw = raw_path.read_bytes()
        try:
            raw_path.write_bytes(original_raw + b'changed')
            rejected = release.build(snapshot, now=now, code_revision=REVISION)
            assert factors(rejected) == FACTORS - {key}
            assert {r['component'] for r in rejected['macro_evidence_inventory']['facts']} == FACTORS - {key}
            assert_no_authority(rejected)
        finally:
            raw_path.write_bytes(original_raw)
    assert snapshot == before
    assert release.build(snapshot, now=now, code_revision=REVISION) == built


def test_forged_cached_release_cannot_pass_acceptance(frozen, tmp_path):
    snapshot, now = frozen
    snapshot['factual_release'] = release.describe(snapshot)
    snapshot['factual_release']['broker_execution'] = True
    # Direct projection is safe, but the acceptance gate must flag the forged cache.
    assert release.describe(snapshot)['broker_execution'] is False
    report = acceptance.run(snapshot, now=now, code_revision=REVISION, output=tmp_path / 'forged')
    assert report['status'] == 'FAIL'
    assert report['failures']
    assert any(check['status'] == 'FAIL' for check in report['checks'])


@pytest.mark.parametrize('cached', [False, True])
def test_missing_profile_required_monthly_fact_fails_with_or_without_cache(frozen, tmp_path, cached):
    snapshot, now = frozen
    if cached:
        snapshot['factual_release'] = release.describe(snapshot)
    del snapshot['components']['rosstat_monthly_cpi']
    # INCOMPLETE is expected; absence of this profile's required factor is the failure.
    assert release.describe(snapshot)['status'] == 'INCOMPLETE'
    report = acceptance.run(snapshot, now=now, code_revision=REVISION, output=tmp_path / 'missing')
    assert report['status'] == 'FAIL'
    assert report['failures']
    assert report['release_status'] == 'INCOMPLETE'
