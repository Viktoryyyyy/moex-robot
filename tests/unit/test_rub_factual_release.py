from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import pytest
from moex_data.rub_factual_release import build, export

NOW = datetime(2026, 9, 8, 4, 0, tzinfo=timezone.utc)
COMMIT = 'a' * 40


def snapshot():
    return {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {}, 'authority': {'broker_execution': False}}


def test_missing_required_input_never_becomes_neutral_or_complete(tmp_path):
    value = snapshot()
    before = deepcopy(value)
    release = build(value, now=NOW, code_revision=COMMIT)
    assert value == before
    assert release['status'] == 'INCOMPLETE'
    assert 'external_cny' in release['blocking_required_factors']
    for horizon in ('D1', 'W1'):
        assert release['horizons'][horizon]['model_probability'] is None
        assert release['horizons'][horizon]['target_date_proven'] is False
    directory = export(value, now=NOW, code_revision=COMMIT, output=tmp_path)
    manifest = json.loads((directory / 'manifest.json').read_text())
    assert sha256((directory / 'release.json').read_bytes()).hexdigest() == manifest['release_sha256']
    assert sha256((directory / 'input_snapshot.json').read_bytes()).hexdigest() == manifest['input_snapshot_sha256']
    replay = build(json.loads((directory / 'input_snapshot.json').read_text()), now=NOW, code_revision=COMMIT)
    assert replay == release


def test_expired_current_pair_is_not_presented_as_a_usable_fact():
    value = snapshot()
    stamp = (NOW - timedelta(seconds=1201)).isoformat()
    value['components']['futoi_live'] = {'status': 'READY', 'data': {
        'factual_authority': True, 'consumer_factual_use_allowed': True,
        'current_intraday': {'status': 'FRESH', 'last_success_at': stamp,
            'factual': {'snapshot_ts': stamp, 'source_publication_time': stamp,
                'availability_ts_utc': stamp, 'ingest_ts_utc': stamp}}}}
    release = build(value, now=NOW, code_revision=COMMIT)
    assert not any(f['factor'] == 'futoi_live' for f in release['facts'])
    assert 'futoi_live' in release['blocking_required_factors']


def test_future_snapshot_or_unpinned_revision_cannot_be_exported():
    with pytest.raises(ValueError): build(snapshot(), now=NOW - timedelta(seconds=1), code_revision=COMMIT)
    with pytest.raises(ValueError): build(snapshot(), now=NOW, code_revision='main')
