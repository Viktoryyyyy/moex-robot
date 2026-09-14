from datetime import datetime, timezone
from hashlib import sha256
import json

import pytest

from moex_data import rub_factual_release as source
from moex_research.external_data import rosstat_factual_export_pin as export_pin
from moex_research.external_data import rosstat_polling_retention as retention


NOW = datetime(2026, 9, 14, 12, 30, tzinfo=timezone.utc)
COMMIT = 'a' * 40


def test_frozen_export_pins_rosstat_evidence_before_writing_artifacts(monkeypatch, tmp_path):
    snapshot = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {}}
    release = {'input_snapshot_sha256': 'b' * 64, 'as_of_utc': NOW.isoformat(), 'status': 'INCOMPLETE'}
    monkeypatch.setattr(source, 'build', lambda *args, **kwargs: dict(release))
    calls = []

    def pin(value, *, pin_id, created_at):
        calls.append((value, pin_id, created_at))
        return {'pin_id': pin_id, 'pin_path': '/pin.json', 'pin_sha256': 'c' * 64, 'manifest_count': 2}

    monkeypatch.setattr(export_pin, 'pin', pin)
    directory = source.export(snapshot, now=NOW, code_revision=COMMIT, output=tmp_path)
    raw = source._encoded(release)
    expected_sha = sha256(raw).hexdigest()

    assert calls == [(snapshot, expected_sha, NOW)]
    assert directory.name == expected_sha
    manifest = json.loads((directory / 'manifest.json').read_text())
    assert manifest['rosstat_evidence_pin']['pin_id'] == expected_sha
    assert manifest['rosstat_evidence_pin']['manifest_count'] == 2


def test_pin_failure_prevents_creation_of_frozen_export(monkeypatch, tmp_path):
    snapshot = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {}}
    release = {'input_snapshot_sha256': 'b' * 64, 'as_of_utc': NOW.isoformat(), 'status': 'INCOMPLETE'}
    monkeypatch.setattr(source, 'build', lambda *args, **kwargs: dict(release))
    monkeypatch.setattr(export_pin, 'pin',
                        lambda *args, **kwargs: (_ for _ in ()).throw(retention.RosstatPollingRetentionError('pin failed')))

    with pytest.raises(retention.RosstatPollingRetentionError, match='pin failed'):
        source.export(snapshot, now=NOW, code_revision=COMMIT, output=tmp_path)
    assert not list(tmp_path.iterdir())
