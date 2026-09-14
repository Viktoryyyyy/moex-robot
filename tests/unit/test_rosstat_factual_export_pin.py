from datetime import datetime, timezone
from pathlib import Path

import pytest

from moex_research.external_data import rosstat_factual_export_pin as source
from moex_research.external_data import rosstat_polling_retention as retention


NOW = datetime(2026, 9, 14, 12, 30, tzinfo=timezone.utc)


def snapshot_with_refs(*paths):
    components = {}
    for index, path in enumerate(paths):
        name = 'rosstat_cpi' if index == 0 else 'rosstat_monthly_cpi'
        components[name] = {'status': 'READY', 'data': {
            'index_manifest_path': str(path),
            'index_manifest_sha256': 'a' * 64,
            'document_manifest_path': str(path),
            'document_manifest_sha256': 'a' * 64,
        }}
    return {'components': components}


def test_external_offline_layout_needs_no_gc_pin(monkeypatch, tmp_path):
    external = tmp_path / 'fixture' / ('a' * 64 + '.json')
    external.parent.mkdir()
    external.write_text('{}')
    monkeypatch.setattr(retention, 'pin_snapshot', lambda *args, **kwargs: pytest.fail('must not pin external fixture'))

    assert source.pin(snapshot_with_refs(external), pin_id='b' * 64, created_at=NOW) is None


def test_governed_layout_is_forwarded_to_retention_pin(monkeypatch, tmp_path):
    governed = tmp_path / retention.EVIDENCE_RELATIVE_DIRS[0] / ('a' * 64 + '.json')
    governed.parent.mkdir(parents=True)
    governed.write_text('{}')
    calls = []
    monkeypatch.setattr(retention, 'pin_snapshot',
                        lambda snapshot, *, pin_id, created_at: calls.append((snapshot, pin_id, created_at)) or {'ok': True})
    value = snapshot_with_refs(governed)

    assert source.pin(value, pin_id='b' * 64, created_at=NOW) == {'ok': True}
    assert calls == [(value, 'b' * 64, NOW)]


def test_mixed_governed_and_external_layout_fails_closed(tmp_path):
    governed = tmp_path / retention.EVIDENCE_RELATIVE_DIRS[0] / ('a' * 64 + '.json')
    governed.parent.mkdir(parents=True)
    governed.write_text('{}')
    external = tmp_path / 'fixture' / ('b' * 64 + '.json')
    external.parent.mkdir()
    external.write_text('{}')

    with pytest.raises(retention.RosstatPollingRetentionError, match='mixes governed and external'):
        source.pin(snapshot_with_refs(governed, external), pin_id='c' * 64, created_at=NOW)
