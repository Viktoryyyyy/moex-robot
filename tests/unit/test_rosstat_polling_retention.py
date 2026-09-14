from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path

import pytest

from moex_research.external_data import rosstat_cpi_vintages as vintages
from moex_research.external_data import rosstat_https as transport
from moex_research.external_data import rosstat_polling_retention as source


T0 = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)
INDEX_URL = 'https://rosstat.gov.ru/compendium/document/50798'
DOC_URL = 'https://rosstat.gov.ru/storage/mediabank/137_09-09-2026.html'
WEEKLY_DIR = Path('raw/external/rosstat_weekly_cpi')
MONTHLY_DIR = Path('raw/external/rosstat_monthly_cpi')


def freeze_receipt(root, relative, *, raw, url=INDEX_URL, received=T0, requested=None):
    directory = root / relative
    directory.mkdir(parents=True, exist_ok=True)
    raw_sha = sha256(raw).hexdigest()
    raw_path = directory / (raw_sha + '.html')
    if not raw_path.exists():
        raw_path.write_bytes(raw)
    requested = requested or (received - timedelta(seconds=1))
    receipt = {
        'policy': transport.POLICY,
        'source_url': url,
        'requested_at_utc': requested.isoformat(),
        'received_at_utc': received.isoformat(),
        'raw_sha256': raw_sha,
        'certificate_sha256': transport.CERTIFICATES,
        'tls_chain_and_hostname_verified': True,
        'semantic_validation_status': 'NOT_PARSED',
        'factual_authority': False,
        'historical_pit_acceptance': False,
        'action_authority': False,
    }
    encoded = json.dumps(receipt, sort_keys=True, separators=(',', ':')).encode()
    digest = sha256(encoded).hexdigest()
    path = directory / (digest + '.json')
    path.write_bytes(encoded)
    return {'path': str(path), 'sha': digest, 'raw_sha': raw_sha, 'raw_path': raw_path}


def refs(index, document=None):
    document = document or index
    return {
        'index_manifest_path': index['path'],
        'index_manifest_sha256': index['sha'],
        'document_manifest_path': document['path'],
        'document_manifest_sha256': document['sha'],
    }


def write_snapshot(root, *, weekly=None, monthly=None):
    components = {}
    if weekly is not None:
        components['rosstat_cpi'] = {'status': 'READY', 'data': dict(weekly)}
    if monthly is not None:
        components['rosstat_monthly_cpi'] = {'status': 'READY', 'data': dict(monthly)}
    path = root / source.CURRENT_SNAPSHOT_RELATIVE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'components': components}), encoding='utf-8')
    return path


def initialize(root):
    result = source.garbage_collect(root, now=T0)
    assert result['status'] == 'INITIALIZED'


def test_gc_deletes_only_post_epoch_unreferenced_receipt_and_raw(tmp_path):
    initialize(tmp_path)
    keep = freeze_receipt(tmp_path, WEEKLY_DIR, raw=b'keep', received=T0 + timedelta(minutes=1))
    drop = freeze_receipt(tmp_path, WEEKLY_DIR, raw=b'drop', received=T0 + timedelta(minutes=2))
    write_snapshot(tmp_path, weekly=refs(keep))

    result = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=30))

    assert result['status'] == 'READY'
    assert result['deleted_manifests'] == 1
    assert result['deleted_raw_files'] == 1
    assert Path(keep['path']).exists()
    assert keep['raw_path'].exists()
    assert not Path(drop['path']).exists()
    assert not drop['raw_path'].exists()


def test_shared_raw_survives_when_any_retained_receipt_still_references_it(tmp_path):
    initialize(tmp_path)
    raw = b'shared release body'
    keep = freeze_receipt(tmp_path, WEEKLY_DIR, raw=raw, received=T0 + timedelta(minutes=1))
    drop = freeze_receipt(tmp_path, WEEKLY_DIR, raw=raw, received=T0 + timedelta(minutes=2))
    assert keep['raw_sha'] == drop['raw_sha']
    write_snapshot(tmp_path, weekly=refs(keep))

    result = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=30))

    assert result['deleted_manifests'] == 1
    assert result['deleted_raw_files'] == 0
    assert keep['raw_path'].exists()
    assert Path(keep['path']).exists()
    assert not Path(drop['path']).exists()


def test_unreferenced_receipt_is_protected_for_live_replay_grace(tmp_path):
    initialize(tmp_path)
    candidate = freeze_receipt(tmp_path, WEEKLY_DIR, raw=b'in-flight',
                               received=T0 + timedelta(minutes=1))
    write_snapshot(tmp_path)

    early = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=20))
    assert early['status'] == 'READY'
    assert early['grace_seconds'] == 1200
    assert early['deleted_manifests'] == 0
    assert Path(candidate['path']).exists()
    assert candidate['raw_path'].exists()

    late = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=21))
    assert late['status'] == 'READY'
    assert late['deleted_manifests'] == 1
    assert late['deleted_raw_files'] == 1
    assert not Path(candidate['path']).exists()
    assert not candidate['raw_path'].exists()


def test_pre_epoch_evidence_is_never_deleted_by_migration_gc(tmp_path):
    old = freeze_receipt(tmp_path, WEEKLY_DIR, raw=b'legacy', received=T0 - timedelta(days=1))
    initialize(tmp_path)
    write_snapshot(tmp_path)

    result = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=1))

    assert result['status'] == 'READY'
    assert result['deleted_manifests'] == 0
    assert Path(old['path']).exists()
    assert old['raw_path'].exists()


def test_immutable_vintage_and_current_pointer_both_pin_evidence(tmp_path):
    initialize(tmp_path)
    vintage_evidence = freeze_receipt(tmp_path, WEEKLY_DIR, raw=b'vintage', received=T0 + timedelta(minutes=1))
    pointer_evidence = freeze_receipt(tmp_path, WEEKLY_DIR, raw=b'pointer', received=T0 + timedelta(minutes=2))
    vintage_dir = (tmp_path / source.VINTAGE_RELATIVE_ROOT /
                   'series_id=ROSSTAT_WEEKLY_CPI_ESTIMATE' /
                   'observation_key=2026-09-01__2026-09-07')
    vintage_dir.mkdir(parents=True)
    (vintage_dir / ('vintage_id=' + 'a' * 64 + '.json')).write_text(json.dumps({
        'provenance': refs(vintage_evidence),
    }), encoding='utf-8')
    pointer = vintage_dir.parent / 'current.json'
    pointer.write_text(json.dumps({'latest_provenance': refs(pointer_evidence)}), encoding='utf-8')
    write_snapshot(tmp_path)

    result = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=30))

    assert result['status'] == 'READY'
    assert result['deleted_manifests'] == 0
    assert Path(vintage_evidence['path']).exists()
    assert Path(pointer_evidence['path']).exists()


def test_frozen_snapshot_pin_survives_after_current_snapshot_moves(tmp_path):
    initialize(tmp_path)
    frozen = freeze_receipt(tmp_path, MONTHLY_DIR, raw=b'frozen', received=T0 + timedelta(minutes=1))
    snapshot = {'components': {'rosstat_monthly_cpi': {'status': 'READY', 'data': refs(frozen)}}}
    pin = source.pin_snapshot(snapshot, pin_id='f' * 64, created_at=T0 + timedelta(minutes=2))
    assert pin['manifest_count'] == 1
    write_snapshot(tmp_path)

    result = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=30))

    assert result['status'] == 'READY'
    assert Path(frozen['path']).exists()
    assert frozen['raw_path'].exists()
    assert Path(pin['pin_path']).exists()


def test_malformed_pin_blocks_gc_without_deleting_candidate(tmp_path):
    initialize(tmp_path)
    candidate = freeze_receipt(tmp_path, WEEKLY_DIR, raw=b'candidate', received=T0 + timedelta(minutes=1))
    write_snapshot(tmp_path)
    pin_dir = tmp_path / source.PIN_RELATIVE_DIR
    pin_dir.mkdir(parents=True)
    (pin_dir / ('b' * 64 + '.json')).write_text('{}', encoding='utf-8')

    result = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=30))

    assert result['status'] == 'BLOCKED'
    assert result['deleted_manifests'] == 0
    assert Path(candidate['path']).exists()
    assert candidate['raw_path'].exists()


def test_delete_batch_is_bounded_and_converges(tmp_path):
    initialize(tmp_path)
    for index in range(source.MAX_DELETE_MANIFESTS_PER_CALL + 2):
        freeze_receipt(tmp_path, WEEKLY_DIR, raw=b'shared',
                       received=T0 + timedelta(minutes=1, microseconds=index))
    write_snapshot(tmp_path)

    first = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=30))
    remaining = list((tmp_path / WEEKLY_DIR).glob('*.json'))
    assert first['deleted_manifests'] == source.MAX_DELETE_MANIFESTS_PER_CALL
    assert len(remaining) == 2
    assert (tmp_path / WEEKLY_DIR / (sha256(b'shared').hexdigest() + '.html')).exists()

    second = source.garbage_collect(tmp_path, now=T0 + timedelta(minutes=31))
    assert second['deleted_manifests'] == 2
    assert not list((tmp_path / WEEKLY_DIR).glob('*.json'))
    assert not (tmp_path / WEEKLY_DIR / (sha256(b'shared').hexdigest() + '.html')).exists()


def test_vintage_record_invokes_gc_after_pointer_promotion(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(source, 'garbage_collect',
                        lambda root, *, now: calls.append((Path(root), now)) or {'status': 'READY'})
    data = {
        'series_id': 'ROSSTAT_WEEKLY_CPI_ESTIMATE',
        'geography': 'RU',
        'observation_start': '2026-09-01',
        'observation_end': '2026-09-07',
        'indices': {'previous_registration': '100.05', 'month_start': '100.05', 'year_start': '104.72'},
        'weekly_change_percent': '0.05',
        'units': 'index_percent_base_100',
        'document_format': 'three_explicit_bases',
        'monthly_final': False,
        'source_publication_time': None,
        'listed_publication_date': '2026-09-09',
        'source_url': DOC_URL,
        'raw_sha256': 'a' * 64,
        'document_manifest_path': '/evidence/document.json',
        'document_manifest_sha256': 'b' * 64,
        'index_manifest_path': '/evidence/index.json',
        'index_manifest_sha256': 'c' * 64,
        'received_at': '2026-09-14T12:00:05+00:00',
        'system_available_at': '2026-09-14T12:00:05+00:00',
    }

    ref = vintages.record(tmp_path, data)

    pointer = Path(ref['vintage_path']).parents[1] / 'current.json'
    assert pointer.is_file()
    assert calls == [(tmp_path.resolve(), datetime(2026, 9, 14, 12, 0, 5, tzinfo=timezone.utc))]
