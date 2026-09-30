"""Real refresh/admission/Parquet/read/export; only external source I/O is replaced."""
from datetime import timedelta
import json
from pathlib import Path
import runpy

import pytest
from moex_data import rub_snapshot_serialization as codec, rub_factual_release as release
from test_stage9_analysis_bundle_v2 import source_io, live, shifted_market, NOW


def test_real_refresh_stored_json_canonical_reader_and_delivery_are_lossless(tmp_path, monkeypatch):
    source_io(tmp_path, monkeypatch)
    saved, path = live.refresh_snapshot(now_fn=lambda: NOW, live_loader=lambda: shifted_market(NOW))
    stored = path.read_bytes()
    assert json.loads(stored)['schema_version'] == codec.STORAGE_SCHEMA
    assert codec.encoded(codec.loads(stored)) == codec.encoded(saved)
    assert len(stored) < len(codec.encoded(saved))*.9
    accepted = {str(p): p.read_bytes() for p in tmp_path.rglob('current_accepted_manifest.json')}
    encoded_read, _ = live.base.read_current_snapshot(now_fn=lambda: NOW)
    # Same path, generation and consumption clock; the legacy carrier is the oracle.
    path.write_bytes(codec.encoded(saved))
    legacy_read, _ = live.base.read_current_snapshot(now_fn=lambda: NOW)
    assert codec.encoded(encoded_read) == codec.encoded(legacy_read)
    path.write_bytes(stored)
    wire = codec.delivery(encoded_read)
    assert wire['schema_version'] == codec.DELIVERY_SCHEMA
    assert codec.encoded(codec.expand(wire)) == codec.encoded(encoded_read)
    package = release.compact(encoded_read, now=NOW, code_revision='a'*40)
    exported = release.export_current(output=tmp_path/'exports', now_fn=lambda: NOW, code_revision='a'*40)
    assert codec.loads(exported.read_bytes()) == package
    assert len(exported.read_bytes()) < len(codec.encoded(package))*.9
    helpers = runpy.run_path(str(Path(__file__).parent/'unit/test_rub_factual_snapshot_http_server.py'))
    with helpers['_running_server'](lambda: live.base.read_current_snapshot(now_fn=lambda: NOW)[0],
                                  release_loader=lambda: package) as port:
        status, _, body = helpers['_request'](port, helpers['api'].SNAPSHOT_PATH)
        release_status, _, package_body = helpers['_request'](port, helpers['api'].RELEASE_PATH)
    assert status == release_status == 200
    assert body['schema_version'] == package_body['schema_version'] == codec.DELIVERY_SCHEMA
    assert codec.expand(body) == encoded_read
    assert codec.encoded(package_body) == exported.read_bytes()
    # Historical bytes and accepted pointers are not rewritten by serialization/reads.
    assert all(Path(p).read_bytes() == raw for p, raw in accepted.items())
    assert path.read_bytes() == stored
    later, _ = live.base.read_current_snapshot(now_fn=lambda: NOW+timedelta(minutes=21))
    items = later['components']['stage9_daily']['data']['sections']['current_market']['items']
    assert items['futoi_live']['status'] == items['futoi_live_cr']['status'] == 'UNAVAILABLE'
    assert later['components']['stage9_weekly']['data']['sections']['completed_periods']['status'] == 'AVAILABLE'
    assert codec.expand(codec.delivery(later)) == later
    damaged = json.loads(stored); damaged['expanded_sha256'] = '0'*64
    path.write_bytes(codec.encoded(damaged))
    with pytest.raises(live.base.ChatAnalysisSnapshotError): live.base.read_current_snapshot(now_fn=lambda: NOW)
