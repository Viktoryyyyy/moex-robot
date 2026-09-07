import copy
import hashlib
import json
from datetime import datetime, timezone, timedelta

import pandas as pd
import pytest

from moex_data.futures import futoi_publication_audit as audit
from moex_data.futures import futoi_current_pair_authority as authority
from moex_data.futures import futoi_live_factual_refresh_source_native as source


@pytest.fixture
def sample(tmp_path):
    rows = []
    for ts, offset in [('18:00:00', 1), ('18:05:00', 0)]:
        for group, long, short in [('FIZ', 100, -80), ('YUR', 80 + offset, -100)]:
            rows.append(dict(trade_date='2026-09-07', ts='2026-09-07 ' + ts,
                systime='2026-09-07 18:05:09', availability_ts_utc='2026-09-07T15:06:00+00:00',
                ingest_ts='2026-09-07T15:06:00+00:00', sess_id=7642, seqnum=321,
                clgroup=group, pos=long+short, pos_long=long, pos_short=short,
                pos_long_num=10, pos_short_num=12, source_id=source.SOURCE_ID,
                instrument_id=source.CR_INSTRUMENT_ID, source_ticker='cr', secid='CRU6'))
    frame = pd.DataFrame(rows)
    path = tmp_path / 'raw.parquet'
    frame.to_parquet(path, index=False)
    provenance = {}
    for key, path in [('raw_partition', path), ('raw_quality_report', tmp_path/'quality.json'),
                      ('raw_refresh_manifest', tmp_path/'manifest.json')]:
        if path.suffix == '.json':
            path.write_text('{}')
        provenance[key+'_ref'] = source._rooted_ref(tmp_path, path)
        provenance[key+'_sha256'] = source._sha256_file(path)
    identity = dict(expected_trade_date='2026-09-07', expected_instrument_id=source.CR_INSTRUMENT_ID,
                    expected_source_ticker='cr', expected_secid='CRU6')
    return tmp_path, frame, provenance, identity


def record_for(sample):
    root, frame, provenance, identity = sample
    fact = audit.audited_latest(root, frame, provenance, **identity)
    return dict(status='FRESH', factual=fact, provenance=provenance,
                last_success_at='2026-09-07T15:06:00+00:00', failed_attempt_at=None)


def test_audit_keeps_rejected_history_and_exact_frontier(sample):
    root, _, _, _ = sample
    record = record_for(sample)
    verified = audit.verify_current(root, record)
    assert verified['publication_count'] == 2
    assert verified['rejected_count'] == 1
    assert record['factual']['snapshot_ts'] == '2026-09-07T15:05:00+00:00'


@pytest.mark.parametrize('defect', ['incomplete', 'balance', 'session', 'revision'])
def test_rejected_frontier_has_immutable_attempt_receipt(sample, defect):
    root, frame, provenance, identity = sample
    if defect == 'incomplete':
        frame = frame.iloc[:-1].copy()
    elif defect == 'balance':
        frame.loc[3, 'pos_long'] += 1
        frame.loc[3, 'pos'] += 1
    elif defect == 'session':
        frame.loc[3, 'sess_id'] = 1
    else:
        frame = pd.concat([frame, frame.iloc[[3]]], ignore_index=True)
    path = root / 'raw.parquet'
    frame.to_parquet(path, index=False)
    provenance['raw_partition_sha256'] = source._sha256_file(path)
    with pytest.raises(source.FutoiSourceNativeRefreshError) as caught:
        audit.audited_latest(root, frame, provenance, **identity)
    receipt = caught.value.publication_audit
    path = audit._verified_path(root, receipt['ref'], receipt['sha256'])
    report = json.loads(path.read_text())
    assert report['latest_status'] == 'REJECTED'
    assert report['latest_factual'] is None
    assert caught.value.attempt_provenance['publication_audit'] == receipt


@pytest.mark.parametrize('defect', ['audit', 'raw_partition', 'raw_quality_report', 'raw_refresh_manifest', 'fact', 'retained'])
def test_consumer_rejects_tampering_or_retention(sample, defect):
    root, _, _, _ = sample
    record = record_for(sample)
    if defect == 'fact':
        record['factual']['fiz']['net'] += 1
    elif defect == 'retained':
        record['status'] = 'RETAINED_STALE'
    else:
        provenance = record['provenance']
        ref = provenance['publication_audit']['ref'] if defect == 'audit' else provenance[defect+'_ref']
        (root / ref.removeprefix(source.ROOT_REF_PREFIX)).write_bytes(b'corrupt')
    with pytest.raises(ValueError):
        audit.verify_current(root, record)


def test_admission_requires_explicit_hashed_scope_and_expires(sample):
    root, _, _, _ = sample
    record = record_for(sample)
    evidence = dict(scope=authority.SCOPE, policy=audit.POLICY, instrument_id=source.CR_INSTRUMENT_ID,
                    canonical_live_smoke='PASS', negative_replay='PASS', historical_authority=False)
    path = root / 'acceptance.json'
    path.write_text(json.dumps(evidence))
    entry = dict(accepted=True, scope=authority.SCOPE, evidence_ref='acceptance.json',
                 evidence_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    values = dict(instrument_acceptance={source.CR_INSTRUMENT_ID: {'current_pair_acceptance': entry}},
                  gates=[dict(required=True, status='PASS')])
    now = datetime(2026, 9, 7, 15, 7, tzinfo=timezone.utc)
    assert authority.admit(values, record, root=root, repo_root=root, now=now)['allowed']
    assert not authority.admit(values, record, root=root, repo_root=root, now=now+timedelta(minutes=20))['allowed']
    assert not authority.admit(values, record, root=root, repo_root=root, now=now-timedelta(minutes=5))['allowed']
    values['gates'][0]['status'] = 'BLOCKED'
    assert not authority.admit(values, record, root=root, repo_root=root, now=now)['allowed']
    values['gates'][0]['status'] = 'PASS'
    path.write_text('{}')
    assert not authority.admit(values, record, root=root, repo_root=root, now=now)['allowed']


def test_read_time_revokes_without_mutating_archived_fact(sample):
    record = record_for(sample)
    snapshot = {'components': {'futoi_live_cr': {'status': 'READY', 'data': {
        'factual_authority_scope': authority.SCOPE, 'current_intraday': record,
        'factual_authority': True, 'consumer_factual_use_allowed': True,
        'current_pair_admission': {'allowed': True}}}},
        'authority': {'futoi_by_instrument': {'cr_futures_family': {'factual_authority': True}}}}
    original_fact = copy.deepcopy(record['factual'])
    authority.apply_read_freshness(snapshot, now=datetime(2026,9,7,15,25,1,tzinfo=timezone.utc))
    assert snapshot['components']['futoi_live_cr']['status'] == 'UNAVAILABLE'
    assert snapshot['authority']['futoi_by_instrument']['cr_futures_family']['factual_authority'] is False
    assert record['factual'] == original_fact
