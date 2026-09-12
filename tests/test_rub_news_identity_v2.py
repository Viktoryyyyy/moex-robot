from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import runpy

import pytest

from moex_research.intelligence.usdrubf_news_macro import NewsSourceRecord, process_news_batch, publication_identity
from moex_research.intelligence.usdrubf_news_live_pipeline import deterministic_neutral_news_classifier as classifier
from moex_research.intelligence.rub_news_selection import select, select_v1, replay, freeze_audit, POLICY
from moex_research.intelligence.rub_news_audit import read_audit
from moex_data.rub_news_read_view import compact_primary, project

NOW = datetime(2026, 9, 9, 3, 13, 53, 454649, tzinfo=timezone.utc)


def record(index=0, *, days=0, source='feed_a', body='Точный исходный текст ё €', receipt=NOW):
    published = NOW - timedelta(days=days, minutes=index)
    return NewsSourceRecord(source, 'OFFICIAL_PRIMARY', f'https://example.org/release/{index}',
                            published, published, receipt, 'Recurring weekly template', body)


def run(records, now=NOW):
    return process_news_batch(records, as_of_timestamp=now, classifier=classifier)


def normalized(value): return json.loads(json.dumps(value))


def test_publication_identity_distinguishes_template_date_version_and_reference():
    first = record()
    duplicate = replace(first, source_id='feed_b', source_reference='https://EXAMPLE.ORG/release/0#fragment')
    dated = replace(first, published_at=NOW-timedelta(days=1), available_at=NOW-timedelta(days=1))
    revised = replace(first, body='Changed version')
    related = replace(first, source_reference='https://example.org/release/other')
    result = run([first, duplicate, dated, revised, related])
    assert len(result.events) == 4 and result.exact_duplicates_removed == 1
    for event in result.events:
        identity = publication_identity(event.source_reference, event.published_at, event.content_hash)
        assert all(publication_identity(p.source_reference, p.published_at, p.content_hash) == identity for p in event.source_provenance)
        assert event.source_provenance[0].source_id == event.source_id


def test_primary_provenance_reserved_before_cap_and_reversal():
    records = [replace(record(), source_id=f'feed_{i:02d}') for i in range(20)]
    first = run(records).events[0]; reverse = run(reversed(records)).events[0]
    assert first == reverse and len(first.source_provenance) == 16
    assert first.source_provenance_total_count == 20 and first.source_provenance_truncated
    assert first.source_provenance[0].source_id == first.source_id


def test_publication_windows_exact_bounds_old_reingest_and_fresh_priority():
    records = [record(i) for i in range(25)] + [record(100+i, days=2, source=f'background{i}') for i in range(10)]
    selected, audit = select(run(records).events, limit=99, as_of=NOW)
    assert len(selected) == 20 and set(audit['selected_bands'].values()) == {'fresh'}
    selected, audit = select(run(records[25:]).events, limit=20, as_of=NOW)
    assert len(selected) == 4 and set(audit['selected_bands'].values()) == {'background'}
    for seconds, expected in [(86400, 'fresh'), (86400.000001, 'background'), (604800, 'background'), (604800.000001, None)]:
        stamp = NOW-timedelta(seconds=seconds)
        event = run([replace(record(), published_at=stamp, available_at=stamp)]).events[0]
        chosen, proof = select([event], limit=20, as_of=NOW)
        assert (proof['selected_bands'].get(event.event_id) if chosen else None) == expected


def test_read_aging_from_retained_pool_preserves_capture_and_unknown_authority():
    events = run([record(0), record(1, days=6)]).events
    pool = [compact_primary(asdict(e), identity_proven=True, audit_ref={'sha256':'a'*64}) for e in events]
    component = {'status':'PARTIAL', 'data': {'retained_event_pool': pool, 'summary': {'failed_source_count':1}}}
    before = deepcopy(component)
    later = project(component, now=NOW+timedelta(days=1, seconds=1))
    assert len(later['events']) == 1 and later['events'][0]['selection_band'] == 'background'
    assert later['events'][0]['retention_reason'] == 'latest_eligible_source_background_within_7d'
    assert project(component, now=NOW+timedelta(days=8))['events'] == []
    assert component == before


def test_legacy_v1_replay_preserves_exact_audit_and_new_policy_is_separate():
    events = run([record(0, days=30), record(1, days=1)]).events
    selected, old = select_v1(events, limit=20, as_of=NOW)
    _, rebuilt = replay(normalized(old))
    assert rebuilt == normalized(old) and old['policy'] == POLICY
    corrected, audit = select(events, limit=20, as_of=NOW)
    assert len(selected) == 2 and len(corrected) == 1 and audit['policy'] != POLICY


def test_capture_exact_roundtrip_future_input_and_dedup_receipt_overlay(tmp_path):
    records = [record(i, body='Тело публикации ' + str(i) + 'я'*4096) for i in range(30)]
    records.append(replace(record(31), published_at=NOW+timedelta(hours=1), available_at=NOW+timedelta(hours=1), ingested_at=NOW+timedelta(hours=1)))
    result = run(records); _, audit = select(result.events, limit=20, as_of=NOW)
    evidence = freeze_audit(audit, root=tmp_path, records=records, source_results=[{'source_id':'failed','quality_status':'SOURCE_UNAVAILABLE'}])
    restored = read_audit(evidence['path'], expected_sha256=evidence['sha256'], root=tmp_path)
    assert restored['records'] == tuple(records) and restored['audit'] == normalized(audit)
    assert run(restored['records']) == result and result.future_records_filtered == 1
    assert 'candidate_refs' not in json.dumps(evidence) and 'body' not in json.dumps(evidence)
    objects = tmp_path/'raw/news_selection/objects'
    before = {p.name:p.read_bytes() for p in objects.iterdir()}
    first_bytes = sum(p.stat().st_size for p in (tmp_path/'raw/news_selection').rglob('*.json'))
    later = NOW+timedelta(minutes=5)
    again_records = [replace(r, ingested_at=later) if r.ingested_at <= NOW else r for r in records]
    _, again_audit = select(run(again_records, later).events, limit=20, as_of=later)
    second = freeze_audit(again_audit, root=tmp_path, records=again_records)
    assert {p.name:p.read_bytes() for p in objects.iterdir()} == before
    total = sum(p.stat().st_size for p in (tmp_path/'raw/news_selection').rglob('*.json'))
    print({'first_capture_bytes':first_bytes,'reingestion_increment_bytes':total-first_bytes,'object_count':len(before),'record_count':len(records),'candidate_count':len(result.events)})
    assert total-first_bytes < first_bytes // 2
    assert second['sha256'] != evidence['sha256']


@pytest.mark.parametrize('defect', ['selection', 'policy', 'band', 'nan'])
def test_corrupt_audit_refuses_before_public_summary(tmp_path, defect):
    records = [record()]; _, audit = select(run(records).events, limit=20, as_of=NOW)
    if defect == 'selection': audit['selected_ids'] = ['missing']
    if defect == 'policy': audit['policy'] = 'unknown'
    if defect == 'band': audit['selected_bands'] = {audit['selected_ids'][0]:'invented'}
    if defect == 'nan': audit['candidates'][0]['confidence'] = float('nan')
    with pytest.raises(ValueError): freeze_audit(audit, root=tmp_path, records=records)


def test_legacy_missing_or_malformed_version_never_merges_ambiguous_events():
    first = asdict(run([record()]).events[0]); second = deepcopy(first)
    first.pop('content_hash'); second['content_hash'] = []; second['event_id'] = 'other-version'
    component = {'status':'READY','data':{'events':[first,second]}}
    selected = project(component, now=NOW)['events']
    assert len(selected) == 2
    assert all(e['publication_identity_policy'] == 'legacy_primary_only_mixed_cluster_identity_unproven' for e in selected)
    assert all('source_provenance' not in e for e in selected)


def test_partial_feed_failure_keeps_valid_events_and_acquisition_gap(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import socket
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    from moex_data.rub_factual_projection import consumer_context
    records = (record(),)
    good = SimpleNamespace(source_id='feed_a', quality_status='OK', records=records, error=None)
    bad = SimpleNamespace(source_id='minfin', quality_status='SOURCE_UNAVAILABLE', records=(), error='blocked')
    acquisition = SimpleNamespace(source_results=(good,bad), records=records, ok_source_count=1, failures=(bad,))
    result = SimpleNamespace(acquisition=acquisition, news=run(records), as_of_timestamp=NOW.isoformat(), acquired_record_count=1)
    def forbidden_network(*args, **kwargs): raise AssertionError('synthetic news test attempted network')
    monkeypatch.setattr(socket.socket, 'connect', forbidden_network)
    monkeypatch.setattr(runner.live, 'run_live_official_news_pipeline', lambda **_: result)
    monkeypatch.setattr(runner, '_data_root', lambda: tmp_path)
    component = runner._news_component(NOW)
    source = {'identity':{'generated_at_utc':NOW.isoformat()}, 'components':{'official_news':{'status':'READY','data':component.data}}}
    before = deepcopy(source)
    view = apply_read_freshness(source, now=NOW)
    news = consumer_context(view)['news_context']
    assert len(news['events']) == 1 and news['source_acquisition_status'] == 'PARTIAL'
    assert news['summary']['failed_source_count'] == 1 and news['summary']['failed_source_ids'] == 'minfin'
    assert news['events'][0]['direction'] == 'UNKNOWN'
    assert '_retained_event_pool' not in component.data['summary']
    assert 'body' not in json.dumps(component.data) and 'source_provenance' not in json.dumps(component.data)
    assert source == before


@pytest.mark.parametrize('defect', ['missing_object', 'wrong_digest', 'overlay_count', 'overlay_identity', 'mixed_publication', 'missing_primary'])
def test_rehashed_capture_or_object_cannot_bypass_reconstruction_contract(tmp_path, defect):
    events = run([record(), replace(record(), source_id='feed_b')]).events
    _, audit = select(events, limit=20, as_of=NOW)
    evidence = freeze_audit(audit, root=tmp_path, records=[record()])
    path = Path(evidence['path']); manifest = json.loads(path.read_bytes())
    ref = manifest['candidate_refs'][0]
    if defect == 'missing_object': ref['sha256'] = 'f'*64
    if defect == 'wrong_digest':
        (path.parent/'objects'/(ref['sha256']+'.json')).write_text('{}')
    if defect == 'overlay_count': ref['provenance_times'].pop()
    if defect == 'overlay_identity': ref['provenance_times'][0]['identity']['source_id'] = 'wrong'
    if defect in ('mixed_publication', 'missing_primary'):
        target = path.parent/'objects'/(ref['sha256']+'.json')
        obj = json.loads(target.read_bytes())
        if defect == 'mixed_publication':
            obj['value']['source_provenance'][0]['content_hash'] = 'f'*64
            ref['provenance_times'][0]['identity']['content_hash'] = 'f'*64
        else:
            obj['value']['source_provenance'].reverse()
            ref['provenance_times'].reverse()
        raw = json.dumps(obj).encode(); digest = sha256(raw).hexdigest()
        (target.parent/(digest+'.json')).write_bytes(raw); ref['sha256'] = digest
    raw = json.dumps(manifest).encode(); digest = sha256(raw).hexdigest()
    changed = path.parent/(digest+'.json'); changed.write_bytes(raw)
    with pytest.raises((ValueError, FileNotFoundError)):
        read_audit(changed, expected_sha256=digest, root=tmp_path)


def test_serialized_http_current_frozen_matrix_parity_and_reverse_oracle(tmp_path, monkeypatch):
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    from moex_data.rub_production_source_matrix import build as matrix
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
    from src.moex_research.consumers.usdrubf_chat_snapshot_consumer import load_factual_release
    helpers = runpy.run_path(str(Path(__file__).parent/'unit/test_rub_factual_snapshot_http_server.py'))
    core = runpy.run_path(str(Path(__file__).parent/'unit/test_rub_factual_projection.py'))
    source = core['core_snapshot'](); metadata = helpers['_snapshot']()
    for key in ('schema_version','refresh_policy','readiness','authority'): source[key] = metadata[key]
    source['identity'].update(project='MOEX_Bot', generated_at_utc=NOW.isoformat())
    events = run([record(), record(1,days=6)]).events
    pool = [compact_primary(asdict(e), identity_proven=True, audit_ref={'sha256':'a'*64}) for e in events]
    source['components']['official_news'] = {'status':'READY','refresh_attempted_at':NOW.isoformat(),
        'data':{'retained_event_pool':pool, 'events':pool, 'summary':{'source_count':1,'ok_source_count':1,'failed_source_count':0,'failed_source_ids':''}}}
    source = normalized(source); original = deepcopy(source)
    consumed = NOW+timedelta(days=1, seconds=1)
    monkeypatch.setenv('MOEX_DATA_ROOT',str(tmp_path))
    base._atomic_write(base.current_snapshot_path(tmp_path),source)
    stored = base.current_snapshot_path(tmp_path).read_bytes()
    with helpers['_running_server'](lambda: base.read_current_snapshot(now_fn=lambda: consumed)[0],
            release_loader=lambda: load_factual_release(now_fn=lambda: consumed,code_revision='a'*40)) as port:
        status,_,actual = helpers['_request'](port,helpers['api'].RELEASE_PATH)
        heavy_status,_,heavy = helpers['_request'](port,helpers['api'].SNAPSHOT_PATH)
    assert status == heavy_status == 200
    assert actual == release.compact(source,now=consumed,code_revision='a'*40)
    exported = release.export_current(output=tmp_path/'exports',now_fn=lambda: consumed,code_revision='a'*40)
    assert json.loads(exported.read_bytes()) == actual
    view = apply_read_freshness(source,now=consumed)
    ids = [e['event_id'] for e in actual['news_context']['events']]
    assert len(ids)==1
    assert [e['event_id'] for e in heavy['components']['official_news']['data']['events']] == ids
    assert [e['event_id'] for e in matrix(view)['news_view']] == ids
    full = release.build(source,now=consumed,code_revision='a'*40)
    projection_completeness(source,full,now=consumed)
    for defect in ('drop','inject','alter'):
        changed = deepcopy(full)
        if defect=='drop': changed['news_context']['events']=[]
        elif defect=='inject': changed['news_context']['events'].append(deepcopy(changed['news_context']['events'][0]))
        else: changed['news_context']['events'][0]['primary_provenance']['source_reference']='invented'
        with pytest.raises(AssertionError): projection_completeness(source,changed,now=consumed)
    assert source == original and base.current_snapshot_path(tmp_path).read_bytes()==stored
