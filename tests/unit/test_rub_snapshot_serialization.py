"""Synthetic histories, exact byte restoration and hostile representation checks."""
from copy import deepcopy
import base64
import json
import zlib

import pytest
from moex_data import rub_snapshot_serialization as codec


def document():
    rows = [{'seqnum': 2**53+i, 'sess_id': 2**53+7, 'date': '2026-09-23',
             'source_time': '2026-09-23T10:35:00+03:00', 'value': i-1500,
             'accepted_at': '2026-09-24T07:35:01+00:00'} for i in range(2500)]
    return {'project': 'MOEX_Bot', 'schema_version': codec.SNAPSHOT_SCHEMA,
        'identity': {'project': 'MOEX_Bot'}, 'components': {},
        'a/history~рус': rows, 'daily': deepcopy(rows), 'weekly': deepcopy(rows),
        'literal': {codec.REF: '/not-a-reference'},
        'literal_escape': {codec.LITERAL: {codec.REF: '/also-literal'}},
        'evidence': {'raw': base64.b64encode(bytes(range(256))*1200).decode()}}


@pytest.mark.parametrize('pack,schema', [(codec.storage, codec.STORAGE_SCHEMA), (codec.delivery, codec.DELIVERY_SCHEMA)])
def test_every_byte_history_row_clock_and_large_identifier_survives(pack, schema):
    source = document(); original = codec.encoded(source)
    packed = pack(source)
    assert packed['schema_version'] == schema
    assert len(codec.encoded(packed)) < len(original)*.7
    restored = codec.loads(codec.encoded(packed))
    assert codec.encoded(restored) == original == codec.encoded(source)
    assert len(restored['weekly']) == 2500
    restored['daily'][0]['value'] = 999
    assert restored['weekly'][0]['value'] == -1500
    assert restored['a/history~рус'][0]['seqnum'] == 2**53
    assert restored['literal'] == source['literal']
    assert pack(source) == packed


@pytest.mark.parametrize('defect', ['truncated', 'trailing', 'hash', 'length', 'bool_size',
                                  'version', 'logical_schema', 'encoding', 'project', 'base64', 'bomb'])
def test_storage_damage_is_rejected(defect):
    value = codec.storage(document())
    if defect == 'truncated': value['payload'] = value['payload'][:-4]
    elif defect == 'trailing': value['payload'] = base64.b64encode(base64.b64decode(value['payload'])+b'extra').decode()
    elif defect == 'hash': value['expanded_sha256'] = '0'*64
    elif defect == 'length': value['expanded_bytes'] += 1
    elif defect == 'bool_size': value['expanded_bytes'] = True
    elif defect == 'version': value['schema_version'] = 'rub_snapshot_storage.v99'
    elif defect == 'logical_schema': value['logical_schema_version'] = codec.PACKAGE_SCHEMA
    elif defect == 'encoding': value['encoding'] = 'pickle'
    elif defect == 'project': value['project'] = 'other'
    elif defect == 'base64': value['payload'] = '%%%'
    else:
        value['expanded_bytes'] = 10
        value['payload'] = base64.b64encode(zlib.compress(b'x'*1_000_000)).decode()
    with pytest.raises(ValueError): codec.expand(value)


@pytest.mark.parametrize('defect', ['missing', 'cycle', 'escape', 'index', 'changed', 'dropped', 'limit', 'identity'])
def test_references_cannot_hide_changed_or_missing_information(defect):
    value = codec.delivery(document())
    if defect == 'missing': value['data']['daily'] = {codec.REF: '/absent'}
    elif defect == 'cycle': value['data']['daily'] = {codec.REF: '/daily'}
    elif defect == 'escape': value['data']['daily'] = {codec.REF: '/a~2history'}
    elif defect == 'index': value['data']['daily'] = {codec.REF: '/a~1history~0рус/01'}
    elif defect == 'changed': value['data']['a/history~рус'][0]['value'] += 1
    elif defect == 'dropped': del value['data']['weekly']
    elif defect == 'identity': value['logical_schema_version'] = codec.PACKAGE_SCHEMA
    else: value['expanded_bytes'] = 100
    with pytest.raises(ValueError): codec.expand(value)


def test_legacy_small_unrelated_and_oversized_objects_remain_lossless(monkeypatch):
    small = {'schema_version': codec.SNAPSHOT_SCHEMA, 'history': [1, 2, 3]}
    assert codec.storage(small) == codec.delivery(small) == codec.expand(small) == small
    unrelated = {**document(), 'schema_version': 'fast_market.v1'}
    assert codec.storage(unrelated) is unrelated and codec.delivery(unrelated) is unrelated
    monkeypatch.setattr(codec, 'MAX_BYTES', 300000)
    value = document()
    assert codec.storage(value) is value and codec.delivery(value) is value


def test_rosstat_retention_reads_encoded_current_and_fails_closed(tmp_path):
    from moex_research.external_data import rosstat_cpi_factual as cpi, rosstat_polling_retention as retention
    value = document()
    value['components']['rosstat_cpi'] = {'data': {'index_manifest_path': '/index.json',
        'index_manifest_sha256': 'a'*64, 'document_manifest_path': '/document.json',
        'document_manifest_sha256': 'b'*64}}
    path = tmp_path / cpi.CURRENT_SNAPSHOT_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    packed = codec.storage(value); path.write_bytes(codec.encoded(packed))
    assert cpi._current_index_manifests(tmp_path) == ('/index.json',)
    assert retention._current_snapshot_refs(tmp_path) == retention._snapshot_refs(value)
    packed['expanded_sha256'] = '0'*64; path.write_bytes(codec.encoded(packed))
    assert cpi._current_index_manifests(tmp_path) is None
    with pytest.raises(retention.RosstatPollingRetentionError): retention._current_snapshot_refs(tmp_path)


def test_source_matrix_cli_expands_input_but_hashes_supplied_bytes(tmp_path, monkeypatch):
    import runpy
    import sys
    from pathlib import Path
    from moex_data import rub_production_source_matrix as matrix
    helpers = runpy.run_path(str(Path(__file__).with_name('test_rub_factual_projection.py')))
    source = helpers['core_snapshot']()
    source.update(schema_version=codec.SNAPSHOT_SCHEMA, synthetic_history=document()['weekly'])
    path = tmp_path/'snapshot.json'; raw = codec.encoded(codec.storage(source))
    assert json.loads(raw)['schema_version'] == codec.STORAGE_SCHEMA
    path.write_bytes(raw); output = tmp_path/'matrix.json'
    monkeypatch.setattr(sys, 'argv', ['matrix', '--snapshot', str(path), '--output', str(output)])
    runpy.run_path(matrix.__file__, run_name='__main__')
    result = json.loads(output.read_bytes())
    assert result.pop('snapshot_sha256') == codec.sha256(raw).hexdigest()
    assert result == matrix.build(source)


@pytest.mark.parametrize('pack', [codec.storage, codec.delivery, lambda value:value])
def test_documented_raw_fallback_cli_restores_logical_root_without_mutation(tmp_path, capsys, pack):
    source = document(); path = tmp_path/'current.json'
    original = codec.encoded(pack(source)); path.write_bytes(original)
    assert codec.main(['--expand', str(path)]) == 0
    assert capsys.readouterr().out.encode('utf-8') == codec.encoded(source)+b'\n'
    assert path.read_bytes() == original


def test_decoder_cli_does_not_emit_partial_output_on_bad_evidence(tmp_path, capsys):
    value = codec.storage(document()); value['expanded_sha256'] = '0'*64
    path = tmp_path/'current.json'; path.write_bytes(codec.encoded(value))
    with pytest.raises(ValueError, match='digest'): codec.main(['--expand', str(path)])
    assert capsys.readouterr().out == ''
