"""Immutable deduplicated captures within the canonical raw/news_selection audit."""
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256
import json
from pathlib import Path
import re

SCHEMA = 'rub_news_capture.v2'
_HASH = re.compile(r'[0-9a-f]{64}')
_RECORD_FIELDS = ('source_id', 'source_tier', 'source_reference', 'published_at', 'headline', 'body')
_IDENTITY_FIELDS = ('source_id', 'source_reference', 'published_at', 'content_hash')


def _bytes(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def _directory(root):
    root = Path(root).resolve()
    directory = root / 'raw' / 'news_selection'
    for path in (root / 'raw', directory, directory / 'objects'):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError('news audit path escapes root or is symlink')
    return directory


def _write(directory, value):
    raw = _bytes(value); digest = sha256(raw).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (digest + '.json')
    if path.is_symlink(): raise ValueError('news audit symlink refused')
    try:
        with path.open('xb') as stream: stream.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw: raise ValueError('news audit collision')
    return digest


def _load(directory, digest, schema):
    if not isinstance(digest, str) or not _HASH.fullmatch(digest):
        raise ValueError('invalid news audit digest')
    path = directory / (digest + '.json')
    if path.is_symlink(): raise ValueError('news audit symlink refused')
    raw = path.read_bytes()
    if sha256(raw).hexdigest() != digest: raise ValueError('news audit digest mismatch')
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get('schema_version') != schema:
        raise ValueError('news audit schema mismatch')
    return value


def _times(value):
    return {key: value.pop(key) for key in ('available_at', 'ingested_at')}


def _identity(value):
    return {key: value[key] for key in _IDENTITY_FIELDS}


def _causal(value, as_of, *, input_record=False):
    from .rub_news_selection import _time
    if not _time(value['published_at']) <= _time(value['available_at']) <= _time(value['ingested_at']):
        raise ValueError('news audit causal order invalid')
    if not input_record and _time(value['ingested_at']) > _time(as_of):
        raise ValueError('news audit candidate not yet available at capture')


def freeze(audit, *, root, records=(), source_results=()):
    """Store exact parser output and classifier output; return bounded public metadata."""
    from .rub_news_selection import CURRENT_POLICY
    if audit.get('policy') != CURRENT_POLICY: raise ValueError('v2 audit policy required')
    directory = _directory(root); objects = directory / 'objects'
    manifest = {'schema_version': SCHEMA, 'audit': deepcopy(audit),
                'records': [], 'candidate_refs': [], 'source_results': deepcopy(list(source_results)),
                'input_scope': 'exact_acquired_parser_records_not_original_wire_responses'}
    candidates = manifest['audit'].pop('candidates')
    for record in records:
        value = asdict(record)
        for key in ('published_at', 'available_at', 'ingested_at'): value[key] = value[key].isoformat()
        _causal(value, audit['as_of'], input_record=True)
        receipt = _times(value)
        stable = {'schema_version': 'rub_news_record.v2', 'value': {key: value[key] for key in _RECORD_FIELDS}}
        manifest['records'].append({'sha256': _write(objects, stable), **receipt})
    for candidate in candidates:
        value = deepcopy(candidate); _causal(value, audit['as_of'])
        receipt = _times(value); provenance_times = []
        for provenance in value.get('source_provenance', []):
            _causal(provenance, audit['as_of'])
            provenance_times.append({'identity': _identity(provenance), **_times(provenance)})
        stable = {'schema_version': 'rub_news_candidate.v2', 'value': value}
        manifest['candidate_refs'].append({'sha256': _write(objects, stable), 'event_id': value['event_id'],
                                          **receipt, 'provenance_times': provenance_times})
    digest = _write(directory, manifest)
    path = directory / (digest + '.json')
    # Validate the same reconstruction used by offline acceptance before returning success.
    read_audit(path, expected_sha256=digest, root=root)
    fields = ('policy', 'as_of', 'limit', 'candidate_count', 'selected_ids', 'eligible_event_count',
              'events_dropped_by_bound', 'eligible_source_count', 'selected_source_count',
              'relevance_validation_complete', 'selection_semantics', 'selected_bands',
              'fresh_horizon_seconds', 'background_horizon_seconds', 'background_limit')
    return {'schema_version': SCHEMA, 'path': str(path), 'sha256': digest,
            'audit_ref': {'path': str(path), 'sha256': digest, 'schema_version': SCHEMA},
            **{key: deepcopy(audit[key]) for key in fields if key in audit}}


def read_audit(path, *, expected_sha256, root):
    """Rehydrate exact v2 audit/records without fetching or executing a classifier."""
    from .usdrubf_news_macro import NewsSourceRecord
    directory = _directory(root)
    expected = directory / (expected_sha256 + '.json') if isinstance(expected_sha256, str) and _HASH.fullmatch(expected_sha256) else None
    if expected is None or Path(path).absolute() != expected:
        raise ValueError('news audit path/digest mismatch')
    manifest = _load(directory, expected_sha256, SCHEMA)
    audit = deepcopy(manifest['audit']); records = []; candidates = []
    if not isinstance(audit, dict) or not all(isinstance(manifest.get(key), list) for key in ('records', 'candidate_refs', 'source_results')):
        raise ValueError('malformed news capture')
    for ref in manifest['records']:
        value = _load(directory / 'objects', ref['sha256'], 'rub_news_record.v2')['value']
        value.update({key: ref[key] for key in ('available_at', 'ingested_at')})
        _causal(value, audit['as_of'], input_record=True); records.append(NewsSourceRecord(**value))
    for ref in manifest['candidate_refs']:
        value = _load(directory / 'objects', ref['sha256'], 'rub_news_candidate.v2')['value']
        if value.get('event_id') != ref['event_id']: raise ValueError('candidate overlay identity mismatch')
        provenance = value.get('source_provenance', [])
        if not isinstance(ref.get('provenance_times'), list) or len(provenance) != len(ref['provenance_times']):
            raise ValueError('candidate provenance overlay count mismatch')
        for item, receipt in zip(provenance, ref['provenance_times']):
            if _identity(item) != receipt.get('identity'): raise ValueError('provenance overlay identity mismatch')
            item.update({key: receipt[key] for key in ('available_at', 'ingested_at')}); _causal(item, audit['as_of'])
        value.update({key: ref[key] for key in ('available_at', 'ingested_at')})
        _causal(value, audit['as_of']); candidates.append(value)
    audit['candidates'] = candidates
    if audit.get('candidate_count') != len(candidates): raise ValueError('candidate count mismatch')
    from .rub_news_selection import CURRENT_POLICY, replay
    from .usdrubf_news_macro import publication_identity
    if audit.get('policy') != CURRENT_POLICY: raise ValueError('unsupported capture policy')
    if len({item['event_id'] for item in candidates}) != len(candidates): raise ValueError('duplicate candidate identity')
    for candidate in candidates:
        identity = publication_identity(candidate['source_reference'], candidate['published_at'], candidate['content_hash'])
        provenance = candidate.get('source_provenance', [])
        primary_fields = (*_IDENTITY_FIELDS, 'source_tier', 'available_at', 'ingested_at')
        if not provenance or any(provenance[0].get(key) != candidate.get(key) for key in primary_fields):
            raise ValueError('primary provenance must occupy first reserved slot')
        for item in candidate.get('source_provenance', []):
            if publication_identity(item['source_reference'], item['published_at'], item['content_hash']) != identity:
                raise ValueError('mixed publication provenance')
    _, reproduced = replay(audit)
    if _bytes(reproduced) != _bytes(audit): raise ValueError('selection audit does not replay exactly')
    return {'audit': audit, 'records': tuple(records), 'source_results': manifest['source_results'],
            'input_scope': manifest['input_scope']}
