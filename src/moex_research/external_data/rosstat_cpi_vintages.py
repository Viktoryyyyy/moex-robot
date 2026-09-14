"""Immutable normalized Rosstat CPI vintages with point-in-time availability."""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from tempfile import NamedTemporaryFile

DATASET_ID = 'rosstat_cpi_vintages'
SCHEMA_VERSION = 'rosstat_cpi_vintages.v1'
CONTRACT_REF = 'contracts/datasets/rosstat_cpi_vintages.v1.yaml'
SOURCE_CONTRACT_REF = 'contracts/sources/macro/rosstat_cpi.v1.yaml'
SOURCE_ID = 'rosstat_cpi'
DATASET_RELATIVE_ROOT = Path('state/datasets/dataset_id=rosstat_cpi_vintages')
_HASH = re.compile(r'[0-9a-f]{64}')
_MONTH = re.compile(r'\d{4}-\d{2}')

SERIES = {
    'ROSSTAT_WEEKLY_CPI_ESTIMATE': {
        'frequency': 'WEEKLY',
        'value_fields': ('indices', 'weekly_change_percent', 'units', 'document_format', 'monthly_final'),
    },
    'ROSSTAT_MONTHLY_CPI': {
        'frequency': 'MONTHLY',
        'value_fields': ('indices', 'changes_percent', 'units', 'estimate_kind', 'decimal_places', 'precision_scope'),
    },
}


class RosstatVintageError(ValueError):
    pass


def _encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _digest(value):
    return sha256(_encoded(value)).hexdigest()


def _utc(value, field):
    try:
        result = datetime.fromisoformat(value) if isinstance(value, str) else value
    except ValueError as exc:
        raise RosstatVintageError(f'{field} must be ISO datetime') from exc
    if not isinstance(result, datetime) or result.utcoffset() is None:
        raise RosstatVintageError(f'{field} must be timezone-aware')
    return result.astimezone(timezone.utc)


def _utc_iso(value, field):
    return _utc(value, field).isoformat()


def _date(value, field):
    if value is None:
        return None
    if not isinstance(value, str):
        raise RosstatVintageError(f'{field} must be ISO date')
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise RosstatVintageError(f'{field} must be ISO date') from exc
    return parsed.isoformat()


def _canonical_copy(value):
    try:
        return json.loads(_encoded(value))
    except (TypeError, ValueError) as exc:
        raise RosstatVintageError('normalized value must be finite JSON') from exc


def _series_shape(data):
    if not isinstance(data, dict):
        raise RosstatVintageError('Rosstat normalized data must be an object')
    series_id = data.get('series_id')
    spec = SERIES.get(series_id)
    if spec is None:
        raise RosstatVintageError('unsupported Rosstat CPI series')
    if data.get('geography') != 'RU':
        raise RosstatVintageError('Rosstat CPI geography must be RU')
    if series_id == 'ROSSTAT_WEEKLY_CPI_ESTIMATE':
        start = _date(data.get('observation_start'), 'observation_start')
        end = _date(data.get('observation_end'), 'observation_end')
        if start is None or end is None or start > end:
            raise RosstatVintageError('invalid weekly observation identity')
        observation = {'observation_start': start, 'observation_end': end}
        observation_key = start + '__' + end
    else:
        month = data.get('observation_month')
        if not isinstance(month, str) or not _MONTH.fullmatch(month):
            raise RosstatVintageError('invalid monthly observation identity')
        try:
            datetime.strptime(month, '%Y-%m')
        except ValueError as exc:
            raise RosstatVintageError('invalid monthly observation month') from exc
        observation = {'observation_month': month}
        observation_key = month
    value = {}
    for field in spec['value_fields']:
        if field not in data:
            raise RosstatVintageError(f'missing normalized value field: {field}')
        value[field] = _canonical_copy(data[field])
    return series_id, spec['frequency'], observation_key, observation, value


def _publication(data):
    published_at = data.get('source_publication_time')
    if published_at is not None:
        published_at = _utc_iso(published_at, 'source_publication_time')
    published_date = _date(data.get('listed_publication_date'), 'listed_publication_date')
    received_at = _utc_iso(data.get('received_at'), 'received_at')
    available_at = _utc_iso(data.get('system_available_at', received_at), 'system_available_at')
    if _utc(available_at, 'available_at') > _utc(received_at, 'received_at'):
        raise RosstatVintageError('available_at cannot follow receipt used to establish it')
    return published_at, published_date, available_at, received_at


def _provenance(data, received_at):
    required = ('source_url', 'raw_sha256', 'document_manifest_path', 'document_manifest_sha256')
    missing = [field for field in required if not isinstance(data.get(field), str) or not data[field]]
    if missing:
        raise RosstatVintageError('missing Rosstat release provenance: ' + ','.join(missing))
    raw_sha = data['raw_sha256']
    document_sha = data['document_manifest_sha256']
    if not _HASH.fullmatch(raw_sha) or not _HASH.fullmatch(document_sha):
        raise RosstatVintageError('invalid Rosstat provenance hash')
    result = {
        'source_id': SOURCE_ID,
        'source_url': data['source_url'],
        'raw_sha256': raw_sha,
        'document_manifest_path': data['document_manifest_path'],
        'document_manifest_sha256': document_sha,
        'received_at': received_at,
        'contract_ref': SOURCE_CONTRACT_REF,
    }
    for field in ('index_manifest_path', 'index_manifest_sha256'):
        value = data.get(field)
        if value is not None:
            if not isinstance(value, str) or not value:
                raise RosstatVintageError(f'invalid optional provenance field: {field}')
            if field.endswith('sha256') and not _HASH.fullmatch(value):
                raise RosstatVintageError(f'invalid optional provenance hash: {field}')
            result[field] = value
    return result


def _root(root):
    root = Path(root)
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise RosstatVintageError('MOEX data root must be an existing absolute non-symlink directory')
    return root.resolve(strict=True)


def _ensure_dir(path, root):
    candidate = path.resolve(strict=False)
    if not candidate.is_relative_to(root):
        raise RosstatVintageError('Rosstat vintage path escapes data root')
    path.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir() or not path.resolve(strict=True).is_relative_to(root):
        raise RosstatVintageError('Rosstat vintage directory must remain inside data root')
    return path


def _read_json(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise RosstatVintageError('Rosstat vintage artifact must be a regular file')
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RosstatVintageError('invalid Rosstat vintage JSON') from exc
    if not isinstance(value, dict):
        raise RosstatVintageError('Rosstat vintage JSON must be an object')
    return value


def _write_immutable(path, payload):
    encoded = _encoded(payload)
    if path.is_symlink():
        raise RosstatVintageError('Rosstat vintage symlink refused')
    try:
        with path.open('xb') as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if path.read_bytes() != encoded:
            raise RosstatVintageError('conflicting existing Rosstat vintage')
    return sha256(encoded).hexdigest()


def _atomic_write(path, payload):
    if path.exists() and path.is_symlink():
        raise RosstatVintageError('Rosstat current pointer symlink refused')
    encoded = _encoded(payload) + b'\n'
    temp_name = None
    try:
        with NamedTemporaryFile(mode='wb', dir=path.parent, prefix='.current.', suffix='.tmp', delete=False) as stream:
            temp_name = stream.name
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
        temp_name = None
    finally:
        if temp_name is not None:
            Path(temp_name).unlink(missing_ok=True)


def _vintages(observation_dir, *, series_id, observation_key):
    records = []
    for path in observation_dir.glob('vintage_id=*.json'):
        if path.is_symlink() or not path.is_file():
            raise RosstatVintageError('invalid Rosstat vintage artifact type')
        vintage_id = path.name.removeprefix('vintage_id=').removesuffix('.json')
        if not _HASH.fullmatch(vintage_id):
            raise RosstatVintageError('invalid Rosstat vintage filename')
        value = _read_json(path)
        if (value.get('schema_version') != SCHEMA_VERSION or value.get('dataset_id') != DATASET_ID
                or value.get('series_id') != series_id or value.get('observation_key') != observation_key
                or value.get('vintage_id') != vintage_id):
            raise RosstatVintageError('Rosstat vintage identity mismatch')
        seq = value.get('revision_seq')
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise RosstatVintageError('invalid Rosstat revision sequence')
        records.append((seq, path, value))
    records.sort(key=lambda item: item[0])
    if records and [seq for seq, _, _ in records] != list(range(len(records))):
        raise RosstatVintageError('Rosstat revision sequence must be contiguous from zero')
    return records


def _pointer_ref(record, path, artifact_sha, *, last_verified_at, latest_provenance):
    return {
        'schema_version': SCHEMA_VERSION,
        'dataset_id': DATASET_ID,
        'contract_ref': CONTRACT_REF,
        'series_id': record['series_id'],
        'frequency': record['frequency'],
        'observation_key': record['observation_key'],
        'vintage_id': record['vintage_id'],
        'vintage_path': str(path),
        'vintage_sha256': artifact_sha,
        'semantic_sha256': record['semantic_sha256'],
        'revision_seq': record['revision_seq'],
        'revision_status': record['revision_status'],
        'published_at': record['published_at'],
        'published_date': record['published_date'],
        'available_at': record['available_at'],
        'last_verified_at': last_verified_at,
        'latest_provenance': deepcopy(latest_provenance),
    }


def record(root, data):
    """Persist or reuse a semantic CPI vintage and atomically refresh its series pointer."""
    root = _root(root)
    series_id, frequency, observation_key, observation, current_value = _series_shape(data)
    published_at, published_date, available_at, received_at = _publication(data)
    provenance = _provenance(data, received_at)
    semantic_sha = _digest({'series_id': series_id, 'observation_key': observation_key,
                            'value_current': current_value})

    series_dir = _ensure_dir(root / DATASET_RELATIVE_ROOT / ('series_id=' + series_id), root)
    observation_dir = _ensure_dir(series_dir / ('observation_key=' + observation_key), root)
    records = _vintages(observation_dir, series_id=series_id, observation_key=observation_key)

    if records:
        initial = records[0][2]
        latest_seq, latest_path, latest = records[-1]
        if _utc(available_at, 'available_at') < _utc(latest['available_at'], 'existing available_at'):
            raise RosstatVintageError('new Rosstat evidence cannot precede latest stored vintage availability')
        if latest.get('semantic_sha256') == semantic_sha:
            artifact_sha = sha256(latest_path.read_bytes()).hexdigest()
            ref = _pointer_ref(latest, latest_path, artifact_sha,
                               last_verified_at=received_at, latest_provenance=provenance)
            current_path = series_dir / 'current.json'
            prior_pointer = _read_json(current_path) if current_path.exists() else None
            if prior_pointer is None or _utc(received_at, 'received_at') >= _utc(prior_pointer['last_verified_at'], 'pointer last_verified_at'):
                _atomic_write(current_path, ref)
            return ref
        revision_seq = latest_seq + 1
        revision_status = 'REVISED'
        value_initial = deepcopy(initial['value_initial'])
        value_previous = deepcopy(latest['value_current'])
        value_revised = deepcopy(current_value)
        revision_at = available_at
    else:
        revision_seq = 0
        revision_status = 'INITIAL'
        value_initial = deepcopy(current_value)
        value_previous = None
        value_revised = None
        revision_at = None

    vintage_id = _digest({'series_id': series_id, 'observation_key': observation_key,
                          'revision_seq': revision_seq, 'semantic_sha256': semantic_sha})
    record_value = {
        'schema_version': SCHEMA_VERSION,
        'dataset_id': DATASET_ID,
        'contract_ref': CONTRACT_REF,
        'source_contract_ref': SOURCE_CONTRACT_REF,
        'series_id': series_id,
        'frequency': frequency,
        'geography': 'RU',
        'observation_key': observation_key,
        'observation': observation,
        'vintage_id': vintage_id,
        'semantic_sha256': semantic_sha,
        'revision_seq': revision_seq,
        'revision_status': revision_status,
        'published_at': published_at,
        'published_date': published_date,
        'available_at': available_at,
        'revision_at': revision_at,
        'value_initial': value_initial,
        'value_previous': value_previous,
        'value_revised': value_revised,
        'value_current': deepcopy(current_value),
        'provenance': provenance,
        'pit': {
            'eligible_from': available_at,
            'no_lookahead': True,
            'availability_authority': 'system_received_at',
            'published_at_does_not_backdate_system_availability': True,
        },
    }
    vintage_path = observation_dir / ('vintage_id=' + vintage_id + '.json')
    artifact_sha = _write_immutable(vintage_path, record_value)
    ref = _pointer_ref(record_value, vintage_path, artifact_sha,
                       last_verified_at=received_at, latest_provenance=provenance)
    current_path = series_dir / 'current.json'
    prior_pointer = _read_json(current_path) if current_path.exists() else None
    if prior_pointer is None or _utc(received_at, 'received_at') >= _utc(prior_pointer['last_verified_at'], 'pointer last_verified_at'):
        _atomic_write(current_path, ref)
    return ref


def validate_reference(reference, data):
    """Verify that a live component references the exact normalized immutable vintage."""
    if not isinstance(reference, dict):
        raise RosstatVintageError('Rosstat vintage reference must be an object')
    series_id, frequency, observation_key, _, current_value = _series_shape(data)
    path_value = reference.get('vintage_path')
    artifact_sha = reference.get('vintage_sha256')
    if not isinstance(path_value, str) or not isinstance(artifact_sha, str) or not _HASH.fullmatch(artifact_sha):
        raise RosstatVintageError('invalid Rosstat vintage reference path or hash')
    path = Path(path_value)
    record_value = _read_json(path)
    if sha256(path.read_bytes()).hexdigest() != artifact_sha:
        raise RosstatVintageError('Rosstat vintage artifact hash mismatch')
    expected = {
        'schema_version': SCHEMA_VERSION,
        'dataset_id': DATASET_ID,
        'contract_ref': CONTRACT_REF,
        'series_id': series_id,
        'frequency': frequency,
        'observation_key': observation_key,
    }
    if any(reference.get(key) != value or record_value.get(key) != value for key, value in expected.items()):
        raise RosstatVintageError('Rosstat vintage reference identity mismatch')
    if reference.get('vintage_id') != record_value.get('vintage_id') or reference.get('semantic_sha256') != record_value.get('semantic_sha256'):
        raise RosstatVintageError('Rosstat vintage reference digest mismatch')
    if record_value.get('value_current') != current_value:
        raise RosstatVintageError('Rosstat vintage normalized value mismatch')
    current_received = _utc(data.get('received_at'), 'received_at')
    if _utc(record_value.get('available_at'), 'vintage available_at') > current_received:
        raise RosstatVintageError('Rosstat vintage is not point-in-time available')
    return True
