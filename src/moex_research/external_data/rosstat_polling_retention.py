"""Reference-aware post-publish GC for Rosstat polling evidence.

The collector never deletes evidence that may still be replayed by the current
snapshot, immutable CPI vintages, series current pointers, or frozen factual
release pins.  Evidence that predates the local GC epoch is also retained so
pre-existing exports remain valid without requiring retroactive discovery.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import gzip
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from tempfile import NamedTemporaryFile

from . import rosstat_https as transport


SCHEMA_VERSION = 'rosstat_polling_retention.v1'
PIN_SCHEMA_VERSION = 'rosstat_evidence_pin.v1'
GC_POLICY = 'post_publish_reference_aware_gc_v1'
EVIDENCE_RELATIVE_DIRS = (
    Path('raw/external/rosstat_weekly_cpi'),
    Path('raw/external/rosstat_monthly_cpi'),
)
CURRENT_SNAPSHOT_RELATIVE_PATH = Path('state/rub_intelligence/chat_analysis_snapshot/current.json')
VINTAGE_RELATIVE_ROOT = Path('state/datasets/dataset_id=rosstat_cpi_vintages')
GC_STATE_RELATIVE_DIR = Path('state/retention/rosstat_polling_gc')
PIN_RELATIVE_DIR = Path('state/retention/rosstat_evidence_pins')
EPOCH_FILENAME = 'epoch.json'
STATUS_FILENAME = 'current.json'
LOCK_FILENAME = '.lock'
MAX_DELETE_MANIFESTS_PER_CALL = 128
_HASH = re.compile(r'[0-9a-f]{64}')
_COMPONENTS = ('rosstat_cpi', 'rosstat_monthly_cpi')


class RosstatPollingRetentionError(ValueError):
    pass


def _encoded(value: object) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'), allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise RosstatPollingRetentionError('retention payload must be finite JSON') from exc


def _utc(value, field: str) -> datetime:
    try:
        result = datetime.fromisoformat(value) if isinstance(value, str) else value
    except ValueError as exc:
        raise RosstatPollingRetentionError(f'{field} must be ISO datetime') from exc
    if not isinstance(result, datetime) or result.utcoffset() is None:
        raise RosstatPollingRetentionError(f'{field} must be timezone-aware')
    return result.astimezone(timezone.utc)


def _root(root) -> Path:
    value = Path(root)
    if not value.is_absolute() or value.is_symlink() or not value.is_dir():
        raise RosstatPollingRetentionError('MOEX data root must be an existing absolute non-symlink directory')
    return value.resolve(strict=True)


def _ensure_dir(path: Path, root: Path) -> Path:
    candidate = path.resolve(strict=False)
    if not candidate.is_relative_to(root):
        raise RosstatPollingRetentionError('retention directory escapes MOEX data root')
    path.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise RosstatPollingRetentionError('retention directory must be a regular directory')
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise RosstatPollingRetentionError('retention directory escaped MOEX data root')
    return resolved


def _read_object(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise RosstatPollingRetentionError('retention JSON must be a regular file')
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RosstatPollingRetentionError('invalid retention JSON') from exc
    if not isinstance(value, dict):
        raise RosstatPollingRetentionError('retention JSON must contain an object')
    return value


def _atomic_write(path: Path, payload: dict) -> None:
    if path.exists() and path.is_symlink():
        raise RosstatPollingRetentionError('retention state symlink refused')
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _encoded(payload) + b'\n'
    temp_name = None
    try:
        with NamedTemporaryFile(mode='wb', dir=path.parent, prefix='.current.', suffix='.tmp',
                                delete=False) as stream:
            temp_name = stream.name
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
        temp_name = None
    finally:
        if temp_name is not None:
            Path(temp_name).unlink(missing_ok=True)


def _write_immutable(path: Path, payload: dict) -> str:
    if path.is_symlink():
        raise RosstatPollingRetentionError('immutable retention artifact symlink refused')
    encoded = _encoded(payload)
    try:
        with path.open('xb') as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if path.read_bytes() != encoded:
            raise RosstatPollingRetentionError('conflicting immutable retention artifact')
    return sha256(encoded).hexdigest()


@contextmanager
def _lock(root: Path):
    state_dir = _ensure_dir(root / GC_STATE_RELATIVE_DIR, root)
    path = state_dir / LOCK_FILENAME
    if path.is_symlink():
        raise RosstatPollingRetentionError('retention lock symlink refused')
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, 'a+b') as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            yield
    finally:
        pass


def _reference_pairs(value: dict, *, context: str, require_document: bool = True) -> list[tuple[str, str]]:
    if not isinstance(value, dict):
        raise RosstatPollingRetentionError(f'{context} provenance must be an object')
    result: list[tuple[str, str]] = []
    pairs = (
        ('index_manifest_path', 'index_manifest_sha256', False),
        ('document_manifest_path', 'document_manifest_sha256', require_document),
    )
    for path_field, hash_field, required in pairs:
        path_value = value.get(path_field)
        hash_value = value.get(hash_field)
        if path_value is None and hash_value is None:
            if required:
                raise RosstatPollingRetentionError(f'{context} missing {path_field}')
            continue
        if not isinstance(path_value, str) or not path_value or not isinstance(hash_value, str) or not _HASH.fullmatch(hash_value):
            raise RosstatPollingRetentionError(f'{context} invalid manifest reference')
        result.append((path_value, hash_value))
    return result


def _snapshot_refs(snapshot: dict) -> list[tuple[str, str]]:
    components = snapshot.get('components')
    if not isinstance(components, dict):
        raise RosstatPollingRetentionError('snapshot components missing during retention')
    refs: list[tuple[str, str]] = []
    for component_name in _COMPONENTS:
        component = components.get(component_name)
        if component is None:
            continue
        if not isinstance(component, dict):
            raise RosstatPollingRetentionError('Rosstat snapshot component must be an object')
        data = component.get('data')
        if data is None:
            continue
        if not isinstance(data, dict):
            raise RosstatPollingRetentionError('Rosstat snapshot data must be an object')
        refs.extend(_reference_pairs(data, context='snapshot ' + component_name))
    return refs


def _current_snapshot_refs(root: Path) -> list[tuple[str, str]]:
    path = root / CURRENT_SNAPSHOT_RELATIVE_PATH
    if not path.exists():
        return []
    return _snapshot_refs(_read_object(path))


def _vintage_refs(root: Path) -> list[tuple[str, str]]:
    dataset = root / VINTAGE_RELATIVE_ROOT
    if not dataset.exists():
        return []
    if dataset.is_symlink() or not dataset.is_dir():
        raise RosstatPollingRetentionError('Rosstat vintage root must be a regular directory')
    refs: list[tuple[str, str]] = []
    for path in sorted(dataset.glob('series_id=*/observation_key=*/vintage_id=*.json')):
        if path.is_symlink() or not path.is_file():
            raise RosstatPollingRetentionError('Rosstat vintage artifact type invalid during retention')
        value = _read_object(path)
        provenance = value.get('provenance')
        refs.extend(_reference_pairs(provenance, context='immutable vintage'))
    for path in sorted(dataset.glob('series_id=*/current.json')):
        if path.is_symlink() or not path.is_file():
            raise RosstatPollingRetentionError('Rosstat current pointer type invalid during retention')
        value = _read_object(path)
        provenance = value.get('latest_provenance')
        refs.extend(_reference_pairs(provenance, context='series current pointer'))
    return refs


def _pin_refs(root: Path) -> list[tuple[str, str]]:
    directory = root / PIN_RELATIVE_DIR
    if not directory.exists():
        return []
    if directory.is_symlink() or not directory.is_dir():
        raise RosstatPollingRetentionError('Rosstat pin registry must be a regular directory')
    refs: list[tuple[str, str]] = []
    for path in sorted(directory.glob('*.json')):
        if path.is_symlink() or not path.is_file() or not _HASH.fullmatch(path.stem):
            raise RosstatPollingRetentionError('invalid Rosstat evidence pin artifact')
        value = _read_object(path)
        if value.get('schema_version') != PIN_SCHEMA_VERSION or value.get('pin_id') != path.stem:
            raise RosstatPollingRetentionError('Rosstat evidence pin identity mismatch')
        manifests = value.get('manifests')
        if not isinstance(manifests, list) or not manifests:
            raise RosstatPollingRetentionError('Rosstat evidence pin manifest list invalid')
        for item in manifests:
            if not isinstance(item, dict):
                raise RosstatPollingRetentionError('Rosstat evidence pin entry invalid')
            path_value = item.get('path')
            digest = item.get('sha256')
            if not isinstance(path_value, str) or not path_value or not isinstance(digest, str) or not _HASH.fullmatch(digest):
                raise RosstatPollingRetentionError('Rosstat evidence pin reference invalid')
            refs.append((path_value, digest))
    return refs


def _evidence_directory(root: Path, manifest: Path) -> Path:
    if not manifest.is_absolute():
        raise RosstatPollingRetentionError('Rosstat manifest path must be absolute')
    for relative in EVIDENCE_RELATIVE_DIRS:
        directory = root / relative
        if directory.exists() and (directory.is_symlink() or not directory.is_dir()):
            raise RosstatPollingRetentionError('Rosstat evidence directory must be regular')
        try:
            if manifest.parent.resolve(strict=True) == directory.resolve(strict=True):
                return directory.resolve(strict=True)
        except FileNotFoundError:
            continue
    raise RosstatPollingRetentionError('Rosstat manifest reference outside governed evidence directories')


def _receipt(root: Path, path_value: str, digest: str) -> tuple[Path, dict]:
    path = Path(path_value)
    if not _HASH.fullmatch(digest) or path.name != digest + '.json' or path.is_symlink() or not path.is_file():
        raise RosstatPollingRetentionError('Rosstat receipt identity mismatch')
    directory = _evidence_directory(root, path)
    resolved = path.resolve(strict=True)
    if resolved.parent != directory:
        raise RosstatPollingRetentionError('Rosstat receipt escaped evidence directory')
    encoded = resolved.read_bytes()
    if sha256(encoded).hexdigest() != digest:
        raise RosstatPollingRetentionError('Rosstat receipt hash mismatch')
    try:
        value = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RosstatPollingRetentionError('Rosstat receipt JSON invalid') from exc
    raw_sha = value.get('raw_sha256') if isinstance(value, dict) else None
    if (not isinstance(value, dict) or value.get('policy') != transport.POLICY
            or not isinstance(raw_sha, str) or not _HASH.fullmatch(raw_sha)):
        raise RosstatPollingRetentionError('unrecognized Rosstat receipt')
    try:
        transport.validate_url(value.get('source_url'))
    except (TypeError, ValueError) as exc:
        raise RosstatPollingRetentionError('Rosstat receipt source invalid') from exc
    requested = _utc(value.get('requested_at_utc'), 'receipt requested_at_utc')
    received = _utc(value.get('received_at_utc'), 'receipt received_at_utc')
    if requested > received:
        raise RosstatPollingRetentionError('Rosstat receipt causal order invalid')
    return resolved, value


def _all_receipts(root: Path) -> list[dict]:
    records = []
    for relative in EVIDENCE_RELATIVE_DIRS:
        directory = root / relative
        if not directory.exists():
            continue
        if directory.is_symlink() or not directory.is_dir():
            raise RosstatPollingRetentionError('Rosstat evidence directory must be regular')
        for path in sorted(directory.iterdir()):
            if path.suffix != '.json':
                continue
            if path.is_symlink() or not path.is_file() or not _HASH.fullmatch(path.stem):
                raise RosstatPollingRetentionError('unexpected JSON in Rosstat evidence directory')
            resolved, receipt = _receipt(root, str(path), path.stem)
            records.append({
                'path': resolved,
                'digest': path.stem,
                'raw_sha256': receipt['raw_sha256'],
                'received_at': _utc(receipt['received_at_utc'], 'receipt received_at_utc'),
            })
    return records


def _validate_protected_refs(root: Path, refs: list[tuple[str, str]]) -> set[Path]:
    protected: set[Path] = set()
    for path_value, digest in refs:
        path, _ = _receipt(root, path_value, digest)
        protected.add(path)
    return protected


def _load_epoch(root: Path, now: datetime) -> tuple[dict, bool]:
    state_dir = _ensure_dir(root / GC_STATE_RELATIVE_DIR, root)
    path = state_dir / EPOCH_FILENAME
    if not path.exists():
        value = {
            'schema_version': SCHEMA_VERSION,
            'policy': GC_POLICY,
            'started_at_utc': now.isoformat(),
            'pre_epoch_evidence_deletion_allowed': False,
        }
        _write_immutable(path, value)
        return value, True
    value = _read_object(path)
    if value.get('schema_version') != SCHEMA_VERSION or value.get('policy') != GC_POLICY:
        raise RosstatPollingRetentionError('Rosstat retention epoch identity mismatch')
    started = _utc(value.get('started_at_utc'), 'retention epoch started_at_utc')
    if started > now:
        raise RosstatPollingRetentionError('Rosstat retention epoch is in the future')
    if value.get('pre_epoch_evidence_deletion_allowed') is not False:
        raise RosstatPollingRetentionError('pre-epoch Rosstat evidence must remain protected')
    return value, False


def _verify_raw_for_delete(path: Path, raw_sha: str) -> int:
    if path.is_symlink() or not path.is_file():
        raise RosstatPollingRetentionError('Rosstat raw candidate must be a regular file')
    try:
        stored = path.read_bytes()
        raw = gzip.decompress(stored) if path.name.endswith('.html.gz') else stored
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise RosstatPollingRetentionError('Rosstat raw candidate verification failed') from exc
    if sha256(raw).hexdigest() != raw_sha:
        raise RosstatPollingRetentionError('Rosstat raw candidate hash mismatch')
    return len(stored)


def _write_status(root: Path, status: dict) -> None:
    state_dir = _ensure_dir(root / GC_STATE_RELATIVE_DIR, root)
    _atomic_write(state_dir / STATUS_FILENAME, status)


def garbage_collect(root, *, now) -> dict:
    """Delete only post-epoch Rosstat polling evidence with no live/replay references.

    The first call creates an epoch and deletes nothing.  This protects all historical
    evidence that may be referenced by exports created before this policy existed.
    """
    root = _root(root)
    now_utc = _utc(now, 'retention now')
    with _lock(root):
        try:
            epoch, created = _load_epoch(root, now_utc)
            if created:
                status = {
                    'schema_version': SCHEMA_VERSION,
                    'policy': GC_POLICY,
                    'status': 'INITIALIZED',
                    'checked_at_utc': now_utc.isoformat(),
                    'epoch_started_at_utc': epoch['started_at_utc'],
                    'deleted_manifests': 0,
                    'deleted_raw_files': 0,
                    'bytes_saved': 0,
                }
                _write_status(root, status)
                return status

            refs = _current_snapshot_refs(root) + _vintage_refs(root) + _pin_refs(root)
            protected = _validate_protected_refs(root, refs)
            receipts = _all_receipts(root)
            receipt_paths = {record['path'] for record in receipts}
            missing = protected - receipt_paths
            if missing:
                raise RosstatPollingRetentionError('protected Rosstat receipt missing from governed inventory')

            epoch_start = _utc(epoch['started_at_utc'], 'retention epoch started_at_utc')
            candidates = [record for record in receipts
                          if record['received_at'] >= epoch_start and record['path'] not in protected]
            candidates.sort(key=lambda item: (item['received_at'], str(item['path'])))
            candidates = candidates[:MAX_DELETE_MANIFESTS_PER_CALL]
            delete_paths = {record['path'] for record in candidates}
            remaining_raw = {record['raw_sha256'] for record in receipts if record['path'] not in delete_paths}
            raw_targets: dict[Path, str] = {}
            for record in candidates:
                raw_sha = record['raw_sha256']
                if raw_sha in remaining_raw:
                    continue
                directory = record['path'].parent
                for suffix in ('.html', '.html.gz'):
                    candidate = directory / (raw_sha + suffix)
                    if candidate.exists():
                        raw_targets[candidate] = raw_sha

            bytes_saved = 0
            for record in candidates:
                if record['path'].is_symlink() or not record['path'].is_file():
                    raise RosstatPollingRetentionError('Rosstat receipt changed during GC planning')
                bytes_saved += record['path'].stat().st_size
            for path, raw_sha in raw_targets.items():
                bytes_saved += _verify_raw_for_delete(path, raw_sha)

            for record in candidates:
                record['path'].unlink()
            for path in raw_targets:
                path.unlink()

            status = {
                'schema_version': SCHEMA_VERSION,
                'policy': GC_POLICY,
                'status': 'READY',
                'checked_at_utc': now_utc.isoformat(),
                'epoch_started_at_utc': epoch['started_at_utc'],
                'protected_manifests': len(protected),
                'inventory_manifests': len(receipts),
                'eligible_unreferenced_manifests': len([record for record in receipts
                    if record['received_at'] >= epoch_start and record['path'] not in protected]),
                'deleted_manifests': len(candidates),
                'deleted_raw_files': len(raw_targets),
                'bytes_saved': bytes_saved,
                'delete_limit': MAX_DELETE_MANIFESTS_PER_CALL,
                'pre_epoch_evidence_preserved': True,
            }
            _write_status(root, status)
            return status
        except (RosstatPollingRetentionError, OSError, UnicodeError, json.JSONDecodeError) as exc:
            status = {
                'schema_version': SCHEMA_VERSION,
                'policy': GC_POLICY,
                'status': 'BLOCKED',
                'checked_at_utc': now_utc.isoformat(),
                'deleted_manifests': 0,
                'deleted_raw_files': 0,
                'bytes_saved': 0,
                'reason': str(exc),
            }
            _write_status(root, status)
            return status


def _derive_root_from_refs(refs: list[tuple[str, str]]) -> Path:
    roots: set[Path] = set()
    for path_value, _ in refs:
        path = Path(path_value)
        if not path.is_absolute() or path.is_symlink() or not path.is_file():
            raise RosstatPollingRetentionError('frozen export Rosstat manifest must exist as a regular absolute file')
        matched = False
        for relative in EVIDENCE_RELATIVE_DIRS:
            depth = len(relative.parts)
            if len(path.parents) <= depth:
                continue
            candidate_root = path.parents[depth]
            if path.parent == candidate_root / relative:
                roots.add(candidate_root)
                matched = True
                break
        if not matched:
            raise RosstatPollingRetentionError('frozen export Rosstat manifest outside governed layout')
    if len(roots) != 1:
        raise RosstatPollingRetentionError('frozen export Rosstat evidence must share one data root')
    return _root(next(iter(roots)))


def pin_snapshot(snapshot: dict, *, pin_id: str, created_at) -> dict | None:
    """Pin Rosstat evidence referenced by a frozen factual input snapshot.

    Snapshots without Rosstat evidence require no pin.  A pin is immutable and is
    consumed by post-publish GC before any referenced receipt can be removed.
    """
    if not isinstance(snapshot, dict):
        raise RosstatPollingRetentionError('frozen snapshot must be an object')
    if not isinstance(pin_id, str) or not _HASH.fullmatch(pin_id):
        raise RosstatPollingRetentionError('frozen evidence pin id must be sha256')
    refs = _snapshot_refs(snapshot)
    if not refs:
        return None
    root = _derive_root_from_refs(refs)
    created = _utc(created_at, 'pin created_at')
    with _lock(root):
        protected = _validate_protected_refs(root, refs)
        manifests = []
        for path_value, digest in refs:
            path = Path(path_value).resolve(strict=True)
            if path not in protected:
                raise RosstatPollingRetentionError('frozen export manifest failed protection validation')
            manifests.append({'path': str(path), 'sha256': digest})
        unique = {(item['path'], item['sha256']): item for item in manifests}
        payload = {
            'schema_version': PIN_SCHEMA_VERSION,
            'pin_id': pin_id,
            'kind': 'rub_factual_release_input_snapshot',
            'created_at_utc': created.isoformat(),
            'manifests': [unique[key] for key in sorted(unique)],
        }
        directory = _ensure_dir(root / PIN_RELATIVE_DIR, root)
        path = directory / (pin_id + '.json')
        digest = _write_immutable(path, payload)
        return {'pin_id': pin_id, 'pin_path': str(path), 'pin_sha256': digest,
                'manifest_count': len(payload['manifests'])}
