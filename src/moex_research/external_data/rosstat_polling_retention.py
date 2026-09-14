"""Bound Rosstat polling storage while preserving immutable semantic-vintage evidence."""
from __future__ import annotations

from copy import deepcopy
import gzip
from hashlib import sha256
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile


class RosstatPollingRetentionError(ValueError):
    pass


def _read_object(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise RosstatPollingRetentionError('Rosstat retention artifact must be a regular file')
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RosstatPollingRetentionError('invalid Rosstat retention JSON') from exc
    if not isinstance(value, dict):
        raise RosstatPollingRetentionError('Rosstat retention JSON must be an object')
    return value


def _atomic_write(path: Path, payload: dict) -> None:
    if path.exists() and path.is_symlink():
        raise RosstatPollingRetentionError('Rosstat current pointer symlink refused')
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                         separators=(',', ':'), allow_nan=False).encode() + b'\n'
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


def _manifest_receipt(capture: dict) -> tuple[Path, dict]:
    path_value = capture.get('manifest_path')
    digest = capture.get('manifest_sha256')
    raw_sha = capture.get('raw_sha256')
    if not all(isinstance(value, str) and value for value in (path_value, digest, raw_sha)):
        raise RosstatPollingRetentionError('capture identity incomplete')
    manifest = Path(path_value)
    if manifest.is_symlink() or not manifest.is_file() or manifest.name != digest + '.json':
        raise RosstatPollingRetentionError('capture manifest identity mismatch')
    encoded = manifest.read_bytes()
    if sha256(encoded).hexdigest() != digest:
        raise RosstatPollingRetentionError('capture manifest hash mismatch')
    try:
        receipt = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RosstatPollingRetentionError('capture manifest JSON invalid') from exc
    if not isinstance(receipt, dict) or receipt.get('raw_sha256') != raw_sha:
        raise RosstatPollingRetentionError('capture receipt/raw identity mismatch')
    return manifest, receipt


def _raw_referenced(directory: Path, raw_sha: str) -> bool:
    for path in directory.glob('*.json'):
        if path.is_symlink() or not path.is_file():
            continue
        try:
            encoded = path.read_bytes()
            if sha256(encoded).hexdigest() != path.stem:
                continue
            value = json.loads(encoded)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(value, dict) and value.get('raw_sha256') == raw_sha:
            return True
    return False


def _discard_capture(capture: dict, *, protected_manifests: set[str]) -> None:
    manifest, receipt = _manifest_receipt(capture)
    if str(manifest) in protected_manifests:
        return
    raw_sha = receipt['raw_sha256']
    directory = manifest.parent
    manifest.unlink()

    raw_path = directory / (raw_sha + '.html')
    gzip_path = directory / (raw_sha + '.html.gz')
    referenced = _raw_referenced(directory, raw_sha)
    if raw_path.is_symlink() or gzip_path.is_symlink():
        raise RosstatPollingRetentionError('Rosstat raw symlink refused during retention')

    if referenced:
        # capture() may recreate an uncompressed body whose canonical copy was already gzipped.
        if raw_path.is_file() and gzip_path.is_file():
            try:
                raw = raw_path.read_bytes()
                unpacked = gzip.decompress(gzip_path.read_bytes())
            except (OSError, EOFError, gzip.BadGzipFile) as exc:
                raise RosstatPollingRetentionError('canonical gzip verification failed') from exc
            if sha256(raw).hexdigest() != raw_sha or unpacked != raw:
                raise RosstatPollingRetentionError('canonical gzip/raw mismatch')
            raw_path.unlink()
        return

    if raw_path.is_file():
        raw = raw_path.read_bytes()
        if sha256(raw).hexdigest() != raw_sha:
            raise RosstatPollingRetentionError('orphan raw hash mismatch')
        raw_path.unlink()
    if gzip_path.is_file():
        try:
            raw = gzip.decompress(gzip_path.read_bytes())
        except (OSError, EOFError, gzip.BadGzipFile) as exc:
            raise RosstatPollingRetentionError('orphan gzip verification failed') from exc
        if sha256(raw).hexdigest() != raw_sha:
            raise RosstatPollingRetentionError('orphan gzip hash mismatch')
        gzip_path.unlink()


def _canonical_evidence(reference: dict) -> tuple[dict, dict]:
    path_value = reference.get('vintage_path')
    if not isinstance(path_value, str) or not path_value:
        raise RosstatPollingRetentionError('vintage reference path missing')
    artifact = _read_object(Path(path_value))
    if artifact.get('vintage_id') != reference.get('vintage_id'):
        raise RosstatPollingRetentionError('vintage identity mismatch during retention')
    provenance = artifact.get('provenance')
    if not isinstance(provenance, dict):
        raise RosstatPollingRetentionError('vintage canonical provenance missing')
    required = ('index_manifest_path', 'index_manifest_sha256',
                'document_manifest_path', 'document_manifest_sha256')
    if any(not isinstance(provenance.get(key), str) or not provenance[key] for key in required):
        raise RosstatPollingRetentionError('vintage canonical replay evidence incomplete')
    refs = {key: provenance[key] for key in required}
    return artifact, refs


def finalize(*, result: dict, reference: dict, captures: tuple[dict, ...], replay,
             now, operational_fields: tuple[str, ...]) -> dict:
    """Discard a semantically redundant poll only when canonical replay is equivalent.

    New observations/revisions always retain their acquisition evidence. A repeated poll
    may be collapsed only when the immutable vintage is unchanged and the selected
    release/calendar semantics are also unchanged. Fresh verification time remains in
    the mutable pointer/component while PIT availability remains on the immutable vintage.
    """
    current = deepcopy(result)
    ref = deepcopy(reference)
    current['last_verified_at'] = ref['last_verified_at']
    current['vintage'] = ref

    if ref.get('last_verified_at') == ref.get('available_at'):
        ref['verification_status'] = 'NEW_SEMANTIC_VINTAGE'
        current['vintage'] = ref
        return current

    artifact, canonical_refs = _canonical_evidence(ref)
    canonical = replay(canonical_refs, now=now, max_receipt_age_seconds=None)
    if any(current.get(field) != canonical.get(field) for field in operational_fields):
        ref['verification_status'] = 'SEMANTIC_DUPLICATE_OPERATIONAL_CHANGE_RETAINED'
        current['vintage'] = ref
        return current

    canonical_provenance = deepcopy(artifact['provenance'])
    ref['latest_provenance'] = canonical_provenance
    ref['last_verification'] = {
        'source_url': current.get('source_url'),
        'raw_sha256': current.get('raw_sha256'),
        'verified_at': ref['last_verified_at'],
        'retained_polling_evidence': False,
    }
    ref['verification_status'] = 'SEMANTIC_DUPLICATE_COLLAPSED'

    current_pointer = Path(ref['vintage_path']).parents[1] / 'current.json'
    _atomic_write(current_pointer, ref)

    protected = {
        canonical_refs['index_manifest_path'],
        canonical_refs['document_manifest_path'],
    }
    for capture in captures:
        _discard_capture(capture, protected_manifests=protected)

    canonical['last_verified_at'] = ref['last_verified_at']
    canonical['vintage'] = ref
    return canonical
