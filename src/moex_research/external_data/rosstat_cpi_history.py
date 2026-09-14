"""Point-in-time reader for normalized Rosstat CPI vintages."""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from pathlib import Path

from . import rosstat_cpi_vintages as store


def _series_dir(root, series_id):
    root = store._root(root)
    if series_id not in store.SERIES:
        raise store.RosstatVintageError('unsupported Rosstat CPI series')
    path = root / store.DATASET_RELATIVE_ROOT / ('series_id=' + series_id)
    if not path.exists():
        return root, None
    if path.is_symlink() or not path.is_dir() or not path.resolve(strict=True).is_relative_to(root):
        raise store.RosstatVintageError('invalid Rosstat CPI series directory')
    return root, path


def _with_ref(path, record):
    result = deepcopy(record)
    result['vintage_path'] = str(path)
    result['vintage_sha256'] = sha256(path.read_bytes()).hexdigest()
    return result


def _observation_sort_key(record):
    observation = record.get('observation')
    if not isinstance(observation, dict):
        raise store.RosstatVintageError('Rosstat vintage observation must be an object')
    if record.get('series_id') == 'ROSSTAT_WEEKLY_CPI_ESTIMATE':
        value = observation.get('observation_end')
    elif record.get('series_id') == 'ROSSTAT_MONTHLY_CPI':
        value = observation.get('observation_month')
    else:
        raise store.RosstatVintageError('unsupported Rosstat CPI series')
    if not isinstance(value, str) or not value:
        raise store.RosstatVintageError('Rosstat vintage observation sort key missing')
    return value


def history(root, *, series_id, observation_key):
    """Return all immutable revisions for one observation in revision order."""
    root, series_dir = _series_dir(root, series_id)
    if series_dir is None:
        return []
    if not isinstance(observation_key, str) or not observation_key or '/' in observation_key or '..' in observation_key:
        raise store.RosstatVintageError('invalid Rosstat observation key')
    observation_dir = series_dir / ('observation_key=' + observation_key)
    if not observation_dir.exists():
        return []
    if observation_dir.is_symlink() or not observation_dir.is_dir() or not observation_dir.resolve(strict=True).is_relative_to(root):
        raise store.RosstatVintageError('invalid Rosstat observation directory')
    return [_with_ref(path, record) for _, path, record in
            store._vintages(observation_dir, series_id=series_id, observation_key=observation_key)]


def as_of(root, *, series_id, observation_key, as_of):
    """Return the latest revision actually available by ``as_of`` for one observation."""
    cutoff = store._utc(as_of, 'as_of')
    eligible = [record for record in history(root, series_id=series_id, observation_key=observation_key)
                if store._utc(record['available_at'], 'available_at') <= cutoff]
    return eligible[-1] if eligible else None


def latest_as_of(root, *, series_id, as_of):
    """Return newest observation and its latest revision available at the PIT cutoff."""
    cutoff = store._utc(as_of, 'as_of')
    root, series_dir = _series_dir(root, series_id)
    if series_dir is None:
        return None
    candidates = []
    for path in series_dir.glob('observation_key=*'):
        if path.is_symlink() or not path.is_dir() or not path.resolve(strict=True).is_relative_to(root):
            raise store.RosstatVintageError('invalid Rosstat observation directory')
        key = path.name.removeprefix('observation_key=')
        records = history(root, series_id=series_id, observation_key=key)
        eligible = [record for record in records if store._utc(record['available_at'], 'available_at') <= cutoff]
        if eligible:
            candidates.append(eligible[-1])
    if not candidates:
        return None
    candidates.sort(key=lambda record: (_observation_sort_key(record), record['revision_seq']))
    return candidates[-1]
