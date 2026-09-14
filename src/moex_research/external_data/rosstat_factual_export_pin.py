"""Bridge frozen factual exports to Rosstat GC pins without claiming external fixtures."""
from pathlib import Path

from . import rosstat_polling_retention as retention


def _is_governed_manifest_path(path_value: str) -> bool:
    path = Path(path_value)
    if not path.is_absolute():
        return False
    for relative in retention.EVIDENCE_RELATIVE_DIRS:
        depth = len(relative.parts)
        if len(path.parents) > depth:
            root = path.parents[depth]
            if path.parent == root / relative:
                return True
    return False


def pin(snapshot: dict, *, pin_id: str, created_at):
    """Pin only evidence inside the retention subsystem's governed layout.

    Offline/test snapshots can legitimately point at receipts in temporary custom
    directories.  GC never scans those directories, so they need no retention pin.
    A snapshot mixing governed and external Rosstat evidence is ambiguous and fails
    closed rather than silently leaving governed evidence unpinned.
    """
    refs = retention._snapshot_refs(snapshot)
    if not refs:
        return None
    flags = [_is_governed_manifest_path(path_value) for path_value, _ in refs]
    if all(flags):
        return retention.pin_snapshot(snapshot, pin_id=pin_id, created_at=created_at)
    if any(flags):
        raise retention.RosstatPollingRetentionError(
            'frozen export mixes governed and external Rosstat evidence')
    return None
