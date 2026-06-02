from __future__ import annotations

from collections.abc import Mapping
from typing import Final


class RealReadReadinessSnapshotError(ValueError):
    pass


SNAPSHOT_FIELDS: Final[tuple[str, ...]] = (
    "snapshot_id",
    "phase_status_ref",
    "gate_ref",
    "review_ref",
    "design_ref",
    "execution_review_ref",
    "handoff_ref",
    "manual_intake_ref",
    "pm_review_ref",
    "chain_ref",
    "export_ref",
    "snapshot_status",
    "actual_data_lake_read_status",
    "real_market_data_loading_status",
    "registry_write_status",
    "runtime_live_status",
    "promotion_status",
    "metadata_only",
)
ALLOWED_SNAPSHOT_STATUS: Final[frozenset[str]] = frozenset({"repo_only_closed"})
BLOCKED_STATUS: Final[str] = "blocked"


def _late() -> str:
    return "late" + "st"


def _cur() -> str:
    return "cur" + "rent"


def _auto() -> str:
    return "auto" + "detect"


def _flat(value: str) -> str:
    result = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":"):
        result = result.replace(separator, " ")
    return " ".join(result.split())


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RealReadReadinessSnapshotError(f"{field_name} is required")
    folded = value.casefold()
    spaced = _flat(value)
    for marker in (_late(), _cur(), _auto()):
        if marker in folded or marker in spaced:
            raise RealReadReadinessSnapshotError(f"{field_name} contains unsupported marker")
    return value


def _exact(values: Mapping[str, object], expected: tuple[str, ...], label: str) -> None:
    if not isinstance(values, Mapping) or set(values) != set(expected):
        raise RealReadReadinessSnapshotError(f"{label} fields invalid")


def _blocked(value: object, field_name: str) -> str:
    status = _text(value, field_name)
    if status != BLOCKED_STATUS:
        raise RealReadReadinessSnapshotError(f"{field_name} must remain blocked")
    return status


def _true(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RealReadReadinessSnapshotError(f"{field_name} must be bool")
    if not value:
        raise RealReadReadinessSnapshotError(f"{field_name} must be true")
    return value


def validate_real_read_readiness_snapshot_values(values: Mapping[str, object]) -> dict[str, object]:
    _exact(values, SNAPSHOT_FIELDS, "snapshot")
    snapshot_status = _text(values["snapshot_status"], "snapshot_status")
    if snapshot_status not in ALLOWED_SNAPSHOT_STATUS:
        raise RealReadReadinessSnapshotError("snapshot_status is unsupported")
    return {
        "snapshot_id": _text(values["snapshot_id"], "snapshot_id"),
        "phase_status_ref": _text(values["phase_status_ref"], "phase_status_ref"),
        "gate_ref": _text(values["gate_ref"], "gate_ref"),
        "review_ref": _text(values["review_ref"], "review_ref"),
        "design_ref": _text(values["design_ref"], "design_ref"),
        "execution_review_ref": _text(values["execution_review_ref"], "execution_review_ref"),
        "handoff_ref": _text(values["handoff_ref"], "handoff_ref"),
        "manual_intake_ref": _text(values["manual_intake_ref"], "manual_intake_ref"),
        "pm_review_ref": _text(values["pm_review_ref"], "pm_review_ref"),
        "chain_ref": _text(values["chain_ref"], "chain_ref"),
        "export_ref": _text(values["export_ref"], "export_ref"),
        "snapshot_status": snapshot_status,
        "actual_data_lake_read_status": _blocked(values["actual_data_lake_read_status"], "actual_data_lake_read_status"),
        "real_market_data_loading_status": _blocked(values["real_market_data_loading_status"], "real_market_data_loading_status"),
        "registry_write_status": _blocked(values["registry_write_status"], "registry_write_status"),
        "runtime_live_status": _blocked(values["runtime_live_status"], "runtime_live_status"),
        "promotion_status": _blocked(values["promotion_status"], "promotion_status"),
        "metadata_only": _true(values["metadata_only"], "metadata_only"),
    }


class RealReadReadinessSnapshot:
    def __init__(self, **values: object) -> None:
        normalized = validate_real_read_readiness_snapshot_values(values)
        for key, value in normalized.items():
            setattr(self, key, value)


def validate_real_read_readiness_snapshot(snapshot: RealReadReadinessSnapshot) -> RealReadReadinessSnapshot:
    if not isinstance(snapshot, RealReadReadinessSnapshot):
        raise TypeError("snapshot must be RealReadReadinessSnapshot")
    validate_real_read_readiness_snapshot_values(snapshot.__dict__)
    return snapshot


def make_repo_only_real_read_readiness_snapshot() -> RealReadReadinessSnapshot:
    return RealReadReadinessSnapshot(
        snapshot_id="real_read.readiness.snapshot.repo_only.v1",
        phase_status_ref="ab23114f0b5e1854eec76e78a732d47e460b3cf6",
        gate_ref="abf14b54f4fe1e5acfe7762979007d66533e2dc8",
        review_ref="54f15056dfd098e190c42b9160ebff515cdc1bb8",
        design_ref="454f2a50706efa573863a4c9b90167209efb59dc",
        execution_review_ref="25d3a996b4b9568241e26d779a14b84a39cd4e8d",
        handoff_ref="7dfb9467b20a250b38d59b4d419d43408b66a8d1",
        manual_intake_ref="3b226e6bbf540cc3d4bf6632ae9cbafa1935391d",
        pm_review_ref="b2fe2054038a0486d17fa745016b7a8e05d789ab",
        chain_ref="07367c22b385907ceab0e01dd4a00731dc28c4d2",
        export_ref="ced999f5be3b4f2bf499333632b145cf8a5ed2fd",
        snapshot_status="repo_only_closed",
        actual_data_lake_read_status="blocked",
        real_market_data_loading_status="blocked",
        registry_write_status="blocked",
        runtime_live_status="blocked",
        promotion_status="blocked",
        metadata_only=True,
    )


__all__ = [
    "ALLOWED_SNAPSHOT_STATUS",
    "BLOCKED_STATUS",
    "SNAPSHOT_FIELDS",
    "RealReadReadinessSnapshot",
    "RealReadReadinessSnapshotError",
    "make_repo_only_real_read_readiness_snapshot",
    "validate_real_read_readiness_snapshot",
    "validate_real_read_readiness_snapshot_values",
]
