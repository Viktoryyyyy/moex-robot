from collections.abc import Mapping, Sequence
from typing import Final

from .contracts import _guard_text, _require_text
from .schemas import EXPECTED_DATASET_CONTRACT_IDS, FuturesRefreshManifest


class FuturesManifestValidationError(ValueError):
    pass


_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "run_id",
    "run_date",
    "dataset_contract_refs",
    "instrument_scope",
    "source_scope",
    "partitions_written",
    "partitions_skipped",
    "quality_report_ref",
    "accepted_manifest_ref",
    "refresh_status",
)
_ALLOWED_STATUS: Final[frozenset[str]] = frozenset({"succeeded", "failed", "partial"})


def _require_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise FuturesManifestValidationError(f"{field_name} must be a mapping")
    return value


def _require_sequence(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise FuturesManifestValidationError(f"{field_name} must be a sequence")
    return tuple(_guard_text(_require_text(item, field_name), field_name) for item in value)


def _require_pointer_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FuturesManifestValidationError(f"{field_name} must be text")
    return value.strip()


def validate_refresh_manifest_values(values: Mapping[str, object]) -> FuturesRefreshManifest:
    values = _require_mapping(values, "manifest")
    missing = tuple(field for field in _REQUIRED_FIELDS if field not in values)
    if missing:
        raise FuturesManifestValidationError("manifest is missing required fields")
    dataset_contract_refs = _require_sequence(values["dataset_contract_refs"], "dataset_contract_refs")
    if dataset_contract_refs != EXPECTED_DATASET_CONTRACT_IDS:
        raise FuturesManifestValidationError("manifest dataset contracts are invalid")
    refresh_status = _require_text(values["refresh_status"], "refresh_status")
    if refresh_status not in _ALLOWED_STATUS:
        raise FuturesManifestValidationError("refresh_status is unsupported")
    return FuturesRefreshManifest(
        run_id=_require_text(values["run_id"], "run_id"),
        run_date=_require_text(values["run_date"], "run_date"),
        dataset_contract_refs=dataset_contract_refs,
        partitions_written=_require_sequence(values["partitions_written"], "partitions_written"),
        partitions_skipped=_require_sequence(values["partitions_skipped"], "partitions_skipped"),
        quality_report_ref=_require_text(values["quality_report_ref"], "quality_report_ref"),
        refresh_status=refresh_status,
        instrument_scope=_require_sequence(values["instrument_scope"], "instrument_scope"),
        source_scope=_require_sequence(values["source_scope"], "source_scope"),
        accepted_manifest_ref=_require_pointer_text(values["accepted_manifest_ref"], "accepted_manifest_ref"),
    )
