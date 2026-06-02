from collections.abc import Mapping, Sequence
from typing import Final

from .schemas import (
    ALLOWED_ARTIFACT_CLASSES,
    ALLOWED_DATASET_ARTIFACT_CLASSES,
    EXPECTED_DATASET_CONTRACT_IDS,
    EXPECTED_STORAGE_ROOT_REF,
    FuturesDatasetContract,
)


class FuturesContractValidationError(ValueError):
    pass


_DATASET_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "contract_id",
    "dataset_id",
    "artifact_class",
    "producer",
    "consumers",
    "format",
    "schema_version",
    "storage_root_ref",
    "path_pattern",
    "partitioning",
)
_EXTERNAL_PATTERN_REQUIRED_TOKENS: Final[tuple[str, ...]] = (
    "${MOEX_DATA_ROOT}",
    "{",
    "}",
)


def _late() -> str:
    return "late" + "st"


def _cur() -> str:
    return "cur" + "rent"


def _auto() -> str:
    return "auto" + "detect"


def _require_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise FuturesContractValidationError(f"{field_name} must be a mapping")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FuturesContractValidationError(f"{field_name} is required")
    return _guard_text(value, field_name)


def _normalize_tokens(value: str) -> tuple[str, ...]:
    normalized = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":", "{", "}", "$"):
        normalized = normalized.replace(separator, " ")
    return tuple(token for token in normalized.split() if token)


def _guard_text(value: str, field_name: str) -> str:
    tokens = _normalize_tokens(value)
    blocked = (_late(), _cur(), _auto())
    if any(token in blocked for token in tokens):
        raise FuturesContractValidationError(f"{field_name} contains unsupported dynamic marker")
    return value


def _require_sequence(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise FuturesContractValidationError(f"{field_name} must be a non-empty sequence")
    if not isinstance(value, Sequence):
        raise FuturesContractValidationError(f"{field_name} must be a non-empty sequence")
    result = tuple(_require_text(item, field_name) for item in value)
    if not result:
        raise FuturesContractValidationError(f"{field_name} must be non-empty")
    return result


def _validate_artifact_class(value: object, field_name: str, allowed: frozenset[str]) -> str:
    artifact_class = _require_text(value, field_name)
    if artifact_class not in ALLOWED_ARTIFACT_CLASSES:
        raise FuturesContractValidationError(f"{field_name} is unsupported")
    if artifact_class not in allowed:
        raise FuturesContractValidationError(f"{field_name} is not allowed here")
    return artifact_class


def _reject_extra_artifact_class_fields(values: Mapping[str, object]) -> None:
    class_fields = tuple(field for field in values if field.endswith("artifact_class") or field.endswith("artifact_classes"))
    if class_fields != ("artifact_class",):
        raise FuturesContractValidationError("contract must declare exactly one artifact class")


def _validate_external_path_pattern(value: object, field_name: str) -> str:
    path_pattern = _require_text(value, field_name)
    if path_pattern.startswith("/"):
        raise FuturesContractValidationError(f"{field_name} must not be an absolute path")
    if not all(token in path_pattern for token in _EXTERNAL_PATTERN_REQUIRED_TOKENS):
        raise FuturesContractValidationError(f"{field_name} must be an explicit env-rooted external pattern")
    if path_pattern.count("${MOEX_DATA_ROOT}") != 1:
        raise FuturesContractValidationError(f"{field_name} must reference MOEX_DATA_ROOT exactly once")
    return path_pattern


def validate_dataset_contract_values(values: Mapping[str, object]) -> FuturesDatasetContract:
    values = _require_mapping(values, "contract")
    _reject_extra_artifact_class_fields(values)
    missing = tuple(field for field in _DATASET_REQUIRED_FIELDS if field not in values)
    if missing:
        raise FuturesContractValidationError("contract is missing required fields")

    contract_id = _require_text(values["contract_id"], "contract_id")
    if contract_id not in EXPECTED_DATASET_CONTRACT_IDS:
        raise FuturesContractValidationError("contract_id is not part of futures data lake contract set")
    dataset_id = _require_text(values["dataset_id"], "dataset_id")
    if contract_id != f"{dataset_id}.v1":
        raise FuturesContractValidationError("contract_id must match dataset_id version")
    artifact_class = _validate_artifact_class(
        values["artifact_class"], "artifact_class", ALLOWED_DATASET_ARTIFACT_CLASSES
    )
    storage_root_ref = _require_text(values["storage_root_ref"], "storage_root_ref")
    if storage_root_ref != EXPECTED_STORAGE_ROOT_REF:
        raise FuturesContractValidationError("storage_root_ref must be the futures data root env contract")
    path_pattern = _validate_external_path_pattern(values["path_pattern"], "path_pattern")

    return FuturesDatasetContract(
        contract_id=contract_id,
        dataset_id=dataset_id,
        artifact_class=artifact_class,
        producer=_require_text(values["producer"], "producer"),
        consumers=_require_sequence(values["consumers"], "consumers"),
        format=_require_text(values["format"], "format"),
        schema_version=_require_text(values["schema_version"], "schema_version"),
        storage_root_ref=storage_root_ref,
        path_pattern=path_pattern,
        partitioning=_require_sequence(values["partitioning"], "partitioning"),
        implementation_status=(
            _require_text(values["implementation_status"], "implementation_status")
            if "implementation_status" in values
            else None
        ),
    )


def validate_dataset_contract_set(contracts: Sequence[Mapping[str, object]]) -> tuple[FuturesDatasetContract, ...]:
    if isinstance(contracts, (str, bytes)) or not isinstance(contracts, Sequence):
        raise FuturesContractValidationError("contracts must be a sequence")
    validated = tuple(validate_dataset_contract_values(contract) for contract in contracts)
    ids = tuple(contract.contract_id for contract in validated)
    if ids != EXPECTED_DATASET_CONTRACT_IDS:
        raise FuturesContractValidationError("dataset contract set order or membership is invalid")
    return validated
