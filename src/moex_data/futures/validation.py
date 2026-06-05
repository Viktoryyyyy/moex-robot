from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

APPROVED_TIMEFRAMES: Final[tuple[str, ...]] = ("5m", "10m", "15m", "30m", "1h", "4h", "1D", "1W")
DERIVED_TIMEFRAMES: Final[tuple[str, ...]] = ("10m", "15m", "30m", "1h", "4h", "1D", "1W")
REQUIRED_IDENTIFIER_FIELDS: Final[tuple[str, ...]] = ("FAMILY", "SECID", "BOARD", "MARKET", "SERIES_TYPE")
FORBIDDEN_PATH_MARKERS: Final[tuple[str, ...]] = ("latest", "current", "autodetect")
FORBIDDEN_GLOB_MARKERS: Final[tuple[str, ...]] = ("*", "?", "[")
ALLOWED_SERIES_TYPES: Final[frozenset[str]] = frozenset({"native", "continuous"})


class FuturesValidationError(ValueError):
    pass


@dataclass(frozen=True)
class FuturesInstrumentIdentity:
    family: str
    secid: str
    board: str
    market: str
    series_type: str


@dataclass(frozen=True)
class FuturesDatasetContract:
    contract_id: str
    dataset_id: str
    artifact_class: str
    storage_root_ref: str
    path_pattern: str
    partitioning: tuple[str, ...]
    timeframe: str | None
    allowed_timeframes: tuple[str, ...]
    required_identifier_fields: tuple[str, ...]
    parent_manifest_required: bool


@dataclass(frozen=True)
class ContinuousSeriesPolicy:
    schema_id: str
    continuous_series_status: str
    materialization_allowed: bool
    roll_policy_required: bool
    expiration_family_mapping_required: bool


def require_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise FuturesValidationError(field_name + " must be a mapping")
    return value


def require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FuturesValidationError(field_name + " is required")
    return guard_text(value.strip(), field_name)


def require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise FuturesValidationError(field_name + " must be boolean")
    return value


def require_non_negative_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise FuturesValidationError(field_name + " must be a non-negative integer")
    return value


def require_text_sequence(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise FuturesValidationError(field_name + " must be a sequence")
    result = tuple(require_text(item, field_name) for item in value)
    if not result:
        raise FuturesValidationError(field_name + " must be non-empty")
    return result


def _tokens(value: str) -> tuple[str, ...]:
    normalized = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":", "{", "}", "$", "=", " "):
        normalized = normalized.replace(separator, " ")
    return tuple(token for token in normalized.split() if token)


def guard_text(value: str, field_name: str) -> str:
    tokens = _tokens(value)
    if any(token in FORBIDDEN_PATH_MARKERS for token in tokens):
        raise FuturesValidationError(field_name + " contains forbidden dynamic marker")
    if any(marker in value for marker in FORBIDDEN_GLOB_MARKERS):
        raise FuturesValidationError(field_name + " contains forbidden glob marker")
    return value


def guard_external_pattern(path_pattern: object, field_name: str = "path_pattern") -> str:
    value = require_text(path_pattern, field_name)
    if value.startswith("/"):
        raise FuturesValidationError(field_name + " must not be absolute")
    if value.count("${MOEX_DATA_ROOT}") != 1:
        raise FuturesValidationError(field_name + " must reference MOEX_DATA_ROOT exactly once")
    if not value.startswith("${MOEX_DATA_ROOT}/"):
        raise FuturesValidationError(field_name + " must stay under MOEX_DATA_ROOT")
    return value


def validate_identifier_values(values: Mapping[str, object]) -> FuturesInstrumentIdentity:
    values = require_mapping(values, "identifier")
    missing = tuple(field for field in REQUIRED_IDENTIFIER_FIELDS if field not in values)
    if missing:
        raise FuturesValidationError("missing identifier field: " + missing[0])
    identity = FuturesInstrumentIdentity(
        family=require_text(values["FAMILY"], "FAMILY"),
        secid=require_text(values["SECID"], "SECID"),
        board=require_text(values["BOARD"], "BOARD"),
        market=require_text(values["MARKET"], "MARKET"),
        series_type=require_text(values["SERIES_TYPE"], "SERIES_TYPE"),
    )
    if identity.series_type not in ALLOWED_SERIES_TYPES:
        raise FuturesValidationError("SERIES_TYPE is unsupported")
    return identity


def validate_timeframe(value: object, *, derived_only: bool = False) -> str:
    timeframe = require_text(value, "timeframe")
    allowed = DERIVED_TIMEFRAMES if derived_only else APPROVED_TIMEFRAMES
    if timeframe not in allowed:
        raise FuturesValidationError("unsupported timeframe")
    return timeframe


def validate_dataset_contract_values(values: Mapping[str, object]) -> FuturesDatasetContract:
    values = require_mapping(values, "dataset_contract")
    contract_id = require_text(values.get("contract_id"), "contract_id")
    dataset_id = require_text(values.get("dataset_id"), "dataset_id")
    artifact_class = require_text(values.get("artifact_class"), "artifact_class")
    if artifact_class != "external_pattern":
        raise FuturesValidationError("dataset artifact_class must be external_pattern")
    storage_root_ref = require_text(values.get("storage_root_ref"), "storage_root_ref")
    if storage_root_ref != "MOEX_DATA_ROOT":
        raise FuturesValidationError("storage_root_ref must be MOEX_DATA_ROOT")
    path_pattern = guard_external_pattern(values.get("path_pattern"))
    partitioning = require_text_sequence(values.get("partitioning"), "partitioning")
    required_identifiers = require_text_sequence(values.get("required_identifier_fields"), "required_identifier_fields")
    if tuple(required_identifiers) != REQUIRED_IDENTIFIER_FIELDS:
        raise FuturesValidationError("required identifier fields must match universal futures identity")
    timeframe = None
    if "timeframe" in values:
        timeframe = validate_timeframe(values.get("timeframe"))
    allowed_timeframes = ()
    if "allowed_timeframes" in values:
        allowed_timeframes = require_text_sequence(values.get("allowed_timeframes"), "allowed_timeframes")
        for item in allowed_timeframes:
            validate_timeframe(item, derived_only=True)
        if "10m" not in allowed_timeframes:
            raise FuturesValidationError("10m timeframe must be declared")
    parent_manifest_required = bool(values.get("parent_manifest_required", False))
    return FuturesDatasetContract(
        contract_id=contract_id,
        dataset_id=dataset_id,
        artifact_class=artifact_class,
        storage_root_ref=storage_root_ref,
        path_pattern=path_pattern,
        partitioning=partitioning,
        timeframe=timeframe,
        allowed_timeframes=allowed_timeframes,
        required_identifier_fields=required_identifiers,
        parent_manifest_required=parent_manifest_required,
    )


def validate_continuous_series_policy_values(values: Mapping[str, object]) -> ContinuousSeriesPolicy:
    values = require_mapping(values, "continuous_series_policy")
    schema_id = require_text(values.get("schema_id"), "schema_id")
    status = require_text(values.get("continuous_series_status"), "continuous_series_status")
    materialization_allowed = require_bool(values.get("materialization_allowed"), "materialization_allowed")
    roll_policy_required = require_bool(values.get("roll_policy_required"), "roll_policy_required")
    expiration_required = require_bool(
        values.get("expiration_family_mapping_required"), "expiration_family_mapping_required"
    )
    if status != "blocked_placeholder":
        raise FuturesValidationError("continuous series must remain blocked_placeholder")
    if materialization_allowed:
        raise FuturesValidationError("continuous materialization is blocked")
    if not roll_policy_required or not expiration_required:
        raise FuturesValidationError("continuous series requires roll policy and expiration mapping before unblock")
    return ContinuousSeriesPolicy(
        schema_id=schema_id,
        continuous_series_status=status,
        materialization_allowed=materialization_allowed,
        roll_policy_required=roll_policy_required,
        expiration_family_mapping_required=expiration_required,
    )


def reject_continuous_without_complete_roll_policy(identity: FuturesInstrumentIdentity, policy: Mapping[str, object]) -> None:
    if identity.series_type != "continuous":
        return
    values = require_mapping(policy, "continuous_roll_policy")
    if values.get("roll_policy_contract_ref") and values.get("expiration_family_mapping_contract_ref"):
        return
    raise FuturesValidationError("continuous series requires complete roll policy")


def validate_no_dynamic_scan(values: Mapping[str, object]) -> None:
    values = require_mapping(values, "config")
    if values.get("dynamic_scan_allowed") is not False:
        raise FuturesValidationError("dynamic scan/autodiscovery must be disabled")


def clean_mapping_text(values: Mapping[str, object]) -> None:
    for key, value in values.items():
        guard_text(str(key), "key")
        if isinstance(value, Mapping):
            clean_mapping_text(value)
        elif isinstance(value, str):
            guard_text(value, str(key))
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            for item in value:
                if isinstance(item, Mapping):
                    clean_mapping_text(item)
                elif isinstance(item, str):
                    guard_text(item, str(key))
        elif value is None or isinstance(value, (bool, int, float)):
            continue
        else:
            raise FuturesValidationError("unsupported config value")
