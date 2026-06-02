import ast
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from .contracts import FuturesContractValidationError, _guard_text, _require_text
from .schemas import (
    ALLOWED_ARTIFACT_CLASSES,
    ALLOWED_CONFIG_ARTIFACT_CLASSES,
    ALLOWED_EXTERNAL_ROOT_ARTIFACT_CLASSES,
    EXPECTED_CONFIG_ID,
    EXPECTED_CONFIG_PATH,
    EXPECTED_DATASET_CONTRACT_IDS,
    EXPECTED_DATASET_CONTRACT_PATHS,
    EXPECTED_STORAGE_ROOT_REF,
    FuturesDataLakeConfig,
)


class FuturesConfigValidationError(ValueError):
    pass


_CONFIG_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "config_id",
    "artifact_class",
    "repo_path",
    "external_storage_root",
    "dataset_contract_refs",
    "artifact_class_index",
    "blocked_contracts",
)


def _as_config_error(exc: Exception) -> FuturesConfigValidationError:
    return FuturesConfigValidationError(str(exc))


def _require_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise FuturesConfigValidationError(f"{field_name} must be a mapping")
    return value


def _require_sequence(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise FuturesConfigValidationError(f"{field_name} must be a sequence")
    result = tuple(_require_text(item, field_name) for item in value)
    if not result:
        raise FuturesConfigValidationError(f"{field_name} must be non-empty")
    return result


def _validate_artifact_class(value: object, field_name: str, allowed: frozenset[str]) -> str:
    artifact_class = _require_text(value, field_name)
    if artifact_class not in ALLOWED_ARTIFACT_CLASSES:
        raise FuturesConfigValidationError(f"{field_name} is unsupported")
    if artifact_class not in allowed:
        raise FuturesConfigValidationError(f"{field_name} is not allowed here")
    return artifact_class


def _validate_external_root(value: object) -> str:
    root = _require_mapping(value, "external_storage_root")
    artifact_class = _validate_artifact_class(
        root.get("artifact_class"), "external_storage_root.artifact_class", ALLOWED_EXTERNAL_ROOT_ARTIFACT_CLASSES
    )
    if artifact_class != "env_contract":
        raise FuturesConfigValidationError("external root must be env_contract")
    env_var = _require_text(root.get("env_var"), "external_storage_root.env_var")
    if env_var != EXPECTED_STORAGE_ROOT_REF:
        raise FuturesConfigValidationError("external root env var is invalid")
    if root.get("hardcoded_server_path_allowed") is not False:
        raise FuturesConfigValidationError("hardcoded path dependency must be rejected")
    return env_var


def _validate_artifact_class_index(value: object) -> dict[str, str]:
    mapping = dict(_require_mapping(value, "artifact_class_index"))
    expected_keys = set(EXPECTED_DATASET_CONTRACT_IDS).union({EXPECTED_CONFIG_ID, "moex_data_root"})
    if set(mapping) != expected_keys:
        raise FuturesConfigValidationError("artifact_class_index membership is invalid")
    result: dict[str, str] = {}
    for key, item in mapping.items():
        guarded_key = _guard_text(str(key), "artifact_class_index.key")
        result[guarded_key] = _validate_artifact_class(item, "artifact_class_index.value", ALLOWED_ARTIFACT_CLASSES)
    return result


def _validate_path_rules(value: object) -> None:
    rules = _require_mapping(value, "path_rules")
    if rules.get("external_root_source") != "env_contract":
        raise FuturesConfigValidationError("external root source must be env_contract")
    if rules.get("hardcoded_server_path_allowed") is not False:
        raise FuturesConfigValidationError("hardcoded path dependency must be rejected")
    if rules.get("implicit_file_selection_allowed") is not False:
        raise FuturesConfigValidationError("implicit file selection must be rejected")


def validate_futures_data_lake_config_values(values: Mapping[str, object]) -> FuturesDataLakeConfig:
    values = _require_mapping(values, "config")
    missing = tuple(field for field in _CONFIG_REQUIRED_FIELDS if field not in values)
    if missing:
        raise FuturesConfigValidationError("config is missing required fields")

    try:
        config_id = _require_text(values["config_id"], "config_id")
        artifact_class = _validate_artifact_class(values["artifact_class"], "artifact_class", ALLOWED_CONFIG_ARTIFACT_CLASSES)
        repo_path = _require_text(values["repo_path"], "repo_path")
        if repo_path.startswith("/"):
            raise FuturesConfigValidationError("repo_path must be relative")
        dataset_contract_refs = _require_sequence(values["dataset_contract_refs"], "dataset_contract_refs")
        blocked_contracts = _require_sequence(values["blocked_contracts"], "blocked_contracts")
    except FuturesContractValidationError as exc:
        raise _as_config_error(exc) from exc

    if config_id != EXPECTED_CONFIG_ID:
        raise FuturesConfigValidationError("config_id is invalid")
    if repo_path != EXPECTED_CONFIG_PATH:
        raise FuturesConfigValidationError("repo_path is invalid")
    if dataset_contract_refs != EXPECTED_DATASET_CONTRACT_PATHS:
        raise FuturesConfigValidationError("dataset contract refs are invalid")
    if blocked_contracts != ("futures_continuous_5m.v1",):
        raise FuturesConfigValidationError("blocked contracts are invalid")

    env_var = _validate_external_root(values["external_storage_root"])
    artifact_class_index = _validate_artifact_class_index(values["artifact_class_index"])
    _validate_path_rules(values.get("path_rules"))

    return FuturesDataLakeConfig(
        config_id=config_id,
        artifact_class=artifact_class,
        repo_path=repo_path,
        storage_root_env_var=env_var,
        dataset_contract_refs=dataset_contract_refs,
        artifact_class_index=artifact_class_index,
        blocked_contracts=blocked_contracts,
    )


def load_literal_config(path: str | Path) -> FuturesDataLakeConfig:
    config_path = Path(path)
    if config_path.is_absolute():
        raise FuturesConfigValidationError("config path must be repo-relative or caller-controlled")
    values = ast.literal_eval(config_path.read_text(encoding="utf-8"))
    if not isinstance(values, Mapping):
        raise FuturesConfigValidationError("loaded config must be a mapping")
    return validate_futures_data_lake_config_values(values)
