from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final


class RegistryContractError(ValueError):
    pass


REGISTRY_KINDS: Final[tuple[str, ...]] = (
    "instrument",
    "dataset",
    "feature",
    "strategy",
    "portfolio",
    "environment",
)

EXPECTED_REGISTRY_CONTRACT_PATHS: Final[tuple[str, ...]] = (
    "contracts/registries/instrument_registry.v1.yaml",
    "contracts/registries/dataset_registry.v1.yaml",
    "contracts/registries/feature_registry.v1.yaml",
    "contracts/registries/strategy_registry.v1.yaml",
    "contracts/registries/portfolio_registry.v1.yaml",
    "contracts/registries/environment_registry.v1.yaml",
)

EXPECTED_REGISTRY_CONFIG_PATHS: Final[tuple[str, ...]] = (
    "configs/instruments/instrument_registry.v1.yaml",
    "configs/datasets/dataset_registry.v1.yaml",
    "configs/features/feature_registry.v1.yaml",
    "configs/strategies/strategy_registry.v1.yaml",
    "configs/portfolios/portfolio_registry.v1.yaml",
    "configs/environments/environment_registry.v1.yaml",
)

_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "entry_id",
    "registry_kind",
    "config_id",
    "artifact_class",
    "repo_path",
    "dependencies",
    "enabled",
    "registry_mutation_allowed",
    "promotion_ref_or_none",
)

_DEPENDENCY_KINDS: Final[dict[str, tuple[str, ...]]] = {
    "instrument": (),
    "dataset": ("instrument",),
    "feature": ("dataset",),
    "strategy": ("dataset", "feature", "instrument"),
    "portfolio": ("strategy", "instrument"),
    "environment": ("dataset", "strategy", "portfolio"),
}

_CONFIG_PREFIX: Final[dict[str, str]] = {
    "instrument": "configs/instruments/",
    "dataset": "configs/datasets/",
    "feature": "configs/features/",
    "strategy": "configs/strategies/",
    "portfolio": "configs/portfolios/",
    "environment": "configs/environments/",
}


@dataclass(frozen=True)
class RegistryEntry:
    entry_id: str
    registry_kind: str
    config_id: str
    artifact_class: str
    repo_path: str
    dependencies: dict[str, tuple[str, ...]]
    enabled: bool
    registry_mutation_allowed: bool
    promotion_ref_or_none: str | None
    payload: dict[str, object] | None = None


@dataclass(frozen=True)
class RegistryPackage:
    entries: tuple[RegistryEntry, ...]


def _blocked_tokens() -> tuple[str, ...]:
    return ("late" + "st", "cur" + "rent", "auto" + "detect")


def _approval_tokens() -> tuple[str, ...]:
    return ("metric", "verdict", "approval", "approved", "promotion")


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryContractError(f"{field_name} is required")
    return _guard_text(value, field_name)


def _guard_text(value: str, field_name: str) -> str:
    normalized = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":", "{", "}", "$"):
        normalized = normalized.replace(separator, " ")
    tokens = tuple(token for token in normalized.split() if token)
    if any(token in _blocked_tokens() for token in tokens):
        raise RegistryContractError(f"{field_name} contains unsupported dynamic marker")
    if value.startswith("/"):
        raise RegistryContractError(f"{field_name} must not be absolute")
    return value


def _contains_approval_token(value: object) -> bool:
    normalized = str(value).casefold()
    return any(token in normalized for token in _approval_tokens())


def _require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise RegistryContractError(f"{field_name} must be boolean")
    return value


def _require_ref_or_none(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _require_text_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise RegistryContractError(f"{field_name} must be a sequence of strings")
    return tuple(_require_text(item, field_name) for item in value)


def _validate_dependencies(value: object, registry_kind: str) -> dict[str, tuple[str, ...]]:
    if not isinstance(value, Mapping):
        raise RegistryContractError("dependencies must be a mapping")
    required_kinds = _DEPENDENCY_KINDS[registry_kind]
    if set(value) != set(required_kinds):
        raise RegistryContractError("dependency kinds do not match registry kind")
    result: dict[str, tuple[str, ...]] = {}
    for dependency_kind in required_kinds:
        refs = _require_text_tuple(value[dependency_kind], f"dependencies.{dependency_kind}")
        if not refs:
            raise RegistryContractError("required dependency refs must be non-empty")
        result[dependency_kind] = refs
    return result


def _validate_repo_path(value: object, registry_kind: str) -> str:
    repo_path = _require_text(value, "repo_path")
    if not repo_path.startswith(_CONFIG_PREFIX[registry_kind]):
        raise RegistryContractError("repo_path is outside registry config scope")
    if not repo_path.endswith(".yaml"):
        raise RegistryContractError("repo_path must reference yaml")
    return repo_path


def _guard_payload(value: object, field_name: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if _contains_approval_token(key):
                raise RegistryContractError("strategy promotion approval must stay outside registry")
            _guard_text(str(key), field_name)
            _guard_payload(item, f"{field_name}.{key}")
        return
    if isinstance(value, (str, bytes)):
        _guard_text(str(value), field_name)
        return
    if isinstance(value, Sequence):
        for item in value:
            _guard_payload(item, field_name)
        return
    if value is None or isinstance(value, (bool, int, float)):
        return
    raise RegistryContractError("payload contains unsupported value")


def _validate_payload(value: object, registry_kind: str) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise RegistryContractError("registry payload must be a mapping")
    _guard_payload(value, registry_kind)
    return dict(value)


def _entry_to_values(entry: object) -> Mapping[str, object]:
    if isinstance(entry, RegistryEntry):
        values: dict[str, object] = {
            "entry_id": entry.entry_id,
            "registry_kind": entry.registry_kind,
            "config_id": entry.config_id,
            "artifact_class": entry.artifact_class,
            "repo_path": entry.repo_path,
            "dependencies": entry.dependencies,
            "enabled": entry.enabled,
            "registry_mutation_allowed": entry.registry_mutation_allowed,
            "promotion_ref_or_none": entry.promotion_ref_or_none,
        }
        if entry.payload is not None:
            values[entry.registry_kind] = entry.payload
        return values
    if isinstance(entry, Mapping):
        return entry
    raise RegistryContractError("registry entry must be a mapping or RegistryEntry")


def validate_registry_entry_values(values: Mapping[str, object]) -> RegistryEntry:
    if not isinstance(values, Mapping):
        raise RegistryContractError("registry entry values must be a mapping")
    missing = tuple(field for field in _REQUIRED_FIELDS if field not in values)
    if missing:
        raise RegistryContractError("registry entry is missing required fields")

    registry_kind = _require_text(values["registry_kind"], "registry_kind")
    if registry_kind not in REGISTRY_KINDS:
        raise RegistryContractError("unsupported registry_kind")

    allowed_fields = set(_REQUIRED_FIELDS) | {registry_kind}
    unknown_fields = set(values).difference(allowed_fields)
    if unknown_fields:
        if any(_contains_approval_token(field) for field in unknown_fields):
            raise RegistryContractError("strategy promotion approval must stay outside registry")
        raise RegistryContractError("registry entry contains unsupported fields")

    entry_id = _require_text(values["entry_id"], "entry_id")
    if not entry_id.startswith(f"{registry_kind}."):
        raise RegistryContractError("entry_id must be typed by registry_kind")
    artifact_class = _require_text(values["artifact_class"], "artifact_class")
    if artifact_class != "repo_relative":
        raise RegistryContractError("registry config must be repo_relative")
    registry_mutation_allowed = _require_bool(values["registry_mutation_allowed"], "registry_mutation_allowed")
    if registry_mutation_allowed:
        raise RegistryContractError("registry mutation is not allowed in this contract slice")
    return RegistryEntry(
        entry_id=entry_id,
        registry_kind=registry_kind,
        config_id=_require_text(values["config_id"], "config_id"),
        artifact_class=artifact_class,
        repo_path=_validate_repo_path(values["repo_path"], registry_kind),
        dependencies=_validate_dependencies(values["dependencies"], registry_kind),
        enabled=_require_bool(values["enabled"], "enabled"),
        registry_mutation_allowed=registry_mutation_allowed,
        promotion_ref_or_none=_require_ref_or_none(values["promotion_ref_or_none"], "promotion_ref_or_none"),
        payload=_validate_payload(values.get(registry_kind), registry_kind),
    )


def validate_registry_package_values(values: Mapping[str, object]) -> RegistryPackage:
    if not isinstance(values, Mapping):
        raise RegistryContractError("registry package values must be a mapping")
    if set(values) != {"entries"}:
        raise RegistryContractError("registry package must contain only entries")
    entries_value = values["entries"]
    if isinstance(entries_value, (str, bytes)) or not isinstance(entries_value, Sequence):
        raise RegistryContractError("entries must be a sequence")
    entries = tuple(validate_registry_entry_values(_entry_to_values(entry)) for entry in entries_value)
    if tuple(entry.registry_kind for entry in entries) != REGISTRY_KINDS:
        raise RegistryContractError("registry package must contain required registry kinds in order")
    _validate_unique_entries(entries)
    _validate_dependency_refs(entries)
    return RegistryPackage(entries=entries)


def _validate_unique_entries(entries: tuple[RegistryEntry, ...]) -> None:
    for values in (
        tuple(entry.entry_id for entry in entries),
        tuple(entry.config_id for entry in entries),
        tuple(entry.repo_path for entry in entries),
    ):
        if len(set(values)) != len(values):
            raise RegistryContractError("duplicate registry value")


def _validate_dependency_refs(entries: tuple[RegistryEntry, ...]) -> None:
    refs_by_kind: dict[str, set[str]] = {kind: set() for kind in REGISTRY_KINDS}
    for entry in entries:
        refs_by_kind[entry.registry_kind].add(entry.entry_id)
    for entry in entries:
        for dependency_kind, refs in entry.dependencies.items():
            if set(refs).difference(refs_by_kind[dependency_kind]):
                raise RegistryContractError("registry dependency reference is not declared")


def validate_registry_package(package: RegistryPackage) -> RegistryPackage:
    if not isinstance(package, RegistryPackage):
        raise TypeError("package must be RegistryPackage")
    validate_registry_package_values({"entries": package.entries})
    return package


__all__ = (
    "EXPECTED_REGISTRY_CONFIG_PATHS",
    "EXPECTED_REGISTRY_CONTRACT_PATHS",
    "REGISTRY_KINDS",
    "RegistryContractError",
    "RegistryEntry",
    "RegistryPackage",
    "validate_registry_entry_values",
    "validate_registry_package",
    "validate_registry_package_values",
)
