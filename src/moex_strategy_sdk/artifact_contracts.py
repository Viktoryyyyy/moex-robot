from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable

from .errors import ArtifactContractValidationError

ALLOWED_CONTRACT_CLASSES = frozenset({"repo_relative", "external_pattern", "cli_argument", "env_contract"})


def _require_non_empty_string(field_name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ArtifactContractValidationError(f"{field_name} is required")
    return value


def _normalize_consumers(consumers: object) -> tuple[str, ...]:
    if isinstance(consumers, str):
        normalized = (consumers,)
    elif isinstance(consumers, Iterable):
        normalized = tuple(consumers)
    else:
        raise ArtifactContractValidationError("consumers is required")

    if not normalized:
        raise ArtifactContractValidationError("consumers is required")

    for consumer in normalized:
        if not isinstance(consumer, str) or not consumer.strip():
            raise ArtifactContractValidationError("consumers must contain non-empty strings")

    return normalized


@dataclass(frozen=True)
class ArtifactContract:
    artifact_id: str
    artifact_role: str
    contract_class: str
    producer: str
    consumers: tuple[str, ...]
    format: str
    schema_version: str | int
    partitioning_rule: str | None = None
    retention_policy: str | None = None
    locator_ref: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string("artifact_id", self.artifact_id)
        _require_non_empty_string("artifact_role", self.artifact_role)
        _require_non_empty_string("contract_class", self.contract_class)
        _require_non_empty_string("producer", self.producer)
        _require_non_empty_string("format", self.format)

        if self.contract_class not in ALLOWED_CONTRACT_CLASSES:
            raise ArtifactContractValidationError("unsupported contract_class")

        object.__setattr__(self, "consumers", _normalize_consumers(self.consumers))

        if isinstance(self.schema_version, bool):
            raise ArtifactContractValidationError("invalid schema_version")
        if isinstance(self.schema_version, int):
            if self.schema_version < 1:
                raise ArtifactContractValidationError("schema_version must be >= 1")
        elif not isinstance(self.schema_version, str) or not self.schema_version.strip():
            raise ArtifactContractValidationError("schema_version is required")

    @property
    def artifact_class(self) -> str:
        return self.artifact_role

    @property
    def consumer(self) -> str:
        return self.consumers[0]


def validate_artifact_contract(contract: ArtifactContract) -> ArtifactContract:
    if not isinstance(contract, ArtifactContract):
        raise TypeError("contract must be ArtifactContract")
    return contract
