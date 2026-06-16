from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

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


@dataclass(frozen=True, init=False)
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

    def __init__(
        self,
        *,
        artifact_id: str | None = None,
        artifact_role: str | None = None,
        contract_class: str | None = None,
        producer: str | None = None,
        consumers: object = None,
        format: str | None = None,
        schema_version: str | int | None = None,
        partitioning_rule: str | None = None,
        retention_policy: str | None = None,
        locator_ref: str | None = None,
        artifact_class: str | None = None,
        consumer: str | None = None,
    ) -> None:
        if artifact_role is None and artifact_class is not None:
            artifact_role = artifact_class
        elif artifact_class is not None and artifact_role != artifact_class:
            raise ArtifactContractValidationError("artifact_class conflicts with artifact_role")

        if consumers is None and consumer is not None:
            consumers = (consumer,)
        elif consumers is not None and consumer is not None:
            normalized_consumers = _normalize_consumers(consumers)
            if normalized_consumers[0] != consumer:
                raise ArtifactContractValidationError("consumer conflicts with consumers")
            consumers = normalized_consumers

        object.__setattr__(self, "artifact_id", artifact_id)
        object.__setattr__(self, "artifact_role", artifact_role)
        object.__setattr__(self, "contract_class", contract_class)
        object.__setattr__(self, "producer", producer)
        object.__setattr__(self, "consumers", consumers)
        object.__setattr__(self, "format", format)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "partitioning_rule", partitioning_rule)
        object.__setattr__(self, "retention_policy", retention_policy)
        object.__setattr__(self, "locator_ref", locator_ref)
        self.__post_init__()

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
        elif not isinstance(self.schema_version, str):
            raise ArtifactContractValidationError("invalid schema_version")
        elif not self.schema_version.strip():
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
