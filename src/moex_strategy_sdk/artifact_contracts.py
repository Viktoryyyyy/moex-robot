from __future__ import annotations

from dataclasses import dataclass

from .errors import ArtifactContractValidationError

ALLOWED_CONTRACT_CLASSES = frozenset({"repo_relative", "external_pattern", "cli_argument", "env_contract"})


@dataclass(frozen=True)
class ArtifactContract:
    artifact_id: str
    artifact_class: str
    producer: str
    consumer: str
    format: str
    schema_version: str | int
    contract_class: str | None = None
    partitioning_rule: str | None = None
    retention_policy: str | None = None
    locator_ref: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("artifact_id", "artifact_class", "producer", "consumer", "format"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ArtifactContractValidationError(f"{field_name} is required")
        if isinstance(self.schema_version, int):
            if self.schema_version < 1:
                raise ArtifactContractValidationError("schema_version must be >= 1")
        elif not isinstance(self.schema_version, str) or not self.schema_version.strip():
            raise ArtifactContractValidationError("schema_version is required")
        if self.contract_class is not None and self.contract_class not in ALLOWED_CONTRACT_CLASSES:
            raise ArtifactContractValidationError("unsupported contract_class")


def validate_artifact_contract(contract: ArtifactContract) -> ArtifactContract:
    if not isinstance(contract, ArtifactContract):
        raise TypeError("contract must be ArtifactContract")
    return contract
