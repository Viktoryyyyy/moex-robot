from __future__ import annotations

from dataclasses import dataclass

ALLOWED_ARTIFACT_ROLES = frozenset({"input", "output", "state", "report"})
ALLOWED_CONTRACT_CLASSES = frozenset({"repo_relative", "external_pattern", "cli_argument", "env_contract"})


@dataclass(frozen=True)
class ArtifactContract:
    artifact_id: str
    artifact_role: str
    contract_class: str
    producer: str
    consumers: tuple[str, ...]
    format: str
    schema_version: int
    partitioning_rule: str | None
    retention_policy: str | None
    locator_ref: str


def validate_artifact_contract(contract: ArtifactContract) -> ArtifactContract:
    if contract.artifact_role not in ALLOWED_ARTIFACT_ROLES:
        raise ValueError("unsupported artifact_role")
    if contract.contract_class not in ALLOWED_CONTRACT_CLASSES:
        raise ValueError("unsupported contract_class")
    if not contract.artifact_id:
        raise ValueError("artifact_id is required")
    if not contract.producer:
        raise ValueError("producer is required")
    if not contract.consumers:
        raise ValueError("consumers must be non-empty")
    if not contract.format:
        raise ValueError("format is required")
    if contract.schema_version < 1:
        raise ValueError("schema_version must be >= 1")
    if not contract.locator_ref:
        raise ValueError("locator_ref is required")
    return contract
