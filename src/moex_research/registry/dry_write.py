from __future__ import annotations

from dataclasses import dataclass

from moex_research.registry.schemas import ArtifactManifest, ExperimentRegistryEntry, RegistryValidationError
from moex_research.registry.validation import validate_registry_entry_against_manifest

_ALLOWED_WRITE_MODES = frozenset({"dry_write", "controlled_write"})


@dataclass(frozen=True)
class DryRegistryWriteResult:
    run_id: str
    registry_entry_id: str
    artifact_manifest_ref: str
    write_mode: str
    persisted: bool
    producer: str
    consumer: str
    format: str
    artifact_class: str


class DryExperimentRegistryWriter:
    writer_id = "moex_research.registry.dry_write"

    def write(
        self,
        entry: ExperimentRegistryEntry,
        manifest: ArtifactManifest,
        *,
        mode: str = "dry_write",
    ) -> DryRegistryWriteResult:
        if mode not in _ALLOWED_WRITE_MODES:
            raise RegistryValidationError("unsupported experiment registry write mode")
        validate_registry_entry_against_manifest(entry, manifest)
        return DryRegistryWriteResult(
            run_id=entry.run_id,
            registry_entry_id=entry.registry_entry_id,
            artifact_manifest_ref=entry.artifact_manifest_ref,
            write_mode=mode,
            persisted=False,
            producer=self.writer_id,
            consumer="PM_L3_DELIVERY_VALIDATION_OWNER",
            format="ExperimentRegistryEntry",
            artifact_class="registry_dry_write_contract",
        )


__all__ = ["DryExperimentRegistryWriter", "DryRegistryWriteResult"]
