from __future__ import annotations

from collections.abc import Mapping

from moex_research.metrics.schemas import MetricsSummary
from moex_research.registry.schemas import ArtifactManifest, ExperimentRegistryEntry
from moex_research.registry.validation import validate_registry_entry_against_manifest

_PRODUCER_COMPONENT = "moex_research.publishers.review_artifact"
_CONSUMER_COMPONENT = "PM_L3_DELIVERY_VALIDATION_OWNER"


def build_pm_review_artifact(
    *,
    run_id: str,
    artifact_ref: str,
    artifact_manifest: ArtifactManifest,
    registry_entry: ExperimentRegistryEntry,
    metrics_summary: MetricsSummary,
) -> Mapping[str, object]:
    validate_registry_entry_against_manifest(registry_entry, artifact_manifest)
    if metrics_summary.run_id != run_id:
        raise ValueError("metrics_summary run_id mismatch")
    return {
        "run_id": run_id,
        "artifact_ref": artifact_ref,
        "artifact_class": "pm_review_artifact",
        "producer": _PRODUCER_COMPONENT,
        "consumer": _CONSUMER_COMPONENT,
        "format": "json",
        "schema_version": "pm_review_artifact.v1",
        "run_status": registry_entry.run_status,
        "result_status": registry_entry.result_status,
        "canonicality_status": registry_entry.canonicality_status,
        "artifact_manifest_ref": artifact_manifest.artifact_manifest_id,
        "metrics_artifact_ref": metrics_summary.artifact_ref,
        "review_boundary": "PM review only; no market conclusion or strategy readiness claim",
        "execution_path_enabled": False,
        "decision_action_enabled": False,
    }


__all__ = ["build_pm_review_artifact"]
