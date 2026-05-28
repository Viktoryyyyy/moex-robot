# Promotion Verdict Contract v1

contract_id: promotion_verdict.v1
schema_version: v1
artifact_class: promotion_verdict
producer: PM review
consumer: PM / Architect / future runtime governance

## purpose

Declares the separate PM review artifact for promotion decisions.

The promotion verdict is separate from registry metrics.
The runner must not create promotion verdict.
Runtime/live remains blocked unless explicitly allowed by separate review.

## required_fields

- promotion_verdict_id
- registry_entry_ref
- artifact_manifest_ref
- reviewed_by
- reviewed_ts
- source_commit
- decision
- decision_scope
- evidence_refs
- runtime_live_allowed
- blockers
- notes

## validation_rules

- promotion_verdict_id must be unique.
- registry_entry_ref must point to experiment_registry_entry.v1.
- artifact_manifest_ref must point to artifact_manifest.v1.
- reviewed_by and reviewed_ts must identify the PM review.
- source_commit must identify the repo commit under review.
- decision must explicitly state hold, reject, research_only, promote_to_next_research_phase, or allow_runtime_live_by_exception.
- decision_scope must describe the exact strategy, instrument, timeframe, and environment covered.
- evidence_refs must point to formal artifacts, not stdout.
- runtime_live_allowed must be false unless a separate runtime/live review explicitly allows it.
- blockers must list unresolved data, semantics, fragility, cost, or runtime governance issues.

## forbidden_patterns

- Research runner creating or modifying a promotion verdict.
- Promotion verdict stored inside registry metrics.
- Runtime/live enablement from supported_canonical or supported_provisional result status alone.
- Promotion decisions without artifact_manifest_ref and registry_entry_ref.
- Broad runtime/live permission without separate review scope.
