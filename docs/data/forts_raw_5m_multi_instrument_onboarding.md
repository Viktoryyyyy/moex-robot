# FORTS raw 5m multi-instrument onboarding

Status: active contract note

This note is the human-readable companion to `contracts/datasets/forts_raw_5m_multi_instrument_onboarding.v1.yaml`.

## Pointer strategy

The accepted manifest pointer for `dataset.forts.raw_5m.tradestats.v1` is per instrument:

```text
${MOEX_DATA_ROOT}/state/datasets/artifact_id=dataset.forts.raw_5m.tradestats.v1/instrument_id={INSTRUMENT_ID}/secid={SECID}/current_accepted_manifest.json
```

The artifact-level pointer path is legacy USDRUBF-only state and must not be used for onboarding or scheduling new instruments.

Backward compatibility: the pointer runner may read and update the legacy artifact-level pointer only for `forts.usdrubf` / `USDRUBF` when the per-instrument pointer is absent. It must not use that path for any other instrument and does not silently migrate pointer state.

## Required onboarding sequence

1. Add registry entry in `configs/instruments/forts_instrument_registry.v1.yaml`.
2. Run one-date pilot materialization.
3. Run full backfill.
4. Validate manifest and quality report.
5. Create the per-instrument accepted pointer.
6. Run observed-source refresh check.
7. Enable scheduler only after the observed-source check passes.

## Required registry fields

A new raw 5m FORTS instrument entry must define:

- `instrument_id`
- `canonical_symbol`
- `display_name`
- `market`
- `board`
- `secid`
- `source_artifact_id`
- `raw_5m_artifact_id`
- `enabled_for_raw_5m_materialization`
- `enabled_for_d1_derivation`
- `enabled_for_research`
- `storage_partition_values.instrument_id`
- `storage_partition_values.secid`
- `evidence_status`

## Production scheduler policy

Production scheduler must use:

```text
--incremental-mode observed-source
```

Calendar mode is not allowed for server cron until the MOEX calendar endpoint contract is resolved. This patch does not add server apply, cron, systemd, or D1 changes.

## Forbidden patterns

- latest-file discovery
- glob discovery
- implicit path selection
- artifact-level pointer reuse for new instruments
- scheduler enablement before observed-source refresh check
