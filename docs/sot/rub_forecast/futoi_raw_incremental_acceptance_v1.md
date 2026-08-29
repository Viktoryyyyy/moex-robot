# FUTOI Raw Incremental Acceptance v1

Status: implementation candidate

## Scope

This slice adds a fail-closed acceptance layer for canonical `futures_futoi_raw` data after the immutable historical Stage 2 baseline. It is limited to `si_futures_family` and `cr_futures_family` and source `moex_algopack_futoi`.

## Boundary

The historical Stage 2 `current_accepted_manifest.json` is not mutated. The first incremental acceptance chains from that historical pointer. Later increments chain from `current_incremental_accepted_manifest.json`.

A candidate must come from an explicit-date canonical `backfill_futoi_instrument` run. Acceptance performs no network calls and rejects gaps, source failures, quality defects, missing FIZ/YUR participation groups, duplicate source keys, non-canonical partition paths, source scope mismatch, or parent-state mutation.

Accepted incremental manifests bind the parent pointer and manifest, source backfill manifest and quality report, and every accepted parquet partition by SHA-256. The incremental pointer advances atomically only after validation.

## Authority

This slice does not assert historical PIT research readiness, directional signal authority, or trading action authority. Stage 5 live derived integration and recurring freshness remain separate acceptance gates.
