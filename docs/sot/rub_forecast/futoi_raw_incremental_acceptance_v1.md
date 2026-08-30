# FUTOI Raw Incremental Acceptance v1

Status: implementation candidate

## Scope

This slice adds a fail-closed acceptance layer for canonical `futures_futoi_raw` data after the immutable historical Stage 2 baseline. It is limited to `si_futures_family` and `cr_futures_family` and source `moex_algopack_futoi`.

## Boundary

The historical Stage 2 `current_accepted_manifest.json` is not mutated. The first incremental acceptance chains from that historical pointer. Later increments chain from `current_incremental_accepted_manifest.json`.

A candidate must come from an explicit-date canonical `backfill_futoi_instrument` run. Acceptance performs no network calls and rejects gaps, source failures, stale aggregate quality, missing FIZ/YUR groups, duplicate source keys, null canonical fields, invalid positions, symlink partitions, non-canonical partition paths, source scope mismatch, or parent-state mutation.

Each candidate parquet is opened fail-closed as a non-symlink descriptor. One exact byte snapshot is read, hashed and parsed. Acceptance requires every column declared by `contracts/datasets/futures_futoi_raw.v1.yaml`, revalidates `instrument_id/secid/board/market/engine/source_id/source_ticker` against the canonical instrument registry, requires UTC-aware `availability_ts_utc` and `ingest_ts`, and recomputes canonical raw quality from that same snapshot.

The validated parquet bytes are copied create-only into the immutable incremental acceptance run. Parent pointer/manifest and source backfill manifest/quality bytes are snapshotted into the same run. The accepted manifest therefore references immutable accepted raw snapshots and SHA-256 bindings rather than requiring later reads from mutable canonical raw partitions. The incremental pointer advances atomically only after the accepted evidence set is written and the parent state is rechecked.

## Authority

This slice does not assert historical PIT research readiness, directional signal authority, or trading action authority. Stage 5 live derived integration and recurring freshness remain separate acceptance gates.
