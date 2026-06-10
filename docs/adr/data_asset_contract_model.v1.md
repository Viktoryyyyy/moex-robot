# ADR: Data Asset Contract Model v1

Status: accepted target declaration
Date: 2026-06-10
Scope: repo-only contract documentation package

## Context

MOEX Bot is moving from fixture-only strategy testing toward real strategy testing. The move must keep reproducibility and must not create one-off result artifacts. GitHub remains the architectural source of truth for code and contracts. Data files and execution artifacts may live outside the repository only when their contract is explicit.

This ADR is additive. It does not activate or mutate the current loaded futures data lake schema, validators, configs, loaders, resamplers, strategy code, runtime code, or existing research scripts.

## Decision

The target data asset contract model separates semantic artifact class from path binding.

`artifact_class` describes what the artifact is, for example `external_source_contract`, `raw_native_dataset`, `derived_dataset`, `feature_set`, `label_set`, `research_result`, `runtime_input_snapshot`, `runtime_state`, `asset_manifest`, or `quality_report`.

`path_contract_type` describes how the artifact is located. Exactly one `path_contract_type` is allowed per artifact. The only allowed values for this ADR are:

- `repo_relative`
- `external_pattern`
- `cli_argument`
- `env_contract`

A contract must not encode multiple path binding modes in one artifact. Environment credentials and endpoint configuration are dependencies, not additional artifact path contract types.

## Storage vs recompute policy

Raw/native data accepted from an external source is stored as canonical data when the manifest and quality report pass. Raw/native data is not silently rebuilt during research or runtime.

Derived datasets, features, labels, and research result tables may be recomputable only when all input references, builder/config versions, schema versions, and calendar/session bindings are explicit.

A materialized artifact is canonical only when the artifact path matches its declared contract, its manifest exists, its quality report exists and passes, and its input references are deterministic.

Temporary generated data inside a strategy or research script is not canonical proof.

## Timeframe materialization policy

FORTS raw 5m tradestats is the current native source grain for the accepted path. D1 may be derived from accepted raw 5m partitions when the derivation contract, manifest, quality report, and builder/config version are present.

The following timeframes are future scope only until separately authorized: 10m, 15m, 30m, 1h, 4h, and 1W. The 10m, 15m, 30m, 1h, and 4h datasets must derive from raw 5m. The 1W dataset must derive from accepted D1.

Future-scope declarations do not authorize materialization.

## Research/backtest canonical input policy

Research and backtests must consume contract-declared canonical artifacts. A research/backtest may not select inputs through implicit latest-file discovery, uncontrolled glob selection, remembered server paths, or strategy-local generated files.

A research/backtest result is valid only when it declares the dataset, feature, optional label, calendar/session, strategy config, and deterministic builder/config versions that produced it.

D1 TSMOM Minimal is authorized only for research/backtest contract preparation. It is not authorized for live/runtime execution.

## Runtime/live input and state separation

Runtime input snapshots and runtime state are separate artifacts.

A runtime input snapshot is an immutable read-side view of the latest approved features/context for a strategy and instrument. Runtime state is mutable operational state such as position state, locks, heartbeat, and reconciliation status.

Runtime must not use research outputs, labels, or temporary generated files as live input.

## Runtime fail-closed policy

Runtime must fail closed if any of the following is true:

- input snapshot is stale;
- manifest is missing;
- quality report is missing or failed;
- calendar/session is unresolved;
- feature/context snapshot is missing;
- runtime state or lock is missing;
- only implicit latest/autodetect input exists.

Runtime must not silently fall back to a previous file, latest file, glob match, or autodetected artifact.

## Research/backtest generation rule

Research/backtest generation must be deterministic from contract-declared inputs. The output package must include input references, schema versions, builder/config version, and quality status. A strategy-local temporary file, notebook export, stdout-only summary, or ad hoc generated table cannot be used as canonical proof.

## Consequences

The new target contract files can use semantic `artifact_class` plus explicit `path_contract_type` while legacy loaded contracts continue to use their existing shape until migrated by a separate task.

This ADR does not add the new contracts to `configs/datasets/futures_data_lake.v1.yaml`.
