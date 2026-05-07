# MOEX Bot — W/MM/MX Controlled Eligibility Artifact Contract

Status: implemented contract
Project: MOEX Bot
Scope: controlled_batch_w_mm_mx raw-only eligibility
Schema version: futures_controlled_wmmmx_eligibility.v1

## Purpose

This artifact is the canonical controlled eligibility input for `controlled_wmmmx_select` and `controlled_raw_pipeline_runner`.

It exists because the CR/GD/GL pilot classification artifact is not the W/MM/MX controlled raw-only eligibility source.

## Artifact

Dataset id:

```text
futures_controlled_wmmmx_eligibility
```

Artifact class: external_pattern

Path pattern:

```text
${MOEX_DATA_ROOT}/futures/registry/controlled_wmmmx_eligibility/snapshot_date={snapshot_date}/controlled_wmmmx_eligibility.parquet
```

Summary path pattern:

```text
${MOEX_DATA_ROOT}/futures/registry/controlled_wmmmx_eligibility/snapshot_date={snapshot_date}/controlled_wmmmx_eligibility_summary.json
```

Producer:

```text
src.moex_data.futures.controlled_wmmmx_eligibility_producer
```

Consumers:

```text
src.moex_data.futures.controlled_wmmmx_select
src.moex_data.futures.controlled_raw_pipeline_runner
```

## Eligibility rules

Required families:

```text
W
MM
MX
```

Required board:

```text
RFUD
```

Required status fields:

```text
classification_status=controlled_accepted_for_data_pipeline
continuous_eligibility_status=not_accepted
```

## Required parquet columns

```text
schema_version
snapshot_date
secid
family_code
board
classification_status
continuous_eligibility_status
source_status_artifact
source_registry_artifact
eligibility_status
```

## Hard gates

- output must contain only W/MM/MX families
- output must contain only RFUD board rows
- `classification_status` must be `controlled_accepted_for_data_pipeline`
- `continuous_eligibility_status` must be `not_accepted`
- empty output is a failure
- continuous eligibility promotion is forbidden
- continuous builders are not invoked
- CR/GD/GL pilot classification behavior is not changed
- Slice 1 defaults are not changed
- Si continuous behavior is not changed
