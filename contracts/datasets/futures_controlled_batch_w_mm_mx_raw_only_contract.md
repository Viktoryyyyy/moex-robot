# MOEX Bot — Controlled Batch W/MM/MX Raw-Only Contract

Status: implemented contract
Project: MOEX Bot
Scope: controlled_batch_w_mm_mx raw-only data pipeline
Schema contract version: futures_controlled_batch_w_mm_mx_raw_only.v1

## Boundary

This contract enables raw-only data generation for W/MM/MX controlled instruments.

GitHub stores code, dataset contracts, and config. Server stores data artifacts only.
All external data paths are resolved through `MOEX_DATA_ROOT` or explicit `--data-root`.

## Included scope

Universe scope id:

```text
controlled_batch_w_mm_mx
```

Families:

```text
W
MM
MX
```

Required normalized registry gates:

```text
classification_status=controlled_accepted_for_data_pipeline
continuous_eligibility_status=not_accepted
```

## Raw-only rule

Allowed outputs:

- `futures_raw_5m`
- `futures_futoi_5m_raw`
- `futures_derived_d1_ohlcv`
- raw-only diagnostics manifest

Forbidden outputs:

- `futures_continuous_5m`
- `futures_continuous_d1`
- continuous roll-map promotion
- continuous eligibility promotion
- strategy/research/runtime trading artifacts

## CLI contract

Producer:

```text
src.moex_data.futures.controlled_raw_pipeline_runner
```

Arguments:

```text
--universe-scope slice1|controlled_batch_w_mm_mx
--config configs/datasets/futures_controlled_batch_w_mm_mx_raw_scope_config.json
--snapshot-date YYYY-MM-DD
--run-date YYYY-MM-DD
--from YYYY-MM-DD
--till YYYY-MM-DD
--data-root PATH
```

Default behavior must remain Slice 1:

```text
--universe-scope slice1
```

## Diagnostics artifact

Dataset id:

```text
futures_controlled_batch_raw_only_diagnostics
```

Artifact class: external_pattern

Path pattern:

```text
${MOEX_DATA_ROOT}/futures/runs/controlled_raw_pipeline/universe_scope={universe_scope}/run_date={run_date}/manifest.json
```

Format: json
Schema version: futures_controlled_batch_raw_only_diagnostics.v1

Required fields:

- schema_version
- universe_scope
- selected_secids
- classification_gate
- raw_5m_status
- futoi_raw_status
- derived_d1_status
- continuous_absence_checks
- preservation_checks
- final_verdict

## Continuous absence checks

The runner must confirm that no continuous components are executed for `controlled_batch_w_mm_mx`.
It must also record whether continuous roots already existed before/after the raw-only run.
Existing unrelated historical continuous artifacts are not deleted and are not interpreted as artifacts created by this run.

## Slice 1 preservation

For `--universe-scope slice1`, existing Slice 1 defaults and existing Si continuous behavior are not changed by this contract.
