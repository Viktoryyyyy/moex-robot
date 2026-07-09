# Phase 6.0 internal modeling dataset builder v1

Status: checked-in contract / builder only  
Project: MOEX Bot  
Lane: `ema_3_19_ai`  
Task: `ema_3_19_ai_market_phase_phase_6_0_internal_factor_batches_modeling_dataset`

## Purpose

Phase 6.0 defines an internal-only supervised modeling dataset builder for later factor-batch modeling of USDRUBF / Si D1 market phases.

This PR creates repository code, contracts, tests, and documentation only. It does not run server materialization, does not train a model, does not produce predictions, and does not create trading signals.

## Authority boundary

Execution and dataset materialization require a separate PM L2 server approval after this PR is merged into `main`.

This PR does not authorize:

- server commands;
- server output writes;
- external data ingestion;
- provider API calls;
- model fitting;
- prediction generation;
- trading or broker actions;
- runtime or systemd changes;
- generated data committed to the repository.

Merge authority remains `PM_L2_ONLY`.

## CLI entrypoint

```text
python -m moex_research.runners.usdrubf_phase6_internal_modeling_dataset_builder
```

Required input and output flags:

```text
--panel-path
--panel-manifest-path
--label-contract-path
--output-dir
--run-id
```

Required safety gates:

```text
--internal-d1-only
--no-external-data
--no-model-fitting
--no-prediction
--no-trading
--no-overwrite
```

The builder fails before reading inputs or writing outputs if any safety gate is missing.

## Inputs for later authorized server execution

The builder expects:

1. Existing Phase 3.4 internal D1 panel parquet.
2. Existing D1 panel manifest JSON.
3. Checked-in manual B / S / OUT label contract JSON.
4. Checked-in manual label materializer:
   `src/moex_research/labels/usdrubf_d1_manual_phase_labels.py`.

The builder does not fetch market data and does not call external providers.

## Outputs when executed later

The builder writes only these artifacts to `--output-dir`:

```text
modeling_dataset.parquet
manifest.json
feature_schema.json
dataset_preview.csv
target_distribution.csv
```

The builder refuses a non-empty output directory and refuses existing artifacts under `--no-overwrite`.

## Dataset semantics

The output dataset includes these target columns:

```text
target_phase_label
target_is_labeled
target_source
target_trade_date
target_instrument_id
```

`target_source` is fixed to `manual_phase_labels_v1`.

Manual B / S / OUT labels are targets only. They are not feature inputs and are not assistant/runtime state.

## Feature batches

The checked-in feature contract is:

```text
contracts/features/usdrubf_phase6_internal_factor_batches_v1.json
```

It defines these required batches:

- `internal_price_return`
- `internal_volatility_range`
- `internal_volume_liquidity`
- `ema_3_19_baseline_context`
- `session_index_context`

All price, range, volume, and EMA features are lagged relative to `target_trade_date`.

`session_index` is derived only from the internal D1 panel row order. It is not derived from manual phase intervals or phase boundaries.

EMA 3/19 is baseline diagnostic context only. It is computed from the internal D1 close series and then lagged. It is not a label source, not a model, not a prediction, and not a trading signal.

## Leakage rules

Forbidden as feature columns:

```text
phase_label
B
S
OUT
target
y
future_return
source_interval_id
phase_remaining_sessions
next_regime_if_current_ends
transition_exit_day
boundary_distance
future_phase
future_volatility
label_annotation
annotator
label_availability_ts
```

Also forbidden:

- manual label metadata as features;
- phase-boundary-derived metadata as features;
- label availability timestamps as features;
- same-day D1 OHLCV values of the target session as features;
- external factors in Phase 6.0.

The builder fails closed if the input panel already contains target-like, label, future, or manual phase metadata columns.

## Current PR boundary

This PR creates only:

```text
docs/sot/strategies/ema_3_19_ai/phase6_internal_modeling_dataset_v1.md
contracts/datasets/usdrubf_phase6_internal_modeling_dataset.v1.yaml
contracts/features/usdrubf_phase6_internal_factor_batches_v1.json
src/moex_research/runners/usdrubf_phase6_internal_modeling_dataset_builder.py
tests/ema_3_19_ai/test_phase6_internal_modeling_dataset_builder.py
```

No server apply is performed. No real dataset is materialized. No existing Phase 3.4 D1 panel output or manual label contract is modified.

## Later materialization gate

A future server execution must be separately approved by PM L2 and must provide concrete paths through the CLI flags. The repository contract does not hardcode server paths.
