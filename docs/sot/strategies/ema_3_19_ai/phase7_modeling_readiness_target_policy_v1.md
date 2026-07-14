# Phase 7.0 modeling-readiness target policy v1

Status: repository readiness contract only  
Project: MOEX Bot  
Lane: `ema_3_19_ai`  
Task: `ema_3_19_ai_market_phase_phase_7_0_modeling_readiness_target_policy`

## Purpose and scope

Phase 7.0 defines the repository-level readiness gate that must be satisfied before any future supervised modeling run for USDRUBF / Si D1 market phases.

This package defines target semantics, supervised-row eligibility, chronological validation, baseline metrics, leakage prevention, and the required shape of future evaluation evidence. It does not fit a model, generate predictions, run ingestion or materialization, create trading signals, or authorize runtime/server execution.

## Manual-label-only target policy

The only allowed supervised target source is the checked-in manual phase-label source:

```text
manual_phase_labels_v1
```

EMA 3/19 state, price action, indicators, model output, assistant output, inferred regimes, future returns, or any other derived field must not create, overwrite, or backfill supervised targets.

A supervised target row must carry:

- `target_source == manual_phase_labels_v1`;
- `target_is_labeled == true`;
- `target_phase_label` equal to exactly one of `B`, `S`, or `OUT`.

## Supervised target semantics

The supervised target is a three-class categorical label:

- `B` is the manual `B` phase class;
- `S` is the manual `S` phase class;
- `OUT` is the manual `OUT` phase class and is a valid supervised class.

These values are consumed exactly as manual annotations. Phase 7.0 does not redefine them from EMA state, return sign, model probability, or trading position. `OUT` must not be treated as missing data.

`UNLABELED` is not a fourth supervised class. It denotes absence of an eligible manual target.

## UNLABELED exclusion policy

Rows with `target_phase_label == UNLABELED`, `target_is_labeled != true`, a missing target label, an unknown target source, or a target class outside `B / S / OUT` are ineligible for supervised fitting.

Such rows must also be excluded from every supervised validation metric, class-support count, confusion matrix, probability score, and aggregate score. They may be retained only for non-supervised coverage accounting that is clearly separated from model metrics.

No implicit conversion from `UNLABELED` to `OUT` is allowed.

## Supervised row eligibility

A row is eligible for future supervised fitting or validation only when all of the following hold:

1. The target is sourced from `manual_phase_labels_v1`.
2. `target_is_labeled` is exactly true.
3. `target_phase_label` is exactly one of `B`, `S`, or `OUT`.
4. `target_trade_date` and `target_instrument_id` are present.
5. Every selected feature is available under the applicable point-in-time rule before the target decision anchor.
6. No target, manual-label metadata, phase-boundary metadata, future-derived field, or same-session target leakage is present in the feature set.
7. The row belongs to exactly one chronological walk-forward fold role: training or validation.

Eligibility filtering must occur before fitting and before supervised metric computation. Eligibility must not be changed using validation outcomes.

## Chronological walk-forward validation plan

Future supervised evaluation must use chronological walk-forward validation.

Required ordering rules:

- validation rows are ordered by `target_trade_date`;
- every training row precedes every validation row in the same fold;
- `max(train.target_trade_date) < min(validation.target_trade_date)`;
- random train/test splitting and shuffled cross-validation are forbidden;
- fold boundaries are declared before evaluating validation outcomes;
- feature selection, imputation, scaling, encoding, class weighting, calibration, and threshold selection are fitted from the training portion only;
- later folds may expand the training window, but may not introduce future rows into an earlier fold;
- the same target row must not appear in both training and validation.

Fold evidence must report train and validation date boundaries, row counts, and class support after supervised eligibility filtering.

## Baseline metric contract

Every future evaluation must report deterministic train-only baselines alongside any candidate model.

Required baselines:

- `majority_class_train_only`: predict the most frequent eligible class in the training fold;
- `class_prior_train_only`: use training-fold class proportions as fixed validation probabilities.

Required supervised metrics:

- eligible validation row count;
- per-class support for `B`, `S`, and `OUT`;
- confusion matrix with fixed class order `B, S, OUT`;
- accuracy;
- balanced accuracy;
- macro F1;
- weighted F1;
- per-class precision, recall, and F1;
- multiclass log loss when probability outputs are available.

All baseline and candidate metrics must use the same eligible labeled validation rows. `UNLABELED` rows must not contribute to denominators or class support. Undefined per-class metrics must be reported explicitly and must not be silently replaced with a favorable value.

## Leakage-prevention checklist

Before any future fit, the evaluation implementation must verify:

- targets come only from `manual_phase_labels_v1`;
- `B / S / OUT` are targets only and never feature values;
- `UNLABELED` rows are excluded from fitting and supervised metrics;
- target columns, label metadata, interval metadata, phase-boundary metadata, annotation metadata, and future-derived fields are absent from features;
- no future return, future volatility, future drawdown, future phase, transition outcome, or boundary-distance field enters training features;
- same-session target OHLCV values are not used where the feature contract requires lagged inputs;
- feature availability satisfies the point-in-time rule for the target anchor;
- preprocessing and feature selection are fitted on training data only;
- chronological folds are not shuffled;
- training dates strictly precede validation dates;
- duplicate target rows do not cross fold boundaries;
- validation labels and validation outcomes do not influence training, thresholds, calibration, or feature selection.

Any failed leakage check invalidates the fold.

## Future evaluation artifact contract

A future separately authorized modeling run must produce the following evaluation evidence:

```text
evaluation_manifest.json
fold_boundaries.csv
fold_metrics.csv
aggregate_metrics.json
per_class_metrics.csv
confusion_matrix.csv
baseline_metrics.json
validation_predictions.parquet
```

Required artifact semantics:

- `evaluation_manifest.json`: contract id/version, source commit, dataset identity, target source, feature schema identity, class order, fold count, and artifact inventory;
- `fold_boundaries.csv`: fold id, train start/end, validation start/end, eligible row counts, and per-class support;
- `fold_metrics.csv`: required candidate and baseline metrics by fold;
- `aggregate_metrics.json`: aggregation method and aggregate required metrics;
- `per_class_metrics.csv`: precision, recall, F1, and support for `B`, `S`, and `OUT`;
- `confusion_matrix.csv`: fixed row/column class order `B, S, OUT`;
- `baseline_metrics.json`: majority-class and class-prior baseline definitions and results;
- `validation_predictions.parquet`: eligible validation rows only, with fold id, target identity, `y_true`, `y_pred`, and probability columns when available.

Future evaluation artifacts must identify and exclude `UNLABELED` rows from supervised outputs. Coverage-only counts for excluded rows, if emitted, must be separate from supervised metrics.

These declarations do not authorize creation of the artifacts in Phase 7.0.

## Forbidden capabilities and authority boundary

This contract does not authorize:

- model training or fitting;
- prediction or probability generation;
- threshold optimization;
- trading-signal generation;
- broker or order actions;
- external data ingestion or provider calls;
- dataset materialization or generated data;
- runtime execution;
- server commands or server apply;
- direct writes to `main`;
- merge.

Merge authority and server-apply authority remain `PM_L2_ONLY`.
