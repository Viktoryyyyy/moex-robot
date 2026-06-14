# USDRUBF Event ML Research Design v1

Status: design_only
Project: MOEX Bot
Strategy candidate: usdrubf_event_ml
Instrument scope: Si / USDRUBF futures research
Timeframe scope: D1 primary, optional 5m context later
Runtime scope: forbidden in v1
Cloud scope: not required in v1

## 1. Purpose

Define the first reproducible research design for an event-conditioned ML model on Si / USDRUBF.

The model class is a tabular event-conditioned ensemble:

- primary candidate: LightGBM or XGBoost classifier for direction probability
- optional secondary candidate: LightGBM or XGBoost regressor for normalized next-day return
- required baselines: random walk, logistic regression, deterministic event rules
- later benchmark only: LSTM / Transformer

This document is not a trading approval and not a live strategy specification.

## 2. Research hypothesis

A tabular model trained only on information known by the close of event day E can improve out-of-sample directional filtering for the next tradable session compared with random walk, logistic regression, and simple event rules.

The model should not predict every day. It should score only event-conditioned rows where at least one approved market event is active.

## 3. Time semantics

- event_day_symbol: E
- setup_day_symbol: E
- known_by_when: after official close of E and before the first executable point of E+1
- earliest_executable_point: next regular tradable session after E
- primary_outcome_windows:
  - close(E) to close(E+1)
  - close(E) to close(E+2), fragility only
- secondary_execution_windows:
  - open(E+1) to close(E+1)
  - open(E+1) to high/low(E+1), only if intraday or reliable D1 high/low semantics are approved

Primary research labels and secondary execution-compatible labels must remain separate.

## 4. Event definitions

### 4.1 large_daily_move

A row is eligible when at least one condition is true:

- abs(close_to_close_return_1d) is in the rolling 252-day top 15 percent
- abs(close_to_close_return_1d) / ATR20 is greater than or equal to 1.0
- abs(close - open) / ATR20 is greater than or equal to 0.8

All thresholds are initial design defaults and must be swept in fragility checks.

### 4.2 volatility_compression

A row is eligible when at least one condition is true:

- ATR20 percent rank over 252 trading days is less than or equal to 0.25
- rolling 5-day high-low range / ATR20 is less than or equal to 1.5
- volatility z-score is less than or equal to -0.3 if the canonical regime feature exists

### 4.3 breakout_rejection

A row is eligible when price interacts with an approved level artifact.

Initial D1-only semantics:

- approach: abs(close(E) - nearest_level) / ATR20 less than or equal to 0.5
- breakout: close(E) is beyond nearest_level by at least 0.2 ATR20
- rejection: high(E) or low(E) touches the nearest_level zone, but close(E) returns inside the prior side

If the existing Levels & Breakout/Rejection artifact is available in repo contracts, the script must use that contract instead of redefining the event ad hoc.

### 4.4 regime_transition

A row is eligible when an approved regime feature marks one of:

- enter_trend
- vol_upshift
- exit_flat
- volatile_risk_on

If the canonical R1-R4 or FLAT/TREND/VOLATILE regime artifact is available, it must be used as prior information only.

## 5. Minimal feature set

All features must be computable by known_by_when.

Required D1 features:

- return_1d, return_2d, return_5d
- range_1d, body_1d, upper_wick_1d, lower_wick_1d
- ATR20, ATR20 percentile, ATR20 z-score
- rolling realized volatility 5/10/20
- close position inside daily range
- distance to nearest approved support/resistance level in ATR units
- event flags from section 4
- regime_day if canonical feature exists
- day_of_week and expiration-week flag if calendar contract supports it

Optional later features:

- FUTOI open interest changes
- ALGOPACK tradestats
- ALGOPACK obstats
- intraday regime_5m

Optional features must not block the v1 D1-only design.

## 6. Labels

### 6.1 Primary research labels

Primary labels answer whether the event had predictive information, not whether a live trade could be executed.

- y_direction_1d: sign(close(E+1) - close(E))
- y_return_atr_1d: (close(E+1) - close(E)) / ATR20(E)
- y_abs_move_bucket_1d: small / medium / large bucket based on ATR-normalized move

### 6.2 Secondary execution-compatible labels

Secondary labels are used only after the primary research result is understood.

- y_exec_direction_oc_1d: sign(close(E+1) - open(E+1))
- y_exec_return_atr_oc_1d: (close(E+1) - open(E+1)) / ATR20(E)
- y_exec_mfe_mae_atr_1d: high/low based excursion from open(E+1), only if approved semantics exist

Secondary labels must not be presented as the primary answer to the hypothesis.

## 7. Model design

### 7.1 Baselines

Required baselines:

- random_walk_direction: prior direction equals last observed direction or 50/50 neutral
- logistic_regression_direction: regularized logistic regression on the same features
- deterministic_event_rule: rule-only direction by event type and regime

### 7.2 Candidate models

Required candidate models:

- lightgbm_classifier_direction if dependency is available
- xgboost_classifier_direction if dependency is available
- sklearn_hist_gradient_boosting_classifier as fallback if neither LightGBM nor XGBoost is available

Optional candidate:

- gradient_boosting_regressor_return_atr

### 7.3 Probability policy

The model output must be a probability table, not only hard signals.

Required fields:

- prob_up
- prob_down
- predicted_side
- confidence
- event_mask
- model_version
- feature_schema_version

No live order intent is allowed in v1.

## 8. Validation design

Forbidden:

- random train/test split
- using future labels or future-dependent features
- fitting scalers or thresholds on the full dataset before split
- selecting thresholds on test folds

Required:

- chronological walk-forward validation
- train/validation/test separation
- threshold selection only on training/validation data
- event-level diagnostics by event type and regime
- leakage check that every feature timestamp is less than or equal to known_by_when

Suggested initial split if enough history exists:

- train: expanding or rolling window with at least 500 trading days
- validation: next 120 trading days
- test: next 120 trading days
- repeat until data ends

If history is insufficient, the run must report insufficient_data instead of inventing metrics.

## 9. Metrics

Primary ML metrics:

- balanced_accuracy
- precision_up and precision_down at selected thresholds
- ROC_AUC if both classes exist in the fold
- Brier score
- calibration by probability bucket

Trading-adjacent diagnostics, secondary only:

- average y_exec_return_atr_oc_1d at allowed thresholds
- hit rate by threshold bucket
- event count after filters
- worst fold result
- max drawdown proxy if execution-compatible labels are available

## 10. Pass/fail criteria

The design passes the first screening only if all conditions are true:

- no leakage or timing-anchor violation is detected
- at least 3 walk-forward test folds are available, or the run is explicitly provisional
- the candidate model beats random walk on balanced_accuracy in most test folds
- the candidate model beats logistic regression on at least one primary metric without materially worse calibration
- selected probability thresholds do not collapse the sample below the minimum event count
- no single event type explains the entire result

Minimum event count defaults:

- at least 30 scored events across all test folds
- at least 10 scored events per side for any side-specific conclusion

If these counts are not met, status must be provisional or insufficient_data.

## 11. Artifact contracts

No absolute server paths are declared in this design.

### 11.1 Input artifacts

- artifact_id: usdrubf_event_ml_d1_bars
  - artifact_class: cli_argument
  - producer: canonical dataset builder or approved data script
  - consumer: first research script
  - format: parquet or csv
  - contract: --input-d1

- artifact_id: usdrubf_event_ml_levels_optional
  - artifact_class: cli_argument
  - producer: approved levels research artifact
  - consumer: first research script
  - format: parquet or csv
  - contract: --levels-path, optional

- artifact_id: usdrubf_event_ml_regime_optional
  - artifact_class: cli_argument
  - producer: approved regime feature artifact
  - consumer: first research script
  - format: parquet or csv
  - contract: --regime-path, optional

### 11.2 Output artifacts

- artifact_id: usdrubf_event_ml_feature_table
  - artifact_class: cli_argument
  - producer: first research script
  - consumer: reviewer / later trainer
  - format: parquet or csv
  - contract: --output-root/features.parquet or --output-root/features.csv

- artifact_id: usdrubf_event_ml_predictions_table
  - artifact_class: cli_argument
  - producer: first research script
  - consumer: reviewer
  - format: parquet or csv
  - contract: --output-root/predictions.parquet or --output-root/predictions.csv

- artifact_id: usdrubf_event_ml_metrics_table
  - artifact_class: cli_argument
  - producer: first research script
  - consumer: reviewer
  - format: json and csv
  - contract: --output-root/metrics.json and --output-root/metrics.csv

- artifact_id: usdrubf_event_ml_diagnostics_table
  - artifact_class: cli_argument
  - producer: first research script
  - consumer: reviewer
  - format: csv
  - contract: --output-root/diagnostics.csv

- artifact_id: usdrubf_event_ml_run_metadata
  - artifact_class: cli_argument
  - producer: first research script
  - consumer: reviewer
  - format: json
  - contract: --output-root/run_metadata.json

## 12. First script scope

The first executable research step must be one script only.

Suggested file:

- scripts/research/usdrubf_event_ml_first_pass.py

Allowed responsibilities:

- load explicit input paths from CLI
- validate required columns
- build D1 event flags
- build primary labels
- run baselines
- run candidate model only if dependency is available
- write declared output artifacts
- print a short machine-readable summary

Forbidden responsibilities:

- live trading
- broker or runtime integration
- implicit latest-file discovery
- cloud calls
- Telegram sending
- server path hardcoding
- modifying source datasets

## 13. Required critic checks before implementation

Before the first script is implemented, the design must be checked for:

- whether input D1 dataset contract already exists in repo
- whether continuous futures adjustment semantics are known
- whether levels/regime artifacts are accessible through explicit contracts
- whether close/open session semantics are canonical or provisional
- whether LightGBM/XGBoost dependencies exist or fallback must be used

If these are unresolved, implementation may still proceed as a provisional first pass, but the run result must not be closed as canonical.

## 14. Closeout status rules

- supported_canonical: allowed only after real server run and accepted data/calendar semantics
- supported_provisional: allowed after real server run with unresolved but non-blocking data semantics
- hold_pending_data_semantics: use when input dataset or calendar semantics block interpretation
- rejected_design: use when critic finds leakage or invalid timing anchors

No research conclusion may be claimed from this design file alone.
