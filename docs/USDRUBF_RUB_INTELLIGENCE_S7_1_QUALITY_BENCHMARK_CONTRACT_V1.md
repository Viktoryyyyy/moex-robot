# USDRUBF RUB Intelligence — S7.1 Quality Benchmark Contract v1

## Status

Stage: S7.1
Scope: deterministic evaluation framework only
Instrument: USDRUBF
Runtime authority: none
Broker/order authority: none
Telegram dependency: none
Flowise dependency: none

## Objective

Measure whether persisted RUB Intelligence decisions are useful before any additional model calibration or new signal receives factual/action authority.

The benchmark evaluates outputs already produced by the intelligence stack against future USDRUBF prices. It must not feed future labels into `DecisionInput`, `MarketState`, live shadow runtime, scheduler, or alert delivery.

## Evaluation Axes

The v1 benchmark measures:

1. `final_bias` classification quality:
   - `BULLISH_USD`
   - `NEUTRAL`
   - `BEARISH_USD`
2. action/exposure quality derived deterministically from existing fields:
   - `LONG_USD` when `trade_state in {ENTER,HOLD,ADD}` and `final_bias=BULLISH_USD`
   - `SHORT_USD` when `trade_state in {ENTER,HOLD,ADD}` and `final_bias=BEARISH_USD`
   - `OUT` otherwise
3. confidence calibration of the selected `final_bias` class;
4. high-confidence error rate;
5. active directional success rate;
6. missed directional opportunity while `OUT`;
7. signed forward return while active, reported in basis points without costs or sizing.

The benchmark does not claim strategy PnL, Sharpe, execution quality, transaction-cost-adjusted return, or portfolio-level performance.

## Horizons

Default evaluation horizons:

```text
1 trading day
3 trading days
5 trading days
10 trading days
```

The evaluator accepts an explicit alternative set. Missing end-of-sample future labels are never imputed. They are counted as `missing_label_count` and excluded only from the affected horizon denominator.

## Realized Direction Label

For each horizon:

```text
forward_return_bps = (future_price / as_of_price - 1) * 10000
```

Given explicit `neutral_band_bps`:

```text
forward_return_bps > +neutral_band_bps -> BULLISH_USD
forward_return_bps < -neutral_band_bps -> BEARISH_USD
otherwise                              -> NEUTRAL
```

S7.1 does not approve a production neutral-band threshold. `neutral_band_bps=0` is only the deterministic default for framework validation. S7.2 must test economically sensible bands and report sensitivity.

## Metrics

Per horizon:

- `eligible_count`
- `missing_label_count`
- `label_coverage`
- `bias_correct_count`
- `bias_accuracy`
- `selected_class_brier`
- `high_confidence_count`
- `high_confidence_error_rate`
- `active_count`
- `active_coverage`
- `active_directional_success_rate`
- `positive_active_return_rate`
- `mean_signed_return_bps_when_active`
- `out_count`
- `missed_directional_opportunity_rate`
- prediction distribution
- realized distribution
- exposure distribution

`selected_class_brier` is deliberately named as a selected-class calibration proxy because the current Decision output has one confidence value, not a complete probability vector `P(B)/P(S)/P(OUT)`. It must not be described as full multiclass Brier score.

## Point-In-Time Boundary

The benchmark has two logically separate data domains:

```text
prediction domain:
  only information available at as_of_timestamp

label domain:
  future USDRUBF prices used only after prediction is frozen
```

Future prices are forbidden from:

- DecisionInput construction;
- MarketState construction;
- level/structure calculation at the prediction timestamp;
- EMA/FUTOI/news/macro context at the prediction timestamp;
- confidence generation;
- scenario generation;
- alert generation.

Historical reconstruction in S7.2 must prove that each prediction row is frozen before its forward labels are joined.

## Data Quality Rules

Each `BenchmarkObservation` must have:

- instrument exactly `USDRUBF`;
- timezone-aware unique `as_of_timestamp`;
- finite positive as-of price;
- allowed `final_bias`;
- allowed existing `trade_state`;
- confidence in `0..1`;
- non-empty trend and market regime;
- future label horizons as positive integers;
- finite positive future prices.

Duplicate prediction timestamps fail closed.

## Interpretation Rules

The benchmark separates bias quality from action quality.

Example:

```text
final_bias=BULLISH_USD
trade_state=WAIT
```

This counts as a bullish bias prediction but `OUT` exposure. That distinction is intentional: the system may correctly identify direction while correctly or incorrectly deciding that entry quality is insufficient.

Likewise `REDUCE` and `EXIT` are always benchmarked as `OUT` exposure in v1. Position inventory is not reconstructed in S7.1.

## Acceptance Boundary

S7.1 is complete when:

- deterministic evaluator exists in repository;
- explicit semantics are documented;
- unit tests cover labels, exposure mapping, metrics, missing labels, and fail-closed validation;
- full repository CI passes;
- no live runtime, scheduler, alert, Flowise, Telegram, broker, or order path is changed.

S7.1 completion does not mean the intelligence model is validated.

## S7.2 Handoff

S7.2 must create a historical 2025–2026 dataset containing frozen decision outputs plus future labels and run the S7.1 evaluator across at least 1/3/5/10 trading-day horizons.

Required S7.2 analysis:

- whole-period metrics;
- uptrend/downtrend/range segmentation;
- phase-transition windows;
- confidence buckets;
- neutral-band sensitivity;
- false regime changes;
- missed reversals;
- early exits / excessive OUT;
- comparison of full intelligence against simpler baselines such as market structure and EMA-only views.

No calibration change may be promoted solely from in-sample improvement.
