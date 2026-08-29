# USDRUBF RUB Intelligence — S7.2 Stability Analysis Contract v1

```text
PROJECT=MOEX_Bot
STAGE=S7.2
MODE=EMPIRICAL_STABILITY_ANALYSIS
RUNTIME_AUTHORITY=none
BROKER_ORDER_AUTHORITY=none
```

## Purpose

This contract hardens the S7.2 historical component benchmark after the sparse-safe replay achieved full historical coverage on the exact accepted Phase-3 manifest.

The stability analysis is descriptive research only. It does not create a trading rule, does not alter the live bridge, and does not grant directional authority to EMA or structure features.

## Inputs

The source boundary is unchanged from S7.2:

- exact explicit immutable CSV, or
- exact accepted Phase-3 panel manifest with its recorded input partitions;
- no directory scan;
- no `latest`, `current` or autodetect path selection;
- future prices are post-hoc labels only.

Default horizons remain:

```text
1,3,5,10 trading days
```

## EMA Majority-Class Reference

For every horizon the analysis reports:

- EMA directional accuracy;
- realized class distribution;
- deterministic majority realized class;
- majority-class accuracy;
- `EMA accuracy - majority accuracy`;
- mean forward return signed by EMA direction.

The majority-class reference is explicitly **post-hoc and descriptive**. It uses realized labels from the evaluation sample and therefore is not a causal deployable predictor.

It exists only to answer whether EMA beats the simplest class-frequency reference on the same labels.

## Year-by-Year EMA Stability

The same metrics are reported separately by trade year and horizon.

This is intended to detect:

- a result driven by one calendar period;
- sign changes in signed return;
- accuracy instability;
- changing realized class imbalance.

No year is silently dropped because of weak performance.

## Structure Year-by-Year Stability

The analysis reports year-by-year forward outcomes for:

- `market_regime`;
- exact `structure_signature`.

For each year / horizon / group it records:

- count;
- mean and median forward return bps;
- bullish / bearish / neutral realized rates;
- a deterministic minimum-sample gate.

Default minimum sample threshold:

```text
20 observations per year / horizon / group
```

The threshold is configurable with:

```text
--min-group-sample
```

A separate summary reports how many years pass the sample gate and whether the directional majority is bullish in every passing year, bearish in every passing year, mixed, or insufficient.

This remains descriptive evidence. No structure group becomes a trading rule merely because it is directionally consistent.

## Sparse-Safe vs Complete-Only 15m Sensitivity

Two historical EMA reconstructions are compared from the same native 5m source:

### Sparse-safe

```text
minimum constituents per non-empty closed 15m bucket = 1
```

Observed native 5m rows are aggregated without imputation.

### Complete-only sensitivity

```text
minimum constituents per closed 15m bucket = 3
```

Incomplete non-empty buckets are dropped entirely.

The complete-only variant does **not**:

- forward fill;
- back fill;
- synthesize missing 5m bars;
- carry prices;
- interpolate;
- shift timestamps.

Both policies retain the same nominal-close causality guard.

The sensitivity output reports:

- coverage of each reconstruction;
- number/rate of matching EMA directions on common trade dates;
- number of changed EMA-direction days;
- horizon metrics under each reconstruction;
- complete-only minus sparse accuracy and signed-return deltas.

## Level / Structure Boundary

Level and Structure remain calculated from the original native 5m observations under both EMA sensitivity variants.

The 15m reconstruction is used only for the EMA component.

## Runtime Boundary

This analysis does not modify:

- `usdrubf_live_shadow_bridge.py` fail-closed behavior;
- scheduler behavior;
- Telegram;
- Flowise;
- broker execution;
- Stage 3-9 canonical data pipelines.

```text
LIVE_BRIDGE_RUNTIME_SEMANTICS_RELAXED=no
MISSING_5M_IMPUTATION=no
HISTORICAL_SENSITIVITY_ONLY=yes
```

## Declared Outputs

The runner produces:

```text
run_metadata.json
quality_report.json
historical_sparse_replay_rows.csv
historical_complete_only_replay_rows.csv
historical_sparse_replay_exclusions.csv
historical_complete_only_replay_exclusions.csv
ema_overall_stability.json
ema_yearly_stability.csv
structure_yearly_stability.csv
structure_stability_summary.csv
ema_sparse_vs_complete_sensitivity.json
```

## Acceptance Boundary

S7.2 stability evidence may be accepted only after:

```text
SOURCE_PROVENANCE=exact
FULL_HISTORY_RUN=completed
EMA_MAJORITY_REFERENCE=reported
EMA_YEARLY_STABILITY=reported
STRUCTURE_YEARLY_STABILITY=reported
MIN_SAMPLE_GATE=reported
SPARSE_VS_COMPLETE_SENSITIVITY=reported
MISSING_5M_IMPUTATION=no
LIVE_RUNTIME_CHANGED=no
FULL_REPOSITORY_CI=pass
```

Full Decision Agent evaluation remains outside this component analysis while no frozen non-SAFE_WAIT production decision policy is being evaluated here.
