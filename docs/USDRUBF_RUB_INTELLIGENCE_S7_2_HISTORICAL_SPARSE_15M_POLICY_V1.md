# USDRUBF RUB Intelligence — S7.2 Historical Sparse 15m Policy v1

```text
PROJECT=MOEX_Bot
STAGE=S7.2
POLICY=HISTORICAL_SPARSE_NATIVE_5M_TO_15M
LIVE_RUNTIME_BEHAVIOR=unchanged_fail_closed
BROKER_ORDER_AUTHORITY=none
```

## Why This Policy Exists

The controlled S7.2 replay against the exact accepted Phase-3 manifest initially attempted to reuse the live bridge's strict `3/3` 5-minute constituent rule for every 15-minute EMA bucket.

After documented FORTS clearing gaps at 14:00 and 19:00 were handled, the controlled run still produced:

```text
candidate_prediction_days=1027
eligible_days=480
excluded_days=547
coverage=0.4673807205452775
```

All 547 exclusions were `broken 15m bucket` errors.

A direct diagnostic of the exact raw partitions used by the Phase-3 manifest found:

```text
missing exactly 1 native 5m slot = 511 buckets
missing exactly 2 native 5m slots = 36 buckets
```

The gaps were distributed across many clock labels rather than a new fixed exchange-session boundary.

## Repository Data-Contract Evidence

The current canonical `futures_raw_5m.v1` materializer records `gap_count` as a diagnostic metric but does not make timestamp gaps a hard failure when identity, OHLC, duplicate-key and monotonicity checks pass.

The current canonical D1 resampler aggregates the native rows that are actually present for a trading day and does not require a complete wall-clock 5-minute grid.

The existing intraday feature implementation `src/moex_features/intraday/si_15m_ohlc_from_5m.py` likewise resamples available 5-minute observations into 15-minute OHLC bars and drops empty buckets rather than requiring exactly three constituent rows.

Therefore the historical benchmark must not interpret every absent native 5-minute row as a corrupt day.

## Historical-Only 15m Semantics

S7.2 historical sparse replay uses the following rule:

```text
accepted native 5m rows observable by as_of
→ floor into aligned 15m bucket labels
→ use only non-empty buckets
→ require nominal bucket close (label + 10 minutes) <= as_of
→ OHLCV from observed rows only
→ current EMA(3/19) state engine
```

For each non-empty 15-minute bucket:

```text
open   = first observed 5m open
high   = max observed 5m high
low    = min observed 5m low
close  = last observed 5m close
volume = sum observed 5m volume
source_available_at = timestamp of last observed constituent
```

No missing 5-minute row is synthesized.

Explicitly forbidden:

```text
forward fill
back fill
zero-volume synthetic bar creation
price carry-forward
interpolation
timestamp shift
future-row use
```

## Causal Guard

A sparse bucket is not usable merely because it has at least one observed row.

The nominal close guard is:

```text
bucket_label + 10 minutes <= as_of_timestamp
```

This preserves the current live bridge convention that an aligned `HH:00/05/10` bucket becomes closed only at the `+10` boundary, while allowing the historical benchmark to tolerate an absent native row inside a bucket.

`source_available_at` remains the last actual constituent timestamp, never an invented bucket-close timestamp.

## Level / Structure Semantics

Level and structure calculations remain based on the original native 5-minute observations:

- previous-session high/low zones use observed prior-session bars;
- current level interactions use observed current-session bars;
- no sparse 15-minute synthetic OHLC is fed into Level/Structure;
- no directional structure rule is added by this policy.

Only the historical EMA replay path uses sparse 15-minute aggregation.

## Live Runtime Boundary

This policy does **not** alter `build_closed_15m_bars` or `build_live_decision_input` live behavior.

Current live semantics remain fail-closed for non-governed broken 15-minute triples.

The historical sparse bridge is implemented as a separate research-only module and runner.

Therefore:

```text
LIVE_BRIDGE_RUNTIME_SEMANTICS_RELAXED=no
LIVE_MISSING_5M_TOLERANCE_ADDED=no
HISTORICAL_RESEARCH_SPARSE_TOLERANCE=yes
```

## Runner

Historical empirical replay uses:

```text
python -m src.moex_research.runners.usdrubf_s7_2_historical_sparse_component_benchmark
```

The source contract remains identical to S7.2 v1:

- exact explicit CSV, or
- exact accepted Phase-3 panel manifest with exact recorded input partitions;
- no directory scan;
- no `latest/current/autodetect` resolution;
- future prices remain post-hoc labels only.

## Output Evidence

The replay rows additionally record:

```text
ema_bar_count
ema_sparse_bucket_count
ema_min_constituent_count
```

`quality_report.json` records:

```text
historical_sparse_eligible_prediction_days
historical_sparse_excluded_prediction_days
historical_sparse_coverage
historical_sparse_exclusion_reasons
historical_missing_5m_imputed=false
historical_bars_synthesized=false
historical_timestamps_shifted=false
live_bridge_runtime_semantics_relaxed=false
```

## Acceptance Gate

The sparse policy is code-accepted only when:

```text
SPARSE_NATIVE_5M_AGGREGATION=yes
NOMINAL_CLOSE_GUARD=yes
MISSING_5M_IMPUTATION=no
HISTORICAL_BAR_SYNTHESIS=no
LEVEL_STRUCTURE_ON_NATIVE_5M=yes
LIVE_BRIDGE_RUNTIME_CHANGED=no
UNIT_TESTS=pass
FULL_REPOSITORY_CI=pass
```

Empirical S7.2 remains unaccepted until the new runner is executed against the same exact Phase-3 manifest and the resulting coverage and component metrics are reviewed.

Full Decision Agent evaluation remains blocked because the operational scheduler is still pinned to `SAFE_WAIT`; this policy does not change that boundary.
