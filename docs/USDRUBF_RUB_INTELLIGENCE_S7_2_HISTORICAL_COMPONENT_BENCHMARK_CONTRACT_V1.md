# USDRUBF RUB Intelligence — S7.2 Historical Component Benchmark Contract v1

## Status And Scope

```text
PROJECT=MOEX_Bot
STAGE=S7.2
MODE=historical_component_benchmark
INSTRUMENT=USDRUBF
LIVE_RUNTIME_MUTATION=none
BROKER_ORDER_AUTHORITY=none
TELEGRAM_DEPENDENCY=none
FLOWISE_DEPENDENCY=none
```

S7.2 evaluates deterministic components that can be reconstructed causally from the accepted historical USDRUBF 5-minute market dataset.

It does **not** claim to evaluate the full Decision Agent. The operational scheduler is currently pinned to `SAFE_WAIT`; there is no frozen non-SAFE_WAIT production decision policy whose historical outputs can be reconstructed honestly.

## Full Decision Agent Blocker

Current operational `safe_wait_decision_agent` semantics are:

```text
final_bias = deterministic trend
trade_state = WAIT
confidence = 0.25
```

`WAIT` is forced by the shadow safety boundary. Therefore backtesting this output as if it were the intended trading intelligence would conflate an infrastructure safety mode with a trading policy.

S7.2 records:

```text
FULL_DECISION_AGENT_EVALUATED=false
BLOCKER=no_frozen_non_SAFE_WAIT_production_decision_policy
```

Full-agent benchmarking must wait until a bounded production decision policy is frozen in a later stage.

## Components Evaluated

### Current Operational EMA Component

The replay uses `build_live_decision_input`, so EMA semantics match the current live bridge:

```text
closed historical 5m bars
-> complete aligned 15m bars
-> current EMA(3/19) runtime replay
-> BULLISH_USD / NEUTRAL / BEARISH_USD
```

This is **not** the older D1 EMA crossover research baseline. Results must not be described as D1 EMA results.

The current bridge emits confidence `1.0` when the EMA context is available. S7.2 preserves that fact; it does not recalibrate confidence before evaluation.

Two explicit views are produced:

1. `EMA bias-only`: same directional prediction, `trade_state=WAIT`, so exposure remains `OUT`. This isolates directional classification quality.
2. `EMA always-active baseline`: bullish/bearish EMA direction is mapped to synthetic `HOLD` exposure only for benchmark comparison. This is a research baseline, not an approved production trading rule.

### Current Level / Structure Component

For each prediction day, S7.2 reconstructs:

- previous complete trading-session high zone;
- previous complete trading-session low zone;
- current-session interaction state against each zone;
- structural quality;
- current deterministic `market_regime`.

No new directional trading rule is invented for structure. Instead, S7.2 reports future outcome distributions grouped by:

- `market_regime`;
- exact high/low interaction signature.

This allows later stages to determine whether specific structural states carry measurable information before assigning directional authority.

## Historical Input Contract

The runner accepts an explicit immutable CSV path through:

```text
--source-dataset-path
```

Requirements:

- existing regular file;
- no symlink;
- `.csv` suffix;
- no mutable path alias token `latest`, `current`, or `autodetect`;
- required normalized OHLC fields;
- volume required because the current live bridge bar contract includes volume;
- timestamps strictly increasing and unique under the existing 5m normalizer.

No server path is hard-coded or inferred by S7.2.

## Complete Trading-Day Rule

S7.2 reuses the existing D1 aggregation contract. A trading day enters the replay universe only if the existing D1 builder considers the bucket finalized.

The current completeness rule requires the last normalized bar timestamp to be at or after `18:50` Moscow time.

Incomplete trailing days are excluded rather than filled or extrapolated.

## Point-In-Time Replay Boundary

For each prediction day `D`:

```text
prior context = immediately preceding complete trading day
current facts = only bars belonging to D and observable by D's replay as_of
future labels = joined only after the prediction row is frozen
```

The previous session used for Level/Structure may be earlier than `--start-date`. This is intentional and required for causal reconstruction of the first requested prediction day.

Likewise, trading days later than `--end-date` may supply forward label prices for an in-range prediction row. Those later rows are label-domain data only and are never passed into `build_live_decision_input` for the earlier prediction.

Therefore date filtering occurs **after** full causal replay and forward-label attachment:

```text
full history
-> causal prediction replay
-> post-hoc label join
-> filter prediction rows to requested start/end window
```

This prevents both loss of prior-session context and artificial end-window label truncation.

## Excluded Historical Inputs

S7.2 v1 deliberately excludes:

```text
FUTOI = BLOCKED / no directional authority
News = excluded from historical component replay
Macro = excluded from historical component replay
Oil = excluded
CNY = excluded
```

These exclusions are not interpreted as neutral evidence. They simply mean S7.2 v1 is a component benchmark, not a complete historical reconstruction of the eventual intelligence stack.

## Forward Labels

Default trading-day horizons:

```text
1
3
5
10
```

Future label price is the complete D1 close at `D+h` in trading-day sequence.

Realized classification uses the S7.1 contract:

```text
forward_return_bps = (future_price / frozen_prediction_price - 1) * 10000
```

with explicit `neutral_band_bps`.

A production neutral-band threshold is not approved by S7.2 v1. Sensitivity testing remains required before calibration.

## Artifacts

Declared outputs:

```text
run_metadata.json
historical_replay_rows.csv
ema_bias_only_metrics.json
ema_always_active_metrics.json
structure_forward_summary.csv
quality_report.json
```

`historical_replay_rows.csv` contains frozen prediction facts and post-hoc future prices in one research artifact. Future-price columns are labels only and must never be reused as prediction features.

## Structure Summary Semantics

For each `market_regime` and exact structure signature, per horizon:

- observation count;
- mean forward return bps;
- median forward return bps;
- bullish realized rate;
- neutral realized rate;
- bearish realized rate.

No `BUY`, `SELL`, `ENTER`, `EXIT`, or confidence is inferred from these groups in S7.2.

## Acceptance Criteria

S7.2 code acceptance requires:

```text
CURRENT_LIVE_BRIDGE_COMPONENTS_REPLAYED=yes
PRIOR_SESSION_BEFORE_START_PRESERVED=yes
POST_END_FORWARD_LABELS_ALLOWED=yes_label_domain_only
FUTURE_DATA_IN_DECISION_INPUT=no
EMA_BIAS_ONLY_METRICS=yes
EMA_ALWAYS_ACTIVE_BASELINE=yes_research_only
STRUCTURE_OUTCOME_GROUPING=yes
STRUCTURE_DIRECTIONAL_RULE_INVENTED=no
FULL_DECISION_AGENT_EVALUATED=no_blocked
UNIT_TESTS=pass
FULL_REPOSITORY_CI=pass
LIVE_RUNTIME_CHANGED=no
BROKER_ORDER_EXECUTION=no
```

Code acceptance alone does not complete empirical S7.2. A controlled run against the accepted canonical historical dataset must still produce and review the declared artifacts.

## Handoff

After empirical S7.2 results are available:

1. compare EMA directional edge across 1/3/5/10 horizons;
2. identify Level/Structure states with stable conditional outcome differences;
3. inspect regime/transition subsets rather than whole-period averages only;
4. test neutral-band sensitivity;
5. compare against trivial unconditional direction/base-rate benchmarks;
6. use the findings to inform S7.3 production decision/playbook design;
7. only then freeze a non-SAFE_WAIT Decision policy eligible for full-agent historical evaluation.
