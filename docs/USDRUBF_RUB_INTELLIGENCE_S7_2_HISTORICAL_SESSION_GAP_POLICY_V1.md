# USDRUBF RUB Intelligence — S7.2 Historical Session Gap Policy v1

```text
PROJECT=MOEX_Bot
STAGE=S7.2
POLICY=FAIL_CLOSED_EXCLUDE_DAY_NO_REPAIR
LIVE_RUNTIME_MUTATION=none
```

## Trigger

The first empirical S7.2 run against the accepted Phase-3 manifest stopped on:

```text
LiveShadowBridgeError: broken 15m bucket aligned to broker label 2022-05-31T14:00:00+03:00
```

The accepted raw 5m materialization contract records gaps but does not require `gap_count=0` for quality status `pass`. Therefore an accepted historical partition may still be incompatible with the stricter current live 15m bridge.

## Governing Rule

S7.2 must preserve current live-bridge semantics. It may not make historical rows pass by:

- synthesizing missing 5m bars;
- forward/back filling OHLCV;
- shifting timestamps;
- deleting selected bars from an otherwise failing session;
- weakening `build_closed_15m_bars` validation;
- changing live runtime behavior for the purpose of improving backtest coverage.

If `build_live_decision_input` raises `LiveShadowBridgeError` for prediction day `D`, S7.2 records that day as ineligible and continues with later days.

The skipped day remains part of the chronological D1 price sequence for post-hoc forward labels. It is excluded only from the prediction universe. This preserves trading-day horizon semantics and avoids selecting future labels only from bridge-eligible days.

## Required Evidence

S7.2 writes:

```text
replay_exclusions.csv
```

with:

```text
trade_date
prior_trade_date
error_type
reason
current_bar_count
prior_bar_count
```

`quality_report.json` records:

```text
candidate_prediction_days
live_bridge_eligible_prediction_days
live_bridge_excluded_prediction_days
live_bridge_coverage
live_bridge_exclusion_reasons
prediction_window_excluded_days
historical_session_gaps_repaired=false
historical_bars_synthesized=false
live_bridge_runtime_semantics_relaxed=false
```

## Interpretation

Excluded days are not neutral observations and are not failed directional predictions. They are unavailable predictions under the current live input contract.

If exclusion coverage is material, S7.2 results must be treated as conditional on live-bridge eligibility. Before S7.3 can claim broad historical representativeness, the exclusion distribution must be reviewed by date/regime and the underlying session-gap pattern must be classified.

A high exclusion rate is evidence of a historical/live session-semantics compatibility problem, not evidence about EMA directional quality.

## Acceptance Boundary

```text
HISTORICAL_BARS_REPAIRED=no
LIVE_BRIDGE_VALIDATION_WEAKENED=no
INELIGIBLE_DAYS_RECORDED=yes
INELIGIBLE_DAYS_EXCLUDED_FROM_PREDICTIONS=yes
INELIGIBLE_DAYS_RETAINED_IN_FORWARD_LABEL_CALENDAR=yes
COVERAGE_REPORTED=yes
LIVE_RUNTIME_CHANGED=no
BROKER_ORDER_EXECUTION=no
```
