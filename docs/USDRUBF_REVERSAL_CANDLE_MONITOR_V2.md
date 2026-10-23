# USDRUBF completed-session two-sided reversal monitor v2

## Purpose

The monitor evaluates the last completed Moscow session at 00:20 MSK on
five-minute MOEX ALGOPACK TradeStats. It reports candidates for:

- a bearish reversal after a local high;
- a bullish reversal after a local low.

It does not assign retrospective H/L labels in production, predict a precise
target, place orders or grant execution authority.

## Common data gates

- Instrument `USDRUBF`, five-minute interval.
- The evaluated date must precede the current Moscow date.
- At least 60 unique bars, positive prices and volume, and exact
  `vol_b + vol_s == vol` are required.
- At least 21 earlier observed sessions are required for ATR20 and relative
  volume. Weekday and weekend volume baselines are kept separate.
- Missing or unbalanced FUTOI is omitted without zero filling. It cannot block
  an otherwise valid price signal.
- Mandatory source or calculation failure yields `DATA_BLOCKED` and never
  notifies. An absent session yields `NO_SESSION`.

## Bearish rule after a high

The v1 rule is preserved. `ALERT` requires all three gates:

1. Session high is at least the maximum of the previous five sessions.
2. Upper wick is at least 40% of the range and close location is at most 50%.
3. Within 60 minutes after the first final high, a close breaks the preceding
   six contiguous lows with negative bar delta; or the session closes below
   VWAP and high-to-close distance is at least 0.5 ATR20.

The original score and alert threshold 10 remain unchanged.

## Bullish rule after a low

`ALERT` requires all three gates:

1. Session low is at most the minimum of the previous five sessions.
2. Lower wick is at least 40% of the range and close location is at least 50%.
3. Within 60 minutes after the first final low, a close breaks the preceding
   six contiguous highs with positive bar delta; or the session closes above
   VWAP and low-to-close distance is at least 0.5 ATR20.

The bullish score is:

| Evidence | Points |
| --- | ---: |
| Lower wick at least 40% | 2 |
| Close in upper 35% of range | 2 |
| Close above session VWAP | 1 |
| Six-bar resistance break with positive delta within 60 minutes | 2 |
| Low-to-close distance at least 0.5 ATR20 | 1 |
| Relative volume at least 1.3x | 2 |
| Volume at least 1.25x the previous session | 1 |
| Daily signed flow at most -2% of volume | 1 |
| Closing total OI change at most -1% | 1 |
| Previous session fell and closed in its lower 25% | 1 |

`ALERT` requires all mandatory gates and at least 10 points. `WATCH` requires
the new-low and price-rejection gates plus at least 7 points, but remains
silent. FUTOI is not scored for the bullish direction: the minimum study found
no stable bullish FUTOI confirmation.

Negative flow and falling OI are intentional in this rule. They represent
capitulation or position liquidation followed by price rejection, rather than
requiring buying pressure to be positive over the full session.

## Research evidence and limits

The bearish rule uses the v1 H3/H4/H5 study. The bullish rule uses 14 L3+
events, including five L4+ and two L5+, and 28 matched L1/L2 controls through
2026-10-06. The frozen score >=10 diagnostic identifies 4/14 events and 1/28
controls, including both L5+ events. On the post-2023-06 subset it identifies
2/11 events and 1/22 controls.

These are in-sample counts. The labels and extreme alignment are retrospective,
and next-session returns did not outperform controls. The alert means candidate
session-level exhaustion, not a calibrated probability or long/short entry.

Reproduction code and aggregate evidence are in
`research/usdrubf_reversal_minima_20261023/`.

## Output and scheduling

The artifact keeps the existing handoff paths:

- `${MOEX_DATA_ROOT}/state/usdrubf_reversal_candle/current.json`;
- `${MOEX_DATA_ROOT}/state/usdrubf_reversal_candle/history/YYYY-MM-DD.json`.

`decisions.bearish` and `decisions.bullish` retain separate gates, scores and
statuses. Top-level `status=ALERT` and `notify=true` are set when either
direction alerts. `alert_directions` identifies the direction and the Russian
notification names the corresponding high or low. The legacy `decision` field
continues to expose the bearish decision for compatibility.

The existing systemd timer remains daily at 00:20 Europe/Moscow. No scheduler
or delivery-path change is required.
