# USDRUBF completed-session reversal candle monitor v1

## Purpose and authority

The monitor evaluates the last completed Moscow trading date at 00:20 MSK from
five-minute MOEX ALGOPACK TradeStats and optional FUTOI. It detects a candidate
bearish reversal after a local high. It does not identify retrospective H3/H4/H5
labels, predict a precise correction target, place orders, or grant execution
authority.

The research basis is the 2022-08-01 through 2026-09-02 event study of 15 H3+
maxima, 30 matched H1/H2 controls, and the separate 2026-10-06 reference case.
The study found that high activity and post-peak selling were common, but no
universal pre-peak FUTOI signal distinguished major from minor highs. The rules
therefore require price rejection and an intraday transition. FUTOI adds one
optional point only.

## Inputs and data gates

- Instrument: `USDRUBF`; interval: five minutes.
- The evaluated date must be earlier than the current Moscow date.
- Exact-date TradeStats must exist, contain at least 60 unique bars, positive
  prices and volume, and satisfy `vol_b + vol_s == vol` on every row.
- At least 21 earlier observed sessions are required.
- Relative volume is current volume divided by the median of up to 20 earlier
  sessions of the same weekday/weekend type.
- ATR20 is the arithmetic mean of the previous 20 daily true ranges.
- Missing or unbalanced FUTOI is omitted. It is never zero-filled and cannot
  block an otherwise valid price signal.
- A FUTOI endpoint must contain both FIZ and YUR, have exact zero aggregate net
  position, and be no more than 10 minutes older than the corresponding final
  TradeStats bar.

Invalid data produces `DATA_BLOCKED`; an absent session produces `NO_SESSION`.
Neither state may generate a market alert.

## Mandatory alert gates

All three gates must pass:

1. The session high is at least the maximum of the previous five observed
   sessions.
2. Price rejection: upper wick is at least 40% of the daily range and the close
   is in the lower 50% of the daily range.
3. Intraday confirmation: within 60 clock minutes after the first session high,
   a five-minute close breaks the lows of the preceding six contiguous bars with
   negative bar delta; or the session closes below VWAP and the high-to-close
   distance is at least 0.5 ATR20.

## Score

After the mandatory gates, the monitor adds:

| Evidence | Points |
| --- | ---: |
| Upper wick at least 40% | 2 |
| Close in lower 35% of range | 2 |
| Close below session VWAP | 1 |
| Six-bar support break with negative delta within 60 minutes | 2 |
| High-to-close distance at least 0.5 ATR20 | 1 |
| Relative volume at least 1.3x | 1 |
| Volume at least 1.25x the previous session | 1 |
| Daily signed flow at most -2% of volume | 1 |
| Closing total open interest change at most -1% | 1 |
| Complete post-peak 60-minute flow at most -5% | 1 |
| FIZ long change at most -1 percentage point of one-sided OI | 1 |
| Previous session rose and closed in its upper 25% | 1 |

`ALERT` requires all mandatory gates and at least 10 points. `WATCH` requires the
new-high and price-rejection gates plus at least 7 points, but never produces a
notification. All other valid sessions are `NO_ALERT`.

The point system is an explicit operational heuristic, not a calibrated
probability of an H3/H4/H5 top. It deliberately permits an alert without a
bearish FUTOI change, as in the atypical 2026-10-06 reference case.

On the frozen research sample, this threshold identifies 5 of 15 H3+ events,
5 of 30 matched H1/H2 controls, and the separate 2026-10-06 reference case. The
counts are an in-sample diagnostic, not an out-of-sample performance estimate.

## Persistence and notification handoff

Each run atomically writes:

- `${MOEX_DATA_ROOT}/state/usdrubf_reversal_candle/current.json`;
- `${MOEX_DATA_ROOT}/state/usdrubf_reversal_candle/history/YYYY-MM-DD.json`.

Only `status=ALERT` sets `notify=true` and supplies a bounded Russian
`notification` text. `WATCH`, `NO_ALERT`, `NO_SESSION`, and `DATA_BLOCKED` stay
silent. The artifact is the server-to-chat handoff and retains
`execution_authority=false`.

The systemd timer `moex-usdrubf-reversal-candle.timer` runs every calendar day
at 00:20 Europe/Moscow. It fetches the prior calendar date directly from MOEX,
so it does not depend on the Stage 10 refresh scheduled for 00:30.
