# EMA Backtest Contract (Step 2)

## Purpose
This layer provides reusable pure functions for EMA signal generation and point-based backtest accounting on top of canonical OHLC bars. It is intentionally limited to strategy signal semantics, bar-level PnL semantics, and daily/segment summaries.

## Canonical Input Columns
All core functions require canonical OHLC columns from the Step 1 foundation:

- `ts`
- `open`
- `high`
- `low`
- `close`

If any required canonical columns are missing, the library fails explicitly.

## Direction Modes
EMA is always computed from `close`.

Given `ema_fast` and `ema_slow`:

- `trend_long_short`: base signal = `sign(ema_fast - ema_slow)` in `{-1,0,+1}`
- `trend_long_only`: base signal = `1` when `ema_fast > ema_slow`, else `0`
- `trend_short_only`: base signal = `-1` when `ema_fast < ema_slow`, else `0`

## Anti-Leakage Semantics
Signal is formed on bar `t` and applied as position on bar `t+1`:

- `position = signal.shift(1)`

This shift is mandatory and prevents same-bar lookahead leakage.

## Point-Based PnL Semantics
Backtest math uses point deltas, not percentage returns:

- `dclose = close.diff()`
- `trades = abs(position.diff())`
- `fee = trades * commission_points`
- `pnl_bar = position * dclose - fee`

## Commission Semantics
Commission is an explicit argument named `commission_points`.

Trade counting uses absolute position change per bar:

- `trades = abs(position.diff())`

Therefore a direct flip `+1 -> -1` counts as `2` trade actions.

## Daily Summary Semantics
Daily aggregation produces reusable columns:

- `pnl_day`: sum of `pnl_bar` over the day
- `num_trades_day`: sum of `trades` over the day
- `cum_pnl_day`: cumulative sum of `pnl_day`
- `dd_day`: day-level drawdown vs running peak (`cum_pnl_day - cummax(cum_pnl_day)`)

## Segment Summary Semantics
A segment-level helper consumes day summary data and returns:

- `pnl_day_mean`
- `win_rate`
- `near_zero_rate`
- `total_pnl`
- `num_days`
- `num_trades`
- `max_dd`

`near_zero_rate` uses an explicit `near_zero_threshold` argument and is not hardcoded.
