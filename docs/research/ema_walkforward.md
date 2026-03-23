# EMA Walk-Forward Month Split (Research-Only)

## Purpose

`run_ema_walkforward.py` performs deterministic walk-forward evaluation for one dataset, one instrument, one timeframe, and one mode across many EMA `fast/slow` pairs.

This step is strictly research-only and uses month-based windows only (no day-count windows, no untouched holdout in this step).

## Required CLI args

All CLI args are required (no implicit defaults):

- `--input-csv`
- `--schema-json`
- `--instrument`
- `--timeframe`
- `--mode`
- `--fast-min`
- `--fast-max`
- `--slow-min`
- `--slow-max`
- `--commission-points`
- `--near-zero-threshold`
- `--train-months`
- `--valid-months`
- `--step-months`
- `--output-dir`

Validation rules:

- `fast-min > 0`
- `fast-max > 0`
- `slow-min > 0`
- `slow-max > 0`
- `train-months > 0`
- `valid-months > 0`
- `step-months > 0`
- evaluated pair must satisfy `fast < slow`
- if no full walk-forward windows can be built, the run fails clearly

## Semantics inherited from Step 1 and Step 2

### Step 1 foundation

The runner uses `lib_ema_search.py` to:

1. Load schema JSON.
2. Load/normalize OHLC source CSV.
3. Resample to the requested timeframe.

### Step 2 backtest layer

For each pair and for each split role (`train`, `valid`), the runner uses Step 2 functions to:

1. Generate EMA signals.
2. Run point backtest.
3. Summarize by day.
4. Summarize segment metrics.

## Month-based split semantics

- Splits are built from calendar months (`YYYY-MM`) extracted from resampled bars.
- Train window uses full months only.
- Validation window uses full months only.
- Rolling advances by `--step-months` months.
- No leakage: validation months always come strictly after train months inside each window.
- No day-count windows are used.

## Per-window selection rule

Best pair is selected using train metrics with deterministic stable sorting:

1. `train pnl_day_mean` descending.
2. `train win_rate` descending.
3. `train max_dd` ascending.

Selection rule string in summary JSON:

- `train pnl_day_mean desc, train win_rate desc, train max_dd asc`

## Output artifacts

The runner writes exactly three artifacts to `--output-dir`:

1. `ema_walkforward_results.csv`
2. `ema_walkforward_best_by_window.csv`
3. `ema_walkforward_summary.json`

### `ema_walkforward_results.csv`

One row per:

- `window_id`
- `split_role`
- `fast`
- `slow`

Contains at least:

- `window_id`
- `split_role` (`train` or `valid`)
- `train_start_month`
- `train_end_month`
- `valid_start_month`
- `valid_end_month`
- `instrument`
- `timeframe`
- `mode`
- `fast`
- `slow`
- `commission_points`
- `near_zero_threshold`
- `pnl_day_mean`
- `win_rate`
- `near_zero_rate`
- `total_pnl`
- `num_days`
- `num_trades`
- `max_dd`

### `ema_walkforward_best_by_window.csv`

One row per window containing train-selected pair and validation outcome. Contains at least:

- `window_id`
- `train_start_month`
- `train_end_month`
- `valid_start_month`
- `valid_end_month`
- `instrument`
- `timeframe`
- `mode`
- `best_fast`
- `best_slow`
- `train_pnl_day_mean`
- `train_win_rate`
- `train_max_dd`
- `valid_pnl_day_mean`
- `valid_win_rate`
- `valid_near_zero_rate`
- `valid_total_pnl`
- `valid_num_days`
- `valid_num_trades`
- `valid_max_dd`

### `ema_walkforward_summary.json`

Contains at least:

- `instrument`
- `timeframe`
- `mode`
- `fast_min`
- `fast_max`
- `slow_min`
- `slow_max`
- `commission_points`
- `near_zero_threshold`
- `train_months`
- `valid_months`
- `step_months`
- `windows_built`
- `pairs_per_window`
- `selection_rule`
- `artifacts`

## Example

    python -m src.research.ema.run_ema_walkforward \
      --input-csv data/research/example.csv \
      --schema-json data/research/example.schema.json \
      --instrument Si-9.26 \
      --timeframe 5m \
      --mode trend_long_short \
      --fast-min 3 \
      --fast-max 12 \
      --slow-min 8 \
      --slow-max 40 \
      --commission-points 0.5 \
      --near-zero-threshold 1.0 \
      --train-months 6 \
      --valid-months 1 \
      --step-months 1 \
      --output-dir out/research/ema_walkforward
