# EMA Holdout Runner (Research-Only)

## Purpose

`src/research/ema/run_ema_holdout.py` evaluates EMA fast/slow pairs on a final untouched holdout segment. This holdout is separate from any train/validation walk-forward model selection logic and is intended only for research analysis.

## Required CLI arguments

All arguments are required (no implicit defaults):

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
- `--holdout-months`
- `--output-dir`

## Whole-month holdout semantics

- Data load, normalization, and timeframe resampling are inherited from Step 1 (`lib_ema_search`).
- The runner identifies full calendar months only from the resampled bars.
- A month is considered full only if the dataset has coverage at or before that month start and at or after the next month start.
- Holdout is the last N full calendar months (`--holdout-months`), where `N > 0`.
- If there are fewer than `N` full months, the runner fails with a clear error.

## Backtest semantics inherited from Step 2

For each valid EMA pair (`fast < slow`):

1. Generate EMA signals.
2. Run point backtest with commission.
3. Summarize by day.
4. Summarize segment metrics.

The runner does not add train/valid logic in this step.

## Output artifacts

The output directory contains exactly two artifacts:

1. `ema_holdout_results.csv`
   - One row per evaluated pair.
   - Includes at least:
     - `holdout_start_month`
     - `holdout_end_month`
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

2. `ema_holdout_best.json`
   - Contains at least:
     - `holdout_start_month`
     - `holdout_end_month`
     - `instrument`
     - `timeframe`
     - `mode`
     - `fast_min`
     - `fast_max`
     - `slow_min`
     - `slow_max`
     - `commission_points`
     - `near_zero_threshold`
     - `best_pair`
     - `best_metrics`
     - `ranking_rule`

## Ranking rule

Results are deterministically ranked using:

- primary: `pnl_day_mean` descending
- secondary: `win_rate` descending
- tertiary: `max_dd` ascending

Ranking rule string in JSON:

- `"pnl_day_mean desc, win_rate desc, max_dd asc"`

## Example command

    python -m src.research.ema.run_ema_holdout \
      --input-csv data/si_ohlc.csv \
      --schema-json configs/research/ohlc_schema.json \
      --instrument SI \
      --timeframe 5m \
      --mode trend_long_short \
      --fast-min 5 \
      --fast-max 20 \
      --slow-min 30 \
      --slow-max 100 \
      --commission-points 1.0 \
      --near-zero-threshold 0.0 \
      --holdout-months 3 \
      --output-dir artifacts/ema_holdout
