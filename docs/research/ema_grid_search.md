# EMA Grid Search (Research-Only)

## Purpose

`run_ema_grid_search.py` performs a deterministic, single-dataset EMA parameter sweep for one instrument/timeframe/mode configuration. It evaluates many `fast/slow` EMA pairs and ranks them for research comparison.

This is intentionally **research-only** and **single-run semantics only**.

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
- `--output-dir`

Validation rules:

- `fast > 0`
- `slow > 0`
- `fast < slow` (per evaluated pair)

## Semantics inherited from Step 1 and Step 2

### Step 1 foundation

The runner uses `lib_ema_search.py` Step 1 functions to:

1. Load schema JSON.
2. Load/normalize OHLC source CSV to canonical columns.
3. Resample bars into requested timeframe.

### Step 2 backtest layer

For each valid `(fast, slow)` pair, the runner uses Step 2 library functions to:

1. Generate EMA signals.
2. Run point backtest.
3. Summarize by day.
4. Summarize segment-level metrics.

## Output artifacts

The runner writes exactly two artifacts into `--output-dir`:

1. `ema_grid_search_results.csv`
2. `ema_grid_search_best.json`

### `ema_grid_search_results.csv`

Contains one row per evaluated pair and includes at least:

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

### `ema_grid_search_best.json`

Contains:

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

## Ranking rule

Deterministic stable ranking is applied using:

1. Primary: `pnl_day_mean` descending.
2. Secondary: `win_rate` descending.
3. Tertiary: `max_dd` ascending.

If rows are tied on these keys, stable sorting preserves deterministic iteration order from the input grid traversal.

## Example

    python -m src.research.ema.run_ema_grid_search \
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
      --output-dir out/research/ema_grid_search
