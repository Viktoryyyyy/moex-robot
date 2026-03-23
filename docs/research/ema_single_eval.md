# EMA Single Eval (Research CLI)

## Purpose

`run_ema_single_eval.py` runs one deterministic EMA research evaluation from explicit CLI inputs.
It is designed for a single run only (no grid search, no walk-forward, no ranking, no orchestration).

## Required CLI args

The script requires all of the following arguments:

- `--input-csv` — source OHLC CSV path.
- `--schema-json` — JSON schema mapping for source columns.
- `--instrument` — instrument label to store in summary.
- `--timeframe` — target timeframe for resampling (`5m`, `30m`, `1h`, `4h`, `1d`).
- `--mode` — one of:
  - `trend_long_short`
  - `trend_long_only`
  - `trend_short_only`
- `--fast` — fast EMA span.
- `--slow` — slow EMA span.
- `--commission-points` — fee per trade action in points.
- `--near-zero-threshold` — threshold used for near-zero day rate.
- `--output-dir` — destination directory for exactly three output artifacts.

## Artifact outputs

The evaluator writes exactly three artifacts into `--output-dir`:

1. `ema_single_eval_bars.csv`
   - includes at least: `ts`, `open`, `high`, `low`, `close`, `ema_fast`, `ema_slow`, `signal`, `position`, `dclose`, `trades`, `fee`, `pnl_bar`.
2. `ema_single_eval_days.csv`
   - includes at least: `date`, `pnl_day`, `num_trades_day`, `cum_pnl_day`, `dd_day`.
3. `ema_single_eval_summary.json`
   - includes at least: `instrument`, `timeframe`, `mode`, `fast`, `slow`, `commission_points`, `near_zero_threshold`, `pnl_day_mean`, `win_rate`, `near_zero_rate`, `total_pnl`, `num_days`, `num_trades`, `max_dd`.

## Semantics inherited from Step 1 and Step 2

Step 1 foundation (from `lib_ema_search.py`):
- load schema,
- load/normalize OHLC,
- resample to target timeframe.

Step 2 backtest layer (also from `lib_ema_search.py`):
- `generate_ema_signals`,
- `run_point_backtest`,
- `summarize_by_day`,
- `summarize_segment`.

The CLI normalizes summary fields so that:
- `max_dd` is emitted as positive drawdown magnitude,
- `num_trades` is emitted as integer.

## Example command

    python -m src.research.ema.run_ema_single_eval \
      --input-csv data/research/si_raw.csv \
      --schema-json data/research/schema_si.json \
      --instrument si \
      --timeframe 5m \
      --mode trend_long_short \
      --fast 5 \
      --slow 12 \
      --commission-points 2.0 \
      --near-zero-threshold 5.0 \
      --output-dir data/research/ema_single_eval
