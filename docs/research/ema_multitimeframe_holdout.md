# EMA Multi-Timeframe Holdout (Research-Only)

## Purpose

`run_ema_multitimeframe_holdout.py` runs the final untouched holdout evaluation across multiple timeframes for one dataset, one instrument, one mode, and one EMA search range, then writes consolidated cross-timeframe artifacts.

This is research-only tooling and does not add production/runtime integration.

## Required CLI args

- `--input-csv`
- `--schema-json`
- `--instrument`
- `--timeframes`
- `--mode`
- `--fast-min`
- `--fast-max`
- `--slow-min`
- `--slow-max`
- `--commission-points`
- `--near-zero-threshold`
- `--holdout-months`
- `--output-dir`

## Multi-timeframe semantics

- `--timeframes` is a comma-separated list.
- Allowed values (after normalization) are exactly: `5m`, `30m`, `1h`, `4h`, `1d`.
- Duplicates after normalization are rejected.
- For each timeframe, the runner:
  1. Resamples normalized OHLC bars.
  2. Builds holdout from the last `N` full calendar months.
  3. Evaluates all valid EMA pairs where `fast < slow`.
  4. Ranks pairs deterministically.
  5. Persists the best pair/metrics for that timeframe.

## Holdout semantics

- Holdout uses only the last N full calendar months (`--holdout-months`).
- `holdout-months` must be `> 0`.
- If the requested holdout cannot be built for any requested timeframe, execution fails with a clear error.

## Output artifacts

Written in `--output-dir`:

1. `ema_multitimeframe_holdout_results.csv`
   - One row per `(timeframe, fast, slow)` evaluation.
   - Includes holdout boundaries, run metadata, and metrics.
2. `ema_multitimeframe_holdout_best.csv`
   - One row per timeframe.
   - Includes best pair (`best_fast`, `best_slow`) and best metrics.
3. `ema_multitimeframe_holdout_summary.json`
   - Includes run parameters, requested/evaluated timeframes,
     ranking rule string, and artifact paths.

## Ranking rule

Exactly:

`pnl_day_mean desc, win_rate desc, max_dd asc`

## Inherited semantics from Step 1 and Step 2

This runner reuses the established foundations from earlier EMA research steps:

- Schema-driven OHLC loading and canonical normalization.
- Timeframe resampling from normalized OHLC.
- EMA signal generation and point backtest summary flow.
- Deterministic grid evaluation with valid `fast < slow` constraints.

## Example command

```bash
python -m src.research.ema.run_ema_multitimeframe_holdout \
  --input-csv data/research/IMOEX_15m.csv \
  --schema-json config/research/imoex_schema.json \
  --instrument IMOEX \
  --timeframes 5m,30m,1h,4h,1d \
  --mode trend_long_short \
  --fast-min 5 \
  --fast-max 20 \
  --slow-min 21 \
  --slow-max 80 \
  --commission-points 0.5 \
  --near-zero-threshold 0.0 \
  --holdout-months 6 \
  --output-dir artifacts/research/ema_multitimeframe_holdout
```
