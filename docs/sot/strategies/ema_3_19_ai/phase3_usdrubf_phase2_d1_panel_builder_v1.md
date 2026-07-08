# Phase 3.2 — USDRUBF Phase 2 D1 panel builder

Status: PR-only builder interface  
Project: MOEX Bot  
Lane: ema_3_19_ai  
Task: ema_3_19_ai_market_phase_phase_3_2_d1_panel_builder_pr

## Purpose

This phase defines the minimal controlled output contract and command interface for `usdrubf_phase2_d1_panel.v1`.

It unblocks the next phase, Phase 3.3, where PM L2 may separately authorize the first controlled five-date server build.

This phase does not authorize a server run, data build, model work, prediction, broker action, or trading action.

## New repository files

Exact file scope:

- `contracts/datasets/usdrubf_phase2_d1_panel.v1.yaml`
- `src/moex_data/futures/usdrubf_phase2_d1_panel_builder.py`
- `tests/ema_3_19_ai/test_phase3_usdrubf_phase2_d1_panel_builder.py`
- `docs/sot/strategies/ema_3_19_ai/phase3_usdrubf_phase2_d1_panel_builder_v1.md`

No existing file is changed.

## Contracted paths

Panel output pattern:

```text
${MOEX_DATA_ROOT}/research/ema_3_19_ai/usdrubf_phase2_d1_panel.v1/instrument_id={INSTRUMENT_ID}/run_id={RUN_ID}/part.parquet
```

Manifest output pattern:

```text
${MOEX_DATA_ROOT}/research/ema_3_19_ai/usdrubf_phase2_d1_panel.v1/instrument_id={INSTRUMENT_ID}/run_id={RUN_ID}/manifest.json
```

Default raw 5m input root pattern:

```text
${MOEX_DATA_ROOT}/forts/raw_5m/tradestats
```

Default raw 5m partition pattern:

```text
${MOEX_DATA_ROOT}/forts/raw_5m/tradestats/trade_date={YYYY-MM-DD}/instrument_id=forts.usdrubf/secid=USDRUBF/part.parquet
```

The contract uses `MOEX_DATA_ROOT`. It does not hardcode a server filesystem root.

## Builder module

Module entry point:

```text
python -m moex_data.futures.usdrubf_phase2_d1_panel_builder
```

Required arguments:

```text
--data-root
--instrument-id
--secid
--start-date
--end-date
--run-id
--no-overwrite
```

Optional arguments:

```text
--input-root
--output-path
--manifest-path
```

## Future command shape, not authorized in Phase 3.2

The command shape for Phase 3.3 is documented here only. It must not be run during Phase 3.2.

```text
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && PYTHONPATH=src python -m moex_data.futures.usdrubf_phase2_d1_panel_builder --data-root /home/trader/moex_bot/data --instrument-id forts.usdrubf --secid USDRUBF --start-date 2026-06-11 --end-date 2026-06-18 --run-id phase3_3_first_5_trade_dates --no-overwrite
```

## Builder behavior

The builder:

- resolves raw input partitions only under `${MOEX_DATA_ROOT}/forts/raw_5m/tradestats`;
- requires explicit start and end dates;
- uses available `trade_date=YYYY-MM-DD` partitions in the requested date range;
- fails closed when no raw 5m partitions are found;
- builds one D1 session row per discovered raw 5m trade-date partition;
- writes only the panel parquet and manifest JSON when explicitly run;
- refuses existing output or manifest paths when `--no-overwrite` is set;
- writes deterministic metadata columns:
  - `trade_date`
  - `instrument_id`
  - `secid`
  - `source_raw_5m_partition_count`
  - `panel_schema_version`
  - `build_run_id`;
- includes only OHLC and optional raw-derived `volume`, `value`, `num_trades`, `first_ts`, and `last_ts`;
- excludes B/S/OUT labels and target-like future outcome columns.

## Explicit non-authorizations

Phase 3.2 authorizes repository code only.

It does not authorize:

- server apply;
- data build;
- generated data output;
- ingestion or backfill;
- network/provider calls;
- subprocess execution;
- model fitting;
- prediction;
- broker, trading, or live runtime action.

## Phase 3.3 boundary

After PM L2 merge approval and any required server apply window, Phase 3.3 may run the first controlled five-date build against:

```text
2026-06-11
2026-06-15
2026-06-16
2026-06-17
2026-06-18
```

That run requires a separate PM L2 approval and is outside this PR.
