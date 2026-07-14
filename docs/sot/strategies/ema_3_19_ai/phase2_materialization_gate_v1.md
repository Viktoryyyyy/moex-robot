# EMA 3/19 AI — Phase 2.8 Materialization Gate v1

Status: repository-file validation gate  
Lane: `ema_3_19_ai`  
Task: `ema_3_19_ai_market_phase_phase_2_8_materialization_gate_loader_interface`

## Purpose

Phase 2.8 adds a minimal pure-code gate for future PIT-safe D1 materialization readiness.

The gate validates that the repository design state from Phase 2.3 through Phase 2.7 is internally consistent before any future data-build task is allowed to request ingestion, loader work, materialization, feature computation, modeling, or prediction.

This package is intentionally not a loader, not a materializer, and not a feature builder.

## Files added

Exact new files:

- `src/moex_data/futures/phase2_materialization_gate.py`
- `tests/ema_3_19_ai/test_phase2_materialization_gate.py`
- `docs/sot/strategies/ema_3_19_ai/phase2_materialization_gate_v1.md`

No existing files are changed by this phase.

## Link to Phase 2.3–2.7 artifacts

The gate reads repository files only and links the accepted Phase 2 artifacts as follows:

- Phase 2.3 source-contract package:
  - `contracts/sources/internal/usdrubf_d1_ohlc_from_5m.v1.yaml`
  - `contracts/sources/internal/usdrubf_d1_ema_3_19_cross_context.v1.yaml`
  - `contracts/sources/internal/usdrubf_d1_classical_indicators.v1.yaml`
  - `contracts/sources/futures/roll_expiry_mapping.v1.yaml`
  - `contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml`
  - `contracts/calendars/calendar/ru_tax_periods.v1.yaml`
  - `contracts/calendars/calendar/ru_us_holidays.v1.yaml`
- Phase 2.5 provider/source placeholder boundary:
  - approved placeholders remain design-only;
  - FUTOI participant positioning, oil, dollar index, currency, raw news ingestion, and news LLM classification remain blocked.
- Phase 2.6 registry and dataset config binding:
  - `configs/instruments/forts_instrument_registry.v1.yaml`
  - `configs/datasets/futures_data_lake.v1.yaml`
- Phase 2.7 PIT and label-leakage test assumptions:
  - `tests/ema_3_19_ai/test_phase2_registry_dataset_config_contracts.py`
  - `tests/ema_3_19_ai/test_phase2_pit_and_label_leakage_contracts.py`
  - `contracts/features/usdrubf_phase2_d1_feature_export_v1.json`
  - `contracts/validation/usdrubf_phase2_pit_availability_validation_v1.yaml`

## What the gate validates

The Phase 2.8 gate validates:

1. Required repository config, contract, feature, validation, and Phase 2.7 test files exist.
2. `phase2_6_source_bindings.approved_source_contract_refs` exactly matches the approved source/calendar placeholder set.
3. Blocked provider/source refs remain denied:
   - FUTOI participant-positioning source contract;
   - oil;
   - dollar index;
   - currency;
   - raw news ingestion;
   - news LLM classification.
4. Dataset readiness flags remain false for ingestion, runtime, loader, materialization, feature computation, and modeling.
5. Instrument registry loading/update/retrieval/materialization/research flags remain false.
6. No generated data path is authorized.
7. No current contract month selection automation is authorized.
8. `availability_ts_utc` and `forecast_anchor_ts` gates are represented.
9. The rule `availability_ts_utc <= forecast_anchor_ts` remains represented.
10. Unknown `availability_ts_utc` must still be excluded or shifted by at least one trading day.
11. Label leakage denylist coverage remains represented.
12. Phase 2.7 test assumptions remain present in repository tests.

The module returns a plain `dict` report with explicit blocked status:

```text
materialization_gate_status: blocked_pending_data_build_approval
can_run_ingestion: false
can_run_materialization: false
can_compute_features: false
can_model: false
```

## What remains blocked

The following remain blocked after Phase 2.8:

- ingestion;
- backfill;
- loader execution;
- materialization run;
- generated data output;
- feature computation;
- model fitting;
- prediction;
- runtime execution;
- server apply;
- current contract month selection automation;
- provider expansion beyond approved design placeholders.

## Explicit non-authorization

Phase 2.8 performs and authorizes none of the following:

- no ingestion;
- no backfill;
- no materialization run;
- no feature computation;
- no modeling;
- no prediction;
- no server apply.

No market data is loaded. No parquet, CSV, or generated dataset is read or written. No MOEX/ISS call, network call, subprocess, server command, or runtime command is executed.

## Next gate before any real data build

Before any real data build, a later PM-approved task must create a separate data-build authorization gate that explicitly answers at least:

1. Which exact instrument, source, contract month, and D1 date range are authorized.
2. Which provider/source contract is promoted from placeholder to usable source.
3. Which loader or materializer entrypoint is approved.
4. Where output is authorized to be written.
5. How `availability_ts_utc` is generated, audited, and compared to `forecast_anchor_ts`.
6. How unknown availability timestamps are excluded or shifted.
7. How label, annotation, interval, future-target, and post-fact leakage fields are excluded.
8. Which tests and CI checks must pass before any merge.
9. Whether server apply is authorized, and under which PM L2 server-apply window.

Until that future gate is approved, Phase 2 remains design/test readiness only and the materialization status remains `blocked_pending_data_build_approval`.
