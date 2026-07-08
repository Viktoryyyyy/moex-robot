# EMA 3/19 AI — Phase 2.7 Validation Tests v1

Status: design-contract validation tests  
Lane: `ema_3_19_ai`  
Task: `ema_3_19_ai_market_phase_phase_2_7_contract_pit_validation_tests`

## Purpose

Phase 2.7 adds the first automated repository-file validation tests for the Phase 2.3–2.6 design contracts and configs.

The tests mechanically protect contract/config boundaries for:

- Phase 2.6 registry and dataset config bindings;
- point-in-time availability semantics;
- D1 forecast-anchor timing;
- label leakage exclusion;
- readiness and non-authorization flags;
- blocked provider/source exclusion.

These tests are deterministic repository-file checks. They do not load market data, compute features, fit models, run ingestion, or require server state.

## Tests added

### `tests/ema_3_19_ai/test_phase2_registry_dataset_config_contracts.py`

This test module validates:

1. `configs/instruments/forts_instrument_registry.v1.yaml` exists and parses as the supported repository YAML subset.
2. `configs/datasets/futures_data_lake.v1.yaml` exists and parses as the supported repository YAML subset.
3. `phase2_6_source_bindings.approved_source_contract_refs` equals the approved Phase 2.5/2.6 source and calendar placeholder set.
4. Blocked provider/source references remain outside approved bindings:
   - FUTOI participant-positioning source contract;
   - oil sources;
   - dollar-index sources;
   - currency proxy sources;
   - raw news ingestion;
   - news LLM classification.
5. Dataset readiness flags remain false for ingestion, runtime, loader, materialization, feature computation, and modeling.
6. Instrument registry entries remain disabled for loading, update, retrieval, D1 derivation, raw 5m materialization, and research.
7. The configs do not authorize generated data paths, runtime loaders, current contract month automation, continuous contract runtime selection, materialization jobs, feature computation, model fitting, prediction, or server apply.

### `tests/ema_3_19_ai/test_phase2_pit_and_label_leakage_contracts.py`

This test module validates:

1. `availability_ts_utc` is the canonical point-in-time availability field across relevant contracts.
2. `forecast_anchor_ts` is present where Phase 2 PIT rules require it.
3. The D1 `T+1 06:00 Europe/Moscow` rule is present for D1 feature/export/source timing.
4. Unknown or unresolved availability timestamps require exclusion or a shift by at least one trading day.
5. Label, interval, annotation, future-target, future-volatility, drawdown, LLM-classification, and post-fact annotation leakage markers are denied across the Phase 2 feature/export/source contracts.
6. EMA 3/19 is context/diagnostic only and is not a B/S/OUT label source.
7. FUTOI means participant positioning only, not generic open interest, and the FUTOI source contract remains excluded.
8. The CBR official USD/RUB rate is reference-only and not a causal market USD/RUB input.
9. Calendar contracts split `schedule_known_before_anchor` from blocked `post_fact_outcome` namespaces.
10. Runtime, data loading, ingestion, materialization, feature computation, modeling, prediction, and server apply remain unauthorized.

## Contracts and configs covered

The tests cover these repository files:

- `configs/instruments/forts_instrument_registry.v1.yaml`
- `configs/datasets/futures_data_lake.v1.yaml`
- `contracts/sources/internal/usdrubf_d1_ohlc_from_5m.v1.yaml`
- `contracts/sources/internal/usdrubf_d1_ema_3_19_cross_context.v1.yaml`
- `contracts/sources/internal/usdrubf_d1_classical_indicators.v1.yaml`
- `contracts/sources/futures/roll_expiry_mapping.v1.yaml`
- `contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml`
- `contracts/calendars/calendar/ru_tax_periods.v1.yaml`
- `contracts/calendars/calendar/ru_us_holidays.v1.yaml`
- `contracts/features/usdrubf_phase2_d1_feature_export_v1.json`
- `contracts/features/usdrubf_phase2_unified_external_feature_contract_v1.json`
- `contracts/validation/usdrubf_phase2_pit_availability_validation_v1.yaml`
- `docs/sot/strategies/ema_3_19_ai/phase2_futoi_pit_policy_v1.md`

## Mechanically protected now

The test package now mechanically protects:

- no unapproved provider/source references in Phase 2.6 approved bindings;
- no provider placeholder escalation into ingestion/runtime/modeling readiness;
- no current contract month selection automation;
- no generated data path or runtime loader authorization;
- no weakening of the `availability_ts_utc <= forecast_anchor_ts` rule;
- no weakening of the D1 `T+1 06:00 Europe/Moscow` availability rule;
- no silent use of unknown availability timestamps;
- no leakage of label, interval, future target, annotation, LLM classification, or post-fact outcome semantics into eligible feature/source evidence;
- no reinterpretation of EMA 3/19 as a B/S/OUT label source;
- no reinterpretation of FUTOI as generic open interest;
- no use of the CBR official USD/RUB rate as a causal market USD/RUB input;
- no merging of pre-anchor calendar schedules with post-fact outcomes.

## Untested or blocked

The tests do not validate real data values, market-data availability, external provider delivery, ingestion completeness, materialized files, feature matrix correctness, model quality, prediction quality, or runtime behavior.

The tests do not prove that future source/provider contracts are ready. They only protect the design-only boundaries already expressed in repository files.

The tests do not validate server state. Server filesystem state is not architectural proof.

## Explicit non-authorization

These tests do not authorize runtime, ingestion, backfill, materialization, feature computation, modeling, prediction, trading, broker execution, or server apply.

No production data path, runtime loader, source ingestion, feature export generation, model training, prediction, or deployment is created by this package.
