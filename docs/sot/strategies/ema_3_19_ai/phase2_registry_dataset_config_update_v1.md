# Phase 2.6 Registry / Dataset Config Update v1

Status: design/config binding only  
Lane: ema_3_19_ai  
Task: ema_3_19_ai_market_phase_phase_2_6_registry_dataset_config_mutation  
Created: 2026-07-06

## Scope

Phase 2.6 binds minimal USDRUBF / Si research scope into existing registry and dataset configuration files.

This update is intentionally non-runtime. It does not authorize ingestion, loader work, backfill, materialization, feature computation, modeling, prediction, server apply, or current contract month selection automation.

## Files updated

- `configs/instruments/forts_instrument_registry.v1.yaml`
- `configs/datasets/futures_data_lake.v1.yaml`

## File created

- `docs/sot/strategies/ema_3_19_ai/phase2_registry_dataset_config_update_v1.md`

## Registry binding

`configs/instruments/forts_instrument_registry.v1.yaml` now contains minimal design/config entries for:

- `usdrubf_futures_family`
- `si_futures_family`

Both entries remain disabled for loading, update, retrieval, raw 5m materialization, D1 derivation, and research execution. The entries use `evidence_status: pilot_required` and explicit readiness statuses of `not_ready` or `blocked`.

The registry binding includes the Moscow market-time policy:

- market timezone: `Europe/Moscow`
- local forecast anchor: `06:00 Europe/Moscow`
- forecast anchor timezone: `UTC`

## Dataset/source binding

`configs/datasets/futures_data_lake.v1.yaml` now binds only the approved Phase 2 placeholders as design/config source references.

Approved source placeholders referenced:

- `contracts/sources/internal/usdrubf_d1_ohlc_from_5m.v1.yaml`
- `contracts/sources/internal/usdrubf_d1_ema_3_19_cross_context.v1.yaml`
- `contracts/sources/internal/usdrubf_d1_classical_indicators.v1.yaml`
- `contracts/sources/futures/roll_expiry_mapping.v1.yaml`
- `contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml`
- `contracts/calendars/calendar/ru_tax_periods.v1.yaml`
- `contracts/calendars/calendar/ru_us_holidays.v1.yaml`

The config keeps `availability_ts_utc` and `forecast_anchor_ts` as the gating time semantics:

- `availability_ts_utc` is required.
- Eligibility rule remains `availability_ts_utc <= forecast_anchor_ts`.
- Unknown availability timestamps require exclusion or at least one-trading-day shift.

## Blocked providers and excluded sources

FUTOI remains blocked pending provider timestamp, schema, and revision policy. This update does not bind or authorize:

- `contracts/sources/futoi/participant_positioning.v1.yaml`
- oil sources
- dollar index / DXY sources
- CNY/RUB proxy sources
- USD/RUB spot proxy sources
- raw news ingestion
- LLM news/event classification

## Non-authorization boundary

This registry/config mutation does not authorize:

- server apply
- runtime commands
- data loading
- ingestion
- backfill
- materialization
- feature computation
- statistics calculation
- model fitting
- prediction
- source contract mutation
- tests
- `src/**` implementation

## Gates required before loader/materialization

Before any loader, ingestion, or materialization work, a later PM-approved task must provide:

1. Provider-level timestamp policy for each source.
2. Point-in-time revision policy for source updates and corrections.
3. Explicit dataset contract changes if generated paths or materialized artifacts are introduced.
4. Loader/runtime implementation scope in `src/**`.
5. Test scope for the chosen loader/materialization path.
6. CI evidence tied to the final PR head SHA.
7. PM L2 approval for merge and any later server apply.

## Current remaining state

- Registry/config design binding: created.
- Server apply: not required and not performed.
- Runtime: not authorized and not performed.
- Ingestion/backfill/materialization: not authorized and not performed.
- Feature computation/statistics/modeling/prediction: blocked and not performed.
