# EMA 3/19 AI — Phase 2.5 Provider Registry Scope v1

Status: design-only registry scope proposal  
Lane: `ema_3_19_ai`  
Task: `ema_3_19_ai_market_phase_phase_2_5_provider_registry_placeholder_contracts`  
Runtime status: blocked  
Server apply status: not authorized  
Registry mutation status: excluded

## Boundary

Registry scope proposal is documentation only. It is not registry mutation, not materialization readiness, not data loading readiness, and not runtime readiness.

This document does not change `configs/instruments/**`, `configs/datasets/**`, `contracts/datasets/**`, `contracts/features/**`, `contracts/validation/**`, `src/**`, `tests/**`, or server state.

## Phase 2.5 included placeholder families

- `internal.usdrubf_d1_ohlc_from_5m`
- `internal.usdrubf_d1_ema_3_19_cross_context`
- `internal.usdrubf_d1_classical_indicators`
- `futures.roll_expiry_mapping`
- `rates.cbr_key_rate_calendar` as `schedule_known_before_anchor` only
- `calendar.ru_tax_periods` as `schedule_known_before_anchor` only
- `calendar.ru_us_holidays` as `schedule_known_before_anchor` only

All included families are design-only placeholders. They are not approved for ingestion, runtime, materialization, feature computation, modeling, prediction, or registry/config mutation.

## Later registry fields to define

### Later instrument registry fields

- `instrument_id`
- `market`
- `board`
- `symbol_aliases`
- `timezone`
- `trading_calendar_id`
- `session_calendar_id`
- `continuous_contract_policy_id`
- `phase_2_usage_status`
- `availability_policy_id`

### Later dataset registry fields

- `dataset_id`
- `source_id`
- `contract_id`
- `instrument_scope`
- `grain`
- `calendar_id`
- `availability_ts_utc_field`
- `forecast_anchor_ts_field`
- `timezone_conversion_policy`
- `revision_policy_id`
- `pit_validation_policy_id`
- `materialization_status`
- `runtime_status`
- `modeling_status`

### Later source registry fields

- `source_id`
- `provider_family`
- `provider_status`
- `phase_2_usage_status`
- `source_contract_path`
- `availability_ts_utc_source`
- `source_timezone`
- `utc_conversion_rule`
- `known_before_anchor_policy`
- `post_fact_outcome_policy`
- `revision_or_restatement_policy`
- `blocked_reason`

## Later shared-file lock requirements

Any future mutation of `configs/instruments/**` or `configs/datasets/**` requires a PM L2 shared-file lock before implementation. These shared areas are not locked for mutation in Phase 2.5 because Phase 2.5 does not mutate them.

## PIT rules that future registry entries must preserve

- Every source or calendar record must declare an `availability_ts_utc` source.
- Every source or calendar record must declare timezone conversion to UTC.
- Every D1 value for trade date T is unavailable until at least T+1 06:00 Europe/Moscow unless an enforceable provider timestamp proves earlier availability.
- Every row is eligible only if `availability_ts_utc <= forecast_anchor_ts`.
- Unknown `availability_ts_utc` means exclude or shift by at least one trading day.
- Revisions or restatements require PIT revision history or exclusion from prior anchors.
- Calendar fields must separate `schedule_known_before_anchor` from `post_fact_outcome`.

## Input-feature exclusions

The following remain excluded from input feature contracts and registry-ready feature definitions:

- B/S/OUT labels
- EMA cross labels
- future returns
- future volatility
- drawdown
- phase-completion targets
- LLM classifications
- post-fact annotations
- realized event outcomes

## CBR and FUTOI boundaries

The Bank of Russia official USD/RUB rate is reference-only and must not be used or described as a causal market USD/RUB input.

FUTOI means participant positioning, not generic open interest. FUTOI source registration remains blocked until an exact provider timestamp, schema, timezone, trade-date join, holiday/missing-report, and revision policy is approved.

## Explicit non-authorizations

- no registry mutation
- no configs mutation
- no source implementation
- no tests
- no data loading
- no ingestion
- no backfill
- no materialization
- no runtime
- no feature computation
- no modeling
- no prediction
- no server apply
