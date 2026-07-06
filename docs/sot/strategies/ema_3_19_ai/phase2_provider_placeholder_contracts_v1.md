# EMA 3/19 AI — Phase 2.5 Provider Placeholder Contracts v1

Status: design-only provider placeholder package  
Lane: `ema_3_19_ai`  
Task: `ema_3_19_ai_market_phase_phase_2_5_provider_registry_placeholder_contracts`  
Runtime status: blocked  
Server apply status: not authorized  
Modeling status: blocked

## Scope

Phase 2.5 creates design-only placeholder contracts and documentation. It is not registry mutation, not provider implementation, not ingestion, not materialization, not runtime, not feature computation, not modeling, and not server apply.

## Included design-only placeholders

| Family | Artifact | Phase 2.5 status |
| --- | --- | --- |
| `internal.usdrubf_d1_ohlc_from_5m` | `contracts/sources/internal/usdrubf_d1_ohlc_from_5m.v1.yaml` | design-only placeholder |
| `internal.usdrubf_d1_ema_3_19_cross_context` | `contracts/sources/internal/usdrubf_d1_ema_3_19_cross_context.v1.yaml` | design-only placeholder |
| `internal.usdrubf_d1_classical_indicators` | `contracts/sources/internal/usdrubf_d1_classical_indicators.v1.yaml` | design-only placeholder |
| `futures.roll_expiry_mapping` | `contracts/sources/futures/roll_expiry_mapping.v1.yaml` | design-only placeholder |
| `rates.cbr_key_rate_calendar` | `contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml` | schedule-known-before-anchor only |
| `calendar.ru_tax_periods` | `contracts/calendars/calendar/ru_tax_periods.v1.yaml` | schedule-known-before-anchor only |
| `calendar.ru_us_holidays` | `contracts/calendars/calendar/ru_us_holidays.v1.yaml` | schedule-known-before-anchor only |

## Design-only semantics

Every included placeholder has `design_only: true` and all readiness flags false for ingestion, runtime, loader, materialization, feature computation, modeling, registry mutation, and provider approval.

No placeholder may be interpreted as approval to load, compute, backfill, export, model, predict, register, or run anything.

## Required PIT rules

- Every source/calendar placeholder declares an `availability_ts_utc` source.
- Every source/calendar placeholder declares timezone conversion to UTC.
- D1 trade date T is unavailable until at least T+1 06:00 Europe/Moscow unless an enforceable provider timestamp proves earlier availability.
- Every row is eligible only when `availability_ts_utc <= forecast_anchor_ts`.
- Unknown `availability_ts_utc` means exclude or shift by at least one trading day.
- Revisions/restatements require PIT revision history or exclusion from prior anchors.
- Rolling windows must include only rows eligible by the same forecast anchor.
- No post-anchor rows may enter context or indicator calculations.

## Calendar namespace split

Calendar contracts must separate:

- `schedule_known_before_anchor`: preannounced schedule fields available before the forecast anchor.
- `post_fact_outcome`: realized outcomes, commentary, market reactions, liquidity effects, or any other after-event information.

Only `schedule_known_before_anchor` is allowed as design-only context in Phase 2.5. `post_fact_outcome` remains blocked.

## Blocked providers and reasons

| Provider/family | Phase 2.5 status | Reason |
| --- | --- | --- |
| `futoi.participant_positioning` | blocked | Requires exact provider release timestamp source, schema, timezone, UTC conversion, trade-date join key, holiday/missing-report handling, and PIT revision policy. |
| `oil.brent_or_br_or_urals_proxy` | blocked | External provider availability, timestamp, and revision policy are unresolved. |
| `dollar_index.dxy` | blocked | External provider availability, timestamp, and licensing/source policy are unresolved. |
| `currency.cnyrub_proxy` | blocked | External/market proxy source policy and PIT availability are unresolved. |
| `currency.usdrub_spot_proxy` | blocked | Market USD/RUB source and PIT availability are unresolved. |
| `news_events.raw_ingestion` | blocked | Raw ingestion policy, availability timestamp, source reliability, and storage boundary are unresolved. |
| `news_events.llm_classification` | blocked | LLM classifications are excluded from input feature contracts and require separate PIT-safe policy. |

## Input-feature exclusions

The following are excluded from input feature contracts:

- B/S/OUT labels
- EMA cross labels
- future returns
- future volatility
- drawdown
- phase-completion targets
- LLM classifications
- post-fact annotations
- realized event outcomes

EMA context is allowed only as context/diagnostic design metadata and is not a B/S/OUT label source.

## CBR reference-only rule

The Bank of Russia official USD/RUB rate is a reference-only lagged indicator and must not be used or described as a causal market USD/RUB input. Market USD/RUB dynamics must remain separate from the official reference rate.

The CBR key-rate calendar is allowed only for preannounced `schedule_known_before_anchor` fields. Realized rate decisions and outcome fields remain blocked until exact `publication_ts_utc` policy is approved.

## FUTOI boundary

FUTOI means participant positioning, not generic open interest. Phase 2.5 does not create a FUTOI source contract. FUTOI remains blocked pending provider timestamp, schema, timezone, UTC conversion, trade-date join, holiday/missing-report handling, and revision policy.

## Explicit non-authorizations

- no registry mutation
- no configs mutation
- no source/provider implementation
- no tests
- no data files
- no data loading
- no ingestion
- no backfill
- no materialization
- no runtime
- no feature computation
- no modeling
- no prediction
- no server apply
