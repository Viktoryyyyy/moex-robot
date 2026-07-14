# EMA 3/19 AI — Source Availability Matrix

Status: design artifact only  
Lane: `ema_3_19_ai`  
Runtime impact: none  
Modeling status: blocked  
Calculation status: blocked  
Ingestion status: blocked  
Backfill status: blocked

## 1. Purpose and boundary

This document fixes the Phase 2.1 source availability matrix for the EMA 3/19 AI market phase research path.

It records source/provider availability, point-in-time and leakage rules, and implementation readiness for Phase 2 candidate factors.

This is not a runtime contract. It does not authorize source ingestion, materialization, data loading, backfill, feature computation, statistics calculation, model fitting, prediction generation, server apply, tests, contracts, configs, source code changes, or Route B / n8n changes.

The matrix distinguishes:

- source availability: whether the source/provider and its timestamp semantics can be reasoned about;
- repository contract status: whether a repository artifact already defines the source or feature contract;
- implementation readiness: whether the source can be used now, only designed, or is blocked;
- label availability: supervised target / research governance only, never runtime feature evidence.

## 2. Forecast anchor and point-in-time rules

```yaml
forecast_anchor_local: 06:00 Europe/Moscow
forecast_target: next actionable MOEX trading session
data_cutoff: availability_ts <= forecast_anchor_ts
D1_rule: trade_date T usable only from T+1 06:00 Europe/Moscow or later
unknown_release_ts_rule: shift by minimum 1 trading day or exclude
timestamp_storage: UTC
```

Rules:

1. Every feature candidate must have an `availability_ts` stored in UTC or a documented conservative lag rule that can be converted to UTC.
2. A row is eligible at an anchor only when `availability_ts <= forecast_anchor_ts`.
3. Same-day unavailable data is forbidden.
4. Daily data for trade date `T` is not usable at the same date's forecast anchor and becomes usable only from `T+1 06:00 Europe/Moscow` or later, unless a stricter source-specific rule is approved.
5. If release time is unknown, the source must be shifted by at least one trading day or excluded from modeling design.
6. Corrected, revised, filled, or vendor-restated values must not be joined into earlier anchors unless revision history is modeled point-in-time.
7. Calendar schedules known before the anchor must be separated from post-fact outcomes.
8. Labels and annotations must not be included as model features or used as source availability evidence.

## 3. Status vocabulary

### 3.1 Provider status

| status | meaning |
|---|---|
| `approved` | Candidate source/provider is already acceptable for Phase 2 design under the current source boundary. |
| `not_approved` | Candidate source/provider may be useful, but the provider decision is not approved. |
| `forbidden_now` | Current PM decision blocks use of the source for Phase 2 until explicitly changed. |
| `proposal` | Candidate provider or method is proposed only and needs a next artifact before use. |

### 3.2 Repository contract status

| status | meaning |
|---|---|
| `existing` | A repository artifact exists for the relevant source/feature family. This does not imply runtime readiness. |
| `missing` | No repository contract is currently approved for this source. |
| `proposal` | A future contract/artifact is required before implementation. |

### 3.3 Phase 2 usage status

| status | meaning |
|---|---|
| `allowed_now` | Allowed for Phase 2 design / source reasoning only. This does not authorize modeling, calculation, ingestion, or runtime. |
| `design_only` | May be described as a candidate factor, but implementation readiness is incomplete. |
| `blocked` | Must not be used until PM L2 changes the decision or approves a required artifact. |

## 4. Source availability matrix

| source_id | business_factor | candidate_provider | provider_status | repository_contract_status | data_granularity | expected_update_time | availability_timestamp_rule | forecast_anchor_compatibility | leakage_risk | phase_2_usage_status | required_next_artifact | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `internal.usdrubf_d1_ohlc_from_5m` | Market price context for USDRUBF / Si daily phase research. | Internal D1 OHLC derived from approved 5m futures data lake. | `approved` | `existing` | D1 derived from 5m bars | After source 5m data for trade date `T` is complete and validated; usable no earlier than `T+1 06:00 Europe/Moscow`. | Apply D1 rule: trade date `T` usable only from `T+1 06:00 Europe/Moscow`; store availability timestamps in UTC. | Compatible if T+1 lag is enforced. | Medium if same-day close or resampled incomplete session data is used before availability; low after D1 rule enforcement. | `allowed_now` | Phase 2 implementation contract / validation artifact before modeling. | Existing feature-family reference only. This matrix does not claim ingestion or materialization readiness. |
| `internal.usdrubf_d1_ema_3_19_cross_context` | EMA 3/19 crossover context around potential B / S / OUT transitions. | Internal EMA 3/19 feature derived from approved D1 OHLC. | `approved` | `existing` | D1 | Same as parent D1 OHLC source; usable no earlier than `T+1 06:00 Europe/Moscow`. | Availability inherits from `internal.usdrubf_d1_ohlc_from_5m`; no same-day derived EMA values before parent D1 availability. | Compatible if parent D1 lag is enforced. | Medium if EMA uses same-day or future-close values; low after parent D1 rule enforcement. | `allowed_now` | Phase 2 implementation contract / validation artifact before modeling. | EMA features are internal context, not a label source. EMA state must not define supervised labels or replace research governance. |
| `internal.usdrubf_d1_classical_indicators` | Classical market context such as returns, range, volatility, momentum, and related D1 diagnostics. | Internal classical indicators derived from approved D1 OHLC. | `approved` | `existing` | D1 | Same as parent D1 OHLC source; usable no earlier than `T+1 06:00 Europe/Moscow`. | Availability inherits from `internal.usdrubf_d1_ohlc_from_5m`; all rolling windows must include only rows available by the anchor. | Compatible if parent D1 lag and rolling-window point-in-time joins are enforced. | Medium if rolling windows include unavailable rows; low after point-in-time window enforcement. | `allowed_now` | Phase 2 implementation contract / validation artifact before modeling. | Internal indicators are feature candidates only. They do not provide label or annotation evidence. |
| `futoi.participant_positioning` | Participant positioning / positioning pressure for Si-related futures market structure. | MOEX FUTOI participant positioning dataset family. | `approved` | `existing` | Daily / report-level participant positioning, subject to contract-specific schema. | Use only after the report is available to the system; if exact release time is not proven, shift by at least one trading day. | Require explicit source `availability_ts`; otherwise apply unknown-release rule: minimum 1 trading day shift or exclude. | Conditionally compatible after release-time proof or conservative lag. | High until release timestamp and report revision behavior are proven; medium after conservative lag. | `design_only` | Provider timestamp / point-in-time availability artifact for FUTOI participant positioning. | FUTOI must not be described as open interest unless a separate open-interest source is approved. This row is participant positioning only. |
| `oil.brent_or_br_or_urals_proxy` | Oil-price / export revenue proxy that may affect RUB supply-demand expectations. | Provider to be selected: Brent vendor, MOEX BR futures, or Urals proxy. | `proposal` | `missing` | D1 or intraday-to-D1, provider-dependent | Provider-specific close/release time; unknown until provider selected. | Require explicit vendor timestamp; if unknown, shift by minimum 1 trading day or exclude. | Not compatible until provider and timestamp rule are approved. | High until provider, timezone, settlement timing, and revision behavior are fixed. | `design_only` | Source/provider selection artifact and external feature contract. | Do not mix Brent, BR, and Urals as interchangeable without an approved proxy decision. |
| `dollar_index.dxy` | Broad USD strength proxy. | Provider to be selected for DXY or an approved USD index proxy. | `proposal` | `missing` | D1 or intraday-to-D1, provider-dependent | Provider-specific close/release time; unknown until provider selected. | Require explicit vendor timestamp; if unknown, shift by minimum 1 trading day or exclude. | Not compatible until provider and timestamp rule are approved. | High until provider and timestamp are fixed. | `design_only` | Source/provider selection artifact and external feature contract. | DXY is external context only. It must not be backfilled with future-corrected values without point-in-time proof. |
| `rates.cbr_key_rate_calendar` | Russian monetary policy schedule and post-release rate decision context. | Bank of Russia official calendar / release source, or approved calendar provider. | `proposal` | `missing` | Event schedule and event outcome | Schedule may be known before anchor; realized decision usable only after actual release availability. | Split into schedule-known-before-anchor fields and post-fact outcome fields; unknown outcome release time requires minimum 1 trading day shift or exclusion. | Schedule component can be compatible if known before anchor; outcome component is incompatible before release availability. | High if realized decisions or post-event narratives leak before release. | `design_only` | Calendar/outcome source contract with schedule-vs-outcome separation. | Calendar existence is not the same as realized rate outcome. Post-fact outcome must not enter pre-release anchors. |
| `currency.cnyrub_proxy` | CNY/RUB market proxy for RUB pressure and cross-currency context. | Provider to be selected: MOEX FX market data, broker/market vendor, or approved public proxy. | `proposal` | `missing` | D1 or intraday-to-D1, provider-dependent | Provider-specific close/release time; unknown until provider selected. | Require explicit provider availability timestamp; if unknown, shift by minimum 1 trading day or exclude. | Not compatible until provider and timestamp rule are approved. | High until provider, session time, and holiday behavior are fixed. | `design_only` | Source/provider selection artifact and external feature contract. | Must use market/proxy quote semantics. Official reference rates, if later included, must be lagged/reference only. |
| `currency.usdrub_spot_proxy` | Market USD/RUB proxy for spot/OTC context distinct from USDRUBF futures. | Provider to be selected: MOEX FX market data, broker/market vendor, or approved public proxy. | `proposal` | `missing` | D1 or intraday-to-D1, provider-dependent | Provider-specific close/release time; unknown until provider selected. | Require explicit provider availability timestamp; if unknown, shift by minimum 1 trading day or exclude. | Not compatible until provider and timestamp rule are approved. | High until provider, session time, and quote convention are fixed. | `design_only` | Source/provider selection artifact and external feature contract. | CBR official USD/RUB rate must not be framed as a causal driver of market USD/RUB. If included later, it must be lagged/reference only, not a real-time market driver. |
| `calendar.ru_tax_periods` | Russian tax-period schedule proxy for exporter RUB liquidity timing. | Approved deterministic tax calendar source to be selected. | `proposal` | `missing` | Calendar/event schedule | Schedule should be known before anchor once calendar source is approved. | Only schedule metadata known before `forecast_anchor_ts` may be used. Post-fact liquidity outcomes are excluded unless separately sourced with availability timestamps. | Potentially compatible after deterministic schedule contract. | Medium if post-fact liquidity narratives or realized flow estimates leak into pre-anchor rows. | `design_only` | Calendar source contract with known-before-anchor semantics. | Tax period schedule can be known in advance; realized flow impact cannot be assumed without separate point-in-time data. |
| `calendar.ru_us_holidays` | Trading-session and liquidity calendar context for Russia and US. | Exchange / official holiday calendars, or approved market calendar provider. | `proposal` | `existing` | Calendar/session schedule | Schedule known in advance, subject to approved calendar source and repository calendar contract. | Use only schedule fields known before anchor; do not include post-fact liquidity explanations. | Compatible after explicit mapping to trading sessions and anchor timezone. | Low for fixed holidays; medium for unscheduled closures unless timestamped. | `design_only` | Session calendar mapping artifact for Phase 2 factor joins. | Repository session-calendar family exists, but this matrix does not claim finished feature readiness for RU/US holiday factors. |
| `futures.roll_expiry_mapping` | Contract roll / expiry context for continuous Si / USDRUBF daily series and phase interpretation. | Internal futures instrument registry, exchange schedules, and approved continuous-series mapping. | `approved` | `existing` | Contract metadata / schedule | Known before expiry if mapping and registry are approved; updates follow instrument registry governance. | Use only contract metadata available before anchor; no post-fact roll optimization based on future liquidity or returns. | Compatible after mapping is fixed before the affected anchor. | Medium if roll dates are selected with future liquidity/return knowledge; low after fixed rule. | `allowed_now` | Phase 2 roll/expiry mapping validation artifact before modeling. | Roll mapping is context / data alignment evidence, not label evidence. |
| `news_events.raw_ingestion` | Raw news/event text that might explain shocks, sanctions, geopolitics, or market narratives. | Not approved. | `forbidden_now` | `missing` | Raw text / event stream | Not applicable under current PM decision. | Not applicable. Source is forbidden now. | Not compatible. | Very high: source timing, selection bias, publication edits, and retroactive narratives can leak future information. | `blocked` | PM L2 decision change plus source, timestamp, retention, and ingestion contract. | Raw news/event ingestion remains forbidden_now unless PM L2 changes the decision. No ingestion or raw text pipeline is authorized. |
| `news_events.llm_classification` | LLM-derived classification of news/event text into factor categories. | Not approved. | `forbidden_now` | `missing` | Derived categorical / textual classification | Not applicable under current PM decision. | Not applicable. Source is forbidden now. | Not compatible. | Very high: depends on raw ingestion timing, prompt/version control, model drift, and retroactive classification leakage. | `blocked` | PM L2 decision change plus classification contract, prompt/version governance, and point-in-time evidence. | LLM classification remains forbidden_now unless PM L2 changes the decision. No runtime assistant classification or model feature pipeline is authorized. |

## 5. Mandatory semantic constraints

1. FUTOI in this artifact means participant positioning. It must not be described as open interest unless a separate open-interest source is approved.
2. The Bank of Russia official USD/RUB rate must not be framed as a causal driver of market USD/RUB. If a CBR official rate is included in a later artifact, it must be lagged/reference only.
3. News-events raw ingestion and LLM classification are `forbidden_now` and `blocked` until PM L2 changes the decision.
4. EMA features are internal context, not label sources.
5. Labels are supervised target / research governance artifacts, not runtime features and not source availability evidence.
6. All source rows distinguish provider availability from repository contract status and implementation readiness.
7. Same-day unavailable data is forbidden.
8. Source availability must not include label or annotation availability as feature evidence.
9. This artifact is not a runtime contract and does not claim ingestion, materialization, feature computation, or prediction readiness.

## 6. Phase 2 implementation readiness gate

Before any modeling, calculation, or runtime-oriented implementation can start, a later PM-approved artifact must resolve at least:

- exact provider selection for every external/proposal row;
- source-specific release time and timezone rules;
- UTC storage and conversion rules for `availability_ts`;
- point-in-time join rules;
- revision / correction handling;
- holiday and session mapping rules;
- contract status for any source moving from `design_only` to implementation scope;
- validation checks proving `availability_ts <= forecast_anchor_ts` for each anchor.

Modeling remains explicitly blocked by this document. This matrix is a design input for future Phase 2 governance only.
