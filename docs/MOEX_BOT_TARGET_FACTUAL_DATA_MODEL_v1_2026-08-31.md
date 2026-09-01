# MOEX Bot — Target Factual Data Model v1

Date: 2026-08-31
Revised: 2026-09-01
Status: target management model; not an implementation contract for interpretation or trading logic
Project: MOEX_Bot

## 1. Purpose

Define the target factual data layer required for RUB / USDRUBF analysis before further analytical-model work.

Current priority is data collection and refresh only:
- collect factual data;
- preserve provenance and source timestamps;
- refresh the canonical current snapshot on the existing approximately 10-minute runtime cadence;
- make data freshness explicit at the component/source level;
- preserve truthful stale/retained states after source or transport failures;
- do not generate B/S/OUT labels, phase inference, forecasts, scenarios, recommendations, or trading actions.

The factual layer is designed so that a separate analysis consumer can distinguish what is current, what belongs to the previous completed session, what is publication-driven, and what has been retained after a failed refresh.

The HTTP/API/manual-export delivery layers are read-only consumers. They must not trigger source refresh, reinterpretation, or authority widening.

## 2. Freshness and cadence contract

The outer S7.3 snapshot generation timestamp is not sufficient evidence that every embedded dataset is current.

Every factual component must preserve, where the source provides the relevant concepts:
- source event or observation timestamp;
- source publication timestamp;
- observed/ingest timestamp;
- refresh-attempt timestamp;
- last-success timestamp;
- trade date or effective date;
- provenance/source identity;
- explicit freshness/retention/availability state.

Target rules:

1. **10-minute runtime cadence** means a refresh attempt approximately every 10 minutes for datasets classified as current/intraday and for publication-driven sources that are polled by the current runtime. It does not mean the source itself must publish a new value every 10 minutes.
2. **Current** means the latest valid source observation available to the governed collector at refresh time for the applicable authoritative observed trading date or source publication state.
3. A fresh outer snapshot must never relabel an older retained source observation as fresh.
4. If the source publishes no new observation between cycles, the prior source timestamp may legitimately remain unchanged while the refresh attempt succeeds.
5. If refresh fails, the last valid observation may be retained only with its original source timestamps and an explicit retained/stale/error state. A previous-session record must never be silently substituted for a current intraday record.
6. If no valid observation exists, expose an explicit unavailable/pending state rather than a neutral or fabricated value.
7. Trading-date authority remains source-observed; freshness logic must not infer trading dates from weekdays, weekends, or a removed calendar endpoint.
8. Source-specific publication cadence remains authoritative for daily, scheduled, or event-driven data. The target ingestion objective is to surface a newly published official value or item no later than the next normal factual refresh cycle where the source is technically pollable on that cadence.
9. Derived factual fields must carry references to the input observations used and must not appear more current than their least-current required input.

## 3. Core FX market data

Target datasets and freshness requirements:

| Dataset | Target role | Target refresh / freshness | Required analysis surface |
|---|---|---|---|
| USDRUBF / Si TradeStats and bars | Primary RUB futures market data | Current intraday; refresh attempt each ~10-minute cycle; latest valid observation from the authoritative observed FORTS trade date | Current market observation with source timestamps and provenance |
| CNYRUBF / CR TradeStats and bars | Secondary RUB futures market data and cross-market confirmation | Current intraday; refresh attempt each ~10-minute cycle; latest valid observation from the authoritative observed FORTS trade date | Current market observation with source timestamps and provenance |
| USD000UTSTOM | USD/RUB spot reference | Latest source observation available on the current factual refresh cycle; source timestamp preserved | Current spot observation |
| CNYRUB_TOM | CNY/RUB spot reference | Latest source observation available on the current factual refresh cycle; source timestamp preserved | Current spot observation |
| Open Interest | Positioning / participation context | Latest valid official observation; refresh attempt on the current cycle where the source supports current updates; do not fabricate intraday movement when source cadence is lower | Explicit observation timestamp/effective date and provenance |
| FUTOI Si | Factual physical-vs-legal positioning for Si | Current intraday refresh each ~10-minute cycle plus previous completed observed FORTS session | Both `current_intraday` and `previous_completed_session` simultaneously available |
| FUTOI CR | Factual physical-vs-legal positioning for CR | Current intraday refresh each ~10-minute cycle plus previous completed observed FORTS session | Both `current_intraday` and `previous_completed_session` simultaneously available |
| Si basis / carry | Futures-vs-spot relationship | Recompute/refresh from current governed inputs on the factual cycle; must preserve input timestamps | Current factual/derived relationship with input provenance |
| CR basis / carry | Futures-vs-spot relationship | Recompute/refresh from current governed inputs on the factual cycle; must preserve input timestamps | Current factual/derived relationship with input provenance |
| Market structure fields | Bars, levels, EMA-derived and other structural factual/derived fields already governed by the project | Refresh with current governed bar inputs; derived timestamps must not exceed input freshness | Current structural fields plus underlying data-as-of references |

Target principle: Si and CR should be represented symmetrically where the official source makes equivalent data available.

### 3.1 FUTOI target contract

For both `si_futures_family` and `cr_futures_family`, the canonical analysis snapshot must expose two independent factual layers:

**`current_intraday`**
- latest exact aligned FIZ/YUR source observation for the current authoritative observed FORTS trading date;
- refreshed through the existing approximately 10-minute factual runtime;
- latest aligned event selected using deterministic source timestamp and revision/seqnum rules;
- never backfilled with previous-session data under a current label.

**`previous_completed_session`**
- latest valid FUTOI observation from the authoritative observed FORTS trading session immediately preceding the current intraday trade date;
- selected from observed source dates, not calendar arithmetic;
- remains available while current intraday observations update.

Each layer must preserve at minimum:
- `instrument_id`;
- `trade_date`;
- `snapshot_ts`;
- `source_publication_time`;
- availability/ingest timestamps where available;
- source ticker;
- SECID;
- session identity;
- FIZ long/short/net and participant counts;
- YUR long/short/net and participant counts;
- total open interest;
- provenance;
- freshness/retention state.

The factual layer should preserve longer historical FUTOI evidence in source-native/history artifacts. A separate compact historical analysis surface may be added only as a separate scoped task; it must not replace the mandatory current + previous-session views.

FUTOI collection and analytical authority remain separate. Valid factual candidate data may be present while governance remains blocked.

## 4. External market and macro data

Target datasets:

| Dataset | Target role | Target refresh / freshness |
|---|---|---|
| CBR macro data | RUB domestic macro and monetary context | Publication-driven. Preserve effective date and publication time separately. Surface new official values by the next normal factual refresh cycle where the source is pollable. Retain prior values truthfully between publications. |
| Oil / Russian oil proxy where governed | External RUB commodity factor | Intraday where the governed source supports intraday observations; otherwise latest source publication. Preserve source timestamp and explicit stale state on failure. |
| RUB-denominated oil-price dataset | Research input: oil price × USD/RUB; no production authority until separately validated | Refresh only from governed input observations; preserve both input timestamps and never appear fresher than required inputs. |
| Global rates / USD context | External monetary and USD context | Intraday where governed sources support it; otherwise publication-driven. Preserve source timestamps and provenance. |
| Broader risk-market context | External factual risk environment | Intraday where governed sources support it; otherwise source-publication cadence. Preserve source timestamps and provenance. |

The target factual layer must distinguish raw observations from research-derived features.

## 5. NEWS — published facts and releases

NEWS is a separate dataset class from the future event calendar.

Definition:
> A news item is an already published factual release, statement, decision, announcement, or official publication.

Target coverage:
- Bank of Russia official publications;
- Russian government / finance-related official publications where relevant;
- Federal Reserve official releases;
- BLS and other relevant official US macro releases;
- official sanctions announcements;
- official geopolitical / policy announcements materially relevant to RUB, where a reliable primary source is available;
- official oil / energy publications relevant to the RUB external backdrop.

Target normalized fields:
- `source_id`;
- `source_name`;
- `source_url`;
- `published_at`;
- `observed_at` / ingestion timestamp;
- `title`;
- `body_or_summary` where permitted by source contract;
- `category`;
- `country_or_scope`;
- `dedup_key`;
- provenance metadata;
- quality / completeness flags.

Freshness/cadence target:
- publication-driven, not price-bar-driven;
- source polling/ingestion should participate in the normal factual refresh cycle where technically supported;
- a newly published official item should be surfaced by the next normal factual refresh cycle where the source is reachable and pollable;
- unchanged feeds/items must not be duplicated merely because another runtime cycle occurred;
- source publication time must remain distinct from observed/ingest time;
- failed source polling must be explicit and must not make older news appear newly published.

Required properties:
- primary-source preference;
- deterministic deduplication;
- explicit source provenance;
- publication timestamp preserved separately from ingestion timestamp;
- no trading/action authority.

Current project architecture already contains an `official_news` component and news ingestion pipeline. Source coverage, actual runtime freshness, duplicate behavior and missing-source handling must be audited separately before declaring NEWS complete.

## 6. EVENTS — scheduled and structured events

EVENTS is not the same as NEWS.

Definition:
> An event is a scheduled or structurally identifiable event that may occur in the future and later receive an actual result.

Target event classes:
- Bank of Russia rate meetings / decisions;
- Federal Reserve meetings / decisions;
- scheduled US macro releases relevant to USD/rates;
- OFZ auctions and results;
- Russian tax-period dates where they can be defined from authoritative sources;
- futures expiry / roll events relevant to Si and CR;
- scheduled sanctions / policy deadlines where an authoritative date exists;
- other material scheduled RUB/oil/macro events only when supported by reliable sources.

Target normalized fields:
- `event_id`;
- `event_type`;
- `source_id`;
- `source_url`;
- `scheduled_at`;
- `timezone`;
- `country_or_scope`;
- `instrument_scope`;
- `importance` only if source-defined or governed by an explicit project rule;
- `previous_value` when factual and source-supported;
- `forecast_value` only when a governed source is explicitly approved;
- `actual_value` after publication;
- `actual_published_at`;
- `status` such as scheduled / completed / cancelled / rescheduled;
- provenance metadata;
- quality / completeness flags.

Freshness/cadence target:
- event schedules are source-publication-driven rather than inferred from calendar arithmetic;
- scheduled time, reschedule/cancellation state and actual release state must be refreshed whenever the authoritative source changes;
- where the source is pollable by the normal factual runtime, newly published changes/actuals should be surfaced by the next normal factual refresh cycle;
- the last known schedule may be retained between source publications with its original publication/observation timestamps;
- an event must not be silently moved, invented, or marked completed without authoritative evidence.

The event layer must not infer or invent missing dates.

## 7. Trading-day governance

The removed MOEX Calendar API must not be restored.

Rules:
- never use `/iss/calendars.json` or `/iss/calendars` as a runtime dependency;
- never infer trading dates from weekdays/weekends;
- FORTS trading dates are based on actually observed AlgoPack FO TradeStats data;
- Stage3/4 may use only dates jointly observed by the required FORTS and CETS sources;
- a FO-only observed date must not force Stage3/4;
- financial calendar spreads are unrelated to the removed MOEX Calendar API.

For FUTOI, both the current intraday trade date and the previous completed-session trade date are derived from authoritative observed FORTS TradeStats dates, not from calendar arithmetic.

The future EVENTS dataset is an economic/market event calendar, not a replacement trading-day authority.

## 8. Retention and stale-state semantics

Retention exists to preserve factual evidence, not to hide refresh failure.

Required semantics:
- `FRESH`: latest valid source observation accepted under the component's source/freshness rules;
- retained/stale state: a previously valid observation preserved after a failed or unavailable refresh, with original source timestamps unchanged and the failed refresh attempt recorded separately;
- `UNAVAILABLE`/pending equivalent: no valid observation exists for the required factual slot;
- previous completed-session context is a distinct factual slot and is not stale merely because it belongs to a prior session;
- a retained prior intraday observation must not be confused with the previous completed-session slot;
- outer snapshot generation must not reset source age or publication timestamps.

Exact runtime status labels may differ by component contract, but the above semantics are mandatory.

## 9. Current known gap map at this revision

This section records management-level gaps and does not claim implementation completeness beyond verified project state.

| Area | Current management assessment |
|---|---|
| Si market data | Available |
| CR market data | Current TradeStats available |
| USD/RUB and CNY/RUB spot | Available |
| FUTOI Si | Current intraday + previous completed-session factual context implemented; authority remains governed separately |
| FUTOI CR | Current intraday + previous completed-session factual context implemented; authority remains governed separately |
| Si basis / carry | Available |
| CR basis / carry | Completeness to audit |
| Market structure | Available in existing governed data layer |
| Oil | Partial / requires source-completeness and freshness audit |
| CBR macro | Partial / requires source-completeness and freshness audit |
| Official NEWS | Pipeline exists; coverage/freshness/duplicate/missing-source audit required |
| Structured future EVENTS calendar | Major gap |
| Sanctions/geopolitical structured events | Major gap |
| Global rates / USD context | Partial / source and freshness coverage to audit |
| Broader external macro/risk context | Partial / source and freshness coverage to audit |

## 10. Priorities for subsequent source work

Source-development priority after the completed Si/CR FUTOI current+previous-session work is:

1. Audit NEWS source registry, actual runtime coverage, freshness, duplicates, source failures and missing official sources.
2. Define and implement a separate EVENTS data contract and authoritative-source registry.
3. Audit and complete CR basis/carry, oil, CBR macro, global rates/USD and broader risk-context sources against this freshness/cadence contract.
4. Re-run factual coverage and freshness audit before any analytical-model expansion.

Governance acceptance/promotion of FUTOI is a separate decision and is not a prerequisite for continuing factual source completion.

## 11. Authority boundaries

This target model does not authorize:
- B/S/OUT generation;
- phase-state inference;
- directional forecasts;
- scenario generation;
- recommendations;
- broker execution;
- Telegram trading;
- trading automation;
- Stage5 activation.

FUTOI remains factual context only unless separately governed otherwise:
- source artifacts do not self-grant factual authority;
- `directional_authority=false`;
- `action_authority=false`;
- `standalone_buy_sell_authority=false`.

Stage5 remains OFF unless separately and explicitly authorized.

## 12. Separation of subsequent tasks

Source completion, governance promotion, delivery/API work and analytical interpretation are separate task classes.

Delivery work must not silently modify:
- source coverage;
- data-authority rules;
- freshness semantics;
- event/news semantics;
- analytical interpretation;
- Stage5 state.

A successful factual source smoke does not by itself promote governance authority.
