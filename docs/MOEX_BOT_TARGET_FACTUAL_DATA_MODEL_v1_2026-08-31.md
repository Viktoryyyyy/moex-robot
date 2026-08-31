# MOEX Bot — Target Factual Data Model v1

Date: 2026-08-31
Status: target management model; not an implementation contract for interpretation or trading logic
Project: MOEX_Bot

## 1. Purpose

Define the target factual data layer required for RUB / USDRUBF analysis before further analytical-model work.

Current priority is data collection and refresh only:
- collect factual data;
- preserve provenance and timestamps;
- refresh the current snapshot on the existing 10-minute runtime cadence;
- do not generate B/S/OUT labels, phase inference, forecasts, scenarios, recommendations, or trading actions.

The future API layer is a separate task and must consume the factual layer without widening authority.

## 2. Core FX market data

Target datasets:

| Dataset | Target role |
|---|---|
| USDRUBF / Si TradeStats and bars | Primary RUB futures market data |
| CNYRUBF / CR TradeStats and bars | Secondary RUB futures market data and cross-market confirmation |
| USD000UTSTOM | USD/RUB spot reference |
| CNYRUB_TOM | CNY/RUB spot reference |
| Open Interest | Positioning / participation context |
| FUTOI Si | Factual physical-vs-legal positioning for Si |
| FUTOI CR | Factual physical-vs-legal positioning for CR |
| Si basis / carry | Futures-vs-spot relationship |
| CR basis / carry | Futures-vs-spot relationship |
| Market structure fields | Bars, levels, EMA-derived and other structural factual/derived fields already governed by the project |

Target principle: Si and CR should be represented symmetrically where the official source makes equivalent data available.

## 3. External market and macro data

Target datasets:

| Dataset | Target role |
|---|---|
| CBR macro data | RUB domestic macro and monetary context |
| Oil / Russian oil proxy where governed | External RUB commodity factor |
| RUB-denominated oil-price dataset | Research input: oil price × USD/RUB; no production authority until separately validated |
| Global rates / USD context | External monetary and USD context |
| Broader risk-market context | External factual risk environment |

The target factual layer must distinguish raw observations from research-derived features.

## 4. NEWS — published facts and releases

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

Required properties:
- primary-source preference;
- deterministic deduplication;
- explicit source provenance;
- publication timestamp preserved separately from ingestion timestamp;
- no trading/action authority.

Current project architecture already contains an `official_news` component and news ingestion pipeline. Source coverage and freshness must be audited separately before declaring NEWS complete.

## 5. EVENTS — scheduled and structured events

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

The event layer must not infer or invent missing dates.

## 6. Trading-day governance

The removed MOEX Calendar API must not be restored.

Rules:
- never use `/iss/calendars.json` or `/iss/calendars` as a runtime dependency;
- never infer trading dates from weekdays/weekends;
- FORTS trading dates are based on actually observed AlgoPack FO TradeStats data;
- Stage3/4 may use only dates jointly observed by the required FORTS and CETS sources;
- a FO-only observed date must not force Stage3/4;
- financial calendar spreads are unrelated to the removed MOEX Calendar API.

The future EVENTS dataset is an economic/market event calendar, not a replacement trading-day authority.

## 7. Current known gap map at the time of this target-model revision

This section records management-level gaps and does not claim implementation completeness beyond verified project state.

| Area | Current management assessment |
|---|---|
| Si market data | Available |
| CR market data | Current TradeStats available |
| USD/RUB and CNY/RUB spot | Available |
| FUTOI Si | Available as factual-only context |
| FUTOI CR | Gap to close |
| Si basis / carry | Available |
| CR basis / carry | Completeness to audit |
| Market structure | Available in existing governed data layer |
| Oil | Partial / requires source-completeness audit |
| CBR macro | Partial / requires source-completeness audit |
| Official NEWS | Pipeline exists; coverage/freshness audit required |
| Structured future EVENTS calendar | Major gap |
| Sanctions/geopolitical structured events | Major gap |
| Broader external macro/risk context | Partial |

## 8. Priorities for subsequent source work

After the separate API task, source-development priority is:

1. Close FUTOI CR gap.
2. Audit NEWS source registry, actual runtime coverage, freshness, duplicates, and missing official sources.
3. Define and implement a separate EVENTS data contract and authoritative-source registry.
4. Audit and complete oil, CBR macro, global rates/USD, and broader risk-context sources.
5. Re-run factual coverage audit before any analytical-model expansion.

## 9. Authority boundaries

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

FUTOI remains factual context only:
- `factual_authority=false`;
- `directional_authority=false`;
- `action_authority=false`.

Stage5 remains OFF unless separately and explicitly authorized.

## 10. Separation of subsequent tasks

The next PML2 task for API work is separate from source completion.

API work must not silently modify:
- source coverage;
- data-authority rules;
- event/news semantics;
- analytical interpretation;
- Stage5 state.

After API completion, return to this target model and execute source-gap closure as separate isolated tasks.
