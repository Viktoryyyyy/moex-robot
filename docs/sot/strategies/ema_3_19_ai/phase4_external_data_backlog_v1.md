# Phase 4.0 external data backlog v1

Status: design/contract planning only  
Task: `ema_3_19_ai_market_phase_phase_4_0_external_data_backlog_contracts`  
Lane: `ema_3_19_ai`  
Created: 2026-07-09

## Root task and roadmap binding

Root task: research USDRUBF / Si daily market phases, characterize B / S / OUT phases, design a model for phase completion/new phase start, and design a daily AI market assistant.

This artifact is bound only to fixed roadmap step 4: external data backlog contracts/providers.

Completed upstream steps are context only:
- Phase 3.2 — D1 panel output contract + build command PR — done.
- Phase 3.3 — first controlled 5-date D1 panel build — done.
- Phase 3.4 — full historical D1 panel build — done.

Step 5 first analysis on the internal D1 panel with manual B/S/OUT labels and step 6 factor batches into a modeling dataset remain pending and are not authorized by this artifact.

## Phase 4 purpose

Define a prioritized backlog of external data sources and provider-readiness contracts for later USDRUBF / Si D1 phase research.

For each source this artifact records:
- business factor rationale;
- provider/dataset candidate status;
- point-in-time availability requirement;
- timestamp and publication-time policy;
- license/access status;
- storage contract placeholder;
- D1 join grain;
- expected feature family;
- current readiness status.

## Non-authorizations

This artifact does not authorize:
- external data ingestion;
- provider API calls;
- generated data creation;
- D1 panel output modification;
- data materialization;
- feature computation;
- model fitting;
- prediction;
- broker/trading code;
- runtime/server apply.

## Strict RUB rate rule

CBR official exchange rate must not be treated as a causal market driver. It may only be used as a reference or lagged indicator if explicitly authorized later. Market USD/RUB, CNY/RUB, futures, and spot proxies must remain separate from CBR official rates.

Bank of Russia key rate calendar is a schedule/reference factor only.

## Current Phase 3.4 internal D1 context

The completed internal D1 panel context is known from the upstream PM handoff:
- instrument context: USDRUBF / Si D1 panel;
- run_id: `phase3_4_full_exact_history`;
- row_count: 1048;
- date range: 2022-04-26 .. 2026-06-11;
- forbidden label columns: none.

This Phase 4 artifact does not modify or reference generated output paths as contract paths.

## Priority A backlog

| Source | Factor rationale | Provider / dataset candidate status | PIT requirement | Timestamp / publication-time policy | License / access status | Storage contract placeholder | D1 join grain | Expected feature family | Readiness |
|---|---|---|---|---|---|---|---|---|---|
| `futoi_participant_positioning`<br>FUTOI / participant positioning | Participant positioning can describe real-money/speculative pressure around USDRUBF/Si and may help explain phase persistence or exhaustion, but only if exact point-in-time feed semantics are proven. | `not_approved_exact_provider_required`<br>Provider: TBD exact FUTOI provider.<br>Dataset: TBD exact schema, publication timestamp, contract mapping, and storage contract. | Blocked until every observation has as-of timestamp, publication/availability time, instrument mapping, and evidence that no future revisions leak into trade_date features. | Store provider timestamp, exchange/session timestamp if applicable, and availability timestamp. D1 features may use only observations available before the configured decision cutoff. | `blocked_until_provider_license_and_access_terms_are_documented` | `contracts/datasets/futoi_participant_positioning.v1.yaml` placeholder after provider approval only. | `instrument_id + trade_date` after PIT availability alignment; no same-day use unless publication time is before decision cutoff. | participant_position_direction; participant_position_pressure; participant_group_divergence; participant_position_change | `blocked` |
| `oil_proxy_brent`<br>Oil proxy: Brent or approved oil futures/index provider | Oil prices affect Russia export revenue expectations, current-account expectations, and fiscal/FX-flow expectations that can influence ruble market regimes. | `candidate_required_not_approved`<br>Provider: TBD approved Brent/oil futures or index provider.<br>Dataset: TBD front-month, settlement, continuous futures, or approved index proxy. | Use only prices, settlements, and rolls available as of the D1 decision cutoff; continuous-series construction must not revise past roll decisions with future information. | Store observation timestamp, exchange/session close timestamp, settlement publication timestamp if used, and provider availability timestamp. | `needs_provider_decision` | `contracts/datasets/oil_proxy_brent.v1.yaml` placeholder; no ingestion authorized. | `trade_date/session_date` aligned to USDRUBF D1 trade_date using PIT availability and exchange calendar mapping. | commodity_price; returns; trend_regime; volatility | `needs_provider_decision` |
| `cnyrub_market_quote`<br>CNY/RUB market quote | CNY/RUB can capture current Russian FX market pressure and non-USD settlement dynamics separately from official reference rates. | `candidate_required_not_approved`<br>Provider: TBD MOEX market quote, approved market-data vendor, or other approved tradable CNY/RUB proxy.<br>Dataset: TBD market CNY/RUB spot/futures quote series with timestamped availability. | Use tradable/market observations only; no CBR official rate as causal driver; values must be available before D1 decision cutoff. | Store market timestamp, session timestamp, source availability timestamp, and quote/settlement type. | `needs_provider_decision` | `contracts/datasets/cnyrub_market_quote.v1.yaml` placeholder; no ingestion authorized. | `trade_date` aligned to USDRUBF D1 trade_date by market session and decision cutoff. | market_fx_quote; cross_rate; returns; basis_or_spread | `needs_provider_decision` |
| `usd_index_proxy`<br>DXY or approved USD index proxy | Broad USD strength can distinguish RUB-specific moves from global USD regime pressure. | `candidate_required_not_approved`<br>Provider: TBD DXY or approved USD index proxy provider.<br>Dataset: TBD DXY index, USD futures basket proxy, or approved substitute. | Use only observations available before D1 decision cutoff; no backfilled revised values unless vintages are retained. | Store market timestamp, index calculation timestamp if applicable, and provider availability timestamp. | `needs_provider_decision` | `contracts/datasets/usd_index_proxy.v1.yaml` placeholder; no ingestion authorized. | `calendar_date/trade_date` mapped to USDRUBF D1 trade_date using PIT availability. | usd_index; returns; trend_regime; volatility | `needs_provider_decision` |
| `ru_tax_periods_calendar`<br>RU tax periods calendar | Russian tax-payment periods may affect local FX liquidity and exporter conversion expectations, potentially shaping short-term RUB phases. | `candidate_required_not_approved`<br>Provider: TBD official tax calendar or approved static calendar source.<br>Dataset: TBD tax-payment dates/windows with source and publication/version metadata. | Calendar must be known in advance or versioned by publication date; amendments must be represented by vintage/as-of date. | Store calendar publication/version timestamp and event effective date; D1 features can use only events known as of trade_date. | `needs_provider_decision` | `contracts/datasets/ru_tax_periods_calendar.v1.yaml` placeholder; no ingestion authorized. | `trade_date` joined to event windows: on_event, days_to_event, days_since_event, active_window. | calendar_event; tax_period_window; days_to_from_event | `needs_provider_decision` |
| `cbr_key_rate_calendar`<br>Bank of Russia key rate calendar | Scheduled monetary-policy meetings and key-rate decisions can affect rate expectations and risk appetite; this is schedule/reference context only, not an official FX-rate driver. | `candidate_required_not_approved`<br>Provider: TBD Bank of Russia official policy calendar/decision source or approved mirror.<br>Dataset: TBD key-rate meeting calendar and historical key-rate decisions with publication timestamps. | Meeting schedule must be versioned by publication date; decision values may be used only after official publication timestamp. | Store meeting date, decision publication timestamp, effective date, and source availability timestamp. | `needs_provider_decision` | `contracts/datasets/cbr_key_rate_calendar.v1.yaml` placeholder; no ingestion authorized. | `trade_date` joined to meeting/decision dates with days_to_event, days_since_event, and post_publication flags. | policy_calendar; key_rate_level; days_to_from_meeting; decision_surprise_placeholder | `needs_provider_decision` |
| `ru_us_holidays_calendar`<br>RU/US holidays calendar | Holiday and settlement-calendar effects can alter liquidity, session structure, and cross-market alignment between RUB and global USD markets. | `candidate_required_not_approved`<br>Provider: TBD approved exchange/settlement holiday calendars for Russia and US markets.<br>Dataset: TBD RU exchange holidays, US market holidays, settlement holidays. | Calendar events must be known in advance or versioned by publication date; changes require vintage tracking. | Store calendar publication/version timestamp, market identifier, and holiday/session date. | `needs_provider_decision` | `contracts/datasets/ru_us_holidays_calendar.v1.yaml` placeholder; no ingestion authorized. | `trade_date` joined to holiday/session flags and days_to_from_holiday windows. | calendar_event; liquidity_regime; session_availability | `needs_provider_decision` |
| `futures_expiry_roll_effects`<br>Futures expiry / roll effects | USDRUBF/Si expiry and roll windows can affect liquidity, spreads, and price behavior around contract transitions. | `candidate_required_not_approved`<br>Provider: TBD exchange contract calendar and approved contract metadata source.<br>Dataset: TBD expiry dates, contract codes, roll rules, last trade dates, settlement dates. | Contract metadata must be known as of trade_date; any exchange schedule changes must be versioned. | Store contract-calendar publication/version timestamp and event effective dates. | `needs_provider_decision` | `contracts/datasets/futures_expiry_roll_effects.v1.yaml` placeholder; no ingestion authorized. | `instrument_id + trade_date` with days_to_expiry, active_roll_window, and contract_code mapping placeholders. | futures_calendar; days_to_expiry; roll_window; contract_liquidity_shift | `needs_provider_decision` |

## Priority B backlog

| Source | Factor rationale | Provider / dataset candidate status | PIT requirement | Timestamp / publication-time policy | License / access status | Storage contract placeholder | D1 join grain | Expected feature family | Readiness |
|---|---|---|---|---|---|---|---|---|---|
| `ofz_yield_rates_proxy`<br>OFZ yield / rates proxy | Domestic rates and OFZ yield regimes can affect RUB carry attractiveness, local liquidity, and risk premium. | `candidate_required_not_approved`<br>Provider: TBD approved OFZ yield curve, index, or rates data provider.<br>Dataset: TBD OFZ curve nodes, RGBI/OFZ index proxies, or approved rates proxy. | Use only values published before D1 decision cutoff; revised curves or indices require vintage/as-of tracking. | Store observation date, calculation/publication timestamp, and provider availability timestamp. | `needs_provider_decision` | `contracts/datasets/ofz_yield_rates_proxy.v1.yaml` placeholder; no ingestion authorized. | `trade_date` mapped by publication availability to USDRUBF D1 trade_date. | rates_proxy; yield_level; curve_slope; yield_change | `needs_provider_decision` |
| `open_interest_liquidity_volume_extensions`<br>Open interest / liquidity / volume extensions | Liquidity and participation changes can help distinguish durable phase changes from low-liquidity noise. | `candidate_required_not_approved`<br>Provider: TBD MOEX/FORTS or approved futures market data provider.<br>Dataset: TBD daily open interest, volume, value, number of trades, bid/ask or liquidity proxy if approved. | Use only observations available before D1 decision cutoff; settlement/end-of-day statistics require publication timestamp handling. | Store exchange observation date, session close, statistic publication timestamp, and provider availability timestamp. | `needs_provider_decision` | `contracts/datasets/open_interest_liquidity_volume_extensions.v1.yaml` placeholder; no ingestion authorized. | `instrument_id + trade_date` after publication-time alignment. | market_microstructure_daily; open_interest; volume_value; liquidity_regime | `needs_provider_decision` |
| `volatility_regime_features`<br>Volatility regime features | Volatility regime can condition phase persistence, false breakouts, and risk of stop-outs independently from trend direction. | `candidate_required_not_approved`<br>Provider: TBD internal D1-derived volatility or approved external vol proxy.<br>Dataset: TBD realized volatility from internal D1 panel or approved external implied/realized volatility series. | If derived internally, use only lagged D1 observations available before the decision cutoff; if external, use publication/as-of timestamps. | Store derivation timestamp and source observation cutoff; no feature computation authorized in Phase 4.0. | `needs_provider_decision` | `contracts/datasets/volatility_regime_features.v1.yaml` placeholder if external source is selected; no feature materialization authorized. | `instrument_id + trade_date` using lagged/PIT inputs only. | volatility_regime; realized_volatility; range_expansion; vol_proxy | `needs_provider_decision` |
| `sanctions_geopolitical_event_flags`<br>Sanctions/geopolitical event flags | Sanctions and geopolitical events can shift risk premium, liquidity access, capital-flow expectations, and ruble phase behavior. | `not_approved_governed_event_source_required`<br>Provider: TBD governed event-source policy and approved dataset/provider.<br>Dataset: TBD event taxonomy with publication timestamp, source attribution, severity policy, and revision/version handling. | Blocked until event taxonomy, publication time, source reliability, and revision policy are defined; no hindsight labeling allowed. | Must store first-publication timestamp, source timestamp, event effective timestamp/date, and any correction/revision timestamp. | `blocked_until_source_license_and_attribution_policy_are_approved` | `contracts/datasets/sanctions_geopolitical_event_flags.v1.yaml` placeholder after governed source/taxonomy decision only. | `trade_date` event windows based on first-publication timestamp and event effective date. | event_flag; risk_regime; days_to_from_event; severity_placeholder | `blocked` |
| `import_export_trade_balance_proxy`<br>Import/export or trade balance proxy | External trade-flow proxies can represent medium-term FX supply/demand pressure relevant to RUB phase context. | `candidate_required_not_approved`<br>Provider: TBD approved official statistics source, market proxy, or alternative data provider.<br>Dataset: TBD trade balance, export/import proxy, current-account proxy, or high-frequency substitute. | Macro values must be stored by vintage/as-of publication date; revised history must not overwrite prior vintages for model training. | Store period_end_date, first_publication_timestamp, revision_timestamp if any, and provider availability timestamp. | `needs_provider_decision` | `contracts/datasets/import_export_trade_balance_proxy.v1.yaml` placeholder; no ingestion authorized. | `trade_date` uses latest vintage available before decision cutoff, carried forward only under an approved as-of policy. | macro_flow_proxy; trade_balance; export_import_proxy; publication_lag | `needs_provider_decision` |

## Explicit blocked-source handling

FUTOI / participant positioning is blocked. It remains blocked unless exact provider, schema, timestamp/publication policy, storage contract, and license/access evidence are specified and approved.

FUTOI / participant positioning is participant-positioning only and must not include open-interest feature semantics. Open interest is separate and belongs only under `open_interest_liquidity_volume_extensions` after later PM-approved source/contract approval.

Sanctions/geopolitical event flags are blocked until a governed source, event taxonomy, first-publication timestamp policy, and licensing/attribution policy are approved.

Sources with `needs_provider_decision` are not approved for ingestion. They are candidates for provider selection and contract completion only.

## Provider-readiness gate before step 5 / step 6

Before any external factor ingestion or factor-batch construction:
1. `contracts/validation/usdrubf_phase4_external_data_readiness_gate_v1.yaml` must pass.
2. Provider/license/access decisions must be recorded.
3. PIT and timestamp/publication-time policies must be complete.
4. Storage contracts must be approved.
5. D1 join grain must be defined without lookahead leakage.
6. CBR official FX rates must remain excluded as causal drivers.

Until this gate passes, step 5 must use only the internal D1 panel and manual B/S/OUT labels; step 6 must not materialize external factor batches.
