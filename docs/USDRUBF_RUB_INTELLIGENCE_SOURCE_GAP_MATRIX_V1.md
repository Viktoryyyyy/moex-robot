# USDRUBF RUB Intelligence — Source Gap Matrix v1

PROJECT=MOEX_Bot

Status: working Source of Truth for source-completion and controlled shadow-runtime gates before alert delivery.

Baseline main SHA used for this inventory: `143387789e39c17e2f179251678274b9d5c8e04c`.

## 1. Completion rule

A source is **source-complete** only when it is either:

1. `LIVE_ACCEPTED`: registered, deterministic acquisition adapter exists, provenance/PIT semantics are enforced, all governing license/access gates are passed, a current live source smoke is accepted, and the resulting typed data is wired into the RUB Intelligence `DecisionInput` path; or
2. `GOVERNED_BLOCKED`: it has an explicit blocker and cannot silently contribute facts or ACTION authority.

Registry presence, parser/unit tests, technical endpoint access, Flowise availability, or synthetic classifier output alone do not make a source `LIVE_ACCEPTED`.

Runtime stages have a separate acceptance rule: code/CI does not make a stage operationally accepted until the canonical server proves the intended state and restart semantics.

## 2. Current runtime boundary

The live shadow Decision path is source-integrated for market, EMA, bounded official RSS NewsEvents and accepted CBR MacroState:

- market/EMA use the latest causally closed MOEX 5m data;
- News uses deterministic official-source acquisition and a bounded deterministic-neutral classifier (`direction=NEUTRAL`, `rub_relevance=0`, `confidence=0`); no Flowise/LLM factual or directional authority is required in the current News path;
- successful News records are conservatively stamped after acquisition completion; explicit historical `as_of` excludes records ingested after the cutoff;
- live `DecisionInput` receives at most 20 NewsEvents and reports the dropped count explicitly;
- CBR Key Rate + RUONIA are composed into `MacroState` with their frozen PIT semantics;
- FUTOI remains `GOVERNED_BLOCKED` unless its separate license/access gate is closed; technical wiring does not grant authority;
- the accepted recurring shadow boundary uses one explicit persistent `ShadowJsonStore` root, OS `flock` single-instance protection, bounded cadence, restart-safe prior-state reuse, atomic scheduler status, `SAFE_WAIT` only, no alert delivery and no broker/order execution.

Accepted integrated source server smoke on 2026-08-13 at applied main `b23cc36d35c5219a682f451085800f4493b3745f` proved:

- `STATUS=COMPLETED`;
- `MARKET_DATA_AS_OF_TIMESTAMP=2026-08-13T13:15:00+03:00`;
- `AS_OF_TIMESTAMP=2026-08-13T10:15:46.625377+00:00`;
- `NEWS_MODE=LIVE_RSS_DETERMINISTIC_NEUTRAL`;
- `NEWS_SOURCE_COUNT=11`;
- `NEWS_OK_SOURCE_COUNT=9`;
- `NEWS_FAILED_SOURCE_COUNT=2`;
- `NEWS_FAILED_SOURCE_IDS=bls_employment_situation_rss,bls_cpi_rss`;
- `NEWS_ACQUIRED_RECORD_COUNT=648`;
- `NEWS_PIPELINE_EVENT_COUNT=223`;
- `NEWS_EVENT_COUNT=20`;
- `NEWS_EVENTS_DROPPED_BY_BOUND=203`;
- `MACRO_MODE=LIVE_CBR`;
- `MACRO_OBSERVATION_COUNT=2`;
- `MACRO_METRIC_IDS=cbr_key_rate_pct,cbr_ruonia_rate_pct`;
- `FUTOI_QUALITY=BLOCKED`;
- `DECISION_AGENT_MODE=SAFE_WAIT`;
- `TRADE_STATE=WAIT`;
- `SIGNIFICANT_CHANGE=False`;
- `ACTION_CANDIDATE=False`.

Therefore the nine healthy official RSS paths listed below satisfy the current `LIVE_ACCEPTED` rule. The two BLS RSS routes remain fail-closed because the current server returned `SOURCE_UNAVAILABLE`; they do not silently contribute NewsEvents or ACTION authority.

Accepted S6.1 canonical-server persistence proof on 2026-08-13 at applied main `143387789e39c17e2f179251678274b9d5c8e04c` used the same explicit state root across two separate scheduler process invocations:

- `STATE_ROOT=/tmp/tmp.KPkAlAZrwH` for the bounded proof only;
- first invocation: `SCHEDULER_STATUS=COMPLETED`, `PRIOR_STATE_PRESENT=False`, `LAST_CYCLE_AS_OF_TIMESTAMP=2026-08-13T10:55:50.055752+00:00`;
- restarted invocation on the same root: `SCHEDULER_STATUS=COMPLETED`, `PRIOR_STATE_PRESENT=True`, `PRIOR_AS_OF_TIMESTAMP=2026-08-13T10:55:50.055752+00:00`;
- restarted invocation advanced to `LAST_CYCLE_AS_OF_TIMESTAMP=2026-08-13T10:55:56.550179+00:00`;
- both invocations reported `SUCCESSFUL_CYCLES=1`, `FAILED_CYCLES=0`, `LAST_SIGNIFICANT_CHANGE=False`, `LAST_ACTION_CANDIDATE=False`;
- `shadow_scheduler_status.json` persisted the restored prior timestamp and new generation paths under the same explicit state root.

This proves S6.1 restart-safe persistent state reuse. The temporary proof path is evidence only and is not a canonical permanent server path.

## 3. Market and positioning sources

| Source | Acquisition / validation | PIT / causal boundary | Current live acceptance | Decision wiring | Status | Next gate |
|---|---|---|---|---|---|---|
| USDRUBF/Si 5m MOEX FO feed | Existing `load_fo_5m_day` path reused by live shadow bridge | Only causally closed bars; latest market timestamp is reported separately from decision wall clock | Accepted in current integrated server smoke | Yes | `LIVE_ACCEPTED` | Keep unchanged |
| FUTOI Si | Existing validated loader and prior-session validator | Governing Phase 8.7A contract requires documented provider license/access terms | Current integrated smoke reports `FUTOI_QUALITY=BLOCKED` | Technical wiring exists when explicitly enabled, but no factual/ACTION authority while blocked | `GOVERNED_BLOCKED` | Close documented license/access gate, then re-run explicit technical smoke; directional semantics remain separate |

## 4. News factual-source matrix

The generic live RSS adapter retains the eight `FIRST_SLICE_SOURCE_IDS`; `LIVE_RSS_SOURCE_IDS` adds separately validated official sources. It enforces HTTPS publisher identity, bounded content, timezone-aware publication timestamps, future-item exclusion and source-specific provenance rules. Current live News interpretation is deterministic-neutral and cannot invent source facts or directional conviction.

| source_id | Registry state | Live acquisition adapter | PIT semantics | Current integrated smoke | Live Decision wiring | Source status | Next gate |
|---|---|---|---|---|---|---|---|
| `cbr_press_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | `OK` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `cbr_events_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | `OK` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `moex_all_news_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | `OK` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `moex_fx_news_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | `OK` | Yes | `LIVE_ACCEPTED` | Structurally valid empty feeds remain acceptable; keep live smoke available |
| `fed_press_all_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | `OK` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `fed_monetary_policy_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | `OK` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `bls_employment_situation_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | `SOURCE_UNAVAILABLE` | Excluded by fail-closed source result | `GOVERNED_BLOCKED` | Re-prove stable official BLS RSS access; until then no NewsEvent/ACTION authority |
| `bls_cpi_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | `SOURCE_UNAVAILABLE` | Excluded by fail-closed source result | `GOVERNED_BLOCKED` | Re-prove stable official BLS RSS access; until then no NewsEvent/ACTION authority |
| `bls_release_calendar` | `READY_CANDIDATE` | No factual-content adapter | Calendar time is schedule/context only and never proves content availability | Not part of current live Decision News path | No | `GOVERNED_BLOCKED` | Keep schedule-only unless a separate bounded context adapter is needed |
| `us_treasury_press_releases` | `READY_CANDIDATE` | Yes — bounded Treasury HTML index/detail adapter | Detail publication `<time datetime>` governs publication/availability; source-bound content is bounded | Separate source adapter/smoke exists; not included in the current 11-source integrated RSS run | No current live-shadow wiring | `ADAPTER_READY_NOT_LIVE_ACCEPTED` | Wire only if Treasury coverage is required beyond the already accepted RSS set |
| `ofac_recent_actions` | `READY_CANDIDATE` | No accepted factual adapter | A provable publication timestamp is required | Not eligible | No | `GOVERNED_BLOCKED` | Keep blocked until publication timestamp semantics are provable |
| `whitehouse_releases` | `READY_CANDIDATE` | Yes — generic Stage 12B.1 RSS | RSS publication timestamp; `available_at=published_at` | `OK` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `eu_council_press_releases` | `READY_CANDIDATE` | Yes — generic Stage 12B.1 RSS | Atom `updated`; `available_at=published_at` | `OK` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `eu_commission_news` | `READY_CANDIDATE` | Yes — generic Stage 12B.1 RSS with explicit item-host allowlist | RSS `pubDate`; Council remains primary authority for Council-adopted sanctions when available | `OK` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `opec_press_releases` | `BLOCKED_PENDING_PROVABLE_PUBLICATION_TIMESTAMP` | No | Official pages expose calendar date but no proven timezone-aware publication timestamp; synthetic time substitution forbidden | Not eligible | No | `GOVERNED_BLOCKED` | Reopen only with provable publication timestamp or separately approved first-observed-time contract |
| `kremlin_events` | `BLOCKED_PENDING_STABLE_ROUTE_ADAPTER` | No | Stable route/timestamp not proven | Not eligible | No | `GOVERNED_BLOCKED` | Keep blocked |
| `minfin_ru_press_center` | `BLOCKED_PENDING_ROUTE_VERIFICATION` | No | Stable route/timestamp not proven | Not eligible | No | `GOVERNED_BLOCKED` | Keep blocked |
| `rosstat_official_releases` | `BLOCKED_PENDING_STABLE_INDEX_ROUTE` | No | Stable machine-readable route/availability policy not proven | Not eligible | No | `GOVERNED_BLOCKED` | Keep blocked |
| `mfa_ru_news` | `BLOCKED_PENDING_ROUTE_VERIFICATION` | No | Stable official route/timestamp not proven | Not eligible | No | `GOVERNED_BLOCKED` | Keep blocked |
| `reuters_major_agency` | `BLOCKED_PENDING_APPROVED_ROUTE_AND_RIGHTS` | No approved factual route | Rights/timestamp policy unresolved; no scraping fallback | Not eligible | No | `GOVERNED_BLOCKED` | Keep blocked until approved route/rights policy exists |

### X/Twitter discovery class

The Stage 12A X whitelist remains `DISCOVERY_ONLY`. There is no live X factual acquisition path. An X post alone cannot create a usable NewsEvent, cannot count as factual confirmation, and cannot produce ACTION authority.

## 5. Macro-source matrix

| source_id | Deterministic loader | Source/PIT state | Current RUB Intelligence live smoke | MacroObservation / Decision wiring | Status | Next gate |
|---|---|---|---|---|---|---|
| `cbr_key_rate_daily` | Yes — `external_data/cbr.py::load_key_rate_daily` | Effective-date semantics implemented; route/provenance enforced | 2026-08-12 source smoke `QUALITY=OK`, value `14.0`; 2026-08-13 integrated Decision smoke `MACRO_MODE=LIVE_CBR` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `cbr_ruonia_daily` | Yes — `external_data/cbr.py::load_ruonia_daily` | Row publication date explicit; next-day Moscow availability boundary preserved | 2026-08-12 source smoke `QUALITY=OK`, value `13.57`; 2026-08-13 integrated Decision smoke `MACRO_MODE=LIVE_CBR` | Yes | `LIVE_ACCEPTED` | Keep source/PIT regression and live smoke available |
| `cbr_banking_liquidity_daily` | Yes | `blocked_pending_vintage_policy` | Not eligible | No | `GOVERNED_BLOCKED` | Keep blocked until row-level vintage governance is frozen |
| `moex_brent_futures_daily` | External-data foundation exists | `blocked_pending_source_validation` | Not eligible | No | `GOVERNED_BLOCKED` | Complete source validation before use |
| `cme_wti_pre_moex` | External-data foundation/current delayed route exists | `blocked_pending_license` | Not eligible | No | `GOVERNED_BLOCKED` | Resolve license/approved route before use |

## 6. What is already complete and must not be reopened

- deterministic market structure / levels / test-retest-breakout semantics;
- EMA 3/19 AI live context;
- Unified Decision Engine, Change Detector and restart-safe ShadowRuntime boundaries;
- bounded official RSS acquisition and PIT/provenance controls;
- deterministic-neutral no-Flowise live News classifier path;
- bounded NewsEvents wired into live `DecisionInput`;
- source failure visibility and 20-event DecisionInput bound;
- CBR Key Rate + RUONIA deterministic MacroObservation adapter, current live acceptance and live DecisionInput wiring;
- controlled recurring shadow scheduler with explicit persistent state root, bounded cadence, `flock` single-instance protection, atomic status and restart-safe prior-state reuse;
- historical Flowise Decision/News classifier contracts remain available but are not required by the current no-Flowise path.

FUTOI governance and additional blocked source families remain separate from the accepted integrated shadow path. Alert delivery is not yet accepted. Autonomous broker/order execution remains outside v1.

## 7. Canonical source-completion workflow

### S2.1 — Existing eight RSS current live source acceptance — COMPLETED
Current integrated smoke proves six first-slice RSS sources `OK` and exposes both BLS RSS routes as `SOURCE_UNAVAILABLE`. No source failure is silently dropped.

### S2.2 — Real News end-to-end acceptance — COMPLETED
Accepted current route:

`official RSS -> deterministic acquisition -> PIT/provenance guard -> dedupe/cluster -> deterministic-neutral bounded classifier -> NewsEvent`

Real source-backed NewsEvents are produced without Flowise. Source-bound fields remain deterministic and the classifier contributes zero directional/confidence authority.

### S2.3 — Wire real NewsEvents into live DecisionInput — COMPLETED
The live shadow runner composes bounded source-backed NewsEvents, reports per-source quality/failures and caps DecisionInput at 20 latest events.

### S2.4 — Additional factual News coverage — NON-BLOCKING / GOVERNED
White House, EU Council and EU Commission are now `LIVE_ACCEPTED`. BLS RSS access is governed-blocked by current `SOURCE_UNAVAILABLE`. Treasury has a bounded HTML adapter but is not required for the current minimum integrated path and is not promoted without live-shadow wiring. OFAC, OPEC, Russian official sources and Reuters remain blocked as listed above.

X remains discovery-only and is not a blocker for factual-source completion.

### S3.1 — CBR Macro adapter — COMPLETED
Typed Key Rate and RUONIA MacroObservations are deterministic and source-bound.

### S3.2 — CBR Macro current live acceptance — COMPLETED
Current source smoke proves source access, timestamps, availability and PIT exclusion.

### S3.3 — Wire MacroState into live DecisionInput — COMPLETED
The live Decision runner composes accepted CBR MacroState by default and preserves a separate market-data timestamp versus decision wall clock.

### S3.4 — Additional macro/oil sources — GOVERNED_BLOCKED
`cbr_banking_liquidity_daily`, MOEX Brent and CME WTI remain blocked until their existing source/vintage/license gates are closed.

### S4 — FUTOI governance — GOVERNED_BLOCKED
Technical transport/schema validation does not clear the governing license/access blocker. FUTOI remains excluded from factual/ACTION authority until the required provider evidence is approved. Current frozen direction remains `MIXED`/zero-confidence.

### S5 — Integrated source acceptance — COMPLETED WITH GOVERNED FUTOI EXCLUSION
The accepted 2026-08-13 shadow run consumed:

- live USDRUBF/Si market data;
- EMA context;
- bounded factual official RSS NewsEvents from nine healthy sources while exposing two BLS failures;
- accepted CBR MacroState;
- the bounded Decision Engine/ShadowRuntime path in `SAFE_WAIT`.

It explicitly reported `FUTOI_QUALITY=BLOCKED`, `ACTION_CANDIDATE=False` and did not treat missing FUTOI authority as accepted positioning data.

### S6.1 — Controlled shadow scheduler + persistent state — COMPLETED
Merged runtime: `143387789e39c17e2f179251678274b9d5c8e04c`.

Canonical-server proof demonstrated:

- explicit state root;
- single bounded cycle completing successfully;
- a second independent scheduler process reusing the same root;
- `PRIOR_STATE_PRESENT=True` after restart;
- prior `as_of` restored exactly;
- new `as_of` strictly later than persisted prior state;
- zero failed cycles;
- `LAST_ACTION_CANDIDATE=False`;
- scheduler status persisted atomically under the state root.

No alert delivery or broker/order execution was activated.

### S6.2 — Change Detector alert delivery — NEXT
Alert delivery may now be added only as a consumer of persisted Change Detector / scheduler output. It must not create market facts, alter DecisionInput, invent ACTION authority, or enable broker/order execution. Delivery deduplication and restart safety must be proven before acceptance.

## 8. Immediate next task

`S6.2_change_detector_alert_delivery_v1`

Build the smallest bounded alert-delivery layer after the accepted scheduler/persistence boundary. Requirements: consume only persisted Change Detector / scheduler results, deterministic severity gate, restart-safe deduplication, explicit delivery status, fail-closed transport errors, no mutation of MarketState/DecisionInput, no Flowise dependency, and no broker/order execution. Prove first with a non-delivering/dry transport fixture, then with an explicitly approved live notification transport in a separate acceptance step.
