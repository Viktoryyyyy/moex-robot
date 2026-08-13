# USDRUBF RUB Intelligence — Source Gap Matrix v1

PROJECT=MOEX_Bot

Status: working Source of Truth for source-completion gates before scheduler/ACTION delivery.

Baseline main SHA used for this inventory: `09a68666c2c45d1c96aebddb2211d51c710d6283`.

## 1. Completion rule

A source is **source-complete** only when it is either:

1. `LIVE_ACCEPTED`: registered, deterministic acquisition adapter exists, provenance/PIT semantics are enforced, all governing license/access gates are passed, a current live source smoke is accepted, and the resulting typed data is wired into the RUB Intelligence DecisionInput path; or
2. `GOVERNED_BLOCKED`: it has an explicit blocker and cannot silently contribute facts or ACTION authority.

Registry presence, parser/unit tests, technical endpoint access, Flowise availability, or a synthetic classifier smoke alone do not make a source `LIVE_ACCEPTED`.

## 2. Current runtime boundary

The current live Decision smoke is partially source-integrated:

- `src/moex_research/runners/usdrubf_live_shadow_smoke.py` still composes `news_events=()`; accepted factual NewsEvents are not yet wired into the live Decision runner;
- CBR Macro is enabled by default in the live Decision runner and composes accepted Key Rate + RUONIA `MacroObservation` values into `MacroState`;
- each live CBR source is stamped after its loader response returns, and final `DecisionInput.as_of_timestamp` is captured after CBR acquisition; `MARKET_DATA_AS_OF_TIMESTAMP` separately preserves the latest closed 5m market-bar timestamp;
- FUTOI is `BLOCKED` unless the operator explicitly runs the smoke with `--enable-futoi`; even an enabled technical `OK` does not clear its separate license/access blocker;
- the separate live News composition exists in `src/moex_research/intelligence/usdrubf_news_live_pipeline.py`, but it is not yet composed into the live Decision runner.

Accepted integrated server smoke on 2026-08-13 at applied main `09a68666c2c45d1c96aebddb2211d51c710d6283` proved:

- `STATUS=COMPLETED`;
- `MARKET_DATA_AS_OF_TIMESTAMP=2026-08-13T12:00:00+03:00`;
- `AS_OF_TIMESTAMP=2026-08-13T12:02:51.168876+03:00`;
- `MACRO_MODE=LIVE_CBR`;
- `MACRO_OBSERVATION_COUNT=2`;
- `MACRO_METRIC_IDS=cbr_key_rate_pct,cbr_ruonia_rate_pct`;
- `DECISION_AGENT_MODE=SAFE_WAIT`;
- `ACTION_CANDIDATE=False`.

Therefore CBR Key Rate and RUONIA satisfy the current `LIVE_ACCEPTED` rule. News integration and FUTOI governance remain separate incomplete boundaries.

## 3. Market and positioning sources

| Source | Acquisition / validation | PIT / causal boundary | Current live acceptance | Decision wiring | Status | Next gate |
|---|---|---|---|---|---|---|
| USDRUBF/Si 5m MOEX FO feed | Existing `load_fo_5m_day` path reused by live shadow bridge | Only bars closed by `as_of_timestamp`; prior session is causal | Accepted in current server Decision smoke | Yes | `LIVE_ACCEPTED` | Keep unchanged |
| FUTOI Si | Existing validated FUTOI loader and prior-session validator reused by bridge | Prior-session pair validation exists; governing Phase 8.7A contract additionally requires documented provider license/access terms | Latest Decision smoke ran without `--enable-futoi`; separately, the governing contract status is `blocked_pending_documented_license_and_access_terms` and explicitly says successful authenticated access is insufficient | Typed FUTOI context is technically wired when enabled, but it must not obtain factual/ACTION authority while the governance blocker remains | `GOVERNED_BLOCKED` | An explicit-enabled smoke may prove transport/schema only. Before `LIVE_ACCEPTED`, require the contract's license/access evidence and close `provider_license_and_access_terms_not_documented`; only then accept `FUTOI_QUALITY=OK` as live-source evidence. Direction remains `MIXED`/zero-confidence until a separate directional rule is frozen. |

## 4. News factual-source matrix

The generic live RSS adapter retains the eight `FIRST_SLICE_SOURCE_IDS` as its original baseline; `LIVE_RSS_SOURCE_IDS` may add separately validated official sources. It enforces HTTPS publisher identity, bounded content, timezone-aware publication timestamps, future-item exclusion and `available_at=published_at`.

| source_id | Registry state | Live acquisition adapter | PIT semantics | Current live source smoke | News Classifier path | Live Decision wiring | Source status |
|---|---|---|---|---|---|---|---|
| `cbr_press_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `cbr_events_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `moex_all_news_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `moex_fx_news_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `fed_press_all_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `fed_monetary_policy_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `bls_employment_situation_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `bls_cpi_rss` | `READY_CANDIDATE` | Yes — Stage 12B.1 RSS | Implemented | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `bls_release_calendar` | `READY_CANDIDATE` | No | Policy only: calendar time is context and never proves content availability | Not run | No | No | `ADAPTER_MISSING` |
| `us_treasury_press_releases` | `READY_CANDIDATE` | No | Registry policy only | Not run | No | No | `ADAPTER_MISSING` |
| `ofac_recent_actions` | `READY_CANDIDATE` | No | Registry requires a provable publication timestamp; otherwise timestamp-unprovable | Not run | No | No | `ADAPTER_MISSING` |
| `whitehouse_releases` | `READY_CANDIDATE` | Yes — generic Stage 12B.1 RSS | Implemented: RSS publication timestamp; `available_at=published_at` | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `eu_council_press_releases` | `READY_CANDIDATE` | Yes — generic Stage 12B.1 RSS | Implemented: Atom `updated`; `available_at=published_at` | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `eu_commission_news` | `READY_CANDIDATE` | Yes — generic Stage 12B.1 RSS with explicit item-host allowlist | Implemented: RSS `pubDate`; `available_at=published_at`; Council remains primary authority for Council-adopted sanctions when available | Not proven on current server | Wired through bounded live News pipeline | No | `ADAPTER_READY_NOT_LIVE_ACCEPTED` |
| `opec_press_releases` | `BLOCKED_PENDING_PROVABLE_PUBLICATION_TIMESTAMP` | No | Official index/detail pages expose only a calendar publication date; no timezone-aware publication timestamp is currently proven. Synthetic midnight/meeting-time substitution is forbidden. | Not eligible | No | No | `GOVERNED_BLOCKED` |
| `kremlin_events` | `BLOCKED_PENDING_STABLE_ROUTE_ADAPTER` | No | Not proven | Not run | No | No | `GOVERNED_BLOCKED` |
| `minfin_ru_press_center` | `BLOCKED_PENDING_ROUTE_VERIFICATION` | No | Not proven | Not run | No | No | `GOVERNED_BLOCKED` |
| `rosstat_official_releases` | `BLOCKED_PENDING_STABLE_INDEX_ROUTE` | No | Not proven | Not run | No | No | `GOVERNED_BLOCKED` |
| `mfa_ru_news` | `BLOCKED_PENDING_ROUTE_VERIFICATION` | No | Not proven | Not run | No | No | `GOVERNED_BLOCKED` |
| `reuters_major_agency` | `BLOCKED_PENDING_APPROVED_ROUTE_AND_RIGHTS` | No approved factual route | Rights/timestamp policy unresolved | Not run | No | No | `GOVERNED_BLOCKED` |

OPEC blocker evidence (2026-08-12): the official press-release index provides a calendar date and detail links, while inspected detail pages expose no machine-readable `datePublished` or timezone-aware publication timestamp. Do not infer `00:00`, meeting time, or any other synthetic release time. Reopen this source only when an official provable publication timestamp or a separately approved first-observed-time contract is available.

### X/Twitter discovery class

The Stage 12A X whitelist remains `DISCOVERY_ONLY` by contract. There is no live X acquisition adapter in the current RUB Intelligence runtime. An X post alone cannot create a usable NewsEvent, cannot count as factual confirmation, and cannot produce ACTION authority. No scraping fallback is allowed.

## 5. Macro-source matrix

The existing external-data foundation is reused. `cbr_key_rate_daily` and `cbr_ruonia_daily` now have deterministic loaders, typed `MacroObservation` adapters, current live acceptance, and live `DecisionInput` composition.

| source_id | Deterministic loader | Source/PIT state | Current RUB Intelligence live smoke | MacroObservation / Decision wiring | Status | Next gate |
|---|---|---|---|---|---|---|
| `cbr_key_rate_daily` | Yes — `external_data/cbr.py::load_key_rate_daily` using official change-date history | Effective-date semantics implemented; new rate is not usable before `effective_date`; source provenance/route enforced by adapter | Accepted: 2026-08-12 source smoke `QUALITY=OK`, value `14.0`; 2026-08-13 integrated Decision smoke `MACRO_MODE=LIVE_CBR` | MacroObservation adapter: Yes — `intelligence/usdrubf_macro_live_cbr.py`; Decision wiring: Yes | `LIVE_ACCEPTED` | Keep source/PIT regression tests and current live smoke available |
| `cbr_ruonia_daily` | Yes — `external_data/cbr.py::load_ruonia_daily` | Row publication date explicit; Phase 8.2 strict-prior-publication rule preserved by next-day Moscow availability boundary; source provenance/route enforced by adapter | Accepted: 2026-08-12 source smoke `QUALITY=OK`, value `13.57`, `AVAILABLE_AT=2026-08-12T00:00:00+03:00`; 2026-08-13 integrated Decision smoke `MACRO_MODE=LIVE_CBR` | MacroObservation adapter: Yes — `intelligence/usdrubf_macro_live_cbr.py`; Decision wiring: Yes | `LIVE_ACCEPTED` | Keep source/PIT regression tests and current live smoke available |
| `cbr_banking_liquidity_daily` | Yes | `blocked_pending_vintage_policy` | Not eligible | No | `GOVERNED_BLOCKED` | Keep blocked until row-level historical/current vintage governance is frozen |
| `moex_brent_futures_daily` | External-data foundation exists | `blocked_pending_source_validation` | Not eligible as accepted RUB Intelligence macro source | No | `GOVERNED_BLOCKED` | Complete source validation before use |
| `cme_wti_pre_moex` | External-data foundation/current delayed snapshot route exists | `blocked_pending_license` | Not eligible | No | `GOVERNED_BLOCKED` | Resolve license/approved data route before use |

## 6. What is already complete and must not be reopened

- deterministic market structure / levels / test-retest-breakout semantics;
- EMA 3/19 AI live context;
- Unified Decision Engine and ShadowRuntime boundaries;
- Flowise Decision Agent Applied State and authenticated transport;
- Flowise News Classifier Applied State, authenticated transport, bounded input/output validator;
- Flowise runtime User-Agent correction;
- bounded parsing of the deployed JSON response fence;
- synthetic authenticated News Classifier smoke;
- authenticated live Decision Agent smoke;
- CBR Key Rate + RUONIA deterministic MacroObservation adapter, current live source acceptance, and live DecisionInput wiring.

These prove the interpretation/runtime layer and accepted CBR Macro path. They do not substitute for remaining News source completion or FUTOI governance.

## 7. Canonical source-completion workflow

### S2.1 — Existing eight RSS: current live source acceptance
Run the real Stage 12B.1 RSS smoke from the canonical server on the exact applied SHA. Record per-source status, record count, latest provable timestamp and failure class. No Flowise is needed for this subgate.

Acceptance: every one of the eight sources is either `OK` with timestamp/provenance proof or has an explicit source-specific blocker. Do not silently drop failed sources.

### S2.2 — Real News end-to-end acceptance
Run actual source records through:

`official RSS -> deterministic acquisition -> dedupe/cluster -> authenticated Flowise News Classifier -> Stage12B3 output validation -> NewsEvent`.

Acceptance: at least one real source-backed NewsEvent is produced and all source-bound fields remain deterministic.

### S2.3 — Wire real NewsEvents into live DecisionInput
Replace the current `news_events=()` live-runner boundary with an explicit bounded composition of the accepted live News pipeline. Preserve fail-closed behavior and source failure visibility.

### S2.4 — Expand factual News coverage
Implement one source family at a time, in this order unless new evidence changes priority:

1. BLS release calendar as schedule/context only;
2. US Treasury + OFAC;
3. White House;
4. EU Council, then EU Commission;
5. OPEC — currently `GOVERNED_BLOCKED` pending a provable timezone-aware publication timestamp;
6. blocked Russian official sources only after stable route/timestamp proof;
7. Reuters only after an approved route and rights policy.

X remains discovery-only and is not a blocker for factual-source completion.

### S3.1 — CBR Macro adapter — COMPLETED
Convert `cbr_key_rate_daily` and `cbr_ruonia_daily` deterministic loader outputs into typed `MacroObservation` records without LLM-created numeric facts.

### S3.2 — CBR Macro current live acceptance — COMPLETED
Current live source smoke proved source timestamps, `available_at`, PIT exclusion and current source access for Key Rate + RUONIA.

### S3.3 — Wire MacroState into live DecisionInput — COMPLETED
The live Decision runner now composes accepted CBR MacroState by default. CBR acquisition timestamps are not backdated to the latest market bar; decision `as_of` is captured after acquisition and the latest closed market-bar time is reported separately.

### S3.4 — Additional macro/oil sources
Only after registry blockers are resolved. `cbr_banking_liquidity_daily`, MOEX Brent and CME WTI remain blocked until their existing blocker is actually closed.

### S4 — FUTOI governance and technical acceptance
The existing explicit-enabled live smoke may be used only to prove current transport/schema/PIT runtime behavior. A technical `FUTOI_QUALITY=OK` does **not** promote the source.

The governing `usdrubf_phase8_7a_futoi_si_source_and_feature_contract_v1` requires documented MOEX AlgoPack FUTOI license/access evidence and states that a successful authenticated request is insufficient. Until `provider_license_and_access_terms_not_documented` is closed with the required evidence artifact, FUTOI remains `GOVERNED_BLOCKED` and must not receive factual/ACTION authority.

Acceptance for `LIVE_ACCEPTED` requires both:

1. approved license/access evidence satisfying the governing contract; and
2. an explicit-enabled current smoke with `FUTOI_QUALITY=OK` and causal prior-session validation.

Do not invent directional semantics; current frozen positioning-only semantics remain `MIXED`/zero-confidence until a separate directional rule is frozen.

### S5 — Integrated source acceptance
One current shadow run must consume, at minimum:

- live USDRUBF/Si market data;
- EMA context;
- FUTOI only after S4 is fully `LIVE_ACCEPTED`; while its governance blocker remains, it must stay excluded/blocked and S5 cannot claim FUTOI integration;
- at least one accepted factual News source path, with real NewsEvent when an eligible current item exists;
- accepted CBR MacroState from key rate/RUONIA;
- authenticated Decision Agent.

The acceptance output must retain per-source quality/degraded status so `0` events or missing observations are distinguishable from source failure. If FUTOI remains `GOVERNED_BLOCKED`, integrated acceptance must explicitly report that blocker rather than treating technical access as source acceptance.

### S6 — Only after S5
Proceed to scheduler, persistence/history policy, significant-change ACTION delivery and user alerts. Autonomous broker/order execution remains outside v1.

## 8. Immediate next task

`S2.3_live_news_decision_wiring_v1`

CBR Macro S3.1-S3.3 is complete and S3.4 remains governed-blocked. The next actionable integration gap is the existing `news_events=()` boundary in the live Decision runner. Compose only accepted factual NewsEvents through the bounded live News pipeline, preserve source failure visibility, keep source-bound facts deterministic, and do not change FUTOI governance, scheduler, alerting, broker, or trading authority in this task.
