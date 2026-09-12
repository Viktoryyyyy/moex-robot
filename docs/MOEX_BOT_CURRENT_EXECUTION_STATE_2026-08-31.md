# MOEX Bot — Current Execution State — 2026-08-31

status: current_execution_handoff
project: MOEX_Bot
repository: Viktoryyyyy/moex-robot
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
operating_model: `docs/MOEX_Bot_Role_Context_Operating_Model_v2_execution_modes_2026-06-16.md`
recorded_date: 2026-08-31
validated_main_sha_before_this_document: `27a1fc9e22fbc5bcd9731aa6f71e06f1b5c6ffc2`

This document records the currently validated RUB market-data runtime after removal of the MOEX Calendar API runtime dependency and restoration of Stage 10. It is an operational handoff, not a replacement for the management canon. GitHub/repository remains Source of Truth; the server is Applied State only. Every mutable SHA/state must be revalidated before a new mutation, merge or server apply.

## 1. Canonical server context

```text
HOME=/home/trader
repo_path=/home/trader/moex_bot/moex-robot
shell_path=~/moex_bot/moex-robot
command_prefix=cd ~/moex_bot && source venv/bin/activate && cd moex-robot
data_root=/home/trader/moex_bot/data
env_path=/home/trader/moex_bot/.env
```

Deprecated underscore repository-path variants are forbidden. Do not infer architecture or accepted state from the server filesystem.

## 2. Current runtime boundary

The validated active runtime is a deterministic market-data/research pipeline. It is not an autonomous trading system.

Current authority constraints:

- Stage 5 full mode remains disabled: `stage5_full_mode_ready=false`;
- FUTOI may be collected and exposed as factual context only;
- FUTOI has no directional authority and no action authority;
- a blocked FUTOI governance gate does not block Stage 7;
- no runtime component may fabricate a trading date from weekday/weekend rules;
- no implicit latest-date request is accepted where an explicit source date is required;
- failed quality/acceptance must not promote canonical pointers.

## 3. Stage 10 daily RUB refresh

Canonical entrypoint:

```text
python -m moex_data.step10_rub_refresh_entrypoint
```

Validated deterministic refresh order:

```text
1. futoi_governance
2. stage5_full_mode_readiness
3. futoi_raw_factual_refresh
4. observed_forts_dates
5. stage7_raw_and_derived
6. observed_cets_common_dates
7. stage3
8. stage4
9. stage7_pointer_promotion
10. stage9_smoke
```

The last validated live run was:

```text
run_id=stage10_cross_market_date_postmerge_20260831_v1
through_date=2026-08-30
status=succeeded
implicit_latest_used=false
network_sources_explicitly_bounded_by_date=true
```

## 4. MOEX Calendar API removal

The active Stage 10 path no longer depends on the MOEX Calendar API `/iss/calendars.json` or `/iss/calendars` for runtime trade-date selection.

Replacement semantics are source-observation based:

1. FO observed dates are obtained from the existing MOEX AlgoPack FO TradeStats source.
2. The FO reference instrument is resolved from the canonical registry; the validated current source secid is `SiU6`.
3. The observed-date transport performs bounded exact-date source requests. For each candidate date, `date`, `from`, and `till` are the same explicit date and the secid is explicit.
4. A date is accepted only when the source returns an actual matching row for the requested secid and exact `TRADEDATE`.
5. Empty source dates are skipped. They are not converted into inferred weekends, holidays, or trading days.
6. Transport errors, invalid schema, repeated pagination, secid mismatches, or returned-date mismatches fail closed.

This policy deliberately separates an observed source date from a presumed exchange calendar date.

## 5. Cross-market Stage 3/4 date selection

FO and CETS do not necessarily publish usable data on the same calendar dates. In particular, an FO source observation must not be used as proof that CETS TOM candles exist on that date.

Stage 3/4 therefore use:

```text
latest_common_observed_fo_tradestats_and_both_cets_tom_sources
```

The date is selected only from actual observations available to all required source legs. No weekday/calendar inference is allowed.

Validated live state for `through_date=2026-08-30`:

```text
stage3_stage4_common_observed_dates:
- 2026-08-25
- 2026-08-26
- 2026-08-27
- 2026-08-28

stage3_stage4_current_trade_date=2026-08-24
stage3_stage4_target_trade_date=2026-08-28
source_refresh.status=refreshed
source_refresh.trade_date=2026-08-28
stage3_pointer_count=10
stage4_pointer_count=2
```

This is the expected behavior when FO has later observations but the required CETS legs do not share those dates.

## 6. Stage 7 validated state

From the same successful live Stage 10 run:

```text
stage7.status=refreshed
stage7.output_count=8
stage7.canonical_pointer_promotion.status=promoted
stage7.canonical_pointer_promotion.pointer_count=8
```

Stage 7 remains independent of a blocked FUTOI governance gate.

## 7. FUTOI validated factual state

Latest validated source-native FUTOI state:

```text
instrument_id=si_futures_family
secid=SiU6
trade_date=2026-08-30
snapshot_ts=2026-08-30T16:00:00+00:00
acceptance_status=PASS
quality_status=PASS
freshness.status=FRESH
status=PASS
```

Validated factual values:

```text
FIZ long=1356772
FIZ short=179755
FIZ net=1177017
FIZ long_participants=12927
FIZ short_participants=3601

YUR long=4416702
YUR short=5593719
YUR net=-1177017
YUR long_participants=400
YUR short_participants=145

total_open_interest=5773474
```

Governance remains intentionally restrictive:

```text
futoi_governance.status=FUTOI_GOVERNED_BLOCKED
blocked_gate_ids=[recurring_live_quality_and_freshness]
factual_live_authority=false
directional_authority=false
action_authority=false
stage5_pointer_promotion_performed=false
```

The FUTOI block does not block Stage 7. These values are factual observations only and must not be treated as a standalone directional or trading signal.

## 8. Stage 5 state

Validated state:

```text
stage5_full_mode_ready=false
stage5.status=governed_blocked_not_run
stage5.canonical_pointer_promotion=false
stage5.output_count=0
```

Stage 5 must remain disabled unless separately authorized through the project governance process.

## 9. Stage 9, news/macro and chat snapshot boundary

Validated Stage 9 smoke from the successful Stage 10 run:

```text
stage9_smoke.status=passed
daily_block_count=20
weekly_block_count=24
daily_bundle_status=partial_external_context_required_position_risk_not_supplied
weekly_bundle_status=partial_external_context_and_policy_gaps_position_risk_not_supplied
```

Therefore Stage 9 by itself must not be described as a complete news/macro/position-risk bundle.

Separately, current `main` contains a dedicated persisted S7.3 chat-analysis snapshot layer. Its canonical producer set explicitly includes:

```text
stage9_daily
stage9_weekly
live_market_structure
cbr_macro
official_news
cnyrub_spot_live
cnyrubf_live
```

The snapshot also carries a governed-blocked oil component until an oil source is `LIVE_ACCEPTED`. The persisted snapshot schema is `rub_chat_analysis_snapshot.v1`; the repository-defined relative state path is:

```text
state/rub_intelligence/chat_analysis_snapshot/current.json
```

under `MOEX_DATA_ROOT`.

The `official_news` component is built from the repository live official-news pipeline and is explicitly bounded as:

```text
mode=LIVE_RSS_DETERMINISTIC_NEUTRAL
directional_action_authority=false
```

The `cbr_macro` component is read as factual live CBR macro state with `action_authority=false`.

This means news/macro collection code is present on current `main` and is part of the chat-snapshot producer architecture. The successful Stage 10 evidence above does not by itself prove that every current chat-snapshot component is live/READY at this exact moment; that must be established by the fresh snapshot refresh/read validation performed after this documentation merge.

## 10. Existing read-only chat consumer boundary

Current `main` already contains a fail-closed read-only consumer:

```text
src/moex_research/consumers/usdrubf_chat_snapshot_consumer.py
```

It reads only the canonical persisted chat snapshot, validates schema/project/freshness/readiness/authority boundaries, and does not fetch arbitrary market/news data on behalf of a chat.

There is also an MCP server adapter:

```text
src/misc/mcp_rub_analysis_snapshot_server.py
```

Its only tool is `rub_analysis_snapshot()`, which returns the validated canonical persisted snapshot through the consumer above. It intentionally exposes no direct MOEX/news/macro fetch, scenario generation, BUY/SELL/OUT, broker, or Telegram action. The current adapter runs MCP over `stdio`; it is not yet the server-network API requested for remote chat access.

## 11. Operational systemd state

The server-applied Stage 10 timer/service were directly verified after the successful runtime repair.

Timer:

```text
unit=moex-rub-stage10-daily-refresh.timer
OnCalendar=*-*-* 00:30:00 Europe/Moscow
Persistent=true
service=moex-rub-stage10-daily-refresh.service
```

Service:

```text
User=trader
WorkingDirectory=/home/trader/moex_bot/moex-robot
MOEX_DATA_ROOT=/home/trader/moex_bot/data
PYTHONPATH=src
MOEX_ENV_FILE=/home/trader/moex_bot/.env
```

Execution uses a filesystem lock under the canonical data root and runs the repository entrypoint through `/home/trader/moex_bot/venv/bin/python`. The service explicitly computes the Moscow-date `through_date` and passes it to Stage 10. Stage 10 then resolves usable source dates from actual bounded source observations.

The server timer inventory also directly showed `moex-rub-chat-snapshot.timer` / `moex-rub-chat-snapshot.service`. Exact service command/configuration must be read from the unit before it is used as deployment evidence; the unit name alone is not architectural proof.

## 12. Implemented repair history

The current working Stage 10 runtime was restored through the following merged changes:

```text
PR #413 — remove MOEX Calendar API runtime dependency
PR #414 — fix Stage 10 observed TradeStats reference secid
PR #415 — fix Stage 10 TradeStats exact-date scan transport
PR #416 — fix Stage 10 cross-market common observed date selection
```

Validated server-applied main SHA before this documentation change:

```text
27a1fc9e22fbc5bcd9731aa6f71e06f1b5c6ffc2
```

A new documentation merge will supersede that repository SHA without changing the runtime semantics described above. Server Applied State must always be compared to the then-current merged GitHub SHA.

## 13. Next implementation boundary: server-network active-data API

The next planned implementation is not a second data pipeline. It is a network-readable, read-only exposure of the existing canonical chat snapshot consumer so authorized chats/consumers can read active state without inspecting arbitrary server files.

Required boundary:

- repository implementation first; server deployment second;
- reuse `load_analysis_chat_snapshot()` as the canonical data-read boundary;
- do not duplicate Stage 9/news/macro/market calculation logic inside the API;
- read-only endpoints/tools only;
- no order placement, position mutation, sizing, scenario generation, BUY/SELL/OUT, Telegram, or trading actions;
- expose source/as-of date, read freshness, component readiness, provenance where present, and authority flags already carried by the snapshot;
- fail closed on missing, stale where freshness is required, schema-invalid, or authority-inconsistent state;
- preserve `stage5_full_mode_ready=false`;
- preserve FUTOI directional/action authority false;
- preserve news directional/action authority false;
- define authentication and network exposure explicitly before binding a remote/public interface.

The existing `stdio` MCP adapter is useful reusable code, but it is not evidence of a remotely reachable API.

## 14. Resume protocol

Before the next mutation or server apply:

1. revalidate current GitHub `main` and exact branch head;
2. preserve one task/branch mutation owner and branch isolation;
3. require exact-current-head CI PASS before merge;
4. merge through GitHub; do not write directly to `main`;
5. server-apply only the exact merged SHA to a clean `main` working tree;
6. run a fresh uniquely identified Stage 10 refresh after apply;
7. run and read a fresh S7.3 chat snapshot, recording component statuses and freshness;
8. use that validated canonical snapshot consumer as the sole data source for the server-network read API.

## Root task checkpoint — rub_snapshot_target_completion_v1 (2026-09-08)

Owner delegates automatic merge after exact-head independent review and CI; server apply requires a separately recorded exact merged SHA. Root coordinator owns that queue. This section is the durable checkpoint for this root task.

Goal: one compact self-contained JSON for a new daily/weekly RUB analytical chat through the canonical reader/export path, with dated accepted facts, explicit limitations, no forecast/trading authority.

Minimal product contract, recorded before implementation:
- A / mandatory server facts: all admitted USDRUBF, Si front/next, CNYRUBF, CR front/next and CNYRUB_TOM OHLC/last/OI, available quotes, contract units/expiry/timestamps; separate price/OI, quote and comparison usability. All admitted individual basis/carry metrics remain lossless. Existing USDRUBF levels, zones, interactions, extrema and admitted H1/D1/W1 context are carried with causal dates. FUTOI current and only instrument-admitted dated history/comparisons remain bounded by existing evidence; CR current-pair-only scope stays intact. Accepted rates/RUONIA/liquidity/CPI/Brent/external CNY and Minfin/calendar context or precise source blockers; compact dated original news marked NOT_ANALYZED/UNKNOWN. Explicit user position only, otherwise NO_EXPLICIT_USER_INPUT. Completeness is bidirectional: admitted mandatory facts present, rejected facts absent.
- B / analytical chat external context: interpretation and scenarios, discretionary external commentary and expectations. This never substitutes for missing mandatory server facts. Unknown consensus and publication timestamps remain unknown.
- C / optional enrichment: additional world instruments (WTI/Urals/DXY/UST), new volume features, relevance scoring, additional derived features; not required for this factual contract.
- D / future model: training, evaluation, probabilities and historical model acceptance do not define factual-package readiness and are not authorized here. No signals, orders, execution or Stage5 promotion.

Acceptance: coherent API/export as_of and component versions; compact facts readable without server paths; source/temporal/freshness gates preserved; explicit missing reasons; frozen replay and immutable archives; production HTTP and fast/heavy refresh evidence plus labelled simulations. Root COMPLETED requires all mandatory usable paths, not just this first projection task.

Closed baseline: PRs #497/#498/#499; main/server baseline verified by root at 0186fc182712d78f647230f9c04d5cf452b46dbe. Do not reopen without regression.
Active task: rub_snapshot_core_projection_v1; sole mutation owner package_implementation; isolated branch codex/rub-snapshot-core-projection-v1; PR/head pending. Scope: existing factual release/projection/acceptance and consumer tests/docs. Root handles delivery/readiness/source follow-up, merge and server apply. No server mutation performed by this task.
Next: implement completeness, independent exact-head review and CI, then root merge/apply/runtime checkpoint. Missing legacy execution-mode/parallel-lane documents are not guessed; management canon and explicit Owner isolation rules govern this execution.

PM_L1 scoped decision (before code): Si previous dated facts require current admission, Si governance factual_use_allowed and temporal_applicability.previous.dated_observation_available. Existing delta/statistics may be projected only with matching current admitted pair identity, accepted observed-date witness and AVAILABLE values; unavailable field reasons stay explicit. Evidence: rub_temporal_applicability.py; futoi_delta_statistics_context.py; snapshot_current_context.py Si parent grant. Engine factual_authority=false prevents self-grant; this consumer scope comes from the Si parent gate. CR remains current_intraday_latest_pair_only; no prior/history grant. Baseline production HTTP passed 17 checks/15 facts at 2026-09-08T19:55:53.980682Z (root evidence).

PM_L1 scope expansion approved before edit: src/moex_research/intelligence/usdrubf_news_macro.py plus related test. NewsEvent dropped representative.headline, making original content unrecoverable after NOT_ANALYZED mechanism normalization. Preserve headline with a backward-compatible default; no new source or impact inference.

PM_L1 metadata completeness decision: join existing admitted basis leg units/expiry only on exact market secid, source timestamp and raw value identity; conflicting or mismatched generations do not join and standalone price/OI survives. Separate admitted quotes survive missing price/OI. Source evidence confirms basis legs carry RFUD LASTTRADEDATE and normalization metadata.
External blocker evidence from root (not simulated): canonical server official Minfin index HTTP503 at 2026-09-08T20:01:36Z; Telegram Errno101 Network unreachable at20:02:49Z with verified TLS/no redirects. Required Minfin remains next-stage source work. Fast cycles19:59/20:01/20:05/20:06Z HTTP200 on heavy19:57:56Z; next heavy20:08:08Z and fast20:08:22Z HTTP200. Genuine expired spot16:15Z versus futures20:06Z correctly leaves basis PARTIAL.

Root-owned bounded factual-data recovery decision, recorded before runtime: Si 2026-08-19 only using existing materialize_futoi_instrument on verified applied0186fc182712d78f647230f9c04d5cf452b46dbe. Independent method review PASS; require absent partition/quality/manifest, unchanged raw/EOD accepted pointers, source-native latest-pair validation with no fallback. No CR, training or pointer promotion. Authorized under original minimum-history permission and independent of projection code. Root alone executes work/rub_target_recover_si_aug19.py; package executor does not perform server recovery.

Root recovery result: PASS at2026-09-08T20:14:23.975770Z on0186fc182712d78f647230f9c04d5cf452b46dbe. SiAug19 source404rows qualitypass, latest-native factual AVAILABLE; SHA25687d3be9f33bad530de5f25bd3921568292c2a50b1c82be2fac1a6e58b64cf836; accepted pointers unchanged. Run rub_snapshot_si_20d_aug19_v1; evidence /home/trader/moex_bot/deploy_backups/rub_snapshot_target_completion_v1_baseline/si_aug19_recovery.json. Await ordinary heavy refresh before claiming consumer20D availability.

Projection validation before commit: affected factual release/projection, headline/news selection and release acceptance101PASS (one Linux fcntl-dependent case deselected). Broader Windows RUB sweep403PASS/36fail before final news assertion update: fcntl unavailable and checked-in schedule/archive hashes affected by Windows autocrlf; source news audit no-headline assertion updated to permit approved headline preservation while still excluding body. Exact-head Linux CI/review required before merge. Implemented core projection includes all source OHLC fields, separately admitted quote-only values, exact-generation basis units/expiry, full structure/dates, Si admitted history and strict CR exclusion, original news headlines, explicit position, and reverse omission/refusal checks. No merge or code server apply by executor. Remaining root stages: readiness/horizons, canonical delivery/export, Minfin source blocker and production acceptance.

PR500 first remote head f24307f (full head tracked by root), independent review CHANGES_REQUIRED: malformed basis raised during full read freshness and legacy catch restored persisted expired FUTOI admission. Same-task correction removes unsafe fallback; malformed read view produces no release, input remains immutable. Synthetic expired Si/CR with malformed basis exercises describe and build. Affected suite103PASS/one Linux-only deselected after correction; previous CI/review stale until updated exact remote head.

PR500 same-task P2 correction authorization before edit: PM_L1 expands scope only to usdrubf_shadow_runtime.py NewsEvent deserialization and backward-compatible headline roundtrip test; no shadow/trading runtime execution or other behavior change. Automated review also requires independent oracle equality for transformed observed_extrema and explicit/absent/invalid position. Add corruption/omission regressions; repeat exact-head CI/review. Do not resolve review threads before evidence inspection.

Observed root production recovery acceptance: ordinary heavy generation2026-09-08T20:17:53.617811Z has Si1D/5D/20D all AVAILABLE,20D target2026-08-19. CR remains current-only and internal20D UNAVAILABLE. This is actual production observation after bounded source recovery, not a simulation.
Three P2 corrections implemented on same branch/PR500: optional headline survives NewsEvent deserialization with legacy empty default; oracle independently compares transformed full extrema and complete explicit/absent/invalid position. Regression cases reject dropped or changed dates/prices/direction/entry and forbidden position fields. Affected suite123PASS/one Linux-only deselected; root to publish next exact head and repeat CI/review before resolving threads.

## Root task next checkpoint — rub_snapshot_delivery_readiness_v1
Prior core phase accepted: PR500 merged/applied a2e8eb48adfabee5fe685da8869976d601179bdb, server clean main,100installedtestsPASS; realHTTP17gatesPASS at2026-09-08T20:41:55.782910Z (13facts). Evidence /home/trader/moex_bot/deploy_backups/rub_snapshot_target_completion_v1_core_installed/factual-acceptance-_c0w_edp/report.json. Initial deployment trailing PowerShell CR triggered clean code rollback to0186fc without data rollback; corrected script explicit exit/main reattach then exacta2e8 retry passed installed/HTTP acceptance: incident resolved. Existing source refusals remain refusals.
Active task rub_snapshot_delivery_readiness_v1, sole owner package_implementation, new isolated branch codex/rub-snapshot-delivery-readiness-v1 from exacta2e8. Prior branch immutable. Root owns publication/merge/apply. Scope existing release/projection/acceptance, canonical consumer/HTTP, appropriate tests/docs, macro/source matrix only as needed.
Contract before implementation: one compact factual package from canonical reader with fast overlay, one captured aware as_of, exact executing revision and explicit slow/fast component generations. API/current export use same builder; preserve frozen audit export compatibility and no-overwrite archives. Compact facts retain numeric values, dates, source URLs and missing reasons; raw files/hashes/manifests stay audit except generation identity. Mandatory factual coverage is separate from presentation integrity and model readiness; all seven markets, basis, current/admitted FUTOI, structure/H1/D1/W1, CBR rates/RUONIA/four liquidity metrics, weekly/monthly CPI, published Brent/external CNY, news content/selection, explicit position or NOINPUT state, Minfin plan and minimum CBRmeeting/weeklyCPI finite calendar coverage remain mandatory. Minfin remains required EXTERNAL_BLOCKER from verified503/Errno101; broader global calendar/model vintages deferred. No arbitrary READY from missing inputs.
D1 review period current Moscow date through as_of; W1 review Monday through as_of, with separate nextMonday-Sunday prospective horizon (Sunday regular preparation). Bar aggregation intervals remain dated source observations. Planned trading dates require source-covered calendar; no schedule-derived session completion. No training/trades/new indicators. Existing accepted EMA/trend may be retained as deterministic dated context without standalone authority. Next: implement, validate positive/refusal coverage and compact/API/export parity, independent exact-head review/CI, then root deployment.

PM_L1 bounded Si previous consumer rule supersession BEFORE edit: current pair failure no longer erases independently admitted Si previous-date observation. Require existing Si governance factual_use_allowed, temporal previous.dated_observation_available, no explicit previous denial and already validated causal/source metadata. It remains dated observation, never current/completed session; comparisons still require admitted matching current. CR previous stays denied. Production20:44:11Z heavy20:41:11Z both current FUTOI refused FIZ/YUR net positions do not balance to zero, RETAINED_STALE; previous Sep7 witness remains valid. Compact must retain precise source refusal and permitted Si previous, without fabricated current/deltas.

Second-task runtime observations from root before deployment of this task:20:46:11Z installed core has20/20headlines, structure AVAILABLE and8timeframe blocks, current FUTOI refused balance, previousSep7datedvalid, sixfreshfutures/stalespot. Ordinary heavy20:51:06.878003Z observed20:57:48Z: Si/CR recovered READY/FRESH current; futures timestamps20:50:01Z were already stale at read, all excluded correctly, spot16:15; basis UNAVAILABLE; HTTP2008facts, structure/timeframes/headlines retained. These are actual observed source transitions, not simulations or new task code acceptance.
W1 plan refinement accepted: civil-date mappings remain explicit including preceding-weekend mappings into the prospective week and weekend spill into later trading dates. Separate planned_trading_dates are restricted to prospective bounds; coverage means civil plan only, never complete session enumeration.

Precommit validation rub_snapshot_delivery_readiness_v1:172PASS, one Linux-only export test skipped and one Linux-only reader test deselected on Windows. Added Linux HTTP current-export parity/auth/failure tests for exact-head CI. git diff --check PASS. Shared compact route/current export captures one as_of; clean tracked checkout required for executing revision, untracked audits allowed. Coverage enforces mandatory source metadata and existing macro requirements, valid finite calendar coverage independent of future-event count; source headlines/selection freshness and explicit position state remain distinct from model false. Legacy frozen audit export preserved. Existing EMA values/dated relation retained only with quality+causal admission; dependent trend excluded on refusal, hardcoded confidence omitted. No second-task merge/apply/production acceptance claimed here.
Further root real ordinary observation: heavy2026-09-08T21:00:57.251484Z observed21:07:41Z after Moscow midnight; current Si/CR refuse current through_date is not an observed authoritative TradeStats date, previousSep8 dated valid; all live markets stale; structure RETAINED_PREVIOUS excluded; eightdated timeframes,20headlines andsixmacro facts survive. New D1/W1 period semantics use consumption Moscow date, never stale source date as today.

PR501 same-task review correction: news coverage requires all configured sources succeeded. Producer source_count is len(source_results), ok_source_count is acquisition successes and failed_source_count is len(failures); all must be nonnegative exact integers with positive total and consistent sum. Missing/inconsistent counters or failures keep mandatory news requirement PARTIAL/unusable while preserving admitted source headlines and explicit failure IDs. Nine regression cases cover allsuccess/onefailure/inconsistent/missing/bool/negative/IDs; affected suite181PASS, one Linux-only skipped/one deselected. Repeat exact-head CI/review.
Latest real Minfin recheck:2026-09-08T21:20:13.981622Z https://minfin.gov.ru/ru/press-center/ HTTP503;2026-09-08T21:20:28.916016Z https://t.me/s/minfin Errno101 Networkunreachable. TLSverified/no redirects. Evidence /home/trader/moex_bot/deploy_backups/rub_snapshot_target_completion_v1_baseline/minfin_recheck.json. This dated outage evidence remains checkpoint only, not permanent package diagnosis.

## Active bounded source correction — rub_snapshot_eia_date_format_v1
PM_L1 authorizes sole mutation owner package_implementation on isolated branch codex/rub-snapshot-eia-date-format-v1 from exact24122e1b543ee31a5a5803ccf940de1de6459dba. Existing final docs checkpoint draft is paused/uncommitted in its own checkout and will be reconciled after this task. Scope only existing usdrubf_news_live_eia.py, focused tests and this checkpoint. No server/data-egress action by executor.
Product contract BEFORE code: admit existing official WPSR index full month names and explicit English month abbreviations with optional periods; require one unambiguous adjacent Data for week ending / Release Date pair. Do not use Next Release Date, previous issues or ambiguous multiple pairs as latest publication. Preserve exact PDF week cross-check, official schedule/holiday matching, publication-before-acquisition causality and HTTPS/host/type/PDFmagic gates. No new source, date fallback or timestamp inference.
Research evidence: real21:39:31–33Z acquisition error TIMESTAMP_UNPROVABLE EIA index release date fields are missing, after transport/type/PDFmagic gates. Official index has Data for week ending Aug.28,2026 / Release Date Sept.2,2026 / Next Release Date Sept.10,2026 (source spaces after month periods); PDF independently has week ending August28,2026 and existing schedule gives September2 10:30EDT. Failure is proved parser/date-format incompatibility, not EIA source inaccessibility; HTTPS secure-PDF redirect is already permitted. Root owns independent exact-head review/CI, publication/merge and separate apply.
Local validation: focused EIA adapter and wiring suites 22 PASS; git diff --check PASS. Regressions cover observed full/dotted month equivalence, Next Release Date exclusion, missing/unsupported/invalid dates, duplicate/conflicting pairs and malformed extra week anchor, PDF mismatch, holiday week mismatch and future publication exclusion. No production acquisition recovery or merge/apply is claimed before root acceptance.

## Current root acceptance checkpoint — 2026-09-09 Moscow / 2026-09-08 UTC

This section supersedes earlier active-task/next-step labels for
`rub_snapshot_target_completion_v1`. Earlier entries remain dated evidence and
scope-decision history, not competing current-state records.

Root completion is **not accepted**. The implemented consumer path is operational,
but mandatory Minfin evidence remains unavailable, night-time current facts have
freshness/date refusals, and the separate fresh-consumer check has not run because
exact-file transfer approval is pending.

Current task: `rub_snapshot_target_acceptance_checkpoint_v1` (documentation only).
Sole mutation owner: `package_implementation`; isolated branch
`codex/rub-snapshot-target-acceptance-checkpoint-v1`, based on
`259f05efbf41b9a444e977a0a0cc6c81ed93365f`. Only this existing execution-state document
is in scope. Root owns review, publication, merge and any separately authorized
exact-SHA server apply. This checkpoint task has not changed production code/data.

### Closed implementation phases and installed state

- Core projection task: PR #500 merged/applied
  `a2e8eb48adfabee5fe685da8869976d601179bdb`; its installed and actual HTTP acceptance
  is recorded above. The initial trailing-CR deployment rollback incident was
  resolved by the verified retry; no data rollback occurred.
- Delivery/readiness task: PR #501 final reviewed head
  `2474417dc1d81cb161ec8ad765456c65619b7d3c`, tree
  `a32f88ff3fadd9d3577a7efeeda98f905497f48b`. Exact-head CI run `34280487426` PASS;
  independent review PASS; material P2 news-source completeness finding resolved
  on that final head before merge.
- PR #501 merged SHA and verified server Applied State:
  `24122e1b543ee31a5a5803ccf940de1de6459dba`, clean canonical `main`.
  Installed Linux tests: **228 PASS**.

Actual runtime acceptance ran from `2026-09-08T21:30:08Z` through
`2026-09-08T21:31:48Z`: compact and heavy production HTTP endpoints, current export,
fixed-input/time compact equality and replay, exclusive-create overwrite refusal,
unchanged earlier audit archives, and actual CLI export without an OpenAI API key
passed. The delivery report records nine checks; the separate factual audit gate
passed all 17 checks. Evidence:

`/home/trader/moex_bot/deploy_backups/rub_snapshot_target_completion_v1_delivery/20260908T213008.211649Z/report.json`

EIA source correction task: PR #502 final reviewed head
`38ccdfd585a6493de748ecd0b7b2a7c9c83e30d7`, exact tree
`a768d0d2eb0faaa166582d6aa2ea7b75d5fb772a`; CI `34282747940` PASS,
independent focused 22-test review PASS, automated review had no findings.
Merged and applied code is now `259f05efbf41b9a444e977a0a0cc6c81ed93365f`,
verified clean canonical main; installed Linux EIA tests: **22 PASS**.
The official dotted index date format is accepted with unchanged PDF, schedule,
holiday and publication-causality gates.

Repeat actual installed HTTP/CLI/replay/archive acceptance ran from
`2026-09-08T21:55:51.092808Z` to `21:57:06.967566Z`: all nine delivery checks and
17 factual audit checks PASS. Earlier archives remained immutable; CLI required no
OpenAI API key. Evidence:

`/home/trader/moex_bot/deploy_backups/rub_snapshot_target_completion_v1_delivery/20260908T215551.092808Z/report.json`

The supported user path is authenticated `GET /v1/rub/factual-release` or the
single current-export command in `docs/MOEX_BOT_RUB_SNAPSHOT_MANUAL_EXPORT.md`.
Both use the canonical reader/fast overlay and same compact builder with one
captured consumption time. Frozen audit export remains separate and replayable.

### Actual observed data state, not simulated outages

At `2026-09-08T21:33:40Z`, compact production HTTP returned 200 on applied
`24122e1b543ee31a5a5803ccf940de1de6459dba`, using heavy generation `21:29:45Z` and
fast generation `21:33:17Z`. Six admitted macro facts, eight dated timeframe blocks
and 20 original news headlines remained available. News coverage was 13 configured
sources, 12 successful and one EIA failure: the package correctly reported PARTIAL
while retaining the admitted headlines and failure detail.

Two further ordinary delivery-code heavy/fast cycles on `24122e1` returned
HTTP 200 PARTIAL: heavy `2026-09-08T21:39:43.003833Z`, observed `21:41:14Z`,
fast `21:41:00Z`; then heavy `21:49:46.313724Z`, observed `21:52:17.898011Z`,
fast `21:51:49Z`. The first had 12/13 successful configured news sources;
the second had 11/13, with `fed_press_all_rss` and
`eia_weekly_petroleum_status_report` failed. These were dated acquisition gaps,
not proof of permanent provider outages. EIA's demonstrated cause was unsupported
index month spelling, not the permitted secure-PDF redirect.

After EIA apply, actual HTTP at `2026-09-08T21:57:38.811330Z` used manual heavy
`21:55:26.561728Z` and fast `21:56:59.128804Z`: all **13/13 configured news
sources succeeded, zero failed**. Fed's observed transient gap recovered and EIA
acquisition recovered after the parser correction. News acquisition is therefore
not an open permanent blocker. A post-EIA ordinary heavy cycle is not yet claimed
in this checkpoint; the acceptance above uses the labelled manual refresh.

Si's independently admitted previous dated observation survived current-pair
refusal for the new source date; it was not labelled current or completed-session
proof and did not grant comparisons. CR previous history remained absent under its
current-pair-only scope. Stale or refused facts were not upgraded to neutral/current.

D1 reports the September 9 Moscow civil day through the actual consumption time.
W1 reports September 7 through consumption time, with a distinct prospective
September 14–20 civil week. September 19/20 civil-date mappings to September 21
trading date remain explicit; the separate in-week trading-date list stays within
prospective bounds. Neither the calendar nor bar aggregates prove actual session
completion. These observations are separate from labelled refusal/unit simulations.

### Remaining required blockers and user-file handoff

1. **Required Minfin announced-plan evidence remains an external blocker.**
   Actual rechecks at `2026-09-08T21:20:13.981622Z` returned HTTP 503 from
   `https://minfin.gov.ru/ru/press-center/`; at `21:20:28.916016Z`,
   `https://t.me/s/minfin` failed with `Errno101 Network unreachable`.
   TLS verification and redirect restrictions were preserved. Evidence is
   `/home/trader/moex_bot/deploy_backups/rub_snapshot_target_completion_v1_baseline/minfin_recheck.json`.
   No announcement was fabricated, no plan was presented as execution, and this
   mandatory source was not demoted to optional enrichment.
2. **Current night-time coverage remains incomplete.** Actual current markets,
   structure, H1 and current FUTOI carry freshness/date refusals. Independently
   admitted dated facts remain useful, but these refusals do not become current
   facts or a full mandatory-coverage PASS. Reassess on a future observed live cycle.
3. **Local exact-file transfer awaits explicit approval.** The preferred compact
   server file is now:
   `/home/trader/moex_bot/exports/rub_snapshots/2026-09-08T21-56-59.789014Z_8871672c4983_rub_factual.json`,
   88,006 bytes, SHA-256
   `8871672c4983a9e160acb0a4bd923319b86891d60c6bc2c4fb31ce2011c665a0`.
   Its position state is `NO_EXPLICIT_USER_INPUT`; the scoped sensitive-key scan
   found none. The earlier 21:31:38Z file (88,988 bytes, SHA-256
   `5b8b43da8c3d2ba7408aab37fa52fb9d88001a75b249729d7349b4d40744828f`)
   remains immutable but is superseded for the proposed handoff.
   Automatic approval review blocked production-data egress and destination
   authorization despite the scoped scan. Root's explicit approval question is
   unanswered. No executor downloaded/copied either production package or bypassed
   the rejection. The preferred artifact remains on the canonical server.
4. **Fresh independent consumer acceptance is pending authorized transfer.**
   No successful fresh-chat/file-only consumer verdict is claimed. The generated
   compact file and production acceptance do not alone prove this final user step.

Broader global calendars, historical model vintages and model training/evaluation
remain explicitly deferred scope; they do not excuse the required blockers above.
No training, trading signals/orders, broker execution or Stage5 promotion occurred.

Next owner/action: root reconciles the pending exact-file approval. If approved,
transfer only the authorized compact artifact to the approved local outputs destination,
verify its digest, run the independent
file-only consumer check, and record its actual result before assessing root
completion. Observe a future live freshness/refusal cycle and recover Minfin only through
approved access/source routes followed by a latest capture, retaining its mandatory status. Do not promise unattended continuation
or mark the root COMPLETED while the mandatory contract remains unsatisfied.

## Active correction batch snapshot_morning_time_news_v1 — task 1 contract

Root task remains rub_snapshot_target_completion_v1. Task rub_snapshot_dated_context_v1; sole mutation owner morning_implementation; isolated branch codex/rub-snapshot-dated-context-v1 at verified GitHub/server b3efdc13c2298f89303d20a20057df40a7a5c7fa. Root owns publication, exact-head review/CI, merge and separate apply. No competing coordinator/store.

PM_L1 approval BEFORE code: current live retains all source/quality/freshness gates, including the existing 60-second market TTL and unchanged source-specific TTLs elsewhere. Separate preparation-only last accepted dated market/OI, individually comparable basis, structure and H1 have an explicit maximum 96-hour source age; causal availability/receipt and acceptance/reference times are distinct. This finite bound covers ordinary overnight and weekend preparation, not arbitrary old observations or session-completion proof. Existing admitted D1/W1 semantics are not given a new global 96-hour restriction. CR remains current-pair-only.

Persist bounded per-factor references to exact accepted market+basis generation payloads inside existing canonical current envelopes; retain only referenced frames. Revalidate digest, original gates at actual acceptance, identity and causality before use. A failed publication stays visible; dated values never enter current facts or clear current coverage gaps. No legacy witness is minted from mutable last_success or renamed stale flags. Missing/corrupt evidence refuses dated use. Historical basis retains its own bindings, SECIDs, expiry and all comparable legs in one original generation.

Metadata is independently assessed despite price-only freshness refusal, with exact SECID/source timestamp/receipt/value matching and explicit applicability date. Missing instruments do not inherit constant leg units; rollover cannot transfer another contract's metadata.

H1 is currently never requested by step9 _stage7_specs, despite projection support. Scope explicitly includes a snapshot-only opt-in to the existing canonical H1 quality/causal reader and focused tests, without widening unrelated model consumers. No new indicator engine, training/evaluation, trading, Stage5 or position scope. Task 2 clock and task 3 news changes remain separate subsequent PRs.

Evidence limits: the referenced morning compact exists but no matching frozen input is established. Forward capture and labelled simulations cannot be called exact morning raw replay. Production transfer approval remains unresolved and no payload is copied by this executor.

H1 method supersession, explicitly approved by PM_L1 before its code edit: no accepted H1 pointer exists. The native5m accepted pointer covers only through2026-08-17; continuous5m only through2026-06-08. Neither admits current September H1. Instead, the existing live structure producer's validated actual closed5m observations supply exactly12 distinct contiguous bars ending :05 through the next :00, all on one observed Moscow date. The existing OHLCV arithmetic helper is reused after this strict clock-hour gate. Retain the bounded12bar witness, requested USDRUBF/source contract, actual post-loader receipt upper bound and original acceptance; rederive and compare at read. This is a complete observed clock hour, never accepted HTF dataset, scheduled/session completion or model permission. Missing/gapped/future/invalid rows refuse H1. No step9 reader/global scope changes were needed; that earlier pointer proposal is superseded.

Trust boundary: canonical producer-written snapshot envelopes are trusted transport, with hash integrity and revalidation of original numeric/identity/quality/causal gates; hashes are not signatures proving a maliciously rewritten entire source. Frames retain first accepted observation when only receipt/check/age changes, while latest acquisition/failure remains independently visible. Full market frames preserve per-factor original bindings and basis derivation; unavailable spot does not erase independently admissible futures. Unreferenced frames alone are pruned. Existing D1/W1 are retained in native components without duplicated slow witnesses.

Actual before-correction baseline: root observed production HTTP on b3efdc13c2298f89303d20a20057df40a7a5c7fa at2026-09-10T18:44:22.361556Z; six futures current with metadata, stale spot missing units, structure available, eightD1/W1 blocks, H1 absent. Frozen canonical baseline remains on server in /home/trader/moex_bot/deploy_backups/snapshot_morning_time_news_v1/baseline/ca4a49f9cc577aa1f4e7946209776a51888ca03fb8bd3298962ed0ed2076ac89/. Compact before size148295bytes. This is September10 live evidence, not exact original September9 morning replay. Original morning raw input remains unavailable. Task2 saved assembly-age defect remains explicitly pending its separate correction.

Precommit task1 validation:247PASS, one platform symlink test deselected and sixsubtestsPASS on Windows with a clearly labelled fcntl import stub; no Linux lock verification claimed locally. Includes actual producer-function receipt/completion ordering, finite96h boundary, firstacceptance retention after newreceipt, partialspot, same-row/integerOI, mutated/rehashed frame/H1/interaction evidence, completeclockhour/refusals, forward/reverse projection and existing HTTP/export/producer regressions. git diff --check PASS. Independent reviewer reproduced35newtestsPASS and closed interim sourceidentity/causality findings; final exact-head review/CI still required. No merge/apply/runtime acceptance claimed by executor.

Same-task exact-head review correction: independent reviewer reproduced a P1 on db19e34105d0d625fcd3f74885300e7e0f21d964: an originally UNAVAILABLE market component could retain permissive row flags and mint dated witnesses. Publication had not occurred. Corrected original component AND data READY/PARTIAL, quality PASS/PARTIAL with factual_context_usable, per-future admission map plus canonical same-row/integerOI gate, and exact FORTS/CETS source IDs. PARTIAL remains admissible per independent valid row; denied basis alone does not remove market facts. Capture and recomputed-hash regressions cover every denial, partial preservation and unknown source even with matching derived basis. Affected suite256PASS, one platformsymlink test deselected, sixsubtestsPASS;44focused witness/hour testsPASS. Finalhead independent review/CI must repeat; root publication/apply not performed by executor.

Same-task metadata P2 correction before publication: matching arbitrary source strings were insufficient for contract_metadata admission. Require the established exact FORTS or CETS source ID for the corresponding logical market; independent projection oracle enforces the same source boundary. Matched unapproved and wrong-market source regressions now refuse metadata; genuine approved stale-price metadata still survives. Existing synthetic fixture uses the actual approved source ID. Affected suite259PASS, one platformsymlink deselected, sixsubtestsPASS. Exact-head review/CI repeats after this correction.

PR504 automated review P2 correction: unsupported or missing original market schema could pass the matching failed-basis-shell comparison. Explicitly require synchronized_live_market_oi_context.v1 before capture or witness revalidation. New missing/unknownschema regressions reconstruct that matching failed basis shell and rehash the witness, proving both capture and read refuse; existing valid PARTIAL regression remains passing. Affected suite261PASS, one platformsymlink deselected, sixsubtestsPASS. Review thread PRRT_kwDOP-F0yM6hNzXN/comment3982716503 awaits root evidence verification and resolution after new exact-head CI/review; no merge/apply by executor.

Additional bounded quote-gate correction before new PR504 publication: dated price/OI admission no longer trusts quote_usable alone. Recompute existing canonical quote semantics from original bid/offer source values, require original quote map, available same-row coherence and exact bid/ask/spread agreement. Invalid quotes are omitted with explicit quote_refusal while valid price/OI survives; matched historical basis leg presentation also omits those refused book fields. Six focused cases include genuine quote, crossedbook, inconsistent spread, denied map, sourcevalue mismatch and incoherentbook. Affected suite267PASS, one platformsymlink deselected, sixsubtestsPASS. No new quote scope or liveTTL changes.

## Active task 2 — rub_snapshot_consumption_clock_v1

Task1 rub_snapshot_dated_context_v1 accepted by root: PR504 merged/applied ba709fb3ae32439abf49ce8cfb69d76b43603031, exact tree735074a506b7dd11406a2a110bfb2af3c2c86ed4. Linux268testsPASS plus6subtests; actual production HTTP, current CLI,17audit checks and immutable September10baseline replay PASS,2026-09-12T01:48:07–01:49:25Z. Runtime evidence remains /home/trader/moex_bot/deploy_backups/snapshot_morning_time_news_v1/runtime/20260912T014807.882159Z/report.json. Compact93973bytes,hashbc50b5f839e9224505ec2d86648ec05560102f1c036c785fcd392573490f0d78, no position. All7nighttime current markets stale while metadata retained; new dated witnesses/H1 absent because first installation occurred outside fresh-source window. No legacy acceptance bootstrap. Original morning fullraw replay remains unavailable. Initial deploy fetch failed diskfull before mutation; root removed only257.4MB pipcache then applied, leaving about230MB free; raw/audit evidence preserved.

PM_L1 contract approved BEFORE code: sole mutation owner morning_implementation; isolated branch codex/rub-snapshot-consumption-clock-v1 from exactba709fb, existing root rub_snapshot_target_completion_v1/correctionbatch snapshot_morning_time_news_v1. One fractional-precision consumption as_of governs all declared age_seconds_at_as_of and named source ages in canonical read views, heavy/compact APIs,current and frozen export. Source event/update/availability/receipt ages stay distinct with explicit references. Preserve all source timestamps,OHLC/indicatorvalues,current sourcegates and exact threshold precision. Acceptedframes/hashes/witnesses remain immutable: normalize only projected datedvalues. H1 eventhourend differs from actualreceiptupperbound. Assemblyage, if retained, needs distinctname and actualreference; never invent a reference. Preserve refusal behavior, invalid/futuretimestamp handling and idempotence; explicit projection boundaries rather than unrestricted recursive source rewrites.

Actual task1 nighttime age defect still present: D1 stored168393 vs consumption168552.412718; W1stored427593 vs427752.412718. Task2 tests include originalcompact-derived arithmetic833.454649/173633.454649 at2026-09-09T03:13:53.454649Z (not exactrawreplay), sourcepreservation, fractionalboundaries, H1/datedages, immutablewitnesses and API/frozenparity. Root owns publication/exactheadreview/CI/merge/apply and actual serverbaseline replay. No task3news changes in this branch.

Task2 implementation validation: the canonical read boundary recomputes explicit stage3/stage7 block ages, market observation/update/receipt ages, basis leg ages and structure ages against one fractional consumption clock. Current and copied dated H1 projections use hour-end event age and separate receipt upper-bound age; immutable accepted frames remain unchanged. Repeated reads and unchanged-source reingestion preserve original acceptance. The affected Windows suite passed 294 tests plus 6 subtests; 2 platform symlink tests were deselected and an fcntl import stub was used, so Linux locking/symlink validation is reserved for root CI/runtime. Coverage includes handler HTTP/current/frozen parity, one clock capture, original compact-derived arithmetic, invalid/naive/future timestamp refusal, microsecond threshold boundaries, source-value preservation and unchanged witness hashes. Independent reviewer additionally passed 60 focused clock/dated/H1 tests with no blocking finding before final freeze. Task2 is not yet merged, applied or runtime-accepted; task3 remains pending.

PR505 review correction before merge/apply: comment3994739365 identified that the new clock traversal could raise on malformed stored basis pairs previously skipped by read freshness. Explicit container checks now skip malformed pairs, pair objects, legs and metrics, and equivalent newly introduced stage9/market/structure traversal assumptions. No exception swallowing, source repair or admission is added. Four canonical-read/compact regressions preserve other timeframe facts and input immutability; ten focused boundary cases exercise malformed containers. Affected suite308PASS plus6subtests,2Windows symlink deselections, with the same explicitly labelled fcntl import stub. Exact-head review/CI must repeat; root has not merged or applied task2.

Task2 consumer-contract synchronization: daily and weekly snapshot_age_seconds wording now explicitly preserves fractional seconds for freshness comparisons. This documentation-only correction changes no analysis workflow or production code; the308-test result above applies to the unchanged implementation. Root exact-head CI/review remains required before merge/apply.

## Follow-up rub_snapshot_view_clock_v1

PR505 reviewed at2a37a84/tree077b66bf7727863fd6db0413905f12beca45653d, CI34667468094, merged/applied361cf4c75d83a1d9848e577cc6b98ec1415c595d. Installed Linux310tests plus6subtests PASS. Actual runtime acceptance at /home/trader/moex_bot/deploy_backups/snapshot_morning_time_news_v1/runtime/20260912T163100.753361Z was PARTIAL, not PASS: real compact/current and synthetic current/dated/hour checks passed, but heavy HTTP analysis_views.carry and cny_accepted_context retained saved assembly ages after JSON serialization separated their objects from stage9 components. Cleanup of23clean review worktrees released569MiB; canonical applied checkout stayed clean, original raw/audit and untracked Minfin evidence preserved. About1.2GiB free supersedes the earlier230MB status.

PM_L1 scope approved BEFORE code: same root rub_snapshot_target_completion_v1 and correction batch snapshot_morning_time_news_v1; sole mutation owner morning_implementation, isolated branch codex/rub-snapshot-view-clock-v1 from exact361cf4c/tree077b66. Normalize only explicit duplicated analysis view block ages against the same consumption clock, preserving own source timestamps, selected observations, values, eligibility/status and immutable accepted archives. Audit analogous levels/interactions views; no unrestricted recursive rewriting or source admission. Regression must serialize before canonical read/heavy HTTP, preserve input and handle malformed views. Root owns publication/review/CI/merge/apply. No task3 news changes.

Follow-up validation: five production lines explicitly normalize guarded carry/cny block copies with their own selected causal timestamps. Serialized canonical-current/heavy HTTP regression checks833.454649 for03:00 carry and233.454649 for independent03:10 CNY at03:13:53.454649, source observations/status preservation, idempotence and unchanged saved input bytes; compact/export/frozen parity remains passing. Six malformed analysis-view cases preserve other facts without exceptions. Audit confirmed ordinary LevelZone/InteractionSnapshot views have no declared age fields and remain untouched; enriched structure ages already use the clock. Affected Windows suite314PASS plus6subtests,2platform symlink deselections, with explicitly labelled fcntl import stub; no local Linux lock validation claimed. Initial regression setup captured pristine input too early; corrected fixture only, then full affected rerun passed. Root exact-head review/CI and real runtime acceptance remain pending; no full production PASS claimed.

## Active task3 rub_snapshot_news_identity_v1

Task2 including PR506 ACCEPTED by root: reviewed e906f896e25b74d52cddf014abb05f2cf53281c9, CI34706000951 PASS, merged/applied a61a22deec1ce251654b36af260df94742a2d12e/tree08b3f3dd2c4a9289ba5ae1105afe5a4d502a2284. Independent81tests and3remote blobs PASS; installed Linux316tests plus6subtests PASS without deselections. Actual HTTP/current/frozen acceptance PASS2026-09-12T16:58:57.999280–17:00:31.035922Z, report /home/trader/moex_bot/deploy_backups/snapshot_morning_time_news_v1/runtime/20260912T165857.999280Z/report.json. Compact75declared ages, frozen79,heavy419 PASS; D1/W1 actual223214.236595/482414.236595, Sept10 replay56662.361556/315862.361556 exact. Intermediate consumer file102339bytes SHA814bf475f8549823302a3b40ca0fb2dff46fcd07441fb037194574ab0a6e57e3. Coverage remains PARTIAL:7stale current markets with metadata, no new dated/H1 acceptance, currentFUTOI refused, Minfin missing. Original news audit/raw evidence unchanged.

PM_L1 task3 approval BEFORE code: sole mutation owner morning_implementation, branch codex/rub-snapshot-news-identity-v1 from exactApplieda61a22de, same root/correctionbatch. Publication identity is normalized exact reference plus actual UTC publication instant plus content/version hash; headline similarity and hash alone never identity. Reserve representative primary provenance beforecap, no mixed events. New versioned selection: published<=24h fresh first, older<=7days background atmost4, total<=20, source representation only inside eligible pools with explicit background reason; causal available/receipt separate. Re-evaluate at consumption from retained available pool only, never historicalfetch/fabricated candidates. Feed acquisition completeness distinct from event usability, mandatory Minfin remains mandatory, no overallREADY claim. Compact carries short primary provenance/auditref; legacy mixedprovenance limitation explicit. Exact policyv1 postcluster replay remains unchanged; original precluster archive unavailable, corrected legacy comparison is not original regroup. Audit design awaits root confirmation before implementation; proposal reuses raw/news_selection with deduplicated immutable record/candidate objects and capture receipt references, never raw bodies in snapshot. Classifier/model/UNKNOWN/NOT_ANALYZED authority unchanged; no trades, position, training, evaluation or Stage5 expansion.

PM_L1 approved v2 audit layout before implementation: reuse raw/news_selection immutable SHA-addressed publication and candidate objects plus capture manifests. Publication objects preserve exact source/tier/reference/publication/headline/body; BOTH availability and ingestion are per-capture overlays. Candidate objects preserve classifier output and provenance order with explicit top/provenance availability/receipt overlays, so exact output replay needs no model or network. Preserve record order, versioned schemas, digest/type/causal/identity checks and exclusive collision/symlink/root checks. Summary is an explicit bounded whitelist, never records/body/candidate-reference arrays. Old v1 files unchanged. Plain JSON approved; measure first and identical-reingestion incremental fixture bytes. Researcher measured original v1 audit326998bytes (candidates325933,summary1051), about94MB/day at5min; actual raw body bytes remain unknown.

Task3 pre-publication validation: exact publication grouping, primary-first provenance, versioned24h/7day selection and consumption re-selection from the retained metadata pool are implemented; compact exposes primary provenance/audit_ref with UNKNOWN/NOT_ANALYZED authority and separate feed gaps. Exact v1 selection replay remains available. V2 immutable audit reconstructs precluster parser records and postcluster candidates without network/classifier calls; future raw inputs are preserved but cannot become admitted candidates. Digest, overlay identity/order, selection replay equality, primary reservation, finite JSON and same-publication provenance checks fail closed.

Affected Windows suite479PASS plus6subtests,1platform symlink deselection,33.01s, using the explicitly labelled fcntl import stub. Final reserved-primary audit guard and regression then passed24focused audit/legacy-selection tests in4.68s. Local serialized HTTP/current/frozen/matrix parity, reverse omission/injection/alteration checks, partial-feed failures, publication window boundaries, receipt-only reingestion and immutable source checks pass. A fixture initially patched the alternate import namespace; fixed to runner.live with an explicit network-forbidden guard. Its interrupted public-feed diagnostic is not production acceptance evidence. Measured synthetic31records/30candidates: first capture310200bytes; identical bodies with new receipts add24489bytes;61immutable objects unchanged. These are fixture measurements, not observed production footprint. Original precluster regroup remains unavailable; original v1 postcluster audit remains unchanged. Task3 exact-head review, CI, actual production footprint/HTTP/current acceptance and root merge/apply are still PENDING; no full readiness claim or Minfin demotion.

PR507 initial exact tree686a06910a461deed98dc2076bf201a3bcffdbe2 was published as a draft at407806753e84e23cee3cc087ebaa111748d114a5, identical to local1cbbfdb2bb0d2a7e31c28a40a1209e42fb418cd6. The single large publication request exceeded the automatic200000-byte review limit; the user explicitly approved19 separate reviewed GitHub operations, and every blob plus the final tree matched. No merge/apply occurred. After writer freeze, root became the sole mutation owner for independent-review corrections. Reviewer passed44focused tests but reproduced two blocking compact-oracle mismatches: conflicting causal event IDs and malformed publication references. The independent completeness oracle now refuses conflicting IDs before age/quality windows and independently handles legacy unproven reference fallback versus proven-v2 refusal. Eight compact regressions cover conflicts, old/non-OK conflicting copies, equivalent default fields, malformed URL and whitespace references. Targeted108PASS; affected news/Snapshot/HTTP/current/frozen/dated/hour/clock set434PASS plus6subtests,2Windows symlink deselections,14.00s with the explicit fcntl import stub. Initial local runner dependency/temp-directory setup errors are not product failures or Linux validation. Exact corrected-head review, full CI and separate real server acceptance remain required.

PR507 initial full Linux CI run34709230381/job103594798850 reported4939PASS plus37subtests and7FAIL: six existing macro-inventory describe regressions supplied an invalid/missing read clock, and an unrelated-component preservation test supplied eventless governed-blocked news. Root corrected the news read view to return explicit invalid_consumption_clock refusal without substituting current or generation time, and to preserve eventless source components. Five regressions cover populated retained news with invalid clocks and inert component preservation. Targeted257PASS; expanded affected suite588PASS plus6subtests,2Windows symlink deselections,16.42s. This supersedes the earlier local corrected head for final review/publication. Full exact-head Linux CI, independent remote attestation and server acceptance remain pending; production is still a61a22de from PR506.
