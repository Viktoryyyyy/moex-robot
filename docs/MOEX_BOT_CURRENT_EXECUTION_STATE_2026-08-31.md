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
