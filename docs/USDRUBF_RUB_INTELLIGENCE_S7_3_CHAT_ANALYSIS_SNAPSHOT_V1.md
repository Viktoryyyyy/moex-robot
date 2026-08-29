# USDRUBF RUB Intelligence — S7.3 Chat Analysis Snapshot v1

## Status

Research/runtime data contract for separate analysis chats. This contract does not create a trading decision policy.

## Objective

Persist one stable server-side factual snapshot that separate analysis chats can read without independently refetching market, news, or macro sources.

The intended downstream reasoning sequence is:

`weekly regime -> daily structure -> levels -> carry/rates -> CNY/oil -> news/macro -> scenario -> BUY/SELL/OUT + invalidation`

The server snapshot supplies data/context only. The reasoning sequence itself belongs to separate chats and later S7.3/S7.5 policy work.

## Refresh architecture

Two refresh lanes remain intentionally separate:

1. Slow/daily canonical acceptance lane: completed-date D1/W1 and accepted Stage 3/4/5/7 datasets. Heavy history/catch-up work must not run every ten minutes.
2. Fast chat-snapshot lane: `moex-rub-chat-snapshot.timer` invokes a bounded oneshot approximately every 10 minutes.

The fast lane reads Stage 9 accepted daily/weekly bundles and refreshes factual live-context sources that already have deterministic adapters.

## Canonical snapshot location

The repository defines the current snapshot relative to the data root:

`${MOEX_DATA_ROOT}/state/rub_intelligence/chat_analysis_snapshot/current.json`

The path is derived from `MOEX_DATA_ROOT`; application code does not hard-code the server data-root path.

On the canonical server service unit, `MOEX_DATA_ROOT=/home/trader/moex_bot/data`, therefore the applied current-file location is derived deterministically from that service configuration.

## Atomic publication

`current.json` is written through a same-directory temporary file, flushed/fsynced, and atomically replaced with `os.replace`.

A process-scoped non-blocking lock prevents overlapping refresh writers. A reader therefore sees either the previous complete snapshot or the new complete snapshot, never a partially written JSON document.

## Top-level freshness

- expected refresh interval: 600 seconds;
- snapshot stale threshold: 1200 seconds;
- `--read-current` calculates snapshot age from `identity.generated_at_utc` and reports `FRESH` or `STALE`;
- source/component timestamps remain explicit and are not backdated to the snapshot generation time.

## Component failure semantics

Each refresh component is independent:

- `READY`: current refresh succeeded;
- `RETAINED_PREVIOUS`: current refresh failed, but the last successfully persisted component is retained with its original `data_as_of` and `last_success_at` plus the current error;
- `UNAVAILABLE`: current refresh failed and there is no prior component to retain;
- `GOVERNED_BLOCKED`: source is deliberately unavailable under current source governance.

A retained component must never be relabelled as fresh current data.

## Components

### `stage9_daily`

Deterministic Stage 9 daily server-core bundle. It contains accepted Stage 3/4/5/7 observations with provenance and causal timestamps.

### `stage9_weekly`

Deterministic Stage 9 weekly server-core bundle. Existing Stage 9 policy gaps remain explicit; the fast snapshot does not invent missing weekly features.

### `live_market_structure`

Current USDRUBF factual context built from closed 5m observations plus prior-session Level & Structure semantics:

- price;
- deterministic trend context;
- market-regime label from current Level & Structure implementation;
- active levels;
- level interactions;
- EMA(3/19) context;
- governed FUTOI context.

S7.2 verdict is binding: EMA(3/19) has no standalone directional authority. It remains descriptive context only.

If the current Moscow date has no usable market bars (weekend, before session, source gap, or insufficient live bridge input), the publisher retains the previous market component if available rather than manufacturing current bars.

### `cnyrub_spot_live`

Current-day partial CNYRUB_TOM context from the exact official AlgoPack route and timestamp policy. It is context-only and carries no action authority.

### `cnyrubf_live`

Current-day partial CNYRUBF context from the exact official AlgoPack FO route. It is context-only and carries no action authority.

### `cbr_macro`

Current accepted CBR macro composition (key rate and RUONIA under the existing live CBR contract). It supplies facts/context only.

### `official_news`

Bounded current official-news events from the existing deterministic live RSS pipeline. News classification remains neutral/context-only and has no independent BUY/SELL authority.

### `oil`

`GOVERNED_BLOCKED` until an oil source is accepted for this snapshot. Current declared blockers include MOEX Brent futures and CME WTI pre-MOEX. Missing oil data must not be interpreted as neutral oil evidence.

## Convenience views

The snapshot exposes references/views for:

- levels and level interactions;
- accepted Stage 4 carry/basis blocks;
- accepted CNY context;
- live CNY spot/futures component references;
- rates;
- news;
- oil status.

These views do not add any derived trading rule.

## Downstream chat authority

`analysis_workflow` explicitly delegates all reasoning to separate analysis chats:

- weekly regime;
- daily structure;
- levels interpretation;
- carry/rates interpretation;
- CNY/oil interpretation;
- news/macro interpretation;
- scenario;
- BUY/SELL/OUT;
- invalidation.

The server publisher must not generate:

- scenario probabilities;
- BUY/SELL/OUT;
- stop/target/invalidation;
- position size;
- broker orders.

Telegram remains outside this step.

## CLI

Refresh the persisted snapshot:

`python -m moex_research.runners.usdrubf_s7_3_chat_analysis_snapshot --refresh`

Read the current snapshot with reader-side freshness status:

`python -m moex_research.runners.usdrubf_s7_3_chat_analysis_snapshot --read-current`

Both commands require `MOEX_DATA_ROOT` and normal project runtime credentials for live sources.

## systemd

Repository units:

- `ops/systemd/moex-rub-chat-snapshot.service`
- `ops/systemd/moex-rub-chat-snapshot.timer`

The timer uses `OnBootSec=2min` and `OnUnitActiveSec=10min`. Because the service is oneshot, overlapping timer executions are naturally avoided; the application lock additionally protects against a concurrent manual refresh.

## Acceptance gates

The Git-side implementation is acceptable only if:

1. unit/contract tests pass;
2. current-file publication is atomic;
3. symlink/path escape is rejected;
4. component failure never fabricates fresh data;
5. no server-generated scenario or trade action appears;
6. EMA standalone authority remains false;
7. oil remains explicitly blocked until separately accepted;
8. live/runtime broker execution and Telegram are unchanged;
9. a controlled server refresh creates a readable `current.json` and a second read confirms freshness/status.
