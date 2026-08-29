# MOEX Bot — S7.3 Analysis Chat Consumer Contract v1

## Purpose

This contract defines the shared input and safety rules for separate Weekly and Daily analysis chats consuming the canonical persisted RUB Intelligence snapshot.

Canonical snapshot path:

`/home/trader/moex_bot/data/state/rub_intelligence/chat_analysis_snapshot/current.json`

The server publishes data/context only. Analysis chats interpret that context. They do not fetch independent market/news/macro data, write server state, place broker orders, or send Telegram messages.

## Canonical factual source

For current market/news/macro facts, the snapshot supplied to the chat is the only allowed factual source.

The snapshot may be supplied as raw `current.json` or through the canonical reader. In either form, the chat MUST recompute freshness at the actual analysis time from:
- current UTC time;
- `identity.generated_at_utc`;
- `refresh_policy.snapshot_stale_after_seconds`.

Any supplied `read_freshness` is diagnostic metadata from an earlier read only. It cannot make a cached payload remain `FRESH` after time has passed.

If the current time, generation timestamp, or stale threshold cannot be verified, freshness is `UNKNOWN`; `snapshot_age_seconds` is `null`; freshness must not be assumed.

## Component status semantics

- `READY`: factual data may be used normally.
- `RETAINED_PREVIOUS`: factual retained state, but stale relative to the failed refresh attempt; disclose degradation.
- `UNAVAILABLE`: unknown; do not infer or substitute a value.
- `GOVERNED_BLOCKED`: explicit source/governance blocker; never interpret as neutral.

Missing or blocked inputs must not be filled from model memory or web retrieval.

## Authority boundaries

- EMA(3/19) S7.2 verdict is `REJECT_AS_STANDALONE_DIRECTIONAL_SIGNAL`.
- EMA is descriptive context only.
- FUTOI remains blocked/no action authority unless a later accepted contract explicitly changes that status.
- News events may provide catalysts/context but no standalone BUY/SELL authority.
- Missing/blocked oil is unknown, not neutral.
- No calibrated scenario probabilities before S7.5.
- No broker execution authority.

## Shared analytical order

Higher-timeframe context must precede lower-timeframe action logic:

`weekly regime -> daily structure -> levels -> carry/rates -> CNY/oil -> news/macro -> scenario -> BUY/SELL/OUT + invalidation`

The Weekly Chat owns the higher-timeframe regime/context layer and produces a bounded handoff to the Daily Chat.

The Daily Chat owns the current-day synthesis and may produce an analytical `BUY`, `SELL`, or `OUT` recommendation plus explicit invalidation.

## Evidence rule

Every material conclusion must identify the exact snapshot paths or Weekly-context fields actually used.

Do not cite unavailable/blocked data as factual evidence.

## Freshness rule

At analysis start, compute:

`snapshot_age_seconds = max(0, current_utc_time - identity.generated_at_utc)`

A snapshot is `FRESH` only when that current-time age is less than or equal to `refresh_policy.snapshot_stale_after_seconds`. Otherwise it is `STALE`.

If the calculation cannot be performed truthfully, status is `UNKNOWN` and age is JSON `null`.

`read_freshness.status` never overrides this current-time calculation.

## Weekly-to-Daily handoff rule

Standalone `weekly_context_for_daily_chat` must contain:
- `weekly_context_generated_at_utc`;
- `source_snapshot_generated_at_utc`;
- `source_snapshot_freshness`;
- `valid_from_moscow_date`;
- `valid_through_moscow_date`;
- regime/directional context;
- key levels/catalysts/risk flags;
- qualitative confidence.

The Daily Chat must reject or degrade a weekly context whose provenance or Moscow-date validity cannot be verified.

## Execution boundary

All outputs are analysis for the user only.

`execution_authority = false`

No server-side or chat-side broker action is authorized by this contract.
