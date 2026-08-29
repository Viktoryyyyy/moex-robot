# MOEX Bot — S7.3 Analysis Chat Consumer Contract v1

## Purpose

This contract defines the shared input and safety rules for separate Weekly and Daily analysis chats consuming the canonical persisted RUB Intelligence snapshot.

Canonical snapshot path:

`/home/trader/moex_bot/data/state/rub_intelligence/chat_analysis_snapshot/current.json`

The server publishes data/context only. Analysis chats interpret that context. They do not fetch independent market/news/macro data, write server state, place broker orders, or send Telegram messages.

## Canonical factual source

For current market/news/macro facts, the snapshot supplied to the chat is the only allowed factual source.

Preferred form:
- canonical reader-enriched snapshot including `read_freshness`.

Raw `current.json` is allowed only when the chat can independently establish current UTC wall-clock time and recompute freshness from:
- `identity.generated_at_utc`;
- `refresh_policy.snapshot_stale_after_seconds`.

If freshness cannot be verified, it must not be assumed.

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

A snapshot is `FRESH` only when freshness is positively established either by canonical reader metadata or by a wall-clock age calculation against the persisted stale threshold.

Absence of `read_freshness` is not evidence of freshness.

## Weekly-to-Daily handoff rule

Standalone `weekly_context_for_daily_chat` must contain:
- context generation timestamp;
- source snapshot generation timestamp;
- source snapshot freshness;
- explicit Moscow-date validity interval;
- regime/directional context;
- key levels/catalysts/risk flags;
- qualitative confidence.

The Daily Chat must reject or degrade a weekly context whose provenance or validity cannot be verified.

## Execution boundary

All outputs are analysis for the user only.

`execution_authority = false`

No server-side or chat-side broker action is authorized by this contract.
