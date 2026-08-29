# MOEX Bot — S7.3 Analysis Chat Consumer Contract v1

## Purpose

This contract governs separate analysis chats that consume the persisted RUB Intelligence server snapshot and produce interpretation. The server is the factual data plane; chats are the interpretation plane.

Canonical server snapshot:

`/home/trader/moex_bot/data/state/rub_intelligence/chat_analysis_snapshot/current.json`

Schema:

`rub_chat_analysis_snapshot.v1`

The server refreshes this snapshot on the accepted 10-minute cadence. Chats MUST NOT independently fetch or refresh market, macro, CNY, oil, or news facts.

## Authority boundary

Allowed for chats:
- interpret snapshot facts;
- classify regime/structure only when supported by snapshot evidence;
- build scenarios;
- issue bounded BUY / SELL / OUT analysis;
- define invalidation conditions;
- explain uncertainty and missing data.

Forbidden for chats:
- independent web/news/market-data acquisition for current facts;
- inventing missing prices, events, rates, levels, timestamps, or source status;
- treating `UNAVAILABLE`, `RETAINED_PREVIOUS`, or `GOVERNED_BLOCKED` as neutral;
- giving FUTOI directional authority while its quality is blocked;
- giving EMA(3/19) standalone directional authority;
- broker execution or order placement;
- silently using memory as a substitute for current snapshot facts.

## Required input validation

Each analysis MUST inspect:
- `identity.project == MOEX_Bot`;
- `identity.generated_at_utc`;
- `read_freshness.status` when the snapshot is supplied through the canonical reader;
- `readiness.status`;
- `readiness.component_statuses`;
- relevant component `data_as_of` values;
- relevant component `refresh_error` values when status is not `READY`.

If `read_freshness.status == STALE`, the analysis must explicitly say so and may not claim that the snapshot represents current conditions.

If `read_freshness` is absent, freshness must be reported as `UNKNOWN`; do not infer `FRESH` from file existence.

## Component-status semantics

### READY
The component may be used as factual evidence subject to its own `data_as_of` timestamp and authority flags.

### RETAINED_PREVIOUS
The component may be used only as stale context. The analysis MUST:
- name it as retained previous data;
- cite its `data_as_of`;
- reduce confidence;
- avoid claims that require current confirmation from that component.

### UNAVAILABLE
No factual conclusion may be drawn from that component. Missing data is not neutral evidence.

### GOVERNED_BLOCKED
The source is intentionally not accepted for factual/action authority. It must be reported as unavailable by governance, not interpreted as zero/neutral.

## Existing research authority constraints

### EMA(3/19)
S7.2 verdict:

`REJECT_AS_STANDALONE_DIRECTIONAL_SIGNAL`

EMA may be used only as descriptive trend/context evidence. It cannot independently justify BUY, SELL, confidence uplift, or invalidation.

### FUTOI
Current quality/authority:
- direction may be `MIXED`;
- quality is blocked;
- `action_authority == false`.

Do not infer smart-money direction from FUTOI while blocked.

### Oil
Current snapshot source is `GOVERNED_BLOCKED` until an oil source is separately accepted. Missing oil must be explicitly disclosed and cannot be converted to a neutral oil view.

### News
News is factual/contextual and does not itself carry directional action authority. Interpretation must cite specific events or summary fields from `components.official_news`.

## Evidence references

Every material conclusion MUST include one or more evidence references expressed as snapshot paths, for example:
- `components.live_market_structure.data.market_regime`
- `components.live_market_structure.data.active_levels`
- `components.live_market_structure.data.level_interactions`
- `components.live_market_structure.data.ema_3_19`
- `analysis_views.carry`
- `components.cbr_macro.data`
- `components.cnyrub_spot_live.data`
- `components.cnyrubf_live.data`
- `components.official_news.data.events`
- `components.oil`
- `components.stage9_daily.data.server_core.blocks`
- `components.stage9_weekly.data.server_core.blocks`

An evidence reference is not a substitute for explaining what the evidence means.

## Confidence discipline

Use only these confidence bands:
- `HIGH`
- `MEDIUM`
- `LOW`
- `INSUFFICIENT`

Rules:
- confidence may be `HIGH` only when all components essential to the stated conclusion are `READY` and snapshot freshness is `FRESH`;
- any essential `RETAINED_PREVIOUS` component prevents `HIGH`;
- any essential `UNAVAILABLE` component requires either a narrower conclusion that does not depend on it or `INSUFFICIENT`;
- `GOVERNED_BLOCKED` must be disclosed and cannot raise confidence;
- missing/stale weekly context prevents `HIGH` confidence in a Daily Chat conclusion that depends on weekly alignment;
- EMA and blocked FUTOI cannot raise the confidence band by themselves.

No numeric probability is required unless the chat can justify it from explicitly stated scenario assumptions. Do not fabricate statistical precision.

## Separation of facts and interpretation

Each response must keep these conceptually separate:

1. `FACTS_FROM_SNAPSHOT`
2. `INTERPRETATION`
3. `SCENARIOS`
4. `ACTION_VIEW` when applicable
5. `INVALIDATION`
6. `DATA_QUALITY_AND_GAPS`
7. `EVIDENCE_REFS`

## Weekly-to-Daily handoff

Weekly Chat produces a compact `WEEKLY_CONTEXT` block containing:
- regime;
- dominant drivers;
- key weekly levels/zones;
- carry/rates context;
- CNY context;
- oil status/context;
- macro/news risks;
- base/alternative/risk scenarios;
- confidence;
- data gaps;
- snapshot evidence refs.

Daily Chat may consume this block as interpretation context. It must still read the current server snapshot for current facts.

If weekly context is missing or stale, Daily Chat may continue with explicit degraded mode, but:
- `weekly_alignment` must be `MISSING` or `STALE`;
- confidence cannot be `HIGH` for conclusions dependent on higher-timeframe alignment;
- the Daily Chat must not reconstruct a fake weekly view from memory.

## No execution

These chats are analytical only. They do not submit orders, modify broker state, or trigger Telegram delivery.
