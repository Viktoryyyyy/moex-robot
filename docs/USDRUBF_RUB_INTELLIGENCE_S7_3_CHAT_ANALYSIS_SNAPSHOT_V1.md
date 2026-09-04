# USDRUBF RUB Intelligence — S7.3 Chat Analysis Snapshot v1

## Status

Research/runtime data contract for separate analysis chats. This contract does not create a trading decision policy.

## Objective

Persist one stable server-side factual snapshot that separate analysis chats can read without independently refetching market, news, or macro sources.

The server snapshot supplies data/context only. Interpretation and any later trading decision remain outside this publisher.

## Refresh architecture

Two refresh lanes remain intentionally separate:

1. Slow/daily canonical acceptance lane: completed-date D1/W1 and accepted Stage 3/4/5/7 datasets. Heavy history/catch-up work must not run every ten minutes.
2. Fast chat-snapshot lane: `moex-rub-chat-snapshot.timer` invokes a bounded oneshot approximately every 10 minutes.

The fast lane reads accepted daily/weekly bundles and refreshes factual live-context sources through existing deterministic adapters. The canonical systemd service currently invokes `moex_research.runners.usdrubf_s7_3_chat_analysis_snapshot_live_market_oi`.

## Canonical snapshot location

The repository defines the current snapshot relative to the data root:

`${MOEX_DATA_ROOT}/state/rub_intelligence/chat_analysis_snapshot/current.json`

The path is derived from `MOEX_DATA_ROOT`; application code does not hard-code a separate data-root path.

On the canonical server service unit, `MOEX_DATA_ROOT=/home/trader/moex_bot/data`, therefore the applied current-file location is derived deterministically from that service configuration.

## Atomic publication

`current.json` is written through a same-directory temporary file, flushed/fsynced, and atomically replaced with `os.replace`.

A process-scoped non-blocking lock prevents overlapping refresh writers. A reader therefore sees either the previous complete snapshot or the new complete snapshot, never a partially written JSON document.

## Top-level freshness

- expected refresh interval: 600 seconds;
- snapshot stale threshold: 1200 seconds;
- reader-side freshness is calculated from `identity.generated_at_utc`;
- source/component timestamps remain explicit and are not backdated to the snapshot generation time;
- the structural-level block independently carries price-source freshness based on its latest closed 5m observation.

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

Canonical current structural context is factual USDRUBF data only.

Source and timeframe:

- instrument/requested SECID: `USDRUBF`;
- source id: `moex_algopack_fo_tradestats_5m`;
- source contract: `contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml`;
- source timeframe: closed 5m TradeStats observations;
- current price and level-interaction bars use the same accepted source family;
- prior-session discovery probes a bounded set of preceding dates and accepts only a date for which actual USDRUBF observations exist;
- no MOEX Calendar API, weekday rule, or weekend rule determines the prior observed session.

The canonical producer reuses, without changing their semantics:

- `contracts/intelligence/usdrubf_market_state_level_structure_v1.json`;
- `src/moex_research/intelligence/usdrubf_level_structure.py`;
- the existing causal `build_previous_session_zones` implementation.

The producer emits all active previous-completed-session high/low LevelZones and classifies their interaction against current closed 5m bars with the existing state machine. It applies no level ranking and silently drops no active level.

The compatibility fields remain directly under `components.live_market_structure.data`:

- `instrument`;
- `trade_date` and `prior_trade_date`;
- `market_data_as_of`;
- `price`;
- `active_levels`;
- `level_interactions`;
- source/provenance identifiers and closed-bar counts.

The compact analyst block is:

`components.live_market_structure.data.structural_levels`

Its schema id is `usdrubf_structural_levels_snapshot.v1` and it contains:

- `price_context`: factual price, source timestamp/data-as-of, freshness, source id/contract, exact requested SECID and timeframe;
- `active_levels`: existing LevelZone fields plus deterministic interaction structural quality, actual level age and provenance;
- `level_interactions`: exact existing state/direction/event/previous-state/structural-quality fields plus engine fields and provenance;
- `observed_extrema.prior_completed_session`: observed high/low from the completed prior USDRUBF session;
- `observed_extrema.current_observed_session`: high/low of closed current-session observations through `data_as_of`;
- `methodology`: exact contract/engine refs, closed-bar/PIT rules, no-ranking rule and observed-session semantics;
- `unsupported_facts`: explicit non-emission of recent D1 high/low and HH/HL/LH/LL swing labels because no accepted deterministic live convention exists in the current structure path;
- `authority`: all directional/action/standalone-trading and Stage5 readiness/promotion flags remain false.

A historical active level keeps its actual `created_at`, current interaction timestamp and calculated age. A fresh price does not make an old level newly created or newly fresh.

If the current observed date has no usable USDRUBF closed 5m bars, the publisher follows the existing component failure policy rather than manufacturing a current market observation.

### `cnyrub_spot_live`

Current-day partial CNYRUB_TOM context from the accepted official route and timestamp policy. It is context-only and carries no action authority.

### `cnyrubf_live`

Current-day partial CNYRUBF context from the accepted official AlgoPack FO route. It is context-only and carries no action authority.

### `cbr_macro`

Current accepted CBR macro composition supplies facts/context only.

### `official_news`

Bounded current official-news events from the existing deterministic live pipeline remain context-only.

### `oil`

`GOVERNED_BLOCKED` until an oil source is accepted for this snapshot. Missing oil data must not be interpreted as neutral oil evidence.

## Convenience views

The base snapshot retains its existing convenience views for levels/interactions and other factual components. The compact structural block is intentionally kept inside the canonical `live_market_structure` component so no second structure component or competing snapshot is introduced.

## Structural no-interpretation boundary

The structural producer may state only deterministic factual observations and contracted state transitions. It does not forecast whether a level will hold or break, does not classify the market as bullish/bearish, and does not create scenarios, probabilities, targets, stops, position sizes or broker actions.

The following flags are fixed false in the compact structural block and component:

- `directional_authority`;
- `action_authority`;
- `standalone_buy_sell_authority`;
- `stage5_full_mode_ready`;
- `stage5_pointer_promotion_performed`.

## CLI and systemd

Repository units:

- `ops/systemd/moex-rub-chat-snapshot.service`;
- `ops/systemd/moex-rub-chat-snapshot.timer`.

The canonical service invokes the layered current snapshot runner. The timer uses `OnBootSec=2min` and `OnUnitActiveSec=10min`. Because the service is oneshot, overlapping timer executions are naturally avoided; the application lock additionally protects against a concurrent manual refresh.

## Acceptance gates

The Git-side implementation is acceptable only if:

1. deterministic same-input structural output is reproducible;
2. current price and level interaction use only observable closed bars;
3. future rows do not enter price, extrema or interaction state;
4. existing LevelZone metadata and exact interaction state machine are preserved;
5. active level ids remain deterministic and interactions reference the same ids;
6. prior/current session facts are based on observed rows, not calendar inference;
7. no subjective level ranking is applied;
8. no HH/HL/LH/LL labels are fabricated;
9. stale/current timestamps and historical level age stay explicit;
10. source/provenance remains attached to price, levels and interactions;
11. all directional/action/standalone-trading and Stage5 flags remain false;
12. no scenario probability, target, stop, position sizing or broker execution is generated;
13. existing component retention/atomic publication semantics are unchanged;
14. no material canonical refresh runtime regression is introduced;
15. after merge, a separately controlled server apply/refresh validates the actual persisted structural block.