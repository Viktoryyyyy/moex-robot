# BACKTEST / EXECUTION SEMANTICS SPEC

Status: FROZEN IMPLEMENTABLE CONTRACT  
Project: MOEX Bot  
Applies to: backtest engine / strategy adapters / runtime boundary  
Source level: canonical semantics contract  
Authority: MOEX_Bot_Target_Architecture_2026_All_In_One.md, STRATEGY_SDK_SPEC.md

---

## 1. Purpose

This document freezes one canonical backtest / execution semantics contract.

It defines:
- time semantics
- signal formation boundary
- execution boundary
- position transition rules
- PnL rules
- cost rules
- invalid-data behavior
- anti-cheat rules
- portfolio boundary
- live runtime boundary
- mandatory validations
- minimum acceptance criteria

No strategy may redefine these semantics ad hoc.

---

## 2. Time model

### 2.1 Canonical bar timestamp

- Bar timestamp is the **end timestamp** of a finalized bar.
- `open/high/low/close` belong to that finalized interval.
- `next_open` means the open of the immediately next valid executable bar in canonical order.

### 2.2 Trade session date

- `trade_session_date` means the exchange session date.
- It is not a naive wall-clock calendar date.
- Trading/non-trading status and session-date interpretation must follow the exchange calendar contract.

### 2.3 Timezone

- Market timestamps must be timezone-aware.
- Canonical market-time semantics must use exchange-local market time.
- User timezone and server timezone must not change signal, execution, session, or PnL semantics.

---

## 3. Signal semantics

### 3.1 Signal formation rule

- A canonical signal is formed only on a **finalized closed bar** `t`.
- Signal logic may use only data available no later than bar-close `t`.

### 3.2 Allowed decision-time inputs

Allowed at signal time:
- historical bars up to and including finalized bar `t`
- derived features computed only from allowed history
- explicitly declared prior persisted state

Forbidden at signal time:
- any future bar
- any future feature value
- any outcome-derived value
- mutable intrabar data not frozen by bar-close semantics

### 3.3 Finalized-bar rule

- Canonical semantics are finalized-bar only.
- Historical signals must not be revised using future bars.

### 3.4 D-1 boundary

- Pre-open or ex-ante day-level decisions for trading day `D` may use only completed `D-1` daily information.
- Any use of `D` end-of-day or future intraday information for ex-ante decision on `D` is forbidden.

---

## 4. Execution semantics

### 4.1 Canonical execution mode

- Canonical backtest execution mode is `next_bar_open`.
- Default execution mode is `next_bar_open`.

### 4.2 Fill timing rule

- Signal formed on finalized bar `t` may first change position at the open of bar `t+1`.
- Same-bar execution from the signal-forming bar is forbidden.

### 4.3 Forbidden execution modes inside canonical backtest semantics

Forbidden unless a future frozen contract explicitly adds them:
- same-bar close execution
- same-bar market fill
- hidden next-tick approximation
- close-to-close execution as canonical default
- strategy-local execution timing overrides

---

## 5. Position transition semantics

### 5.1 Primitive transitions

- `flat -> long` = 1 action
- `flat -> short` = 1 action
- `long -> flat` = 1 action
- `short -> flat` = 1 action

### 5.2 Reversal handling

- `long -> short` = 2 ordered actions at one execution event:
  1. close long
  2. open short
- `short -> long` = 2 ordered actions at one execution event:
  1. close short
  2. open long

### 5.3 Action counting semantics

- Action counting is leg-based, not signal-based.
- Reversal therefore counts as 2 actions.

---

## 6. PnL semantics

### 6.1 Canonical mark basis

- Canonical backtest mark basis is **open-to-open**.
- After execution at `open_t`, the resulting position is held over interval `[open_t, open_{t+1})`.

### 6.2 Canonical bar PnL rule

- Canonical bar PnL is computed from the held position over the open-to-open interval.
- Direction sign must be applied by position side.
- Execution costs must be booked at execution events.

### 6.3 Realized / unrealized

- Realized PnL is recognized on close, reduction, reversal, and forced terminal close.
- Unrealized PnL may exist for observability, but canonical backtest verdicts must follow the frozen engine path above.

### 6.4 Daily aggregation rule

- Daily PnL aggregation is the sum of canonical bar PnL and booked costs by `trade_session_date`.
- Aggregation must follow session-date semantics, not naive local-date grouping.

### 6.5 Terminal forced close rule

- If a non-flat position remains on the final executable bar, terminal forced close is mandatory.
- Forced terminal close must occur exactly once.

---

## 7. Cost model semantics

### 7.1 Commission model

- Canonical commission model is fixed points per action.
- Default assumption: **2 points per action**.
- Reversal bears 2 action-costs.

### 7.2 Slippage model

- Default slippage assumption is `0`.
- Slippage customization, if approved later, must be platform-level and explicit.

### 7.3 Boundary rule

Allowed:
- platform-level cost parameters
- platform-level approved slippage model selection

Forbidden:
- strategy-local ad hoc cost logic
- inline research-script cost engines
- adapter-level redefinition of canonical costs

---

## 8. Missing / invalid data behavior

### 8.1 Fail-closed rule

The canonical engine must fail closed on:
- duplicate timestamps
- broken ordering / non-monotonic bars
- missing required execution bar
- missing required `next_open`
- invalid executable prices
- corrupted bar rows that make execution semantics ambiguous

### 8.2 Forbidden silent repairs

Forbidden:
- silent deduplication
- silent reordering
- silent forward-fill for executable prices
- replacing a missing execution bar with a nearby bar
- hidden fallback to another dataset slice

### 8.3 Skip behavior

- Skip behavior is allowed only when non-trading status is established explicitly by calendar/data contract before semantics are applied.
- Skip behavior must not silently mask data corruption.

---

## 9. Anti-cheat / leakage rules

### 9.1 Mandatory rules

- Each row must have explicit date/time meaning.
- Feature construction and outcome construction must be separated.
- No look-ahead is allowed.
- No future-dependent inputs are allowed in signal generation, selection, filtering, or labeling.

### 9.2 D-1 vs D-day rule

- If the decision is ex-ante for day `D`, only completed `D-1` data may be used.
- Any direct or indirect use of future `D` outcome information is forbidden.

### 9.3 Stability rule

- Historical signal state must not depend on future bars.
- Historical context labels must not be revised using post-decision outcomes unless they are explicitly outcome labels kept separate from decision inputs.

---

## 10. Portfolio semantics boundary

### 10.1 Strategy-level responsibility

Strategy package may own only:
- deterministic signal logic
- strategy-local mapping into backtest/runtime contracts
- declared strategy-local metadata/artifacts

### 10.2 Platform-level responsibility

Platform backtest/runtime layer must own:
- portfolio aggregation
- capital allocation
- cross-strategy combination logic
- netting/exposure aggregation
- portfolio-level PnL aggregation

### 10.3 Forbidden strategy behavior

Strategy package must not implement:
- standalone portfolio engine
- cross-strategy PnL aggregation
- strategy-local redefinition of portfolio semantics

---

## 11. Live execution boundary

### 11.1 Frozen strategy output

- Frozen live-boundary output from strategy/live adapter is **desired position** after finalized signal/context evaluation.

### 11.2 Runtime responsibility

Runtime layer must own:
- reconcile desired position vs actual/current position
- conversion of position delta into order intent
- reject/fail handling
- market calendar gating
- risk gating

### 11.3 Fail behavior boundary

Fail closed when any required boundary input is invalid or unavailable, including:
- malformed desired position output
- calendar closed / non-tradable session
- risk gate closed
- invalid required runtime state

### 11.4 Deferred runtime details

Frozen now:
- desired position semantics
- reconcile boundary
- fail-closed requirement

Deferred to runtime implementation:
- broker-specific order types
- retry schedule
- partial-fill handling details
- broker-specific reject recovery

---

## 12. Validation / invariant model

### 12.1 Mandatory invariants

- timestamps are unique
- timestamps are strictly increasing
- timestamps are timezone-aware
- signal timestamp is strictly earlier than execution timestamp
- no same-bar execution in canonical backtest mode
- position changes only at execution events
- reversal = 2 actions
- reversal = 2 commission charges
- flat position bars do not generate directional PnL
- forced terminal close occurs exactly once if final position is non-flat
- ex-ante day `D` decisions use only completed `D-1` daily information

### 12.2 Fail conditions

The semantics contract must fail on:
- duplicate timestamps
- broken time ordering
- missing `next_open`
- invalid executable prices
- undeclared required artifact dependency
- detected future leakage

### 12.3 Required test classes

Required:
- unit tests for signal-to-execution delay
- unit tests for reversal accounting
- unit tests for commission booking
- unit tests for terminal forced close
- unit tests for daily aggregation
- contract tests for no-lookahead and D-1 boundary
- contract tests for monotonicity / duplicate rejection
- integration tests for strategy adapter -> canonical engine
- integration tests for live adapter -> desired-position -> reconcile boundary
- regression tests on frozen fixture datasets

---

## 13. Minimum acceptance criteria

### 13.1 First reference strategy in backtest

Must prove all of the following:
- uses canonical engine without custom inline PnL logic
- signal on finalized bar `t` changes position only at `t+1` open
- reversal produces exactly 2 actions and 2 commissions
- daily PnL equals the sum of canonical open-to-open intervals and booked costs
- final open position is forcibly closed exactly once
- duplicate/ordering/invalid-price fixtures fail closed
- no-lookahead tests pass

### 13.2 First reference strategy in live runtime boundary

Must prove all of the following:
- `signal_engine.py` remains deterministic and side-effect free
- `live_adapter.py` emits desired position only
- runtime reconcile produces deterministic position delta / order intent
- reject/calendar/risk/data failures do not silently mutate strategy semantics
- strategy package does not own generic lock/session/logger/order-management infrastructure

---

## 14. Final frozen statement

The MOEX Bot platform has one canonical backtest / execution semantics layer.

That layer is finalized-bar, next-bar-open, open-to-open, fail-closed, anti-lookahead, and strategy-independent.

No strategy, adapter, research script, or runtime loop may redefine these semantics ad hoc.
