# Architecture Completion — Runtime Control Plane Spec

## 1. verdict

The correct next architecture-completion step on current priority #2 is:

**freeze one minimal platform runtime control-plane boundary that decides which registered strategy is allowed to run, when it is allowed to run, and what coarse status the platform remembers about that run**

This step is intentionally narrow.
It completes the first missing shared runtime control boundary after the registered backtest runner boundary and the minimal research-result publication boundary are frozen.
It does not approve strategy-local runtime rewrites, observability redesign, live adapter redesign, or broad orchestration replacement.

## 2. repo-backed reason this is the next step

The frozen target architecture already makes this boundary explicit.

The target architecture freezes that:
- `moex_runtime` owns `orchestrator`, `scheduler`, `state_store`, `locks`, `risk`, `execution`, `telemetry`, `notifiers`, and `preflight`
- runtime must support simultaneous execution of multiple strategies, per-strategy isolated state, a shared state/lock framework, shared telemetry/health, and portfolio-aware routing/risk control
- the runtime control plane must be capable of enabling/disabling strategies declaratively, scheduling by strategy/timeframe/market window, controlling failure/restart semantics, preventing state collisions, and exposing current health and last-run status fileciteturn0file0

So after the minimal shared research-result publication plus experiment-registry write path is frozen, the next platform-first completion step is not a strategy-local runtime fix.
It is the minimal shared control-plane boundary that turns runtime from ad hoc loop ownership into platform-owned strategy admission, scheduling, and coarse lifecycle control.

## 3. exact minimal boundary frozen in this spec

This spec freezes only the first shared runtime control-plane boundary.

It freezes exactly:
1. exact minimal control-plane responsibilities
2. declarative enable/disable strategy boundary
3. scheduling boundary by strategy, timeframe, and market window
4. failure/restart semantics boundary
5. current health and last-run status boundary
6. what belongs to control plane vs what remains in runtime execution layers
7. the exact future apply-only acceptance boundary
8. the explicit out-of-scope list

## 4. exact minimal runtime control-plane responsibilities

The minimal runtime control plane owns only the following platform responsibilities.

### 4.1 strategy admission

The control plane decides whether a registered strategy runtime unit is eligible to run at all.

It must evaluate only platform-level admission inputs such as:
- registered strategy identity
- enabled/disabled state
- declared schedule
- declared market window
- coarse prior run state needed for restart/failure policy

It must not evaluate:
- strategy signal math
- market alpha logic
- execution math
- order-routing detail

### 4.2 schedule evaluation

The control plane decides whether a strategy runtime unit is due now according to its declarative schedule.

### 4.3 run-intent dispatch

The control plane emits or authorizes a run intent toward the runtime execution layer when a strategy runtime unit is both enabled and due.

Frozen minimal rule:
- the control plane decides **whether** a run should start
- the runtime execution layer decides **how** that run is executed

### 4.4 coarse lifecycle state ownership

The control plane owns the coarse platform state for one strategy runtime unit, including whether it is:
- disabled
- scheduled and eligible
- running
- blocked by policy
- succeeded on last terminal run
- failed on last terminal run
- restart-eligible or restart-blocked under policy

This is control metadata, not execution-internal state.

### 4.5 current health and last-run publication

The control plane owns one coarse status surface that exposes current health and last-run information for each strategy runtime unit.

## 5. declarative enable/disable strategy boundary

Enable/disable is frozen as a **declarative platform control** attached to a registered strategy runtime unit.

### 5.1 what the runtime unit represents

The minimal controlled unit is:

`strategy_id + runtime_mode + instrument scope if applicable + timeframe + schedule policy`

Frozen minimal rule:
- control-plane enable/disable acts on the registered runtime unit, not on ad hoc script names
- the apply package may choose the exact config shape, but the controlled unit must remain declarative and registry/config driven

### 5.2 enable semantics

When enabled:
- the strategy runtime unit is eligible for schedule evaluation
- eligibility still depends on market window and failure/restart policy

### 5.3 disable semantics

When disabled:
- the control plane must not dispatch new run intents for that runtime unit
- disabled does not imply deletion of prior status/history

### 5.4 what enable/disable does not mean

Enable/disable does not:
- force a strategy to produce signals
- bypass risk/preflight gates inside runtime execution
- rewrite execution-layer state
- modify strategy-local parameters

## 6. scheduling boundary by strategy, timeframe, and market window

Scheduling is frozen as a platform-owned declarative boundary.

### 6.1 minimal schedule inputs

For each registered runtime unit the schedule must be able to declare at minimum:
- `strategy_id`
- `timeframe`
- `market_window`
- cadence or trigger rule

Frozen minimal rule:
- this first slice requires only enough structure to decide whether the unit is due in the current market window
- it does not require a broad event engine or a general workflow engine

### 6.2 market-window meaning

`market_window` is the allowed trading/session window within which the control plane may authorize the run.

Frozen minimal rule:
- market-window eligibility belongs to the control plane
- bar aggregation, session math inside a strategy, and live adapter behavior do not move into the control plane

### 6.3 scheduling output

The only required control-plane scheduling output is:
- due vs not due for a given runtime unit at evaluation time
- and, if due and enabled, one authorized run intent

### 6.4 what scheduling does not own

Scheduling does not own:
- feed aggregation
- signal timing math inside a strategy
- order placement timing inside a live adapter
- notifier formatting

## 7. failure/restart semantics boundary

Failure/restart semantics are frozen as a **policy boundary**, not as execution-internal recovery logic.

### 7.1 control-plane ownership

The control plane owns the minimal policy decision for what a prior terminal or stale state means for the next scheduled admission attempt.

The control plane may classify the last known state of a runtime unit as for example:
- last run succeeded
- last run failed
- run appears still running
- run appears stale/timed out
- restart allowed
- restart blocked pending operator action

### 7.2 execution-layer ownership

The runtime execution layer remains owner of:
- in-process exception handling
- safe stop behavior
- safe checkpoint/resume behavior if such capability exists
- execution-loop cleanup
- strategy-local runtime state transitions

### 7.3 frozen minimal restart rule

The first slice must freeze only enough policy to decide:
- whether the next scheduled start is allowed after prior failure/staleness
- whether the runtime unit is blocked, eligible, or restart-eligible

This first slice does not freeze:
- detailed retry backoff algorithms
- deep worker supervision trees
- distributed recovery machinery

## 8. current health / last-run status boundary

The control plane owns one coarse status view per runtime unit.

### 8.1 minimum current-health payload

The minimum health/status boundary must expose at least:
- runtime unit identity
- enabled/disabled state
- current coarse state
- last evaluation timestamp
- last start timestamp if any
- last finish timestamp if any
- last terminal outcome
- restart-policy state if applicable

### 8.2 health meaning in this first slice

Health here means **control-plane health/status**, not full observability.

It is limited to answering questions such as:
- is this runtime unit enabled
- is it due or blocked
- is it currently believed to be running
- what happened on the last completed run
- is automatic next admission allowed

### 8.3 what health does not include in this spec

This first slice does not require:
- deep metrics dashboards
- latency/freshness histograms
- signal-count summaries
- execution-count summaries
- PnL summaries
- broad telemetry redesign

## 9. control plane vs runtime execution layer boundary

The boundary is frozen as follows.

### 9.1 control plane owns

The control plane owns:
- declarative runtime-unit registration/selection inputs
- enable/disable state
- schedule evaluation
- market-window admission
- coarse failure/restart policy state
- current health and last-run coarse status publication
- dispatch authorization toward execution

### 9.2 runtime execution layers own

Runtime execution layers remain owners of:
- strategy-local runtime logic
- feature/context loading needed for execution
- signal generation
- live adapter behavior
- order intent generation
- execution reconciliation
- lock handling implementation
- in-process runtime state/checkpoint details
- preflight/risk gate execution details
- telemetry detail emission

### 9.3 exact no-widen rule

The future apply-only package for this spec must not move strategy-local logic upward into the control plane.

The control plane is a platform admission/scheduling/status boundary.
It is not a replacement for execution-layer strategy runners.

## 10. exact acceptance boundary for a future apply-only package

A later apply-only package is acceptable only if all points below are true.

### 10.1 minimal platform-first file scope

Expected narrow file scope:
- one new or clearly isolated runtime control-plane surface under platform runtime code
- only the narrow contract/config additions required to declare runtime units, enable/disable state, and schedule policy
- only the narrow integration needed to dispatch authorized run intents into existing runtime execution entry surfaces
- only the narrow persisted/status surface required for coarse current health and last-run status

No strategy-local runtime rewrite is required in the same package.

### 10.2 declarative runtime-unit control exists

After apply, at least one registered runtime unit must be controllable declaratively through:
- enabled/disabled state
- declared timeframe
- declared market window
- declared schedule policy

### 10.3 schedule evaluation exists at platform boundary

After apply, the platform must be able to decide for a controlled runtime unit:
- not due
- due but blocked by policy
- due and authorized to dispatch

### 10.4 failure/restart policy boundary exists

After apply, the control plane must persist or expose enough coarse state to decide whether a previously failed or stale runtime unit may be started again automatically.

### 10.5 current health / last-run status exists

After apply, the platform must expose one coarse control-plane status surface for a runtime unit with at least:
- enabled/disabled state
- current coarse state
- last terminal outcome
- last start/finish timestamps if known

### 10.6 execution layers remain below the boundary

After apply:
- strategy-local signal/execution logic must remain outside the control plane
- live adapter behavior must remain outside the control plane
- observability redesign must not be bundled into the same package

### 10.7 no widening is allowed

The apply-only package must not widen into:
- strategy-local fixes
- observability redesign
- experiment-registry redesign
- runtime execution math
- live adapter redesign
- broad orchestration rewrite
- server apply proof in the same cycle

## 11. explicit out-of-scope list

Out of scope for this contract-freeze cycle:
- apply
- server proof
- strategy-local runtime fixes
- observability redesign
- runtime execution math
- live adapter redesign
- deep lock/state-store redesign
- deep retry/backoff framework design
- portfolio routing redesign
- experiment-registry changes
- broad workflow/orchestration engine redesign
- execution-loop implementation details below the control-plane boundary

## 12. blocker section

No real repo-backed blocker is evidenced for this spec-only cycle.

The frozen target architecture already declares the existence and responsibilities of a runtime control plane, including declarative enable/disable, scheduling by strategy/timeframe/market window, failure/restart control, and current health/last-run status exposure fileciteturn0file0

So the next architecture-completion package on current priority #2 can be frozen now as one minimal platform-owned runtime control-plane boundary.

## 13. short final statement

Freeze the next architecture-completion package as one minimal platform-owned runtime control plane that declaratively admits registered strategy runtime units, schedules them by timeframe and market window, applies coarse failure/restart policy, and publishes current health plus last-run status without absorbing execution-layer logic.
