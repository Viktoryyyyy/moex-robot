# Architecture Completion — Multi-Unit Scheduler Loop Spec

## 1. verdict

The correct next architecture-completion step on current runtime-control-plane priority is:

**freeze one minimal platform-owned multi-unit scheduler loop that discovers declared runtime units from repo config, evaluates them in deterministic order, and dispatches them one-by-one through the already-frozen one-unit control-plane execution boundary while producing one coarse aggregated run summary**

This step is intentionally narrow.
It closes the next shared platform gap after the minimal one-unit runtime control-plane boundary is frozen and applied.
It does not approve strategy-local runtime rewrites, observability redesign, deep worker supervision, retry/backoff redesign, or broad orchestration replacement.

## 2. repo-backed reason this is the next step

The frozen target architecture already requires runtime to support simultaneous execution of multiple strategies and already places both `orchestrator` and `scheduler` under `moex_runtime` fileciteturn0file0

The frozen registry/config model already requires declarative strategy discovery and runtime dependency resolution from canonical registry/config state under `configs/strategies/`, `configs/portfolios/`, and `configs/environments/`, with runtime eligibility forbidden from being inferred from ad hoc script names or guessed paths fileciteturn5file0

The already-frozen runtime-control-plane spec has also narrowed the first shared boundary to one controlled runtime unit with declarative enable/disable, schedule evaluation, market-window admission, coarse failure/restart policy, and coarse current-health / last-run status publication fileciteturn3file0

So the next platform-first completion step is not another one-unit boundary change.
It is the minimal loop that lifts that existing one-unit control boundary to the first deterministic multi-unit platform scheduler surface.

## 3. exact minimal boundary frozen in this spec

This spec freezes only the first minimal multi-unit scheduler loop.

It freezes exactly:
1. exact minimal boundary for runtime-unit discovery
2. exact minimal scheduler loop over declared runtime units
3. deterministic iteration and evaluation order
4. interaction with the existing one-unit control-plane execution boundary
5. coarse aggregated run result / summary boundary
6. what remains out of scope
7. the exact future apply-only acceptance boundary

## 4. exact minimal boundary for runtime-unit discovery

Runtime-unit discovery is frozen as a **repo/config-derived platform step**.

### 4.1 discovery source

The scheduler loop must discover candidate runtime units only from declared repo configuration / registry state.

Frozen minimal rule:
- discovery is from canonical repo-backed config/registry contracts
- discovery is not from script-name scanning
- discovery is not from manual in-code hardcoded unit lists
- discovery is not from guessed server filesystem state

### 4.2 minimal discovered unit identity

The minimal discovered runtime unit identity is:

`runtime_unit_id = strategy_id + runtime_mode + timeframe + instrument_scope or equivalent declared scope`

The apply package may choose the exact serialized id shape, but it must remain stable, explicit, and declarative.

### 4.3 minimal discovered fields

For each discovered runtime unit the scheduler loop must be able to resolve at minimum:
- runtime unit identity
- strategy identity
- enabled/disabled state
- declared timeframe
- declared market window
- declared schedule policy
- references needed to call the existing one-unit control-plane evaluation / dispatch boundary

### 4.4 narrow scope rule

This first slice freezes only enough discovery to enumerate declared runtime units that are scheduler-visible.
It does not require general dependency-graph planning, portfolio optimization, or dynamic strategy generation.

## 5. exact minimal scheduler loop over declared runtime units

The scheduler loop is frozen as one **platform-owned sequential evaluation loop** over the discovered runtime units.

### 5.1 loop responsibility

The loop must do only the following:
1. resolve the declared runtime-unit set
2. place that set into deterministic order
3. evaluate each unit exactly once per scheduler pass
4. call the existing one-unit control-plane boundary for that unit
5. collect one coarse per-unit outcome for the pass
6. publish one coarse aggregated scheduler-pass summary

### 5.2 loop non-responsibility

The loop must not own:
- strategy signal math
- bar/feed aggregation
- order routing
- strategy-local runtime state logic
- deep retry control
- distributed execution
- telemetry redesign

### 5.3 minimal pass meaning

One scheduler pass means one complete deterministic sweep across the discovered runtime-unit set.
The first slice does not freeze cron policy, daemon supervision policy, or distributed pass coordination.

## 6. deterministic iteration and evaluation order

Deterministic iteration is mandatory.

### 6.1 frozen ordering rule

For one scheduler pass, all discovered runtime units must be evaluated in one stable deterministic order derived from declared unit identity.

Frozen minimal rule:
- same discovered unit set + same declared ordering inputs -> same scheduler iteration order
- no nondeterministic map/set iteration
- no dependence on filesystem enumeration order
- no dependence on process start race order

### 6.2 acceptable ordering basis

The apply package may choose the exact stable ordering mechanism, but it must be based only on declared runtime-unit identity fields.

Examples of acceptable basis:
- lexical order of stable `runtime_unit_id`
- explicit ordered list materialized from canonical config
- deterministic tuple order such as `(strategy_id, timeframe, instrument_scope, runtime_mode)`

### 6.3 evaluation rule

The scheduler loop must evaluate units one-by-one in that deterministic order.

Frozen minimal rule:
- this first slice is sequential, not parallel
- one unit is evaluated and dispatched to the one-unit boundary before the next unit is evaluated
- parallel fan-out remains out of scope

## 7. interaction with the existing one-unit control-plane execution boundary

The multi-unit scheduler loop does not replace the one-unit control plane.
It sits one level above it.

### 7.1 exact interaction rule

For each runtime unit in the scheduler pass, the scheduler loop must call the already-frozen one-unit control-plane boundary as the sole per-unit admission / dispatch surface.

So the ownership split is:
- multi-unit scheduler loop decides **which declared runtime units are iterated this pass and in what deterministic order**
- one-unit control-plane boundary decides **whether that single unit is due, blocked, or dispatch-authorized**
- execution layer decides **how the authorized single-unit run is actually executed**

### 7.2 no duplicate admission logic rule

The multi-unit scheduler loop must not duplicate or absorb:
- per-unit enable/disable semantics
- per-unit market-window admission logic
- per-unit failure/restart policy logic
- per-unit run dispatch semantics

Those remain inside the existing one-unit boundary.

### 7.3 minimal per-unit result shape

For one scheduler pass, the loop only needs one coarse per-unit result from the one-unit boundary such as:
- skipped_not_due
- skipped_disabled
- skipped_blocked
- dispatch_authorized
- dispatch_failed
- evaluation_failed

The exact enum names may differ in apply, but the result must remain coarse and scheduler-facing.

## 8. coarse aggregated run result / summary boundary

The scheduler loop owns one coarse aggregated scheduler-pass summary.

### 8.1 minimum aggregated summary payload

For each completed scheduler pass the platform must be able to expose at minimum:
- scheduler pass timestamp or pass id
- total discovered runtime units
- total evaluated runtime units
- count skipped_disabled
- count skipped_not_due
- count skipped_blocked
- count dispatch_authorized
- count dispatch_failed
- count evaluation_failed
- final scheduler-pass status

### 8.2 summary meaning

This is a coarse control-plane scheduler summary only.
It answers questions such as:
- how many runtime units were visible this pass
- how many were skipped vs authorized
- whether the pass completed cleanly or with coarse failures

### 8.3 what the aggregated summary does not include

This first slice does not require:
- deep per-unit telemetry
- latency histograms
- signal-count summaries
- execution-count summaries
- PnL summaries
- portfolio summaries
- notifier redesign

## 9. what remains out of scope

Out of scope for this contract-freeze cycle:
- apply
- server proof
- strategy-local runtime fixes
- observability redesign
- deep worker supervision
- distributed scheduling
- retry/backoff redesign
- live adapter redesign
- broad orchestration rewrite
- portfolio routing redesign
- execution math redesign
- parallel multi-unit fan-out
- dynamic load balancing
- fairness policies beyond deterministic stable order

## 10. exact acceptance boundary for a future apply-only package

A later apply-only package is acceptable only if all points below are true.

### 10.1 narrow platform-first file scope

Expected narrow file scope:
- one new or clearly isolated platform scheduler-loop surface under runtime code
- only the narrow config / registry resolution additions needed to discover scheduler-visible runtime units
- only the narrow integration needed to call the existing one-unit control-plane boundary for each discovered unit
- only the narrow persisted/status surface needed for one coarse aggregated scheduler-pass summary

No strategy-local runtime rewrite is required in the same package.

### 10.2 repo-config discovery exists

After apply, the scheduler loop must discover runtime units from repo-backed registry/config state, not from ad hoc script-name lists or guessed paths.

### 10.3 deterministic full pass exists

After apply, one scheduler pass must:
- enumerate the discovered runtime units
- place them in deterministic order
- evaluate each unit exactly once in that pass

### 10.4 one-unit boundary remains the per-unit gate

After apply, each unit in the loop must still pass through the existing one-unit control-plane evaluation / dispatch boundary.
The multi-unit loop must not inline or replace that logic.

### 10.5 coarse aggregated summary exists

After apply, the platform must expose one coarse scheduler-pass summary with at least:
- total discovered units
- total evaluated units
- coarse per-unit outcome counts
- final scheduler-pass status

### 10.6 no widening is allowed

The apply-only package must not widen into:
- strategy-local fixes
- observability redesign
- deep worker supervision
- distributed scheduling
- retry/backoff redesign
- live adapter redesign
- broad orchestration rewrite

## 11. blocker section

No real repo-backed blocker is evidenced for this spec-only cycle.

The frozen target architecture already requires multi-strategy runtime support and already reserves runtime `scheduler` / `orchestrator` ownership at the platform layer fileciteturn0file0
The registry/config model already freezes declarative runtime discovery and forbids runtime eligibility by script-name inference or guessed paths fileciteturn5file0
The existing runtime-control-plane spec already freezes the one-unit admission / dispatch boundary that this scheduler loop must call rather than replace fileciteturn3file0

So the next runtime-control-plane package can be frozen now as one minimal deterministic multi-unit scheduler loop over declared runtime units.

## 12. short final statement

Freeze the next architecture-completion package as one minimal platform-owned deterministic multi-unit scheduler loop that discovers declared runtime units from repo config, iterates them in stable order, dispatches them through the existing one-unit control-plane boundary, and emits one coarse aggregated scheduler-pass summary.
