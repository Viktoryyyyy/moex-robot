# Architecture Completion — Guardrail and Test Hardening Spec

## 1. verdict

The correct next architecture-completion step on the current remaining priority is:

**freeze one minimal mandatory platform guardrail and test-hardening layer that enforces architecture-critical validation boundaries for artifact integrity, no-lookahead safety, canonical backtest semantics, ordered time series behavior, and runtime state isolation**

This step is intentionally narrow.
It closes the first mandatory quality/guardrail layer after the research-result publication path, minimal runtime control plane, and deterministic multi-unit scheduler loop are already frozen and applied.
It does not approve strategy-local fixes, runtime redesign, observability redesign, dashboard work, or a broad test-suite rewrite.

## 2. repo-backed reason this is the correct next step

The frozen target architecture already makes this boundary mandatory.

The target architecture freezes that:
- the platform must be reproducible by default and observable by default
- runtime must host multiple strategies without collisions in state or artifacts
- backtest semantics are single-source and reused by all strategies
- path/data contracts are explicit and enforced
- required test layers are `unit`, `contract`, `integration`, and `regression`
- mandatory guardrail tests must include `artifact contract integrity`, `no-lookahead constraints`, `backtest semantics invariants`, `schema validation`, `time ordering / monotonicity where required`, and `runtime state collision prevention` fileciteturn0file0

The registry/config model also already freezes fail-closed validation, explicit artifact-contract linkage, declared ids, and forbids path guessing, undeclared mutable server state, hidden cwd dependencies, and silent latest-file fallback fileciteturn5file0

The already-frozen runtime-control-plane and multi-unit scheduler loop boundaries also make state isolation and deterministic multi-unit behavior part of the platform contract, not strategy-local behavior fileciteturn3file0 fileciteturn4file0

So after those platform surfaces are in place, the correct next architecture-completion step is not another feature addition.
It is the first minimal mandatory guardrail/test layer that proves those frozen contracts cannot silently regress.

## 3. exact minimal mandatory guardrail/test categories required by target architecture

This spec freezes only the **mandatory minimal categories** required for architecture integrity.

Exactly these categories are required:
1. artifact contract integrity validation
2. no-lookahead validation
3. backtest semantics invariant validation
4. time-ordering / monotonicity validation
5. runtime state collision prevention validation

### 3.1 required layer meaning

This first guardrail/test hardening package is not a broad quality program.
It is the narrow minimum needed so that the already-frozen architecture contracts are enforceable and regression-resistant.

### 3.2 minimal test-layer placement rule

The apply-only package may distribute these validations across unit / contract / integration / regression surfaces as appropriate, but each mandatory category must exist in an explicit platform-owned validation surface.

Frozen minimal rule:
- the package is accepted by **coverage of categories**, not by forcing every category into the same test type
- broad snapshot testing or general code-coverage expansion is not required

## 4. artifact contract integrity validation boundary

Artifact contract integrity validation is mandatory.

### 4.1 what this boundary protects

It protects the frozen architecture rule that artifacts are resolved only through explicit contracts and registries, not through guessed paths, silent fallback, or mutable server memory.

### 4.2 exact minimum validations required

The first guardrail package must validate at minimum that:
- every required artifact reference resolves through one declared contract
- artifact class is one of the approved classes only
- producer/consumer linkage is explicit
- partitioning rule is explicit when required
- schema/version metadata is explicit where the contract requires it
- missing artifact contract fails closed
- conflicting artifact contract binding fails closed
- undeclared latest-file discovery is rejected
- path guessing is rejected

### 4.3 exact boundary

This boundary is about **contract integrity**, not file freshness or content-business correctness.

It must answer only:
- is the artifact declared
- is the contract structurally valid
- is resolution explicit and deterministic
- does invalid or missing contract state fail closed

### 4.4 what is not required here

Not required in this first slice:
- broad artifact retention policy testing
- large-scale end-to-end storage migration tests
- dashboard/report rendering tests

## 5. no-lookahead validation boundary

No-lookahead validation is mandatory.

### 5.1 what this boundary protects

It protects the frozen anti-cheat architecture rule that ex-ante decisions may not consume future information and that features/labels/results remain causally valid for the declared row semantics and decision timing fileciteturn0file0

### 5.2 exact minimum validations required

The first guardrail package must validate at minimum that:
- prior-information vs future-outcome boundaries are explicit where required
- feature sets declared lookahead-safe do not use future-dependent values relative to their decision timestamp semantics
- label construction does not leak future outcome into ex-ante feature state
- D-1 style ex-ante decision boundaries fail when implemented with D-day or later information where forbidden by contract
- row/date semantics used for validation are explicit and deterministic

### 5.3 exact boundary

This boundary is about **information timing correctness**.
It is not a generic model-quality test.

It must answer only:
- does any declared ex-ante decision surface consume future information
- do declared lookahead-safe feature/label paths preserve the frozen timing contract

### 5.4 what is not required here

Not required in this first slice:
- statistical edge validation
- strategy alpha tests
- broad feature-library refactor

## 6. backtest semantics invariant boundary

Backtest semantics invariant validation is mandatory.

### 6.1 what this boundary protects

It protects the frozen rule that the platform has **one canonical backtest semantics layer** and that strategies may adapt into it but may not silently redefine it ad hoc fileciteturn0file0

### 6.2 exact minimum invariants required

The first guardrail package must validate at minimum the invariant behavior of:
- signal timestamp vs execution timestamp semantics
- declared execution delay semantics
- reversal handling
- commission/cost application boundary
- forced terminal close rule
- invalid/missing data fail behavior where already frozen by semantics

### 6.3 exact boundary

This boundary is about **semantic invariants**, not broad performance testing.

It must answer only:
- does the canonical engine preserve the frozen execution contract
- can a regression silently alter the execution semantics used across strategies

### 6.4 what is not required here

Not required in this first slice:
- exhaustive parameter-grid regression suites
- large historical benchmark reruns across the full research estate
- portfolio redesign tests

## 7. time-ordering / monotonicity validation boundary

Time-ordering and monotonicity validation is mandatory.

### 7.1 what this boundary protects

It protects the frozen contract that time-series artifacts and runtime/backtest inputs must behave deterministically in declared temporal order, without nondeterministic ordering or silently accepted backward time movement.

### 7.2 exact minimum validations required

The first guardrail package must validate at minimum that, where ordering is required by contract:
- timestamps or declared ordering keys are monotonic
- duplicate ordering keys are either explicitly rejected or handled by already-frozen contract semantics
- backtest input ordering cannot silently invert execution order
- runtime scheduler/control surfaces that rely on declared identity/order do not depend on nondeterministic iteration order
- time-partitioned artifact inputs do not silently cross declared partition boundaries in a way that breaks ordering semantics

### 7.3 exact boundary

This boundary is about **ordered processing correctness**.
It is not a data-quality program for all possible malformed input.

It must answer only:
- is ordering deterministic where the architecture requires it
- are monotonicity and sequence assumptions explicitly guarded where they are contract-critical

### 7.4 what is not required here

Not required in this first slice:
- broad lateness/freshness observability redesign
- full data-cleansing framework
- every historical anomaly case ever seen on the server

## 8. runtime state collision prevention validation boundary

Runtime state collision prevention validation is mandatory.

### 8.1 what this boundary protects

It protects the frozen architecture rule that runtime must support multiple strategies with isolated state and prevent state/artifact collisions across runtime units fileciteturn0file0

### 8.2 exact minimum validations required

The first guardrail package must validate at minimum that:
- runtime-unit identity used for persisted control/state isolation is explicit and stable
- two distinct runtime units cannot silently bind to the same persisted state namespace when the contract requires isolation
- lock/state/session ownership surfaces reject or guard against incompatible shared binding
- collision-prone artifact/state naming derived from incomplete identity is rejected or fails closed
- multi-unit scheduler/control-plane interactions do not erase per-unit isolation guarantees

### 8.3 exact boundary

This boundary is about **state namespace isolation**, not deep supervision design.

It must answer only:
- can two distinct runtime units collide in persisted control/state artifacts
- is that collision prevented or fail-closed by platform validation

### 8.4 what is not required here

Not required in this first slice:
- distributed lock-manager redesign
- worker pool redesign
- broad runtime recovery redesign

## 9. exact acceptance boundary for a future apply-only package

A later apply-only package is acceptable only if all points below are true.

### 9.1 narrow platform-first file scope

Expected narrow file scope:
- one clearly platform-owned validation/guardrail surface or small set of surfaces
- only the narrow test utilities/helpers needed to exercise the mandatory categories
- only the narrow integration needed to wire those validations into existing repo test structure
- only the narrow contract/config additions strictly required for deterministic validation inputs

No strategy-local fix bundle is required in the same package.
No broad rewrite of the whole test suite is allowed.

### 9.2 all mandatory categories exist

After apply, the repo must contain explicit validation coverage for all five frozen categories:
- artifact contract integrity
- no-lookahead
- backtest semantics invariants
- time-ordering / monotonicity
- runtime state collision prevention

### 9.3 fail-closed behavior is testable

After apply, at least one explicit failing-path validation must exist for each category where the frozen architecture requires hard rejection or fail-closed behavior.

### 9.4 platform ownership remains intact

After apply:
- validations must remain platform-owned, not hidden inside one strategy package
- semantic invariants must validate the shared engine/contract surfaces, not strategy-local duplicates
- runtime isolation validation must target platform runtime-unit identity/state boundaries, not ad hoc loop behavior only

### 9.5 no redesign is bundled

After apply, the package must prove guardrails around the existing frozen contracts without reopening:
- runtime control-plane design
- scheduler design
- research-result publication design
- registry/config model
- observability design
- strategy SDK design

### 9.6 exact acceptance condition

The package is accepted when the repo proves that the mandatory architecture-critical regressions listed in this spec are detectable and fail closed at platform level.

It is **not** required in the same package to:
- redesign all tests
- maximize code coverage
- create UI/reporting around test results
- migrate every legacy script

## 10. explicit out-of-scope list

Out of scope for this contract-freeze cycle:
- apply
- server proof
- strategy-local fixes
- runtime redesign
- observability redesign
- dashboard/UI
- broad test-suite rewrite
- generalized performance benchmarking
- broad data-quality framework design
- portfolio redesign
- distributed runtime supervision redesign
- execution-engine redesign
- research-result layer redesign
- registry/config redesign

## 11. blocker section

No real repo-backed blocker is evidenced for this spec-only cycle.

The target architecture already explicitly requires the needed guardrail categories and test layers, and the registry/config plus runtime-control-plane plus scheduler-loop specs already define the contract surfaces those guardrails must protect fileciteturn0file0 fileciteturn5file0 fileciteturn3file0 fileciteturn4file0

So the next architecture-completion package can be frozen now as one minimal mandatory guardrail and test-hardening layer.

## 12. short final statement

Freeze the next architecture-completion package as one minimal platform-owned mandatory guardrail/test layer that validates artifact contract integrity, no-lookahead safety, canonical backtest semantics invariants, time-ordering/monotonicity, and runtime state collision prevention without widening into redesign.