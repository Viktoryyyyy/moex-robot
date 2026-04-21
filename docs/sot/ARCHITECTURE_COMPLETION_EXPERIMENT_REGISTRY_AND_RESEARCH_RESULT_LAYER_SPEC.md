# Architecture Completion — Experiment Registry and Research-Result Layer Spec

## 1. verdict

The correct first architecture-completion step on priority #1 is:

**freeze one minimal platform contract that makes every backtest/research run end in a standard research-result publication boundary plus one minimal experiment-registry write boundary**

This step is intentionally narrow.
It completes the first missing shared research/result layer after the registered backtest runner boundary is generalized.
It does not approve runtime/live/orchestration work, strategy-local migration, UI, or broad research-engine redesign.

## 2. repo-backed reason this is the next step

Current frozen architecture already makes this boundary explicit.

The target architecture freezes that:
- `moex_research` owns `runners`, `registry`, `metrics`, `publishers`, and `assets`
- the standard research path is `canonical dataset(s) -> canonical feature set(s) -> optional label set(s) -> strategy config -> single backtest engine -> common metrics/report publisher -> experiment registry write`
- each experiment must track `run id`, `strategy id`, `strategy version`, `dataset references`, `feature references`, `parameter set`, `metrics`, `artifact references`, and `run status`
- research runs must produce contract-declared outputs such as `run metadata`, `parameter snapshot`, `metrics table`, and `primary result table` fileciteturn0file0

So after the shared registered backtest runner boundary is generalized, the next missing platform-first completion step is not another strategy-local migration.
It is the minimal shared result-fixation and registry boundary that turns a completed research/backtest run into a reproducible, comparable platform result.

## 3. exact minimal boundary frozen in this spec

This spec freezes only the first shared research-result and experiment-registry boundary for backtest/research runs.

It freezes exactly:
1. one standard research-result path for backtest/research runs
2. one minimal experiment-registry record shape
3. platform ownership of the registry write boundary
4. the mandatory output artifacts/metadata that must exist for each run
5. the unchanged surfaces that this package must not reopen
6. the exact future apply-only acceptance boundary
7. the explicit out-of-scope list

## 4. exact minimal standard research-result path

The standard path for one backtest/research run is frozen as:

`registered run request -> registry/config resolution -> canonical dataset artifact resolution -> feature materialization -> strategy signal generation -> canonical backtest execution -> result publication -> experiment registry write`

### 4.1 meaning of result publication

Result publication is the first narrow platform-owned boundary that fixes a run into a reproducible research result.

It must do only the following:
- assign one canonical `run_id`
- materialize the mandatory result artifacts and metadata
- expose those artifact references to the experiment registry writer

It must not do:
- live/runtime orchestration
- dashboard/UI work
- strategy-specific interpretation logic
- portfolio redesign
- promotion workflow widening beyond a stored status field

### 4.2 minimal layer ownership

The minimal ownership split is frozen as:
- `moex_backtest` owns execution semantics and canonical backtest result production
- `moex_research` owns result publication, result fixation, and experiment-registry integration
- strategy packages remain owners only of strategy-local inputs, signal math, adapters, and declared artifact contracts

This means registry write does **not** belong inside a strategy package and does **not** belong inside runtime/live layers.

## 5. exact minimal experiment-registry fields required for backtest/research runs

The minimal registry record for one published backtest/research run must contain exactly these required fields:

### 5.1 run identity

- `run_id`
- `run_type`
- `run_status`
- `created_at_utc`

Frozen minimal rule:
- `run_type` in this first slice is limited to backtest/research runs only
- `run_status` must at minimum distinguish successful publication vs failed publication

### 5.2 strategy identity

- `strategy_id`
- `strategy_version`
- `strategy_config_id` or equivalent config record identifier

Frozen minimal rule:
- the registry must identify the exact registered strategy/config line that produced the result

### 5.3 input references

- `dataset_artifact_id`
- `dataset_artifact_path` or equivalent resolved dataset artifact reference
- `feature_set_id` or equivalent feature producer/config reference
- `label_set_id` if labels are used, otherwise explicit null/empty

Frozen minimal rule:
- the registry stores references, not copied datasets
- this first slice does not require full lineage graph modeling beyond these minimal references

### 5.4 parameter fixation

- `parameter_snapshot`

Frozen minimal rule:
- the effective parameter set used by the run must be fixed as published metadata
- it may be stored as structured JSON/object payload

### 5.5 result references

- `run_metadata_artifact_ref`
- `metrics_artifact_ref`
- `primary_result_artifact_ref`

Frozen minimal rule:
- the registry must point to the mandatory published outputs, not replace them

### 5.6 comparison-facing summary

- `summary_metrics`

Frozen minimal rule:
- this is the minimal registry-side comparison payload for later result comparison/filtering
- this field is a compact metrics subset, not the full result table

### 5.7 optional governance field allowed in first slice

- `verdict_status`

Frozen minimal rule:
- allowed values may remain minimal and implementation-defined in the apply package
- this spec only freezes that one stored verdict/promotability field is allowed
- no broader approval workflow is frozen here

## 6. where registry write belongs in platform terms

Registry write belongs to the **platform research-result publication boundary** under `moex_research`, after canonical backtest execution has completed and after mandatory result artifacts have been materialized.

### 6.1 exact platform placement rule

The write must be:
- platform-owned
- research-layer owned
- downstream of canonical result publication
- upstream of any later comparison/promotion consumer

The write must not be:
- inside strategy-local packages
- inside `moex_backtest` semantics code
- inside runtime/live/orchestrator code
- dependent on server-only memory or implicit latest-file discovery

### 6.2 why this placement is frozen

This placement matches the frozen target architecture where `moex_research` owns `registry`, `publishers`, and `assets`, while `moex_backtest` owns semantics and reports primitives only fileciteturn0file0

## 7. which artifacts and metadata become mandatory outputs

The first slice freezes the following mandatory published outputs for every backtest/research run.

### 7.1 mandatory artifact 1 — run metadata

One run metadata artifact must exist.

It must minimally contain:
- `run_id`
- `run_type`
- `strategy_id`
- `strategy_version`
- resolved dataset reference
- resolved feature reference
- label reference if any
- effective parameter snapshot
- execution timestamp metadata
- publication status metadata

### 7.2 mandatory artifact 2 — metrics table

One metrics artifact must exist.

It must contain the full metrics payload produced for the run.
This remains the detailed metrics source of truth.

### 7.3 mandatory artifact 3 — primary result table

One primary result artifact must exist.

Frozen minimal rule:
- this is the main result table needed to reproduce and inspect the run outcome
- exact table schema is not widened here, but the artifact must be explicit and contract-declared

### 7.4 mandatory registry-side summary payload

In addition to those artifacts, the registry record must store:
- one compact `summary_metrics` payload
- references to all three mandatory artifacts

### 7.5 optional artifacts not frozen as mandatory in this slice

Not mandatory in this first slice:
- joined dataset snapshot artifacts
- HTML/Markdown report artifacts
- charts/dashboard outputs
- portfolio aggregation outputs
- promotion notes or reviewer notes

Those may exist later, but they are not required for the first apply-only package.

## 8. what remains unchanged

The future apply-only package for this spec must keep the following frozen surfaces unchanged.

### 8.1 backtest semantics remain unchanged

Unchanged:
- canonical execution semantics
- signal timing semantics
- next-bar/open execution rules
- commission/slippage semantics already frozen
- forced close rules
- fail-closed validation behavior

This spec does not reopen `docs/sot/BACKTEST_EXECUTION_SEMANTICS_SPEC.md`.

### 8.2 strategy SDK remains unchanged

Unchanged:
- package shape
- strategy manifest/config/signal/backtest adapter contract
- artifact contract declaration model

This spec does not approve new strategy SDK hooks.

### 8.3 runtime/live/orchestration remains unchanged

Unchanged:
- runtime control plane
- live execution routing
- scheduler/orchestrator behavior
- runtime observability surfaces

This spec is backtest/research only.

### 8.4 registry/config model remains unchanged outside this narrow addition

Unchanged:
- existing registered strategy/config boundary
- existing dataset/feature/strategy/environment declarations
- existing artifact-root/path-resolution model

This first slice adds only the minimal experiment-registry and published-result boundary required for research/backtest comparability.

## 9. exact acceptance boundary for a future apply-only package

A later apply-only package is acceptable only if all points below are true.

### 9.1 minimal platform-first file scope

Expected narrow file scope:
- one new SoT-driven experiment registry implementation surface under research/platform code
- one new or updated research-result publication surface under research/platform code
- only the narrow runner integration needed to call that publication boundary
- only the narrow contract/config additions strictly required to declare the mandatory artifacts and registry location

No concrete strategy migration is required in the same package.

### 9.2 one standard published result path exists

After apply, one completed backtest/research run must end in one standard platform path that produces exactly:
- run metadata artifact
- metrics artifact
- primary result artifact
- experiment registry record referencing those artifacts

### 9.3 registry write is research-layer owned

After apply:
- registry write must occur from a platform research-layer boundary
- no strategy package may write directly to the registry
- no runtime/live/orchestrator surface may own this write

### 9.4 minimal registry record shape exists

After apply, a published record must contain the required fields frozen in section 5, including at minimum:
- run identity
- strategy identity
- input references
- parameter snapshot
- result artifact references
- summary metrics
- run status

### 9.5 mandatory outputs are contract-declared

After apply, the three mandatory outputs must be explicit declared artifacts.
Implicit stdout-only output is not acceptable.
Undeclared latest-file discovery is not acceptable.

### 9.6 comparison boundary becomes possible without redesign

After apply, two published runs with the same registry record shape must be comparable by reading:
- strategy identity
- input references
- parameter snapshot
- summary metrics
- artifact references

This package is accepted once that comparison boundary exists.
It is not required in the same cycle to build a dashboard, review workflow, or broad ranking engine.

### 9.7 no widening is allowed

The apply-only package must not widen into:
- runtime/live/orchestration
- observability redesign
- portfolio redesign
- UI/dashboard
- broad research engine rewrite
- migration of any concrete strategy
- strategy-local fixes that are not required for platform proof

## 10. explicit out-of-scope list

Out of scope for this contract-freeze cycle:
- apply
- server proof
- runtime/live/orchestration control plane
- observability redesign
- dashboard/UI
- portfolio layer redesign
- migration of any concrete strategy
- broad research engine redesign
- strategy-local result schemas beyond what is required for the standard primary result artifact
- reviewer workflow, approval workflow, or promotion workflow beyond one stored verdict field
- artifact retention redesign

## 11. blocker section

No real repo-backed blocker is evidenced for this spec-only cycle.

The frozen target architecture already declares:
- a dedicated `moex_research` layer
- a standard research path ending in experiment registry write
- a required experiment metadata set
- a required research artifact standard fileciteturn0file0

So the first architecture-completion package on priority #1 can be frozen now as one minimal platform-owned research-result publication and experiment-registry boundary.

## 12. short final statement

Freeze the first architecture-completion package as one minimal platform-owned research-result publication path plus one minimal experiment-registry record for backtest/research runs, so results become fixed, reproducible, and comparable without reopening semantics, SDK, or runtime.