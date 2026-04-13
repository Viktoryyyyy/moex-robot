# Strategy SDK Spec v1

Status: FROZEN IMPLEMENTABLE CONTRACT  
Project: MOEX Bot  
Applies to: `src/moex_strategy_sdk/`, `src/strategies/<strategy_id>/`  
Authority: `docs/sot/MOEX_Bot_Target_Architecture_2026_All_In_One.md`

---

## 1. Scope

This document freezes the implementable Strategy SDK contract for MOEX Bot.

It defines:
- package shape
- required and optional files
- manifest/config/interface contracts
- artifact declaration contract
- validation and error model
- minimum acceptance criteria for the first reference strategy package

It does not redefine target architecture, runtime orchestration, or full registry design.

---

## 2. Strategy package shape

The only valid SDK v1 strategy package path is:

`src/strategies/<strategy_id>/`

`strategy_id` is the package identity, manifest identity, and registry identity.

### 2.1 Directory contract

Required shape:

- `src/strategies/<strategy_id>/manifest.py`
- `src/strategies/<strategy_id>/config.py`
- `src/strategies/<strategy_id>/signal_engine.py`
- `src/strategies/<strategy_id>/backtest_adapter.py`
- `src/strategies/<strategy_id>/live_adapter.py`
- `src/strategies/<strategy_id>/artifact_contracts.py`

Allowed optional files:

- `src/strategies/<strategy_id>/reports.py`
- `src/strategies/<strategy_id>/risk_policy.py`
- `src/strategies/<strategy_id>/feature_mapping.py`

SDK v1 forbids nested subpackages under one strategy package.

### 2.2 Naming contract

`strategy_id` must match regex:

`^[a-z][a-z0-9_]*$`

Rules:
- directory name must equal `strategy_id`
- `strategy_id` is immutable after first registration
- reserved names are forbidden:
  - `core`
  - `data`
  - `features`
  - `backtest`
  - `runtime`
  - `research`
  - `sdk`
  - `common`
  - `shared`

### 2.3 Import contract

Each required module must be importable by:

- `strategies.<strategy_id>.manifest`
- `strategies.<strategy_id>.config`
- `strategies.<strategy_id>.signal_engine`
- `strategies.<strategy_id>.backtest_adapter`
- `strategies.<strategy_id>.live_adapter`
- `strategies.<strategy_id>.artifact_contracts`

Import-time side effects are forbidden.

Import must not:
- read files
- read env
- read runtime/session/lock state
- perform network calls
- mutate persisted artifacts

### 2.4 Forbidden at package root

Forbidden:
- generic feed layer
- generic locking/session/logger framework
- standalone backtest engine
- runtime loops
- artifact autodiscovery helpers
- cross-strategy shared code
- hidden alternate entrypoints
- strategy-owned CLI entrypoints
- strategy-owned data files at package root

---

## 3. Required files

All six required files are mandatory even if one mode is unsupported.

If a mode is unsupported, the required adapter file still exists and exports the required symbol, but calling it must raise `UnsupportedModeError`.

### 3.1 `manifest.py`

Responsibility:
- declare immutable strategy identity, dependencies, and capabilities

Required exported symbol:
- `STRATEGY_MANIFEST`

Required type:
- `StrategyManifest`

Failure mode:
- missing file -> `StrategyRegistrationError`
- missing symbol -> `ManifestValidationError`
- malformed symbol/type -> `ManifestValidationError`

### 3.2 `config.py`

Responsibility:
- define typed config schema and validation entrypoint

Required exported symbols:
- `StrategyConfig`
- `validate_config`

Required contract:
- `StrategyConfig` is a subclass of `BaseStrategyConfig`
- `validate_config(raw_config: Mapping[str, object]) -> StrategyConfig`

Failure mode:
- missing file -> `StrategyRegistrationError`
- missing symbol -> `ConfigValidationError`
- malformed schema or validator signature -> `ConfigValidationError`

### 3.3 `signal_engine.py`

Responsibility:
- pure deterministic signal generation only

Required exported symbol:
- `generate_signals`

Required contract:
- `generate_signals(*, inputs: StrategyInputFrame, config: StrategyConfig) -> StrategySignalFrame`

Failure mode:
- missing file -> `StrategyRegistrationError`
- missing symbol -> `InterfaceValidationError`
- malformed signature or output contract -> `InterfaceValidationError`

### 3.4 `backtest_adapter.py`

Responsibility:
- map strategy outputs into canonical platform backtest request only

Required exported symbol:
- `build_backtest_request`

Required contract:
- `build_backtest_request(*, inputs: StrategyInputFrame, signals: StrategySignalFrame, config: StrategyConfig) -> BacktestAdapterRequest`

Failure mode:
- missing file -> `StrategyRegistrationError`
- missing symbol -> `InterfaceValidationError`
- malformed signature or normalized output -> `InterfaceValidationError`

### 3.5 `live_adapter.py`

Responsibility:
- map current strategy context into canonical platform runtime decision only

Required exported symbol:
- `build_live_decision`

Required contract:
- `build_live_decision(*, inputs: LiveStrategyInput, signals: StrategySignalFrame, config: StrategyConfig) -> LiveAdapterDecision`

Failure mode:
- missing file -> `StrategyRegistrationError`
- missing symbol -> `InterfaceValidationError`
- malformed signature or normalized output -> `InterfaceValidationError`

### 3.6 `artifact_contracts.py`

Responsibility:
- declare all strategy-visible artifacts

Required exported symbol:
- `ARTIFACT_CONTRACTS`

Required type:
- `tuple[ArtifactContract, ...]`

Failure mode:
- missing file -> `StrategyRegistrationError`
- missing symbol -> `ArtifactContractValidationError`
- malformed declaration -> `ArtifactContractValidationError`

---

## 4. Optional files

### 4.1 `reports.py`

Allowed purpose:
- strategy-specific mapping into shared report schema only

Allowed exported symbol:
- `build_report_payloads`

Allowed contract:
- `build_report_payloads(*, run_context: Mapping[str, object]) -> list[ReportPayload]`

Forbidden drift:
- standalone reporting pipeline
- custom report storage rules
- ad hoc summary as substitute for declared report artifacts

If present but malformed -> `InterfaceValidationError`

### 4.2 `risk_policy.py`

Allowed purpose:
- declarative strategy-local risk policy consumed by platform risk layer

Allowed exported symbol:
- `RISK_POLICY`

Allowed type:
- `StrategyRiskPolicy`

Forbidden drift:
- custom generic risk engine
- runtime gate implementation
- broker-side rejection logic

If present but malformed -> `InterfaceValidationError`

### 4.3 `feature_mapping.py`

Allowed purpose:
- declarative mapping between manifest-declared feature ids and strategy-local field names

Allowed exported symbol:
- `FEATURE_MAPPING`

Allowed type:
- `FeatureMapping`

Forbidden drift:
- feature building
- joins
- label generation
- fallback file discovery

If present but malformed -> `InterfaceValidationError`

---

## 5. Manifest contract

Required symbol:

`STRATEGY_MANIFEST: StrategyManifest`

### 5.1 Exact object shape

Mandatory fields:
- `strategy_id: str`
- `version: str`
- `instrument_scope: tuple[str, ...]`
- `timeframe: str`
- `required_datasets: tuple[str, ...]`
- `required_features: tuple[str, ...]`
- `required_labels: tuple[str, ...]`
- `supports_backtest: bool`
- `supports_live: bool`
- `report_schema_version: int`
- `artifact_contract_version: int`

Optional fields:
- `tags: tuple[str, ...] = ()`
- `owner: str | None = None`
- `default_portfolio_group: str | None = None`
- `default_risk_profile: str | None = None`

### 5.2 Mandatory field rules

- all mandatory fields must exist
- `strategy_id`, `version`, `timeframe` must be non-empty strings
- `instrument_scope`, `required_datasets`, `required_features`, `required_labels` must be tuples after validation
- `required_datasets` and `required_features` must be non-empty
- `required_labels` may be empty
- `supports_backtest` and `supports_live` may not both be `False`
- `report_schema_version >= 1`
- `artifact_contract_version >= 1`

### 5.3 Field semantics

- `strategy_id`: canonical strategy key
- `version`: strategy implementation version in semver form
- `instrument_scope`: explicit instrument ids only; wildcard forbidden
- `timeframe`: explicit decision timeframe id
- `required_datasets`: registry ids of datasets the platform must supply
- `required_features`: registry ids of features the platform must supply
- `required_labels`: registry ids of labels permitted only in contract-legal contexts
- `supports_backtest`: backtest capability flag
- `supports_live`: live capability flag
- `report_schema_version`: version of optional report payload schema
- `artifact_contract_version`: version of artifact declarations for this strategy package
- `tags`: non-semantic classification only
- `owner`: human/team owner metadata only
- `default_portfolio_group`: default registry key only
- `default_risk_profile`: default registry key only

### 5.4 Compatibility rules

- `strategy_id` must equal directory name
- `strategy_id` must equal registration key
- breaking change to manifest semantics requires major `version` bump
- breaking artifact change requires `artifact_contract_version` bump
- breaking report payload change requires `report_schema_version` bump
- duplicate ids in tuple fields are forbidden

### 5.5 Forbidden manifest patterns

Forbidden:
- paths instead of ids
- wildcard `instrument_scope`
- implicit latest dataset semantics
- both mode flags false
- empty dataset/feature dependencies
- version strings not matching `MAJOR.MINOR.PATCH`

---

## 6. Config contract

Required exported symbols:
- `StrategyConfig`
- `validate_config`

### 6.1 Schema expectations

`StrategyConfig` must be a typed schema class.

Rules:
- typed fields only
- every optional field has explicit default
- every bounded numeric field has explicit min/max
- cross-field invariants live in schema validation, not in adapters
- schema must be instantiable without filesystem or network access

### 6.2 Validator contract

`validate_config(raw_config: Mapping[str, object]) -> StrategyConfig`

Rules:
- single validator entrypoint
- raw config input is mapping-like only
- validator must return validated `StrategyConfig`
- validator may not mutate caller-owned config in place

### 6.3 Unknown field policy

Unknown fields are forbidden.

Unknown field -> `ConfigValidationError`

### 6.4 Parameter boundary rules

- bounds must be explicit in schema
- bound breach -> `ConfigValidationError`
- invalid enum/value set -> `ConfigValidationError`
- empty required string/list/tuple -> `ConfigValidationError`

### 6.5 Forbidden dependencies

Forbidden:
- filesystem autodiscovery
- env reads during validation
- session state
- current position
- lock state
- broker state
- network calls
- mutable server memory
- undeclared artifact lookup

### 6.6 Forbidden config behavior

Forbidden:
- hidden defaults outside schema
- auto-repair of invalid user config
- fallback to latest file
- dynamic parameter mutation at import time

---

## 7. Signal engine contract

Required exported symbol:
- `generate_signals`

Required contract:
- `generate_signals(*, inputs: StrategyInputFrame, config: StrategyConfig) -> StrategySignalFrame`

### 7.1 Input assumptions

- platform already materialized declared datasets/features
- `inputs` already passed platform-level schema validation
- rows are sorted ascending by decision input order
- labels are present only in contract-legal research/backtest contexts
- signal engine must not assume current broker/session/lock state

### 7.2 Required outputs

Required output columns:
- `instrument_id: str`
- `decision_ts: datetime`
- `desired_position: float`

Optional output columns:
- `signal_code: str`
- `signal_strength: float`
- `reason_code: str`

### 7.3 Output field semantics

- `instrument_id`: target instrument for the decision row
- `decision_ts`: timestamp at which the strategy decision becomes valid for downstream shared semantics
- `desired_position`: normalized target exposure from strategy logic only
- optional columns are metadata only and do not override shared execution semantics

### 7.4 Determinism and purity

- same `inputs` + same `config` => same output
- no side effects
- no mutation of input object
- no hidden randomness
- monotonic ascending `decision_ts`
- duplicate `(instrument_id, decision_ts)` forbidden

### 7.5 Forbidden responsibilities

Forbidden:
- file IO
- artifact discovery
- logging side effects
- lock/session logic
- runtime reconciliation
- cost/slippage/PnL
- fill rules
- forced close rules
- broker/exchange calls
- network calls unless separately approved by platform contract

---

## 8. Backtest adapter contract

Required exported symbol:
- `build_backtest_request`

Required contract:
- `build_backtest_request(*, inputs: StrategyInputFrame, signals: StrategySignalFrame, config: StrategyConfig) -> BacktestAdapterRequest`

### 8.1 Boundary with platform backtest

The adapter may only normalize strategy outputs into shared engine request.

Canonical ownership remains in platform backtest layer for:
- signal timestamp semantics
- execution delay semantics
- fills
- costs
- slippage
- reversal handling
- forced terminal close
- daily aggregation
- portfolio aggregation

### 8.2 Required normalized outputs

`BacktestAdapterRequest` must contain:
- `strategy_id: str`
- `strategy_version: str`
- `normalized_signals: StrategySignalFrame`
- `hook_overrides: Mapping[str, object]`

### 8.3 Allowed hooks

Allowed only if already supported by platform backtest contract.

Rules:
- hooks must be explicit key-value overrides in `hook_overrides`
- empty `hook_overrides` is valid
- unknown hook key -> `InterfaceValidationError`

### 8.4 Forbidden responsibilities

Forbidden:
- inline PnL engine
- custom cost model
- custom slippage model
- custom fill engine
- ad hoc dataset loading
- direct report writing
- direct artifact path selection

---

## 9. Live adapter contract

Required exported symbol:
- `build_live_decision`

Required contract:
- `build_live_decision(*, inputs: LiveStrategyInput, signals: StrategySignalFrame, config: StrategyConfig) -> LiveAdapterDecision`

### 9.1 Boundary with platform runtime

The adapter may only normalize current strategy context into shared runtime decision.

Canonical ownership remains in platform runtime layer for:
- orchestration
- scheduling
- locking
- shared state/session framework
- preflight
- risk gates
- telemetry
- notifier integration
- execution routing

### 9.2 Required normalized outputs

`LiveAdapterDecision` must contain:
- `strategy_id: str`
- `strategy_version: str`
- `instrument_id: str`
- `decision_ts: datetime`
- `desired_position: float`
- `reason_code: str`
- `supports_execution: bool`
- `state_patch: Mapping[str, object]`

### 9.3 Output field semantics

- `desired_position`: target exposure only
- `reason_code`: machine-readable explanation
- `supports_execution`: adapter-level allow/deny result before shared runtime action
- `state_patch`: strategy-local persisted state delta only for declared state artifacts; empty mapping allowed

### 9.4 Allowed hooks

Allowed:
- state patch for declared strategy-local state artifact only
- reason-code mapping
- runtime metadata fields only if platform runtime contract allows them

### 9.5 Forbidden responsibilities

Forbidden:
- own lock model
- own session store design
- own generic logger framework
- own scheduler/orchestrator
- own risk engine
- direct broker side effects
- direct exchange/network calls
- hidden persistence outside declared artifacts

---

## 10. Artifact contracts contract

Required exported symbol:
- `ARTIFACT_CONTRACTS`

Required type:
- `tuple[ArtifactContract, ...]`

### 10.1 Exact declaration model

Rules:
- one artifact = one `ArtifactContract`
- every persisted or consumed strategy-visible artifact must be declared
- undeclared artifact usage is forbidden

### 10.2 Required fields per artifact

Each `ArtifactContract` must contain:
- `artifact_id: str`
- `artifact_role: str`
- `contract_class: str`
- `producer: str`
- `consumers: tuple[str, ...]`
- `format: str`
- `schema_version: int`
- `partitioning_rule: str | None`
- `retention_policy: str | None`
- `locator_ref: str`

### 10.3 Allowed values

Allowed `artifact_role` values:
- `input`
- `output`
- `state`
- `report`

Allowed `contract_class` values:
- `repo_relative`
- `external_pattern`
- `cli_argument`
- `env_contract`

### 10.4 Field semantics

- `artifact_id`: unique within strategy package
- `producer`: canonical producer id
- `consumers`: explicit non-empty tuple of consumer ids
- `format`: declared serialization format
- `schema_version >= 1`
- `partitioning_rule`: explicit partition contract or `None`
- `retention_policy`: explicit retention note or `None`

`locator_ref` semantics by class:
- `repo_relative` -> repo-relative path
- `external_pattern` -> explicit path pattern
- `cli_argument` -> CLI argument name
- `env_contract` -> environment variable name

### 10.5 Required declarations

Every strategy must declare:
- all external inputs consumed by the strategy package
- all persisted outputs produced by adapters or reports
- all persisted strategy-local state
- all report artifacts if `reports.py` exists

### 10.6 Forbidden patterns

Forbidden:
- undeclared artifact
- generic glob autodetect
- silent fallback to latest file
- absolute server path hardcoded in strategy code
- stdout summary as sole formal result

---

## 11. Lifecycle hooks

SDK v1 does not require strategy-local lifecycle hooks beyond the fixed module entrypoints defined in this document.

Frozen v1 rule:
- no extra strategy-local lifecycle hook surface is allowed

Rationale:
- lifecycle ownership stays in SDK/platform
- first reference slice remains minimal
- hook expansion is deferred until after semantics freeze and reference implementation

---

## 12. Validation and error model

### 12.1 Registration validation

Registration must check:
- directory name valid and matches manifest `strategy_id`
- all six required files exist
- all six required modules import cleanly
- each required exported symbol exists
- each required symbol has required type/signature
- present optional files, if any, import cleanly
- present optional files, if any, expose the required symbol/type

### 12.2 Manifest validation

Must check:
- all mandatory fields present
- correct field types
- semver `version`
- non-empty `required_datasets`
- non-empty `required_features`
- explicit `instrument_scope`
- positive `report_schema_version`
- positive `artifact_contract_version`
- no duplicate ids in tuple fields
- no reserved names/patterns violations

### 12.3 Config validation

Must check:
- `StrategyConfig` instantiable
- `validate_config` returns `StrategyConfig`
- unknown fields rejected
- explicit bounds enforced
- explicit defaults applied only from schema
- cross-field invariants enforced
- no implicit filesystem/env/network dependencies

### 12.4 Interface validation

Must check:
- `generate_signals` signature exact
- `build_backtest_request` signature exact
- `build_live_decision` signature exact
- required output fields present in normalized outputs
- forbidden unknown hook keys rejected
- unsupported mode adapters raise `UnsupportedModeError`

### 12.5 Artifact validation

Must check:
- `ARTIFACT_CONTRACTS` exists and is tuple-like
- each `artifact_id` unique
- each `contract_class` allowed
- each `artifact_role` allowed
- each `schema_version >= 1`
- each `locator_ref` non-empty
- every persisted strategy artifact has declaration
- every declared consumer tuple non-empty

### 12.6 Fail-closed model

Rules:
- any required-file/import/type/signature/contract violation blocks registration
- any config violation blocks run start
- any artifact contract violation blocks run start
- any unsupported mode call blocks that mode
- warnings do not downgrade contract violations

### 12.7 Error classes

Frozen error classes:
- `StrategyRegistrationError`
- `StrategyIdMismatchError`
- `ManifestValidationError`
- `ConfigValidationError`
- `InterfaceValidationError`
- `ArtifactContractValidationError`
- `UnsupportedModeError`
- `ForbiddenResponsibilityError`

### 12.8 Blocking vs non-blocking behavior

Blocking:
- missing required file
- missing required symbol
- malformed required symbol
- present optional file malformed
- undeclared persisted artifact

Non-blocking:
- missing optional file

Mode-specific blocking:
- unsupported mode on registration is allowed only if adapter exists and raises `UnsupportedModeError`
- unsupported mode at runtime/backtest call blocks that mode only

---

## 13. Forbidden patterns

SDK v1 forbids:
- strategy-local standalone backtest engines
- strategy-local generic lock/session/logger frameworks
- hidden alternate runtime entrypoints
- file/path autodetect
- latest-file heuristics
- cross-strategy shared code under one strategy package
- feature building inside strategy package
- label building inside strategy package
- report artifacts without declaration
- state persistence outside `ARTIFACT_CONTRACTS`
- strategy math inside CLI loops
- strategy package becoming orchestration shell

---

## 14. Minimum acceptance criteria for first reference strategy package

### 14.1 Package existence

Must exist:
- one package at `src/strategies/<strategy_id>/`
- package name matches `strategy_id`
- only required files plus allowed optional files at package root

### 14.2 Contract validation

Must validate:
- all required modules import cleanly
- all required exported symbols exist with exact signatures/types
- `STRATEGY_MANIFEST` validates
- `StrategyConfig` and `validate_config` validate
- `generate_signals` returns contract-valid signal frame
- `build_backtest_request` returns contract-valid normalized backtest request
- `build_live_decision` returns contract-valid normalized live decision
- `ARTIFACT_CONTRACTS` fully declare all consumed/persisted artifacts

### 14.3 Behavior validation

Must hold:
- signal generation deterministic on repeated calls
- no import-time side effects
- no undeclared file/env/network access
- no strategy-owned generic infra
- no inline backtest semantics override
- no live orchestration logic inside strategy package

### 14.4 Minimum tests

Required minimum tests:
- registration success test
- invalid manifest rejection test
- invalid config rejection test
- artifact contract integrity test
- signal output schema test
- backtest adapter boundary test
- live adapter boundary test
- no-lookahead guard test
- runtime state collision prevention relevance test if state artifact exists

### 14.5 Must be absent

Must be absent:
- standalone runtime loop
- standalone PnL engine
- generic session/lock/logger/store framework
- hidden data discovery code
- undeclared artifacts
- cross-strategy helpers under package root

---

## 15. Final frozen statement

Strategy SDK v1 is frozen as follows:
- strategy package is the unit of extension
- generic backtest/runtime/infra remain outside strategy package
- required package shape is fixed
- manifest/config/interface/artifact contracts are explicit and fail-closed
- undeclared artifacts and implicit path behavior are forbidden
- first reference strategy package must satisfy the acceptance criteria in this document before broader migration continues
