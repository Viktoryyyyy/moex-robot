# REGISTRY / CONFIG MODEL SPEC

Status: FROZEN IMPLEMENTABLE CONTRACT  
Project: MOEX Bot  
Applies to: platform registries, config loading, strategy discovery, backtest/runtime dependency resolution

---

## 1. Scope

This spec freezes:
- registry model
- config scopes and layering
- strategy / dataset / feature / portfolio / environment registration contracts
- artifact linkage model
- validation and fail-closed rules

This spec does not freeze:
- SDK base class implementation
- runtime orchestration flow
- reference strategy vertical slice design

---

## 2. Canonical model

The platform is registry-driven.

Canonical registries live under:
- `configs/instruments/`
- `configs/datasets/`
- `configs/features/`
- `configs/strategies/`
- `configs/portfolios/`
- `configs/environments/`

Each registry record is canonical configuration state.
Derived runtime plans, resolved dependency graphs, effective configs, and discovered artifact paths are derived state.

One registry record = one id.
Cross-registry references must use explicit ids, not guessed paths.

---

## 3. Required registries

### 3.1 Instrument registry

Purpose:
- canonical instrument identity and market semantics binding
- shared dependency anchor for datasets, features, and strategies

Ownership boundary:
- owned by platform / core config layer
- not owned by individual strategies

Canonical fields:
- `instrument_id: str`
- `venue: str`
- `market: str`
- `board: str`
- `symbol: str`
- `instrument_type: str`
- `timezone: str`
- `trade_calendar_id: str`
- `tick_size: number | null`
- `lot_size: number | null`
- `price_scale: int | null`
- `is_active: bool`

Derived only:
- resolved current contract mapping
- live feed handles
- broker/runtime handles

### 3.2 Dataset registry

Purpose:
- canonical declaration of loadable datasets and their contracts

Ownership boundary:
- owned by platform data layer
- producers are declared, but producer implementation is outside registry

Canonical fields:
- `dataset_id: str`
- `version: str`
- `instrument_ids: list[str]`
- `granularity: str`
- `session_semantics: str`
- `timezone: str`
- `schema_ref: str`
- `partitioning: str`
- `artifact_ref: str`
- `producer_ref: str`
- `consumer_tags: list[str]`
- `is_research_allowed: bool`
- `is_runtime_allowed: bool`
- `status: str`

Canonical vs derived:
- canonical: dataset id, version, schema ref, artifact ref, producer ref, partitioning
- derived: discovered concrete files for a run, materialized partitions, freshness state

### 3.3 Feature registry

Purpose:
- canonical declaration of feature sets and label-like derived inputs used by strategies

Ownership boundary:
- owned by platform feature layer
- strategies may consume; they do not own generic feature set definitions

Canonical fields:
- `feature_set_id: str`
- `version: str`
- `input_dataset_ids: list[str]`
- `schema_ref: str`
- `partitioning: str`
- `artifact_ref: str`
- `producer_ref: str`
- `row_semantics: str`
- `lookahead_safe: bool`
- `applicability: list[str]`
- `status: str`

Derived only:
- resolved input partitions
- computed feature materializations
- freshness / completeness state

### 3.4 Strategy registry

Purpose:
- canonical strategy discovery and dependency declaration

Ownership boundary:
- owned by strategy package plus platform registration entry
- strategy-local manifest/config/artifact contracts remain inside strategy package
- registry owns discoverability and cross-platform linkage

Canonical fields:
- `strategy_id: str`
- `version: str`
- `package_ref: str`
- `manifest_ref: str`
- `config_schema_ref: str`
- `default_config_ref: str`
- `artifact_contract_ref: str`
- `required_dataset_ids: list[str]`
- `required_feature_set_ids: list[str]`
- `required_label_set_ids: list[str]`
- `instrument_scope: list[str]`
- `timeframe: str`
- `supports_backtest: bool`
- `supports_live: bool`
- `report_schema_version: str`
- `artifact_contract_version: str`
- `status: str`

Canonical vs derived:
- canonical: strategy identity, package linkage, manifest/config/artifact references, dependency ids, capability flags
- derived: resolved effective config, instantiated adapters, run plan

### 3.5 Portfolio registry

Purpose:
- canonical grouping of enabled strategies and portfolio-level allocation/risk references

Ownership boundary:
- owned by platform portfolio layer
- strategies cannot self-assign portfolio membership at runtime

Canonical fields:
- `portfolio_id: str`
- `version: str`
- `enabled_strategy_ids: list[str]`
- `instrument_scope: list[str]`
- `allocation_policy_ref: str`
- `risk_policy_ref: str`
- `execution_policy_ref: str | null`
- `reconciliation_policy_ref: str | null`
- `is_live_allowed: bool`
- `status: str`

Derived only:
- current portfolio positions
- realized pnl
- runtime state

### 3.6 Environment registry

Purpose:
- canonical non-secret environment binding for research/runtime execution

Ownership boundary:
- owned by platform environment layer
- secrets stay outside registry

Canonical fields:
- `environment_id: str`
- `mode: str`
- `is_research: bool`
- `is_backtest: bool`
- `is_live: bool`
- `broker_adapter_ref: str | null`
- `market_data_adapter_ref: str | null`
- `calendar_source_ref: str`
- `artifact_root_refs: list[str]`
- `allowed_override_keys: list[str]`
- `required_env_vars: list[str]`
- `status: str`

Allowed env-bound values:
- adapter ids
- calendar source ids
- artifact root ids
- names of required environment variables
- mode/capability flags

Forbidden in environment registry:
- secret values
- tokens
- passwords
- broker credentials
- raw absolute paths not wrapped by an artifact/env contract

---

## 4. Config model

### 4.1 Config scopes

Allowed config scopes only:
- instrument scope
- dataset scope
- feature scope
- strategy scope
- portfolio scope
- environment scope
- explicit invocation override scope

No hidden extra scope is allowed.

### 4.2 Config layering

Effective config is built in this order:
1. instrument registry record
2. dataset / feature registry records
3. strategy default config
4. portfolio binding layer
5. environment layer
6. explicit invocation overrides

Later layers may override earlier layers only when the field is declared overrideable by schema.

### 4.3 Override rules

May be overridden only if typed schema marks field as overrideable.
Must not be overridden by later layers:
- ids
- versions
- package refs
- manifest refs
- schema refs
- artifact contract refs
- artifact classes
- support flags
- lookahead safety flags
- session/time semantics

Unknown override key = hard error.
Type mismatch on override = hard error.
Override of non-overrideable field = hard error.

### 4.4 Required typed fields

Every typed config schema must define at minimum:
- field name
- type
- required/optional
- default if optional
- overrideable: bool
- validation rule set

Strategy config must additionally define:
- `strategy_id`
- `version`
- `params`
- `artifact_bindings`
- `runtime_policy_ref | null`
- `risk_policy_ref | null`

### 4.5 Forbidden hidden dependencies

Forbidden:
- reading undeclared files from current working directory
- inferring dependencies from filenames or latest matching glob
- importing live values from mutable server state into config resolution
- strategy-local autodetect of datasets/features/artifacts outside registry links
- secret lookup from repo config files

---

## 5. Strategy registration contract

A strategy becomes discoverable only when all of the following exist and validate together:
- strategy package under `src/strategies/<strategy_id>/`
- package `manifest.py`
- package `config.py`
- package `artifact_contracts.py`
- strategy registry record under `configs/strategies/`

Required registry links:
- `package_ref` -> strategy package
- `manifest_ref` -> `manifest.py`
- `config_schema_ref` -> typed config schema in `config.py`
- `default_config_ref` -> default strategy config record
- `artifact_contract_ref` -> `artifact_contracts.py`
- `required_dataset_ids`
- `required_feature_set_ids`

Validation rules:
- `strategy_id` in registry must equal `manifest.strategy_id`
- registry `version` must equal manifest version
- required dataset/feature ids must exist and be active
- `supports_live=true` is invalid when live adapter contract is missing
- `supports_backtest=true` is invalid when backtest adapter contract is missing
- manifest capability flags and registry capability flags must match
- artifact contract version must match registry declaration

Failure modes:
- missing package link -> registration-time hard error
- missing dependency id -> registration-time hard error
- manifest/registry mismatch -> registration-time hard error
- inactive dependency -> load-time hard error
- unsupported runtime mode -> load-time hard error

---

## 6. Dataset / feature registration contract

### 6.1 IDs

- ids are stable, lowercase, snake_case
- ids are immutable once published
- breaking change requires new `version`
- dataset id and feature_set id are independent namespaces

### 6.2 Schema / version expectations

- every dataset and feature set must reference one explicit schema contract
- schema-compatible additive change may keep id and bump version
- schema-breaking change requires version bump and explicit migration
- consumer must bind by id + version expectation, never by guessed latest artifact

### 6.3 Partitioning expectations

Each registered dataset / feature set must declare one partitioning rule exactly:
- `unpartitioned`
- `by_trade_date`
- `by_session_date`
- `by_instrument`
- `by_instrument_trade_date`
- `custom:<declared_rule_id>`

Undeclared partitioning is forbidden.

### 6.4 Artifact linkage

Each dataset / feature registry record must link to exactly one artifact contract by `artifact_ref`.
That contract must declare:
- artifact id
- contract class
- format
- partition rule
- producer
- consumer class
- schema version

### 6.5 Producer / consumer semantics

Producer semantics must state which component materializes the artifact.
Consumer semantics must state whether the artifact is valid for:
- research only
- backtest only
- runtime only
- multi-mode

A runtime consumer may not consume an artifact marked research-only.

---

## 7. Portfolio registration contract

Canonical fields that define runtime eligibility:
- `portfolio_id`
- `enabled_strategy_ids`
- `allocation_policy_ref`
- `risk_policy_ref`
- `is_live_allowed`

Rules:
- every enabled strategy id must exist in strategy registry
- strategy instrument scope must be compatible with portfolio instrument scope
- live runtime is forbidden when `is_live_allowed=false`
- allocation/risk refs must resolve before any run starts
- portfolio cannot enable a strategy whose registry status is not active

Portfolio registry does not own:
- current positions
- runtime locks
- session state
- realized pnl artifacts

Those are runtime-derived artifacts only.

---

## 8. Environment registry contract

Environment registry is the canonical non-secret execution context.

Rules:
- `environment_id` is stable and unique
- one record may be research-only, backtest-only, live-only, or mixed if explicitly allowed
- live environment must declare required external adapters and required env var names
- environment may bind only declared artifact roots and adapter ids
- environment may not carry raw secrets

Allowed secret handling pattern only:
- registry declares `required_env_vars`
- secret values are loaded from process environment at load time
- absence of a required secret in a mode that needs it = hard error

Forbidden secret handling patterns:
- secret literal in registry file
- secret literal in strategy config default
- secret embedded in artifact path
- fallback to local shell memory or undeclared `.env` field name

---

## 9. Artifact linkage model

Registries reference artifacts only through explicit artifact contracts.

Allowed artifact classes only:
- `repo_relative`
- `external_pattern`
- `cli_argument`
- `env_contract`

Each registry-level artifact reference must resolve to a contract that defines:
- `artifact_id`
- `class`
- `format`
- `producer_ref`
- `consumer_refs`
- `partitioning`
- `schema_version | null`

Binding rule:
- registry/config may bind to `artifact_id` or `artifact_ref`
- concrete path resolution must be delegated to the artifact contract
- no registry/config consumer may guess a file path from naming conventions

Forbidden:
- direct hardcoded absolute server path in registry record
- implicit latest-file scan
- missing artifact class
- multiple artifact classes for one artifact id

---

## 10. Validation / error model

### 10.1 Registration-time checks

Required checks:
- id uniqueness within each registry
- required field presence
- type validation
- cross-reference existence
- strategy manifest / registry consistency
- artifact contract existence
- schema ref existence
- capability flag consistency

### 10.2 Load-time checks

Required checks:
- requested ids resolve to active records
- requested mode is allowed by environment + strategy + portfolio
- required datasets/features are present and mode-compatible
- required artifact contracts resolve
- required env var names are declared and values exist when needed
- no override violation

### 10.3 Fail-closed cases

Must fail closed on:
- unresolved registry id
- unresolved artifact contract
- missing schema ref
- missing required env var
- inactive dependency
- strategy/package/manifest mismatch
- live request against non-live strategy, portfolio, or environment
- research/backtest/runtime mode mismatch
- undeclared override key

### 10.4 Conflict detection

Hard conflict:
- same id with incompatible duplicated record
- same strategy enabled multiple times in one portfolio without explicit portfolio rule support
- same artifact id bound to different contract classes
- incompatible versions of the same dependency inside one effective run plan
- portfolio/environment mode conflict

---

## 11. Forbidden patterns

Forbidden in the frozen model:
- path guessing
- hidden cwd-based file IO dependencies
- direct strategy discovery by filesystem scan without strategy registry entry
- dataset/feature consumption without registry record
- runtime eligibility inferred from script name or CLI module name
- live secret values stored in repo configs
- silent fallback from missing specific artifact to generic latest artifact
- undeclared mutable server state participating in config resolution

---

## 12. Minimum acceptance criteria

### 12.1 First reference strategy vertical slice

Must pass all of the following:
- one strategy package exists in `src/strategies/<strategy_id>/`
- one strategy registry record exists and validates
- linked manifest/config/artifact contracts resolve
- all required dataset/feature ids resolve
- no hidden path dependency is needed to instantiate effective config

### 12.2 First registry-backed backtest run

Given `strategy_id`, `portfolio_id`, and `environment_id`, the platform must:
- resolve one effective config from registries + allowed overrides only
- resolve required datasets/features by explicit ids
- resolve artifacts by artifact contracts, not path guessing
- reject the run if any dependency is missing or mode-incompatible
- produce a traceable run record with resolved ids/versions

### 12.3 First registry-backed live runtime boundary

Given `strategy_id`, `portfolio_id`, and live `environment_id`, the platform must:
- reject startup if strategy, portfolio, or environment is not live-eligible
- reject startup if any required secret env var is missing
- reject startup if any required artifact contract is unresolved
- start only from declared ids, declared artifact contracts, and typed config
- require no undeclared absolute path and no hidden dependency

---

## 13. Final frozen statement

The registry/config model is frozen as:
- six canonical registries
- typed multi-scope config with explicit layering
- strategy discovery only through registry + package linkage
- dataset/feature usage only through registered ids and schema/artifact contracts
- portfolio/environment eligibility enforced before run start
- artifact resolution only through declared artifact contracts
- fail-closed validation with explicit conflict detection
