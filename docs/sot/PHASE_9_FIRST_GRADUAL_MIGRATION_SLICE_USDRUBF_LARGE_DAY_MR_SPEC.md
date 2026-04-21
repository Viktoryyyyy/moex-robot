# PHASE 9 First Gradual Migration Slice — USDRUBF Large Day MR Spec

## 1. verdict

The correct first phase-9 gradual migration slice for `usdrubf_large_day_mr` is:

**one backtest-capable target-package migration of the already repo-backed USDRUBF large-day MR research line, without live enablement, portfolio runtime enablement, or orchestration widening**

More exactly:
- promote the current research-only USDRUBF large-day MR line into `src/strategies/usdrubf_large_day_mr/` under the frozen SDK v1 package contract
- keep the first slice backtest-capable only: `supports_backtest=True`, `supports_live=False`
- still provide all six SDK-required package files, with `live_adapter.py` present and fail-closed as unsupported in this slice
- move only the strategy-local deterministic line that is already repo-backed today: canonical USDRUBF 5m input -> daily day-pair feature frame -> strategy package signal/backtest contract
- do not widen into live runtime registration, live portfolio enablement, broker routing, notifier work, observability redesign, or experiment-registry work

This is the narrowest target-first slice because current main already proves the USDRUBF large-day MR line as a research substrate, but does not yet prove it as a registered strategy package.

## 2. repo proof

Repo proof is exact and narrow:

- `docs/sot/STRATEGY_SDK_SPEC.md` already freezes the only valid SDK v1 package shape as six required files under `src/strategies/<strategy_id>/`, and explicitly requires that unsupported modes still keep the required adapter file and fail closed via `UnsupportedModeError`.
- `docs/sot/REGISTRY_CONFIG_MODEL_SPEC.md` already freezes the registry-driven model: strategy discovery happens only through explicit instrument / dataset / feature / strategy / portfolio / environment records, and strategy packages may not bypass registry linkage with guessed paths.
- current main already contains one registered reference package line, `src/strategies/ema_3_19_15m/`, plus its strategy registry/default-config surfaces under `configs/strategies/`, which proves the current-main target package and registry pattern.
- `src/moex_core/contracts/registry_loader.py` already proves that current-main loading is registry-based and requires explicit strategy, dataset, and feature registration plus a portfolio binding; it does not load loose research scripts directly.
- `docs/sot/PATHS_INDEX.json` already declares two exact USDRUBF research artifacts that are directly relevant to this strategy line:
  - `research_usdrubf_5m_full_history` -> `data/master/usdrubf_5m_2022-04-26_2026-04-06.csv`
  - `research_usdrubf_large_day_mr_day_pairs` -> `data/research/usdrubf_large_day_mr_day_pairs.csv`
- `src/research/build_usdrubf_large_day_mr_day_pairs.py` already proves the current strategy-local data substrate and its no-lookahead daily pairing rule:
  - input is canonical USDRUBF 5m OHLC data
  - rows are aggregated into daily OHLC
  - each outcome `date=d` is paired only with the immediately previous completed trading day `source_trade_date=s`
  - output fields already include `prior_open`, `prior_high`, `prior_low`, `prior_close`, `prior_body_points`, `prior_abs_body_points`, `prior_range_points`, `prior_rel_range`, `prior_dir`, `outcome_open`, `outcome_close`, `outcome_oc_points`, `mr_outcome_points`, and `MR_EDGE_DAY`
- `configs/portfolios/reference_ema_3_19_15m_single.json` still proves that current registered portfolio scope is the EMA reference line only, so any direct live enablement of `usdrubf_large_day_mr` would be extra scope rather than part of the smallest correct first migration slice.

So current main already proves five important facts:

1. the USDRUBF large-day MR line exists in repo today only as a research substrate, not as a registered strategy package
2. the deterministic no-lookahead daily source/outcome pairing logic already exists and is the correct migration source
3. current-main package, registry, and loader contracts already exist and must be reused rather than redesigned
4. a first slice can be backtest-capable without widening into live runtime registration
5. the smallest correct move is package migration of the existing research line, not strategy runtime activation

## 3. exact migration boundary for `usdrubf_large_day_mr`

Frozen migration boundary for the first phase-9 slice:

- packageize the current repo-backed USDRUBF large-day MR line into one target strategy package
- keep the first slice limited to the already evidenced daily day-pair substrate
- keep the slice backtest-capable only under the current registry model
- keep live mode unsupported in this slice even though the required `live_adapter.py` file exists
- keep the current day-pair logic no-lookahead and prior-day anchored exactly as already evidenced by `source_trade_date < date`
- keep the strategy package responsible only for strategy-local config, signal generation, backtest normalization, live-mode fail-closed adapter presence, and artifact declarations
- keep generic feature materialization outside the strategy package
- keep generic registration / config / environment resolution in the existing platform registry model

This first slice does **not** freeze or approve:
- a live-trading promotion of the strategy
- direct portfolio enablement
- broker routing
- notifier integration
- new observability surfaces
- experiment-registry writes
- a broad rewrite of USDRUBF research

## 4. exact current repo source surfaces in scope

In scope only:

- `docs/sot/STRATEGY_SDK_SPEC.md`
- `docs/sot/REGISTRY_CONFIG_MODEL_SPEC.md`
- `docs/sot/PATHS_INDEX.json`
- `src/research/build_usdrubf_large_day_mr_day_pairs.py`
- `src/moex_strategy_sdk/manifest.py`
- `src/moex_strategy_sdk/config_schema.py`
- `src/moex_strategy_sdk/interfaces.py`
- `src/moex_strategy_sdk/artifact_contracts.py`
- `src/moex_core/contracts/registry_loader.py`
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/backtest_adapter.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/strategies/ema_3_19_15m.default.json`
- `configs/datasets/si_fo_5m_intraday.json`
- `configs/features/si_15m_ohlc_from_5m.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/instruments/si.json`

Not in scope:

- any broad audit outside the exact USDRUBF large-day MR line
- any EMA strategy redesign
- any `mr1` revival work
- any observability / runtime-report widening
- any experiment-registry work
- any server-apply work

## 5. exact target destination in target architecture terms

The first slice must land only at the target architecture surfaces below:

### 5.1 strategy package destination

- `src/strategies/usdrubf_large_day_mr/manifest.py`
- `src/strategies/usdrubf_large_day_mr/config.py`
- `src/strategies/usdrubf_large_day_mr/signal_engine.py`
- `src/strategies/usdrubf_large_day_mr/backtest_adapter.py`
- `src/strategies/usdrubf_large_day_mr/live_adapter.py`
- `src/strategies/usdrubf_large_day_mr/artifact_contracts.py`

No `reports.py` is required in this first slice.

### 5.2 minimal registry / config destination

Because current-main loading is registry-driven, the first slice also requires only the smallest additional registry/config surfaces needed to make the package loadable in backtest mode:

- one strategy registry record under `configs/strategies/`
- one default strategy config under `configs/strategies/`
- one dataset registry record under `configs/datasets/` bound to the already-declared canonical USDRUBF full-history input
- one feature registry record under `configs/features/` bound to the already-declared daily day-pair artifact
- one dedicated backtest-only portfolio record under `configs/portfolios/` containing exactly `usdrubf_large_day_mr`

### 5.3 minimal feature-layer destination

Because SDK v1 forbids feature building inside the strategy package, the existing day-pair builder logic must be promoted out of research-only positioning into one platform feature-builder destination:

- one narrow daily feature materializer under `src/moex_features/daily/` for the current USDRUBF large-day MR day-pair frame

That feature materializer becomes the platform-owned producer for the day-pair feature frame consumed by the strategy package.

## 6. required package shape

The required package shape for this first slice is frozen exactly as:

### 6.1 `manifest.py`

Must export `STRATEGY_MANIFEST` with:
- `strategy_id = "usdrubf_large_day_mr"`
- `supports_backtest = True`
- `supports_live = False`
- `instrument_scope = ("usdrubf",)`
- `timeframe = "1d"`
- `required_datasets = ("research_usdrubf_5m_full_history",)`
- `required_features = ("research_usdrubf_large_day_mr_day_pairs",)`
- `required_labels = ()`
- `artifact_contract_version >= 1`
- `report_schema_version >= 1`

### 6.2 `config.py`

Must define typed config only for strategy-local rule parameters that are expressible from the already repo-backed day-pair fields.

Allowed config domain in this first slice:
- `instrument_id`
- `timeframe`
- prior-day direction filter
- prior-day magnitude / range threshold parameters derived from existing day-pair columns
- holding-window semantics limited to the already evidenced single outcome day attached to `date`

Forbidden in this first slice:
- config fields that depend on undeclared external files
- config fields that depend on live broker/runtime state
- hidden threshold inference from research notebooks or server memory

### 6.3 `signal_engine.py`

Must consume only the materialized day-pair feature frame and emit deterministic normalized strategy signals.

Frozen signal-engine boundary:
- no file IO
- no feature building
- no path discovery
- no runtime state reads
- no network calls
- no PnL / fill / execution logic

The signal engine may use only the already repo-backed day-pair inputs and explicit config.

### 6.4 `backtest_adapter.py`

Must map the daily strategy output into the current canonical backtest request contract only.

Frozen backtest-adapter rule:
- no custom backtest engine
- no custom cost model
- no custom fill model
- no shared backtest semantics override in this first slice

### 6.5 `live_adapter.py`

Must exist because SDK v1 requires all six files.

Frozen first-slice rule:
- `live_adapter.py` exports the required symbol
- because `supports_live=False`, calling the live adapter must fail closed as unsupported in this slice
- no runtime registration or live portfolio enablement is approved here

### 6.6 `artifact_contracts.py`

Must declare all strategy-visible artifacts for this package.

Mandatory first-slice declarations:
- input dataset contract for canonical USDRUBF 5m history
- input feature contract for the USDRUBF large-day MR day-pair frame
- backtest output contract
- one reserved strategy-local state contract only if needed for future live promotion; otherwise omit state from first apply slice

### 6.7 optional `reports.py`

`reports.py` is explicitly **not required** in this first slice because no strategy-specific report surface is evidenced as necessary.

## 7. minimal platform touchpoints strictly required for this slice

Only the following non-package touchpoints are strictly required:

1. one dataset registry record bound to the already declared canonical USDRUBF 5m full-history artifact
2. one feature-builder surface under `src/moex_features/daily/` that materializes the already existing day-pair frame now produced by the research script
3. one feature registry record bound to that day-pair artifact
4. one strategy registry record and one default strategy config for `usdrubf_large_day_mr`
5. one dedicated backtest-only portfolio record so the package can load through the current registry loader without widening the existing EMA portfolio

Everything else is explicitly not required for this first slice.

So this slice must **not** touch:
- runtime orchestrator
- runtime boundary reuse model
- live environment configs
- broker adapters
- notifier integrations
- portfolio runtime reports
- experiment-registry surfaces
- multi-strategy live portfolio orchestration

## 8. artifact / config contract model for inputs, outputs, and state

### 8.1 input dataset contract

Frozen input dataset identity:
- dataset id: `research_usdrubf_5m_full_history`
- source artifact key already evidenced in `docs/sot/PATHS_INDEX.json`
- canonical locator: `data/master/usdrubf_5m_2022-04-26_2026-04-06.csv`
- format: `csv`
- contract class: `repo_relative`
- producer lineage: `src/api/futures/fo_5m_period_paged.py`
- usage mode in this first slice: backtest/research only

### 8.2 input feature contract

Frozen input feature identity:
- feature set id: `research_usdrubf_large_day_mr_day_pairs`
- source artifact key already evidenced in `docs/sot/PATHS_INDEX.json`
- canonical locator: `data/research/usdrubf_large_day_mr_day_pairs.csv`
- format: `csv`
- contract class: `repo_relative`
- producer destination for the migrated slice: one narrow feature materializer in `src/moex_features/daily/`
- row semantics: one row per outcome `date`, paired only to the immediately previous completed trading day `source_trade_date`
- lookahead rule: `source_trade_date` must remain strictly earlier than `date`

Frozen minimum feature payload for the first slice must remain based on the currently repo-backed columns already emitted by the research builder:
- `date`
- `source_trade_date`
- `prior_open`
- `prior_high`
- `prior_low`
- `prior_close`
- `prior_body_points`
- `prior_abs_body_points`
- `prior_range_points`
- `prior_rel_range`
- `prior_dir`
- `outcome_open`
- `outcome_close`
- `outcome_oc_points`
- `mr_outcome_points`
- `MR_EDGE_DAY`

No hidden extra fields may be assumed in the first slice.

### 8.3 backtest output contract

Frozen first-slice backtest output:
- one strategy backtest output artifact declared in `artifact_contracts.py`
- format may remain `csv`
- locator must be explicit, pattern-based, and strategy-scoped
- producer remains platform backtest layer, not the strategy package

Allowed role:
- backtest day metrics / verdict-support output only

Forbidden in this slice:
- strategy-owned standalone report pipeline
- strategy-owned results autodiscovery

### 8.4 state contract

State handling rule for the first slice:
- because `supports_live=False`, no active runtime state model is required for the first apply-only cycle
- if a reserved future state contract is declared, it must remain purely declarative and must not trigger live registration or runtime enablement
- no hidden strategy-local state outside declared artifact contracts is allowed

### 8.5 config binding model

The first slice must keep config explicit and typed:
- one strategy registry record
- one default strategy config
- no guessed file binding
- no latest-file heuristics
- no server-memory thresholds
- no undeclared environment dependence

## 9. explicit out-of-scope list

Out of scope for this first phase-9 slice:

- code apply beyond the exact first migration slice
- server apply
- live enablement of `usdrubf_large_day_mr`
- adding `usdrubf_large_day_mr` to any live portfolio
- multi-strategy runtime orchestration changes
- broker routing
- notifier integration
- risk-engine redesign
- runtime observability redesign
- experiment-registry integration
- research-wide rewrite of the USDRUBF branch
- renaming or redefining the frozen SDK contract
- renaming or redefining the frozen registry/config model
- migrating any other strategy
- broad roadmap rewrite

## 10. acceptance boundary for later apply-only cycle

Later apply is acceptable only if all points below are true:

- `src/strategies/usdrubf_large_day_mr/manifest.py` exists
- `src/strategies/usdrubf_large_day_mr/config.py` exists
- `src/strategies/usdrubf_large_day_mr/signal_engine.py` exists
- `src/strategies/usdrubf_large_day_mr/backtest_adapter.py` exists
- `src/strategies/usdrubf_large_day_mr/live_adapter.py` exists
- `src/strategies/usdrubf_large_day_mr/artifact_contracts.py` exists
- no `reports.py` is added unless a later repo-backed need is proven
- manifest validates under the frozen SDK v1 rules
- manifest declares `strategy_id = "usdrubf_large_day_mr"`
- manifest declares `supports_backtest = True`
- manifest declares `supports_live = False`
- manifest depends only on the explicit USDRUBF dataset/feature ids frozen in this spec
- `live_adapter.py` exports the required symbol and fails closed as unsupported
- `signal_engine.py` remains pure and uses only explicit config plus the materialized day-pair input frame
- `backtest_adapter.py` returns the canonical `BacktestAdapterRequest` and does not introduce custom backtest semantics
- `artifact_contracts.py` declares the required input/output/state surface without undeclared artifacts
- one strategy registry record exists under `configs/strategies/`
- one default strategy config exists under `configs/strategies/`
- one dataset registry record exists under `configs/datasets/`
- one feature registry record exists under `configs/features/`
- one dedicated backtest-only portfolio record exists under `configs/portfolios/`
- one narrow feature materializer exists under `src/moex_features/daily/` for the current day-pair frame
- the existing research script is either retained as a thin compatibility wrapper or clearly superseded, but the feature-building responsibility is no longer treated as strategy-package logic
- no live environment config is widened
- no runtime orchestrator file is widened
- no notifier / observability / experiment-registry / broker surface is widened
- no server proof is claimed in that apply-only cycle unless separately owner-run later

## 11. blockers if any

No blocker is evidenced for this contract-freeze cycle.

Current main already contains:
- the frozen SDK/package contract
- the frozen registry/config model
- one working reference strategy package line
- the canonical USDRUBF research input artifact contract
- the canonical USDRUBF day-pair research artifact contract
- the exact research builder that proves the current no-lookahead source/outcome pairing logic

So the first phase-9 slice is spec-supportable now without reopening broader architecture.

## 12. one sentence final scope statement

Freeze phase-9 first gradual migration for `usdrubf_large_day_mr` as one backtest-capable target-package promotion of the already repo-backed USDRUBF daily day-pair research line into the frozen strategy SDK and registry model, while keeping live mode unsupported and leaving runtime/orchestration surfaces untouched.