# MOEX Bot Target Architecture 2026 All In One

context_ref: MOEX_Bot_Target_Architecture_2026_All_In_One
status: frozen_static_context_ref
source_level: architectural_target_and_operating_canon

## Canonical source summary

This file is the Route B resolvable static context reference for the frozen MOEX Bot target architecture.

## Non-negotiable canon

- GitHub / repo is Source of Truth.
- Server is Applied State only.
- Server filesystem is not architectural proof.
- Architectural conclusions must come from repo/contracts.
- Artifact paths must be explicit contracts, not implicit discovery.
- Strategy, research, backtest, runtime, data, and config boundaries are contract-first.
- Target architecture is package/workspace-first, strategy-pluggable, registry-driven, reproducible, observable, and migration-friendly.

## Target layers

- `moex_core`: domain primitives, calendars, shared contracts.
- `moex_data`: ISS/API clients, normalization, dataset builders, data quality.
- `moex_features`: feature and label builders, anti-leakage validation.
- `moex_backtest`: one canonical backtest semantics layer.
- `moex_runtime`: orchestration, state, locks, risk, telemetry, preflight.
- `moex_research`: research runners, metrics, registry integration, publishers.
- `moex_strategy_sdk`: strategy interfaces and artifact contract base.
- `strategies/<strategy_id>`: strategy-specific config, signal logic, adapters, artifact contracts.

## Artifact contract rule

Each input/output artifact must declare exactly one contract class:

- `repo_relative`
- `external_pattern`
- `cli_argument`
- `env_contract`

Forbidden patterns:

- generic glob autodetect without registry binding
- silent latest-file fallback
- hardcoded absolute server paths inside research/runtime logic
- stdout-only result substitution

## Route B relevance

Route B must preserve this target architecture by routing work through repo-first context refs and evidence packages. n8n orchestration may move task packages and evidence, but GitHub remains the architectural Source of Truth.
