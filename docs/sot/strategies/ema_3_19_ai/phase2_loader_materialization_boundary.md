# EMA 3/19 AI — Phase 2 Loader and Materialization Boundary

Status: design-only boundary  
Lane: `ema_3_19_ai`  
Runtime authorized: no  
Server apply authorized: no  
Registry mutation authorized: no  
Source/provider mutation authorized: no  

## 1. Purpose

This document prevents Phase 2.3 broad contract artifacts from being interpreted as runtime authorization.

The Phase 2.3 package defines only broad design contracts. It does not implement loaders, data transforms, feature materialization, registries, tests, source ingestion, or server deployment.

## 2. No loaders authorized

No loader is authorized by this package.

Forbidden:

- adding or modifying loader code;
- creating runtime loader configuration;
- invoking data loaders;
- downloading source data;
- querying provider APIs;
- reading server data as implementation evidence;
- creating backfill scripts.

A future loader task must be separately approved by PM L2 and must define exact file scope.

## 3. No materialization authorized

No materialization is authorized by this package.

Forbidden:

- feature table creation;
- D1 feature export generation;
- feature-store writes;
- CSV/parquet creation;
- backfilled datasets;
- point-in-time joined tables;
- prediction-ready matrices.

The D1 feature export contract is a schema and governance contract only.

## 4. No registry mutation authorized

No registry mutation is authorized.

Forbidden:

- `configs/instruments/**`;
- `configs/datasets/**`;
- registry yaml/json files;
- provider registry entries;
- source registry entries;
- calendar registry entries.

Roll/expiry mapping is defined only as a design-only dataset contract. It is not registered by this package.

## 5. No src mutation authorized

No files under `src/**` are authorized.

This includes:

- runtime loaders;
- feature engineering code;
- validators;
- model code;
- assistant code;
- CLI code;
- SDK changes.

## 6. No data mutation authorized

No files under `data/**` are authorized.

No server data, generated dataset, raw dump, intermediate file, cache, or derived feature file may be created by this package.

## 7. No runtime authorized

Runtime remains blocked.

Forbidden runtime actions:

- ingestion;
- backfill;
- calculation;
- feature computation;
- validation run;
- model fit;
- model inference;
- daily forecast;
- daily market assistant output.

## 8. No server apply authorized

Server apply is not authorized.

No command is required or expected for this handoff. The canonical server context remains informational only:

```text
HOME=/home/trader
repo_path=/home/trader/moex_bot/moex-robot
shell_path=~/moex_bot/moex-robot
command_prefix=cd ~/moex_bot && source venv/bin/activate && cd moex-robot
```

Deprecated underscore paths remain forbidden.

## 9. Forecast anchor boundary

The governing anchor is:

```yaml
forecast_anchor_local: "06:00 Europe/Moscow"
D1_rule: "trade_date T usable only from T+1 06:00 Europe/Moscow or later"
canonical_availability_field: availability_ts_utc
```

Existing D+1 open wording, if found elsewhere, must not weaken this stricter Phase 2.2 rule.

## 10. Future split tasks

Future work must be split into separate PM-approved tasks:

| future task | required scope before execution |
|---|---|
| Registry task | exact registry files, source ids, dataset ids, and rollback rule |
| Loader/materialization task | exact `src/**` files, runtime contract, no provider ambiguity |
| Validation tests task | exact `tests/**` files and expected fixtures |
| Source providers task | exact `contracts/sources/**` files and provider timestamp semantics |
| Calendar providers task | exact `contracts/calendars/**` files and schedule/outcome separation |
| Server apply task | exact origin/main SHA, command scope, single apply window |

Until such tasks are approved, Phase 2.3 remains documentation and design contracts only.
