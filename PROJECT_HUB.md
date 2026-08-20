# MOEX Bot — Project Hub

status: current_navigation_hub
repository: `Viktoryyyyy/moex-robot`

## Authority

GitHub repository is Source of Truth. Server filesystem is Applied State only.

Management canon:

`docs/MOEX_BOT_MANAGEMENT_CANON.md`

Browser project context:

`docs/BROWSER_PROJECT_CONTEXT.md`

## Market-data architecture

Canonical data-access architecture:

`contracts/architecture/moex_data_access_canon_v1.yaml`

Canonical data-lake config:

`configs/datasets/futures_data_lake.v1.yaml`

Canonical FORTS instrument registry:

`configs/instruments/forts_instrument_registry.v1.yaml`

Operational ingestion entrypoint for any chat/agent:

`docs/data/moex_market_data_ingestion_runbook.v1.md`

Current Stage2 evidence/status note:

`docs/MOEX_MARKET_DATA_CORE_STAGE2_FORTS_CHANGE_2026-08-18.md`

## Canonical raw datasets

Quotes:

- dataset contract: `contracts/datasets/futures_raw_5m.v1.yaml`
- source contract: `contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml`

FUTOI:

- dataset contract: `contracts/datasets/futures_futoi_raw.v1.yaml`
- source contract: `contracts/sources/futures/moex_algopack_futoi.v1.yaml`

## Data-path rule

Canonical new writes use `${MOEX_DATA_ROOT}/market` only.

Legacy `${MOEX_DATA_ROOT}/forts`, `${MOEX_DATA_ROOT}/futures/raw_5m`, `${MOEX_DATA_ROOT}/futures/futoi_raw`, old CSV master files and strategy-local files are not architecture proof and are not authorized for new ingestion.

## Server context

```text
HOME=/home/trader
repo_path=/home/trader/moex_bot/moex-robot
shell_path=~/moex_bot/moex-robot
command_prefix=cd ~/moex_bot && source venv/bin/activate && cd moex-robot
```

Forbidden deprecated repository path uses underscore (`moex_robot`) instead of hyphen (`moex-robot`).

## Working rule

When a task concerns market-data loading, updating, backfill, fields, metadata, storage, quality or accepted pointers, start from `docs/data/moex_market_data_ingestion_runbook.v1.md` and follow its referenced current GitHub contracts. Do not reconstruct ingestion from historical chat memory or old contract files.
