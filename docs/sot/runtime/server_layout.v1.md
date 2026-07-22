# MOEX Bot Server Layout Contract v1

Status: active
Scope: server runtime path references used by ChatGPT/project handoffs.
Evidence source: user-provided server file listing screenshot dated 2026-06-09 showing `/home/trader/moex_bot` with child directories `data`, `moex-robot`, and `venv`.

## Current observed paths

- Server project parent: `/home/trader/moex_bot`
- Repository root: `/home/trader/moex_bot/moex-robot`
- Virtual environment root: `/home/trader/moex_bot/venv`
- Data root: `/home/trader/moex_bot/data`
- Project environment file: `/home/trader/moex_bot/moex-robot/.env`

## Command construction rule

Use the current observed paths above when constructing server commands for this project.

Do not substitute underscore variants or another Linux user unless a newer server-state proof explicitly supersedes this contract.

The project-local `.env` under the repository root is the only canonical dotenv file for MOEX Bot repository commands. `/home/trader/moex_bot/.env` is not a canonical project runtime source and must not be used as a fallback without a newer merged runtime contract.

## Runtime environment identity

- `MOEX_DATA_ROOT=/home/trader/moex_bot/data`
- `MOEX_ALGOPACK_TOKEN` is the sole credential variable for subscribed MOEX AlgoPack routes.
- `MOEX_API_KEY` is not an alias or fallback for `MOEX_ALGOPACK_TOKEN`.
- Real credential values must remain outside GitHub and must not appear in logs, artifacts, commands, or reports.

## Canonical AlgoPack runtime entrypoint

Controlled Phase 8.6A AlgoPack runs must use this launcher:

```bash
cd /home/trader/moex_bot/moex-robot && source /home/trader/moex_bot/venv/bin/activate && PYTHONPATH=.:src python -m moex_research.runners.usdrubf_phase8_6a_algopack_cnyrub_runtime <approved-arguments>
```

The launcher resolves the repository-local `.env` from its own module path, loads it with `override=False`, and then delegates to `usdrubf_phase8_6a_algopack_cnyrub_source_validation`.

Direct `python -m moex_research.runners.usdrubf_phase8_6a_algopack_cnyrub_source_validation` invocation is not the canonical controlled-server entrypoint because that validator expects the required environment to be loaded already.

## Deprecated / forbidden path claims

The following paths are not valid runtime contract paths for this project and must not be used in generated commands:

- `/home/ubuntu/moex_bot/moex_robot`
- `/home/ubuntu/moex_bot/moex-robot`
- `/home/trader/moex_bot/moex_robot`
- `~/moex_bot/moex_robot`

## Runtime evidence boundary

This contract records the current server layout for command construction. It does not by itself prove that a command has run successfully. Server apply and real-run status require returned shell output from the current server session.

## Supersession rule

This contract supersedes older PR-body path claims and chat-memory path claims. If this file conflicts with old PR text, old chat context, or prior assistant messages, this file wins unless replaced by a newer merged runtime contract.
