# MOEX Bot Server Layout Contract v1

Status: active
Scope: server runtime path references used by ChatGPT/project handoffs.
Evidence source: user-provided server file discovery and explicit confirmation dated 2026-08-24 that `/home/trader/moex_bot/.env` is the canonical MOEX Bot runtime environment file.

## Current observed paths

- Server project parent: `/home/trader/moex_bot`
- Repository root: `/home/trader/moex_bot/moex-robot`
- Virtual environment root: `/home/trader/moex_bot/venv`
- Data root: `/home/trader/moex_bot/data`
- Project environment file: `/home/trader/moex_bot/.env`

## Command construction rule

Use the current observed paths above when constructing server commands for this project.

Do not substitute underscore variants or another Linux user unless a newer server-state proof explicitly supersedes this contract.

The parent project `.env` at `/home/trader/moex_bot/.env` is the only canonical dotenv file for MOEX Bot runtime commands. `/home/trader/moex_bot/moex-robot/.env` is not a canonical project runtime source and must not be used as a fallback without a newer merged runtime contract.

Commands whose implemented CLI accepts an explicit env-file argument must use `/home/trader/moex_bot/.env` when runtime secrets need to be loaded explicitly.

## Applied-state dotenv invariant

The canonical parent file `/home/trader/moex_bot/.env` must exist on the applied server state before runtime execution.

The duplicate repository-local file `/home/trader/moex_bot/moex-robot/.env` must be absent after this runtime-path migration. It must not be copied, recreated, or used as a compatibility secret source.

Python entrypoints that call `load_dotenv()` without an explicit path may rely on python-dotenv upward discovery only under this invariant: with the repository-local duplicate absent, discovery continues from the source tree through the repository root to the canonical parent file. Top-level controlled runtime launchers may instead resolve and load the canonical parent path explicitly.

Removal of the duplicate repository-local `.env` is an Applied State migration step and must happen only after the canonical parent `.env` has been verified to exist. Secret values must never be printed during this verification.

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

The launcher resolves the parent project `.env` from its own module path, loads it with `override=False`, and then delegates to `usdrubf_phase8_6a_algopack_cnyrub_source_validation`.

Direct `python -m moex_research.runners.usdrubf_phase8_6a_algopack_cnyrub_source_validation` invocation is not the canonical controlled-server entrypoint because that validator expects the required environment to be loaded already.

## Deprecated / forbidden path claims

The following paths are not valid runtime contract paths for this project and must not be used in generated commands:

- `/home/ubuntu/moex_bot/moex_robot`
- `/home/ubuntu/moex_bot/moex-robot`
- `/home/trader/moex_bot/moex_robot`
- `~/moex_bot/moex_robot`

For runtime secrets, `/home/trader/moex_bot/moex-robot/.env` is also non-canonical; the canonical path is `/home/trader/moex_bot/.env`.

## Runtime evidence boundary

This contract records the current server layout for command construction. It does not by itself prove that a command has run successfully. Server apply and real-run status require returned shell output from the current server session.

## Supersession rule

This contract supersedes older PR-body path claims and chat-memory path claims. If this file conflicts with old PR text, old chat context, or prior assistant messages, this file wins unless replaced by a newer merged runtime contract.
