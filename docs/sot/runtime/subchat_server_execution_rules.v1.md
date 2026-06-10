# MOEX Bot Sub-chat Server Execution Rules v1

Status: active
Scope: server command construction and server-side code execution rules for MOEX Bot sub-chats.
Owner: project static/runtime context
Depends on: docs/sot/runtime/server_layout.v1.md

## 1. Purpose

This file defines the server execution convention that sub-chats must use when they ask the user to run commands on the MOEX Bot server.

The goal is to prevent wrong paths, broken one-line Python, implicit imports, guessed artifact locations, and server-first editing.

## 2. Source-of-truth boundary

GitHub / repo is the source of truth for code and contracts.

Server is applied state only.

Sub-chats must not use remembered server paths as proof. Use `docs/sot/runtime/server_layout.v1.md` for command construction unless a newer merged runtime contract supersedes it.

Server command output is required to prove server apply and real-run status.

## 3. Current server layout

Use these exact paths:

- Project parent: `/home/trader/moex_bot`
- Repository root: `/home/trader/moex_bot/moex-robot`
- Virtual environment root: `/home/trader/moex_bot/venv`
- Data root: `/home/trader/moex_bot/data`

Do not use:

- `/home/trader/moex_bot/moex_robot`
- `/home/ubuntu/moex_bot/moex_robot`
- `/home/ubuntu/moex_bot/moex-robot`
- `~/moex_bot/moex_robot`

## 4. Canonical command prefix

Every server command for this project must begin with the explicit repo root and venv activation:

```bash
cd /home/trader/moex_bot/moex-robot && source /home/trader/moex_bot/venv/bin/activate && <command>
```

Do not use `cd ~/...`.

Do not use underscore repo directory variants.

## 5. Python import path rule

The repository uses a `src/` package layout. Server Python commands that import project modules must set:

```bash
PYTHONPATH=.:src
```

Canonical Python command prefix:

```bash
cd /home/trader/moex_bot/moex-robot && source /home/trader/moex_bot/venv/bin/activate && PYTHONPATH=.:src python -c '<python_code>'
```

If `PYTHONPATH=.:src` is omitted, imports such as `moex_data` may fail with `ModuleNotFoundError`.

## 6. Environment loading rule

Do not rely on shell `source .env` for production or controlled server runs.

Load `.env` inside the Python process:

```python
from dotenv import load_dotenv
load_dotenv(".env", override=False)
```

For controlled data runs, set required explicit env contracts inside Python where needed, for example:

```python
import os
os.environ["MOEX_DATA_ROOT"] = "/home/trader/moex_bot/data"
```

Then snapshot env if a repo function expects an env mapping:

```python
env = dict(os.environ)
```

## 7. One-line Python rules

Sub-chats should provide phone-paste-safe one-line commands.

Rules:

- Prefer `python -c '<code>'`.
- Use double quotes inside Python strings.
- Avoid f-strings in terminal-paste commands.
- Avoid heredoc unless explicitly approved for a large file operation.
- Avoid defining `def ...` after semicolons in one-line Python; Python rejects compound statements in that position.
- Use `lambda`, comprehensions, direct module calls, or `python -m <module>` instead of inline `def` where possible.
- Keep commands deterministic and explicit.

Bad pattern:

```bash
python -c 'import x; def run(): return 1; print(run())'
```

Good pattern:

```bash
python -c 'run=lambda: 1; print(run())'
```

## 8. Git apply rule on server

Server apply commands must pin or verify the intended commit.

Canonical apply pattern:

```bash
cd /home/trader/moex_bot/moex-robot && source /home/trader/moex_bot/venv/bin/activate && git fetch origin main && git checkout main && git reset --hard <COMMIT_SHA> && PYTHONPATH=.:src python -c 'import subprocess; print("COMMIT=" + subprocess.check_output(["git","rev-parse","HEAD"], text=True).strip())'
```

Do not claim server apply is complete until returned shell output confirms the expected commit.

## 9. Code writing rule

Sub-chats must not edit files directly on the server as the primary authoring path.

Required order:

1. GitHub/repo mutation first.
2. CI/test proof when required.
3. Merge or update origin/main according to approved flow.
4. Server apply from origin/main or explicit merged commit.
5. Real run on server if required by the task.
6. Report only values from returned server output.

Server filesystem output is not architecture proof.

## 10. Artifact path rule

Do not invent artifact paths.

Use repo contracts and env contracts.

Current data root env contract:

```text
MOEX_DATA_ROOT=/home/trader/moex_bot/data
```

When a command creates data artifacts, outputs must be produced by repo code using declared contracts. Do not manually create result JSON, parquet, report, or manifest files to satisfy acceptance criteria.

## 11. Real-run reporting rule

When the user returns server output, the sub-chat must separate:

- repo implementation status
- server apply status
- real run status
- outputs created
- validation checks
- blockers

Do not infer rows, paths, dates, or success from planned commands. Use only actual returned output.

## 12. Minimal validation pattern

For server materialization/check commands, validate at minimum:

- expected commit
- expected module import
- partition exists
- manifest exists
- quality report exists
- row count condition
- instrument coverage
- trade_date coverage
- reproducibility rerun if required by task

## 13. Example module check

```bash
cd /home/trader/moex_bot/moex-robot && source /home/trader/moex_bot/venv/bin/activate && git fetch origin main && git checkout main && git reset --hard <COMMIT_SHA> && PYTHONPATH=.:src python -c 'import sys, subprocess; print("PYTHON=" + sys.executable); print("COMMIT=" + subprocess.check_output(["git","rev-parse","HEAD"], text=True).strip()); import moex_data.futures.materialize_raw_5m_full_session as m; print("MODULE_OK=" + m.__name__)'
```

## 14. Stop conditions

Stop and report a blocker if:

- a required path is not defined in repo/runtime SoT;
- a command would require guessed server filesystem paths;
- a task requires server-first editing;
- a real run fails and no returned output supports success;
- data/source selection would need to change outside approved scope;
- output artifacts would need to be manually created.

## 15. Supersession

This file is subordinate to `docs/sot/runtime/server_layout.v1.md` for concrete server paths.

If server paths change, update `server_layout.v1.md` first, then update this file if command construction rules also change.
