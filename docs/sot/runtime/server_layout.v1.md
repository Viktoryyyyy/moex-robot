# MOEX Bot Server Layout Contract v1

Status: active
Scope: server runtime path references used by ChatGPT/project handoffs.
Evidence source: user-provided server file listing screenshot dated 2026-06-09 showing `/home/trader/moex_bot` with child directories `data`, `moex-robot`, and `venv`.

## Current observed paths

- Server project parent: `/home/trader/moex_bot`
- Repository root: `/home/trader/moex_bot/moex-robot`
- Virtual environment root: `/home/trader/moex_bot/venv`
- Data root: `/home/trader/moex_bot/data`

## Command construction rule

Use the current observed paths above when constructing server commands for this project.

Do not substitute underscore variants or another Linux user unless a newer server-state proof explicitly supersedes this contract.

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
