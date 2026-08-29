# MOEX Bot — S7.3 Snapshot-Only MCP Consumer Contract v1

## Purpose

Provide one machine-readable, read-only server interface for Weekly/Daily analysis consumers without allowing them to bypass the accepted RUB Intelligence data plane.

Canonical persisted snapshot remains:

`/home/trader/moex_bot/data/state/rub_intelligence/chat_analysis_snapshot/current.json`

The MCP interface does not become a new factual source. It is only a transport for the canonical reader-enriched snapshot.

## Implementation

Dedicated local MCP server:

`src/misc/mcp_rub_analysis_snapshot_server.py`

Dedicated fail-closed consumer:

`src/moex_research/consumers/usdrubf_chat_snapshot_consumer.py`

Exposed MCP tool:

`rub_analysis_snapshot`

The tool returns the output of the canonical `read_current_snapshot()` path after authority/schema validation.

## Deliberate isolation

This MCP server is separate from the older experimental `src/misc/mcp_moex_server.py`.

It intentionally exposes no tools for:
- direct MOEX market-data retrieval;
- direct AlgoPack retrieval;
- direct news retrieval;
- direct macro retrieval;
- broker execution;
- Telegram delivery;
- scenario generation;
- BUY/SELL/OUT generation.

Therefore an analysis consumer connected only to this MCP cannot bypass the persisted 10-minute snapshot by asking the server to fetch a fresher fact independently.

## Fail-closed validation

Before returning a snapshot, the consumer requires:
- `schema_version == rub_chat_analysis_snapshot.v1`;
- `identity.project == MOEX_Bot`;
- persisted generation timestamp;
- canonical reader `read_freshness` metadata;
- readiness `READY` or `PARTIAL`;
- `authority.data_only == true`;
- all server analysis/action/execution authority flags remain false.

A `STALE` snapshot may still be transported because Weekly/Daily contracts own the final analysis-time freshness decision. The transport must not silently replace or upgrade it.

Weekly/Daily contracts require freshness to be recomputed again at the actual analysis time from `identity.generated_at_utc` and `refresh_policy.snapshot_stale_after_seconds`; MCP read-time freshness is diagnostic only.

## Environment

The process requires the same canonical data-root environment used by the accepted snapshot publisher:

`MOEX_DATA_ROOT=/home/trader/moex_bot/data`

The project Python path/runtime must resolve the repository modules. No new server data path is introduced.

## Transport status

Current status after repository implementation:

`LOCAL_STDIO_READY_NOT_CHATGPT_CONNECTED`

The MCP process runs through local stdio only. This contract does **not** claim that ChatGPT UI chats can already reach the VPS process remotely.

Remote ChatGPT connectivity is a separate acceptance step and must not be achieved by guessing/opening a public port or bypassing server/network governance.

## Acceptance gates

Repository acceptance:
- dedicated snapshot-only MCP exists;
- one snapshot read tool only;
- fail-closed consumer tests pass;
- no direct source-fetch/action tools enter this MCP;
- full repository CI passes.

Server local acceptance, after merge/apply:
- module imports in canonical venv;
- `MOEX_DATA_ROOT` resolves the accepted snapshot;
- snapshot-only consumer returns current reader-enriched snapshot;
- returned authority remains data-only;
- no server networking change is required for this local smoke.

Remote ChatGPT access remains `NOT_ACCEPTED` until a separately governed transport is configured and proven.
