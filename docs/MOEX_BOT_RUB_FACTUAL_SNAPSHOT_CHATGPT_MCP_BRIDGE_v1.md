# MOEX Bot RUB factual snapshot ChatGPT MCP bridge v1

Status: repository delivery contract

Task ID: `rub_factual_snapshot_chatgpt_mcp_bridge_v1`

## Purpose

This bridge gives an authorized MCP client a read-only path to the existing canonical RUB factual snapshot through the already deployed localhost factual HTTP API.

It is a transport/integration component only.

Canonical flow:

```text
existing factual data sources
-> existing factual producer
-> canonical persisted snapshot
-> localhost factual HTTP API
-> read-only stdio MCP bridge
-> Secure MCP Tunnel
-> ChatGPT analyst
```

The bridge does not collect, refresh, generate, mutate, analyze, forecast, recommend, trade, call a broker, or send Telegram messages.

## Repository inventory and selected boundaries

Canonical persisted schema:

```text
rub_chat_analysis_snapshot.v1
```

Canonical persisted state relative to `MOEX_DATA_ROOT`:

```text
state/rub_intelligence/chat_analysis_snapshot/current.json
```

Existing factual HTTP service:

```text
src/misc/rub_factual_snapshot_http_server.py
```

Canonical local endpoints:

```text
GET http://127.0.0.1:8765/v1/rub/factual-snapshot
GET http://127.0.0.1:8765/readyz
```

Existing legacy stdio MCP adapter:

```text
src/misc/mcp_rub_analysis_snapshot_server.py
```

The legacy adapter directly calls the governed snapshot consumer. It remains unchanged. This task introduces a separate bridge downstream of the factual HTTP API so the transport boundary is explicit and testable.

Python dependencies already present before this task include:

```text
requests>=2.31.0
python-dotenv>=1.0.1
mcp>=1.28,<2
```

No additional Python dependency is required.

Repository inventory found no existing `tunnel-client`, nginx, cloudflared, reverse-proxy, or external-ingress architecture to reuse. This task therefore does not invent a public ingress.

## MCP implementation

Bridge implementation:

```text
src/misc/mcp_rub_factual_snapshot_bridge.py
```

Transport:

```text
stdio
```

The bridge exposes exactly two tools.

### `get_rub_factual_snapshot`

Performs only:

```text
GET http://127.0.0.1:8765/v1/rub/factual-snapshot
Authorization: Bearer <existing governed factual API token>
```

A successful HTTP `200` JSON object is returned as the MCP structured tool result without field renaming, wrapping, aggregation, synthesis, or status upgrade.

### `get_rub_snapshot_readiness`

Performs only:

```text
GET http://127.0.0.1:8765/readyz
Authorization: Bearer <existing governed factual API token>
```

HTTP `200` and canonical HTTP `503 NOT_READY` JSON objects are returned unchanged. A `503` readiness response is factual operational state; the bridge does not reinterpret it as analytical or trading readiness.

Both tools are advertised with MCP annotations:

```text
readOnlyHint=true
destructiveHint=false
idempotentHint=true
openWorldHint=false
```

They have no input parameters and expose no mutation tool.

## Factual API authentication

The existing factual API authentication boundary remains intact.

Required secret:

```text
MOEX_RUB_SNAPSHOT_API_TOKEN
```

The bridge resolves it using the existing project convention:

1. direct process environment `MOEX_RUB_SNAPSHOT_API_TOKEN`;
2. otherwise `MOEX_ENV_FILE`, reading only `MOEX_RUB_SNAPSHOT_API_TOKEN` through `python-dotenv`.

Canonical server project environment remains:

```text
/home/trader/moex_bot/.env
```

No secret value is committed to GitHub or emitted by the bridge.

Invalid, missing, blank, whitespace-corrupted, or non-ASCII token configuration fails closed before the stdio MCP server starts when launched through `main()`.

## Timestamp semantics

The bridge does not add an MCP request timestamp to the canonical factual payload.

This is deliberate. It prevents a transport timestamp from being confused with factual freshness.

The existing canonical distinctions remain authoritative:

1. `identity.generated_at_utc` — producer snapshot generation time;
2. `read_freshness.read_at_utc` — factual API governed read time;
3. source/component timestamps — source freshness already carried by components.

The MCP invocation time belongs to transport/client telemetry only and is not market-data freshness.

## Failure semantics

| Condition | MCP behavior |
| --- | --- |
| Snapshot HTTP `200` | Return canonical snapshot unchanged |
| Snapshot is valid `PARTIAL` or `STALE` | Return it unchanged; degraded state remains visible |
| Component is `GOVERNED_BLOCKED` | Preserve it unchanged |
| Factual API `401` | MCP tool error: factual API authentication failed |
| Snapshot HTTP `503` | MCP tool error: factual API snapshot unavailable |
| Factual API connection/timeout failure | MCP tool error: factual API unavailable |
| Malformed/non-object HTTP response | MCP tool error; no fallback |
| Readiness HTTP `200` | Return canonical readiness unchanged |
| Readiness HTTP `503` | Return canonical NOT_READY state unchanged |
| Missing/invalid bridge security configuration | Fail closed before serving stdio MCP |
| MCP/tunnel transport unavailable | No factual fallback or direct source access |

The bridge performs no retry to another source and no source refresh.

## Read-only authority boundary

The bridge imports only generic runtime/configuration/HTTP/MCP libraries. It does not import or invoke:

- MOEX/ISS adapters;
- AlgoPack adapters;
- CBR adapters;
- NEWS/RSS providers;
- oil sources;
- factual producer runners;
- snapshot refresh runners;
- Stage 3/4/5/7/9/10 runners;
- analytical, scenario, forecast, recommendation, B/S/OUT modules;
- broker modules;
- Telegram action modules.

The localhost factual HTTP API remains the only factual authority consumed by this bridge.

Stage5 remains OFF. FUTOI remains factual context only with no directional/action authority. MOEX Calendar API remains absent.

## Current OpenAI / ChatGPT connectivity requirement

Current authoritative OpenAI documentation was re-verified for this task on 2026-08-31:

- ChatGPT cannot connect directly to a local MCP server.
- ChatGPT connects to remote MCP paths.
- OpenAI Secure MCP Tunnel is the supported path for a private/on-premises MCP server without public inbound exposure.
- `tunnel-client` runs inside the private network and uses outbound HTTPS to OpenAI.
- `tunnel-client` can reach the private MCP server over stdio or HTTP.
- The host requires outbound HTTPS to `api.openai.com:443` by default.
- Tunnel creation/editing requires Tunnels Read + Manage; running/selecting a tunnel requires Tunnels Read + Use.
- ChatGPT developer-mode permission is separate from Platform tunnel permissions.

Authoritative references:

```text
https://developers.openai.com/api/docs/guides/secure-mcp-tunnels
https://help.openai.com/en/articles/12584461-developer-mode-apps-and-full-mcp-connectors-in-chatgpt-beta
```

The selected architecture is therefore:

```text
ChatGPT
-> OpenAI-hosted Secure MCP Tunnel endpoint
-> outbound-only tunnel-client in the MOEX Bot server trust boundary
-> stdio MCP command
-> src/misc/mcp_rub_factual_snapshot_bridge.py
-> http://127.0.0.1:8765
-> canonical factual HTTP API
-> canonical persisted snapshot
```

No public listener is added by the MCP bridge and the factual REST API remains loopback-only.

## Why stdio is selected

OpenAI Secure MCP Tunnel explicitly supports a local MCP server reached over stdio or HTTP.

Using stdio here is the minimum sufficient design because:

- no second network listener is needed;
- no new inbound firewall rule is needed;
- no second application authentication surface is created;
- the existing localhost Bearer boundary remains intact;
- `tunnel-client` is the only component that needs outbound OpenAI connectivity;
- repository already uses FastMCP stdio for the legacy adapter.

The bridge itself is therefore not a new long-running systemd network service. `tunnel-client`, once configured with an OpenAI tunnel identity, is the long-running external transport component.

## Local repository/runtime invocation

Canonical server invocation of the bridge is:

```bash
cd /home/trader/moex_bot/moex-robot
MOEX_ENV_FILE=/home/trader/moex_bot/.env PYTHONPATH=.:src /home/trader/moex_bot/venv/bin/python -m misc.mcp_rub_factual_snapshot_bridge
```

This starts stdio MCP only; it opens no listening TCP port.

## Secure MCP Tunnel configuration boundary

A tunnel cannot be finalized from repository information alone because OpenAI must issue/associate:

- a real `tunnel_id`;
- a runtime API key with Tunnels Read + Use;
- the target ChatGPT workspace/account association.

Those values must not be invented or committed.

After they exist, use the official `tunnel-client` downloaded from OpenAI Platform tunnel settings or the current public `openai/tunnel-client` release. Do not hard-code a release URL in this repository.

The local stdio command supplied to `tunnel-client` must execute the canonical bridge with the governed project environment available. OpenAI's documented shape is:

```text
tunnel-client init
  --sample sample_mcp_stdio_local
  --profile <governed-profile-name>
  --tunnel-id <real-tunnel-id>
  --mcp-command <canonical-bridge-command>

tunnel-client doctor --profile <governed-profile-name> --explain
tunnel-client run --profile <governed-profile-name>
```

Do not create a systemd unit with a guessed `tunnel-client` binary path or guessed profile location. Once the actual OpenAI-provided binary/profile locations and tunnel identity exist, the deployment authority may add a repository-governed runtime artifact in a separate exact change if persistent service management is required.

## Validation contract

Repository tests must prove:

- exact factual snapshot preservation;
- generated timestamp preservation;
- readiness/freshness preservation;
- `PARTIAL`, `STALE`, and `GOVERNED_BLOCKED` preservation;
- explicit auth/unavailable/malformed failures;
- exact localhost factual API routes;
- no direct source/refresh/analysis/trading imports;
- exactly two no-argument read-only MCP tools;
- fail-closed secret configuration;
- real stdio MCP client round-trip through the canonical factual HTTP server.

Server apply, after merge, must additionally prove:

```text
canonical persisted snapshot
-> active localhost factual API
-> stdio MCP bridge
-> MCP client
```

and compare `identity.generated_at_utc` with the factual API result.

It must also re-verify:

- `moex-rub-factual-snapshot-api.service` active;
- `moex-rub-chat-snapshot.timer` active with `OnUnitActiveSec=10min`;
- no producer/source refresh caused by MCP invocation;
- no analytical/trading action caused by MCP invocation.

## ChatGPT-side connection

After a real OpenAI Secure MCP Tunnel is created and `tunnel-client` is healthy:

1. Ensure the tunnel is associated with the intended ChatGPT workspace/account.
2. Ensure the app creator has Tunnels Read + Use.
3. Enable ChatGPT Developer Mode where the target plan/workspace permits it.
4. Create a developer-mode custom app.
5. Choose `Tunnel` under Connection.
6. Select the associated tunnel or enter the real `tunnel_id`.
7. Scan/refresh tools.
8. Confirm exactly these factual tools are exposed:
   - `get_rub_factual_snapshot`
   - `get_rub_snapshot_readiness`
9. Invoke `get_rub_snapshot_readiness`.
10. Invoke `get_rub_factual_snapshot` and verify `schema_version`, `identity.generated_at_utc`, `read_freshness`, component statuses, provenance, and authority fields are present.

Current OpenAI plan policy is material: full MCP is available for Business and Enterprise/Edu; Pro can connect MCPs with read/fetch permissions in developer mode. The Help Center does not currently state Plus support for custom MCP connections. Account/workspace eligibility must therefore be verified on the actual target ChatGPT account before claiming end-to-end completion.

## Completion states

Repository/server implementation may be complete before the account-side tunnel/app registration is possible.

Use these states explicitly:

```text
REPOSITORY: DONE
SERVER: DONE
CHATGPT CONNECTION: DONE
```

or, when an OpenAI account/workspace UI, tunnel identity, permission, or plan action is still required:

```text
REPOSITORY: DONE
SERVER: DONE
CHATGPT CONNECTION: USER CONFIGURATION REQUIRED
```

Do not mark the full task DONE until ChatGPT itself can invoke the factual MCP tool.
