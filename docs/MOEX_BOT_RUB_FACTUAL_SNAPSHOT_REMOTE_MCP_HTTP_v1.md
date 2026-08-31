# MOEX Bot RUB factual snapshot remote MCP HTTP v1

Status: repository delivery contract

Task ID: `rub_factual_snapshot_remote_mcp_https_transport_v1`

## Purpose

Provide the network origin required for a ChatGPT-facing remote MCP connection without changing the canonical RUB factual pipeline.

This document supersedes only the external transport selection in `MOEX_BOT_RUB_FACTUAL_SNAPSHOT_CHATGPT_MCP_BRIDGE_v1.md` for the current Russian-hosted Applied State. The factual MCP tools, factual API boundary, timestamps, provenance, status semantics, and authority boundary remain unchanged.

## Why Secure MCP Tunnel is not selected for the current Applied State

The first bridge delivery selected OpenAI Secure MCP Tunnel because it avoids public inbound exposure. Live server validation then proved that the current Russian-hosted server cannot establish the required OpenAI API control-plane connection:

```text
HTTP 403
code=unsupported_country_region_territory
message=Country, region, or territory not supported
```

Therefore `tunnel-client` on this host is not an operational path. This repository does not add a VPN, proxy, location workaround, or any mechanism intended to bypass OpenAI regional policy.

## Selected architecture

```text
ChatGPT
-> approved stable public HTTPS MCP endpoint in a supported region
-> authenticated HTTPS forwarding / reverse relay
-> private MOEX Bot MCP origin
-> http://127.0.0.1:8766/mcp
-> read-only MCP bridge
-> http://127.0.0.1:8765/v1/rub/factual-snapshot
-> canonical persisted snapshot
```

The Russian MOEX Bot server does not need to call OpenAI in this architecture. ChatGPT initiates the MCP request against the public remote endpoint.

## Local MCP origin

Implementation remains:

```text
src/misc/mcp_rub_factual_snapshot_bridge.py
```

It now supports two transports:

```text
stdio
streamable-http
```

`stdio` remains the default for backward compatibility and local MCP clients.

The network origin is:

```text
http://127.0.0.1:8766/mcp
```

Properties:

- bind is restricted in application code to exact `127.0.0.1`;
- non-loopback bind is rejected before serving;
- Streamable HTTP endpoint is `/mcp`;
- `stateless_http=true`;
- `json_response=true`;
- no extra MCP tools are added;
- no public TLS listener is implemented by the application;
- no source, producer, refresh, analysis, trading, broker, or Telegram path is added.

The local origin is intentionally inaccessible from the public Internet by itself.

## MCP tool contract remains unchanged

Exactly two tools are exposed:

```text
get_rub_factual_snapshot
get_rub_snapshot_readiness
```

Both remain explicitly annotated:

```text
readOnlyHint=true
destructiveHint=false
idempotentHint=true
openWorldHint=false
```

They continue to call only the authenticated localhost factual API on `127.0.0.1:8765`.

No semantic transformation, regeneration, fallback market-data access, analysis, forecast, recommendation, or trading authority is introduced.

## Timestamp and degraded-state semantics

The remote transport must preserve the existing factual result unchanged, including:

- `identity.generated_at_utc`;
- `read_freshness` and `read_freshness.read_at_utc`;
- component timestamps;
- provenance;
- readiness/component statuses;
- `READY`;
- `PARTIAL`;
- `STALE`;
- `GOVERNED_BLOCKED`;
- unavailable/missing states;
- authority fields.

Neither the Streamable HTTP request time nor the public relay request time is market-data freshness.

No degraded state may be silently upgraded.

## Runtime artifact

Repository-governed unit:

```text
ops/systemd/moex-rub-factual-snapshot-mcp-http.service
```

It starts only the loopback MCP origin:

```text
/home/trader/moex_bot/venv/bin/python -m misc.mcp_rub_factual_snapshot_bridge --transport streamable-http --host 127.0.0.1 --port 8766
```

It remains a separate process from:

- the factual producer;
- `moex-rub-factual-snapshot-api.service`;
- `moex-rub-chat-snapshot.timer`.

Before server apply, port `8766` must be checked for an existing listener. Repository absence of another `8766` allocation is not runtime proof that the port is free.

## ChatGPT remote MCP requirement

Current OpenAI product documentation requires ChatGPT custom MCP connectivity to use a remote MCP endpoint; local MCP servers cannot be connected directly. For a public remote deployment, use a stable HTTPS endpoint supporting MCP Streamable HTTP, conventionally ending in `/mcp`.

Authoritative OpenAI references re-verified for this task:

```text
https://developers.openai.com/plugins/deploy/connect-chatgpt
https://developers.openai.com/plugins/concepts/mcp-server
https://help.openai.com/en/articles/12584461-developer-mode-and-mcp-apps-in-chatgpt
```

The MCP Python SDK also identifies Streamable HTTP as the deployment transport; SSE is superseded and is not introduced here.

## Public endpoint security boundary

The local origin is not authorization for public exposure.

The final public endpoint MUST NOT be an anonymous open proxy to the MOEX Bot MCP origin.

Before publication, an approved external HTTPS layer must provide:

1. valid public TLS;
2. a stable hostname in a jurisdiction where the intended OpenAI/ChatGPT connection is supported;
3. authenticated access compatible with current ChatGPT custom-app configuration;
4. forwarding only to the bounded MCP origin;
5. no forwarding to the factual REST API directly;
6. no generic TCP/HTTP access to other server services;
7. appropriate request/log secret hygiene.

OpenAI's ChatGPT custom-app configuration supports selecting an authentication mechanism and documents OAuth flows. A custom static API key must not be assumed to be a ChatGPT-supported authentication mechanism unless current OpenAI documentation explicitly supports it at deployment time.

The exact external relay/provider, public hostname, domain ownership, TLS certificate, and authentication provider are not defined by the MOEX Bot repository today. They must not be invented in this task.

## Forwarding requirement

The external HTTPS relay must preserve MCP protocol headers and request bodies and forward the MCP path to:

```text
http://127.0.0.1:8766/mcp
```

The MCP Python server enables localhost DNS-rebinding protection by default. Any same-host forwarding agent must therefore preserve/rewrite the upstream `Host` consistently with the local origin rather than weakening the MCP application's loopback security based on a guessed public hostname.

If the external relay is not on the MOEX Bot host, the mechanism by which it securely reaches the private loopback origin must be separately governed. Do not change the MCP application to `0.0.0.0` as a shortcut.

## Source-of-truth boundary for external infrastructure

No existing repository artifact defines:

- nginx;
- Caddy;
- cloudflared;
- a reverse SSH relay;
- a VPN overlay;
- an external VPS;
- a routable domain;
- external TLS termination;
- an OAuth provider for this MCP service.

Therefore this delivery creates the required loopback Streamable HTTP origin but does not fabricate an external provider or hostname.

A later external-infrastructure apply must be represented in Source of Truth where appropriate before it is treated as canonical architecture.

## Server validation contract

After merge, server apply must prove:

1. repository is on the merge SHA;
2. `127.0.0.1:8766` is free before service start;
3. `moex-rub-factual-snapshot-mcp-http.service` is active;
4. listener is exactly loopback `127.0.0.1:8766`;
5. Streamable HTTP MCP client connects to `http://127.0.0.1:8766/mcp`;
6. only the two read-only tools are exposed;
7. MCP `identity.generated_at_utc` matches the canonical factual API result;
8. degraded statuses remain unchanged;
9. factual producer invocation is unchanged by MCP calls;
10. factual producer timer remains `OnUnitActiveSec=10min`;
11. factual API remains active;
12. no analysis/trading action occurs.

## Final ChatGPT connection procedure

Once an approved authenticated public relay exists, the final ChatGPT endpoint is:

```text
https://<approved-remote-mcp-host>/mcp
```

Do not replace the placeholder until a real governed hostname exists.

Then, on an eligible ChatGPT account/workspace:

1. enable Developer Mode as permitted by the account/workspace;
2. create a custom app/MCP connection;
3. enter the real public HTTPS `/mcp` endpoint;
4. select/configure the approved authentication mechanism;
5. click Scan Tools;
6. verify exactly `get_rub_factual_snapshot` and `get_rub_snapshot_readiness`;
7. invoke readiness;
8. invoke factual snapshot;
9. verify current `identity.generated_at_utc`, `read_freshness`, provenance, component statuses, and authority fields.

Current OpenAI plan/account restrictions are a separate product gate and must be checked against the actual target account before claiming end-to-end completion.

## Completion states

Until an approved public authenticated HTTPS relay and ChatGPT-side registration exist:

```text
REPOSITORY REMOTE MCP ORIGIN: DONE after merge
SERVER REMOTE MCP ORIGIN: PENDING server_apply
EXTERNAL HTTPS RELAY: USER INFRASTRUCTURE REQUIRED
CHATGPT CONNECTION: PENDING
```

The original end-to-end ChatGPT bridge task is not fully DONE until ChatGPT itself can invoke the factual MCP tool.
