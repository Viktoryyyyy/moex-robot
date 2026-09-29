# Price/FUTOI/basis final delivery

PROJECT=MOEX_Bot

Task ID: price_futoi_basis_final_delivery_v1

Base: `32c7044fcfeb74d988d9517b83829ccde94fc14d`.

The existing server had admitted current Price/OI, Si/CR FUTOI v2 and 22 basis
metrics, but its MCP bridge timed out after 5 seconds. A read-only production
measurement on 2026-09-29 found the full snapshot was 22,096,888 bytes / 13.26s
and the general factual package was 1,907,659 bytes / 35.41s. Reproducing the
existing bridge call failed at 5.01s. This was a delivery failure, not missing
source admission. No source or data-quality gate is relaxed to repair it.

`GET /v1/rub/market-factual` and MCP `get_rub_market_factual` provide the bounded
`rub_market_factual_delivery.v1` current view. They use the existing canonical
snapshot reader and its admitted factual release once, without another collector,
network source lookup, copied statistics engine or storage pointer. The legacy
snapshot and full factual-release routes remain available and unchanged.

The response contains:

- seven current Price/OI or spot rows, separately admitted quotes and exact
  source units/normalization/expiry, source clocks and deterministic evidence IDs;
- Si/CR root-v2 FUTOI facts, selected source revision identifiers, full replay
  verdict and hash-bound evidence references, or exact source refusal;
- each admitted basis/carry metric with its value, units, formula, leg identity
  and clocks, plus independently refused metric IDs and reasons;
- the existing admitted Si previous observation in its separate dated scope;
  CR current-pair-only permission is unchanged;
- explicit references to the separate full historical/external/risk contexts,
  not a claim that the narrow response completes the whole daily/weekly package.

The canonical read clock and final delivery clock are distinct. Cheap existing
freshness gates recheck data immediately before projection completes. Source
TTL remains 60 seconds for Price/OI/basis and 1200 seconds for FUTOI current
source/receipt and previous receipt/witness. Data admitted at the start of a
slow read can only be revoked, never upgraded or replaced by dated/EOD data.
Consumers must recheck original deadlines at their actual analysis time.
Current receipt freshness uses the governing envelope `last_success_at`, not
source availability. Previous dated observations expose their original receipt
and witness refresh clocks, request time and the earlier governing deadline;
their historical event timestamp is explicitly not subject to the current TTL.

The response is limited to 128 KiB of strict JSON. An unexpected oversize is an
explicit failure, never silent truncation of metrics, identity or refusals. Raw
Parquet/base64/history buffers are not transported. Evidence remains in the
existing canonical snapshot/audit. MCP returns the HTTP object unchanged.
Its 60-second HTTP read budget accommodates measured replay; it is a transport
timeout, not an increase in any data TTL. Redirects are refused.

Scope is the final projection, existing consumer/API/MCP callers, their contract
documentation and targeted tests. Source collectors, scheduler, Stage9 selection,
governance, historical accepted pointers, Stage5 and trading authority are unchanged.

Tests exercise real refresh, saved JSON, canonical reader, HTTP and MCP; only
external source I/O and clocks are controlled. They cover current Si/CR refusal
isolation, full v2 evidence and >2**53 identifiers, delayed-delivery TTL crossing,
independent quote use, lossless metric values and partial leg failure, immutable
input bytes, bounded response and no fallback on transport/schema errors.
The existing MOEX Analyst web-chat caller also exposes the new tool and selects
it for the three current-market topics. Its real function-call routing and
lossless tool-result handoff are tested with synthetic external model responses;
there is no new analysis engine, model configuration or remote transport.
Full final-head CI, independent review and exact merged-SHA runtime acceptance
are required; their final evidence is recorded in the PR.

Pre-publication validation on 2026-09-29: 179 focused tests passed in 91.13s
using the repository environment and real Parquet engine, in the existing
network-isolated test helper. This includes the new final delivery tests,
Stage9 v2, MCP stdio/HTTP, factual API and release/package regressions.
Independent read-only review found and verified the fix for a malformed or
timezone-less timestamp incorrectly failing the whole response. Such timestamps
now remain visible as refusal evidence with no age/deadline or admitted value;
other instruments, root and independent metrics survive the full HTTP/MCP path.
