# Stage 9 — Daily / Weekly RUB Analysis Bundle v1

PROJECT=MOEX_Bot

## Task

Implement the deterministic server-side bundle used by the RUB / USDRUBF daily and weekly analysis workflow.

The bundle is a read-only evidence package. It must not generate a trade, position size, stop, target, scenario probability, market regime, fundamental bias, or recommendation.

## Canonical upstream scope

Only repository-declared accepted/readiness semantics may be used.

Mandatory server-core blocks:

- Stage 3 canonical raw market state: Si/CR front+next, USD_TOM/CNY_TOM and supplementary OI where accepted;
- Stage 4 RUB basis/carry/term-structure outputs;
- Stage 5 Si/CR FUTOI EOD and positioning features;
- Stage 7 USDRUBF/CNYRUBF D1/W1 OHLCV and technical features;
- Stage 8 position/risk state, but only from an explicit manual/read-only validated input supplied for the bundle build. Stage 8 has no accepted current pointer and must never be auto-discovered.

Do not use arbitrary physical files, mutable `latest`, directory scanning, newest-mtime selection, legacy accepted pointers, or unaccepted partitions as architecture/readiness evidence.

## Interface

Provide one deterministic builder/CLI contract equivalent to:

- `scope=daily|weekly`
- explicit `as_of`
- optional explicit Stage 8 risk-state input path
- output JSON file optional; stdout JSON supported

Do not add a network service dependency if the repository has no canonical FastAPI service. The builder is the Stage 9 data contract and can be wrapped later by FastAPI/MCP without changing semantics.

## Bundle structure

The output must clearly separate:

1. `identity`: project, schema version, scope, as_of;
2. `server_core`: accepted upstream blocks and exact pointer/manifest/partition provenance used;
3. `position_risk`: validated explicit Stage 8 state or explicit `not_supplied` status;
4. `external_context_required`: blocks intentionally supplied by ChatGPT/user rather than the current server core (macro, official event calendar, fresh news/geopolitics, external oil/global-USD series until separately accepted);
5. `readiness`: per-block state plus aggregate completeness;
6. `quality_gates`: facts that constrain later analysis/recommendation.

## Daily workflow coverage

The package must expose evidence needed for:

- what changed in RUB market data;
- FX-flow proxies available from accepted market data;
- carry/basis;
- price confirmation and D1 technical context;
- cross-market confirmation across accepted RUB instruments;
- OI/FUTOI positioning context;
- position/risk state when explicitly supplied.

Do not claim that news, sanctions, government FX operations, exporter/importer cash flow, USD/CNY, Brent/Urals, DXY or UST are server-ready unless an accepted repository contract proves that.

## Weekly workflow coverage

The package must expose accepted W1/D1 USDRUBF/CNYRUBF evidence, FUTOI positioning and basis/carry evidence, while explicitly reporting unresolved weekly gaps already declared by Stage 7/config:

- Si/CR continuous weekly series: not ready;
- weekly OI: not ready;
- advanced technical policies such as EMA/realized-volatility/range-percentile/swing/BOS where not approved: not ready;
- external macro/events/oil/global-USD context: external context required.

No substitute series or inferred readiness is allowed.

## As-of and causality

- `as_of` is mandatory and timezone-aware.
- Reject naive timestamps.
- Do not select evidence with an observation/publication/availability timestamp later than `as_of` when such timestamp exists in the accepted artifact.
- Where an accepted pointer references a historical partition whose content extends beyond `as_of`, select rows causally inside the partition; do not treat pointer publication time as observation time.
- Never silently fall forward to a later observation.
- Preserve provenance for the selected observation/row(s).

## Accepted-pointer integrity

For every accepted current pointer consumed:

- require a regular file under `MOEX_DATA_ROOT/state/datasets` at the deterministic dataset/instrument/timeframe path defined by its producing stage;
- validate expected dataset/instrument/timeframe identity;
- require `quality_status=pass` where contracted;
- resolve only `${MOEX_DATA_ROOT}/...` refs under the configured data root;
- reject traversal, absolute foreign paths and symlink escapes;
- if pointer carries SHA256 for manifest/partition/quality report, re-hash and require exact match before reading;
- require referenced files to be regular files;
- fail closed on malformed JSON, duplicate JSON object members, unknown critical identities, missing mandatory provenance or duplicate logical blocks.

## Stage 8 integration

- Accept only an explicit file path argument; no directory scan/autodetect/current pointer.
- Parse using the Stage 8 strict JSON/Decimal validation path already in the repository rather than duplicating looser validation.
- Carry the validated Stage 8 output verbatim/semantically lossless into the bundle.
- If omitted, mark `position_risk.status=not_supplied`; this lowers bundle completeness but is not a fabricated zero-risk state.
- Do not recompute price-based futures P&L, risk per contract or position size; instrument payout/lot mapping remains unapproved.

## Readiness/completeness semantics

Use explicit statuses, not optimistic booleans inferred from file existence.

At minimum distinguish:

- `ready`
- `not_supplied`
- `not_ready_policy_gap`
- `external_context_required`
- `missing_or_invalid_accepted_evidence`

Aggregate bundle status must not be `complete` when any workflow-mandatory server block for the requested scope is missing/invalid/not-ready. A usable partial bundle may be emitted only if the missing blocks are explicitly represented and the contract marks the package partial; corrupted or identity-invalid accepted evidence must fail closed rather than degrade silently.

## Quality gates carried to downstream analysis

The bundle must encode, without itself making a recommendation:

- stale/misaligned core evidence blocks exact trigger generation downstream;
- missing CR/CNY confirmation, FUTOI or basis lowers downstream confidence;
- front/next must not be mixed without explicit roll mapping;
- participant groups are descriptive evidence, not smart-money labels;
- fact/evidence must remain separate from later interpretation/recommendation;
- absent Stage 8 risk state blocks position-size/add recommendations downstream.

## Output determinism

For identical input files and arguments the JSON result must be byte-stable apart from an explicitly excluded generated-at field. Prefer no wall-clock generated-at field. Sort map keys for CLI serialization and preserve deterministic list ordering.

## Tests / Done when

- daily and weekly happy paths are independently tested;
- exact accepted-pointer identities are tested;
- `as_of` causality is tested with later rows that must be excluded;
- missing Stage 8 input produces explicit `not_supplied`, never zeros;
- malformed/foreign/traversal/symlink pointer refs fail closed;
- pointer SHA mismatch fails closed where SHA is present;
- duplicate JSON keys fail closed;
- missing/invalid mandatory accepted evidence fails closed;
- declared Stage 7 weekly gaps remain explicit and false/not-ready, not normalized to green;
- no network/broker writes/orders/auto-sizing/trade recommendation are introduced;
- typed contract/config tests assert exact readiness semantics, not substring matches;
- exact-head CI passes and fresh exact-head Codex review has no unresolved material findings before merge.
