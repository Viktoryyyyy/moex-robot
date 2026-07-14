# EMA 3/19 AI — Phase 2 FUTOI PIT Policy v1

Status: policy placeholder; FUTOI source contract excluded from Phase 2.5  
Lane: `ema_3_19_ai`  
Task: `ema_3_19_ai_market_phase_phase_2_5_provider_registry_placeholder_contracts`  
Runtime status: blocked  
Modeling status: blocked

## Decision

FUTOI means participant positioning / participant position structure only. It does not mean generic open interest.

The FUTOI source contract is excluded from Phase 2.5. No file under `contracts/sources/futoi/**` is created in this task.

## Current status

`futoi.participant_positioning` remains blocked for ingestion, runtime, materialization, feature computation, modeling, prediction, registry mutation, and server apply.

The block remains in force until PM L2 approves an exact future provider/source policy.

## Required future policy before any FUTOI source contract

A later FUTOI provider/source contract must define all of the following before any data is eligible for a forecast anchor:

1. Exact provider release timestamp source.
2. Source timezone.
3. UTC conversion rule for `availability_ts_utc`.
4. Trade-date join key to USDRUBF / Si daily rows.
5. Holiday and missing-report handling.
6. PIT revision behavior for corrections, restatements, and late files.
7. Eligibility rule: `availability_ts_utc <= forecast_anchor_ts`.
8. Conservative unresolved timestamp rule: if release timestamp is unresolved, exclude the row or shift by at least one trading day.
9. Schema proof that fields represent participant positioning and not open interest.

## Timing rules

- `availability_ts_utc` is required for every eligible FUTOI row.
- Source timestamps must be converted to UTC before comparison with `forecast_anchor_ts`.
- Unknown or unverifiable release timestamp means the row is not eligible for the current anchor.
- Revisions/restatements require PIT revision history. Without PIT revision history, revised values must be excluded from prior anchors.
- Rows may not be backfilled into earlier anchors using after-anchor knowledge.

## Forecast-anchor rule

A row may be used only if:

```text
availability_ts_utc <= forecast_anchor_ts
```

If this cannot be proven, the row must be excluded or shifted by at least one trading day.

## Semantic exclusions

FUTOI policy must not be used to introduce:

- generic open interest semantics;
- post-fact participant outcome annotations;
- future return labels;
- future volatility labels;
- drawdown labels;
- phase-completion targets;
- B/S/OUT labels;
- EMA cross labels;
- LLM classifications;
- realized event outcomes.

## Explicit non-authorizations

- no FUTOI source contract in Phase 2.5
- no `contracts/sources/futoi/participant_positioning.v1.yaml`
- no open-interest interpretation
- no registry mutation
- no configs mutation
- no source/provider implementation
- no tests
- no data loading
- no ingestion
- no backfill
- no materialization
- no runtime
- no feature computation
- no modeling
- no prediction
- no server apply
