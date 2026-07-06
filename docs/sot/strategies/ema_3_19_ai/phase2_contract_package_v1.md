# EMA 3/19 AI — Phase 2.3 Broad Design-Only Contract Package v1

Status: design-only contract package  
Lane: `ema_3_19_ai`  
Task: `ema_3_19_ai_market_phase_phase_2_3_broad_design_contract_package_v1`  
Runtime status: blocked  
Server apply status: not authorized  
Ingestion status: blocked  
Backfill status: blocked  
Feature computation status: blocked  
Modeling status: blocked  

## 1. Purpose

This package fixes the broad Phase 2 contract boundary for USDRUBF / Si daily market phase research.

The package is intentionally limited to docs and contract definitions. It does not authorize runtime loaders, data ingestion, feature materialization, backfill, statistics, model fitting, prediction generation, server apply, registry updates, source-provider files, calendar-provider files, tests, or code changes.

## 2. Phase 2.2 decisions carried forward

Phase 2.2 remains `conditional_accepted`.

Binding decisions carried into this package:

1. Phase 2 may describe broad feature and validation contracts only.
2. Provider-specific source and calendar files are excluded from Phase 2.3.
3. External provider approval is not implied by this package.
4. Registry mutation is excluded.
5. Source ingestion, calculation, and materialization are excluded.
6. Modeling remains blocked until later PM-approved implementation gates are satisfied.
7. Labels and annotations are research governance / supervised target artifacts only; they are not runtime features.
8. EMA 3/19 values may be used as market-context or diagnostic features only; EMA is not a B/S/OUT label source.
9. FUTOI means participant positioning only and must not be interpreted as open interest unless a separate open-interest source is approved.

## 3. Approved scope

Created files:

- `docs/sot/strategies/ema_3_19_ai/phase2_contract_package_v1.md`
- `docs/sot/strategies/ema_3_19_ai/phase2_loader_materialization_boundary.md`
- `contracts/features/usdrubf_phase2_unified_external_feature_contract_v1.json`
- `contracts/features/usdrubf_phase2_d1_feature_export_v1.json`
- `contracts/features/usdrubf_futoi_participant_positioning_phase_features_v1.json`
- `contracts/datasets/futures_roll_expiry_mapping.v1.yaml`
- `contracts/validation/usdrubf_phase2_pit_availability_validation_v1.yaml`

No existing file is updated by this package.

## 4. Excluded scope

The following remain explicitly excluded:

- provider-specific source contracts;
- provider-specific calendar contracts;
- instrument registry changes;
- dataset registry changes;
- `src/**` changes;
- `tests/**` changes;
- runtime loader implementation;
- runtime materialization implementation;
- data loading or data mutation;
- ingestion;
- backfill;
- feature computation;
- statistics calculation;
- model fitting;
- prediction generation;
- server apply;
- Route B / n8n files.

## 5. Source / provider classification

This package defines a broad contract vocabulary only.

Provider status values:

| value | meaning |
|---|---|
| `approved_internal_context` | Internal source family may be referenced as design context, but this file does not authorize runtime materialization. |
| `design_placeholder` | Source family may be described only as a future candidate; provider approval is not implied. |
| `forbidden_now` | Source family remains blocked unless PM L2 changes the decision. |

Phase 2 usage status values:

| value | meaning |
|---|---|
| `design_only` | May appear in design contracts and validation rules only. |
| `blocked` | Must not be used for modeling, ingestion, calculation, or runtime. |
| `future_gate_required` | Requires a later PM-approved implementation gate before use. |

Provider placeholder rule:

```text
A placeholder source_id, provider_id, or calendar_id is not provider approval.
No Phase 2.3 artifact may imply provider ingestion readiness, source approval, calendar approval, modeling readiness, or runtime readiness.
```

## 6. PIT rules

The governing forecast anchor is:

```yaml
forecast_anchor_local: "06:00 Europe/Moscow"
forecast_anchor_field: forecast_anchor_ts
canonical_availability_field: availability_ts_utc
timestamp_storage: UTC
```

Rules:

1. `availability_ts_utc` is the canonical point-in-time availability field.
2. If an upstream artifact uses `availability_timestamp`, it is only a strict alias and must be converted to `availability_ts_utc`.
3. Rows that cannot be converted to UTC must be rejected or excluded.
4. A feature row is eligible only when `availability_ts_utc <= forecast_anchor_ts`.
5. D1 trade date `T` is usable only from `T+1 06:00 Europe/Moscow` or later.
6. Same-day unavailable data is forbidden.
7. Unknown release timestamp requires at least a one-trading-day conservative shift or exclusion.
8. Corrected, revised, filled, or vendor-restated values must not be joined into earlier anchors unless revision history is modeled point-in-time.

## 7. Labels boundary

Runtime / model feature export must not include label, annotation, interval, future-target, or label-derived metadata.

Denied feature evidence includes at least:

- `phase_label`
- `phase_remaining_sessions`
- `current_regime_ends_within_*`
- `next_regime_if_current_ends`
- interval identifiers
- interval start/end boundaries
- annotation fields
- label-derived metadata
- post-fact phase explanations

Labels may be used only for supervised target definition, offline research evaluation, or governance review after the relevant feature export is already point-in-time safe.

## 8. EMA boundary

EMA 3/19 features are context and diagnostics only.

Allowed design interpretation:

- price-context feature;
- trend-state diagnostic;
- lagged D1 indicator derived from approved D1 OHLC only after the D1 T+1 anchor rule is satisfied.

Forbidden interpretation:

- EMA as B/S/OUT label source;
- EMA as proof that a phase started or ended;
- EMA as replacement for supervised label governance;
- same-day EMA derived from unavailable close values.

## 9. FUTOI semantics

FUTOI in this package means participant positioning.

Canonical semantic boundary:

```text
FUTOI = participant positioning / participant position structure.
FUTOI != open interest.
```

The source-id mapping is defined in the FUTOI feature contract:

- canonical semantic source id: `futoi.participant_positioning`
- raw provider-family alias: `positioning.moex_algopack_futoi_raw`

No open-interest interpretation is allowed without a separate PM-approved open-interest source and contract.

## 10. Calendar and source exclusion policy

Provider-specific calendars and source files are excluded from Phase 2.3.

If calendar fields are referenced in broad contracts, they must be namespace-separated:

| namespace | allowed meaning |
|---|---|
| `schedule_known_before_anchor.*` | Deterministic schedule values known before `forecast_anchor_ts`. |
| `post_fact_outcome.*` | Realized decisions, outcomes, narratives, flow estimates, or post-event explanations. These are denied unless a later PM-approved PIT source contract permits them. |

This package does not create files under `contracts/calendars/**` or `contracts/sources/**`.

## 11. Modeling blocked status

Modeling remains explicitly blocked.

The following are not authorized by this package:

- train/test split;
- labels export;
- feature matrix generation;
- statistics;
- feature importance;
- model fit;
- predictions;
- daily assistant output;
- trading signal output.

## 12. Next implementation gates

Future work must be split into separately approved tasks:

1. Registry gate: instrument/dataset registry mutation, if required.
2. Loader/materialization gate: code and runtime design for allowed data sources.
3. Validation gate: PIT validation tests and contract tests.
4. Source-provider gate: provider-specific external source contracts.
5. Calendar-provider gate: provider-specific calendar contracts.
6. Modeling-readiness gate: feature export plus labels separation proof.
7. Server-apply gate: only after PM L2 approval and after repository Source of Truth is updated.

Until those gates are approved, this package remains design-only.
