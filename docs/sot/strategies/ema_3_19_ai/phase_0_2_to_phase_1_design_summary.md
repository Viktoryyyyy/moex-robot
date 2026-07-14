# EMA 3/19 AI — Phase 0.2 to Phase 1 Design Summary

Status: design artifact only  
Lane: `ema_3_19_ai`  
Runtime impact: none  
Modeling status: blocked

## 1. Root objective

The research objective is to study USDRUBF / Si market phases on daily data and use the results to design a controlled AI-assisted strategy workflow.

The target workstream has three goals:

1. characterize B / S / OUT market phases;
2. design a modeling path for estimating current phase completion and likely next phase;
3. design a daily market assistant that can support review without bypassing strategy, risk, or execution controls.

## 2. Consolidated Phase 0.2 status

Phase 0.2 is treated as the accepted design/spec foundation for the Phase 1 design package.

The accepted foundation establishes the following constraints:

- manual research labels are supervised targets only;
- manual labels must not become features, runtime input fields, runtime state, assistant-visible state, or source availability rows;
- runtime wording must use `predicted_current_phase_for_next_actionable_session`, not observed manual phase wording;
- EMA 3/19 is `diagnostic_only` and does not define labels;
- point-in-time availability must control every feature row used in prediction;
- calendar data must separate information known before the forecast anchor from post-fact outcomes;
- assistant output is advisory-only and cannot bypass strategy, risk, or execution controls.

## 3. Phase 1 Design-Only status

Phase 1 Design-Only is accepted for docs artifact creation.

This document set captures design/spec conclusions from Phase 0.2 through Phase 1.1. It does not authorize modeling, calculations, ingestion, backfill, runtime behavior, server apply, code implementation, tests, contracts, configs, or Route B / n8n changes.

## 4. Phase 1 Modeling remains blocked

Phase 1 Modeling remains explicitly blocked until the design/spec package is reviewed and a separate implementation or modeling task is authorized.

These docs are not model outputs. They do not contain fitted parameters, prediction results, statistics, feature computation, or executable logic.

## 5. Accepted design constraints

### 5.1 Label and runtime semantics

Manual phase labels are target annotations for offline supervised training and evaluation only.

The runtime output must be named:

```text
predicted_current_phase_for_next_actionable_session
```

Production wording must not imply that the model or assistant observes the current manual phase.

### 5.2 EMA 3/19 status

EMA 3/19 is `diagnostic_only`.

It may be analyzed against labels during offline research, but it is not the label source and must not be described as defining B / S / OUT labels.

### 5.3 Forecast anchor

The forecast anchor is:

```text
06:00 Europe/Moscow
```

All runtime and simulation language must be anchored to that timestamp unless a later approved contract supersedes it.

### 5.4 D1 T+1 06:00 availability rule

Daily D1 data for date `T` may be used only from `T+1 06:00 Europe/Moscow`, unless a stricter source-specific availability timestamp is proven.

Same-day close data or post-close derived data must not be used before its approved availability timestamp.

### 5.5 Feature availability rule

Every feature row used for prediction must satisfy:

```text
availability_ts <= forecast_anchor_ts
```

If feature availability cannot be proven, a conservative lag must be applied or the feature must be excluded.

### 5.6 Calendar event separation

Calendar information must distinguish:

- `scheduled_event_known_before_anchor`: event schedule or existence known before `forecast_anchor_ts`;
- post-fact outcomes: realized decisions, realized rates, meeting results, official outcomes, and narratives available only after release/publication.

Only information known before the forecast anchor may be used before the event outcome becomes available.

### 5.7 Source availability matrix boundary

A source availability matrix may describe feature/data source availability only.

It must not include label or annotation availability as feature rows. Label availability belongs to offline audit/training governance, not to runtime feature availability.

### 5.8 Assistant authority boundary

No assistant output may bypass strategy, risk, or execution controls.

The assistant may provide advisory context, uncertainty, contradictions, bias gate status, and human review triggers. It cannot authorize entries, exits, order placement, position changes, or risk overrides independently.

## 6. Artifact boundary

These files are docs-first repository artifacts only.

They create no executable implementation and do not change runtime behavior.