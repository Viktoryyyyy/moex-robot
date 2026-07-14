# EMA 3/19 AI — Phase 1.1 Wording Hardening

Status: design artifact only  
Lane: `ema_3_19_ai`  
Runtime impact: none  
Modeling status: blocked

## 1. Purpose

This document records validated wording hardening rules for the EMA 3/19 AI Phase 1 design package.

It is a docs-only artifact. It does not authorize modeling, calculations, ingestion, backfill, runtime behavior, server apply, code implementation, tests, contracts, configs, or Route B / n8n changes.

## 2. Validated wording rules

### 2.1 Runtime output name

Use the following name for runtime-oriented phase output:

```text
predicted_current_phase_for_next_actionable_session
```

Do not replace it with ambiguous wording such as current phase, observed phase, or manual phase in production context.

### 2.2 No observed manual phase in runtime context

Do not write:

```text
observed current manual phase
```

for runtime context.

Manual labels are offline targets and audit artifacts only.

### 2.3 Manual labels are target-only

Manual labels are `y` / target only for offline supervised training and evaluation.

Manual labels are never:

- features;
- input fields;
- runtime state;
- assistant state;
- source availability rows;
- production context values.

### 2.4 EMA 3/19 wording

EMA 3/19 is `diagnostic_only`.

It may be analyzed against labels in offline research, but it is not the label source.

### 2.5 Calendar wording

Calendar wording must separate:

```text
scheduled_event_known_before_anchor
```

from post-fact outcomes.

Post-fact outcomes include realized decisions, realized rates, meeting results, official outcomes, and retrospective narratives.

### 2.6 Source availability matrix wording

A source availability matrix must describe only feature/data source availability.

It must not describe label or annotation availability as model features.

### 2.7 Daily assistant wording

Daily assistant wording must be advisory-only.

No assistant wording may imply bypass of strategy, risk, or execution controls.

### 2.8 Offline vs production wording

Offline research/audit wording must be separated from production/runtime wording.

Observed manual labels belong to offline research and evaluation.

Predicted runtime output belongs to production-oriented inference and must be named `predicted_current_phase_for_next_actionable_session`.

## 3. Bad / good wording examples

### 3.1 Manual phase in runtime context

Bad:

```text
model uses the current manual phase
```

Good:

```text
model predicts predicted_current_phase_for_next_actionable_session from point-in-time available features
```

### 3.2 EMA label source

Bad:

```text
EMA crossover defines B/S/OUT labels
```

Good:

```text
EMA 3/19 is diagnostic_only and may be analyzed against labels but does not define labels
```

### 3.3 Assistant authority

Bad:

```text
assistant decides whether to enter
```

Good:

```text
assistant returns advisory output subject to strategy, risk, and execution controls
```

### 3.4 Calendar outcomes

Bad:

```text
the model uses meeting results at the morning forecast anchor
```

Good:

```text
the model may use scheduled_event_known_before_anchor when the event was known before forecast_anchor_ts, while post-fact outcomes are unavailable until their release timestamp
```

### 3.5 Label availability as feature availability

Bad:

```text
manual label availability is included in the source availability matrix for model inputs
```

Good:

```text
the source availability matrix describes feature/data source availability only; manual labels remain offline supervised targets
```

## 4. Required wording posture

All future docs, specs, contracts, implementation prompts, validation reports, and assistant output descriptions for this lane must preserve these wording constraints.

Any wording that converts manual labels into runtime context or allows assistant bypass of controls must be corrected before modeling or implementation proceeds.