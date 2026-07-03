# EMA 3/19 AI — Phase Label Semantics

Status: design artifact only  
Lane: `ema_3_19_ai`  
Runtime impact: none  
Modeling status: blocked

## 1. Purpose

This document defines B / S / OUT label semantics for the EMA 3/19 AI research path.

It is a research-label and wording-governance artifact only. It does not create executable behavior, runtime state, model output, contracts, configs, tests, ingestion, backfill, or server changes.

## 2. Research phase labels

B / S / OUT are research phase labels.

### 2.1 B

`B` means buy / long-directional phase according to the strategy label convention.

### 2.2 S

`S` means sell / short-directional phase according to the strategy label convention.

### 2.3 OUT

`OUT` means outside market / no active directional phase / neutral phase according to the strategy label convention.

## 3. Label role in research

Manual labels are supervised targets `y` for offline training and evaluation only.

Manual labels may be used for:

- supervised offline target definition;
- offline evaluation;
- offline label audit;
- research documentation.

Manual labels must never be used as:

- features;
- input fields;
- runtime state;
- assistant-visible state;
- source availability rows;
- production context fields;
- execution signals.

## 4. 2025-09-11 OUT rule

The manual research label for `2025-09-11` is `OUT`.

This is a target / annotation rule only.

It must not be converted into runtime state, model input, assistant context, feature value, or execution rule.

## 5. EMA 3/19 boundary

EMA 3/19 is `diagnostic_only`.

EMA 3/19 may be analyzed against labels during offline research, but it is not the source of B / S / OUT labels.

Forbidden wording:

```text
EMA crossover defines B/S/OUT labels.
```

Required meaning:

```text
EMA 3/19 is diagnostic_only and may be analyzed against manual research labels, but it does not define labels.
```

## 6. Observed manual labels vs predicted runtime phase

Observed manual phase labels belong to offline supervised research and evaluation.

Predicted runtime phase belongs to production-oriented inference wording and must use:

```text
predicted_current_phase_for_next_actionable_session
```

The assistant or model must not be described as observing the current manual phase in production.

## 7. Runtime wording prohibition

The following production wording is forbidden:

```text
The model uses the current manual phase.
The assistant observes the current B/S/OUT label.
The runtime state is the manual phase label.
The model knows the current manual label.
```

Acceptable production wording:

```text
The model predicts predicted_current_phase_for_next_actionable_session from point-in-time available features.
```

## 8. Leakage boundary

Manual labels are target annotations.

Any use of manual labels as features, runtime inputs, assistant state, source availability rows, or production context is leakage and blocks modeling.