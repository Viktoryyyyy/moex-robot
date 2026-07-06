# EMA 3/19 AI — Phase 1 Experiment Design

Status: design artifact only  
Lane: `ema_3_19_ai`  
Runtime impact: none  
Modeling status: blocked

## 1. Scope

This document defines the design-only experiment structure for the EMA 3/19 AI market phase research path.

It does not authorize modeling, fitting, calculation, backfill, ingestion, feature computation, runtime deployment, server apply, tests, contracts, configs, or Route B / n8n changes.

## 2. Experiment components

The Phase 1 experiment design is split into four conceptual components:

1. `current_phase_classifier`;
2. `transition_hazard_model`;
3. `next_phase_model`;
4. daily assistant design.

Each component must be validated under point-in-time availability rules before any modeling task can proceed.

## 3. current_phase_classifier

### 3.1 Purpose

The `current_phase_classifier` is designed to predict the current research phase for the next actionable session using only features available at the forecast anchor.

The required runtime output name is:

```text
predicted_current_phase_for_next_actionable_session
```

This wording is mandatory because production must not imply that the model observes the current manual phase.

### 3.2 Target

The target is the manual phase label only in supervised offline training and evaluation.

Manual labels are not features, input fields, runtime state, assistant-visible state, or source availability rows.

### 3.3 Feature boundary

Only point-in-time available features may be used.

Every candidate feature must satisfy:

```text
availability_ts <= forecast_anchor_ts
```

Feature availability must be proven by explicit source timestamps or controlled through conservative lag assumptions.

## 4. transition_hazard_model

### 4.1 Purpose

The `transition_hazard_model` estimates the probability that the current phase is ending over defined horizons.

The horizons must be defined before modeling and evaluated without using future labels or future returns as features.

### 4.2 Leakage boundary

The transition model must not use:

- future labels;
- future returns;
- future OHLCV;
- post-fact macro outcomes before release;
- corrected or revised values unless they are modeled point-in-time;
- manually adjusted label information as a feature.

## 5. next_phase_model

### 5.1 Purpose

The `next_phase_model` estimates the likely next phase among B / S / OUT after a transition.

It answers a different question from the current-phase classifier and must be evaluated separately.

### 5.2 Evaluation separation

Evaluation must not merge current-phase classification quality with next-phase transition quality.

Required separation:

- current phase prediction quality;
- transition end probability quality;
- next phase probability quality after a transition.

## 6. Daily assistant design

### 6.1 Authority

The daily assistant is advisory-only.

It has no trade execution authority and cannot authorize entries, exits, order placement, position changes, or risk overrides independently.

No assistant output may bypass strategy, risk, or execution controls.

### 6.2 Required assistant behavior

The assistant must provide:

- uncertainty;
- contradiction flags;
- bias gate status;
- evidence summary;
- human review triggers;
- advisory action classification only.

Contradictions, missing critical data, or failed availability checks must default to `no_decision` or `human_review`.

## 7. Validation design

Validation must be time-ordered and point-in-time.

Required validation controls:

- walk-forward or other time-ordered validation;
- point-in-time joins;
- no same-day or future leakage;
- `availability_ts <= forecast_anchor_ts` for every feature row;
- embargo/purge where needed around label boundaries;
- separate offline label audit from runtime simulation;
- separate runtime simulation from supervised label review;
- no post-fact calendar outcome use before availability.

## 8. Metrics

### 8.1 Classifier metrics

Candidate metrics for B / S / OUT class imbalance:

- balanced accuracy;
- macro F1;
- MCC or similar class-imbalance-aware metric;
- confusion matrix by B / S / OUT.

### 8.2 Probability and calibration metrics

Candidate probability metrics:

- Brier score;
- calibration curve or equivalent calibration review;
- probability sharpness review subject to calibration quality.

### 8.3 Transition metrics

Candidate transition metrics:

- transition detection lead/lag;
- false transition alarms;
- missed transition events;
- horizon-specific transition probability quality.

### 8.4 Assistant contract metrics

Candidate assistant governance metrics:

- `no_decision` rate;
- `human_review` rate;
- contradiction rate;
- stale or missing data gate rate;
- failed availability/leakage gate rate.

## 9. Failure modes

The experiment design must explicitly control the following failure modes:

- leakage;
- class imbalance;
- label drift;
- small sample / overfit;
- post-fact macro or calendar data;
- EMA proxy misuse;
- feature availability ambiguity;
- contradiction between model output and strategy/risk/execution controls;
- stale data;
- calendar outcome leakage;
- confusion between offline label audit and runtime simulation.

Any unresolved leakage violation blocks modeling.