# EMA 3/19 AI — Daily Assistant Decision Contract

Status: design artifact only  
Lane: `ema_3_19_ai`  
Runtime impact: none  
Modeling status: blocked

## 1. Purpose

This document defines the advisory output contract for the EMA 3/19 AI daily market assistant.

It is a design/spec artifact only. It does not create executable behavior, runtime deployment, code, tests, contracts, configs, ingestion, backfill, calculations, server apply, or Route B / n8n changes.

## 2. Advisory-only boundary

Assistant output is advisory-only.

No assistant output may bypass strategy, risk, or execution controls.

The assistant cannot independently approve trading actions, execution actions, position changes, size changes, risk overrides, or control overrides.

Any final action must remain subject to the approved strategy, risk, and execution control stack.

## 3. Output contract fields

A later approved implementation must include these fields:

```text
instrument
forecast_anchor_ts
data_as_of_ts
predicted_current_phase_for_next_actionable_session
phase_probabilities for B/S/OUT
transition_hazard if available
next_phase_probabilities if available
confidence
evidence_summary
contradiction_flags
bias_gate_status
recommended_action_class as advisory text only
decision_status
required_human_review_reason when applicable
```

Allowed `decision_status` values:

```text
no_decision
human_review
monitor
advisory_ok
```

## 4. Field semantics

`instrument` identifies the covered instrument or instrument family.

`forecast_anchor_ts` is the forecast anchor timestamp, defined as `06:00 Europe/Moscow` unless superseded by an approved contract.

`data_as_of_ts` is the latest timestamp of source data included after point-in-time availability validation.

`predicted_current_phase_for_next_actionable_session` is the model-predicted phase for the next actionable session. It must not be described as an observed manual phase.

`phase_probabilities for B/S/OUT` is the probability distribution across B, S, and OUT.

`transition_hazard if available` is the estimated probability that the current predicted phase is ending over defined horizons.

`next_phase_probabilities if available` is the probability distribution for the next phase among B, S, and OUT after transition.

`confidence` is the assistant-level confidence assessment subject to calibration and governance rules.

`evidence_summary` is a concise explanation of the data and signals considered, without future or unavailable information.

`contradiction_flags` records contradictions between data, model components, calendar risks, and strategy/risk/execution controls.

`bias_gate_status` records required gates that may force `no_decision` or `human_review`.

`recommended_action_class` is advisory text only. It is not an execution instruction or approval.

`required_human_review_reason` is required when `decision_status` is `human_review` or when a critical gate fails.

## 5. Bias gates

The assistant must evaluate these gates when implemented by a later approved task:

- stale or missing data;
- failed availability/leakage validation;
- model confidence below threshold;
- contradiction between phase model and transition model;
- contradiction with strategy/risk/execution controls;
- known calendar risk not safely represented;
- out-of-distribution conditions.

## 6. Default behavior

Contradiction default is `no_decision` or `human_review`.

Missing critical data default is `no_decision` or `human_review`.

Failed availability or leakage validation default is `no_decision` or `human_review`.

The assistant cannot independently approve entries, exits, or execution actions.

## 7. Label and calendar boundary

Manual labels are offline supervised targets only and must not be assistant-visible runtime state.

Runtime output wording must use `predicted_current_phase_for_next_actionable_session`.

The assistant may use `scheduled_event_known_before_anchor` only if the event information was known before `forecast_anchor_ts`.

Post-fact outcomes, realized decisions, realized rates, meeting results, and outcome narratives must not be used before their availability timestamp.

## 8. Final control rule

No assistant wording may imply that advisory output bypasses strategy, risk, or execution controls.