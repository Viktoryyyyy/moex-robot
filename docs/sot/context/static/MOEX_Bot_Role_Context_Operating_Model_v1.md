# MOEX Bot Role Context Operating Model v1

context_ref: MOEX_Bot_Role_Context_Operating_Model_v1
status: active_static_context_ref
source_level: project_operating_model

## Purpose

This file is the Route B resolvable static context reference for the MOEX Bot role/context operating model.

## Context classes

### Static context

Long-lived project canon: target architecture, GitHub Source of Truth, server Applied State, artifact contracts, GitHub commit flow, PR-first CI, global constraints.

Static context is referenced, not pasted.

### Role context

Stable behavior contract per role: mandate, authority, forbidden actions, required checks, expected output, escalation rules.

Role context is referenced, not pasted.

### Dynamic context

Cycle-specific handoff: original task, approved scope, current goal, evidence packet reference, decisions already made, blockers, acceptance criteria, required output.

Dynamic context is the only normal handoff payload between Route B roles.

## Route B management chain

`PM L2 -> PM L3 -> Sub-chat -> PM L3 -> PM L2`

PM L2 forms phase-level requests. PM L3 converts them into executable sub-chat task packages. Sub-chat returns to PM L3. PM L3 validates evidence and returns to PM L2.

## Analysis rule

Each cycle has one analysis owner. Downstream roles consume existing evidence and validate sufficiency instead of repeating full analysis.

## Required alignment check

Every material role step must compare current scope to the original task and report done, not done, blockers, and next_step.

## Route B relevance

This operating model is the static context basis for compact n8n Route B envelopes and role references.
