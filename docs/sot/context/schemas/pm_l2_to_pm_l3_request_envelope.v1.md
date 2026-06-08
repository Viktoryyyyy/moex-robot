# PM L2 to PM L3 Request Envelope v1

schema_id: pm_l2_to_pm_l3_request_envelope.v1
producer: PM_L2_PHASE_OWNER
consumer: PM_L3_DELIVERY_VALIDATION_OWNER

## Required shape

```yaml
route_b_request:
  schema_id: pm_l2_to_pm_l3_request_envelope.v1
  project: MOEX Bot
  request_id: <stable_request_id>
  static_context_refs:
    - <registered_static_context_ref>
  role_context_ref:
    role_id: PM_L3_DELIVERY_VALIDATION_OWNER
    role_context_version: v1
  dynamic_handoff:
    original_task: <task>
    phase_goal: <goal>
    approved_scope: <scope>
    not_in_scope: <boundaries>
    evidence_packet_ref: <repo_or_pr_ref_or_none>
    decisions_already_made: <decisions>
    blockers: <blockers_or_none>
    acceptance_criteria: <criteria>
    required_next_output: pm_l3_subchat_task_package
    stop_conditions: <stop_conditions>
```

## Rules

- Envelope contains context refs plus dynamic_handoff.
- Envelope must not paste full static context or full role context.
- PM L2 sends this envelope to PM L3, not directly to sub-chat.
