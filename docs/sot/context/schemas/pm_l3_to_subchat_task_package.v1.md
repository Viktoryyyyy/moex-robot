# PM L3 to Sub-chat Task Package v1

schema_id: pm_l3_to_subchat_task_package.v1
producer: PM_L3_DELIVERY_VALIDATION_OWNER
consumer: SUBCHAT_BASE_or_variant

## Required shape

```yaml
subchat_task_package:
  schema_id: pm_l3_to_subchat_task_package.v1
  project: MOEX Bot
  target_subchat_role:
    role_id: <registered_subchat_role_id>
    role_context_version: v1
  static_context_refs:
    - <registered_static_context_ref>
  dynamic_handoff:
    original_task: <task>
    assigned_scope: <scope>
    not_in_scope: <boundaries>
    evidence_packet_ref: <evidence_ref_or_none>
    acceptance_criteria: <criteria>
    expected_return_to: PM_L3_DELIVERY_VALIDATION_OWNER
    required_output_contract: <exact_return_contract>
    stop_conditions: <stop_conditions>
```

## Rules

- PM L3 targets exactly one sub-chat role.
- Sub-chat must return to PM L3.
- Task package must not ask the sub-chat to recommend next roles or roadmap continuation.
