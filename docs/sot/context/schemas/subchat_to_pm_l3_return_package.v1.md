# Sub-chat to PM L3 Return Package v1

schema_id: subchat_to_pm_l3_return_package.v1
producer: SUBCHAT_BASE_or_variant
consumer: PM_L3_DELIVERY_VALIDATION_OWNER

## Required shape

```yaml
subchat_return_package:
  schema_id: subchat_to_pm_l3_return_package.v1
  project: MOEX Bot
  return_to: PM_L3_DELIVERY_VALIDATION_OWNER
  source_subchat_role:
    role_id: <registered_subchat_role_id>
    role_context_version: v1
  task_alignment_check:
    original_task: <task>
    assigned_scope: <scope>
    evidence_packet_ref: <evidence_ref_or_none>
    does_assigned_scope_match_original_task: yes_or_no
    if_no: <explanation_or_empty>
  role_specific_report: <implementation_or_validation_or_audit_report>
  role_step_report:
    alignment_with_root_task: <alignment>
    done: <completed_items>
    not_done: <incomplete_items>
    blockers: <blockers_or_none>
    next_step: none
```

## Rules

- Sub-chat return target is PM L3.
- Sub-chat does not issue PM L2 verdict.
- `next_step` is `none` unless the assigned return contract explicitly requires otherwise.
