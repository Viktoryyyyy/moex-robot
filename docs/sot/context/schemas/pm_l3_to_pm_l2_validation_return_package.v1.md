# PM L3 to PM L2 Validation Return Package v1

schema_id: pm_l3_to_pm_l2_validation_return_package.v1
producer: PM_L3_DELIVERY_VALIDATION_OWNER
consumer: PM_L2_PHASE_OWNER

## Required shape

```yaml
pm_l3_validation_return_package:
  schema_id: pm_l3_to_pm_l2_validation_return_package.v1
  project: MOEX Bot
  return_to: PM_L2_PHASE_OWNER
  source_pm_l3_role:
    role_id: PM_L3_DELIVERY_VALIDATION_OWNER
    role_context_version: v1
  original_task: <task>
  approved_scope: <scope>
  subchat_return_ref: <subchat_return_ref>
  evidence_summary: <evidence>
  acceptance_criteria_check: <checklist>
  validation_verdict: pass_or_conditional_pass_or_fail_or_blocked
  blockers: <blockers_or_none>
  role_step_report:
    alignment_with_root_task: <alignment>
    done: <completed_items>
    not_done: <incomplete_items>
    blockers: <blockers_or_none>
    next_step: none
```

## Rules

- PM L3 returns validation and evidence to PM L2.
- PM L3 does not make PM L2 closeout decision.
- GitHub/repo proof remains the primary Source of Truth for repo changes.
