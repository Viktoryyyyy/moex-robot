# ROUTE_B_RESULT_QUERY_AND_PM_L3_RETURN_INTERFACES.v1

Status: architecture contract package
Project: MOEX Bot
Scope: Route B Result Query v2 and PM L3 validation/evidence return intake

## Purpose

This package closes the Route B evidence-return gap after the GitHub Branch/PR Executor package.

Route B can already create PM L3 task packages and can prepare repository implementation cycles through GitHub feature branches and pull requests. PM L2 still needs a formal full-evidence result query and a formal PM L3 return interface before PM L2 can review a complete cycle.

This package defines repository-level contracts only. It does not define n8n workflow JSON, Postgres DDL, credentials, merge automation, runtime/live logic, broker logic, strategy logic, research logic, or backtest logic.

## Required chain

```text
PM L2
-> Route B Intake
-> PM L3 package generation
-> Sub-chat / GitHub Branch PR Executor
-> PM L3 validation return
-> PM L2 review / verdict
```

Sub-chat output returns to PM L3. PM L3 validates and returns evidence to PM L2. PM L2 owns final verdict and merge approval authority.

## Result Query v2

Result Query v2 lets PM L2 retrieve the complete Route B cycle evidence package. It is not a status endpoint.

The request contract is:

```text
contracts/route_b/route_b_result_query_request.v1.yaml
```

The response contract is:

```text
contracts/route_b/route_b_result_query_response.v1.yaml
```

A PM L2 result query must request these sections:

```text
status
pm_l3_package
github_execution_result
pr_validation_package
pm_l3_return_package
events
steps
authority_boundary
```

A status-only response is insufficient for PM L2 verdict.

Missing evidence sections must be explicit. If a section is unavailable, the response must set the section to null and include a blocker note naming the missing section. Silent omission is rejected.

## Result Query response contents

The response records:

- workflow and idempotency identity
- repository binding
- status/current_state/current_phase
- PM L3 package reference or payload
- GitHub execution result reference or payload
- PR validation package reference or payload
- PM L3 return package reference or payload
- evidence refs
- PR refs
- CI refs
- changed file refs
- blockers
- required fixes
- PM L2 review flag
- authority boundary
- events summary
- steps summary

PM L2 receives the full evidence package. PM L2 must not infer approval from status, PR existence, or CI success.

## PM L3 Return Intake

The PM L3 Return Intake receives validation/evidence from PM L3 after sub-chat output, GitHub PR execution, CI validation, or blocker classification.

The intake request contract is:

```text
contracts/route_b/route_b_pm_l3_return_intake_request.v1.yaml
```

The PM L3 return package contract is:

```text
contracts/route_b/route_b_pm_l3_return_package.v1.yaml
```

Allowed return types:

```text
pm_l3_after_subchat_validation
pm_l3_after_github_pr_validation
pm_l3_blocker_return
```

The source role must be `PM_L3_DELIVERY_VALIDATION_OWNER`.

The return target must be `PM_L2_PHASE_OWNER`.

Sub-chats must not return directly to PM L2.

## PM L3 return package contents

The return package records:

- PM L3 validation status: pass, conditional_pass, fail, or blocked
- acceptance criteria check
- repo scope check
- artifact contract check
- test or CI check
- PR refs
- CI refs
- changed file refs
- blockers
- required fixes
- final PM L2 review requirement
- PM L2 decision needed
- authority boundary

PM L3 may mark that PM L2 review is required. PM L3 must not claim PM L2 approval, approve merge, change PM L2 scope, or merge.

## Authority boundary

PM L2 owns:

- final review
- final phase verdict
- merge approval authority
- any merge approval outside n8n automation

PM L3 owns:

- validation/evidence return
- blocker classification
- acceptance criteria validation against assigned scope

PM L3 does not own:

- final PM L2 verdict
- merge approval
- scope change

Sub-chat owns:

- returning implementation or validation evidence to PM L3 only

Sub-chat does not own:

- direct return to PM L2
- PM L2 verdict
- merge approval

n8n may:

- store evidence
- expose evidence
- route evidence through declared interfaces

n8n may not:

- infer approval
- merge
- write directly to main
- force push
- delete files

## CI boundary

GitHub Actions workflow `tests` may prove that repository checks passed for the PR head SHA.

CI success does not imply merge approval.

`ci_passed` must never be treated as `approved_for_merge`.

## Python validator boundary

Validator path:

```text
src/moex_core/contracts/route_b_result_return.py
```

The validator fails closed for:

- missing workflow_run_id where required
- missing repository_full_name
- repository mismatch when an expected repository is provided
- source_role not `PM_L3_DELIVERY_VALIDATION_OWNER`
- return_to_role not `PM_L2_PHASE_OWNER`
- missing authority_boundary
- PM L2 review not required for full evidence or PM L3 return package
- merge_performed_by_executor=true
- n8n_merge_allowed=true
- direct_main_write_allowed=true
- force_push_allowed=true
- file_delete_allowed=true
- approved_for_merge=true without explicit PM L2 approval package
- sub-chat return directly to PM L2
- status-only Result Query response used as PM L2 verdict package
- CI success treated as merge approval

## Approved file scope

```text
docs/sot/route_b/result_query_and_pm_l3_return_interfaces.v1.md
contracts/route_b/route_b_result_query_request.v1.yaml
contracts/route_b/route_b_result_query_response.v1.yaml
contracts/route_b/route_b_pm_l3_return_intake_request.v1.yaml
contracts/route_b/route_b_pm_l3_return_package.v1.yaml
src/moex_core/contracts/route_b_result_return.py
tests/contract/test_route_b_result_query_contract.py
tests/contract/test_route_b_pm_l3_return_contract.py
tests/contract/test_route_b_pm_l3_to_pm_l2_authority_boundary.py
```

## Out of scope

- n8n workflow JSON
- Intake Ack edits
- Worker Poller edits
- Status Query edits
- Watchdog edits
- GitHub Branch/PR Executor JSON edits
- Postgres DDL
- credentials
- secrets
- GitHub token changes
- merge automation
- direct main write
- force push
- file delete implementation
- runtime/live/trading/broker logic
- strategy/research/backtest logic
- server commands
