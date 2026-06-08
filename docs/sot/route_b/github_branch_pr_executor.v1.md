# ROUTE_B_GITHUB_BRANCH_PR_EXECUTOR.v1

Status: architecture contract package
Project: MOEX Bot
Scope: Route B repository implementation cycle through GitHub feature branches and pull requests

## Purpose

Branch/PR Executor v1 moves Route B beyond PM-package generation and defines the repo-first boundary for implementation cycles.

The executor may prepare repository changes only through a feature branch and pull request. It collects evidence for PM L3 validation and PM L2 review. It does not own final approval and must not merge.

## Required flow

```text
PM L2
-> Route B Intake
-> PM L3 package generation
-> GitHub execution request
-> Branch/PR Executor
-> feature branch mutation only
-> PR to main
-> GitHub Actions tests
-> PM L3 validation package
-> PM L2 review / merge authority
```

## Branch rule

The branch name is deterministic:

```text
n8n/<workflow_run_id>-<short_request_id>
```

The branch prefix must be `n8n/`.

`route-b/` is rejected.

## Authority boundary

Allowed executor actions:

- create feature branches
- commit to feature branches
- open pull requests to `main`
- update pull request comments
- read pull request status
- read GitHub Actions status
- collect evidence

Forbidden executor actions:

- write directly to `main`
- merge into `main`
- bypass PM L2 approval
- force push
- delete files
- create credentials
- create secrets
- change runtime/live trading logic
- change broker integration
- change strategy, research, or backtest logic

Merge authority is `PM_L2_ONLY`.

A passing GitHub Actions workflow is evidence for PM L3 validation, not approval for merge.

## Contracts

Machine-readable contracts:

- `contracts/route_b/route_b_github_execution_request.v1.yaml`
- `contracts/route_b/route_b_github_execution_result.v1.yaml`
- `contracts/route_b/route_b_pr_validation_package.v1.yaml`

Python validator boundary:

- `src/moex_core/contracts/route_b_github_execution.py`

## Request package

The request package declares:

- exact repository
- base branch and base SHA
- context registry binding
- PM L2 and PM L3 package refs
- execution scope
- branch plan
- PR plan
- validation requirements
- governance flags
- rejection rules

The request must include `role_context_ref`.

`target_role_context_ref` is forbidden.

Dynamic markers `latest`, `current`, and `autodetect` are forbidden.

## Result package

The result package records:

- feature branch
- branch ref
- implementation commit and tree
- changed files
- pull request refs
- GitHub Actions refs
- evidence refs
- authority boundary
- error object when blocked or failed

The branch ref must point to the implementation commit.

Deleted files are forbidden.

A PR without CI refs is incomplete evidence.

CI refs must point to the PR head SHA.

`ci_passed` does not imply `approved_for_merge`.

`merge_performed_by_executor` must remain false.

## PR validation package

The PR validation package records:

- context registry binding
- scope validation
- git refs
- PR refs
- CI refs
- changed file refs
- PM L3 validation status
- PM L2 boundary

PM L2 review is mandatory.

`approved_for_merge` must remain false unless an explicit PM L2 approval package exists.

n8n merge remains forbidden even when CI passes.

## Phase 1 approved file scope

```text
docs/sot/route_b/github_branch_pr_executor.v1.md
contracts/route_b/route_b_github_execution_request.v1.yaml
contracts/route_b/route_b_github_execution_result.v1.yaml
contracts/route_b/route_b_pr_validation_package.v1.yaml
src/moex_core/contracts/route_b_github_execution.py
tests/contract/test_route_b_github_execution_contracts.py
tests/contract/test_route_b_github_executor_authority_boundaries.py
tests/contract/test_route_b_github_registry_integration.py
tests/contract/test_route_b_pr_validation_package_contract.py
```

No n8n workflow JSON, credentials, secrets, Postgres DDL, merge automation, server authoring, runtime/live trading logic, broker integration, strategy, research, or backtest logic is included in this package.
