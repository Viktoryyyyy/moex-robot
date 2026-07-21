# MOEX Bot Management Canon

status: active
version: 1.0
adopted_at: 2026-07-21
repository: Viktoryyyyy/moex-robot

## 1. Purpose

This document is the authoritative management contract for MOEX Bot delivery across Browser-controlled GitHub work and Flowise-automated GitHub work.

It defines:
- active execution routes;
- role ownership;
- task identity;
- scope and authority;
- GitHub state control;
- handoff and intake rules;
- concurrency locks;
- review, CI, merge and server-apply gates;
- route transfer and timeout reconciliation;
- canonical result reporting;
- deprecated execution modes.

If another management document conflicts with this document, this document prevails unless the owner explicitly approves a newer version.

## 2. Source of Truth

```text
GitHub repository = Source of Truth
Server filesystem = Applied State only
```

The server filesystem is not architectural proof and must not be used to infer repository structure, current branch state, accepted implementation or management authority.

Canonical repository:

```text
Viktoryyyyy/moex-robot
```

Canonical server context:

```text
HOME=/home/trader
repo_path=/home/trader/moex_bot/moex-robot
shell_path=~/moex_bot/moex-robot
command_prefix=cd ~/moex_bot && source venv/bin/activate && cd moex-robot
```

Deprecated underscore paths must not be used.

## 3. Active execution routes

Exactly two execution routes are active.

### 3.1 Browser Controlled GitHub Route

```text
execution_mode: browser_controlled_github_route
```

Use for:
- complex or high-risk tasks;
- tasks requiring explicit control of each stage;
- multi-role delivery;
- broad repository analysis;
- sensitive scope or authority boundaries;
- tasks where PM L2 requires direct inspection before each mutation.

The Browser route may perform direct GitHub read and write operations through authorized roles. Direct writes to `main` remain forbidden.

### 3.2 Flowise Automated GitHub Route

```text
execution_mode: flowise_automated_github_route
```

Use for:
- formalized GitHub tasks;
- exact file scope;
- explicit acceptance criteria;
- standard branch → implementation → PR → review → correction → CI lifecycle;
- delegated automatic merge when PM L2 explicitly authorizes it.

The Flowise Lead controls orchestration, review, checks and authorized merge. `github_worker` performs file mutations and never merges.

### 3.3 Closed route

```text
execution_mode: route_b_n8n_universal_role_runner
status: deprecated
new_tasks_allowed: false
```

Route B / n8n is closed for new delivery. Historical evidence may remain in the repository, but it does not authorize new Route B tasks, branches, PRs or server actions.

## 4. Management hierarchy

```text
PM L1 = control tower and root task owner
PM L2 = phase owner, scope owner and merge authority owner
PM L3 = delivery validation owner when assigned
Subchat / Browser implementation role = delegated executor
Flowise Lead = automated route orchestrator
Flowise github_worker = delegated implementation worker
```

### 4.1 PM L1

PM L1 owns:
- portfolio and task registry;
- root task identity;
- route coordination;
- cross-lane conflicts;
- closure and supersession;
- management canon compliance.

PM L1 does not widen technical scope without PM L2 ownership.

### 4.2 PM L2

PM L2 owns:
- current goal;
- task decomposition;
- approved and forbidden scope;
- acceptance criteria;
- route selection or approval;
- branch and PR ownership;
- merge policy;
- merge delegation;
- server-apply authorization;
- final acceptance.

Flowise is an execution route, not the scope owner and not the final acceptance owner.

### 4.3 PM L3

PM L3 validates delivery when assigned, including:
- repository evidence;
- exact changed files;
- diff quality;
- acceptance criteria;
- CI on the exact head SHA;
- unresolved review findings;
- readiness for PM L2 decision.

### 4.4 Executors

Executors must:
- stay within approved scope;
- use the assigned branch and PR;
- preserve task identity;
- return factual GitHub evidence;
- stop on authority conflict, scope conflict or stale state.

Executors must not:
- write directly to `main`;
- create a replacement branch or PR without authorization;
- widen scope;
- merge without delegated authority;
- apply to the server without separate authorization;
- invent branch, SHA, PR, checks, review or merge state.

## 5. Task identity

Every managed task uses:

```text
project: MOEX_Bot
root_task_id: <stable parent identifier>
task_id: <stable task identifier>
execution_id: <unique execution attempt identifier>
attempt_no: <integer>
contract_version: <version>
```

Rules:
- `task_id` remains unchanged across retries and route transfer;
- `execution_id` changes for every new execution attempt;
- `attempt_no` increments for every retry or transferred execution;
- one PR must not contain multiple unrelated task IDs;
- one task must not have multiple simultaneous mutation owners.

## 6. Unified task contract

A task contract contains only task-specific information.

Required fields:

```text
project
root_task_id
task_id
execution_id
execution_mode
current_goal
exact_task
repository_full_name
base_ref
lane
approved_scope
forbidden_scope
acceptance_criteria
merge_policy
server_apply_allowed
required_result
```

Optional task-specific fields:

```text
branch
base_sha
pr_number
review_comments
constraints
dependencies
blockers
unknowns
evidence_requirements
merge_delegated
merge_executor
expected_head_sha
```

Static role descriptions, project paths, standard workflow rules, default authority and common result schemas belong in persistent project or agent context and must not be copied into every handoff.

## 7. Dynamic-only handoff

A handoff contains only:
- task identity;
- current goal;
- exact task;
- verified current state;
- repository state;
- decisions already made;
- approved and forbidden scope;
- task-specific constraints;
- task-specific authority overrides;
- acceptance criteria;
- required evidence;
- known blockers;
- required next action.

A handoff does not contain:
- generic role descriptions;
- the full GitHub workflow;
- static project paths;
- standard result schema;
- general authority rules;
- general stop conditions;
- full historical context.

The receiving role must reconstruct available repository facts directly from GitHub instead of relying on copied stale state.

## 8. Soft intake

A receiving role must not block merely because optional fields are absent.

Use `BLOCKED` only when a critical value:
- is absent;
- cannot be derived from the task context;
- cannot be verified in GitHub;
- is required for safe execution.

Examples of critical blockers:
- repository cannot be identified;
- approved mutation scope is unknown;
- two active owners control the same branch or file scope;
- requested mutation conflicts with current GitHub state;
- merge authority is ambiguous for a requested merge;
- server apply is requested without explicit authorization.

## 9. Route selection

Prefer Browser when:
- task complexity is high;
- scope is evolving;
- architecture decisions are unresolved;
- multiple roles are required;
- every mutation stage requires explicit control;
- repository state is inconsistent or risky.

Prefer Flowise when:
- task is formalized;
- scope is exact;
- acceptance criteria are testable;
- GitHub lifecycle is standard;
- retries can be bounded;
- automated review and correction are appropriate.

Route selection is recorded with:

```text
execution_mode
selected_by
route_reason
fallback_allowed
```

## 10. Browser route contract

Browser route lifecycle:

```text
intake
→ GitHub state verification
→ branch ownership verification
→ implementation
→ PR creation or update
→ changed-file and diff review
→ acceptance validation
→ exact-head CI validation
→ correction if required
→ merge decision
→ optional separately authorized server apply
→ closure
```

Browser route rules:
- use one task branch;
- reuse the existing task PR;
- do not create a replacement branch or PR during correction;
- verify `origin/main` before implementation;
- record base SHA and latest head SHA;
- review exact changed files;
- verify checks against the exact latest head SHA;
- invalidate prior approval if the head SHA changes;
- preserve all approved scope until final result;
- report incomplete work explicitly.

## 11. Flowise route contract

Flowise architecture:

```text
User / GPT Action
→ public proxy
→ Flowise Lead Agent
→ github_worker and/or GitHub MCP
→ GitHub
```

Lead responsibilities:
- interpret the dynamic task request;
- inspect GitHub state;
- call `github_worker` for file mutations;
- inspect PR details, changed files and diff;
- validate acceptance criteria and scope;
- obtain checks for the exact head SHA;
- perform post-PR review;
- return blocking findings to the worker;
- repeat review after correction;
- merge only when explicitly delegated and all gates pass;
- return factual structured output.

Worker responsibilities:
- read the repository;
- create or reuse the authorized branch;
- modify only approved files;
- create or update the authorized PR;
- correct the same branch and PR;
- return actual branch, commit, PR and changed files;
- never merge;
- never apply to the server.

## 12. Branch, PR and mutation ownership

Mandatory invariants:

```text
one task = one active route
one branch = one active mutation owner
one PR = one task
one merge at a time
one server apply at a time
```

A role must verify before mutation:
- current route owner;
- current branch owner;
- current PR number;
- current head SHA;
- overlapping file scope;
- whether another execution is active.

If ownership cannot be established, return `BLOCKED` with `routing_error` or `authority_violation`.

## 13. Shared-file locks

Before concurrent work, PM L1 or PM L2 must identify shared files.

Lock record:

```text
file_or_scope
owner_task_id
owner_execution_id
branch
lock_status
acquired_at
released_at
```

No second mutation owner may edit a locked shared file or scope.

A read-only reviewer may inspect locked files but must not mutate them.

## 14. Lifecycle statuses

Canonical statuses:

```text
DRAFT
READY
RUNNING
WAITING_FOR_INPUT
WAITING_FOR_CHECKS
CHANGES_REQUIRED
BLOCKED
FAILED
READY_FOR_MANUAL_MERGE
MERGED
COMPLETED
CANCELLED
SUPERSEDED
```

Status rules:
- `READY_FOR_MANUAL_MERGE` requires approved review and completed acceptance validation;
- `MERGED` requires factual GitHub merge evidence;
- `COMPLETED` must not be used for unfinished repository or server actions;
- `SUPERSEDED` identifies a task or execution replaced by a newer authorized one;
- timeout alone does not prove failure or completion.

## 15. Review and CI gates

For every PR, the responsible reviewer must verify:
- PR state;
- base branch;
- head branch;
- full head SHA;
- exact changed-file list;
- diff;
- scope compliance;
- acceptance criteria;
- review findings;
- checks associated with the exact head SHA;
- mergeability and conflicts.

If the head SHA changes:
- prior checks must not be reused;
- prior review approval is stale;
- review and checks must be repeated.

## 16. Merge policy

Default:

```text
merge_policy: manual
merge_delegated: false
```

PM L2 may delegate merge to:
- an authorized Browser role;
- the Flowise Lead.

Delegation must be bound to:

```text
task_id
repository
branch
pr_number
head_sha
merge_policy
merge_executor
```

Any new head SHA invalidates the delegation until PM L2 reauthorizes or the task contract explicitly permits revalidation and delegated merge on the updated exact head.

Automatic merge is allowed only if:
- PR exists;
- the exact head SHA is known;
- changed files are within scope;
- acceptance criteria pass;
- review is approved;
- checks for the exact head SHA pass;
- blocking comments are absent;
- conflicts are absent;
- PM L2 explicitly delegated automatic merge;
- the task does not prohibit merge.

## 17. Server apply policy

Server apply is separate from GitHub merge.

Default:

```text
server_apply_allowed: false
server_apply_status: not_performed
```

Only PM L2 may authorize server apply.

Server apply must:
- use the canonical server path;
- apply a verified GitHub state;
- record the applied commit SHA;
- avoid treating server files as architectural evidence;
- stop on repository/server divergence.

## 18. Route transfer

Browser ↔ Flowise transfer preserves:

```text
root_task_id
task_id
approved_scope
forbidden_scope
acceptance_criteria
branch
pr_number
merge_policy
authority
```

The transfer creates:

```text
new execution_id
incremented attempt_no
new execution_mode
route_transfer_reason
```

Before transfer:
- stop the prior mutation owner;
- verify GitHub state;
- reconcile branch, PR and head SHA;
- acquire the new route lock;
- reuse the existing branch and PR;
- do not create a replacement branch or PR unless PM L2 explicitly authorizes it.

## 19. Flowise timeout reconciliation

An external timeout does not prove that Flowise stopped.

After timeout:
1. do not immediately retry a mutation;
2. inspect GitHub for branch, commits and PR changes;
3. inspect available Flowise execution trace;
4. determine whether the original execution is still active, completed or failed;
5. reconcile the actual head SHA and PR state;
6. retry only after idempotency and route ownership are confirmed.

If completion cannot be determined safely, return:

```text
status: BLOCKED
error_class: flowise_timeout
next_step: reconcile_execution_state
```

## 20. Retry and loop limits

Retries must be bounded.

Recommended defaults:
- worker correction cycles: 2;
- checks polling attempts: 2–3;
- route transfer attempts: 1 unless PM L2 approves more;
- no retry after an authority or scope violation without correction of the task contract.

When the limit is exhausted, return `BLOCKED` or `FAILED` with factual evidence.

## 21. Error classification

Canonical error classes:

```text
handoff_error
routing_error
context_error
flowise_transport_error
flowise_timeout
flowise_output_error
github_access_error
github_mutation_error
scope_violation
stale_state
review_failure
ci_pending
ci_failure
merge_blocked
authority_violation
server_apply_blocked
execution_loop_exhausted
```

Errors must identify:
- affected task and execution;
- factual state;
- whether mutation occurred;
- branch and PR if present;
- whether another retry is safe;
- required next owner.

## 22. Canonical result

Every execution returns, as applicable:

```text
task_id
execution_id
execution_mode
status
summary
done
not_done
branch
commit_sha
pr_number
pull_request_url
head_sha
actual_changed_files
checks_status
checks_source
review_status
review_comments
merge_status
server_apply_status
evidence
blockers
errors
next_step
next_owner
```

Unknown values must be reported as unknown or not available. They must never be invented.

## 23. Read-only tasks

Read-only tasks must not:
- create or modify branches;
- create commits;
- create or update PRs;
- add reviews or comments;
- rerun checks;
- merge;
- apply to the server.

A read-only result should include the requested factual GitHub evidence and explicitly state that no mutation was performed.

## 24. Correction tasks

Correction of an existing PR must:
- retain the same `task_id`;
- retain the same branch;
- retain the same PR;
- use a new `execution_id`;
- stay within the approved scope;
- address only approved findings;
- re-run review and exact-head checks after mutation.

## 25. Deprecated context

The following must not be used as active management authority:
- Route B / n8n execution contracts;
- deprecated `moex_robot` underscore paths;
- old `Контекст.md (1)`;
- server filesystem assumptions;
- stale branch, PR or SHA values copied from prior chats without GitHub verification.

## 26. Adoption gate

The management canon is operationally adopted when:
- this document is merged into `main`;
- PM L2 and Flowise orchestration documents reference it;
- Browser project and role contexts use the same rules;
- Flowise Lead and worker prompts use the same rules;
- Browser and Flowise pilots pass;
- Route B is marked deprecated for new work.
