# MOEX Bot Management Canon

status: active
version: 1.0
adopted_at: 2026-07-21
repository: Viktoryyyyy/moex-robot

## 1. Authority

This document is the authoritative management contract for MOEX Bot delivery.

If another management document conflicts with this document, this document prevails unless the owner explicitly approves a newer version.

## 2. Project isolation

Every management output must start with:

```text
PROJECT=MOEX_Bot
```

Use only:
- the current MOEX Bot project context;
- repository evidence from `Viktoryyyyy/moex-robot`;
- task-specific evidence explicitly supplied for MOEX Bot.

Do not import assumptions, memories, files or decisions from other projects.

Unknown values must remain unknown until verified. Do not invent file paths, branches, SHAs, PRs, checks, reviews, merge state or server state.

## 3. Source of Truth

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

Deprecated underscore paths must not be used:

```text
/home/trader/moex_bot/moex_robot
~/moex_bot/moex_robot
```

The old file `Контекст.md (1)` is deprecated and must not be used.

## 4. Active execution routes

Exactly two execution routes are active.

### 4.1 Browser Controlled GitHub Route

```text
execution_mode: browser_controlled_github_route
```

Use for:
- complex or high-risk tasks;
- tasks requiring explicit control of each stage;
- multi-role delivery;
- broad repository analysis;
- sensitive scope or authority boundaries;
- evolving architecture or scope.

Authorized Browser roles may perform direct GitHub read and write operations. Direct writes to `main` remain forbidden.

### 4.2 Flowise Automated GitHub Route

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

### 4.3 Closed route

```text
execution_mode: route_b_n8n_universal_role_runner
status: deprecated
new_tasks_allowed: false
```

Route B / n8n is closed for new delivery. Historical evidence may remain, but it does not authorize new Route B tasks, branches, PRs or server actions.

## 5. Management roles

```text
PM L1 = control tower and root task owner
PM L2 = phase owner, scope owner and merge authority owner
PM L3 = delivery validation owner when assigned
Browser Subchat = delegated executor or reviewer
Flowise Lead = automated route orchestrator
Flowise github_worker = delegated implementation worker
```

### PM L1

Owns:
- task registry;
- root task identity;
- route coordination;
- cross-lane conflicts;
- closure and supersession;
- management canon compliance.

PM L1 does not widen technical scope without PM L2 ownership.

### PM L2

Owns:
- current goal;
- task decomposition;
- approved and forbidden scope;
- acceptance criteria;
- route selection or approval;
- branch and PR ownership;
- merge policy and delegation;
- server-apply authorization;
- final acceptance.

Flowise is an execution route, not the scope owner and not the final acceptance owner.

### PM L3

When assigned, validates:
- repository evidence;
- exact changed files;
- diff quality;
- acceptance criteria;
- CI on the exact head SHA;
- unresolved review findings;
- readiness for PM L2 decision.

### Executors

Must:
- stay within approved scope;
- use the assigned branch and PR;
- preserve task identity;
- return factual GitHub evidence;
- stop on authority conflict, scope conflict or stale state.

Must not:
- write directly to `main`;
- create a replacement branch or PR without authorization;
- widen scope;
- merge without delegated authority;
- apply to the server without separate authorization;
- invent evidence.

## 6. Task identity

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
- `execution_id` changes for each new execution attempt;
- `attempt_no` increments for each retry or transfer;
- one PR must not contain unrelated task IDs;
- one task must not have simultaneous mutation owners.

## 7. Unified task contract

Required task-specific fields:

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

Static role descriptions, project paths, common workflow rules, default authority and standard result schemas belong in persistent project or agent context and must not be copied into each handoff.

## 8. Dynamic-only handoff

A handoff contains only:
- task identity;
- current goal and exact task;
- verified current state;
- repository state;
- decisions already made;
- approved and forbidden scope;
- task-specific constraints and authority overrides;
- acceptance criteria and required evidence;
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

## 9. Soft intake

Do not block merely because optional fields are absent.

Use `BLOCKED` only when a critical value:
- is absent;
- cannot be derived from the task context;
- cannot be verified in GitHub;
- is required for safe execution.

Critical blockers include:
- repository cannot be identified;
- approved mutation scope is unknown;
- two active owners control the same branch or file scope;
- requested mutation conflicts with current GitHub state;
- merge authority is ambiguous for a requested merge;
- server apply is requested without explicit authorization.

## 10. Route selection

Prefer Browser when:
- complexity or risk is high;
- scope is evolving;
- architecture decisions are unresolved;
- multiple roles are required;
- every mutation stage needs explicit control;
- repository state is inconsistent.

Prefer Flowise when:
- task is formalized;
- scope is exact;
- acceptance criteria are testable;
- GitHub lifecycle is standard;
- retries can be bounded;
- automated review and correction are appropriate.

Record:

```text
execution_mode
selected_by
route_reason
fallback_allowed
```

## 11. Browser route contract

Lifecycle:

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

Rules:
- use one task branch;
- reuse the existing task PR;
- do not create replacement branch or PR during correction;
- verify the base ref before implementation;
- record base SHA and latest head SHA;
- review exact changed files;
- verify checks against the exact latest head SHA;
- invalidate prior approval if the head SHA changes;
- preserve all approved scope until final result;
- report incomplete work explicitly.

## 12. Flowise route contract

Architecture:

```text
User / GPT Action
→ public proxy
→ Flowise Lead Agent
→ github_worker and/or GitHub MCP
→ GitHub
```

Lead must:
- interpret the dynamic request;
- inspect GitHub state;
- call `github_worker` for file mutations;
- inspect PR details, changed files and diff;
- validate scope and acceptance criteria;
- obtain checks for the exact head SHA;
- perform post-PR review;
- return blocking findings to Worker;
- repeat review after correction;
- merge only when explicitly delegated and all gates pass;
- return factual structured output.

Worker must:
- read the repository;
- create or reuse the authorized branch;
- modify only approved files;
- create or update the authorized PR;
- correct the same branch and PR;
- return actual branch, commit, PR and changed files;
- never merge;
- never apply to the server.

## 13. Ownership and locks

Mandatory invariants:

```text
one task = one active route
one branch = one active mutation owner
one PR = one task
one merge at a time
one server apply at a time
```

Before mutation verify:
- current route owner;
- current branch owner;
- current PR number;
- current head SHA;
- overlapping file scope;
- whether another execution is active.

Shared-file lock record:

```text
file_or_scope
owner_task_id
owner_execution_id
branch
lock_status
acquired_at
released_at
```

No second mutation owner may edit a locked file or scope. A read-only reviewer may inspect it.

## 14. Lifecycle statuses

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

Rules:
- `READY_FOR_MANUAL_MERGE` requires approved review and completed acceptance validation;
- `MERGED` requires factual GitHub merge evidence;
- `COMPLETED` must not be used for unfinished repository or server actions;
- timeout alone does not prove failure or completion.

## 15. Review and CI gates

For every PR verify:
- PR state;
- base and head branches;
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

PM L2 may delegate merge to an authorized Browser role or the Flowise Lead.

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

Automatic merge is allowed only if:
- PR exists;
- exact head SHA is known;
- changed files are within scope;
- acceptance criteria pass;
- review is approved;
- exact-head checks pass;
- blocking comments and conflicts are absent;
- PM L2 explicitly delegated automatic merge;
- the task does not prohibit merge.

A new head SHA invalidates prior delegation unless the task contract explicitly permits revalidation and delegated merge on the updated exact head.

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

Transfer creates:

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
- do not create replacements without explicit PM L2 authorization.

## 19. Flowise timeout reconciliation

An external timeout does not prove that Flowise stopped.

After timeout:
1. do not immediately retry a mutation;
2. inspect GitHub for branch, commits and PR changes;
3. inspect available Flowise execution trace;
4. determine whether the original execution is active, completed or failed;
5. reconcile actual head SHA and PR state;
6. retry only after idempotency and route ownership are confirmed.

If completion cannot be determined safely:

```text
status: BLOCKED
error_class: flowise_timeout
next_step: reconcile_execution_state
```

## 20. Retry limits

Recommended defaults:
- worker correction cycles: 2;
- checks polling attempts: 2–3;
- route transfer attempts: 1 unless PM L2 approves more;
- no retry after authority or scope violation until the task contract is corrected.

When exhausted, return `BLOCKED` or `FAILED` with factual evidence.

## 21. Error classification

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
- whether retry is safe;
- required next owner.

## 22. Canonical result

Every execution returns, when applicable:

```text
PROJECT=MOEX_Bot
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

Unknown values must be reported as unknown or unavailable. They must never be invented.

## 23. Read-only tasks

Read-only tasks must not:
- create or modify branches;
- create commits;
- create or update PRs;
- add reviews or comments;
- rerun checks;
- merge;
- apply to the server.

The result must state that no mutation was performed.

## 24. Correction tasks

Correction of an existing PR must:
- retain the same `task_id`;
- retain the same branch;
- retain the same PR;
- use a new `execution_id`;
- stay within approved scope;
- address only approved findings;
- repeat review and exact-head checks after mutation.

## 25. Deprecated context

Do not use as active authority:
- Route B / n8n execution contracts;
- deprecated `moex_robot` underscore paths;
- old `Контекст.md (1)`;
- server filesystem assumptions;
- stale branch, PR or SHA values copied from prior chats without GitHub verification.

## 26. Adoption gate

This canon is operationally adopted when:
- this document is merged into `main`;
- PM L2 and Flowise orchestration documents reference it;
- Browser project and role contexts use the same rules;
- Flowise Lead and Worker prompts use the same rules;
- Browser and Flowise pilots pass;
- Route B is marked deprecated for new work.
