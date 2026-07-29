# MOEX Bot Management Canon

status: current_merged_source
version: 2.0
approved_at: 2026-07-22
repository: Viktoryyyyy/moex-robot

## 1. Authority

This document is the authoritative management contract for MOEX Bot delivery after it is merged into `main`.

Precedence:

```text
Owner decision
→ docs/MOEX_BOT_MANAGEMENT_CANON.md
→ approved canon amendments
→ target-specific source documents
→ applied Browser or Flowise settings
→ dynamic task request
```

A lower-level document or task request must not silently replace a higher-level rule or widen authority.

## 2. Project isolation and output marker

Every human-readable management output starts with:

```text
PROJECT=MOEX_Bot
```

Machine-readable JSON output uses the first property:

```json
{
  "project": "MOEX_Bot"
}
```

Use only MOEX Bot context, repository evidence and task-specific evidence supplied for MOEX Bot. Do not import assumptions, paths, decisions or artifacts from other projects.

Unknown facts remain unknown until verified. Never invent repository state, file paths, branches, SHAs, PRs, reviews, checks, merge state or server state.

## 3. Source of Truth

```text
GitHub repository = Source of Truth
Server filesystem = Applied State only
```

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

Deprecated paths are forbidden:

```text
/home/trader/moex_bot/moex_robot
~/moex_bot/moex_robot
cd ~/moex_bot/moex_robot && source venv/bin/activate
```

The old file `Контекст.md (1)` is deprecated and must not be used.

## 4. Active execution routes

Active routes:

```text
browser_controlled_github_route
flowise_automated_github_route
```

Closed route:

```text
route_b_n8n_universal_role_runner
status: deprecated
new_tasks_allowed: false
```

Do not create new Route B / n8n tasks, branches, PRs or runtime actions.

Use Browser when scope or architecture is evolving, risk is high, repository state is inconsistent, multiple roles are required or each stage needs explicit control.

Use Flowise when the task is formalized, scope is exact, completion criteria are testable, the GitHub lifecycle is standard and retries can be bounded.

## 5. Management roles

```text
PM L1 = control tower and root-task owner
PM L2 = phase owner, scope owner and final acceptance owner
PM L3 = independent delivery validation owner when assigned
Browser Subchat = delegated executor or reviewer
Flowise Lead = automated route orchestrator
Flowise github_worker = delegated repository mutation executor
```

PM L1 owns portfolio coordination, route coordination, collision control, merge/server queues, closure and supersession.

PM L2 owns the current goal, decomposition, approved scope, acceptance criteria, route approval, branch and PR ownership, correction decisions, merge policy, server-apply authority and final acceptance.

Flowise is an execution route. It is not the scope owner and not the final acceptance owner.

PM L3 validates independently and does not widen scope or repair implementation without separate authority.

Executors remain within approved scope, preserve task identity, use the assigned branch and PR, and return factual evidence.

## 6. Operating principle: result first

The primary deliverable is the requested task result. Technical execution metadata is supporting evidence only.

An execution report must answer the task before listing branch, commit, PR, CI or other metadata.

Examples:

- analysis task → findings, conflicts, affected files and recommended next scope;
- change task → implemented behavior and validation result;
- validation task → verdict and exact findings;
- merge task → verified merge result;
- server-apply task → verified applied commit and runtime result.

A statement such as `all items checked` without the actual findings is not a completed deliverable.

`COMPLETED` is forbidden when the requested substantive result is absent.

## 7. Context model

```text
Static Project Context
+ Persistent Role or Agent Context
+ Dynamic Task Contract
```

Static context contains project identity, Source of Truth, role authority, standard GitHub lifecycle, output rules and common safety boundaries.

The dynamic request contains only task-specific information. Do not require the caller to repeat static role descriptions, repository defaults, standard lifecycle rules or a complete result schema.

## 8. Task classes

Every task has one primary action:

```text
analyze
change
validate
merge
server_apply
```

### analyze

Read-only investigation that returns substantive findings and the minimum recommended next scope.

### change

Repository mutation within approved scope, normally through a task branch and PR.

### validate

Independent validation of an existing result, branch or PR. The validation verdict is:

```text
PASS
CHANGES_REQUIRED
BLOCKED
```

### merge

Exact-authority merge after all current-head gates pass.

### server_apply

Separately authorized application of an exact merged GitHub commit to the server.

Do not combine unrelated primary actions in one execution. A change task may perform self-validation, but independent final validation remains a separate decision when required.

## 9. Minimal dynamic task contract

Required task input:

```text
PROJECT=MOEX_Bot
Action: <analyze|change|validate|merge|server_apply>
Task ID: <stable task identifier>
Task: <required substantive result>
Done when:
- <testable completion criterion>
```

Conditional fields:

```text
Target: <PR, branch, commit, file or functional area>
Scope: <approved mutation boundary or constrained analysis area>
Constraints: <task-specific non-standard limitations>
Authority: <required only for merge or server apply>
Merge mode: <manual|automatic>
```

Rules:

- `Task ID` remains stable across retries, correction and Browser ↔ Flowise transfer.
- The system or executor may generate `execution_id` and `attempt_no`; they are not mandatory caller input for ordinary tasks.
- Repository defaults to `Viktoryyyyy/moex-robot` unless the task explicitly and validly targets another approved MOEX Bot repository.
- Base defaults to the repository default branch.
- Merge defaults to manual.
- Server apply defaults to forbidden.
- Scope is mandatory for mutation when it cannot be determined unambiguously from `Task` and `Done when`.
- Exact branch, PR and SHA are mandatory caller inputs only when they form an authority boundary, such as exact-head merge delegation.

## 10. Information recovered from GitHub

The receiving role or Agent must recover available repository facts directly from GitHub instead of requiring them in every request.

Recover when relevant:

```text
repository and default branch
current main/base SHA
existing task branch
existing PR
latest full head SHA
actual changed files
diff and patches
reviews and review threads
exact-head checks
mergeability and merge state
```

Do not copy stale repository facts from prior reports when current GitHub state is available.

## 11. Soft intake

Do not block because an optional or recoverable field is absent.

Use `BLOCKED` only when safe continuation would require guessing a critical fact that cannot be established from the current task, persistent context, GitHub or another explicitly authorized source.

Critical blockers include:

- repository or target task cannot be identified;
- mutation scope is unknown;
- branch or PR ownership is ambiguous;
- another active executor may control the same mutation scope;
- requested correction requires scope widening;
- merge or server-apply authority is incomplete;
- current GitHub state materially conflicts with the request;
- execution state after timeout cannot be reconciled safely.

Blocker output identifies the exact conflict, checked sources, required decision and next owner.

## 12. Ownership and locks

Mandatory invariants:

```text
one task = one active route
one branch = one active mutation owner
one PR = one task
one merge at a time
one server apply at a time
```

Read-only inspection does not require mutation ownership.

Before mutation verify task identity, route owner, branch owner, existing PR, current head SHA, approved scope and overlapping work.

Do not create a replacement branch or PR for correction without explicit PM L2 authority.

## 13. Standard workflow

### analyze

```text
intake
→ relevant GitHub inspection
→ substantive analysis
→ findings and affected files
→ minimum recommended next scope
→ result with no mutation
```

### change

```text
intake
→ GitHub and ownership reconciliation
→ use or create task branch
→ mutate only approved scope
→ commit
→ create or update the same task PR
→ inspect actual changed files and diff
→ validate Done when
→ review
→ validate CI on exact latest head SHA
→ correction in the same branch and PR when required
→ result
```

### validate

```text
intake
→ target metadata
→ exact latest head SHA
→ actual changed files and diff
→ scope and Done when
→ review findings and unresolved threads
→ exact-head CI
→ mergeability
→ PASS | CHANGES_REQUIRED | BLOCKED
```

### merge

```text
exact authority
→ current head reconciliation
→ scope and acceptance
→ review and unresolved threads
→ exact-head CI
→ mergeability
→ merge
→ merge verification
```

### server_apply

```text
separate authority
→ exact merged GitHub SHA
→ server reconciliation
→ apply
→ runtime validation
→ applied-state evidence
```

## 14. Branch, PR and correction rules

Direct write to `main` is forbidden.

Repository changes use a task-specific branch and PR unless an approved workflow explicitly states otherwise.

Correction preserves:

```text
task ID
working branch
PR
approved scope
```

A new external attempt may use a new execution ID. It must not duplicate an existing branch, commit or PR.

If correction requires files outside approved scope, stop and request explicit PM L2 scope widening.

## 15. Review and exact-head CI

Green CI does not equal approval.

For a mutation PR verify:

- PR state, base and head branches;
- full latest head SHA;
- exact changed files;
- current diff;
- approved and forbidden scope;
- completion criteria;
- defects and security findings;
- unresolved blocking review threads;
- checks tied to the exact latest head SHA;
- mergeability and conflicts.

When head SHA changes, prior review, prior checks and prior merge readiness are stale. Repeat review and checks.

Do not infer checks, thread resolution or outdated state.

## 16. Merge policy

Default:

```text
merge_policy: manual
merge_delegated: false
```

Manual mode never merges automatically.

Automatic merge requires exact delegation bound to:

```text
task ID
repository
working branch
PR number
full expected head SHA
merge executor
merge policy
```

Merge is allowed only when current GitHub state matches the delegation, actual changed files are within scope, completion criteria pass, review is approved, blocking threads are resolved, exact-head checks pass and conflicts are absent.

A new head SHA invalidates old exact-head merge delegation unless the authority explicitly permits revalidation on the updated head.

## 17. Server apply

Server apply is separate from merge.

Default:

```text
server_apply_allowed: false
server_apply_status: not_performed
```

Only PM L2 or an explicitly delegated owner-level authority may authorize server apply.

Server apply must use the canonical server context, apply an exact verified GitHub commit, record that commit and stop on repository/server divergence.

## 18. Timeout, retry and route transfer

Timeout does not prove that execution stopped.

After timeout:

1. do not repeat mutation immediately;
2. inspect branch, commits and PR;
3. inspect available Flowise trace;
4. reconcile latest head, diff, review and checks;
5. determine whether prior mutation occurred;
6. retry only after ownership and idempotency are established.

Browser ↔ Flowise transfer preserves task ID, approved scope, branch, PR, completion criteria and authority. It changes execution attempt and route owner.

Recommended defaults:

- worker correction cycles: 2;
- exact-head checks polling attempts: 3;
- route transfers: 1 unless PM L2 approves more;
- same failed tool retry: 1.

## 19. Result contract

Common machine-readable result:

```json
{
  "project": "MOEX_Bot",
  "taskId": "",
  "status": "",
  "result": {},
  "evidence": {},
  "nextAction": ""
}
```

Rules:

- `result` contains the requested substantive deliverable.
- `evidence` contains only evidence relevant to this task class.
- Do not emit empty optional fields, empty arrays or repetitive false flags.
- Add `blocker` only when blocked.
- Add `changes` only when mutation occurred.
- Add `validation` only when validation was performed.
- Add `merge` only for merge work.
- Add `serverApply` only for server-apply work.
- Unknown values are omitted or explicitly marked unknown only when materially relevant.
- Technical metadata must not replace the substantive result.

Example blocker:

```json
{
  "project": "MOEX_Bot",
  "taskId": "task-123",
  "status": "BLOCKED",
  "result": {
    "completedPart": "Current PR and approved scope were verified"
  },
  "blocker": {
    "code": "scope_widening_required",
    "fact": "Correction requires a file outside approved scope",
    "requiredDecision": "PM L2 must approve scope widening"
  },
  "nextAction": "PM L2 decision"
}
```

## 20. Status semantics

Allowed management statuses:

```text
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

- `COMPLETED` means the requested substantive result and all `Done when` criteria for that task class are complete.
- `COMPLETED` is invalid when the report contains only execution metadata.
- `CHANGES_REQUIRED` means an additional repository or PR mutation is required.
- pending CI uses `WAITING_FOR_CHECKS`, not `CHANGES_REQUIRED`.
- `READY_FOR_MANUAL_MERGE` requires approved current-head review, no unresolved blocking threads, passed exact-head checks and mergeability.
- `MERGED` requires factual GitHub merge evidence.
- `FAILED` is reserved for unrecoverable execution failure; use `BLOCKED` when an upstream decision or safe fact is required.

## 21. Flowise boundaries

Flowise Lead orchestrates, recovers GitHub facts, validates scope and completion criteria, controls `github_worker` for repository mutation, performs post-PR review, validates exact-head checks and merges only with exact authority.

`github_worker` implements or corrects only approved scope, uses the authorized branch and PR, and returns implementation result plus task-relevant GitHub evidence.

Worker never merges, never applies to the server, never widens scope and never claims final acceptance.

Lead and Worker prompts must use this minimal task and result model. They must not restore legacy mandatory field lists through target-specific prompts.

## 22. Configuration Source of Truth

Canonical source files in GitHub are authoritative for Browser project context, Browser role context and Flowise Agent prompts.

Browser and Flowise settings are Applied State. A source-file change is not proof that the corresponding setting was updated.

For each applied setting record:

```text
target
source file
source commit SHA
applied at
applied by
verification status
```

## 23. Adoption workflow

This version is operationally adopted when:

1. the complete approved document set is updated in one task branch and PR;
2. exact changed files and diff are reviewed;
3. CI passes on the exact latest PR head SHA;
4. the PR is merged with authority;
5. Browser and Flowise applied settings are updated from the merged sources;
6. read-only, analysis, mutation and correction regression pilots pass;
7. obsolete conflicting PRs or sources are closed, superseded or marked historical.

Until merge, `main` remains the active management Source of Truth.

## 24. Deprecated management behavior

Do not use as active authority:

- Route B / n8n execution contracts;
- deprecated underscore server paths;
- old `Контекст.md (1)`;
- server filesystem assumptions;
- stale branch, PR or SHA values copied from prior chats;
- legacy universal input contracts that require repository facts recoverable from GitHub;
- legacy universal output schemas that return many empty technical fields instead of the requested result.
