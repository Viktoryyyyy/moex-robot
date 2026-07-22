# Flowise GitHub Orchestration

status: approved_pending_merge
version: 3.0
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
management_canon_version: 2.0
repository: `Viktoryyyyy/moex-robot`

## 1. Purpose

This document defines the Flowise automated GitHub execution route for MOEX Bot.

The governing rule is:

```text
requested result first
minimum task-specific evidence second
```

Flowise must not replace a substantive task result with a large operational report.

If this document conflicts with `docs/MOEX_BOT_MANAGEMENT_CANON.md`, the management canon prevails.

## 2. Active route

```text
execution_mode: flowise_automated_github_route
```

Flowise is an execution route. It is not the scope owner and not the final acceptance owner.

PM L2 owns:

- required substantive result;
- approved scope;
- completion criteria;
- route approval;
- branch and PR ownership;
- correction decisions;
- merge policy and delegation;
- server-apply authorization;
- final acceptance.

## 3. Architecture

```text
User / GPT Action
→ public proxy
→ Flowise Lead Agent `github-change-orchestrator`
→ `github_worker` and/or GitHub MCP
→ GitHub
```

Current documented components:

- public endpoint: `https://flowise-api.foods-tech.store/github-task`;
- Lead Agent: `github-change-orchestrator`;
- Worker Agent: `github-worker`;
- Lead tool name: `github_worker`;
- Worker connection: Agent as Tool.

The proxy returns the authoritative external Agent result in:

```json
{
  "text": "..."
}
```

Do not expose credentials, tokens, passwords, secrets or private runtime metadata.

## 4. Source of Truth

```text
GitHub repository = Source of Truth
Server filesystem = Applied State only
canonical repository = Viktoryyyyy/moex-robot
```

Do not infer repository architecture, accepted implementation, branch state or authority from server files.

## 5. Responsibility model

### Lead Agent

Lead must:

- classify the task by Action;
- recover current repository facts from GitHub;
- validate task identity, route ownership and idempotency;
- call `github_worker` for repository file mutation;
- independently verify Worker claims against GitHub;
- validate actual changed files and diff;
- validate completion criteria;
- perform current-head review when a PR is involved;
- inspect current review threads;
- obtain checks for the exact latest PR head SHA;
- return blocking findings to Worker for correction;
- repeat review and exact-head checks after correction;
- merge only under valid exact-head delegation;
- return the requested substantive result before supporting evidence.

Lead may use GitHub MCP directly for:

- read-only repository inspection;
- PR metadata, changed files and diff;
- reviews and review threads;
- checks and workflow evidence;
- factual replies and resolution of verified corrected review threads;
- authorized merge.

Lead must not perform repository file mutation directly.

### `github_worker`

Worker may:

- read the authorized repository;
- create or reuse the authorized task branch;
- modify only approved files;
- create or update the authorized PR;
- correct approved findings in the same branch and PR;
- return factual implementation and GitHub evidence.

Worker must never:

- merge;
- write directly to `main`;
- widen scope;
- create a replacement branch or PR without authority;
- perform server apply;
- approve its own work;
- claim final acceptance or merge readiness;
- invent GitHub state.

## 6. Route invariants

```text
one task = one active route
one branch = one active mutation owner
one PR = one task
one merge at a time
one server apply at a time
```

Before mutation, Lead establishes when relevant:

- task identity;
- current route owner;
- branch ownership;
- existing branch and PR;
- current full head SHA;
- active or timed-out execution state;
- approved scope;
- overlapping file scope.

A read-only task does not require mutation ownership.

## 7. Task classes

Every request uses one primary Action:

```text
analyze
change
validate
merge
server_apply
```

Do not use `Action: execute` for new tasks.

### analyze

Read-only investigation. Required result normally includes:

- findings;
- conflicts or gaps;
- affected files or components;
- recommended minimum next scope;
- blocker only when a decision cannot be made safely.

### change

Repository mutation in approved scope through the task branch and PR lifecycle.

### validate

Independent validation of an existing result, branch or PR. Required verdict:

```text
PASS
CHANGES_REQUIRED
BLOCKED
```

### merge

Exact-authority merge after all current-head gates pass.

### server_apply

Separately authorized application of an exact merged GitHub commit to the server. The Flowise GitHub route does not itself perform server apply unless a separate approved server capability and task explicitly exist.

## 8. Minimal dynamic request

Common minimum:

```text
PROJECT=MOEX_Bot

@PM L2 Flowise

Action: <analyze|change|validate|merge|server_apply>
Task ID: <stable_task_id>

Task:
<required substantive result>

Done when:
- <testable criterion 1>
- <testable criterion 2>
```

The request contains task-specific data only.

The caller does not need to repeat:

- Lead and Worker role descriptions;
- the standard GitHub lifecycle;
- canonical repository when the task uses `Viktoryyyyy/moex-robot`;
- default base branch;
- current main SHA;
- execution ID or attempt number;
- current branch, PR or head SHA when recoverable;
- standard merge and server defaults;
- the complete output schema.

## 9. Conditional request fields

Use only when required.

### Target

```text
Target:
- Pull Request #260
```

or:

```text
Target:
- docs/FLOWISE_LEAD_AGENT_PROMPT.md
```

### Scope

Required for mutation when the boundary is not unambiguous from Task and Done when.

```text
Scope:
- docs/FLOWISE_LEAD_AGENT_PROMPT.md
- docs/FLOWISE_GITHUB_WORKER_PROMPT.md
```

For analysis, a functional area is sufficient when exact files are not yet known.

### Constraints

Use only for non-standard task-specific limitations.

### Authority

Required for `merge` and `server_apply`.

### Merge mode

Default is manual. State it only when useful to the external integration or when automatic merge is requested.

## 10. Soft intake and GitHub recovery

Lead reconstructs available facts directly from GitHub.

Recover when relevant:

- repository and default branch;
- current base SHA;
- existing task branch;
- existing PR;
- current full head SHA;
- actual changed files;
- diff and patches;
- reviews and review threads;
- checks tied to the current head;
- mergeability and merge state.

Missing recoverable information is not a blocker.

Use `BLOCKED` only when safe execution would require guessing a critical fact such as:

- repository or target task cannot be identified;
- mutation scope is ambiguous;
- conflicting mutation ownership exists;
- multiple branches or PRs could represent the task and cannot be reconciled;
- correction requires scope widening;
- merge authority is incomplete or mismatched;
- server apply lacks separate authority;
- timeout state cannot be reconciled safely.

## 11. Task identity and idempotency

```text
taskId = stable across retries and route transfer
executionId = generated per execution attempt when needed
attemptNo = generated or incremented when needed
```

Before repeating mutation:

- inspect GitHub for existing branch, commit and PR;
- determine whether requested changes already exist;
- reconcile any prior timeout;
- reuse existing task state;
- do not create duplicate commits or PRs.

Correction uses the same task ID, branch and PR.

## 12. Workflow by Action

### analyze

```text
parse task
→ recover relevant repository state
→ inspect only necessary files and evidence
→ perform substantive analysis
→ return findings and recommended next scope
```

No mutation, comments, review mutation, check rerun, merge or server apply.

### change

```text
verify task, ownership and scope
→ recover existing branch and PR
→ call github_worker
→ mutate only approved scope
→ create or update same PR
→ independently verify actual GitHub state
→ review current diff
→ inspect review threads
→ obtain exact-head checks
→ correct same branch and PR if required
→ final reconciliation
→ return implementation result and relevant evidence
```

A mutation task without a Worker execution is an architecture violation unless the task contains no repository file mutation.

### validate

```text
recover target branch or PR
→ inspect actual files and diff
→ check scope
→ test Done when criteria
→ inspect current review threads
→ obtain exact-head checks when relevant
→ return PASS, CHANGES_REQUIRED or BLOCKED
```

Validation does not mutate unless a separate change task is authorized.

### merge

```text
verify exact delegation
→ re-fetch current PR state
→ verify current head, scope, review, threads and checks
→ merge only if all gates pass
→ verify factual merged state
```

### server_apply

Treat as a separate authority boundary. Do not infer authorization from merge or from the existence of server files.

## 13. Read-only boundary

For `analyze` and read-only `validate`, do not:

- create or update branches;
- create commits;
- create or update PRs;
- add comments or reviews;
- resolve or unresolve threads;
- rerun checks;
- merge;
- perform server apply.

Return the requested findings or verdict. A statement that no mutation occurred may be included in evidence when relevant; do not add a large list of false mutation flags.

## 14. Mutation and PR lifecycle

For repository file mutation:

1. validate task identity, ownership and approved scope;
2. recover or create only the authorized task branch;
3. reuse the existing task PR for correction;
4. call `github_worker`;
5. require complete approved implementation, not a placeholder;
6. obtain Worker implementation result;
7. independently fetch branch, commit, PR and actual changed files;
8. inspect the diff;
9. validate scope and Done when criteria;
10. perform current-head review;
11. inspect all current review threads;
12. obtain checks for the exact latest head SHA;
13. send blocking findings to Worker when required;
14. repeat steps 7–12 after every correction;
15. perform final GitHub reconciliation before output.

Direct write to `main` is forbidden.

## 15. Review and review threads

Green CI is not review approval.

For a PR, verify when applicable:

- PR state;
- base and head branches;
- full latest head SHA;
- actual changed files;
- current diff;
- approved scope;
- Done when criteria;
- defects and security concerns;
- current review submissions;
- all review threads;
- merge conflicts.

GitHub review-thread state is authoritative.

A thread is outdated only when GitHub explicitly reports `is_outdated=true`.

A blocking thread remains blocking while `is_resolved=false`.

After Worker correction:

1. verify the correction in the current diff;
2. verify scope compliance;
3. reply to the existing thread with factual evidence when authorized;
4. resolve that same thread when authorized;
5. fetch threads again;
6. confirm `is_resolved=true`;
7. check for other unresolved blocking threads.

Do not return approval or merge readiness while an unresolved blocking thread remains.

## 16. Exact-head CI

Checks must belong to the exact latest PR head SHA.

Normalize checks status:

```text
passed
failed
pending
not_configured
unknown
```

If the head SHA changes:

- prior review approval is stale;
- prior checks are stale;
- prior merge readiness is stale;
- prior exact-head merge delegation is invalid unless explicitly designed for revalidation;
- review, thread inspection and checks must be repeated.

For merge readiness, factual checks evidence must refer to the current exact head.

Recommended polling maximum: 3 attempts.

If required checks remain pending after the configured attempts, use:

```text
status: WAITING_FOR_CHECKS
nextAction: wait_for_exact_head_ci
```

Do not use `CHANGES_REQUIRED` merely because CI is pending.

## 17. Correction cycle

Default maximum Worker correction cycles: 2.

Correction rules:

- same task ID;
- same branch;
- same PR;
- unchanged approved scope;
- only blocking findings and explicitly approved improvements;
- no unrelated refactoring;
- new head SHA after mutation;
- repeated review, thread inspection and exact-head checks.

If correction requires files outside approved scope, return `BLOCKED` with an exact request for PM L2 scope widening.

When correction limits are exhausted, return `BLOCKED` and identify the remaining findings.

## 18. Merge policy

Default:

```text
Merge mode: manual
```

For manual mode, Lead never merges.

Return `READY_FOR_MANUAL_MERGE` only when:

- a mutation PR exists and is open;
- actual changed files are within approved scope;
- all Done when criteria pass;
- current-head review is approved;
- no unresolved blocking threads remain;
- exact-head checks passed;
- the PR is mergeable;
- merge was not performed.

Automatic merge requires complete exact delegation:

```text
Authority:
  Task ID: <exact taskId>
  Repository: Viktoryyyyy/moex-robot
  Working branch: <exact branch>
  Pull request: <exact PR number>
  Expected head SHA: <full exact SHA>
  Merge policy: automatic
  Merge executor: flowise_lead
```

`Merge mode: automatic` alone is not authority.

Worker never merges.

## 19. Server apply boundary

Flowise GitHub orchestration does not authorize server apply.

Default:

```text
server_apply_allowed: false
```

Server apply requires:

- a separate task;
- explicit PM L2 authority;
- an exact merged GitHub commit SHA;
- the canonical MOEX Bot server context;
- a separate factual applied-state result.

## 20. Timeout reconciliation

An external timeout does not prove that Flowise stopped.

After timeout:

1. do not immediately repeat mutation;
2. inspect GitHub for branch, commits and PR changes;
3. inspect available Flowise execution trace;
4. determine whether the original execution is active, completed or failed;
5. reconcile current head SHA, changed files, reviews and checks;
6. retry only after idempotency and ownership are confirmed.

If execution state cannot be determined safely, return `BLOCKED` with:

- factual observed state;
- whether mutation may have occurred;
- why retry is unsafe;
- required next action.

## 21. Result-first response contract

The external `text` value contains exactly one JSON object. No prose or markdown precedes or follows it.

First property:

```json
{
  "project": "MOEX_Bot"
}
```

Common output:

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

- `result` is mandatory and contains the requested substantive deliverable;
- `evidence` contains only facts relevant to the task and status;
- `nextAction` contains one concrete next action or is omitted when none exists;
- omit optional fields that are not applicable;
- do not emit empty optional strings, empty placeholder objects or repeated false flags;
- do not invent unavailable values;
- `summary` may be used only inside `result`, not as a substitute for the deliverable;
- internal runtime metadata is excluded unless debugging was explicitly requested.

`COMPLETED` is forbidden when `result` does not contain the requested deliverable.

## 22. Evidence by task class

### analyze

Possible evidence:

- repository ref inspected;
- files inspected;
- PR inspected;
- relevant commit or head SHA;
- no mutation performed.

Do not include irrelevant CI or merge fields.

### change

Possible evidence:

- branch;
- commit SHA;
- PR number or URL;
- actual changed files;
- validation results;
- current head SHA;
- current review and checks state when performed.

### validate

Possible evidence:

- target PR or branch;
- current head SHA;
- actual changed files;
- criteria evaluated;
- blocking findings;
- review threads;
- exact-head checks.

### merge

Required evidence:

- PR;
- exact delegated head SHA;
- merge gate results;
- factual merge commit SHA and merged state.

### server_apply

Required evidence:

- authorized merged commit SHA;
- applied commit SHA;
- canonical server target;
- validation result;
- divergence status.

## 23. Status semantics

Allowed main statuses:

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
SUPERSEDED
```

Rules:

- `COMPLETED` means the requested non-merge deliverable is present in `result`;
- `CHANGES_REQUIRED` means repository or PR mutation is required;
- `WAITING_FOR_CHECKS` means exact-head CI remains pending;
- `READY_FOR_MANUAL_MERGE` means every current-head merge-readiness gate passed in manual mode;
- `MERGED` requires factual GitHub merge evidence;
- `BLOCKED` requires a real safe-progress blocker;
- `FAILED` is reserved for unrecoverable execution or tool failure.

## 24. Blocker contract

A blocker result includes only relevant facts:

```json
{
  "project": "MOEX_Bot",
  "taskId": "<task_id>",
  "status": "BLOCKED",
  "result": {
    "completed": "<work completed before the blocker>",
    "notCompleted": "<requested result still missing>"
  },
  "blocker": {
    "code": "<classification>",
    "fact": "<exact missing or conflicting fact>",
    "checkedSources": ["task request", "GitHub"],
    "requiredDecision": "<exact upstream action>"
  },
  "nextAction": "<one safe next action>"
}
```

Do not return `BLOCKED` merely because an optional field was not supplied.

## 25. Final reconciliation gate

Immediately before final output, re-fetch only the state relevant to the task.

For a PR mutation or validation, reconcile:

- current PR state;
- base and head branches;
- full latest head SHA;
- actual changed files and current diff;
- scope and completion criteria;
- current review threads;
- exact-head checks;
- mergeability and merge state.

Discard stale evidence from older head SHAs.

## 26. Compatibility transition

During migration from the old expanded schema:

- the proxy may continue accepting legacy request fields;
- Lead must treat them as optional evidence or authority, not universal requirements;
- the external result must use the result-first contract;
- legacy empty fields and mutation flags must not be emitted unless a temporary downstream parser explicitly requires them;
- any temporary compatibility layer must be removed after regression pilots pass.

## 27. Required regression pilots

Before production sign-off:

1. broad read-only analysis that returns actual findings;
2. read-only PR validation with exact-head evidence;
3. docs-only mutation with exact file scope;
4. blocking review → Worker correction → repeated review in the same PR;
5. timeout reconciliation without duplicate mutation;
6. exact-head delegated automatic merge on a safe task;
7. Browser ↔ Flowise transfer preserving task ID, branch and PR.

A pilot is successful only when the requested substantive result is present and the supporting GitHub evidence is factual.

## 28. Closed Route B

```text
route_b_n8n_universal_role_runner
status: deprecated
new_tasks_allowed: false
```

Historical Route B artifacts may remain as evidence. They do not authorize new Route B tasks, branches, PRs or runtime actions.
