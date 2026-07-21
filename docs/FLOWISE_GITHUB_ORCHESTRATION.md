# Flowise GitHub Orchestration

status: active
version: 2.1
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`

## 1. Purpose

This document defines the active Flowise GitHub execution route for MOEX Bot.

Authoritative management rules are defined in:

```text
docs/MOEX_BOT_MANAGEMENT_CANON.md
```

If this document conflicts with the management canon, the management canon prevails.

## 2. Active route

```text
execution_mode: flowise_automated_github_route
```

Flowise is an execution route. It is not the scope owner and not the final acceptance owner.

PM L2 owns:
- task scope;
- acceptance criteria;
- route approval;
- merge policy and delegation;
- server-apply authorization;
- final acceptance.

## 3. Architecture

```text
User / GPT Action
→ https://flowise-api.foods-tech.store/github-task
→ proxy server
→ Flowise
→ Lead Agent `github-change-orchestrator`
→ `github_worker` and/or GitHub MCP
→ GitHub
```

Components:
- Public endpoint: `https://flowise-api.foods-tech.store/github-task`;
- Main Flowise flow: `github-change-orchestrator`;
- Worker flow/agent: `github-worker`;
- Lead tool name: `github_worker`;
- Worker connection: Agent as Tool.

Secrets, API keys, tokens and passwords must not be stored in documentation, prompts or logs.

## 4. Source of Truth

```text
GitHub repository = Source of Truth
Server filesystem = Applied State only
```

Canonical repository:

```text
Viktoryyyyy/moex-robot
```

Do not infer repository architecture, accepted branch state or authority from server files.

## 5. Responsibility model

### Lead Agent

Lead must:
- interpret the dynamic task request;
- identify task and execution IDs;
- inspect repository, branch and PR state;
- validate route ownership and idempotency;
- call `github_worker` for file mutations;
- inspect PR metadata, exact changed files and diff;
- validate scope and acceptance criteria;
- perform post-PR review;
- obtain checks for the exact head SHA;
- return blocking findings to Worker;
- repeat review and checks after correction;
- merge only when PM L2 delegated merge for the exact task, repository, branch, PR, head SHA and executor;
- return factual structured output.

Lead may use GitHub MCP directly for read-only inspection, checks, review support and authorized merge.

Lead must not perform file mutations directly.

### `github_worker`

Worker must:
- read the repository;
- create or reuse the authorized branch;
- modify only approved files;
- create or update the authorized PR;
- apply approved corrections to the same branch and PR;
- return actual branch, commit, PR and changed files.

Worker must never:
- merge;
- write directly to `main`;
- widen scope;
- create a replacement branch or PR without explicit authorization;
- perform server apply;
- invent GitHub state.

## 6. Route invariants

```text
one task = one active route
one branch = one active mutation owner
one PR = one task
one merge at a time
one server apply at a time
```

Before mutation, Lead must establish:
- current route owner;
- current branch owner;
- existing PR, if any;
- current head SHA;
- active execution state;
- overlapping file scope;
- whether a prior timed-out execution may still be running.

If safe ownership cannot be established, return `BLOCKED`.

## 7. Dynamic request contract

The request contains task-specific information only.

Recommended format:

```text
@PM L2 Flowise

Action: execute
Task ID: <task_id>
Execution ID: <execution_id>
Repository: Viktoryyyyy/moex-robot
Target branch: <base branch>
Working branch: <existing or requested task branch>
Pull request: <existing PR when applicable>

Task:
<exact task>

Allowed scope:
<exact files or scope>

Forbidden scope:
<forbidden files/actions>

Acceptance criteria:
<testable criteria>

Constraints:
<task-specific constraints>

Merge mode: <manual|automatic>
```

For automatic merge, the request must also include:

```text
Merge delegation:
  Task ID: <task_id>
  Repository: Viktoryyyyy/moex-robot
  Working branch: <exact branch>
  Pull request: <exact PR number>
  Expected head SHA: <full exact SHA>
  Merge executor: flowise_lead
```

`Merge mode: automatic` alone is not sufficient authority to merge.

Optional fields:
- root task ID;
- attempt number;
- base SHA;
- review comments;
- evidence requirements;
- route transfer reason;
- fallback allowed;
- server apply allowed.

Do not repeat generic Lead/Worker descriptions, full GitHub workflow, static paths or standard schemas in every request.

## 8. Soft intake

Lead must reconstruct available facts from GitHub.

Missing optional fields must not automatically block execution.

Use `BLOCKED` only when a critical value:
- is absent;
- cannot be derived from the request;
- cannot be verified in GitHub;
- is required for safe execution.

Examples:
- repository cannot be identified;
- approved mutation scope is unknown;
- two executions may control the same branch;
- requested correction has no identifiable PR;
- merge is requested without complete exact-head delegation;
- current GitHub state conflicts with the request.

## 9. Task identity and idempotency

Use:

```text
taskId
executionId
attemptNo
```

Rules:
- `taskId` remains stable across retry and route transfer;
- `executionId` is unique per execution attempt;
- repeated requests with the same `executionId` are potential duplicates;
- inspect GitHub before repeating mutations;
- correction reuses the same branch and PR;
- a replacement branch or PR requires explicit PM L2 authorization.

## 10. Proxy behaviour

The proxy:
1. accepts `POST /github-task`;
2. checks for the `question` field;
3. submits the task to Flowise;
4. filters the response;
5. returns only:

```json
{
  "text": "..."
}
```

Treat `text` as the authoritative external Flowise result and verify repository-changing claims against GitHub.

## 11. Read-only execution

For read-only tasks, Lead may inspect GitHub directly.

Read-only tasks must not:
- create or update branches;
- create commits;
- create or update PRs;
- add comments or reviews;
- rerun checks;
- merge;
- perform server apply.

The result must state that no mutation was performed.

## 12. Mutation execution

For any file mutation:
1. Lead validates task identity, ownership and scope;
2. Lead inspects existing branch and PR state;
3. Lead calls `github_worker`;
4. Worker creates or reuses the authorized branch;
5. Worker modifies only approved files;
6. Worker creates or updates the authorized PR;
7. Worker returns actual GitHub evidence;
8. Lead performs mandatory post-PR review;
9. Lead returns blocking findings to Worker when required;
10. Worker corrects the same branch and PR;
11. Lead repeats review and exact-head checks.

For mutation tasks, observing only Lead and GitHub MCP without a Worker execution is an architecture violation.

## 13. Mandatory post-PR review

After each PR creation or update, Lead must:
1. obtain PR metadata;
2. obtain exact changed files;
3. obtain the diff;
4. verify base and head branches;
5. verify the full head SHA;
6. verify approved and forbidden scope;
7. verify acceptance criteria;
8. perform code review;
9. obtain checks for the exact head SHA;
10. identify blocking findings;
11. return Worker for correction when required;
12. repeat review and checks after a new head SHA.

If PR exists but review was not performed:

```text
status: BLOCKED
reviewStatus: NOT_PERFORMED
mergeStatus: not_merged
```

This combination is forbidden:

```text
pullRequestUrl != ""
reviewStatus = NOT_PERFORMED
status = READY_FOR_MANUAL_MERGE
```

## 14. Exact-head checks

Checks must be tied to the exact latest PR head SHA.

Return when available:
- full head SHA;
- checks status;
- checks source;
- workflow run ID;
- relevant check names.

If the head SHA changes:
- previous review approval is stale;
- previous checks must not be reused;
- review and checks must be repeated;
- prior merge delegation is invalid unless the task contract explicitly permits revalidation and delegated merge on the updated exact head.

Checks must never be inferred or invented.

## 15. Review comments

Recommended structure:

```json
[
  {
    "file": "src/app.py",
    "line": 42,
    "priority": "blocking",
    "comment": "Описание проблемы"
  }
]
```

If there are no findings:

```json
[]
```

Only blocking findings require Worker correction unless the task explicitly includes non-blocking improvements.

## 16. Correction cycle

Correction rules:
- same `taskId`;
- new `executionId` for a new external execution;
- same working branch;
- same PR;
- unchanged approved scope;
- only approved findings corrected;
- actual changed files revalidated;
- review and checks repeated on the new exact head SHA.

Replacement branch or PR creation is forbidden unless PM L2 explicitly authorizes it.

## 17. Merge policy

### Manual

Default:

```text
Merge mode: manual
```

Lead must not merge.

If all gates pass:

```text
status: READY_FOR_MANUAL_MERGE
reviewStatus: APPROVED
mergeStatus: not_merged
```

### Automatic

Automatic merge is exact task-specific delegation by PM L2.

Lead may merge only when all conditions pass:
1. `Merge mode: automatic` is explicit;
2. a complete `Merge delegation` block is present;
3. delegation Task ID equals the active task ID;
4. delegation Repository equals the active repository;
5. delegation Working branch equals the PR head branch;
6. delegation Pull request equals the exact active PR number;
7. delegation Expected head SHA equals the full current PR head SHA;
8. delegation Merge executor equals `flowise_lead`;
9. actual changed files are within approved scope;
10. acceptance criteria pass;
11. post-PR review is complete;
12. `reviewStatus=APPROVED`;
13. checks for the exact delegated head SHA pass;
14. blocking comments are absent;
15. conflicts are absent;
16. the task does not prohibit merge.

If the Worker updates the PR after delegation, the head SHA changes and the previous delegation becomes invalid. Lead must not merge until PM L2 provides a new exact-head delegation or the original task contract explicitly authorizes revalidation and delegated merge on the updated exact head.

After successful merge:

```text
status: MERGED
mergeStatus: merged
```

If any gate fails:

```text
status: BLOCKED
mergeStatus: not_merged
```

Worker never merges.

## 18. Response contract

Recommended result:

```json
{
  "taskId": "",
  "executionId": "",
  "status": "",
  "summary": "",
  "branch": "",
  "commitSha": "",
  "pullRequestUrl": "",
  "headSha": "",
  "actualChangedFiles": [],
  "checksStatus": "",
  "checksSource": "",
  "workflowRunId": "",
  "reviewStatus": "",
  "reviewComments": [],
  "mergeStatus": "",
  "errors": [],
  "nextStep": ""
}
```

Extended fields are recommended, not a reason to invent unavailable data.

Operational success requires:
- the task reached the agent;
- requested GitHub action completed or was safely blocked;
- branch and PR state are factual;
- checks and review state are factual;
- no unauthorized mutation occurred.

## 19. Status values

Recommended `status` values:

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

Checks:

```text
passed
failed
pending
not_configured
unknown
```

Review:

```text
APPROVED
CHANGES_REQUESTED
NOT_PERFORMED
```

Merge:

```text
merged
not_merged
```

## 20. Timeout reconciliation

A GPT Action or proxy timeout does not prove that Flowise stopped.

After timeout:
1. do not immediately repeat a mutation;
2. inspect GitHub for branch, commit and PR changes;
3. inspect Flowise execution trace when available;
4. determine whether the original execution is active, completed or failed;
5. reconcile latest head SHA and PR state;
6. retry only after idempotency and route ownership are confirmed.

If state cannot be determined safely:

```text
status: BLOCKED
errorClass: flowise_timeout
nextStep: reconcile_execution_state
```

## 21. Route transfer

Flowise ↔ Browser transfer preserves:

```text
root task ID
task ID
approved scope
forbidden scope
acceptance criteria
working branch
PR number
merge policy
authority
```

Transfer creates:

```text
new execution ID
incremented attempt number
new execution mode
route transfer reason
```

Before transfer:
- stop or confirm completion of the previous mutation owner;
- inspect GitHub state;
- reconcile branch, PR and exact head SHA;
- reuse the same branch and PR;
- do not create replacements without explicit PM L2 authorization.

## 22. Loop limits

Recommended defaults:
- Worker correction cycles: 2;
- checks polling attempts: 2–3;
- route transfer attempts: 1;
- total execution time below the external timeout with safety margin where possible.

When limits are exhausted, return `BLOCKED` with factual reason.

## 23. Error classification

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
- whether mutation occurred;
- branch and PR if present;
- actual current state;
- whether retry is safe;
- required next owner.

## 24. Troubleshooting

### Empty request smoke test

```bash
curl -i -X POST https://flowise-api.foods-tech.store/github-task -H 'Content-Type: application/json' -d '{}'
```

Expected response:

```json
{
  "error": "Missing question"
}
```

### Historical transport errors

Previously observed:
- `fetch failed`;
- `SSE 405`;
- long waits during checks polling.

Agent as Tool resolved the prior SSE issue. Reappearance must be diagnosed from current traces rather than assumed to have the same cause.

### Execution trace inspection

Inspect:
- Lead Agent node;
- Lead tool calls;
- `github_worker` call for mutation tasks;
- separate Worker execution;
- GitHub MCP calls;
- final structured output;
- repeated checks loops;
- actual GitHub branch and PR state.

### Ports and deployment

Exact internal proxy and Flowise ports are not established by this management document.

Do not guess them. Verify deployment configuration before making infrastructure claims.

## 25. Server apply boundary

Flowise GitHub orchestration does not authorize server apply.

Default:

```text
serverApplyAllowed: false
serverApplyStatus: not_performed
```

Server apply requires separate PM L2 authorization and the canonical MOEX Bot server context.

## 26. Closed Route B

```text
route_b_n8n_universal_role_runner
status: deprecated
new_tasks_allowed: false
```

Old Route B evidence may remain for history, but it is not active Flowise authority.

Do not:
- send new tasks through Route B;
- create new Route B branches;
- create new Route B PRs;
- use Route B registry state as the active route owner.

## 27. Required pilots

1. Read-only PR inspection with full head SHA and exact checks evidence.
2. Docs-only mutation with exact file scope.
3. Blocking review → Worker correction → repeated review on the same PR.
4. Timeout reconciliation without duplicate mutation.
5. Explicit exact-head delegated automatic merge on a safe task.
6. Route transfer preserving task ID, branch and PR.

Production sign-off requires factual results recorded in the repository or approved management evidence.
