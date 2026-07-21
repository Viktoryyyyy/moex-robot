# Flowise GitHub Orchestration

status: active
version: 2.2
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
structured_output_amendment: `docs/MOEX_BOT_MANAGEMENT_CANON_AMENDMENT_1_STRUCTURED_OUTPUT.md`

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
canonical repository = Viktoryyyyy/moex-robot
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
- inspect current review threads;
- obtain checks for the exact head SHA;
- return blocking findings to Worker;
- repeat review, thread inspection and checks after correction;
- merge only when PM L2 delegated merge for the exact task, repository, branch, PR, head SHA, merge policy and executor;
- return factual structured output.

Lead may use GitHub MCP directly for read-only inspection, checks, review support, review-thread replies and resolution after verified correction, and authorized merge.

Lead must not perform repository file mutations directly.

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
- approve its own work;
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
  Merge policy: automatic
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

The receiving Agent must not rely on memory from earlier executions. It reconstructs current state from the task identity and GitHub.

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
- resolve review threads;
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
8. Lead independently verifies branch, commit, PR and changed files;
9. Lead performs mandatory post-PR review;
10. Lead inspects current review threads;
11. Lead returns blocking findings to Worker when required;
12. Worker corrects the same branch and PR;
13. Lead repeats review, thread inspection and exact-head checks;
14. Lead performs final GitHub reconciliation before output.

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
8. perform code or documentation review;
9. obtain all current review threads;
10. obtain checks for the exact head SHA;
11. identify blocking findings;
12. return Worker for correction when required;
13. repeat review, thread inspection and checks after a new head SHA.

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

## 14. Review thread authority

GitHub review-thread state is authoritative.

Lead must obtain, when available:
- thread ID;
- `is_resolved`;
- `is_outdated`;
- file path and line;
- current comments.

A thread is outdated only when GitHub explicitly returns `is_outdated=true`.

A blocking thread remains blocking while GitHub returns `is_resolved=false`.

A changed PR head SHA, edited file or apparently corrected finding does not itself make a thread stale or outdated.

After Worker correction, Lead must:
1. verify the correction in the current diff;
2. verify scope compliance;
3. reply to the existing thread with factual evidence;
4. resolve that same thread;
5. fetch review threads again;
6. confirm `is_resolved=true`;
7. check for other unresolved blocking threads.

Do not return `reviewStatus=APPROVED` while any blocking thread remains unresolved.

## 15. Exact-head checks

Checks must be tied to the exact latest PR head SHA.

Return when available:
- `prHeadSha`;
- `checksStatus`;
- `checksSource`;
- `checksHeadSha`;
- workflow run ID;
- relevant check names.

If the head SHA changes:
- previous review approval is stale;
- previous checks must not be reused;
- previous merge readiness is stale;
- review, review-thread inspection and checks must be repeated;
- prior merge delegation is invalid unless the task contract explicitly permits revalidation and delegated merge on the updated exact head.

Checks must never be inferred or invented.

Normalize checks:

```text
passed
failed
pending
not_configured
unknown
```

Mapping:
- queued, requested, waiting, in_progress → `pending`;
- failure, timed_out, cancelled, action_required → `failed`;
- completed with success → `passed`;
- no configured checks → `not_configured`;
- unavailable or inconclusive evidence → `unknown`.

`checksHeadSha` must equal `prHeadSha` for merge readiness.

Recommended `checksSource` values:

```text
github_actions
check_runs
commit_status
not_configured
unknown
```

## 16. Checks polling

Recommended maximum checks polling attempts: 3.

If checks remain pending after the configured attempts:

```text
status: WAITING_FOR_CHECKS
checksStatus: pending
failureClassification: ci_pending
mergeStatus: not_merged
nextStep: wait_for_exact_head_ci
```

Do not return `CHANGES_REQUIRED` merely because CI is pending.

Do not rerun checks unless the task explicitly authorizes it and a rerun is required.

## 17. Review comments

`reviewComments` is externally represented as a string containing a serialized JSON array.

No findings:

```json
"reviewComments": "[]"
```

Finding example:

```json
"reviewComments": "[{\"threadId\":\"PRRT_example\",\"is_resolved\":false,\"is_outdated\":false,\"priority\":\"blocking\",\"comment\":\"Description\"}]"
```

Only blocking findings require Worker correction unless the task explicitly includes non-blocking improvements.

## 18. Correction cycle

Correction rules:
- same `taskId`;
- new `executionId` for a new external execution;
- same working branch;
- same PR;
- unchanged approved scope;
- only approved findings corrected;
- actual changed files revalidated;
- review, review-thread inspection and checks repeated on the new exact head SHA.

Replacement branch or PR creation is forbidden unless PM L2 explicitly authorizes it.

If correction requires scope widening:

```text
status: BLOCKED
failureClassification: scope_violation
nextStep: PM L2 must approve explicit scope widening
```

Recommended maximum Worker correction cycles: 2.

When correction limits are exhausted:

```text
status: BLOCKED
failureClassification: execution_loop_exhausted
```

## 19. Merge policy

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
checksStatus: passed
mergeStatus: not_merged
mergePerformed: false
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
8. delegation Merge policy equals `automatic`;
9. delegation Merge executor equals `flowise_lead`;
10. actual changed files are within approved scope;
11. acceptance criteria pass;
12. post-PR review is complete;
13. `reviewStatus=APPROVED`;
14. all blocking review threads are resolved;
15. checks for the exact delegated head SHA passed;
16. `checksHeadSha=prHeadSha`;
17. conflicts are absent;
18. the task does not prohibit merge.

If Worker updates the PR after delegation, the head SHA changes and the previous delegation becomes invalid. Lead must not merge until PM L2 provides a new exact-head delegation or the original task contract explicitly authorizes revalidation and delegated merge on the updated exact head.

After successful merge:

```text
status: MERGED
mergeStatus: merged
mergePerformed: true
```

If any merge gate fails:

```text
status: BLOCKED
mergeStatus: not_merged
mergePerformed: false
```

Worker never merges.

## 20. Final reconciliation gate

Immediately before final output, Lead must re-fetch and reconcile:
1. current PR metadata and state;
2. base and head branches;
3. full current PR head SHA;
4. exact changed filenames;
5. current diff;
6. scope and acceptance criteria;
7. all current review threads and their resolution/outdated state;
8. workflow runs and jobs for the exact current head SHA;
9. `checksHeadSha=prHeadSha`;
10. mergeability and merge state.

Discard stale evidence from older head SHAs.

Build final output only from the reconciled state.

## 21. Response contract

The external `text` result must contain exactly one JSON object. No prose or markdown may precede or follow it.

The first property must be:

```json
{
  "project": "MOEX_Bot"
}
```

Recommended result:

```json
{
  "project": "MOEX_Bot",
  "taskId": "",
  "executionId": "",
  "status": "",
  "summary": "",
  "branch": "",
  "commitSha": "",
  "pullRequestUrl": "",
  "prHeadSha": "",
  "actualChangedFiles": [],
  "checksStatus": "",
  "checksSource": "",
  "checksHeadSha": "",
  "workflowRunId": "",
  "reviewStatus": "",
  "reviewComments": "[]",
  "mergeStatus": "",
  "filesChanged": false,
  "branchCreated": false,
  "commitCreated": false,
  "prCreated": false,
  "reviewCreated": false,
  "checksRerun": false,
  "mergePerformed": false,
  "serverApplyStatus": "not_performed",
  "failureClassification": "",
  "nextStep": "",
  "errors": ""
}
```

Extended fields are not a reason to invent unavailable data.

Types:
- `actualChangedFiles` is an array of strings;
- mutation flags are JSON booleans;
- `reviewComments` is a string containing a serialized JSON array;
- `errors` is always a string;
- all SHA, ID, status, source, classification and next-step fields are strings.

Use `"errors": ""` when no error exists. Do not return `errors` as a boolean, array, null or object.

Operational success requires:
- the task reached the Agent;
- the requested GitHub action completed or was safely blocked;
- branch and PR state are factual;
- checks and review state are factual;
- no unauthorized mutation occurred.

## 22. Status values

Allowed `status` values:

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

Allowed checks:

```text
passed
failed
pending
not_configured
unknown
```

Allowed review:

```text
APPROVED
CHANGES_REQUESTED
NOT_PERFORMED
```

Allowed merge:

```text
merged
not_merged
```

Rules:
- `CHANGES_REQUIRED` means an additional repository or PR mutation is required;
- pending CI uses `WAITING_FOR_CHECKS`, not `CHANGES_REQUIRED`;
- `READY_FOR_MANUAL_MERGE` requires current-head review approval, no unresolved blocking threads, passed exact-head checks and `mergeStatus=not_merged`;
- `MERGED` requires factual GitHub merge evidence and `mergeStatus=merged`;
- `COMPLETED` is normally used for read-only, inspection or no-PR tasks.

## 23. Timeout reconciliation

A GPT Action or proxy timeout does not prove that Flowise stopped.

After timeout:
1. do not immediately repeat a mutation;
2. inspect GitHub for branch, commit and PR changes;
3. inspect Flowise execution trace when available;
4. determine whether the original execution is active, completed or failed;
5. reconcile latest head SHA, review threads, checks and PR state;
6. retry only after idempotency and route ownership are confirmed.

If state cannot be determined safely:

```text
status: BLOCKED
failureClassification: flowise_timeout
nextStep: reconcile_execution_state
```

## 24. Route transfer

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

## 25. Loop limits

Recommended defaults:
- Worker correction cycles: 2;
- checks polling attempts: 3;
- route transfer attempts: 1;
- total execution time below the external timeout with safety margin where possible.

When limits are exhausted, return `BLOCKED` with factual evidence and `failureClassification=execution_loop_exhausted` when applicable.

## 26. Error classification

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

## 27. Troubleshooting

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

## 28. Server apply boundary

Flowise GitHub orchestration does not authorize server apply.

Default:

```text
serverApplyAllowed: false
serverApplyStatus: not_performed
```

Server apply requires separate PM L2 authorization and the canonical MOEX Bot server context.

## 29. Closed Route B

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

## 30. Required pilots

1. Read-only PR inspection with full head SHA and exact checks evidence.
2. Docs-only mutation with exact file scope.
3. Blocking review → Worker correction → repeated review on the same PR.
4. Timeout reconciliation without duplicate mutation.
5. Explicit exact-head delegated automatic merge on a safe task.
6. Route transfer preserving task ID, branch and PR.

Production sign-off requires factual results recorded in the repository or approved management evidence.
