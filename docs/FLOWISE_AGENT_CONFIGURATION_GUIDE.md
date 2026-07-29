# Flowise Agent Configuration Guide

status: current_merged_source
version: 1.1
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
management_canon_version: 2.0
route_document: `docs/FLOWISE_GITHUB_ORCHESTRATION.md`
route_document_version: 3.0
lead_prompt: `docs/FLOWISE_LEAD_AGENT_PROMPT.md`
lead_prompt_version: 2.0
worker_prompt: `docs/FLOWISE_GITHUB_WORKER_PROMPT.md`
worker_prompt_version: 2.0
configuration_registry: `docs/MOEX_BOT_CONTEXT_CONFIGURATION_SOURCES.md`
repository: `Viktoryyyyy/moex-robot`

## 1. Purpose

This guide defines the recommended Flowise Applied State for the MOEX Bot GitHub execution route.

It covers:

- Lead Agent configuration;
- Worker Agent configuration;
- Agent as Tool wiring;
- model and memory recommendations;
- tool authority boundaries;
- Flow State and retry controls;
- result formatting;
- regression verification;
- configuration-drift control.

This file is a configuration recommendation and application guide. It is not proof that the Flowise settings have been applied.

The active source text for each Agent prompt remains:

```text
Lead  → docs/FLOWISE_LEAD_AGENT_PROMPT.md
Worker → docs/FLOWISE_GITHUB_WORKER_PROMPT.md
```

Do not manually shorten, paraphrase or merge those prompt sources while applying them to Flowise.

## 2. Target architecture

```text
User / GPT Action
→ public proxy
→ Lead Agent `github-change-orchestrator`
→ GitHub read/review/check tools
→ Agent as Tool `github_worker`
→ Worker Agent `github-worker`
→ GitHub implementation tools
→ Lead independent reconciliation
→ compact result-first response
```

Current documented endpoint:

```text
https://flowise-api.foods-tech.store/github-task
```

Current documented proxy contract:

```json
{
  "question": "<dynamic task request>"
}
```

Authoritative external response:

```json
{
  "text": "<Lead final response>"
}
```

The proxy must not expose Flowise internal runtime objects, credentials, traces or tool-call payloads in its normal response.

## 3. Common configuration principles

### 3.1 Source of Truth

```text
GitHub repository = Source of Truth
Flowise Agent settings = Applied State
Server filesystem = Applied State only
```

A prompt file existing in GitHub does not prove that the corresponding Flowise Agent was updated.

### 3.2 Stateless task execution

Each API execution must begin from the current dynamic request and current GitHub state.

Recommended setting:

```text
persistent conversation memory: disabled
ephemeral execution memory: enabled when available
```

Do not let a new task inherit branch, PR, SHA, scope, authority or result assumptions from a previous Flowise conversation.

Task continuity must come from:

- stable `Task ID`;
- current GitHub state;
- the existing task branch and PR;
- system-generated execution metadata when needed.

### 3.3 Model requirements

Both Agents require a model that supports reliable tool or function calling.

Lead model requirements:

- strong multi-step reasoning;
- reliable tool selection;
- sufficient context for prompts, diffs and review evidence;
- consistent structured JSON text generation;
- low hallucination rate when tools return partial information.

Worker model requirements:

- reliable repository-tool use;
- accurate file-content generation and editing;
- strict scope compliance;
- consistent compact result generation.

Do not hardcode a vendor-specific model name in the management canon. Record the actually applied provider and model in the Applied State record.

Recommended generation settings:

| Setting | Lead | Worker |
|---|---:|---:|
| Temperature | `0.0–0.2` | `0.0–0.1` |
| Top P | provider default or `1.0` | provider default or `1.0` |
| Frequency penalty | `0` | `0` |
| Presence penalty | `0` | `0` |
| Streaming | optional | optional |
| Tool calling | required | required |

Prefer deterministic settings. Creative variation is not useful for repository control.

Set the output-token limit high enough for the requested substantive result and task-relevant evidence. Do not use a small limit that truncates diffs, findings, blocker details or the final JSON.

## 4. Lead Agent configuration

### 4.1 Identity

```text
Flowise Agent name: github-change-orchestrator
Role: Lead orchestration and independent validation
Prompt source: docs/FLOWISE_LEAD_AGENT_PROMPT.md
Required prompt version after adoption: 2.0
```

The first persistent instruction line must retain:

```text
PROJECT=MOEX_Bot
```

### 4.2 Lead responsibilities

Lead must be able to:

- classify `analyze`, `change`, `validate`, `merge` and `server_apply` requests;
- recover current repository facts from GitHub;
- perform read-only inspection directly;
- delegate repository file mutation to `github_worker`;
- independently verify Worker output;
- review the current PR head;
- inspect unresolved review threads;
- obtain exact-head CI evidence;
- control correction cycles;
- merge only under exact authority;
- return the substantive result before technical evidence.

### 4.3 Lead tools

Recommended Lead tool groups:

#### Read-only GitHub tools

Enable only the tools required to retrieve:

- repository and default-branch metadata;
- branches and commits;
- files and patches;
- PR metadata and exact changed filenames;
- review comments and review threads;
- workflow runs, jobs and commit checks;
- mergeability and merged state.

#### Review-thread tools

Enable factual reply and thread-resolution capabilities only when the Lead prompt and GitHub state support the correction workflow.

Lead must re-fetch the thread after resolution and confirm the current GitHub state.

#### Merge tool

Merge capability may be present on Lead only.

Where Flowise supports per-tool approval, configure merge as approval-required or place it behind an explicit deterministic authority condition.

The merge tool must not execute unless the request includes valid exact-head authority and all current-head gates pass.

#### Worker tool

Configure the Worker Agent through Agent as Tool.

Required tool name:

```text
github_worker
```

Recommended tool description:

```text
Implement or correct one MOEX Bot repository task within the approved scope. Reuse the authorized task branch and PR when they exist. Never merge, never apply to the server, never widen scope, and return the implementation result plus factual GitHub evidence.
```

Selected Agentflow or Agent:

```text
github-worker
```

The description must make the authority boundary clear enough that Lead does not delegate read-only analysis, merge or server apply to Worker.

### 4.4 Lead tool exclusions

Do not expose general repository file-write tools directly to Lead when the same mutation is supposed to be performed through Worker.

Lead must not have a hidden alternate mutation path that bypasses:

- Worker scope control;
- branch and PR reuse;
- implementation reporting;
- independent Lead verification.

Do not expose a server execution or server-apply tool to Lead under the normal GitHub route.

### 4.5 Lead input

The dynamic request enters as the current user input or `question` value.

Normal minimum:

```text
PROJECT=MOEX_Bot

@PM L2 Flowise

Action: <analyze|change|validate|merge|server_apply>
Task ID: <stable_task_id>

Task:
<required substantive result>

Done when:
- <testable criterion>
```

Optional task-specific fields:

```text
Target
Scope
Constraints
Authority
Merge mode
```

Do not use separate mandatory input variables for execution ID, attempt number, main SHA, working branch, PR, head SHA, changed files, review status, workflow run ID or merge state when Lead can recover them.

## 5. Worker Agent configuration

### 5.1 Identity

```text
Flowise Agent name: github-worker
Role: delegated repository implementation executor
Prompt source: docs/FLOWISE_GITHUB_WORKER_PROMPT.md
Required prompt version after adoption: 2.0
```

The first persistent instruction line must retain:

```text
PROJECT=MOEX_Bot
```

### 5.2 Worker tools

Enable only repository implementation capabilities required to:

- read authorized repository files;
- inspect the current base and task branch;
- create the authorized task branch when required;
- update or create files within approved scope;
- create task-specific commits;
- create or update the authorized PR;
- run available task-relevant validation when supported;
- return actual branch, commit, PR and changed-file evidence.

### 5.3 Worker tool exclusions

Worker must not receive tools that can:

- merge a PR;
- write directly to `main`;
- perform server commands or server apply;
- approve reviews;
- resolve final acceptance;
- widen repository permissions beyond the canonical repository;
- mutate unrelated repositories.

Do not connect the Lead Agent back to Worker as a recursive Agent as Tool.

### 5.4 Worker input from Lead

Lead should delegate only the task-specific implementation package:

```text
PROJECT=MOEX_Bot
Action: change
Task ID: <stable_task_id>
Task: <implementation or correction result>
Done when:
- <criterion>
Scope:
- <approved path or functional boundary>
```

Add existing branch, PR, blocker findings or constraints only when needed to remove ambiguity or preserve exact task state.

Worker does not need the entire Lead prompt, management canon or universal GitHub lifecycle in every call.

### 5.5 Worker completion boundary

Worker `COMPLETED` means only that the delegated implementation or correction is complete and factual evidence is returned.

It does not mean:

- Lead review passed;
- review threads are resolved;
- exact-head CI passed;
- the PR is merge-ready;
- the PR was merged;
- the implementation was applied to the server.

## 6. Agentflow V2 state recommendations

When this route is implemented in Agentflow V2, initialize all Flow State keys in the Start node before any operational node attempts to update them.

Recommended minimum state schema:

```json
{
  "project": "MOEX_Bot",
  "taskId": "",
  "action": "",
  "requestedResult": "",
  "doneWhen": [],
  "target": null,
  "scope": [],
  "constraints": [],
  "authority": null,
  "mergeMode": "manual",
  "executionId": "",
  "attemptNo": 1,
  "repository": "Viktoryyyyy/moex-robot",
  "baseBranch": "",
  "workingBranch": "",
  "pullRequestNumber": null,
  "currentHeadSha": "",
  "workerResult": null,
  "validationVerdict": "",
  "status": "",
  "blocker": null,
  "finalResult": null
}
```

Use Flow State for one execution only.

Do not use Flow State as a cross-run Source of Truth. Reconstruct repository facts from GitHub on each new run.

## 7. Loop and retry controls

Use explicit workflow limits instead of open-ended Agent recursion.

Recommended limits:

| Control | Maximum |
|---|---:|
| Worker correction cycles | `2` |
| Same failed tool retry | `1` |
| Exact-head CI polling attempts | `3` |
| Browser ↔ Flowise route transfers | `1` unless PM L2 approves more |
| Replacement branch or PR | `0` without explicit authority |

If Flowise Loop nodes are used, set `Max Loop Count` explicitly for the applicable loop. Do not rely on a higher platform default when the management contract requires a lower limit.

Use separate bounded loops for:

- Worker correction;
- exact-head CI polling.

Do not combine correction and CI polling into one ambiguous loop counter.

After the configured limit is reached, return a factual blocker or waiting status rather than continuing indefinitely.

## 8. Human-in-the-loop recommendations

Where supported by the selected Flowise node and tool configuration:

- read-only GitHub tools: no approval required;
- Worker mutation tool: authority is supplied by the approved `change` task and scope;
- review-thread resolution: allow only after Lead verifies the correction;
- merge tool: approval required or deterministic exact-authority gate required;
- server apply: not available in this GitHub route.

Human approval in Flowise does not replace PM L2 authority. It is an additional execution safeguard.

## 9. Result formatting

### 9.1 Lead final result

Lead returns one machine-readable JSON object as text.

Common contract:

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

- `project` is the first property;
- `result` contains the requested substantive deliverable;
- `evidence` contains only task-relevant facts;
- optional sections are omitted when not applicable;
- do not emit large sets of empty fields or false flags;
- do not place prose outside the JSON when the proxy consumer expects machine-readable output.

### 9.2 Worker result

Recommended mutation-result contract:

```json
{
  "project": "MOEX_Bot",
  "taskId": "task-123",
  "status": "COMPLETED",
  "result": {
    "implemented": "Updated the approved document contract",
    "doneWhen": {
      "delegated action is explicit": "passed",
      "result contains no empty optional sections": "passed"
    }
  },
  "changes": {
    "branch": "task/example",
    "commitSha": "<full_sha>",
    "pullRequestUrl": "<verified_url>",
    "actualChangedFiles": [
      "docs/example.md"
    ]
  },
  "validation": {
    "performed": "Compared the current file and approved scope",
    "result": "passed"
  },
  "nextAction": "Lead performs independent verification"
}
```

Worker must omit `changes` or `validation` when not applicable rather than returning empty structures.

### 9.3 Structured-output implementation option

The primary configuration remains prompt-enforced JSON text because the proxy returns the Lead `text` field.

If additional output normalization is needed, use a final deterministic LLM node with JSON Structured Output after Lead processing, provided that:

- it receives the complete substantive Lead result;
- it does not remove blocker or evidence details;
- it does not invent missing GitHub facts;
- it preserves `project` as the first external JSON property;
- it does not become an alternate decision-making or validation Agent.

Do not use output normalization to convert an incomplete result into `COMPLETED`.

## 10. Credentials and permission scope

Store credentials only in Flowise credential or secret-management facilities.

Do not place credentials in:

- Agent prompts;
- task requests;
- Flow State;
- source files;
- screenshots;
- execution summaries;
- proxy responses.

Use separate credentials or permission sets when practical:

- Lead read/review/check access;
- Worker repository mutation access;
- Lead merge access.

Apply least privilege.

Worker permissions must not include merge or server access.

Restrict repository access to:

```text
Viktoryyyyy/moex-robot
```

unless a separate owner-approved task explicitly changes the repository boundary.

## 11. Observability

Enable Flowise execution tracing or logs sufficient to establish:

- execution start and completion;
- generated internal execution ID;
- Agent and tool sequence;
- Worker invocation count;
- correction-loop count;
- CI polling count;
- timeout or tool failure;
- final external result.

Redact secrets and private tool payloads.

Tracing supports diagnosis but does not replace GitHub evidence.

After a proxy or Flowise timeout, inspect both trace and GitHub before retrying mutation.

## 12. Application procedure

Do not apply this package to Flowise as active authority until the package PR is merged.

After merge:

1. Record the merged `main` SHA.
2. Open Flowise Agent `github-change-orchestrator`.
3. Replace the persistent prompt with the exact merged content from `docs/FLOWISE_LEAD_AGENT_PROMPT.md` after its metadata header and separator, preserving `PROJECT=MOEX_Bot` as the first instruction line.
4. Verify Lead tool inventory and remove unauthorized mutation or server tools.
5. Configure Agent as Tool with name `github_worker`, selected Agent `github-worker` and the approved description.
6. Open Flowise Agent `github-worker`.
7. Replace its persistent prompt with the exact merged content from `docs/FLOWISE_GITHUB_WORKER_PROMPT.md` after its metadata header and separator.
8. Verify Worker tool inventory and remove merge, review-approval and server tools.
9. Confirm memory, model, deterministic generation and loop settings.
10. Save both Agents.
11. Inspect the saved prompts and tool lists after saving.
12. Run the regression suite.
13. Record the Applied State evidence.
14. Mark verification `passed` only after all required regressions pass.

Do not edit the applied prompts directly to fix a discovered source defect. Correct the GitHub canonical source first, merge it, then reapply.

## 13. Required regression suite

### 13.1 Minimal read-only analysis

Input contains only:

```text
PROJECT=MOEX_Bot
Action: analyze
Task ID: flowise-config-regression-readonly
Task: Inspect one identified repository document and return the requested findings.
Done when:
- findings are returned
- no mutation occurs
```

Expected:

- Lead accepts the minimal contract;
- Worker is not called;
- no branch, commit, PR, comment, review, check rerun or merge occurs;
- substantive findings are returned before evidence.

### 13.2 Read-only PR validation

Expected:

- current PR state is recovered from GitHub;
- exact changed files and diff are inspected;
- review threads and exact-head CI are checked when available;
- verdict is `PASS`, `CHANGES_REQUIRED` or `BLOCKED`;
- no repository mutation occurs.

### 13.3 Controlled one-file mutation

Expected:

- Lead calls `github_worker`;
- Worker uses a task-specific branch;
- only the approved file changes;
- Worker returns implementation result and evidence;
- Lead independently verifies the PR and current head;
- no merge occurs in manual mode.

### 13.4 Same-branch correction

Expected:

- same Task ID, branch and PR are reused;
- only approved findings are corrected;
- no replacement branch or PR is created;
- Lead repeats review and exact-head checks on the new head.

### 13.5 Merge denial without authority

Expected:

- Lead does not merge when `Merge mode: automatic` is absent or exact authority is incomplete;
- Worker never attempts merge;
- result identifies the missing authority or returns manual readiness only after all gates pass.

### 13.6 Exact-head authority check

For a separately approved controlled test, verify that a changed PR head invalidates old review, checks and merge delegation.

### 13.7 Timeout reconciliation

Expected:

- mutation is not immediately repeated after timeout;
- Lead inspects GitHub and available Flowise trace;
- existing branch, commit and PR are reused;
- duplicate commits and PRs are not created.

## 14. Applied State record

Record separately for Lead and Worker:

```text
target
source_file
source_version
source_commit_sha
source_main_sha
provider
model
memory_configuration
tool_inventory
agent_as_tool_name
loop_limits
applied_at
applied_by
verification_method
verification_status
```

Allowed verification status:

```text
pending
passed
failed
superseded
```

Do not mark `passed` when:

- the source commit is not merged into `main`;
- the prompt differs materially from the merged source;
- tool permissions violate the authority boundary;
- memory can import stale task state;
- required regressions have not passed.

## 15. Configuration drift indicators

Configuration drift exists when any of the following is observed:

- Lead or Worker prompt differs materially from its canonical source;
- Lead and Worker use incompatible task or result contracts;
- `Action: execute` remains required for new tasks;
- execution ID, SHA, branch or PR are mandatory input despite being recoverable;
- Agent memory imports stale task state;
- Lead can mutate repository files without Worker;
- Worker can merge or access the server;
- correction creates replacement branches or PRs;
- exact-head checks are not repeated after a new head;
- output contains only technical metadata and no substantive result;
- an unmerged branch source is applied as active authority.

On detected drift:

1. mark verification `failed`;
2. record the exact difference;
3. determine whether the GitHub source or Applied State is wrong;
4. correct the canonical source first when necessary;
5. merge the correction;
6. reapply the accepted source;
7. rerun the regression suite.

## 16. Explicit non-goals

This guide does not authorize:

- a new Route B / n8n runtime;
- a new repository;
- direct writes to `main`;
- automatic merge by default;
- server apply through Worker;
- server apply through the normal Flowise GitHub route;
- persistent cross-task Agent memory;
- replacement of GitHub evidence with Flowise trace;
- applying unmerged prompt sources as active management authority.

## 17. Official Flowise feature references

Configuration must be verified against the currently installed Flowise version and current official Flowise documentation before application.

Relevant official feature areas:

- Agentflow V2 Agent node and Flow State;
- Ephemeral Memory;
- Agent as Tool;
- Tool approval or human-in-the-loop controls;
- Loop node maximum count;
- JSON Structured Output through an LLM node;
- tracing and execution logs.

Flowise node names and available parameters may change between releases. When the installed UI differs from this guide, do not guess. Record the installed version and escalate the exact mismatch before applying an architectural substitute.
