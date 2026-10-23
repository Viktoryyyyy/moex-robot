# Flowise Agent Configuration Guide

status: proposed_source
version: 2.0
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
management_canon_version: 2.0
route_document: `docs/FLOWISE_GITHUB_ORCHESTRATION.md`
route_document_version: 3.0
lead_prompt: `docs/FLOWISE_LEAD_AGENT_PROMPT.md`
lead_prompt_version: 2.0
worker_prompt: `docs/FLOWISE_GITHUB_WORKER_PROMPT.md`
worker_prompt_version: 2.0
model_profile: `docs/GPT6_ASTRA_EXECUTION_PROFILE.md`
model_profile_version: 1.0
configuration_registry: `docs/MOEX_BOT_CONTEXT_CONFIGURATION_SOURCES.md`
repository: `Viktoryyyyy/moex-robot`

## 1. Purpose

This guide defines the recommended Flowise Applied State for the MOEX Bot automated GitHub route.

It covers:

- Lead Agent configuration;
- Worker Agent configuration;
- Agent as Tool wiring;
- GPT-6 Astra model/API settings;
- memory and context handling;
- tool authority boundaries;
- Flow State and retry controls;
- Structured Outputs and external result formatting;
- regression verification;
- configuration-drift control.

This file is configuration guidance. It does not itself grant management, mutation, merge or server authority and does not prove that Flowise settings have been applied.

The exact persistent prompt sources remain:

```text
Lead  -> docs/FLOWISE_LEAD_AGENT_PROMPT.md
Worker -> docs/FLOWISE_GITHUB_WORKER_PROMPT.md
```

The exact model-specific source is:

```text
docs/GPT6_ASTRA_EXECUTION_PROFILE.md
```

Do not shorten, paraphrase or merge the Lead/Worker prompt sources while applying them to Flowise.

## 2. Target architecture

```text
User / GPT Action
-> public proxy
-> Lead Agent `github-change-orchestrator`
-> GitHub read/review/check tools
-> Agent as Tool `github_worker`
-> Worker Agent `github-worker`
-> GitHub implementation tools
-> Lead independent reconciliation
-> compact result-first response
```

Current documented endpoint:

```text
https://flowise-api.foods-tech.store/github-task
```

Current documented proxy request:

```json
{
  "question": "<dynamic task request>"
}
```

Current documented proxy response:

```json
{
  "text": "<Lead final response>"
}
```

The proxy must not expose Flowise internal runtime objects, credentials, traces or raw tool-call payloads in the normal external response.

## 3. Common configuration principles

### 3.1 Source of Truth

```text
GitHub repository = Source of Truth
Flowise Agent settings = Applied State
Server filesystem = Applied State only
```

A source file or branch existing in GitHub does not prove that the corresponding Flowise configuration was applied.

### 3.2 Stateless task execution

Each API execution begins from the current dynamic request and current GitHub state.

Required baseline:

```text
persistent cross-task conversation memory = disabled
ephemeral execution memory = enabled when available
```

A new task must not inherit branch, PR, SHA, scope, authority or result assumptions from a prior task.

Task continuity comes from:

- stable `Task ID`;
- current GitHub state;
- the existing task branch and PR;
- system-generated execution metadata when needed.

### 3.3 GPT-6 Astra model baseline

Preferred Applied State for both Agents:

```text
provider = OpenAI
model = gpt-6-astra
API for tool use = Responses API
```

The management canon remains vendor/model agnostic. The model name belongs only in configuration guidance and Applied State records.

GPT-6 Astra compatibility rules:

```text
reasoning.effort = low | medium | high | xhigh | max
reasoning.effort = none -> forbidden
custom temperature -> unset
custom top_p -> unset
logprobs -> unset
```

Do not carry forward legacy deterministic-generation settings such as `temperature=0`, `temperature=0.1`, `top_p=1` or custom `logprobs` into the Astra configuration.

Deterministic repository control comes from task contracts, tool verification, Structured Outputs, bounded loops and exact-head reconciliation rather than unsupported sampling controls.

### 3.4 Reasoning defaults

Lead:

```text
reasoning.effort = high
```

Escalate Lead to `xhigh` for architecture, difficult security/review work, conflicting repository evidence, multi-PR reconciliation or exact-head validation where additional reasoning is material.

Use `max` only for exceptional high-complexity work where the latency/cost trade-off is explicitly acceptable.

Worker:

```text
reasoning.effort = medium
```

Escalate Worker to `high` for non-trivial code generation, multi-file approved changes or substantive correction cycles.

Do not use `low` for repository mutation unless the change is mechanical, fully specified and independently verified by Lead.

If the installed integration supports GPT-6 Astra configuration updates during an execution, reasoning effort may be changed without rebuilding the prompt prefix. A reasoning change never changes task ID, scope, ownership, authority or completion criteria.

### 3.5 Output capacity

Do not configure a small output limit that can truncate the substantive result, blocker details, validation findings or task-relevant evidence.

GPT-6 Astra supports up to 128,000 output tokens, but ordinary management responses must remain compact and result-first.

Large capacity is a safety margin, not a target response length.

Streaming is optional.

## 4. Lead Agent configuration

### 4.1 Identity

```text
Flowise Agent name: github-change-orchestrator
Role: Lead orchestration and independent validation
Prompt source: docs/FLOWISE_LEAD_AGENT_PROMPT.md
Required prompt version after adoption: 2.0
Model: gpt-6-astra
API: Responses API
Reasoning default: high
```

The first persistent instruction line must retain:

```text
PROJECT=MOEX_Bot
```

### 4.2 Lead responsibilities

Lead must be able to:

- classify `analyze`, `change`, `validate`, `merge` and `server_apply`;
- recover current repository facts from GitHub;
- perform read-only inspection directly;
- delegate repository mutation to `github_worker`;
- independently verify Worker output;
- review the exact current PR head;
- inspect review threads;
- obtain exact-head CI evidence;
- control correction cycles;
- merge only under valid exact authority;
- return the substantive result before technical metadata.

### 4.3 Lead tools

Enable only the GitHub read/review/check tools needed for:

- repository/default-branch metadata;
- branches and commits;
- files, diffs and patches;
- PR metadata and exact changed filenames;
- reviews and review threads;
- workflow runs/jobs/checks tied to the current head;
- mergeability and merged state.

Merge capability may exist on Lead only and must remain behind the existing exact-authority gate.

Do not expose general repository file-write tools directly to Lead when mutation must be performed through Worker.

Do not expose normal server execution/server-apply capability to Lead in the GitHub route.

### 4.4 Worker as Tool

Required tool name:

```text
github_worker
```

Recommended description:

```text
Implement or correct one MOEX Bot repository task within the approved scope. Reuse the authorized task branch and PR when they exist. Never merge, never apply to the server, never widen scope, and return the implementation result plus factual GitHub evidence.
```

Selected Agent:

```text
github-worker
```

Lead must not delegate ordinary read-only analysis, merge or server apply to Worker.

### 4.5 Lead input

Normal dynamic minimum:

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

Optional only when material:

```text
Target
Scope
Constraints
Authority
Merge mode
```

Do not require execution ID, attempt number, main SHA, branch, PR, head SHA, changed files, review status, workflow run ID or merge state when they can be recovered safely.

## 5. Worker Agent configuration

### 5.1 Identity

```text
Flowise Agent name: github-worker
Role: delegated repository implementation executor
Prompt source: docs/FLOWISE_GITHUB_WORKER_PROMPT.md
Required prompt version after adoption: 2.0
Model: gpt-6-astra
API: Responses API
Reasoning default: medium
```

The first persistent instruction line must retain:

```text
PROJECT=MOEX_Bot
```

### 5.2 Worker tools

Enable only repository implementation capabilities required to:

- read authorized repository files;
- inspect current base/task branch;
- create the authorized task branch when required;
- create/update approved files;
- create task-specific commits;
- create/update the authorized PR;
- run available task-relevant validation;
- return actual branch, commit, PR and changed-file evidence.

### 5.3 Worker exclusions

Worker must not receive tools that can:

- merge a PR;
- write directly to `main`;
- run server commands or server apply;
- approve final review;
- resolve final acceptance;
- widen repository permissions;
- mutate unrelated repositories.

Do not connect Lead back to Worker recursively as an Agent as Tool.

### 5.4 Worker input

Lead delegates only task-specific implementation data:

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

### 5.5 Worker completion boundary

Worker `COMPLETED` means only that the delegated implementation/correction is complete and factual evidence is returned.

It does not mean:

- Lead review passed;
- review threads are resolved;
- exact-head CI passed;
- PR is merge-ready;
- PR was merged;
- implementation was applied to the server.

## 6. GPT-6 Astra tool execution

### 6.1 Responses API requirement

Use GPT-6 Astra tool calling through the Responses API.

If the installed Flowise/OpenAI node cannot provide the required Astra tool set through Responses API, mark the migration `blocked` or `partial`. Do not silently configure Chat Completions tool behavior or another model and label it as Astra Applied State.

### 6.2 Async/concurrent read-only work

When the installed integration exposes Astra async tool calling, independent read-only GitHub calls may run concurrently/asynchronously.

Examples:

- repository metadata plus open-PR inventory;
- changed filenames plus review threads;
- exact-head checks plus unrelated read-only file inspection.

### 6.3 Ordered mutation work

Operations whose order affects repository state or authority remain strictly sequential:

```text
ownership reconciliation
-> branch selection/creation
-> mutation
-> commit
-> PR create/update
-> current-head reconciliation
-> review
-> exact-head checks
-> correction
-> re-review
-> merge when separately authorized
```

Never parallelize writes to the same branch or file scope.

Async execution does not relax one-task/one-route, branch ownership, PR ownership or exact-head rules.

### 6.4 Mid-turn steering

When supported, Astra mid-turn steering may narrow scope, correct a factual misunderstanding, add a compatible criterion or reprioritize independent read-only work.

It must not silently:

- widen mutation scope;
- change branch/PR ownership;
- authorize merge;
- authorize server apply;
- restore Route B/n8n;
- replace the management canon.

Consequential authority changes must be explicitly reconciled before mutation continues.

## 7. Structured Outputs and result formatting

### 7.1 Preferred implementation

For GPT-6 Astra, prefer native Structured Outputs for machine-readable Lead and Worker result contracts when the installed integration supports a supplied schema.

The schema must preserve the project contract rather than force empty technical fields.

Common external contract:

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

- `project` remains the first external property when the consumer requires it;
- `result` contains the requested substantive deliverable;
- `evidence` contains only task-relevant facts;
- optional sections are omitted when not applicable;
- `blocker`, `changes`, `validation`, `merge` and `serverApply` remain conditional;
- the model must not invent GitHub facts merely to satisfy the schema;
- incomplete work must not be normalized into `COMPLETED`.

### 7.2 Compatibility fallback

Prompt-enforced JSON text remains allowed only when the current Flowise/proxy integration cannot expose native Structured Outputs without changing the architecture.

Record this explicitly as:

```text
structured_outputs = compatibility_fallback
```

Do not add a second decision-making LLM solely to normalize Astra output when native Structured Outputs are available.

## 8. Agentflow V2 state

Initialize all Flow State keys in the Start node before operational nodes update them.

Recommended minimum:

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

Use Flow State for one execution only. It is not cross-run Source of Truth.

## 9. Loop and retry controls

Use explicit bounded workflow limits:

| Control | Maximum |
|---|---:|
| Worker correction cycles | `2` |
| Same failed tool retry | `1` |
| Exact-head CI polling attempts | `3` |
| Browser <-> Flowise route transfers | `1` unless PM L2 approves more |
| Replacement branch or PR | `0` without explicit authority |

Use separate counters for Worker correction and CI polling.

After a limit is reached, return a factual blocker or waiting status rather than continuing indefinitely.

## 10. Human-in-the-loop

Where supported:

- read-only GitHub tools: no approval required;
- Worker mutation: authority comes from the approved `change` task and scope;
- review-thread resolution: only after Lead verifies correction;
- merge: approval-required or deterministic exact-authority gate;
- server apply: unavailable in the normal GitHub route.

Flowise approval is an additional safeguard and never replaces PM L2 authority.

## 11. Credentials and permission scope

Store credentials only in Flowise credential/secret facilities.

Never put credentials in:

- prompts;
- task requests;
- Flow State;
- repository source files;
- screenshots;
- execution summaries;
- proxy responses.

Use least privilege.

Worker permissions must not include merge or server access.

Restrict normal repository access to:

```text
Viktoryyyyy/moex-robot
```

unless a separate owner-approved task explicitly changes that boundary.

## 12. Observability and interrupted executions

Enable tracing/logging sufficient to establish:

- execution start/completion;
- internal execution ID;
- Agent/tool sequence;
- Worker invocation count;
- correction-loop count;
- CI polling count;
- timeout, model stop or tool failure;
- final external result.

Redact secrets/private payloads.

Tracing supports diagnosis but never replaces GitHub evidence.

If GPT-6 Astra or the runtime pauses/stops an execution, including a safety/misalignment review interruption:

1. do not assume no mutation occurred;
2. do not immediately retry mutation;
3. inspect GitHub branch, commit and PR state;
4. inspect available Flowise trace;
5. restore ownership/idempotency;
6. only then retry or return a factual blocker.

## 13. Application procedure

Do not apply this package as current Flowise authority until the package PR is merged.

After merge:

1. Record the merged `main` SHA.
2. Verify the installed Flowise/OpenAI integration can use `gpt-6-astra` with Responses API tool calling.
3. Open `github-change-orchestrator`.
4. Replace its persistent prompt with the exact merged content from `docs/FLOWISE_LEAD_AGENT_PROMPT.md` after the metadata header/separator, preserving `PROJECT=MOEX_Bot` as the first instruction line.
5. Set provider/model/API according to `docs/GPT6_ASTRA_EXECUTION_PROFILE.md`.
6. Set Lead reasoning default to `high`.
7. Remove custom `temperature`, custom `top_p` and `logprobs`.
8. Verify Lead tool inventory and remove unauthorized mutation/server tools.
9. Configure Agent as Tool `github_worker` pointing to `github-worker`.
10. Open `github-worker`.
11. Replace its persistent prompt with the exact merged content from `docs/FLOWISE_GITHUB_WORKER_PROMPT.md` after metadata header/separator.
12. Set Worker provider/model/API to OpenAI / `gpt-6-astra` / Responses API.
13. Set Worker reasoning default to `medium`.
14. Remove custom `temperature`, custom `top_p` and `logprobs`.
15. Verify Worker has no merge/review-approval/server tools.
16. Enable native Structured Outputs where the installed integration exposes them without changing authority architecture; otherwise record compatibility fallback.
17. Configure async tool calling only for independent read-only work when supported.
18. Confirm persistent cross-task memory is disabled.
19. Confirm bounded loop settings.
20. Save both Agents and re-inspect saved model, prompt and tool settings.
21. Run the regression suite.
22. Record Applied State evidence.
23. Mark verification `passed` only after all required regressions pass.

Do not fix an applied prompt directly when its canonical source is wrong. Correct GitHub source first, merge, then reapply.

## 14. Required regression suite

### 14.1 Minimal read-only analysis

Expected:

- Lead accepts the minimal contract;
- Worker is not called;
- no mutation occurs;
- substantive findings precede evidence.

### 14.2 Read-only PR validation

Expected:

- current PR state is recovered;
- changed files/diff are inspected;
- review threads and exact-head CI are checked when available;
- verdict is `PASS`, `CHANGES_REQUIRED` or `BLOCKED`;
- no mutation occurs.

### 14.3 Controlled one-file mutation

Expected:

- Lead calls `github_worker`;
- Worker uses the authorized task branch;
- only approved scope changes;
- Worker returns implementation/evidence;
- Lead independently verifies current head;
- no merge occurs in manual mode.

### 14.4 Same-branch correction

Expected:

- same Task ID, branch and PR are reused;
- only approved findings are corrected;
- no replacement branch/PR is created;
- Lead repeats review and exact-head checks.

### 14.5 Merge denial without authority

Expected:

- Lead does not merge with incomplete exact authority;
- Worker never attempts merge.

### 14.6 Exact-head authority

A new PR head invalidates prior review/checks/merge delegation and forces revalidation.

### 14.7 Timeout or model-stop reconciliation

Expected:

- mutation is not immediately repeated;
- Lead inspects GitHub and trace;
- existing branch/commit/PR are reused;
- duplicate mutations are not created.

### 14.8 GPT-6 Astra compatibility

Verify:

- actual model is `gpt-6-astra`;
- tool calls use Responses API;
- `reasoning.effort=none` is not sent;
- custom `temperature`, custom `top_p` and `logprobs` are not sent;
- Lead default reasoning is `high`;
- Worker default reasoning is `medium`;
- Structured Outputs are native when supported, otherwise fallback is recorded;
- async/concurrent calls are read-only and independent;
- mid-turn steering cannot silently widen scope or authority.

## 15. Applied State record

Record separately for Lead and Worker:

```text
target
source_file
source_version
source_commit_sha
source_main_sha
provider
model
api
reasoning_effort_default
reasoning_effort_escalation_policy
structured_outputs
async_tool_calling
mid_turn_steering
custom_temperature
custom_top_p
logprobs
memory_configuration
tool_inventory
agent_as_tool_name
loop_limits
applied_at
applied_by
verification_method
verification_status
```

For Astra, expected values include:

```text
provider = OpenAI
model = gpt-6-astra
api = responses
custom_temperature = unset
custom_top_p = unset
logprobs = unset
```

Allowed verification status:

```text
pending
passed
failed
superseded
```

Do not mark `passed` when the source is unmerged, prompts/settings differ materially, permissions violate authority, stale cross-task memory is enabled or regressions have not passed.

## 16. Drift indicators

Configuration drift exists when any of the following is observed:

- Lead/Worker prompt differs materially from canonical source;
- applied model is not the recorded model;
- Astra tool calling is not using the recorded API mode;
- legacy `temperature`, `top_p`, `logprobs` or `reasoning=none` remain configured for Astra;
- Lead and Worker task/result contracts are incompatible;
- `Action: execute` is required for new tasks;
- recoverable repository facts are mandatory user inputs;
- persistent memory imports stale task state;
- Lead can mutate files without Worker;
- Worker can merge or access server runtime;
- correction creates replacement branches/PRs;
- exact-head checks are not repeated on new head;
- async tools introduce concurrent mutation;
- output contains only technical metadata and no substantive result;
- an unmerged source is applied as active authority.

On drift:

1. mark verification `failed`;
2. record the exact mismatch;
3. determine whether GitHub source or Applied State is wrong;
4. correct canonical source first when needed;
5. merge the correction;
6. reapply accepted source;
7. rerun regressions.

## 17. Explicit non-goals

This guide does not authorize:

- Route B / n8n runtime reactivation;
- a new repository;
- direct writes to `main`;
- automatic merge by default;
- server apply through Worker;
- server apply through the normal Flowise GitHub route;
- persistent cross-task Agent memory;
- replacement of GitHub evidence with model memory or Flowise trace;
- applying unmerged prompt/model sources as current management authority;
- widening authority merely because GPT-6 Astra can execute more complex workflows.

## 18. Official feature references

Before application, verify the installed Flowise version and current OpenAI documentation.

Relevant OpenAI references for the current model profile:

```text
https://developers.openai.com/api/docs/guides/latest-model
https://developers.openai.com/api/docs/models/gpt-6-astra
https://openai.com/products/release-notes/
```

Relevant Flowise feature areas:

- Agentflow V2 Agent node and Flow State;
- Ephemeral Memory;
- Agent as Tool;
- OpenAI/Responses API integration available in the installed version;
- tool approval / human-in-the-loop controls;
- Loop node maximum count;
- JSON Structured Output;
- tracing and execution logs.

When the installed UI or node capability differs from this guide, do not guess. Record the exact installed version and mismatch and treat unsupported Astra features as `blocked`, `partial` or explicit compatibility fallback rather than inventing an architectural substitute.
