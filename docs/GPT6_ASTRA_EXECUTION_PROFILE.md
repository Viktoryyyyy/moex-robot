# GPT-6 Astra Execution Profile

status: proposed_source
version: 1.0
model_provider: OpenAI
model: `gpt-6-astra`
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
configuration_guide: `docs/FLOWISE_AGENT_CONFIGURATION_GUIDE.md`
configuration_registry: `docs/MOEX_BOT_CONTEXT_CONFIGURATION_SOURCES.md`
repository: `Viktoryyyyy/moex-robot`
source_checked_at: 2026-09-06

## 1. Purpose

This document defines the model-specific execution profile for running the MOEX Bot Browser/ChatGPT and Flowise management workflows on OpenAI GPT-6 Astra.

It is configuration guidance only. It does not replace or widen:

- `docs/MOEX_BOT_MANAGEMENT_CANON.md`;
- Browser Project Context or Browser role authority;
- Flowise Lead/Worker authority boundaries;
- task-specific scope, merge authority or server-apply authority.

The management canon and role prompts remain model-agnostic. This file contains only model-dependent behavior and API settings.

## 2. Canonical model identity

```text
provider = OpenAI
model = gpt-6-astra
preferred API for tool use = Responses API
context window = 1,050,000 tokens
max output = 128,000 tokens
knowledge cutoff = 2026-04-30
```

Use the exact model ID `gpt-6-astra` when configuring the OpenAI API.

GPT-6 Astra supports text and image input, text output, function calling, Structured Outputs and reasoning tokens.

## 3. Required migration rules

When migrating an existing OpenAI model configuration to GPT-6 Astra:

```text
reasoning.effort = low | medium | high | xhigh | max
reasoning.effort = none  -> forbidden
custom temperature       -> remove
custom top_p             -> remove
logprobs                  -> remove
```

Do not attempt to emulate deterministic repository behavior with unsupported `temperature` or `top_p` settings. Determinism must come from:

- precise task contracts;
- explicit authority boundaries;
- native Structured Outputs where available;
- tool verification against GitHub;
- bounded loops and retries;
- exact-head reconciliation.

Tool calling must use the Responses API for GPT-6 Astra.

## 4. Default reasoning policy

### Lead / orchestration

Default:

```text
reasoning.effort = high
```

Use `xhigh` when the task materially involves one or more of:

- architecture decisions;
- multi-file or multi-PR reconciliation;
- security-sensitive review;
- conflicting repository evidence;
- exact-head validation before merge;
- complex causal or quantitative analysis.

Use `max` only for exceptional tasks where additional reasoning depth is worth the extra latency/cost and the task is not a routine control action.

Use `medium` for bounded, low-risk follow-up work after the repository state and scope are already established.

### Worker / implementation

Default:

```text
reasoning.effort = medium
```

Escalate to `high` for:

- non-trivial code generation or refactoring;
- multi-file edits within an approved scope;
- test repair where failure cause is not obvious;
- correction after a substantive Lead review finding.

Do not use `low` for mutation work unless the task is mechanical, fully specified and independently verified by Lead.

## 5. Reasoning changes during one execution

When the client/runtime supports GPT-6 Astra configuration updates, reasoning effort may be changed during the same conversation without rebuilding the prompt prefix.

Permitted pattern:

```text
intake / simple recovery     -> medium
complex implementation       -> high
architecture or hard review  -> xhigh
routine final formatting     -> medium
```

A reasoning-effort change is an execution optimization only. It must not change:

- task ID;
- approved scope;
- branch or PR ownership;
- merge authority;
- server-apply authority;
- completion criteria.

If the installed Flowise/OpenAI integration does not expose this feature, keep one fixed reasoning effort for the execution. Do not invent an unsupported substitute.

## 6. Structured Outputs

For machine-readable Lead/Worker contracts, prefer native Structured Outputs over prompt-only JSON when the installed integration supports a supplied schema.

Required behavior:

- preserve `project = MOEX_Bot`;
- preserve the substantive `result`;
- omit optional sections when not applicable;
- never invent GitHub facts to satisfy a schema;
- never convert an incomplete result into `COMPLETED` merely to satisfy validation;
- keep `blocker`, `changes`, `validation`, `merge` and `serverApply` conditional.

Prompt-enforced JSON remains an allowed compatibility fallback only when the current Flowise node/proxy cannot expose native Structured Outputs without changing the architecture.

Do not add a second decision-making LLM merely to normalize formatting when GPT-6 Astra native Structured Outputs are available.

## 7. Tool-calling policy

Use GPT-6 Astra tool calling through the Responses API.

### Read-only tools

Independent read-only GitHub calls may be issued concurrently or asynchronously when the client and tool contract support it and when results are independent.

Examples:

- repository metadata + open PR inventory;
- changed-file inventory + review-thread retrieval;
- exact-head checks + read-only file inspection.

### Mutation tools

Do not use async or parallel execution for operations whose order changes repository state or authority.

The following remain strictly ordered:

```text
branch ownership reconciliation
-> file mutation
-> commit
-> PR create/update
-> current-head reconciliation
-> review
-> exact-head checks
-> correction
-> re-review
-> merge when separately authorized
```

Never run concurrent writes to the same branch or file scope.

Async tool calling does not relax the management canon's ownership or exact-head rules.

## 8. Mid-turn steering

When the transport supports GPT-6 Astra mid-turn steering, additional instructions may be incorporated into the current execution without discarding already completed work.

Steering may:

- narrow scope;
- add a non-conflicting completion criterion;
- correct a factual misunderstanding;
- reprioritize independent read-only work;
- ask a side question that does not alter authority.

Steering must not silently:

- widen mutation scope;
- change branch/PR ownership;
- authorize merge;
- authorize server apply;
- restore Route B / n8n;
- replace the management canon.

A consequential authority change remains an explicit task decision and must be reconciled before mutation continues.

## 9. Memory, caching and long context

Persistent cross-task Agent memory remains disabled.

GPT-6 Astra's long context and prompt caching may be used to reduce repeated prompt cost, but cached text is not Source of Truth.

Current GitHub state must still be re-read for repository facts that can change, including:

- default-branch SHA;
- task branch;
- PR head SHA;
- changed files and diff;
- reviews and threads;
- checks;
- mergeability and merge state.

Do not use model context, prompt cache or persisted reasoning as a substitute for current GitHub evidence.

## 10. Output policy

Do not set a small output limit that can truncate the substantive result, blocker details or task-relevant evidence.

The model maximum is 128,000 output tokens, but ordinary management responses should remain compact and result-first.

Large output capacity is a safety margin, not a target response length.

Streaming is optional.

## 11. Safety and interrupted executions

GPT-6 Astra may be subject to asynchronous misalignment monitoring in supported Responses API executions.

If a model/tool execution is paused or stopped for review:

1. do not treat the interruption as proof that no repository mutation occurred;
2. do not automatically retry a mutation;
3. reconcile GitHub branch, commit and PR state;
4. inspect available execution trace;
5. restore ownership and idempotency before any retry;
6. return a factual blocker when safe continuation requires human review.

A safety stop does not grant permission to bypass the existing route, tool or authority controls.

## 12. Flowise target profile

Preferred Applied State when the installed Flowise/OpenAI integration supports GPT-6 Astra fully:

### Lead

```text
provider = OpenAI
model = gpt-6-astra
API = Responses API
reasoning.effort = high
Structured Outputs = enabled for machine-readable external contract when supported
persistent cross-task memory = disabled
streaming = optional
custom temperature = unset
custom top_p = unset
logprobs = unset
```

### Worker

```text
provider = OpenAI
model = gpt-6-astra
API = Responses API
reasoning.effort = medium
Structured Outputs = enabled for delegated result contract when supported
persistent cross-task memory = disabled
streaming = optional
custom temperature = unset
custom top_p = unset
logprobs = unset
```

If the installed Flowise version cannot use GPT-6 Astra through the Responses API with the required tool set, mark the migration `blocked` or `partial`; do not silently fall back to Chat Completions tool behavior or another model as if Astra were applied.

## 13. Browser / ChatGPT target profile

The existing Browser Project Context and Browser role contexts remain valid because their authority and lifecycle rules are model-agnostic.

For a ChatGPT session using GPT-6 Astra:

- keep the current Project Context and relevant role context unchanged unless a separate management-contract defect is found;
- use the model's stronger context handling to avoid unnecessary repeated intake;
- make routine recoverable assumptions only where the project rules already allow recovery;
- ask or block only when a consequential decision cannot be safely recovered;
- preserve the five Actions and result-first output contract;
- continue to use GitHub as Source of Truth.

Do not add GPT-6-specific wording to the management canon merely to exploit a model feature.

## 14. Applied State record additions

For GPT-6 Astra record, when available:

```text
provider = OpenAI
model = gpt-6-astra
api = responses
reasoning_effort_default
reasoning_effort_escalation_policy
structured_outputs = enabled | compatibility_fallback | unavailable
async_tool_calling = enabled_read_only | disabled
mid_turn_steering = enabled | unavailable
custom_temperature = unset
custom_top_p = unset
logprobs = unset
```

Verification status remains:

```text
pending
passed
failed
superseded
```

Do not mark `passed` until the source commit is merged into `main`, the target is actually configured from that source and regression checks pass.

## 15. Regression additions for GPT-6 Astra

In addition to the existing Flowise regression suite, verify:

1. Astra tool calls are executed through Responses API.
2. No configuration sends `reasoning.effort=none`.
3. No configuration sends custom `temperature`, `top_p` or `logprobs`.
4. Lead default reasoning is `high` and Worker default is `medium`, or an explicitly recorded approved override exists.
5. Native Structured Outputs are used when the installed integration supports them.
6. Async/concurrent tool execution is restricted to independent read-only work.
7. Mutation ordering and ownership invariants remain unchanged.
8. A simulated interrupted execution is reconciled before mutation retry.
9. Mid-turn steering cannot silently widen scope or authority.
10. Browser and role contexts remain free of unnecessary model-specific duplication.

## 16. Official OpenAI references used for this profile

Verified on 2026-09-06 against current OpenAI documentation:

- GPT-6 Astra model guide: `https://developers.openai.com/api/docs/guides/latest-model`
- GPT-6 Astra model page: `https://developers.openai.com/api/docs/models/gpt-6-astra`
- OpenAI release notes: `https://openai.com/products/release-notes/`
- GPT-6 Astra launch page: `https://openai.com/index/gpt-6-astra/`

Re-verify these capabilities before a future model migration or when the installed API/Flowise behavior differs from this profile.
