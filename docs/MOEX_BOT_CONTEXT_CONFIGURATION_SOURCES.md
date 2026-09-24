# MOEX Bot Context Configuration Sources

status: proposed_source
version: 2.2
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
management_canon_version: 2.0
repository: `Viktoryyyyy/moex-robot`

## 1. Purpose

This document is the canonical registry of management-context, Agent-prompt, model-execution-profile, configuration-guidance and historical-boundary source files for MOEX Bot.

It defines:

- which GitHub file governs each Browser, ChatGPT or Flowise configuration target;
- the precedence between management sources;
- the boundary between GitHub Source of Truth and Applied State;
- the role of model-specific configuration guidance that does not itself grant authority;
- the classification of historical Route B sources;
- the evidence required before an applied configuration may be treated as current;
- the coordinated package review, update and regression procedure.

This registry does not itself prove that any Browser, ChatGPT or Flowise setting has been applied.

## 2. Source of Truth and Applied State

```text
GitHub repository = Source of Truth
Browser / ChatGPT Project and role settings = Applied State
Flowise Agent settings = Applied State
Server filesystem = Applied State only
```

Canonical repository:

```text
Viktoryyyyy/moex-robot
```

A source-file change, commit, branch or merged PR is not proof that the corresponding Browser, ChatGPT or Flowise setting was updated.

An applied setting is current only when its recorded source commit is merged into `main`, the target has been updated from that exact source and the target-specific verification has passed.

## 3. Authority precedence

```text
Owner decision
-> docs/MOEX_BOT_MANAGEMENT_CANON.md
-> approved canon amendments
-> route or role governing document
-> target-specific prompt/context source
-> model/configuration guidance
-> applied Browser / ChatGPT / Flowise setting
-> dynamic task request
```

A lower-level source, model profile, applied setting or task request must not silently:

- replace a higher-level rule;
- widen scope or authority;
- restore a deprecated execution route;
- reintroduce legacy mandatory input or output schemas;
- treat Applied State as architectural proof;
- change merge or server-apply authority because a newer model is more capable.

If a target-specific source conflicts with the active management canon, the management canon prevails.

Configuration guidance and model profiles explain how to apply accepted sources safely. They do not replace the management canon, route contract, role prompt or task-specific authority.

Historical sources preserve prior evidence only. They do not participate in the active authority chain.

## 4. Canonical management sources

| Purpose | Canonical source | Target or consumer |
|---|---|---|
| Unified management authority | `docs/MOEX_BOT_MANAGEMENT_CANON.md` | All MOEX Bot management roles and routes |
| PM L2 task formulation and handoff | `docs/PM_L2_HANDOFF_PROMPT.md` | PM L2 Browser role and Flowise task authoring |
| Flowise route lifecycle and contracts | `docs/FLOWISE_GITHUB_ORCHESTRATION.md` | Flowise route configuration and operating review |
| Machine-readable project marker clarification | `docs/MOEX_BOT_MANAGEMENT_CANON_AMENDMENT_1_STRUCTURED_OUTPUT.md` | JSON-producing roles and Agents where applicable |

The structured-output amendment clarifies placement of the project marker. It does not restore a universal large output schema.

## 5. Canonical applied-configuration sources

| Applied target | Canonical source file | Required source version after adoption |
|---|---|---|
| ChatGPT Project context for MOEX Bot | `docs/BROWSER_PROJECT_CONTEXT.md` | 2.0 |
| Browser / ChatGPT role chats | `docs/BROWSER_ROLE_CONTEXTS.md` | 2.0 |
| Flowise Lead Agent `github-change-orchestrator` | `docs/FLOWISE_LEAD_AGENT_PROMPT.md` | 2.0 |
| Flowise Worker Agent `github-worker` | `docs/FLOWISE_GITHUB_WORKER_PROMPT.md` | 2.0 |

These prompt/context sources remain model-agnostic. Do not add model-specific API settings to them merely because the applied model changes.

Do not copy the complete management canon into every target. Each applied target uses its own canonical source file and references the higher-level governing documents.

## 6. Canonical configuration guidance and model profile

| Classification | Source | Role |
|---|---|---|
| Canonical Flowise configuration guidance | `docs/FLOWISE_AGENT_CONFIGURATION_GUIDE.md` | Recommended Flowise Applied State, tool inventory, memory, loop, output, GPT-6 Astra application and regression configuration |
| Canonical model-specific execution profile | `docs/GPT6_ASTRA_EXECUTION_PROFILE.md` | GPT-6 Astra API, reasoning, Structured Outputs, tool-calling, async/steering and migration constraints |
| Deprecated historical support | `docs/sot/context/README.md` | Explicit historical classification and non-reactivation boundary for the former Route B context package |

Required versions after adoption:

```text
FLOWISE_AGENT_CONFIGURATION_GUIDE = 2.0
GPT6_ASTRA_EXECUTION_PROFILE = 1.0
```

Rules:

- `docs/FLOWISE_AGENT_CONFIGURATION_GUIDE.md` guides application of accepted Lead and Worker prompt sources but does not create independent management, mutation, merge or server authority.
- `docs/GPT6_ASTRA_EXECUTION_PROFILE.md` contains model-dependent settings only and must not widen task or repository authority.
- Exact Agent prompt text remains governed by `docs/FLOWISE_LEAD_AGENT_PROMPT.md` and `docs/FLOWISE_GITHUB_WORKER_PROMPT.md`.
- Exact Browser / ChatGPT Project and role behavior remains governed by `docs/BROWSER_PROJECT_CONTEXT.md` and `docs/BROWSER_ROLE_CONTEXTS.md`.
- `docs/sot/context/README.md` must remain `deprecated_historical` and must not be loaded into active Browser, ChatGPT or Flowise contexts.
- Historical Route B files may be inspected read-only only when a task explicitly requires historical analysis or reconciliation.

## 7. GPT-6 Astra configuration boundary

Preferred model-specific Applied State after adoption:

```text
provider = OpenAI
model = gpt-6-astra
api = responses
Lead reasoning.effort = high
Worker reasoning.effort = medium
reasoning.effort = none -> forbidden
custom temperature = unset
custom top_p = unset
logprobs = unset
persistent cross-task memory = disabled
```

Native Structured Outputs are preferred for machine-readable result contracts when the installed integration supports them without changing the authority architecture.

Prompt-enforced JSON is a compatibility fallback only and must be recorded as such.

Async/concurrent tool execution may be enabled only for independent read-only operations. Repository mutation remains ordered and subject to the existing ownership and exact-head rules.

Mid-turn steering may narrow or clarify work but must not silently widen mutation scope, alter branch/PR ownership, authorize merge/server apply or restore Route B / n8n.

If the installed Flowise/OpenAI integration cannot provide required GPT-6 Astra tool calling through the Responses API, record the migration as `blocked` or `partial`. Do not silently substitute another model or Chat Completions tool behavior and label it as Astra Applied State.

## 8. Coordinated package consistency

The following eleven sources form the coordinated management/configuration package:

```text
docs/MOEX_BOT_MANAGEMENT_CANON.md
docs/PM_L2_HANDOFF_PROMPT.md
docs/FLOWISE_GITHUB_ORCHESTRATION.md
docs/FLOWISE_LEAD_AGENT_PROMPT.md
docs/FLOWISE_GITHUB_WORKER_PROMPT.md
docs/BROWSER_PROJECT_CONTEXT.md
docs/BROWSER_ROLE_CONTEXTS.md
docs/MOEX_BOT_CONTEXT_CONFIGURATION_SOURCES.md
docs/FLOWISE_AGENT_CONFIGURATION_GUIDE.md
docs/GPT6_ASTRA_EXECUTION_PROFILE.md
docs/sot/context/README.md
```

The first eight files are active management, role, route or applied-configuration sources after adoption.

`docs/FLOWISE_AGENT_CONFIGURATION_GUIDE.md` and `docs/GPT6_ASTRA_EXECUTION_PROFILE.md` are configuration guidance and do not independently grant authority.

`docs/sot/context/README.md` is a historical-boundary source and not active execution authority.

Package consistency requires:

- the same active execution routes;
- the same project identity and Source of Truth rules;
- the same five primary Actions;
- the same minimal dynamic task model;
- the same result-first principle;
- compatible Lead and Worker authority boundaries;
- compatible review, exact-head CI, merge and server-apply gates;
- model/configuration guidance that does not override prompt or canon authority;
- GPT-6 Astra settings compatible with its current API contract;
- async/concurrent execution restricted so mutation ownership is preserved;
- Route B classified as `deprecated_historical` with no new task or runtime authority;
- no deprecated underscore server paths;
- no requirement to pass GitHub facts that the receiving role can safely recover.

## 9. Dynamic request boundary

Applied contexts and Agent prompts contain static rules.

A normal dynamic task request contains only:

```text
PROJECT=MOEX_Bot
Action: <analyze|change|validate|merge|server_apply>
Task ID: <stable_task_id>
Task: <required substantive result>
Done when:
- <testable completion criterion>
```

Add only when material:

```text
Target
Scope
Constraints
Authority
Merge mode
```

Do not treat `execution_id`, `attempt_no`, current main SHA, working branch, PR, exact head SHA, changed files, workflow run ID, review status, model sampling settings or the complete output schema as mandatory caller input when those facts are recoverable or are execution configuration rather than task data.

## 10. Applied-state record

For every applied Browser / ChatGPT or Flowise configuration, record:

```text
target
source_file
source_version
source_commit_sha
source_main_sha
applied_at
applied_by
verification_method
verification_status
```

For Flowise Lead and Worker also record when available:

```text
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
```

Expected GPT-6 Astra compatibility values:

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

`source_commit_sha` identifies the exact commit containing the applied source text.

`source_main_sha` confirms that the applied source belongs to an accepted merged state of `main`.

Do not record `passed` when the source exists only in an unmerged branch or when the applied target has not been inspected after update.

## 11. Update and adoption procedure

1. Update canonical source files in one task-specific branch.
2. Keep all related corrections in the same branch and PR.
3. Review the exact eleven-file package inventory and current diff.
4. Validate cross-document consistency against the management canon.
5. Confirm that the configuration guide and GPT-6 profile remain guidance and do not widen authority.
6. Confirm that Browser/role/Lead/Worker prompt sources remain model-agnostic unless a real prompt defect requires a separate change.
7. Confirm that the Route B README remains `deprecated_historical` and cannot authorize new work.
8. Validate CI on the exact latest PR head SHA.
9. Resolve all blocking review findings on the current head.
10. Merge only with explicit authority.
11. Apply the merged source text/settings to each Browser / ChatGPT or Flowise target.
12. Record source version, source commit SHA and merged `main` SHA.
13. Run target-specific regression checks.
14. Mark verification `passed` only after the applied target matches the merged source and regression checks pass.

## 12. Target verification

### Browser / ChatGPT Project context

Verify that the applied Project Context:

- starts with the MOEX Bot project identity;
- uses GitHub as Source of Truth;
- contains only canonical server paths;
- lists only Browser and Flowise as active routes;
- uses the five Actions and minimal dynamic contract;
- does not require legacy universal fields;
- does not duplicate model-specific API configuration unnecessarily.

### Browser / ChatGPT role contexts

Verify that each applied role:

- stays within its authority boundary;
- returns the requested substantive result first;
- recovers current GitHub facts when appropriate;
- preserves same-branch and same-PR correction;
- does not claim `COMPLETED` without the requested deliverable;
- remains valid independent of the selected model unless the model-specific profile explicitly applies to execution settings.

### Flowise Lead Agent

Verify through regression tasks that Lead:

- accepts the minimal dynamic request;
- classifies the Action correctly;
- performs read-only work without Worker mutation;
- calls `github_worker` for repository file mutation;
- independently verifies Worker claims;
- performs current-head review and exact-head CI checks when applicable;
- returns a compact result-first response;
- does not merge without exact authority;
- uses the recorded GPT-6 Astra model/API/reasoning settings after adoption;
- does not send unsupported Astra sampling parameters.

### Flowise Worker Agent

Verify through delegated mutation and correction tasks that Worker:

- accepts a delegated `Action: change` contract;
- modifies only approved scope;
- reuses the authorized branch and PR;
- returns the implementation result before metadata;
- reports actual validation and changed files;
- never merges, applies to the server, widens scope or claims final acceptance;
- uses the recorded GPT-6 Astra model/API/reasoning settings after adoption;
- does not send unsupported Astra sampling parameters.

### GPT-6 Astra execution profile

Verify that the applied model configuration:

- uses exact model `gpt-6-astra`;
- uses Responses API for tool calling;
- never sends `reasoning.effort=none`;
- leaves custom `temperature`, `top_p` and `logprobs` unset;
- uses Lead default reasoning `high` and Worker default `medium`, or records an explicitly approved override;
- prefers native Structured Outputs where supported;
- restricts async/concurrent calls to independent read-only work;
- preserves ordered mutation and exact-head reconciliation;
- does not treat mid-turn steering as new authority;
- reconciles GitHub before retrying after an interrupted execution.

### Flowise configuration guidance

Verify that the guide:

- references the accepted Canon, orchestration, Lead, Worker and GPT-6 profile source versions;
- keeps Agent prompt files as the exact prompt Source of Truth;
- does not give Lead a direct repository mutation path;
- does not give Worker merge or server tools;
- disables persistent cross-task memory;
- uses bounded correction and CI polling loops;
- does not contain unsupported Astra generation parameters;
- does not contain empty optional result examples presented as required output;
- requires regression verification before Applied State is marked passed.

### Historical Route B boundary

Verify that `docs/sot/context/README.md`:

- is classified as `deprecated_historical`;
- states `new_tasks_allowed: false`;
- states `new_runtime_execution_allowed: false`;
- points to the active configuration registry rather than defining a partial competing authority list;
- does not authorize Route B fallback, context resolution, branches, PRs or runtime execution.

## 13. Drift control

Configuration drift exists when:

- an applied target differs materially from its canonical source;
- the applied target references an older source version or commit;
- Lead and Worker prompts implement incompatible contracts;
- the configuration guide conflicts with accepted prompt, route or model-profile contracts;
- the recorded Astra model is not actually applied;
- Astra tool calling uses an unrecorded or unsupported API path;
- legacy custom `temperature`, `top_p`, `logprobs` or `reasoning=none` remain configured;
- async/concurrent tooling allows overlapping mutation;
- a target restores deprecated Route B behavior;
- the historical Route B README presents a partial authority list as current authority;
- a target requires the legacy overloaded task or result schema;
- an unmerged branch source was applied as if it were active `main` authority.

On detected drift:

1. mark verification `failed`;
2. identify the exact differing source and target behavior;
3. do not silently edit the applied target;
4. correct the canonical GitHub source first when the source itself is wrong;
5. reapply from the accepted merged source;
6. rerun regression verification.

## 14. Security

Do not store in canonical sources, applied prompts, model settings, screenshots, logs or regression reports:

- credentials;
- API keys;
- access tokens;
- passwords;
- private runtime secrets;
- token values returned by tools.

Record configuration identity and verification evidence, not secret values.

## 15. Deprecated context and obsolete model guidance

Do not use as active configuration sources:

- Route B / n8n Universal Role Runner prompts or contracts;
- `docs/sot/context/README.md` or its child registry as active context resolution;
- deprecated underscore server paths;
- old `Контекст.md (1)`;
- server filesystem assumptions;
- stale task values copied from previous chats;
- legacy universal input contracts;
- legacy large output schemas with empty technical fields;
- historical model-selection reports that recommend older providers/models as current Applied State;
- old OpenAI generation settings that require custom temperature/top_p for GPT-6 Astra.

Historical files may remain as evidence but must be clearly classified as deprecated, historical or superseded guidance and must not authorize new work.

## 16. Adoption state

Until the coordinated source package is reviewed, merged and applied, the current configuration in `main` and the currently verified Browser / ChatGPT and Flowise settings remain the active state.

A branch status of `proposed_source` or `approved_pending_merge` means the source is proposed and internally aligned, not yet active authority.
