# Route B Context Registry — Historical Package

status: deprecated_historical
project: MOEX_Bot
package: route_b_context_registry
new_tasks_allowed: false
new_runtime_execution_allowed: false
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
repository: `Viktoryyyyy/moex-robot`

## 1. Status

This directory preserves the former Route B / n8n Universal Role Runner context package as historical evidence.

It is not an active management, orchestration, task-authoring or runtime source.

Do not use this package to create or execute new:

- Route B tasks;
- n8n Universal Role Runner workflows;
- PM or sub-chat handoffs;
- branches or pull requests;
- context-resolution requests;
- server or runtime actions.

Historical files may remain unchanged when they are needed to explain prior decisions, branches, pull requests or workflow behavior.

## 2. Active authority

Current MOEX Bot management authority is defined by:

```text
docs/MOEX_BOT_MANAGEMENT_CANON.md
docs/PM_L2_HANDOFF_PROMPT.md
docs/FLOWISE_GITHUB_ORCHESTRATION.md
docs/BROWSER_PROJECT_CONTEXT.md
docs/BROWSER_ROLE_CONTEXTS.md
```

Active execution routes are limited to:

```text
browser_controlled_github_route
flowise_automated_github_route
```

Route B is not an active fallback route.

## 3. Historical contents

The preserved package may include:

- `docs/sot/context/registry.route_b.v1.yaml`;
- static context references under `docs/sot/context/static/`;
- role context references under `docs/sot/context/roles/`;
- request and return schemas under `docs/sot/context/schemas/`;
- historical descriptions of the former PM L2 → PM L3 → Sub-chat chain.

These files describe prior design intent only. Their existence in GitHub does not grant current execution authority.

## 4. Resolution prohibition

Active Browser, Flowise, PM or sub-chat workflows must not resolve dynamic task context through this Route B registry.

Do not:

- treat `registry.route_b.v1.yaml` as an active registry;
- load Route B static or role contexts into current agents;
- restore Route B request or return schemas;
- copy Route B mandatory fields into new task contracts;
- infer that Postgres or n8n state is current management Source of Truth;
- create replacement Route B branches or pull requests.

Current tasks use the minimal dynamic task contract and recover repository facts directly from GitHub when safe.

## 5. Source of Truth boundary

```text
GitHub repository = Source of Truth
Server filesystem = Applied State only
```

This historical package does not prove:

- current repository architecture;
- active branch or PR ownership;
- accepted implementation;
- current Agent configuration;
- server Applied State;
- authority to execute any workflow.

Do not use server paths or server files to reactivate this package.

## 6. Use in audits

A role may inspect this package read-only when the task explicitly requires historical analysis, migration review or reconciliation of an old Route B artifact.

Such analysis must:

- identify the package as `deprecated_historical`;
- distinguish historical statements from current authority;
- avoid mutation unless a separate approved change task exists;
- recommend current Browser or Flowise handling rather than Route B reactivation.

## 7. Supersession rule

Any open branch, pull request, issue, document or prompt that describes this package as active must be reviewed for closure, supersession or historical relabelling.

Do not merge a change that restores `active`, `active SoT package`, `new_tasks_allowed: true` or equivalent Route B authority unless the project owner explicitly changes the management canon.

## 8. Security

Historical files must not contain or expose credentials, API keys, tokens, passwords or private runtime secrets.
