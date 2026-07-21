# MOEX Bot Context Configuration Sources

status: active_source
version: 1.1
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
structured_output_amendment: `docs/MOEX_BOT_MANAGEMENT_CANON_AMENDMENT_1_STRUCTURED_OUTPUT.md`
repository: `Viktoryyyyy/moex-robot`

## Purpose

This document maps the canonical source text for Browser project context, Browser role contexts and Flowise Agent settings.

The repository stores the approved source text. The ChatGPT Project settings and Flowise Agent settings are applied states and must be updated from these files without unreviewed edits.

## Canonical sources

| Target | Canonical source |
|---|---|
| ChatGPT Project context for MOEX Bot | `docs/BROWSER_PROJECT_CONTEXT.md` |
| Browser role chats | `docs/BROWSER_ROLE_CONTEXTS.md` |
| Flowise Lead Agent `github-change-orchestrator` | `docs/FLOWISE_LEAD_AGENT_PROMPT.md` |
| Flowise Worker Agent `github-worker` | `docs/FLOWISE_GITHUB_WORKER_PROMPT.md` |
| Machine-readable project marker clarification | `docs/MOEX_BOT_MANAGEMENT_CANON_AMENDMENT_1_STRUCTURED_OUTPUT.md` |

## Authority

The precedence order is:

```text
Owner decision
→ docs/MOEX_BOT_MANAGEMENT_CANON.md
→ approved canon amendments
→ target-specific source file in this index
→ applied UI or Agent setting
→ dynamic task request
```

A dynamic task request supplies task-specific scope, constraints and authority within the higher-level rules. It must not silently replace static role rules or widen authority.

## Application control

For each applied setting record:

```text
target
source_file
source_commit_sha
applied_at
applied_by
verification_status
```

Do not claim that a setting is applied merely because its source file exists in GitHub.

## Update procedure

1. Change the canonical source in a task-specific branch and PR.
2. Review exact changed files and diff.
3. Validate CI on the exact head SHA.
4. Merge after approval.
5. Apply the merged source text to the target UI or Flowise Agent.
6. Run the relevant regression pilot.
7. Record the source commit SHA used for the applied setting.

## Security

Do not store credentials, API keys, tokens, passwords or private runtime metadata in these source files.

## Deprecated context

Do not copy or reuse Route B / n8n Universal Role Runner prompts for new work.

Historical Route B files may remain as evidence but are not active configuration sources.