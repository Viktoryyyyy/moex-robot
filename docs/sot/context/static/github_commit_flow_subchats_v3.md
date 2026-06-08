# GitHub Commit Flow for Sub-chats v3

context_ref: github_commit_flow_subchats_v3
status: active_static_context_ref
source_level: project_git_operating_canon

## Core rules

- Inspect GitHub first.
- Do not use server for primary authoring.
- New files may be created through GitHub contents API.
- Existing files must not be overwritten blindly.
- Apply proof requires branch/ref state verification, not only blob/tree creation.
- Partial batch apply is forbidden.
- Approved scope must be assembled before final apply.

## PR-first CI flow

For code/config/tests changes:

1. Start from current `origin/main`.
2. Create a feature branch.
3. Commit full approved scope to the feature branch.
4. Open PR to `main`.
5. Verify GitHub Actions workflow `tests`.
6. Merge/update origin/main only after successful check.
7. Server apply is allowed only after origin/main proof, when required by the task.

## Server boundary

Server read-only metadata bridge is allowed only if GitHub tool metadata is insufficient for safe repo mutation. Server-first editing remains forbidden.

## Route B relevance

Route B implementation sub-chats must use this flow for repo-first context package changes and must not mutate server state for methodology artifacts.
