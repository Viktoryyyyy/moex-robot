# Current Flowise workflow for GitHub work

This document is the authoritative, current description of the Flowise workflow used to run GitHub repository tasks from ChatGPT.

Historical Flowise experiments, webhook sketches, Route B executor/poller drafts, old endpoints, and other agent-orchestration variants in archived design artifacts are not the active workflow unless they explicitly match the flow below.

## Entry point

1. ChatGPT calls Flowise through a GPT Action.
2. The GPT Action sends the request to the current Flowise prediction endpoint:

```http
POST /api/v1/prediction/33b1fd8e-e97f-4f23-a1bb-697d8863e594
```

No other Flowise endpoint or webhook URL is documented as the working GitHub execution path.

## Main Flowise flow

The main Flowise flow is named `github-change-orchestrator`.

Pipeline:

```text
Chat Input → Lead Agent → Direct Reply
```

The Lead Agent has access to:

- Github MCP for repository, branch, commit, file, and Pull Request reads;
- a separate Github MCP for merge operations;
- Chatflow Tool for calling the worker flow `github-worker`.

## Worker Flowise flow

The worker Flowise flow is named `github-worker`.

Pipeline:

```text
Chat Input → Worker Agent → Direct Reply
```

The Worker Agent executes one concrete delegated subtask at a time, such as:

- repository analysis;
- documentation or file edits within the allowed scope;
- adding tests when the task explicitly requires tests;
- checking the resulting diff.

The Worker Agent does not merge Pull Requests.

## Lead Agent responsibilities

The Lead Agent owns orchestration and final GitHub control:

1. validates required input fields;
2. decomposes the request into concrete subtasks;
3. calls `github-worker` for implementation subtasks;
4. creates or updates the working branch `ai/<Task ID>`;
5. creates or updates the Pull Request into the target branch;
6. checks GitHub checks for the Pull Request;
7. merges only when `Merge mode` is `automatic` and checks are successful;
8. does not merge when `Merge mode` is `manual`.

## Input format

Requests sent from ChatGPT to Flowise use this text format:

```text
Action: <execute|resume>
Task ID: <stable task id>
Repository: <owner>/<repo>
Target branch: <branch>
Task: <task description>
Acceptance criteria:
- <criterion 1>
- <criterion 2>
Merge mode: <manual|automatic>
```

`Acceptance criteria` may be omitted when the task does not provide it. `Task ID`, `Repository`, `Target branch`, `Task`, and `Merge mode` are required.

## Supported result statuses

The Lead Agent returns one of these statuses:

- `INVALID_INPUT` — required input is missing.
- `BLOCKED` — execution cannot continue because of permissions, conflicts, constraints, or unsafe changes.
- `FAILED` — a technical error occurred.
- `COMPLETED` — the task completed without creating a Pull Request.
- `PR_CREATED` — a Pull Request was created or updated.
- `PENDING_CHECKS` — GitHub checks are still running.
- `CHECKS_FAILED` — GitHub checks completed with failures.
- `READY_FOR_MANUAL_MERGE` — checks passed and the Pull Request is ready for a human merge.
- `MERGED` — the Pull Request was merged successfully.

## Merge policy

- `Merge mode: manual` means the Lead Agent must stop after creating or updating the Pull Request and checking status; it must not merge.
- `Merge mode: automatic` allows merge only after the Pull Request checks are successful.
- The Worker Agent must never perform merge operations.
