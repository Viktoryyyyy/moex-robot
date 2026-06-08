# Route B n8n Operating Model v1

Status: active SoT operating model
Project: MOEX Bot
Scope: Route B n8n orchestration across the current seven workflows

## 1. Purpose

Route B is the `PM L2 -> PM L3 -> Sub-chat -> PM L3 -> PM L2` orchestration route for semi-autonomous implementation cycles.

n8n orchestrates request intake, PM L3 packaging, GitHub branch/PR execution, evidence capture, result query, PM L3 return, and watchdog recovery.

GitHub/repo is Source of Truth.

Postgres is workflow state/evidence store.

n8n execution data is not Source of Truth. It is operational telemetry and must not replace GitHub commits, pull requests, CI results, or repo-level SoT documents.

## 2. Authority model

### PM L2

PM L2 owns phase scope.

PM L2 owns final review/verdict.

PM L2 owns merge approval authority.

PM L2 owns merge approval outside n8n.

### PM L3

PM L3 converts PM L2 handoff into an executable sub-chat or executor package.

PM L3 validates evidence.

PM L3 returns to PM L2.

PM L3 does not approve merge.

PM L3 does not claim PM L2 verdict.

### Sub-chat

Sub-chat implements or validates only inside approved scope.

Sub-chat returns to PM L3, not PM L2.

Sub-chat does not approve merge.

### n8n

n8n orchestrates Route B steps and persists evidence in Postgres.

n8n may create branches and PRs only through the approved GitHub Branch/PR Executor.

n8n may not merge main.

n8n may not infer approval.

n8n may not treat CI passed as PM L2 approval.

### GitHub

GitHub/repo is Source of Truth.

GitHub pull requests and GitHub Actions are PR/CI evidence sources.

### Postgres

Postgres is workflow state/evidence store.

Postgres stores workflow state, locks, evidence references, retry state, and PM L3 return payloads.

## 3. Current seven workflows

### 3.1 MOEX_ROUTE_B_INTAKE_ACK_V1_10_3

Purpose: accepts PM L2 requests and queues a Route B run in Postgres.

Endpoint: `POST /webhook/moex/route-b/intake`.

Write scope: Postgres only.

Primary credential: `postgres_moex_n8n`.

### 3.2 MOEX_ROUTE_B_WORKER_POLLER_V1_10_3

Purpose: claims a queued run, resolves GitHub SoT context, calls the PM L3 model, and persists the PM L3 package.

Schedule: polling.

Write scope: Postgres only.

External calls: GitHub read and Ollama Cloud.

Primary credentials: `postgres_moex_n8n`, `github_read_moex_bot`, `ollama_cloud_moex_route_b`.

### 3.3 MOEX_ROUTE_B_STATUS_QUERY_V1_10_3

Purpose: status-level lookup.

Endpoint: `GET /webhook/moex/route-b/status`.

Read-only.

Primary credential: `postgres_moex_n8n`.

### 3.4 MOEX_ROUTE_B_WATCHDOG_ERROR_HANDLER_V1_10_3

Purpose: recovers stale processing locks and retry state.

Schedule: watchdog polling.

Write scope: Postgres only.

Primary credential: `postgres_moex_n8n`.

### 3.5 MOEX_ROUTE_B_GITHUB_BRANCH_PR_EXECUTOR_V1_10_3

Purpose: Branch/PR Executor.

Status: accepted_candidate_for_import_test.

Allowed actions:
- may create an `n8n/...` branch;
- may commit only approved file changes to the feature branch;
- may open PR to `main`;
- may read CI evidence.

Forbidden actions:
- must not merge;
- must not direct-write main;
- must not force push;
- must not delete files.

Primary credentials: `github_branch_write_moex_bot`, `github_pr_comment_moex_bot`, `github_read_moex_bot`.

The GitHub branch write credential must only be used in Branch/PR Executor.

### 3.6 MOEX_ROUTE_B_RESULT_QUERY_V1_10_3

Purpose: Result Query v2 for PM L2 full evidence retrieval, not status-only lookup.

Endpoint: `GET /webhook/moex/route-b/result`.

Status: accepted_candidate_for_import_test.

Read-only.

Primary credential: `postgres_moex_n8n`.

### 3.7 MOEX_ROUTE_B_PM_L3_RETURN_INTAKE_V1_10_3

Purpose: PM L3 Return Intake receives validation/evidence package from PM L3 and makes it available to PM L2.

Endpoint: `POST /webhook/moex/route-b/pm-l3-return`.

Status: accepted_candidate_for_import_test.

Write scope: Postgres only.

Primary credential: `postgres_moex_n8n`.

## 4. End-to-end target flow

```text
PM L2 request
-> Intake Ack
-> Worker Poller / PM L3 package
-> optional sub-chat task
-> optional GitHub Branch/PR Executor
-> PR + GitHub Actions tests
-> PM L3 Return Intake
-> Result Query v2
-> PM L2 final verdict / merge approval outside n8n
```

## 5. Activation and testing sequence

Already accepted / production-smoke-tested:

- Intake Ack
- Status Query
- Watchdog

Needs manual import-test / smoke-test:

- Worker Poller context resolver if not already validated
- GitHub Branch/PR Executor
- Result Query v2
- PM L3 Return Intake

Recommended manual test order:

1. Result Query v2 no/known run query.
2. PM L3 Return Intake invalid payload / 404 / valid synthetic return.
3. Worker Poller PM L2 envelope smoke-test.
4. GitHub Branch/PR Executor no-job test.
5. GitHub Branch/PR Executor controlled branch/PR smoke-test.
6. Result Query v2 full evidence check.

## 6. Credentials

Expected credentials by workflow:

- `postgres_moex_n8n`: Intake Ack, Status Query, Watchdog, Result Query v2, PM L3 Return Intake, Worker Poller state writes.
- `github_read_moex_bot`: Worker Poller context resolver and Branch/PR Executor read checks.
- `github_branch_write_moex_bot`: Branch/PR Executor only.
- `github_pr_comment_moex_bot`: Branch/PR Executor PR evidence/comment updates only.
- `ollama_cloud_moex_route_b`: Worker Poller PM L3 model call only.

Credential boundaries:

- GitHub branch write credential must only be used in Branch/PR Executor.
- Merge credential is not part of current Route B.
- No secrets should be added to workflow JSON.
- Workflow JSON must reference existing credentials by configured n8n credential name only.

## 7. Postgres state model

Main statuses/states:

- `queued`
- `processing`
- `retry_wait`
- `ready_for_review`
- `github_execution_requested`
- `github_executor_pr_opened_waiting_ci`
- `github_execution_retry_wait`
- `github_executor_ci_passed_pm_l3_validation_pending`
- `github_executor_ci_failed_pm_l3_validation_pending`
- `pm_l2_review_required`
- `manual_review_required`
- `failed`

## 8. Forbidden actions

The following actions are forbidden in Route B:

- direct main write is forbidden;
- n8n merge is forbidden;
- merge to main by n8n is forbidden;
- force push is forbidden;
- file delete is forbidden;
- creating credentials/secrets is forbidden;
- runtime/live/broker/trading activation is forbidden;
- treating CI passed as merge approval is forbidden;
- CI passed is not merge approval;
- sub-chat returning directly to PM L2 is forbidden;
- PM L3 claiming PM L2 verdict is forbidden;
- `latest`, `current`, and `autodetect` refs are forbidden;
- absolute/server paths in evidence refs are forbidden.

## 9. Failure handling

- invalid request -> 400;
- missing run -> 404;
- stale processing -> Watchdog;
- CI missing/pending -> evidence gap / waiting state;
- CI failed -> PM L3 validation / PM L2 review;
- contract violation -> fail closed / manual review;
- branch collision -> fail closed unless same request/base evidence exists.

## 10. Production readiness boundary

The existing seven workflows are not all production-active until smoke-tests are completed.

Import-test candidates must remain inactive until manual tests pass.

PM L2 merge approval remains outside n8n.

This document does not approve runtime/live trading, broker integration, strategy promotion, Postgres DDL changes, credentials/secrets changes, n8n workflow JSON edits, or merge automation.
