# RB-REC-01B PostgreSQL schema evidence v1

- **Evidence ID:** `RB-REC-01B_POSTGRES_SCHEMA_EVIDENCE_v1`
- **Lane:** `route_b_n8n`
- **Execution mode:** `browser_chatgpt_github_direct`
- **Produced by:** `SUBCHAT_DATA_STEWARD`
- **Evidence status:** `targeted_gap_required`
- **Source capture timestamp (UTC):** `2026-06-22T14:58:02.814315Z`
- **Supplied column-output SHA-256:** `37865b09056a3fe52c24fa6a5cede91f785ef79e21a704a595b079040d1617f5`
- **Scope:** exactly three approved PostgreSQL relations
- **Evidence class:** read-only applied-state catalog evidence
- **Authority boundary:** evidence only; no DDL, DML, migration, n8n publication, runtime smoke, or server apply

## Source identity

| Field | Captured value |
|---|---|
| Current database | `moex_n8n` |
| PostgreSQL version | `16.14 (Ubuntu 16.14-0ubuntu0.24.04.1)` |
| Full version | `PostgreSQL 16.14 (Ubuntu 16.14-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit` |
| Session user | `postgres` |
| Current user | `postgres` |
| Transaction read-only | `on` |
| Transaction isolation | `repeatable read` |
| Server address | `local_socket` |
| Server port | `unknown` |
| Data directory | `/var/lib/postgresql/16/main` |

## Relation identity

| Requested relation | Status | Exact resolution | Kind | Owner | OID |
|---|---|---|---|---|---|
| `public.moex_n8n_workflow_run_events` | `present` | `moex_n8n_workflow_run_events` | `ordinary_table` (`r`) | `moex_n8n_user` | `16404` |
| `public.moex_n8n_workflow_runs` | `present` | `moex_n8n_workflow_runs` | `ordinary_table` (`r`) | `moex_n8n_user` | `16391` |
| `public.moex_n8n_workflow_step_runs` | `present` | `moex_n8n_workflow_step_runs` | `ordinary_table` (`r`) | `moex_n8n_user` | `16412` |

## Column evidence coverage

| Relation | Classification | Captured coverage | Targeted gap |
|---|---|---|---|
| `public.moex_n8n_workflow_run_events` | `unknown` | No column records are present in the supplied artifact | Capture all column metadata |
| `public.moex_n8n_workflow_runs` | `partial` | Ordinal positions `7..32` | Capture ordinal positions `1..6` |
| `public.moex_n8n_workflow_step_runs` | `present` for columns | Ordinal positions `1..11` | None for the column category |

The supplied artifact begins with `public.moex_n8n_workflow_runs` ordinal position `7` and then continues from psql record marker `15` through marker `50`. Records `1..13` are absent. Missing metadata is not inferred.

## `public.moex_n8n_workflow_run_events`

### Columns

**Status:** `unknown` — column query output not supplied.

## `public.moex_n8n_workflow_runs`

### Columns — partial capture (`7..32`)

ordinal | column | postgres_type | pg_type_identity | pg_type_oid | underlying_type | underlying_identity | underlying_oid | column_not_null | type_not_null | nullable | default | identity | generated | collation
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---
`7` | `target_branch_name` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`8` | `pr_ref` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`9` | `pr_number` | `integer` | `pg_catalog.int4` | `23` | `integer` | `pg_catalog.int4` | `23` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `NULL`
`10` | `final_pr_head_sha` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`11` | `ci_status` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`12` | `validation_verdict` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`13` | `durable_evidence_ref` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`14` | `pre_ci_branch_artifact_ref` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`15` | `final_pm_l2_evidence_ref` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`16` | `fallback_route` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`17` | `error_message` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`18` | `locked_by` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`19` | `worker_execution_id` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`20` | `claim_token` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`21` | `lock_expires_at` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `NULL`
`22` | `last_heartbeat_at` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `NULL`
`23` | `heartbeat_count` | `integer` | `pg_catalog.int4` | `23` | `integer` | `pg_catalog.int4` | `23` | `t` | `f` | `NO` | `0` | `NO` | `NEVER` | `NULL`
`24` | `retry_count` | `integer` | `pg_catalog.int4` | `23` | `integer` | `pg_catalog.int4` | `23` | `t` | `f` | `NO` | `0` | `NO` | `NEVER` | `NULL`
`25` | `max_retries` | `integer` | `pg_catalog.int4` | `23` | `integer` | `pg_catalog.int4` | `23` | `t` | `f` | `NO` | `3` | `NO` | `NEVER` | `NULL`
`26` | `next_retry_at` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `NULL`
`27` | `last_completed_step` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`28` | `github_branch_name` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`29` | `github_commit_sha` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`30` | `github_pr_url` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`31` | `updated_at` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `t` | `f` | `NO` | `NULL` | `NO` | `NEVER` | `NULL`
`32` | `created_at` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `t` | `f` | `NO` | `NULL` | `NO` | `NEVER` | `NULL`

## `public.moex_n8n_workflow_step_runs`

### Columns — captured (`1..11`)

ordinal | column | postgres_type | pg_type_identity | pg_type_oid | underlying_type | underlying_identity | underlying_oid | column_not_null | type_not_null | nullable | default | identity | generated | collation
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---
`1` | `workflow_run_id` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `t` | `f` | `NO` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`2` | `step_name` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `t` | `f` | `NO` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`3` | `status` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `t` | `f` | `NO` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`4` | `attempt` | `integer` | `pg_catalog.int4` | `23` | `integer` | `pg_catalog.int4` | `23` | `t` | `f` | `NO` | `1` | `NO` | `NEVER` | `NULL`
`5` | `input_hash` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`6` | `output_hash` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`7` | `started_at` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `NULL`
`8` | `completed_at` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `NULL`
`9` | `error_message` | `text` | `pg_catalog.text` | `25` | `text` | `pg_catalog.text` | `25` | `f` | `f` | `YES` | `NULL` | `NO` | `NEVER` | `pg_catalog."default"`
`10` | `created_at` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `t` | `f` | `NO` | `now()` | `NO` | `NEVER` | `NULL`
`11` | `updated_at` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `timestamp without time zone` | `pg_catalog."timestamp"` | `1114` | `t` | `f` | `NO` | `now()` | `NO` | `NEVER` | `NULL`

## Required categories not yet captured

For every target relation, the following categories remain `unknown` because their read-only query output has not been supplied:

- primary-key, foreign-key, unique, and check constraints, including exact definitions, deferrability, and validation state;
- indexes, including exact definitions, uniqueness, primary status, validity, readiness, predicates, and expressions;
- triggers, including enabled state, timing/events, exact definition, and invoked function;
- effective n8n DB-role identity;
- schema, table, and relevant sequence privileges;
- inherited role-membership basis;
- row-level-security status and policies.

## Classification

- **Dataset contract status:** `targeted_gap_required`
- **Gate 1:** `open`
- **Empty result observed:** `false`
- **Failed query observed:** `false`
- **Missing output is classified as:** `unexecuted_or_unavailable`, not `absent`

## Expiry conditions

This evidence expires upon any of the following:

- DDL affecting any target relation;
- a relevant index, trigger, constraint, ownership, ACL, RLS-policy, sequence-privilege, or role-membership change;
- change of the inspected database environment;
- a superseding schema capture;
- approved-scope change;
- inability to tie a summarized finding to the source read-only output.

## Next targeted evidence step

Capture only the missing read-only PostgreSQL catalog evidence: omitted columns, constraints, indexes, triggers, n8n-role privileges and role membership, sequences, and RLS/policies.
