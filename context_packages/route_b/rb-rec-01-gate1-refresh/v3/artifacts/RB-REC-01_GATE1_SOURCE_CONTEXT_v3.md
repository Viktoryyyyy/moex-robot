# RB-REC-01 Gate 1 Source Context v3

```yaml
context_evidence_id: RB-REC-01-GATE1-SOURCE-CONTEXT-v3
lane: route_b_n8n
produced_by: PM_L2_PHASE_OWNER
produced_at: 2026-06-23T16:13:02+05:00
context_publication_only: true

root_task:
  Bring the Route B / n8n lane back under management control and complete it according to the active completion plan.

current_phase:
  status: gate_1_open_pending_targeted_github_server_n8n_freshness_refresh
  credentialed_runtime: frozen
  implementation_authorized: false
  database_mutation_authorized: false
  n8n_publication_authorized: false
  activation_authorized: false
  runtime_smoke_authorized: false
  server_apply_authorized: false

source_state:
  repository_full_name: Viktoryyyyy/moex-robot
  prior_rb_rec_01a_main_sha: cd0c2e9e494e582692cfeb61a0e47264ea42bb48
  current_github_main_sha: 613cb593445e452d9a81918721d9292c876fd45d
  current_main_reason: merged PR #217
  canonical_workflow_path: docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_CREDENTIALED_RUNTIME_V0_1_TARGET.json
  canonical_workflow_blob_sha_at_current_main: e2ff4c078ec4bcaa69d5691e02b4c1d68a453e75
  canonical_workflow_changed_by_pr_217: false

rb_rec_01a_prior_baseline:
  evidence_id: RB-REC-01A-APPLIED-STATE-BASELINE-v2
  produced_by: SUBCHAT_REPO_AUDIT
  captured_at: 2026-06-22T16:59:12+05:00
  status: expired
  expiry_reason: GitHub main changed from cd0c2e9e494e582692cfeb61a0e47264ea42bb48 to 613cb593445e452d9a81918721d9292c876fd45d.
  prior_findings:
    github_server_status: exact_match_at_prior_capture
    server_worktree: clean_at_prior_capture
    n8n_workflow_id: Kldo3tG9fbZAXk7e
    n8n_version_id: 7113e23f-1992-4ace-9e34-efd1b1685a54
    n8n_active_state: inactive_at_prior_capture
    n8n_publication_state: operator_reported_not_published_unverified
    github_to_n8n_status: confirmed_substantive_drift
    drift_summary:
      - step checkpoint SQL used NULL placeholders instead of the GitHub table lookup
      - accepted, blocked and final event INSERT SQL differed from GitHub and added new_state
      - switch serialization and workflow settings differed
      - credential references differed; manual attribution was unproven
      - canonical governance fields were absent from n8n export
  prior_hashes:
    github_workflow_raw_sha256: 0aaaace52fa92db0b9ac3a3fd9d1e72e157605a0a204bd40248d5178efc2f57c
    n8n_export_raw_sha256: 4f29a99f87b9daa2d7846693df679862aade4500ff8dfa32360e9a5defbc9138

rb_rec_01b_db_schema_evidence:
  evidence_ids:
    - RB-REC-01B-DB-SCHEMA-BASELINE
    - RB-REC-01B-DB-SCHEMA-BASELINE-GAP-01
  produced_by: SUBCHAT_DATA_STEWARD
  database_name: moex_n8n
  server_version: PostgreSQL 16.14
  n8n_role: moex_n8n_user
  status: complete_for_requested_metadata_scope
  reusable_without_repeat_db_audit: true
  tables:
    public.moex_n8n_workflow_run_events:
      owner: moex_n8n_user
      columns:
        - event_id bigint not null default sequence
        - workflow_run_id text not null
        - previous_state text nullable
        - new_state text not null
        - event_type text not null
        - event_payload_json jsonb nullable
        - created_at timestamp without time zone not null without default
      constraints:
        primary_key: event_id
        foreign_key: none
        unique: none
        check: none
      indexes:
        - primary key on event_id
        - idx_moex_events_run_created on workflow_run_id, created_at
      triggers: none
      effective_privileges_for_moex_n8n_user: SELECT, INSERT, UPDATE, DELETE
      sequence_privileges: USAGE, SELECT
    public.moex_n8n_workflow_runs:
      owner: moex_n8n_user
      ordered_columns:
        - workflow_run_id text not null
        - idempotency_key text nullable
        - status text not null
        - current_state text not null
        - repository_full_name text nullable
        - current_phase text nullable
        - target_branch_name text nullable
        - pr_ref text nullable
        - pr_number integer nullable
        - final_pr_head_sha text nullable
        - ci_status text nullable
        - validation_verdict text nullable
        - durable_evidence_ref text nullable
        - pre_ci_branch_artifact_ref text nullable
        - final_pm_l2_evidence_ref text nullable
        - fallback_route text nullable
        - error_message text nullable
        - locked_by text nullable
        - worker_execution_id text nullable
        - claim_token text nullable
        - lock_expires_at timestamp without time zone nullable
        - last_heartbeat_at timestamp without time zone nullable
        - heartbeat_count integer not null default 0
        - retry_count integer not null default 0
        - max_retries integer not null default 3
        - next_retry_at timestamp without time zone nullable
        - last_completed_step text nullable
        - github_branch_name text nullable
        - github_commit_sha text nullable
        - github_pr_url text nullable
        - updated_at timestamp without time zone not null without default
        - created_at timestamp without time zone not null without default
      constraints:
        primary_key: workflow_run_id
        unique: idempotency_key
        foreign_key: none
        check: none
      indexes:
        - primary key on workflow_run_id
        - unique index on idempotency_key
        - idx_moex_runs_heartbeat on status, last_heartbeat_at
        - idx_moex_runs_lock_expiry on status, lock_expires_at
        - idx_moex_runs_queue on status, next_retry_at, created_at
      triggers: none
      effective_privileges_for_moex_n8n_user: SELECT, INSERT, UPDATE, DELETE
    public.moex_n8n_workflow_step_runs:
      owner: moex_n8n_user
      ordered_columns:
        - workflow_run_id text not null
        - step_name text not null
        - status text not null
        - attempt integer not null default 1
        - input_hash text nullable
        - output_hash text nullable
        - started_at timestamp without time zone nullable
        - completed_at timestamp without time zone nullable
        - error_message text nullable
        - created_at timestamp without time zone not null default now()
        - updated_at timestamp without time zone not null default now()
      constraints:
        primary_key: workflow_run_id, step_name
        foreign_key: none
        unique: none
        check: none
      indexes:
        - primary key on workflow_run_id, step_name
        - idx_moex_steps_run_status on workflow_run_id, status
      triggers: none
      effective_privileges_for_moex_n8n_user: SELECT, INSERT, UPDATE, DELETE
  workflow_assumption_matrix:
    current_state_json: confirmed_absent
    current_state: confirmed_supported_text_not_null
    input_json: confirmed_absent
    output_json: confirmed_absent
    previous_state: confirmed_supported_text_nullable
    new_state: confirmed_supported_text_not_null
    event_payload_json: confirmed_supported_jsonb_nullable
    run_events_created_at: confirmed_supported_not_null_no_default
    runs_created_at_updated_at: confirmed_supported_not_null_no_default
    step_runs_created_at_updated_at: confirmed_supported_not_null_default_now
    step_name: confirmed_supported
    status: confirmed_supported
    attempt: confirmed_supported_integer_not_null_default_1
  confirmed_incompatibilities:
    - current_state_json is absent; actual run projection field is current_state text
    - input_json and output_json are absent; actual checkpoint fields are hashes and lifecycle columns
    - run_events INSERT must supply new_state and created_at
    - workflow_runs created_at and updated_at have no defaults
    - no FK constraints link events or steps to workflow_runs

freshness_delta:
  pr_217:
    merged: true
    merge_commit_sha: 613cb593445e452d9a81918721d9292c876fd45d
    changed_files:
      - docs/sot/RB-REC-01B_POSTGRES_SCHEMA_EVIDENCE_v1.md
    canonical_workflow_changed: false
    server_apply_status_from_pr_body: not_done
  current_server_head: unknown_requires_read_only_refresh
  current_server_worktree: unknown_requires_read_only_refresh
  current_n8n_version_and_hash: unknown_requires_read_only_refresh
  db_schema_refresh_required: false

binding_decisions:
  - RB-REC-01A v2 must not be reused to close Gate 1 because it expired.
  - RB-REC-01B plus GAP-01 remains reusable; do not repeat PostgreSQL inspection.
  - Next analysis owner is SUBCHAT_REPO_AUDIT.
  - Next task is RB-REC-01A-FRESHNESS-REFRESH.
  - The task scope is only current GitHub/server/n8n freshness and comparisons.
  - Credentialed runtime remains frozen.
  - No implementation or applied-state mutation authority is granted.

required_next_output_from_pm_l3:
  - validate this immutable source package
  - publish one role-specific task context package for SUBCHAT_REPO_AUDIT
  - return exact task-context repository, commit SHA and manifest path

expiry_condition:
  - GitHub main changes from 613cb593445e452d9a81918721d9292c876fd45d
  - canonical workflow blob changes from e2ff4c078ec4bcaa69d5691e02b4c1d68a453e75
  - approved task scope changes
  - a newer server or n8n snapshot supersedes this routing context
  - PM L2 explicitly supersedes this package
```
