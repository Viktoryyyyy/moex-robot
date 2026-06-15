-- artifact_id: route_b_universal_role_runner_db_migration_proposal.v0.1
-- artifact_class: repo_relative
-- producer: PM_L2_PHASE_OWNER
-- consumer: PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER
-- format: postgresql_sql_proposal_only
-- repo_relative_path: docs/sot/context/schemas/route_b_universal_role_runner_db_migration_proposal.v0.1.sql
-- proposal_only: true
-- executable_by_repo_package: false
-- production_migration_executed: false
-- server_apply_allowed: false
-- db_execution_allowed: false
-- n8n_mutation_allowed: false
-- runtime_live_trading_allowed: false
-- broker_execution_allowed: false
-- auth_secrets_scope_allowed: false
-- github_repo_source_of_truth: true
-- server_applied_state_only: true
-- merge_authority: PM_L2_ONLY
-- direct_main_write_allowed: false
-- force_push_allowed: false
-- file_delete_allowed: false
-- executor_merge_allowed: false

-- Route B Universal Role Runner DB migration proposal v0.1.
-- This artifact is a repository contract proposal only.
-- It must not be executed by this repository package and does not approve any production database change.

CREATE TABLE IF NOT EXISTS route_b_phase_runs (
    phase_run_id text PRIMARY KEY,
    workflow_run_id text NOT NULL,
    status text NOT NULL CHECK (status IN (
        'phase_queued',
        'pm_l3_planning',
        'role_task_ready',
        'role_task_running',
        'role_task_completed',
        'role_task_failed',
        'pm_l3_validating',
        'github_execution_requested',
        'github_executor_pr_opened_waiting_ci',
        'pm_l3_finalizing',
        'pm_l2_review_required',
        'blocked',
        'failed'
    )),
    current_state text,
    result_status_extension_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    role_tasks_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    role_outputs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    pm_l3_decisions_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    blockers_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    required_fixes_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS route_b_role_task_queue (
    role_task_id text PRIMARY KEY,
    phase_run_id text NOT NULL REFERENCES route_b_phase_runs (phase_run_id),
    workflow_run_id text NOT NULL,
    parent_role_task_id text REFERENCES route_b_role_task_queue (role_task_id),
    sequence_no integer NOT NULL CHECK (sequence_no >= 0),
    role_id text NOT NULL CHECK (role_id IN (
        'PM_L3_DELIVERY_VALIDATION_OWNER',
        'SUBCHAT_REPO_AUDIT',
        'SUBCHAT_SPEC_CONTRACT_DESIGNER',
        'SUBCHAT_IMPLEMENTATION',
        'SUBCHAT_VALIDATION',
        'SUBCHAT_DATA_STEWARD',
        'SUBCHAT_EXPERIMENT_DESIGNER',
        'SUBCHAT_RESEARCH_CRITIC',
        'SUBCHAT_RESEARCH_EXECUTION'
    )),
    task_type text NOT NULL CHECK (task_type IN (
        'pm_l3_planning',
        'subchat_execution',
        'pm_l3_validation',
        'pm_l3_finalization'
    )),
    task_payload_json jsonb NOT NULL,
    context_refs_json jsonb NOT NULL,
    expected_output_schema_ref text NOT NULL,
    status text NOT NULL DEFAULT 'role_task_ready' CHECK (status IN (
        'role_task_ready',
        'role_task_running',
        'role_task_completed',
        'role_task_failed',
        'blocked'
    )),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    max_retries integer NOT NULL DEFAULT 3 CHECK (max_retries >= 0),
    blocker_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz,
    completed_at timestamptz,
    CHECK (role_id <> ''),
    CHECK (expected_output_schema_ref <> '')
);

CREATE TABLE IF NOT EXISTS route_b_role_outputs (
    role_output_id text PRIMARY KEY,
    phase_run_id text NOT NULL REFERENCES route_b_phase_runs (phase_run_id),
    workflow_run_id text NOT NULL,
    role_task_id text NOT NULL REFERENCES route_b_role_task_queue (role_task_id),
    role_id text NOT NULL,
    output_json jsonb NOT NULL,
    validation_status text NOT NULL CHECK (validation_status IN (
        'validation_pending',
        'validation_passed',
        'validation_failed',
        'blocked'
    )),
    raw_content_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (raw_content_hash <> '')
);

CREATE TABLE IF NOT EXISTS route_b_pm_l3_decisions (
    pm_l3_decision_id text PRIMARY KEY,
    phase_run_id text NOT NULL REFERENCES route_b_phase_runs (phase_run_id),
    workflow_run_id text NOT NULL,
    role_task_id text NOT NULL REFERENCES route_b_role_task_queue (role_task_id),
    decision_type text NOT NULL CHECK (decision_type IN (
        'create_next_role_task',
        'request_github_execution_package',
        'return_blocker',
        'return_final_pm_l3_evidence_package'
    )),
    decision_payload_json jsonb NOT NULL,
    authority_boundary_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS route_b_phase_runs_workflow_run_id_uidx
    ON route_b_phase_runs (workflow_run_id);

CREATE UNIQUE INDEX IF NOT EXISTS route_b_role_task_queue_phase_sequence_uidx
    ON route_b_role_task_queue (phase_run_id, sequence_no);

CREATE INDEX IF NOT EXISTS route_b_role_task_queue_ready_poll_idx
    ON route_b_role_task_queue (status, created_at, sequence_no)
    WHERE status = 'role_task_ready';

CREATE INDEX IF NOT EXISTS route_b_role_task_queue_phase_idx
    ON route_b_role_task_queue (phase_run_id, sequence_no);

CREATE INDEX IF NOT EXISTS route_b_role_task_queue_parent_idx
    ON route_b_role_task_queue (parent_role_task_id);

CREATE INDEX IF NOT EXISTS route_b_role_outputs_phase_idx
    ON route_b_role_outputs (phase_run_id, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS route_b_role_outputs_task_hash_uidx
    ON route_b_role_outputs (role_task_id, raw_content_hash);

CREATE INDEX IF NOT EXISTS route_b_pm_l3_decisions_phase_idx
    ON route_b_pm_l3_decisions (phase_run_id, created_at);

CREATE INDEX IF NOT EXISTS route_b_pm_l3_decisions_task_idx
    ON route_b_pm_l3_decisions (role_task_id, decision_type);

CREATE VIEW route_b_result_status_extension AS
SELECT
    p.phase_run_id,
    p.workflow_run_id,
    p.status,
    p.current_state,
    jsonb_build_object(
        'phase_run', to_jsonb(p),
        'role_tasks', COALESCE(tasks.role_tasks, '[]'::jsonb),
        'role_outputs', COALESCE(outputs.role_outputs, '[]'::jsonb),
        'pm_l3_decisions', COALESCE(decisions.pm_l3_decisions, '[]'::jsonb),
        'blockers', p.blockers_json,
        'required_fixes', p.required_fixes_json,
        'authority_boundary', jsonb_build_object(
            'merge_authority', 'PM_L2_ONLY',
            'pm_l2_review_required', true,
            'n8n_merge_allowed', false,
            'direct_main_write_allowed', false,
            'force_push_allowed', false,
            'file_delete_allowed', false,
            'executor_merge_allowed', false,
            'ci_passed_is_not_merge_approval', true
        )
    ) AS result_status_extension_json
FROM route_b_phase_runs p
LEFT JOIN LATERAL (
    SELECT jsonb_agg(to_jsonb(t) ORDER BY t.sequence_no, t.created_at) AS role_tasks
    FROM route_b_role_task_queue t
    WHERE t.phase_run_id = p.phase_run_id
) tasks ON true
LEFT JOIN LATERAL (
    SELECT jsonb_agg(to_jsonb(o) ORDER BY o.created_at) AS role_outputs
    FROM route_b_role_outputs o
    WHERE o.phase_run_id = p.phase_run_id
) outputs ON true
LEFT JOIN LATERAL (
    SELECT jsonb_agg(to_jsonb(d) ORDER BY d.created_at) AS pm_l3_decisions
    FROM route_b_pm_l3_decisions d
    WHERE d.phase_run_id = p.phase_run_id
) decisions ON true;
