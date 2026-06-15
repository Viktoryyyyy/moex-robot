-- artifact_id: route_b_universal_role_runner_db_schema.v0.1
-- artifact_class: repo_relative
-- artifact_path: docs/sot/context/schemas/route_b_universal_role_runner_db_schema.v0.1.sql
-- producer: PM_L2_PHASE_OWNER
-- consumer: PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER
-- format: postgresql_sql
-- proposal_only: true
-- production_migration_executed: false
-- server_apply_executed: false
-- executed_db_changes: false

BEGIN;

CREATE TABLE IF NOT EXISTS route_b_phase_runs (
    phase_run_id text PRIMARY KEY,
    workflow_run_id text NOT NULL UNIQUE,
    status text NOT NULL CHECK (
        status IN (
            'phase_queued',
            'pm_l3_planning',
            'pm_l3_validating',
            'github_execution_requested',
            'github_executor_pr_opened_waiting_ci',
            'pm_l3_finalizing',
            'pm_l2_review_required',
            'blocked',
            'failed'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS route_b_role_task_queue (
    phase_run_id text NOT NULL REFERENCES route_b_phase_runs(phase_run_id),
    workflow_run_id text NOT NULL,
    role_task_id text PRIMARY KEY,
    parent_role_task_id text NULL REFERENCES route_b_role_task_queue(role_task_id),
    sequence_no integer NOT NULL CHECK (sequence_no >= 0),
    role_id text NOT NULL CHECK (
        role_id IN (
            'PM_L3_DELIVERY_VALIDATION_OWNER',
            'SUBCHAT_REPO_AUDIT',
            'SUBCHAT_SPEC_CONTRACT_DESIGNER',
            'SUBCHAT_IMPLEMENTATION',
            'SUBCHAT_VALIDATION',
            'SUBCHAT_DATA_STEWARD',
            'SUBCHAT_EXPERIMENT_DESIGNER',
            'SUBCHAT_RESEARCH_CRITIC',
            'SUBCHAT_RESEARCH_EXECUTION'
        )
    ),
    task_type text NOT NULL,
    task_payload_json jsonb NOT NULL,
    context_refs_json jsonb NOT NULL CHECK (
        context_refs_json ? 'static_context_refs'
        AND context_refs_json ? 'role_context_ref'
        AND context_refs_json ? 'schema_refs'
    ),
    expected_output_schema_ref text NOT NULL,
    status text NOT NULL CHECK (
        status IN (
            'role_task_ready',
            'role_task_running',
            'role_task_completed',
            'role_task_failed',
            'blocked'
        )
    ),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    max_retries integer NOT NULL DEFAULT 3 CHECK (max_retries >= 0),
    blocker_code text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz NULL,
    completed_at timestamptz NULL,
    UNIQUE (phase_run_id, sequence_no)
);

CREATE INDEX IF NOT EXISTS idx_route_b_role_task_queue_poll_ready
    ON route_b_role_task_queue (created_at, sequence_no, role_task_id)
    WHERE status = 'role_task_ready';

CREATE INDEX IF NOT EXISTS idx_route_b_role_task_queue_phase_status
    ON route_b_role_task_queue (phase_run_id, status, sequence_no);

CREATE INDEX IF NOT EXISTS idx_route_b_role_task_queue_workflow_status
    ON route_b_role_task_queue (workflow_run_id, status, sequence_no);

CREATE TABLE IF NOT EXISTS route_b_role_outputs (
    phase_run_id text NOT NULL REFERENCES route_b_phase_runs(phase_run_id),
    workflow_run_id text NOT NULL,
    role_task_id text PRIMARY KEY REFERENCES route_b_role_task_queue(role_task_id),
    role_id text NOT NULL CHECK (
        role_id IN (
            'PM_L3_DELIVERY_VALIDATION_OWNER',
            'SUBCHAT_REPO_AUDIT',
            'SUBCHAT_SPEC_CONTRACT_DESIGNER',
            'SUBCHAT_IMPLEMENTATION',
            'SUBCHAT_VALIDATION',
            'SUBCHAT_DATA_STEWARD',
            'SUBCHAT_EXPERIMENT_DESIGNER',
            'SUBCHAT_RESEARCH_CRITIC',
            'SUBCHAT_RESEARCH_EXECUTION'
        )
    ),
    output_json jsonb NOT NULL,
    validation_status text NOT NULL CHECK (
        validation_status IN (
            'schema_validation_passed',
            'schema_validation_failed',
            'parse_failed',
            'blocked'
        )
    ),
    raw_content_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (phase_run_id, role_task_id)
);

CREATE INDEX IF NOT EXISTS idx_route_b_role_outputs_phase_role
    ON route_b_role_outputs (phase_run_id, role_id, created_at);

CREATE INDEX IF NOT EXISTS idx_route_b_role_outputs_workflow_validation
    ON route_b_role_outputs (workflow_run_id, validation_status, created_at);

CREATE TABLE IF NOT EXISTS route_b_pm_l3_decisions (
    phase_run_id text NOT NULL REFERENCES route_b_phase_runs(phase_run_id),
    workflow_run_id text NOT NULL,
    role_task_id text NOT NULL REFERENCES route_b_role_task_queue(role_task_id),
    decision_type text NOT NULL CHECK (
        decision_type IN (
            'create_next_role_task',
            'request_github_execution_package',
            'return_blocker',
            'return_final_pm_l3_evidence_package'
        )
    ),
    decision_payload_json jsonb NOT NULL,
    authority_boundary jsonb NOT NULL CHECK (
        authority_boundary ? 'merge_authority'
        AND authority_boundary->>'merge_authority' = 'PM_L2_ONLY'
        AND authority_boundary ? 'pm_l2_review_required'
        AND authority_boundary->>'pm_l2_review_required' = 'true'
        AND authority_boundary ? 'n8n_merge_allowed'
        AND authority_boundary->>'n8n_merge_allowed' = 'false'
        AND authority_boundary ? 'direct_main_write_allowed'
        AND authority_boundary->>'direct_main_write_allowed' = 'false'
        AND authority_boundary ? 'force_push_allowed'
        AND authority_boundary->>'force_push_allowed' = 'false'
        AND authority_boundary ? 'file_delete_allowed'
        AND authority_boundary->>'file_delete_allowed' = 'false'
        AND authority_boundary ? 'server_apply_allowed'
        AND authority_boundary->>'server_apply_allowed' = 'false'
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (phase_run_id, role_task_id, decision_type, created_at)
);

CREATE INDEX IF NOT EXISTS idx_route_b_pm_l3_decisions_phase_type
    ON route_b_pm_l3_decisions (phase_run_id, decision_type, created_at);

CREATE INDEX IF NOT EXISTS idx_route_b_pm_l3_decisions_workflow_type
    ON route_b_pm_l3_decisions (workflow_run_id, decision_type, created_at);

CREATE VIEW route_b_workflow_result_status_extension_v0_1 AS
SELECT
    p.workflow_run_id,
    jsonb_build_object(
        'phase_run', to_jsonb(p),
        'role_tasks', COALESCE((
            SELECT jsonb_agg(to_jsonb(t) ORDER BY t.sequence_no)
            FROM route_b_role_task_queue t
            WHERE t.phase_run_id = p.phase_run_id
        ), '[]'::jsonb),
        'role_outputs', COALESCE((
            SELECT jsonb_agg(to_jsonb(o) ORDER BY o.created_at)
            FROM route_b_role_outputs o
            WHERE o.phase_run_id = p.phase_run_id
        ), '[]'::jsonb),
        'pm_l3_decisions', COALESCE((
            SELECT jsonb_agg(to_jsonb(d) ORDER BY d.created_at)
            FROM route_b_pm_l3_decisions d
            WHERE d.phase_run_id = p.phase_run_id
        ), '[]'::jsonb),
        'authority_boundary', jsonb_build_object(
            'merge_authority', 'PM_L2_ONLY',
            'n8n_merge_allowed', false,
            'direct_main_write_allowed', false,
            'force_push_allowed', false,
            'file_delete_allowed', false,
            'executor_merge_allowed', false,
            'server_apply_allowed', false
        ),
        'blockers', '[]'::jsonb,
        'required_fixes', '[]'::jsonb
    ) AS result_status_extension_json
FROM route_b_phase_runs p;

COMMIT;
