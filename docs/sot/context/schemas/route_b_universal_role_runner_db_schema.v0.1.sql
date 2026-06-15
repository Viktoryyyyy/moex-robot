-- artifact_id: route_b_universal_role_runner_db_schema.v0.1
-- artifact_class: repo_relative
-- producer: PM_L2_PHASE_OWNER
-- consumer: ROUTE_B_UNIVERSAL_ROLE_RUNNER
-- format: postgresql_sql
-- repo_relative_path: docs/sot/context/schemas/route_b_universal_role_runner_db_schema.v0.1.sql
-- schema_ref: route_b_universal_role_runner_db_contract.v0.1
-- proposal_only: true
-- production_migration_executed: false
-- server_apply_executed: false
-- executed_db_changes: false
-- source_of_truth: github_repo

BEGIN;

CREATE TABLE IF NOT EXISTS route_b_phase_runs (
    phase_run_id text PRIMARY KEY,
    workflow_run_id text NOT NULL,
    status text NOT NULL CHECK (
        status IN (
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
        )
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT route_b_phase_runs_workflow_run_id_unique UNIQUE (workflow_run_id),
    CONSTRAINT route_b_phase_runs_phase_workflow_unique UNIQUE (phase_run_id, workflow_run_id)
);

CREATE TABLE IF NOT EXISTS route_b_role_task_queue (
    role_task_id text PRIMARY KEY,
    phase_run_id text NOT NULL,
    workflow_run_id text NOT NULL,
    parent_role_task_id text NULL,
    sequence_no integer NOT NULL,
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
    task_type text NOT NULL CHECK (
        task_type IN (
            'pm_l3_planning',
            'subchat_execution',
            'pm_l3_validation',
            'pm_l3_finalization'
        )
    ),
    task_payload_json jsonb NOT NULL,
    context_refs_json jsonb NOT NULL,
    expected_output_schema_ref text NOT NULL,
    status text NOT NULL DEFAULT 'role_task_ready' CHECK (
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
    CONSTRAINT route_b_role_task_queue_phase_fk
        FOREIGN KEY (phase_run_id, workflow_run_id)
        REFERENCES route_b_phase_runs (phase_run_id, workflow_run_id),
    CONSTRAINT route_b_role_task_queue_parent_fk
        FOREIGN KEY (parent_role_task_id)
        REFERENCES route_b_role_task_queue (role_task_id),
    CONSTRAINT route_b_role_task_queue_sequence_unique
        UNIQUE (phase_run_id, sequence_no),
    CONSTRAINT route_b_role_task_queue_context_refs_required_children
        CHECK (
            context_refs_json ? 'static_context_refs'
            AND context_refs_json ? 'role_context_ref'
            AND context_refs_json ? 'schema_refs'
        )
);

CREATE INDEX IF NOT EXISTS route_b_role_task_queue_poll_idx
    ON route_b_role_task_queue (status, sequence_no, created_at)
    WHERE status = 'role_task_ready';

CREATE INDEX IF NOT EXISTS route_b_role_task_queue_phase_status_idx
    ON route_b_role_task_queue (phase_run_id, status, sequence_no);

CREATE INDEX IF NOT EXISTS route_b_role_task_queue_parent_idx
    ON route_b_role_task_queue (parent_role_task_id)
    WHERE parent_role_task_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS route_b_role_outputs (
    role_task_id text PRIMARY KEY,
    phase_run_id text NOT NULL,
    workflow_run_id text NOT NULL,
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
            'schema_validated',
            'schema_validation_failed',
            'blocked'
        )
    ),
    raw_content_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT route_b_role_outputs_role_task_fk
        FOREIGN KEY (role_task_id)
        REFERENCES route_b_role_task_queue (role_task_id),
    CONSTRAINT route_b_role_outputs_phase_fk
        FOREIGN KEY (phase_run_id, workflow_run_id)
        REFERENCES route_b_phase_runs (phase_run_id, workflow_run_id)
);

CREATE INDEX IF NOT EXISTS route_b_role_outputs_phase_idx
    ON route_b_role_outputs (phase_run_id, workflow_run_id, created_at);

CREATE INDEX IF NOT EXISTS route_b_role_outputs_validation_idx
    ON route_b_role_outputs (validation_status, created_at);

CREATE TABLE IF NOT EXISTS route_b_pm_l3_decisions (
    phase_run_id text NOT NULL,
    workflow_run_id text NOT NULL,
    role_task_id text NOT NULL,
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
        authority_boundary ->> 'merge_authority' = 'PM_L2_ONLY'
        AND coalesce((authority_boundary ->> 'pm_l2_review_required')::boolean, false) = true
        AND coalesce((authority_boundary ->> 'n8n_merge_allowed')::boolean, true) = false
        AND coalesce((authority_boundary ->> 'direct_main_write_allowed')::boolean, true) = false
        AND coalesce((authority_boundary ->> 'force_push_allowed')::boolean, true) = false
        AND coalesce((authority_boundary ->> 'file_delete_allowed')::boolean, true) = false
        AND coalesce((authority_boundary ->> 'runtime_live_trading_allowed')::boolean, true) = false
        AND coalesce((authority_boundary ->> 'broker_execution_allowed')::boolean, true) = false
        AND coalesce((authority_boundary ->> 'server_apply_allowed')::boolean, true) = false
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (phase_run_id, workflow_run_id, role_task_id, decision_type, created_at),
    CONSTRAINT route_b_pm_l3_decisions_role_task_fk
        FOREIGN KEY (role_task_id)
        REFERENCES route_b_role_task_queue (role_task_id),
    CONSTRAINT route_b_pm_l3_decisions_phase_fk
        FOREIGN KEY (phase_run_id, workflow_run_id)
        REFERENCES route_b_phase_runs (phase_run_id, workflow_run_id)
);

CREATE INDEX IF NOT EXISTS route_b_pm_l3_decisions_phase_idx
    ON route_b_pm_l3_decisions (phase_run_id, workflow_run_id, created_at);

CREATE INDEX IF NOT EXISTS route_b_pm_l3_decisions_type_idx
    ON route_b_pm_l3_decisions (decision_type, created_at);

CREATE OR REPLACE VIEW route_b_phase_result_status_v0_1 AS
SELECT
    p.phase_run_id,
    p.workflow_run_id,
    p.status AS phase_status,
    p.created_at AS phase_created_at,
    p.updated_at AS phase_updated_at,
    COALESCE((
        SELECT jsonb_agg(
            jsonb_build_object(
                'role_task_id', t.role_task_id,
                'parent_role_task_id', t.parent_role_task_id,
                'sequence_no', t.sequence_no,
                'role_id', t.role_id,
                'task_type', t.task_type,
                'status', t.status,
                'attempt', t.attempt,
                'max_retries', t.max_retries,
                'blocker_code', t.blocker_code,
                'created_at', t.created_at,
                'claimed_at', t.claimed_at,
                'completed_at', t.completed_at
            )
            ORDER BY t.sequence_no, t.created_at
        )
        FROM route_b_role_task_queue t
        WHERE t.phase_run_id = p.phase_run_id
          AND t.workflow_run_id = p.workflow_run_id
    ), '[]'::jsonb) AS role_tasks,
    COALESCE((
        SELECT jsonb_agg(
            jsonb_build_object(
                'role_task_id', o.role_task_id,
                'role_id', o.role_id,
                'validation_status', o.validation_status,
                'raw_content_hash', o.raw_content_hash,
                'output_json', o.output_json,
                'created_at', o.created_at
            )
            ORDER BY o.created_at
        )
        FROM route_b_role_outputs o
        WHERE o.phase_run_id = p.phase_run_id
          AND o.workflow_run_id = p.workflow_run_id
    ), '[]'::jsonb) AS role_outputs,
    COALESCE((
        SELECT jsonb_agg(
            jsonb_build_object(
                'role_task_id', d.role_task_id,
                'decision_type', d.decision_type,
                'decision_payload_json', d.decision_payload_json,
                'authority_boundary', d.authority_boundary,
                'created_at', d.created_at
            )
            ORDER BY d.created_at
        )
        FROM route_b_pm_l3_decisions d
        WHERE d.phase_run_id = p.phase_run_id
          AND d.workflow_run_id = p.workflow_run_id
    ), '[]'::jsonb) AS pm_l3_decisions
FROM route_b_phase_runs p;

COMMIT;
