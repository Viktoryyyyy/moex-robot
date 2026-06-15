-- schema_id: route_b_universal_role_runner_db_migration_execution.v0.1
-- project: MOEX Bot
-- status: executable_migration
-- source_of_truth: github_repo
-- artifact_class: repo_relative
-- artifact_path: docs/sot/context/schemas/route_b_universal_role_runner_db_migration_execution.v0.1.sql
-- accepted_proposal_artifact: docs/sot/context/schemas/route_b_universal_role_runner_db_migration_proposal.v0.1.sql
-- db_contract_artifact: docs/sot/context/schemas/route_b_universal_role_runner_db_contract.v0.1.yaml
-- execution_boundary: repo_only_artifact; operator approval is required before any DB apply.
-- transactional_boundary: explicit_begin_commit

BEGIN;

CREATE TABLE IF NOT EXISTS route_b_phase_runs (
    phase_run_id text PRIMARY KEY,
    workflow_run_id text NOT NULL UNIQUE,
    repository_full_name text NOT NULL,
    root_task_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'phase_run_created',
    blocker_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT ck_route_b_phase_runs_root_task_object CHECK (
        jsonb_typeof(root_task_json) = 'object'
    ),
    CONSTRAINT ck_route_b_phase_runs_status_lifecycle CHECK (
        status IN (
            'phase_run_created',
            'phase_run_running',
            'phase_run_waiting_pm_l3',
            'phase_run_completed',
            'phase_run_failed',
            'blocked'
        )
    ),
    CONSTRAINT ck_route_b_phase_runs_completed_at CHECK (
        (status IN ('phase_run_completed', 'phase_run_failed', 'blocked') AND completed_at IS NOT NULL)
        OR
        (status NOT IN ('phase_run_completed', 'phase_run_failed', 'blocked') AND completed_at IS NULL)
    )
);

COMMENT ON TABLE route_b_phase_runs IS
'Route B phase run state for Universal Role Runner DB migration execution v0.1.';

COMMENT ON COLUMN route_b_phase_runs.root_task_json IS
'Dynamic root task payload captured as strict JSON object.';

CREATE TABLE IF NOT EXISTS route_b_role_task_queue (
    phase_run_id text NOT NULL REFERENCES route_b_phase_runs(phase_run_id),
    workflow_run_id text NOT NULL,
    role_task_id text PRIMARY KEY,
    parent_role_task_id text REFERENCES route_b_role_task_queue(role_task_id),
    sequence_no integer NOT NULL,
    role_id text NOT NULL,
    task_type text NOT NULL,
    task_payload_json jsonb NOT NULL,
    context_refs_json jsonb NOT NULL,
    expected_output_schema_ref text NOT NULL,
    status text NOT NULL DEFAULT 'role_task_ready',
    attempt integer NOT NULL DEFAULT 0,
    max_retries integer NOT NULL DEFAULT 3,
    blocker_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz,
    completed_at timestamptz,
    CONSTRAINT uq_route_b_role_task_queue_phase_sequence UNIQUE (phase_run_id, sequence_no),
    CONSTRAINT uq_route_b_role_task_queue_workflow_task UNIQUE (workflow_run_id, role_task_id),
    CONSTRAINT uq_route_b_role_task_queue_phase_workflow_task UNIQUE (phase_run_id, workflow_run_id, role_task_id),
    CONSTRAINT uq_route_b_role_task_queue_phase_workflow_task_role UNIQUE (phase_run_id, workflow_run_id, role_task_id, role_id),
    CONSTRAINT ck_route_b_role_task_queue_role_id CHECK (
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
    CONSTRAINT ck_route_b_role_task_queue_status_lifecycle CHECK (
        status IN (
            'role_task_ready',
            'role_task_running',
            'role_task_completed',
            'role_task_failed',
            'blocked'
        )
    ),
    CONSTRAINT ck_route_b_role_task_queue_attempts CHECK (
        attempt >= 0
        AND max_retries >= 0
        AND attempt <= max_retries
    ),
    CONSTRAINT ck_route_b_role_task_queue_lifecycle_timestamps CHECK (
        (status = 'role_task_ready' AND claimed_at IS NULL AND completed_at IS NULL)
        OR
        (status = 'role_task_running' AND claimed_at IS NOT NULL AND completed_at IS NULL)
        OR
        (status IN ('role_task_completed', 'role_task_failed', 'blocked') AND completed_at IS NOT NULL)
    ),
    CONSTRAINT ck_route_b_role_task_queue_context_refs_object CHECK (
        jsonb_typeof(context_refs_json) = 'object'
        AND context_refs_json ? 'static_context_refs'
        AND context_refs_json ? 'role_context_ref'
        AND context_refs_json ? 'schema_refs'
    ),
    CONSTRAINT ck_route_b_role_task_queue_context_role_id_matches CHECK (
        jsonb_typeof(context_refs_json->'role_context_ref') = 'object'
        AND context_refs_json #>> '{role_context_ref,role_id}' = role_id
    ),
    CONSTRAINT ck_route_b_role_task_queue_payload_object CHECK (
        jsonb_typeof(task_payload_json) = 'object'
    )
);

COMMENT ON TABLE route_b_role_task_queue IS
'Deterministic task queue for PM L3 and SUBCHAT role_id executions. The accepted proposal table route_b_role_tasks is narrowed to route_b_role_task_queue by the DB contract.';

COMMENT ON COLUMN route_b_role_task_queue.context_refs_json IS
'Required context refs object containing static_context_refs, role_context_ref, and schema_refs. role_context_ref.role_id must equal role_id.';

CREATE INDEX IF NOT EXISTS ix_route_b_role_task_queue_ready_queue
    ON route_b_role_task_queue (phase_run_id, sequence_no, created_at, role_task_id)
    WHERE status = 'role_task_ready';

CREATE INDEX IF NOT EXISTS ix_route_b_role_task_queue_status_claim
    ON route_b_role_task_queue (status, claimed_at, created_at, role_task_id);

CREATE INDEX IF NOT EXISTS ix_route_b_role_task_queue_workflow_status
    ON route_b_role_task_queue (workflow_run_id, status, sequence_no);

CREATE TABLE IF NOT EXISTS route_b_role_outputs (
    role_output_id text PRIMARY KEY,
    phase_run_id text NOT NULL,
    role_task_id text NOT NULL,
    workflow_run_id text NOT NULL,
    role_id text NOT NULL,
    output_json jsonb NOT NULL,
    output_schema_ref text NOT NULL,
    validation_status text NOT NULL DEFAULT 'not_validated',
    validation_errors_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    raw_content_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_route_b_role_outputs_task_schema UNIQUE (role_task_id, output_schema_ref),
    CONSTRAINT fk_route_b_role_outputs_task_scope FOREIGN KEY (
        phase_run_id,
        workflow_run_id,
        role_task_id,
        role_id
    ) REFERENCES route_b_role_task_queue (
        phase_run_id,
        workflow_run_id,
        role_task_id,
        role_id
    ),
    CONSTRAINT ck_route_b_role_outputs_role_id CHECK (
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
    CONSTRAINT ck_route_b_role_outputs_validation_status CHECK (
        validation_status IN (
            'not_validated',
            'pass',
            'conditional_pass',
            'fail',
            'blocked'
        )
    ),
    CONSTRAINT ck_route_b_role_outputs_output_object CHECK (
        jsonb_typeof(output_json) = 'object'
    ),
    CONSTRAINT ck_route_b_role_outputs_validation_errors_array CHECK (
        jsonb_typeof(validation_errors_json) = 'array'
    ),
    CONSTRAINT ck_route_b_role_outputs_raw_content_hash_not_blank CHECK (
        length(btrim(raw_content_hash)) > 0
    )
);

COMMENT ON TABLE route_b_role_outputs IS
'Strict JSON role outputs produced by the Universal Role Runner and tied back to one role task.';

COMMENT ON COLUMN route_b_role_outputs.raw_content_hash IS
'Deterministic hash of the raw role output content for audit comparison.';

CREATE INDEX IF NOT EXISTS ix_route_b_role_outputs_task_created
    ON route_b_role_outputs (role_task_id, created_at);

CREATE INDEX IF NOT EXISTS ix_route_b_role_outputs_workflow_role
    ON route_b_role_outputs (workflow_run_id, role_id, created_at);

CREATE INDEX IF NOT EXISTS ix_route_b_role_outputs_phase_workflow_created
    ON route_b_role_outputs (phase_run_id, workflow_run_id, created_at);

CREATE TABLE IF NOT EXISTS route_b_pm_l3_decisions (
    pm_l3_decision_id text PRIMARY KEY,
    phase_run_id text NOT NULL,
    workflow_run_id text NOT NULL,
    role_task_id text NOT NULL,
    decision_type text NOT NULL,
    decision_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    authority_boundary_json jsonb NOT NULL DEFAULT '{
        "merge_authority": "PM_L2_ONLY",
        "n8n_merge_allowed": false,
        "direct_main_write_allowed": false,
        "force_push_allowed": false,
        "file_delete_allowed": false,
        "executor_merge_allowed": false,
        "runtime_live_trading_allowed": false,
        "broker_execution_allowed": false,
        "server_apply_allowed": false,
        "production_secret_access_allowed": false
    }'::jsonb,
    blocker_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_route_b_pm_l3_decisions_task_scope FOREIGN KEY (
        phase_run_id,
        workflow_run_id,
        role_task_id
    ) REFERENCES route_b_role_task_queue (
        phase_run_id,
        workflow_run_id,
        role_task_id
    ),
    CONSTRAINT ck_route_b_pm_l3_decisions_type CHECK (
        decision_type IN (
            'create_next_role_task',
            'request_github_execution_package',
            'return_blocker',
            'return_final_pm_l3_evidence_package',
            'no_op'
        )
    ),
    CONSTRAINT ck_route_b_pm_l3_decisions_payload_object CHECK (
        jsonb_typeof(decision_payload_json) = 'object'
    ),
    CONSTRAINT ck_route_b_pm_l3_decisions_authority_object CHECK (
        jsonb_typeof(authority_boundary_json) = 'object'
    ),
    CONSTRAINT ck_route_b_pm_l3_decisions_authority_boundary CHECK (
        authority_boundary_json->>'merge_authority' = 'PM_L2_ONLY'
        AND authority_boundary_json @> '{
            "n8n_merge_allowed": false,
            "direct_main_write_allowed": false,
            "force_push_allowed": false,
            "file_delete_allowed": false,
            "executor_merge_allowed": false,
            "runtime_live_trading_allowed": false,
            "broker_execution_allowed": false,
            "server_apply_allowed": false,
            "production_secret_access_allowed": false
        }'::jsonb
    ),
    CONSTRAINT ck_route_b_pm_l3_decisions_payload_by_type CHECK (
        (decision_type = 'create_next_role_task'
            AND decision_payload_json ? 'next_role_id'
            AND decision_payload_json ? 'next_task_payload_json')
        OR
        (decision_type = 'request_github_execution_package'
            AND decision_payload_json ? 'github_execution_request_json')
        OR
        (decision_type = 'return_blocker'
            AND blocker_code IS NOT NULL)
        OR
        (decision_type = 'return_final_pm_l3_evidence_package'
            AND decision_payload_json ? 'final_pm_l3_package_json')
        OR
        (decision_type = 'no_op')
    )
);

COMMENT ON TABLE route_b_pm_l3_decisions IS
'PM L3 planning, validation, GitHub request, blocker, and final evidence decisions. Merge authority remains PM_L2_ONLY.';

COMMENT ON COLUMN route_b_pm_l3_decisions.decision_payload_json IS
'Decision payload JSON object. Type-specific payload requirements are guarded by decision_type.';

CREATE INDEX IF NOT EXISTS ix_route_b_pm_l3_decisions_phase_type_created
    ON route_b_pm_l3_decisions (phase_run_id, decision_type, created_at);

CREATE INDEX IF NOT EXISTS ix_route_b_pm_l3_decisions_role_task_created
    ON route_b_pm_l3_decisions (role_task_id, created_at);

CREATE INDEX IF NOT EXISTS ix_route_b_pm_l3_decisions_workflow_type
    ON route_b_pm_l3_decisions (workflow_run_id, decision_type, created_at);

-- Deterministic queue behavior:
-- Consumers claim role tasks by selecting status = 'role_task_ready' ordered by
-- phase_run_id, sequence_no, created_at, role_task_id inside one transaction.

COMMIT;
