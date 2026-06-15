-- schema_id: route_b_universal_role_runner_db_migration_proposal.v0.1
-- project: MOEX Bot
-- status: proposal
-- source_of_truth: github_repo
-- artifact_class: repo_relative
-- artifact_path: docs/sot/context/schemas/route_b_universal_role_runner_db_migration_proposal.v0.1.sql
-- producer: PM_L2_PHASE_OWNER
-- consumer: PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER
--
-- proposal_only: true
-- production_execution_allowed: false
-- no_production_execution_notice: This SQL proposal is documentation-only and is not approved for production DB execution.
-- server_apply_allowed: false
-- no_server_apply_notice: Server apply is not part of this artifact.
-- migration_execution_allowed: false
-- no_migration_execution_notice: Migration execution is explicitly out of scope.
-- n8n_production_workflow_mutation_allowed: false
-- no_n8n_production_workflow_mutation_notice: Do not mutate production n8n workflows from this proposal.
-- merge_authority_change_allowed: false
-- no_merge_authority_change_notice: PM_L2_ONLY merge authority is preserved.
-- does_not_imply_execution_on_production_db: true
--
-- Boundary:
-- GitHub / repo is the Source of Truth for this proposal.
-- Server is Applied State only and is not architectural proof.
-- server is not architectural proof.
-- This file proposes future persistence shape only.

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
        (status NOT IN ('phase_run_completed', 'phase_run_failed', 'blocked'))
    )
);

COMMENT ON TABLE route_b_phase_runs IS
'Proposal-only Route B phase run state. GitHub/repo is Source of Truth; server is Applied State only. No production execution, no server apply, no migration execution.';

CREATE TABLE IF NOT EXISTS route_b_role_tasks (
    phase_run_id text NOT NULL REFERENCES route_b_phase_runs(phase_run_id),
    workflow_run_id text NOT NULL,
    role_task_id text PRIMARY KEY,
    parent_role_task_id text REFERENCES route_b_role_tasks(role_task_id),
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
    CONSTRAINT uq_route_b_role_tasks_phase_sequence UNIQUE (phase_run_id, sequence_no),
    CONSTRAINT uq_route_b_role_tasks_workflow_task UNIQUE (workflow_run_id, role_task_id),
    CONSTRAINT ck_route_b_role_tasks_role_id CHECK (
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
    CONSTRAINT ck_route_b_role_tasks_status_lifecycle CHECK (
        status IN (
            'role_task_ready',
            'role_task_running',
            'role_task_completed',
            'role_task_failed',
            'blocked'
        )
    ),
    CONSTRAINT ck_route_b_role_tasks_attempts CHECK (
        attempt >= 0
        AND max_retries >= 0
        AND attempt <= max_retries
    ),
    CONSTRAINT ck_route_b_role_tasks_lifecycle_timestamps CHECK (
        (status = 'role_task_ready' AND claimed_at IS NULL AND completed_at IS NULL)
        OR
        (status = 'role_task_running' AND claimed_at IS NOT NULL AND completed_at IS NULL)
        OR
        (status IN ('role_task_completed', 'role_task_failed', 'blocked') AND completed_at IS NOT NULL)
    ),
    CONSTRAINT ck_route_b_role_tasks_context_refs_object CHECK (
        jsonb_typeof(context_refs_json) = 'object'
    ),
    CONSTRAINT ck_route_b_role_tasks_payload_object CHECK (
        jsonb_typeof(task_payload_json) = 'object'
    )
);

COMMENT ON TABLE route_b_role_tasks IS
'Proposal-only deterministic task queue for PM L3 and SUBCHAT role_id executions. Maps accepted role_task_queue contract into proposed persistence. No separate workflow per role and no separate AI node per role.';

CREATE INDEX IF NOT EXISTS ix_route_b_role_tasks_ready_queue
    ON route_b_role_tasks (phase_run_id, sequence_no, created_at, role_task_id)
    WHERE status = 'role_task_ready';

CREATE INDEX IF NOT EXISTS ix_route_b_role_tasks_status_claim
    ON route_b_role_tasks (status, claimed_at, created_at, role_task_id);

CREATE INDEX IF NOT EXISTS ix_route_b_role_tasks_workflow_status
    ON route_b_role_tasks (workflow_run_id, status, sequence_no);

CREATE TABLE IF NOT EXISTS route_b_role_outputs (
    role_output_id text PRIMARY KEY,
    role_task_id text NOT NULL REFERENCES route_b_role_tasks(role_task_id),
    workflow_run_id text NOT NULL,
    role_id text NOT NULL,
    output_json jsonb NOT NULL,
    output_schema_ref text NOT NULL,
    validation_status text NOT NULL DEFAULT 'not_validated',
    validation_errors_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_route_b_role_outputs_task_schema UNIQUE (role_task_id, output_schema_ref),
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
    )
);

COMMENT ON TABLE route_b_role_outputs IS
'Proposal-only persisted strict JSON role outputs for Universal Role Runner. Output evidence remains repo/contract governed; server is Applied State only.';

CREATE INDEX IF NOT EXISTS ix_route_b_role_outputs_task_created
    ON route_b_role_outputs (role_task_id, created_at);

CREATE INDEX IF NOT EXISTS ix_route_b_role_outputs_workflow_role
    ON route_b_role_outputs (workflow_run_id, role_id, created_at);

CREATE TABLE IF NOT EXISTS route_b_pm_l3_decisions (
    pm_l3_decision_id text PRIMARY KEY,
    phase_run_id text NOT NULL REFERENCES route_b_phase_runs(phase_run_id),
    workflow_run_id text NOT NULL,
    source_role_task_id text REFERENCES route_b_role_tasks(role_task_id),
    decision_type text NOT NULL,
    next_role_id text,
    next_task_payload_json jsonb,
    github_execution_request_json jsonb,
    blocker_code text,
    final_pm_l3_package_json jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_route_b_pm_l3_decisions_type CHECK (
        decision_type IN (
            'create_next_role_task',
            'request_github_execution_package',
            'return_blocker',
            'return_final_pm_l3_evidence_package',
            'no_op'
        )
    ),
    CONSTRAINT ck_route_b_pm_l3_decisions_next_role CHECK (
        next_role_id IS NULL
        OR next_role_id IN (
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
    CONSTRAINT ck_route_b_pm_l3_decisions_payload_by_type CHECK (
        (decision_type = 'create_next_role_task' AND next_role_id IS NOT NULL AND next_task_payload_json IS NOT NULL)
        OR
        (decision_type = 'request_github_execution_package' AND github_execution_request_json IS NOT NULL)
        OR
        (decision_type = 'return_blocker' AND blocker_code IS NOT NULL)
        OR
        (decision_type = 'return_final_pm_l3_evidence_package' AND final_pm_l3_package_json IS NOT NULL)
        OR
        (decision_type = 'no_op')
    )
);

COMMENT ON TABLE route_b_pm_l3_decisions IS
'Proposal-only PM L3 decision persistence. PM L3 cannot approve merge; PM_L2_ONLY merge authority is unchanged. GitHub executor remains separate PR-only executor.';

CREATE INDEX IF NOT EXISTS ix_route_b_pm_l3_decisions_phase_created
    ON route_b_pm_l3_decisions (phase_run_id, created_at, pm_l3_decision_id);

CREATE INDEX IF NOT EXISTS ix_route_b_pm_l3_decisions_workflow_type
    ON route_b_pm_l3_decisions (workflow_run_id, decision_type, created_at);

-- Deterministic queue behavior:
-- Consumers claim role tasks by selecting status = 'role_task_ready' ordered by
-- phase_run_id, sequence_no, created_at, role_task_id inside one transaction.
-- This proposal does not execute that behavior and does not change production state.
