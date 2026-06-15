-- artifact_id: route_b_universal_role_runner_db_migration.v0.1
-- artifact_class: repo_relative
-- artifact_path: docs/sot/context/schemas/route_b_universal_role_runner_db_migration.v0.1.sql
-- producer: PM_L2_PHASE_OWNER
-- consumer: PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER
-- format: sql
-- proposal_only: true
-- production_migration_executed: false
-- server_apply_executed: false
-- executed_db_changes: false
-- source_of_truth: github_repo
-- contract_refs:
--   - route_b_universal_role_runner_db_contract.v0.1
--   - route_b_role_task_queue.v0.1
--   - route_b_pm_l3_decision_loop.v0.1
--   - route_b_multi_role_phase_state_machine.v0.1
-- status_result_extension_fields:
--   - phase_run
--   - role_tasks
--   - role_outputs
--   - pm_l3_decisions

create table if not exists route_b_phase_runs (
    phase_run_id text primary key,
    workflow_run_id text not null,
    status text not null,
    status_payload_json jsonb not null default '{}'::jsonb,
    blocker_code text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint route_b_phase_runs_workflow_run_id_uk unique (workflow_run_id),
    constraint route_b_phase_runs_status_ck check (
        status in (
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
    )
);

create table if not exists route_b_role_task_queue (
    role_task_id text primary key,
    phase_run_id text not null,
    workflow_run_id text not null,
    parent_role_task_id text,
    sequence_no integer not null,
    role_id text not null,
    task_type text not null,
    task_payload_json jsonb not null,
    context_refs_json jsonb not null,
    expected_output_schema_ref text not null,
    status text not null default 'role_task_ready',
    attempt integer not null default 0,
    max_retries integer not null default 3,
    blocker_code text,
    created_at timestamptz not null default now(),
    claimed_at timestamptz,
    completed_at timestamptz,
    constraint route_b_role_task_queue_phase_fk foreign key (phase_run_id)
        references route_b_phase_runs (phase_run_id),
    constraint route_b_role_task_queue_parent_fk foreign key (parent_role_task_id)
        references route_b_role_task_queue (role_task_id) deferrable initially deferred,
    constraint route_b_role_task_queue_phase_sequence_uk unique (phase_run_id, sequence_no),
    constraint route_b_role_task_queue_role_id_ck check (
        role_id in (
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
    constraint route_b_role_task_queue_status_ck check (
        status in (
            'role_task_ready',
            'role_task_running',
            'role_task_completed',
            'role_task_failed',
            'blocked'
        )
    ),
    constraint route_b_role_task_queue_attempt_ck check (attempt >= 0),
    constraint route_b_role_task_queue_max_retries_ck check (max_retries >= 0),
    constraint route_b_role_task_queue_context_refs_object_ck check (jsonb_typeof(context_refs_json) = 'object'),
    constraint route_b_role_task_queue_task_payload_object_ck check (jsonb_typeof(task_payload_json) = 'object')
);

create index if not exists route_b_role_task_queue_poll_ready_idx
    on route_b_role_task_queue (status, created_at, sequence_no)
    where status = 'role_task_ready';

create index if not exists route_b_role_task_queue_phase_status_idx
    on route_b_role_task_queue (phase_run_id, status, sequence_no);

create index if not exists route_b_role_task_queue_parent_idx
    on route_b_role_task_queue (parent_role_task_id)
    where parent_role_task_id is not null;

create table if not exists route_b_role_outputs (
    role_task_id text primary key,
    phase_run_id text not null,
    workflow_run_id text not null,
    role_id text not null,
    output_json jsonb not null,
    validation_status text not null,
    raw_content_hash text not null,
    validation_errors_json jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now(),
    constraint route_b_role_outputs_phase_fk foreign key (phase_run_id)
        references route_b_phase_runs (phase_run_id),
    constraint route_b_role_outputs_role_task_fk foreign key (role_task_id)
        references route_b_role_task_queue (role_task_id),
    constraint route_b_role_outputs_validation_status_ck check (
        validation_status in (
            'validation_pending',
            'schema_valid',
            'schema_invalid',
            'blocked'
        )
    ),
    constraint route_b_role_outputs_output_object_ck check (jsonb_typeof(output_json) = 'object'),
    constraint route_b_role_outputs_validation_errors_array_ck check (jsonb_typeof(validation_errors_json) = 'array')
);

create index if not exists route_b_role_outputs_phase_role_idx
    on route_b_role_outputs (phase_run_id, role_id, created_at);

create index if not exists route_b_role_outputs_validation_status_idx
    on route_b_role_outputs (validation_status, created_at);

create table if not exists route_b_pm_l3_decisions (
    pm_l3_decision_id text primary key,
    phase_run_id text not null,
    workflow_run_id text not null,
    role_task_id text not null,
    decision_type text not null,
    decision_payload_json jsonb not null,
    authority_boundary_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    constraint route_b_pm_l3_decisions_phase_fk foreign key (phase_run_id)
        references route_b_phase_runs (phase_run_id),
    constraint route_b_pm_l3_decisions_role_task_fk foreign key (role_task_id)
        references route_b_role_task_queue (role_task_id),
    constraint route_b_pm_l3_decisions_type_ck check (
        decision_type in (
            'create_next_role_task',
            'request_github_execution_package',
            'return_blocker',
            'return_final_pm_l3_evidence_package'
        )
    ),
    constraint route_b_pm_l3_decisions_payload_object_ck check (jsonb_typeof(decision_payload_json) = 'object'),
    constraint route_b_pm_l3_decisions_authority_object_ck check (jsonb_typeof(authority_boundary_json) = 'object')
);

create index if not exists route_b_pm_l3_decisions_phase_type_idx
    on route_b_pm_l3_decisions (phase_run_id, decision_type, created_at);

create index if not exists route_b_pm_l3_decisions_role_task_idx
    on route_b_pm_l3_decisions (role_task_id, created_at);

create or replace view route_b_result_status_extension_v0_1 as
select
    p.phase_run_id,
    p.workflow_run_id,
    p.status as phase_run_status,
    p.status_payload_json as phase_run,
    (
        select coalesce(jsonb_agg(to_jsonb(t) order by t.sequence_no), '[]'::jsonb)
        from route_b_role_task_queue t
        where t.phase_run_id = p.phase_run_id
    ) as role_tasks,
    (
        select coalesce(jsonb_agg(to_jsonb(o) order by o.created_at), '[]'::jsonb)
        from route_b_role_outputs o
        where o.phase_run_id = p.phase_run_id
    ) as role_outputs,
    (
        select coalesce(jsonb_agg(to_jsonb(d) order by d.created_at), '[]'::jsonb)
        from route_b_pm_l3_decisions d
        where d.phase_run_id = p.phase_run_id
    ) as pm_l3_decisions
from route_b_phase_runs p;
