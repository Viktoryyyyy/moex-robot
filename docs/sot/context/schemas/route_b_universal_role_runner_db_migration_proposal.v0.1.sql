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
-- merge_authority: PM_L2_ONLY
-- github_source_of_truth_boundary: true
-- server_applied_state_only: true
-- github_executor_remains_separate_pr_only_executor: true
-- pm_l2_only_merge_authority_boundary: true
-- result_status_extension_support_role_tasks: status, status_detail, result_json, blocker_code, required_fixes_json
-- result_status_extension_support_role_outputs: validation_status, result_status, result_json, output_json
-- result_status_extension_support_pm_l3_decisions: decision_status, result_json, blocker_code, required_fixes_json

CREATE TABLE IF NOT EXISTS route_b_phase_runs (
  phase_run_id text NOT NULL,
  workflow_run_id text NOT NULL,
  repository_full_name text NOT NULL,
  phase_name text NOT NULL,
  status text NOT NULL,
  context_registry_ref text NOT NULL,
  authority_boundary_json jsonb NOT NULL,
  github_executor_ref_json jsonb,
  result_status_json jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (phase_run_id),
  UNIQUE (workflow_run_id, phase_run_id),
  CHECK (status IN (
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
  CHECK (authority_boundary_json->>'merge_authority' = 'PM_L2_ONLY'),
  CHECK (authority_boundary_json->>'n8n_merge_allowed' = 'false'),
  CHECK (authority_boundary_json->>'direct_main_write_allowed' = 'false'),
  CHECK (authority_boundary_json->>'force_push_allowed' = 'false'),
  CHECK (authority_boundary_json->>'executor_merge_allowed' = 'false')
);

COMMENT ON TABLE route_b_phase_runs IS
  'Proposal-only Route B phase run table. GitHub repo is Source of Truth; server is Applied State only.';
COMMENT ON COLUMN route_b_phase_runs.phase_run_id IS 'Phase-level run identifier for DB-driven multi-role execution.';
COMMENT ON COLUMN route_b_phase_runs.status IS 'Phase state machine status with result/status extension support.';
COMMENT ON COLUMN route_b_phase_runs.authority_boundary_json IS 'PM_L2_ONLY merge authority boundary and no n8n/direct main/force push executor merge authority.';

CREATE TABLE IF NOT EXISTS route_b_role_task_queue (
  phase_run_id text NOT NULL,
  workflow_run_id text NOT NULL,
  role_task_id text NOT NULL,
  parent_role_task_id text,
  sequence_no integer NOT NULL,
  role_id text NOT NULL,
  task_type text NOT NULL,
  task_payload_json jsonb NOT NULL,
  context_refs_json jsonb NOT NULL,
  expected_output_schema_ref text NOT NULL,
  status text NOT NULL,
  attempt integer NOT NULL,
  max_retries integer NOT NULL,
  blocker_code text,
  required_fixes_json jsonb,
  result_json jsonb,
  status_detail text,
  created_at timestamptz NOT NULL,
  claimed_at timestamptz,
  completed_at timestamptz,
  PRIMARY KEY (role_task_id),
  UNIQUE (phase_run_id, sequence_no),
  FOREIGN KEY (phase_run_id) REFERENCES route_b_phase_runs (phase_run_id),
  FOREIGN KEY (parent_role_task_id) REFERENCES route_b_role_task_queue (role_task_id),
  CHECK (role_id IN (
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
  CHECK (status IN (
    'role_task_ready',
    'role_task_running',
    'role_task_completed',
    'role_task_failed',
    'blocked'
  )),
  CHECK (attempt >= 0),
  CHECK (max_retries >= 0),
  CHECK (context_refs_json ? 'static_context_refs'),
  CHECK (context_refs_json ? 'role_context_ref'),
  CHECK (context_refs_json ? 'schema_refs')
);

COMMENT ON TABLE route_b_role_task_queue IS
  'Proposal-only role task queue table. Role tasks are DB rows and role_id values, not separate workflows.';
COMMENT ON COLUMN route_b_role_task_queue.role_task_id IS 'Primary identifier for a PM L3 or SUBCHAT role task.';
COMMENT ON COLUMN route_b_role_task_queue.role_id IS 'Role id must match context_refs_json.role_context_ref.role_id.';
COMMENT ON COLUMN route_b_role_task_queue.status IS 'Role task polling status with result/status extension support.';
COMMENT ON COLUMN route_b_role_task_queue.result_json IS 'Result/status extension support for role_tasks.';
COMMENT ON COLUMN route_b_role_task_queue.status_detail IS 'Human-readable status extension detail for polling and PM L3 validation.';

CREATE TABLE IF NOT EXISTS route_b_role_outputs (
  role_output_id text NOT NULL,
  phase_run_id text NOT NULL,
  workflow_run_id text NOT NULL,
  role_task_id text NOT NULL,
  role_id text NOT NULL,
  output_json jsonb NOT NULL,
  validation_status text NOT NULL,
  result_status text NOT NULL,
  result_json jsonb,
  raw_content_hash text NOT NULL,
  error_message text,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (role_output_id),
  UNIQUE (role_task_id, raw_content_hash),
  FOREIGN KEY (phase_run_id) REFERENCES route_b_phase_runs (phase_run_id),
  FOREIGN KEY (role_task_id) REFERENCES route_b_role_task_queue (role_task_id),
  CHECK (validation_status IN (
    'passed_deterministic_validation',
    'failed_deterministic_validation',
    'blocked',
    'not_required'
  )),
  CHECK (result_status IN (
    'role_output_persisted',
    'role_output_rejected',
    'role_output_blocked',
    'role_output_failed'
  ))
);

COMMENT ON TABLE route_b_role_outputs IS
  'Proposal-only Universal Role Runner output persistence table for strict JSON role outputs.';
COMMENT ON COLUMN route_b_role_outputs.output_json IS 'Strict JSON output persisted for evidence.';
COMMENT ON COLUMN route_b_role_outputs.validation_status IS 'Deterministic validation status for role output persistence.';
COMMENT ON COLUMN route_b_role_outputs.result_status IS 'Result/status extension support for role_outputs.';
COMMENT ON COLUMN route_b_role_outputs.raw_content_hash IS 'Hash of raw model content for durable evidence.';

CREATE TABLE IF NOT EXISTS route_b_pm_l3_decisions (
  decision_id text NOT NULL,
  phase_run_id text NOT NULL,
  workflow_run_id text NOT NULL,
  role_task_id text NOT NULL,
  decision_type text NOT NULL,
  decision_payload_json jsonb NOT NULL,
  authority_boundary jsonb NOT NULL,
  decision_status text NOT NULL,
  result_json jsonb,
  blocker_code text,
  required_fixes_json jsonb,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (decision_id),
  UNIQUE (role_task_id, decision_type, created_at),
  FOREIGN KEY (phase_run_id) REFERENCES route_b_phase_runs (phase_run_id),
  FOREIGN KEY (role_task_id) REFERENCES route_b_role_task_queue (role_task_id),
  CHECK (decision_type IN (
    'create_next_role_task',
    'request_github_execution_package',
    'return_blocker',
    'return_final_pm_l3_evidence_package'
  )),
  CHECK (decision_status IN (
    'decision_recorded',
    'queued_next_role_task',
    'github_execution_requested',
    'return_blocker',
    'final_evidence_returned',
    'blocked',
    'failed'
  )),
  CHECK (authority_boundary->>'merge_authority' = 'PM_L2_ONLY'),
  CHECK (authority_boundary->>'pm_l2_review_required' = 'true'),
  CHECK (authority_boundary->>'pm_l3_final_pm_l2_verdict_allowed' = 'false'),
  CHECK (authority_boundary->>'pm_l3_merge_approval_allowed' = 'false'),
  CHECK (authority_boundary->>'n8n_merge_allowed' = 'false'),
  CHECK (authority_boundary->>'direct_main_write_allowed' = 'false'),
  CHECK (authority_boundary->>'force_push_allowed' = 'false'),
  CHECK (authority_boundary->>'executor_merge_allowed' = 'false')
);

COMMENT ON TABLE route_b_pm_l3_decisions IS
  'Proposal-only PM L3 decision persistence table. PM L2 remains the only closeout and merge authority.';
COMMENT ON COLUMN route_b_pm_l3_decisions.decision_type IS 'Allowed PM L3 decisions: create next role task, request separate PR-only GitHub execution, return blocker, return final PM L3 evidence.';
COMMENT ON COLUMN route_b_pm_l3_decisions.authority_boundary IS 'PM_L2_ONLY boundary; PM L3 cannot approve merge or issue PM L2 final verdict.';
COMMENT ON COLUMN route_b_pm_l3_decisions.decision_status IS 'Result/status extension support for pm_l3_decisions.';
COMMENT ON COLUMN route_b_pm_l3_decisions.result_json IS 'Result/status extension support for PM L3 decision evidence.';

CREATE INDEX IF NOT EXISTS idx_route_b_phase_runs_status_updated_at
  ON route_b_phase_runs (status, updated_at);

CREATE INDEX IF NOT EXISTS idx_route_b_role_task_queue_status_created_at
  ON route_b_role_task_queue (status, created_at);

CREATE INDEX IF NOT EXISTS idx_route_b_role_task_queue_phase_sequence
  ON route_b_role_task_queue (phase_run_id, sequence_no);

CREATE INDEX IF NOT EXISTS idx_route_b_role_task_queue_role_status
  ON route_b_role_task_queue (role_id, status);

CREATE INDEX IF NOT EXISTS idx_route_b_role_outputs_task_status
  ON route_b_role_outputs (role_task_id, validation_status, result_status);

CREATE INDEX IF NOT EXISTS idx_route_b_pm_l3_decisions_type_created_at
  ON route_b_pm_l3_decisions (decision_type, created_at);

CREATE INDEX IF NOT EXISTS idx_route_b_pm_l3_decisions_status_created_at
  ON route_b_pm_l3_decisions (decision_status, created_at);

COMMENT ON COLUMN route_b_phase_runs.github_executor_ref_json IS
  'GitHub executor reference only; GitHub executor remains a separate PR-only executor and is not a merge authority.';
