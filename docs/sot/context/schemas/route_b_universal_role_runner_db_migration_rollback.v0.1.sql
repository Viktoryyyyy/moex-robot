-- schema_id: route_b_universal_role_runner_db_migration_rollback.v0.1
-- project: MOEX Bot
-- status: executable_rollback
-- source_of_truth: github_repo
-- artifact_class: repo_relative
-- artifact_path: docs/sot/context/schemas/route_b_universal_role_runner_db_migration_rollback.v0.1.sql
-- forward_migration_artifact: docs/sot/context/schemas/route_b_universal_role_runner_db_migration_execution.v0.1.sql
-- warning: operator-run rollback only after PM L2 approval.
-- rollback_scope: objects introduced by route_b_universal_role_runner_db_migration_execution.v0.1.sql only.
-- transactional_boundary: explicit_begin_commit

BEGIN;

DROP INDEX IF EXISTS ix_route_b_pm_l3_decisions_workflow_type;
DROP INDEX IF EXISTS ix_route_b_pm_l3_decisions_role_task_created;
DROP INDEX IF EXISTS ix_route_b_pm_l3_decisions_phase_type_created;
DROP INDEX IF EXISTS ix_route_b_role_outputs_phase_workflow_created;
DROP INDEX IF EXISTS ix_route_b_role_outputs_workflow_role;
DROP INDEX IF EXISTS ix_route_b_role_outputs_task_created;
DROP INDEX IF EXISTS ix_route_b_role_task_queue_workflow_status;
DROP INDEX IF EXISTS ix_route_b_role_task_queue_status_claim;
DROP INDEX IF EXISTS ix_route_b_role_task_queue_ready_queue;

DROP TABLE IF EXISTS route_b_pm_l3_decisions;
DROP TABLE IF EXISTS route_b_role_outputs;
DROP TABLE IF EXISTS route_b_role_task_queue;
DROP TABLE IF EXISTS route_b_phase_runs;

COMMIT;
