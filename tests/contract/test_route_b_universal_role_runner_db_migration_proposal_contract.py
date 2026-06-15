from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "docs/sot/context/registry.route_b.v1.yaml"
SQL_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_proposal.v0.1.sql"

SCHEMA_REF = "route_b_universal_role_runner_db_migration_proposal.v0.1"
SQL_REPO_PATH = "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_proposal.v0.1.sql"

REQUIRED_METADATA = {
    "artifact_id": "route_b_universal_role_runner_db_migration_proposal.v0.1",
    "artifact_class": "repo_relative",
    "producer": "PM_L2_PHASE_OWNER",
    "consumer": "PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER",
    "format": "postgresql_sql_proposal_only",
    "repo_relative_path": SQL_REPO_PATH,
}

REQUIRED_TABLES = (
    "route_b_phase_runs",
    "route_b_role_task_queue",
    "route_b_role_outputs",
    "route_b_pm_l3_decisions",
)

ROLE_TASK_COLUMNS = (
    "role_task_id",
    "phase_run_id",
    "workflow_run_id",
    "parent_role_task_id",
    "sequence_no",
    "role_id",
    "task_type",
    "task_payload_json",
    "context_refs_json",
    "expected_output_schema_ref",
    "status",
    "attempt",
    "max_retries",
    "blocker_code",
    "created_at",
    "claimed_at",
    "completed_at",
)

ROLE_OUTPUT_COLUMNS = (
    "role_output_id",
    "phase_run_id",
    "workflow_run_id",
    "role_task_id",
    "role_id",
    "output_json",
    "validation_status",
    "raw_content_hash",
    "created_at",
)

PM_L3_DECISION_COLUMNS = (
    "pm_l3_decision_id",
    "phase_run_id",
    "workflow_run_id",
    "role_task_id",
    "decision_type",
    "decision_payload_json",
    "authority_boundary_json",
    "created_at",
)

REQUIRED_ROLE_IDS = (
    "PM_L3_DELIVERY_VALIDATION_OWNER",
    "SUBCHAT_REPO_AUDIT",
    "SUBCHAT_SPEC_CONTRACT_DESIGNER",
    "SUBCHAT_IMPLEMENTATION",
    "SUBCHAT_VALIDATION",
    "SUBCHAT_DATA_STEWARD",
    "SUBCHAT_EXPERIMENT_DESIGNER",
    "SUBCHAT_RESEARCH_CRITIC",
    "SUBCHAT_RESEARCH_EXECUTION",
)

ROLE_TASK_STATUSES = (
    "role_task_ready",
    "role_task_running",
    "role_task_completed",
    "role_task_failed",
    "blocked",
)

PM_L3_DECISION_TYPES = (
    "create_next_role_task",
    "request_github_execution_package",
    "return_blocker",
    "return_final_pm_l3_evidence_package",
)

FORBIDDEN_SQL_PATTERNS = (
    r"\bDROP\b",
    r"\bTRUNCATE\b",
    r"\bDELETE\s+FROM\b",
    r"\bALTER\s+TABLE\b",
    r"\bEXECUTE\b",
)

FORBIDDEN_REF_MARKER_PATTERNS = (
    r"(?<![A-Za-z0-9_])latest(?![A-Za-z0-9_])",
    r"(?<![A-Za-z0-9_])current(?![A-Za-z0-9_])",
    r"(?<![A-Za-z0-9_])autodetect(?![A-Za-z0-9_])",
)

FORBIDDEN_PATH_PATTERNS = (
    r"(^|[\s'\"`])/(home|tmp|mnt|var|root|srv|opt)/",
    r"(^|[\s'\"`])~/",
    r"file://",
    r"[A-Za-z]:[\\/]",
)


def _registry() -> dict[str, object]:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _assert_column(sql: str, column_name: str) -> None:
    assert re.search(r"^\s+" + re.escape(column_name) + r"\s+", sql, re.MULTILINE), column_name


def test_registry_binds_route_b_db_migration_proposal_sql_ref() -> None:
    registry = _registry()
    schema_refs = registry["schema_refs"]
    assert isinstance(schema_refs, dict)
    assert SCHEMA_REF in schema_refs

    entry = schema_refs[SCHEMA_REF]
    assert isinstance(entry, dict)
    assert entry["path"] == SQL_REPO_PATH
    assert entry["producer"] == "PM_L2_PHASE_OWNER"
    assert entry["consumer"] == "PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER"


def test_sql_proposal_artifact_exists_and_declares_metadata() -> None:
    assert SQL_PATH.is_file()
    sql = _sql()

    for key, value in REQUIRED_METADATA.items():
        assert f"-- {key}: {value}" in sql

    assert "-- proposal_only: true" in sql
    assert "-- executable_by_repo_package: false" in sql
    assert "-- production_migration_executed: false" in sql
    assert "-- server_apply_allowed: false" in sql
    assert "-- db_execution_allowed: false" in sql
    assert "-- n8n_mutation_allowed: false" in sql
    assert "-- merge_authority: PM_L2_ONLY" in sql
    assert "-- github_repo_source_of_truth: true" in sql
    assert "-- server_applied_state_only: true" in sql


def test_sql_proposal_has_required_route_b_tables() -> None:
    sql = _sql()

    for table in REQUIRED_TABLES:
        assert re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+" + re.escape(table) + r"\s*\(",
            sql,
            re.IGNORECASE,
        ), table

    assert "phase_run_id text PRIMARY KEY" in sql
    assert "role_task_id text PRIMARY KEY" in sql
    assert "role_output_id text PRIMARY KEY" in sql
    assert "pm_l3_decision_id text PRIMARY KEY" in sql


def test_role_task_queue_columns_constraints_and_indexes_are_represented() -> None:
    sql = _sql()

    for column in ROLE_TASK_COLUMNS:
        _assert_column(sql, column)

    for role_id in REQUIRED_ROLE_IDS:
        assert f"'{role_id}'" in sql

    for status in ROLE_TASK_STATUSES:
        assert f"'{status}'" in sql

    assert "REFERENCES route_b_phase_runs (phase_run_id)" in sql
    assert "REFERENCES route_b_role_task_queue (role_task_id)" in sql
    assert "CHECK (role_id IN" in sql
    assert "CHECK (status IN" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS route_b_role_task_queue_phase_sequence_uidx" in sql
    assert "CREATE INDEX IF NOT EXISTS route_b_role_task_queue_ready_poll_idx" in sql
    assert "WHERE status = 'role_task_ready'" in sql


def test_role_output_persistence_columns_constraints_and_indexes_are_represented() -> None:
    sql = _sql()

    for column in ROLE_OUTPUT_COLUMNS:
        _assert_column(sql, column)

    assert "output_json jsonb NOT NULL" in sql
    assert "validation_status text NOT NULL CHECK" in sql
    assert "raw_content_hash text NOT NULL" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS route_b_role_outputs_task_hash_uidx" in sql


def test_pm_l3_decision_persistence_columns_and_allowed_decisions_are_represented() -> None:
    sql = _sql()

    for column in PM_L3_DECISION_COLUMNS:
        _assert_column(sql, column)

    for decision_type in PM_L3_DECISION_TYPES:
        assert f"'{decision_type}'" in sql

    assert "decision_type text NOT NULL CHECK" in sql
    assert "authority_boundary_json jsonb NOT NULL" in sql
    assert "CREATE INDEX IF NOT EXISTS route_b_pm_l3_decisions_task_idx" in sql


def test_result_status_extension_support_is_represented_without_runtime_apply() -> None:
    sql = _sql()

    assert "CREATE VIEW route_b_result_status_extension AS" in sql
    assert "'phase_run'" in sql
    assert "'role_tasks'" in sql
    assert "'role_outputs'" in sql
    assert "'pm_l3_decisions'" in sql
    assert "'blockers'" in sql
    assert "'required_fixes'" in sql
    assert "'authority_boundary'" in sql
    assert "'PM_L2_ONLY'" in sql
    assert "'ci_passed_is_not_merge_approval'" in sql


def test_sql_proposal_rejects_destructive_sql_and_dynamic_or_server_paths() -> None:
    sql = _sql()

    for pattern in FORBIDDEN_SQL_PATTERNS:
        assert not re.search(pattern, sql, re.IGNORECASE), pattern

    for pattern in FORBIDDEN_REF_MARKER_PATTERNS:
        assert not re.search(pattern, sql, re.IGNORECASE), pattern

    for pattern in FORBIDDEN_PATH_PATTERNS:
        assert not re.search(pattern, sql), pattern

    assert "/home/" not in sql
    assert "/mnt/" not in sql
    assert "/tmp/" not in sql


def test_sql_proposal_preserves_route_b_authority_boundaries() -> None:
    sql = _sql()

    assert "-- direct_main_write_allowed: false" in sql
    assert "-- force_push_allowed: false" in sql
    assert "-- file_delete_allowed: false" in sql
    assert "-- executor_merge_allowed: false" in sql
    assert "-- runtime_live_trading_allowed: false" in sql
    assert "-- broker_execution_allowed: false" in sql
    assert "-- auth_secrets_scope_allowed: false" in sql
    assert "approved_for_merge: true" not in sql
    assert "merge_performed_by_executor: true" not in sql
    assert "pm_l2_approval_claimed_by_executor: true" not in sql
