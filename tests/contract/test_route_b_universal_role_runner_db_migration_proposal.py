from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_proposal.v0.1.sql"
)

REQUIRED_TABLES = {
    "route_b_phase_runs",
    "route_b_role_tasks",
    "route_b_role_outputs",
    "route_b_pm_l3_decisions",
}

ROLE_TASK_COLUMNS = {
    "phase_run_id",
    "workflow_run_id",
    "role_task_id",
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
}

ROLE_OUTPUT_COLUMNS = {
    "role_output_id",
    "role_task_id",
    "workflow_run_id",
    "role_id",
    "output_json",
    "output_schema_ref",
    "validation_status",
    "validation_errors_json",
    "created_at",
}

PM_L3_DECISION_COLUMNS = {
    "pm_l3_decision_id",
    "phase_run_id",
    "workflow_run_id",
    "source_role_task_id",
    "decision_type",
    "next_role_id",
    "next_task_payload_json",
    "github_execution_request_json",
    "blocker_code",
    "final_pm_l3_package_json",
    "created_at",
}

ROLE_TASK_LIFECYCLE_STATUSES = {
    "role_task_ready",
    "role_task_running",
    "role_task_completed",
    "role_task_failed",
    "blocked",
}

FORBIDDEN_SQL_PATTERNS = (
    r"\bDROP\s+TABLE\b",
    r"\bDROP\s+DATABASE\b",
    r"\bTRUNCATE\b",
    r"\bDELETE\s+FROM\b",
    r"\bALTER\s+SYSTEM\b",
    r"\bCREATE\s+EXTENSION\b",
)


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _table_block(sql: str, table_name: str) -> str:
    match = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+"
        + re.escape(table_name)
        + r"\s*\((.*?)\n\);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match, table_name
    return match.group(1)


def _assert_columns(block: str, columns: set[str]) -> None:
    for column in columns:
        assert re.search(r"^\s*" + re.escape(column) + r"\s+", block, re.MULTILINE), column


def test_sql_proposal_file_exists() -> None:
    assert SQL_PATH.is_file()


def test_sql_contains_proposal_only_no_server_apply_no_execute_notices() -> None:
    sql = _sql()
    required_notices = (
        "proposal_only: true",
        "production_execution_allowed: false",
        "no_production_execution_notice:",
        "server_apply_allowed: false",
        "no_server_apply_notice:",
        "migration_execution_allowed: false",
        "no_migration_execution_notice:",
        "n8n_production_workflow_mutation_allowed: false",
        "no_n8n_production_workflow_mutation_notice:",
        "merge_authority_change_allowed: false",
        "no_merge_authority_change_notice:",
        "does_not_imply_execution_on_production_db: true",
    )
    for notice in required_notices:
        assert notice in sql


def test_sql_defines_required_proposed_tables() -> None:
    sql = _sql()
    for table in REQUIRED_TABLES:
        assert re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+" + re.escape(table) + r"\b",
            sql,
            flags=re.IGNORECASE,
        ), table


def test_sql_contains_required_role_task_columns() -> None:
    _assert_columns(_table_block(_sql(), "route_b_role_tasks"), ROLE_TASK_COLUMNS)


def test_sql_contains_required_role_output_columns() -> None:
    _assert_columns(_table_block(_sql(), "route_b_role_outputs"), ROLE_OUTPUT_COLUMNS)


def test_sql_contains_required_pm_l3_decision_columns() -> None:
    _assert_columns(_table_block(_sql(), "route_b_pm_l3_decisions"), PM_L3_DECISION_COLUMNS)


def test_sql_contains_lifecycle_status_constraints() -> None:
    sql = _sql()
    assert "ck_route_b_role_tasks_status_lifecycle" in sql
    assert "ck_route_b_role_tasks_lifecycle_timestamps" in sql
    for status in ROLE_TASK_LIFECYCLE_STATUSES:
        assert re.search(r"'" + re.escape(status) + r"'", sql), status


def test_sql_contains_deterministic_queue_indexes_and_constraints() -> None:
    sql = _sql()
    assert "uq_route_b_role_tasks_phase_sequence" in sql
    assert "ix_route_b_role_tasks_ready_queue" in sql
    assert "ix_route_b_role_tasks_status_claim" in sql
    assert "phase_run_id, sequence_no, created_at, role_task_id" in sql


def test_sql_contains_no_destructive_operations() -> None:
    sql = _sql()
    for pattern in FORBIDDEN_SQL_PATTERNS:
        assert not re.search(pattern, sql, flags=re.IGNORECASE), pattern


def test_sql_does_not_imply_production_db_execution() -> None:
    sql = _sql().lower()
    assert "production_execution_allowed: false" in sql
    assert "migration_execution_allowed: false" in sql
    assert "does_not_imply_execution_on_production_db: true" in sql
    assert "this file proposes future persistence shape only" in sql


def test_sql_includes_github_repo_source_of_truth_boundary() -> None:
    sql = _sql()
    assert "GitHub / repo is the Source of Truth" in sql
    assert "Server is Applied State only" in sql
    assert "server is not architectural proof" in sql
