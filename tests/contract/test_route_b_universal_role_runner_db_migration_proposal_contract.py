from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_REL_PATH = "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_proposal.v0.1.sql"
SQL_PATH = REPO_ROOT / SQL_REL_PATH
REGISTRY_PATH = REPO_ROOT / "docs/sot/context/registry.route_b.v1.yaml"
SCHEMA_REF = "route_b_universal_role_runner_db_migration_proposal.v0.1"

REQUIRED_HEADER_FIELDS = {
    "artifact_id": "route_b_universal_role_runner_db_migration_proposal.v0.1",
    "artifact_class": "repo_relative",
    "producer": "PM_L2_PHASE_OWNER",
    "consumer": "PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER",
    "format": "postgresql_sql_proposal_only",
    "repo_relative_path": SQL_REL_PATH,
    "proposal_only": "true",
    "executable_by_repo_package": "false",
    "production_migration_executed": "false",
    "server_apply_allowed": "false",
    "db_execution_allowed": "false",
    "n8n_mutation_allowed": "false",
    "merge_authority": "PM_L2_ONLY",
}

REQUIRED_TABLES = {
    "route_b_phase_runs",
    "route_b_role_task_queue",
    "route_b_role_outputs",
    "route_b_pm_l3_decisions",
}

ROLE_TASK_QUEUE_COLUMNS = {
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
    "required_fixes_json",
    "result_json",
    "status_detail",
    "created_at",
    "claimed_at",
    "completed_at",
}

ROLE_OUTPUT_COLUMNS = {
    "role_output_id",
    "phase_run_id",
    "workflow_run_id",
    "role_task_id",
    "role_id",
    "output_json",
    "validation_status",
    "result_status",
    "result_json",
    "raw_content_hash",
    "error_message",
    "created_at",
}

PM_L3_DECISION_COLUMNS = {
    "decision_id",
    "phase_run_id",
    "workflow_run_id",
    "role_task_id",
    "decision_type",
    "decision_payload_json",
    "authority_boundary",
    "decision_status",
    "result_json",
    "blocker_code",
    "required_fixes_json",
    "created_at",
}

DESTRUCTIVE_SQL_PATTERNS = (
    "DROP TABLE",
    "DROP COLUMN",
    "TRUNCATE",
    "DELETE FROM",
    "ALTER TABLE DROP",
    "CASCADE",
    "CREATE DATABASE",
    "CREATE EXTENSION",
    "GRANT",
    "REVOKE",
    "SECURITY DEFINER",
    "dblink",
    "postgres_fdw",
    "COPY FROM PROGRAM",
)


def _sql_text() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _registry() -> dict[str, object]:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _table_body(sql: str, table: str) -> str:
    start_marker = f"CREATE TABLE IF NOT EXISTS {table} ("
    end_marker = f"\n);\n\nCOMMENT ON TABLE {table} IS"
    start = sql.index(start_marker) + len(start_marker)
    end = sql.index(end_marker, start)
    return sql[start:end]


def _assert_columns_present(table_body: str, columns: set[str]) -> None:
    for column in columns:
        assert re.search(r"^\s*" + re.escape(column) + r"\s+", table_body, re.MULTILINE), column


def test_sql_proposal_artifact_exists_at_exact_repo_relative_path() -> None:
    assert SQL_PATH.is_file()
    assert SQL_PATH.as_posix().endswith(SQL_REL_PATH)


def test_registry_schema_ref_points_to_exact_sql_path() -> None:
    registry = _registry()
    schema_refs = registry["schema_refs"]
    assert isinstance(schema_refs, dict)
    entry = schema_refs[SCHEMA_REF]
    assert isinstance(entry, dict)
    assert entry["path"] == SQL_REL_PATH
    assert entry["producer"] == "PM_L2_PHASE_OWNER"
    assert entry["consumer"] == "PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER"


def test_artifact_header_contains_required_metadata_fields() -> None:
    text = _sql_text()
    for key, value in REQUIRED_HEADER_FIELDS.items():
        assert f"-- {key}: {value}" in text


def test_proposal_only_and_no_execution_guardrails_are_present() -> None:
    text = _sql_text()
    required = (
        "-- proposal_only: true",
        "-- executable_by_repo_package: false",
        "-- production_migration_executed: false",
        "-- server_apply_allowed: false",
        "-- db_execution_allowed: false",
        "-- n8n_mutation_allowed: false",
        "-- pm_l2_only_merge_authority_boundary: true",
    )
    for marker in required:
        assert marker in text


def test_required_tables_are_present() -> None:
    text = _sql_text()
    for table in REQUIRED_TABLES:
        assert re.search(r"^CREATE TABLE IF NOT EXISTS " + table + r"\s*\(", text, re.MULTILINE), table
        assert f"COMMENT ON TABLE {table}" in text


def test_required_role_task_queue_columns_are_present() -> None:
    body = _table_body(_sql_text(), "route_b_role_task_queue")
    _assert_columns_present(body, ROLE_TASK_QUEUE_COLUMNS)


def test_required_role_outputs_columns_are_present() -> None:
    body = _table_body(_sql_text(), "route_b_role_outputs")
    _assert_columns_present(body, ROLE_OUTPUT_COLUMNS)


def test_required_pm_l3_decisions_columns_are_present() -> None:
    body = _table_body(_sql_text(), "route_b_pm_l3_decisions")
    _assert_columns_present(body, PM_L3_DECISION_COLUMNS)


def test_result_status_extension_terms_are_present() -> None:
    text = _sql_text()
    required_terms = (
        "result_status_extension_support_role_tasks",
        "result_status_extension_support_role_outputs",
        "result_status_extension_support_pm_l3_decisions",
        "result_json",
        "status_detail",
        "result_status",
        "decision_status",
        "required_fixes_json",
    )
    for term in required_terms:
        assert term in text


def test_keys_foreign_keys_status_constraints_and_polling_indexes_are_represented() -> None:
    text = _sql_text()
    for required in ("PRIMARY KEY", "UNIQUE", "FOREIGN KEY", "CHECK"):
        assert required in text
    for status in (
        "role_task_ready",
        "role_task_running",
        "role_task_completed",
        "role_task_failed",
        "blocked",
    ):
        assert status in text
    for decision_type in (
        "create_next_role_task",
        "request_github_execution_package",
        "return_blocker",
        "return_final_pm_l3_evidence_package",
    ):
        assert decision_type in text
    for index_name in (
        "idx_route_b_phase_runs_status_updated_at",
        "idx_route_b_role_task_queue_status_created_at",
        "idx_route_b_role_task_queue_phase_sequence",
        "idx_route_b_role_task_queue_role_status",
        "idx_route_b_role_outputs_task_status",
        "idx_route_b_pm_l3_decisions_type_created_at",
        "idx_route_b_pm_l3_decisions_status_created_at",
    ):
        assert f"CREATE INDEX IF NOT EXISTS {index_name}" in text


def test_latest_current_autodetect_markers_are_absent_from_sql_artifact() -> None:
    assert not re.search(r"\b(latest|current|autodetect)\b", _sql_text(), re.IGNORECASE)


def test_absolute_server_paths_are_absent_from_sql_artifact() -> None:
    text = _sql_text()
    assert not re.search(r"(^|[\s\"'`])(/home/|/mnt/|/tmp/|/var/|/root/|/srv/|/opt/|~/|file://|[A-Za-z]:[\\/])", text)


def test_destructive_sql_patterns_are_absent() -> None:
    sql_lower = _sql_text().lower()
    for pattern in DESTRUCTIVE_SQL_PATTERNS:
        assert pattern.lower() not in sql_lower, pattern


def test_pm_l2_only_merge_authority_and_github_executor_boundary_are_present() -> None:
    text = _sql_text()
    assert "PM_L2_ONLY" in text
    assert "github_executor_remains_separate_pr_only_executor: true" in text
    assert "separate PR-only GitHub execution" in text
    assert "PM L3 cannot approve merge" in text


def test_github_source_of_truth_and_server_applied_state_boundary_is_present() -> None:
    text = _sql_text()
    assert "-- github_source_of_truth_boundary: true" in text
    assert "-- server_applied_state_only: true" in text
    assert "GitHub repo is Source of Truth; server is Applied State only" in text
