from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "docs/sot/context/registry.route_b.v1.yaml"
SQL_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_universal_role_runner_db_migration.v0.1.sql"

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
    "created_at",
    "claimed_at",
    "completed_at",
}

ROLE_OUTPUT_COLUMNS = {
    "phase_run_id",
    "workflow_run_id",
    "role_task_id",
    "role_id",
    "output_json",
    "validation_status",
    "raw_content_hash",
    "created_at",
}

PM_L3_DECISION_COLUMNS = {
    "phase_run_id",
    "workflow_run_id",
    "role_task_id",
    "decision_type",
    "decision_payload_json",
    "created_at",
}

SUPPORTED_ROLE_IDS = {
    "PM_L3_DELIVERY_VALIDATION_OWNER",
    "SUBCHAT_REPO_AUDIT",
    "SUBCHAT_SPEC_CONTRACT_DESIGNER",
    "SUBCHAT_IMPLEMENTATION",
    "SUBCHAT_VALIDATION",
    "SUBCHAT_DATA_STEWARD",
    "SUBCHAT_EXPERIMENT_DESIGNER",
    "SUBCHAT_RESEARCH_CRITIC",
    "SUBCHAT_RESEARCH_EXECUTION",
}

ROLE_TASK_STATUSES = {
    "role_task_ready",
    "role_task_running",
    "role_task_completed",
    "role_task_failed",
    "blocked",
}

PM_L3_DECISION_TYPES = {
    "create_next_role_task",
    "request_github_execution_package",
    "return_blocker",
    "return_final_pm_l3_evidence_package",
}

FORBIDDEN_SQL_PATTERNS = (
    r"\bdrop\s+table\b",
    r"\bdrop\s+view\b",
    r"\btruncate\b",
    r"\bdelete\s+from\b",
    r"\balter\s+table\b.*\bdrop\b",
)

FORBIDDEN_PATH_OR_RESOLVER_MARKERS = (
    "/home/",
    "/mnt/",
    "/tmp/",
    "/var/",
    "/root/",
    "/srv/",
    " latest ",
    " current ",
    " autodetect ",
)


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _sql_lower_spaced() -> str:
    return " " + re.sub(r"\s+", " ", _sql().lower()) + " "


def _table_block(table_name: str) -> str:
    text = _sql()
    pattern = f"create table if not exists {table_name} ("
    start = text.lower().find(pattern)
    assert start >= 0, table_name
    end = text.find("\n);", start)
    assert end > start, table_name
    return text[start : end + 3]


def test_sql_migration_artifact_is_repo_proposal_only() -> None:
    text = _sql()
    assert "-- artifact_id: route_b_universal_role_runner_db_migration.v0.1" in text
    assert "-- artifact_class: repo_relative" in text
    assert "-- artifact_path: docs/sot/context/schemas/route_b_universal_role_runner_db_migration.v0.1.sql" in text
    assert "-- producer: PM_L2_PHASE_OWNER" in text
    assert "-- consumer: PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER" in text
    assert "-- format: sql" in text
    assert "-- proposal_only: true" in text
    assert "-- production_migration_executed: false" in text
    assert "-- server_apply_executed: false" in text
    assert "-- executed_db_changes: false" in text


def test_sql_registers_required_route_b_tables() -> None:
    lowered = _sql().lower()
    for table in REQUIRED_TABLES:
        assert f"create table if not exists {table}" in lowered


def test_role_task_queue_columns_and_constraints_follow_contract() -> None:
    block = _table_block("route_b_role_task_queue").lower()
    for column in ROLE_TASK_QUEUE_COLUMNS:
        assert re.search(r"\b" + re.escape(column) + r"\b", block), column
    assert "role_task_id text primary key" in block
    assert "route_b_role_task_queue_phase_fk" in block
    assert "route_b_role_task_queue_parent_fk" in block
    assert "route_b_role_task_queue_phase_sequence_uk unique (phase_run_id, sequence_no)" in block
    assert "route_b_role_task_queue_role_id_ck" in block
    assert "route_b_role_task_queue_status_ck" in block


def test_role_task_queue_allows_required_roles_and_statuses() -> None:
    block = _table_block("route_b_role_task_queue")
    for role_id in SUPPORTED_ROLE_IDS:
        assert f"'{role_id}'" in block
    for status in ROLE_TASK_STATUSES:
        assert f"'{status}'" in block


def test_role_task_queue_has_polling_and_phase_indexes() -> None:
    text = _sql().lower()
    assert "create index if not exists route_b_role_task_queue_poll_ready_idx" in text
    assert "where status = 'role_task_ready'" in text
    assert "create index if not exists route_b_role_task_queue_phase_status_idx" in text
    assert "create index if not exists route_b_role_task_queue_parent_idx" in text


def test_role_outputs_columns_constraints_and_indexes_follow_contract() -> None:
    block = _table_block("route_b_role_outputs").lower()
    for column in ROLE_OUTPUT_COLUMNS:
        assert re.search(r"\b" + re.escape(column) + r"\b", block), column
    assert "role_task_id text primary key" in block
    assert "route_b_role_outputs_phase_fk" in block
    assert "route_b_role_outputs_role_task_fk" in block
    assert "route_b_role_outputs_validation_status_ck" in block
    assert "route_b_role_outputs_output_object_ck" in block
    text = _sql().lower()
    assert "create index if not exists route_b_role_outputs_phase_role_idx" in text
    assert "create index if not exists route_b_role_outputs_validation_status_idx" in text


def test_pm_l3_decision_columns_constraints_and_indexes_follow_contract() -> None:
    block = _table_block("route_b_pm_l3_decisions").lower()
    for column in PM_L3_DECISION_COLUMNS:
        assert re.search(r"\b" + re.escape(column) + r"\b", block), column
    assert "pm_l3_decision_id text primary key" in block
    assert "route_b_pm_l3_decisions_phase_fk" in block
    assert "route_b_pm_l3_decisions_role_task_fk" in block
    assert "route_b_pm_l3_decisions_type_ck" in block
    assert "route_b_pm_l3_decisions_payload_object_ck" in block
    for decision_type in PM_L3_DECISION_TYPES:
        assert f"'{decision_type}'" in block
    text = _sql().lower()
    assert "create index if not exists route_b_pm_l3_decisions_phase_type_idx" in text
    assert "create index if not exists route_b_pm_l3_decisions_role_task_idx" in text


def test_result_status_extension_view_exposes_required_sections() -> None:
    text = _sql().lower()
    assert "create or replace view route_b_result_status_extension_v0_1" in text
    assert "phase_run" in text
    assert "role_tasks" in text
    assert "role_outputs" in text
    assert "pm_l3_decisions" in text
    assert "from route_b_role_task_queue" in text
    assert "from route_b_role_outputs" in text
    assert "from route_b_pm_l3_decisions" in text


def test_sql_contains_no_destructive_or_implicit_path_patterns() -> None:
    lowered = _sql_lower_spaced()
    for pattern in FORBIDDEN_SQL_PATTERNS:
        assert not re.search(pattern, lowered), pattern
    for marker in FORBIDDEN_PATH_OR_RESOLVER_MARKERS:
        assert marker not in lowered, marker
