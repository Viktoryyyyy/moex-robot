from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "docs/sot/context/registry.route_b.v1.yaml"
SQL_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_universal_role_runner_db_schema.v0.1.sql"
DB_CONTRACT_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_universal_role_runner_db_contract.v0.1.yaml"
ROLE_TASK_CONTRACT_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_role_task_queue.v0.1.yaml"
PM_L3_DECISION_CONTRACT_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_pm_l3_decision_loop.v0.1.yaml"

ROLE_IDS = {
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

REQUIRED_TABLE_COLUMNS = {
    "route_b_phase_runs": {
        "phase_run_id",
        "workflow_run_id",
        "status",
        "created_at",
        "updated_at",
    },
    "route_b_role_task_queue": {
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
    },
    "route_b_role_outputs": {
        "phase_run_id",
        "workflow_run_id",
        "role_task_id",
        "role_id",
        "output_json",
        "validation_status",
        "raw_content_hash",
        "created_at",
    },
    "route_b_pm_l3_decisions": {
        "phase_run_id",
        "workflow_run_id",
        "role_task_id",
        "decision_type",
        "decision_payload_json",
        "authority_boundary",
        "created_at",
    },
}

FORBIDDEN_SQL_PATTERNS = (
    r"\bdrop\s+",
    r"\btruncate\s+",
    r"\bdelete\s+from\b",
    r"\balter\s+table\b",
    r"\binsert\s+into\b",
    r"\bupdate\s+[a-z_]",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _sql() -> str:
    return _read(SQL_PATH)


def _contract_text() -> str:
    return "\n".join(
        [
            _read(DB_CONTRACT_PATH),
            _read(ROLE_TASK_CONTRACT_PATH),
            _read(PM_L3_DECISION_CONTRACT_PATH),
        ]
    )


def _registry() -> dict[str, object]:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _table_body(sql: str, table_name: str) -> str:
    match = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+"
        + re.escape(table_name)
        + r"\s*\((.*?)\n\);",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    assert match, table_name
    return match.group(1)


def test_db_schema_artifact_is_registered_repo_relative_and_proposal_only() -> None:
    registry = _registry()
    schema_refs = registry["schema_refs"]
    assert isinstance(schema_refs, dict)
    ref = "route_b_universal_role_runner_db_schema.v0.1"
    assert ref in schema_refs
    entry = schema_refs[ref]
    assert isinstance(entry, dict)
    assert entry["path"] == "docs/sot/context/schemas/route_b_universal_role_runner_db_schema.v0.1.sql"
    assert entry["producer"] == "PM_L2_PHASE_OWNER"
    assert entry["consumer"] == "PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER"
    assert SQL_PATH.is_file()

    sql = _sql()
    assert "-- artifact_class: repo_relative" in sql
    assert "-- proposal_only: true" in sql
    assert "-- production_migration_executed: false" in sql
    assert "-- server_apply_executed: false" in sql
    assert "-- executed_db_changes: false" in sql


def test_sql_represents_required_route_b_tables_and_columns_from_contracts() -> None:
    sql = _sql()
    contract_text = _contract_text()

    for table_name, columns in REQUIRED_TABLE_COLUMNS.items():
        assert table_name in contract_text, table_name
        body = _table_body(sql, table_name)
        for column in columns:
            assert re.search(r"(^|\n)\s*" + re.escape(column) + r"\s+", body), (table_name, column)


def test_role_task_queue_keys_status_constraints_and_context_refs_are_explicit() -> None:
    sql = _sql()
    body = _table_body(sql, "route_b_role_task_queue")

    assert re.search(r"role_task_id\s+text\s+PRIMARY\s+KEY", body, re.IGNORECASE)
    assert "UNIQUE (phase_run_id, sequence_no)" in body
    assert "REFERENCES route_b_phase_runs(phase_run_id)" in body
    assert "REFERENCES route_b_role_task_queue(role_task_id)" in body

    for role_id in ROLE_IDS:
        assert "'" + role_id + "'" in body, role_id

    for status in ROLE_TASK_STATUSES:
        assert "'" + status + "'" in body, status

    for required_child in ("static_context_refs", "role_context_ref", "schema_refs"):
        assert "context_refs_json ? '" + required_child + "'" in body


def test_role_outputs_and_pm_l3_decisions_have_persistence_constraints() -> None:
    sql = _sql()
    role_outputs = _table_body(sql, "route_b_role_outputs")
    decisions = _table_body(sql, "route_b_pm_l3_decisions")

    assert re.search(r"role_task_id\s+text\s+PRIMARY\s+KEY\s+REFERENCES\s+route_b_role_task_queue", role_outputs, re.IGNORECASE)
    assert "output_json jsonb NOT NULL" in role_outputs
    assert "validation_status text NOT NULL CHECK" in role_outputs
    assert "raw_content_hash text NOT NULL" in role_outputs

    assert "PRIMARY KEY (phase_run_id, role_task_id, decision_type, created_at)" in decisions
    assert "decision_payload_json jsonb NOT NULL" in decisions
    assert "authority_boundary jsonb NOT NULL CHECK" in decisions
    for decision_type in PM_L3_DECISION_TYPES:
        assert "'" + decision_type + "'" in decisions, decision_type
    for boundary_fragment in (
        "authority_boundary->>'merge_authority' = 'PM_L2_ONLY'",
        "authority_boundary->>'pm_l2_review_required' = 'true'",
        "authority_boundary->>'n8n_merge_allowed' = 'false'",
        "authority_boundary->>'direct_main_write_allowed' = 'false'",
        "authority_boundary->>'force_push_allowed' = 'false'",
        "authority_boundary->>'file_delete_allowed' = 'false'",
        "authority_boundary->>'server_apply_allowed' = 'false'",
    ):
        assert boundary_fragment in decisions


def test_polling_indexes_and_result_status_extension_view_are_represented() -> None:
    sql = _sql()

    for index_name in (
        "idx_route_b_role_task_queue_poll_ready",
        "idx_route_b_role_task_queue_phase_status",
        "idx_route_b_role_task_queue_workflow_status",
        "idx_route_b_role_outputs_phase_role",
        "idx_route_b_role_outputs_workflow_validation",
        "idx_route_b_pm_l3_decisions_phase_type",
        "idx_route_b_pm_l3_decisions_workflow_type",
    ):
        assert "CREATE INDEX IF NOT EXISTS " + index_name in sql

    assert "WHERE status = 'role_task_ready'" in sql
    assert "CREATE VIEW route_b_workflow_result_status_extension_v0_1 AS" in sql
    for result_key in ("phase_run", "role_tasks", "role_outputs", "pm_l3_decisions", "authority_boundary", "blockers", "required_fixes"):
        assert "'" + result_key + "'" in sql, result_key


def test_schema_artifact_contains_no_destructive_or_dynamic_path_sql() -> None:
    sql = _sql()
    lowered = sql.lower()

    for pattern in FORBIDDEN_SQL_PATTERNS:
        assert not re.search(pattern, lowered), pattern

    normalized = " " + re.sub(r"[^a-z0-9_]+", " ", lowered) + " "
    for marker in ("latest", "current", "autodetect"):
        assert " " + marker + " " not in normalized

    for forbidden_path_fragment in ("/home/", "/tmp/", "/mnt/", "/var/", "/root/", "/srv/", "/opt/", "file://", "~/"):
        assert forbidden_path_fragment not in lowered
