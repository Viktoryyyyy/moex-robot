from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_universal_role_runner_db_schema.v0.1.sql"


REQUIRED_TABLES = (
    "route_b_phase_runs",
    "route_b_role_task_queue",
    "route_b_role_outputs",
    "route_b_pm_l3_decisions",
)

ROLE_TASK_FIELDS = (
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
)

ROLE_OUTPUT_FIELDS = (
    "phase_run_id",
    "workflow_run_id",
    "role_task_id",
    "role_id",
    "output_json",
    "validation_status",
    "raw_content_hash",
    "created_at",
)

PM_L3_DECISION_FIELDS = (
    "phase_run_id",
    "workflow_run_id",
    "role_task_id",
    "decision_type",
    "decision_payload_json",
    "authority_boundary",
    "created_at",
)

SUPPORTED_ROLE_IDS = (
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
    r"\bdrop\b",
    r"\btruncate\b",
    r"\bdelete\s+from\b",
    r"\balter\s+table\b",
    r"\bcreate\s+database\b",
    r"\bgrant\b",
    r"\brevoke\b",
)

FORBIDDEN_PATH_MARKERS = ("latest", "current", "autodetect")


def _sql_text() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _normalized_sql() -> str:
    return re.sub(r"\s+", " ", _sql_text().lower())


def _table_block(table_name: str) -> str:
    text = _sql_text()
    pattern = (
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+"
        + re.escape(table_name)
        + r"\s*\((.*?)\);"
    )
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    assert match, table_name
    return match.group(1)


def test_route_b_universal_role_runner_sql_artifact_exists() -> None:
    assert SQL_PATH.is_file()


def test_sql_artifact_declares_repo_contract_metadata_and_no_execution() -> None:
    text = _sql_text()
    required_metadata = (
        "artifact_id: route_b_universal_role_runner_db_schema.v0.1",
        "artifact_class: repo_relative",
        "producer: PM_L2_PHASE_OWNER",
        "consumer: ROUTE_B_UNIVERSAL_ROLE_RUNNER",
        "format: postgresql_sql",
        "repo_relative_path: docs/sot/context/schemas/route_b_universal_role_runner_db_schema.v0.1.sql",
        "schema_ref: route_b_universal_role_runner_db_contract.v0.1",
        "proposal_only: true",
        "production_migration_executed: false",
        "server_apply_executed: false",
        "executed_db_changes: false",
        "source_of_truth: github_repo",
    )
    for marker in required_metadata:
        assert marker in text


def test_sql_defines_required_route_b_universal_role_runner_tables() -> None:
    text = _sql_text()
    for table_name in REQUIRED_TABLES:
        assert re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+" + re.escape(table_name) + r"\s*\(",
            text,
            re.IGNORECASE,
        ), table_name


def test_role_task_queue_has_required_contract_columns_and_constraints() -> None:
    block = _table_block("route_b_role_task_queue").lower()
    for field in ROLE_TASK_FIELDS:
        assert re.search(r"\b" + re.escape(field) + r"\b", block), field
    assert "role_task_id text primary key" in block
    assert "foreign key (phase_run_id, workflow_run_id)" in block
    assert "foreign key (parent_role_task_id)" in block
    assert "unique (phase_run_id, sequence_no)" in block
    assert "context_refs_json ? 'static_context_refs'" in block
    assert "context_refs_json ? 'role_context_ref'" in block
    assert "context_refs_json ? 'schema_refs'" in block


def test_role_task_queue_role_ids_and_status_values_match_contract() -> None:
    text = _sql_text()
    for role_id in SUPPORTED_ROLE_IDS:
        assert "'" + role_id + "'" in text, role_id
    for status in ROLE_TASK_STATUSES:
        assert "'" + status + "'" in text, status


def test_role_outputs_have_required_columns_and_validation_status_constraint() -> None:
    block = _table_block("route_b_role_outputs").lower()
    for field in ROLE_OUTPUT_FIELDS:
        assert re.search(r"\b" + re.escape(field) + r"\b", block), field
    assert "role_task_id text primary key" in block
    assert "foreign key (role_task_id)" in block
    assert "foreign key (phase_run_id, workflow_run_id)" in block
    for value in ("schema_validated", "schema_validation_failed", "blocked"):
        assert "'" + value + "'" in block


def test_pm_l3_decisions_have_required_columns_decision_types_and_authority_boundary() -> None:
    block = _table_block("route_b_pm_l3_decisions").lower()
    for field in PM_L3_DECISION_FIELDS:
        assert re.search(r"\b" + re.escape(field) + r"\b", block), field
    for decision_type in PM_L3_DECISION_TYPES:
        assert "'" + decision_type + "'" in block
    required_boundary_markers = (
        "merge_authority' = 'pm_l2_only'",
        "pm_l2_review_required",
        "n8n_merge_allowed",
        "direct_main_write_allowed",
        "force_push_allowed",
        "file_delete_allowed",
        "runtime_live_trading_allowed",
        "broker_execution_allowed",
        "server_apply_allowed",
    )
    for marker in required_boundary_markers:
        assert marker in block


def test_polling_and_phase_indexes_are_declared() -> None:
    normalized = _normalized_sql()
    required_indexes = (
        "create index if not exists route_b_role_task_queue_poll_idx",
        "where status = 'role_task_ready'",
        "create index if not exists route_b_role_task_queue_phase_status_idx",
        "create index if not exists route_b_role_outputs_phase_idx",
        "create index if not exists route_b_role_outputs_validation_idx",
        "create index if not exists route_b_pm_l3_decisions_phase_idx",
        "create index if not exists route_b_pm_l3_decisions_type_idx",
    )
    for marker in required_indexes:
        assert marker in normalized


def test_result_status_extension_view_exposes_role_tasks_outputs_and_pm_l3_decisions() -> None:
    normalized = _normalized_sql()
    assert "create or replace view route_b_phase_result_status_v0_1" in normalized
    assert "as role_tasks" in normalized
    assert "as role_outputs" in normalized
    assert "as pm_l3_decisions" in normalized
    assert "jsonb_agg" in normalized
    assert "jsonb_build_object" in normalized


def test_sql_contract_has_no_destructive_or_unsafe_path_markers() -> None:
    text = _sql_text()
    lowered = text.lower()
    for pattern in FORBIDDEN_SQL_PATTERNS:
        assert not re.search(pattern, lowered), pattern
    assert not re.search(r"(^|\s)(/home/|/tmp/|/mnt/|/var/|/root/|~/)", text)
    assert "\\" not in text
    for marker in FORBIDDEN_PATH_MARKERS:
        assert marker not in lowered, marker
