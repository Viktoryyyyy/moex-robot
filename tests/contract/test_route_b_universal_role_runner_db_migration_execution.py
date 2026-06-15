from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FORWARD_SQL_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_execution.v0.1.sql"
)
ROLLBACK_SQL_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_rollback.v0.1.sql"
)
ACCEPTED_PROPOSAL_PATH = (
    "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_proposal.v0.1.sql"
)
DB_CONTRACT_PATH = "docs/sot/context/schemas/route_b_universal_role_runner_db_contract.v0.1.yaml"

EXPECTED_FORWARD_TABLES = {
    "route_b_phase_runs",
    "route_b_role_task_queue",
    "route_b_role_outputs",
    "route_b_pm_l3_decisions",
}
EXPECTED_ROLLBACK_TABLES = (
    "route_b_pm_l3_decisions",
    "route_b_role_outputs",
    "route_b_role_task_queue",
    "route_b_phase_runs",
)
EXPECTED_INDEXES = {
    "ix_route_b_role_task_queue_ready_queue",
    "ix_route_b_role_task_queue_status_claim",
    "ix_route_b_role_task_queue_workflow_status",
    "ix_route_b_role_outputs_task_created",
    "ix_route_b_role_outputs_workflow_role",
    "ix_route_b_role_outputs_phase_workflow_created",
    "ix_route_b_pm_l3_decisions_phase_type_created",
    "ix_route_b_pm_l3_decisions_role_task_created",
    "ix_route_b_pm_l3_decisions_workflow_type",
}
FORWARD_FORBIDDEN_SQL_PATTERNS = (
    r"\bDROP\s+TABLE\b",
    r"\bDROP\s+DATABASE\b",
    r"\bTRUNCATE\b",
    r"\bDELETE\s+FROM\b",
    r"\bALTER\s+SYSTEM\b",
    r"\bCREATE\s+EXTENSION\b",
    r"\bpsql\b",
)
ROLLBACK_FORBIDDEN_SQL_PATTERNS = (
    r"\bDROP\s+DATABASE\b",
    r"\bTRUNCATE\b",
    r"\bDELETE\s+FROM\b",
    r"\bALTER\s+SYSTEM\b",
    r"\bCREATE\s+EXTENSION\b",
    r"\bCASCADE\b",
    r"\bpsql\b",
)
FORBIDDEN_TEXT_PATTERNS = (
    r"TODO",
    r"FIXME",
    r"<[^>\n]+>",
    r"\$\{[^}\n]+\}",
    r"\{\{[^}\n]+\}\}",
    r"\bhost\s*=",
    r"\buser\s*=",
    r"\bpassword\b",
    r"postgres://",
    r"postgresql://",
    r"file://",
    r"(^|[\s'\"`])/(home|tmp|mnt|var|root|srv|opt)/",
    r"~/",
    r"\b(latest|current|autodetect)\b",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _assert_transactional(sql: str) -> None:
    stripped = sql.strip()
    assert re.search(r"^BEGIN\s*;", stripped, flags=re.IGNORECASE | re.MULTILINE)
    assert re.search(r"COMMIT\s*;\s*$", stripped, flags=re.IGNORECASE)


def _assert_no_forbidden_text(sql: str) -> None:
    for pattern in FORBIDDEN_TEXT_PATTERNS:
        assert not re.search(pattern, sql, flags=re.IGNORECASE | re.MULTILINE), pattern


def test_db_migration_sql_files_exist() -> None:
    assert FORWARD_SQL_PATH.is_file()
    assert ROLLBACK_SQL_PATH.is_file()


def test_forward_migration_references_accepted_proposal_and_db_contract() -> None:
    sql = _read(FORWARD_SQL_PATH)
    assert "schema_id: route_b_universal_role_runner_db_migration_execution.v0.1" in sql
    assert ACCEPTED_PROPOSAL_PATH in sql
    assert DB_CONTRACT_PATH in sql


def test_forward_migration_is_not_proposal_only_text() -> None:
    sql = _read(FORWARD_SQL_PATH).lower()
    assert "status: executable_migration" in sql
    assert "proposal_only: true" not in sql
    assert "production_execution_allowed: false" not in sql
    assert "migration_execution_allowed: false" not in sql
    assert "this sql proposal is documentation-only" not in sql
    assert "this file proposes future persistence shape only" not in sql


def test_forward_migration_has_deterministic_executable_sql_structure() -> None:
    sql = _read(FORWARD_SQL_PATH)
    _assert_transactional(sql)
    assert "CREATE TABLE IF NOT EXISTS route_b_phase_runs" in sql
    assert "CREATE TABLE IF NOT EXISTS route_b_role_task_queue" in sql
    assert "CREATE TABLE IF NOT EXISTS route_b_role_outputs" in sql
    assert "CREATE TABLE IF NOT EXISTS route_b_pm_l3_decisions" in sql
    assert "phase_run_id, sequence_no, created_at, role_task_id" in sql
    assert "WHERE status = 'role_task_ready'" in sql


def test_forward_migration_has_idempotency_guards() -> None:
    sql = _read(FORWARD_SQL_PATH)
    for table in EXPECTED_FORWARD_TABLES:
        assert re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+" + re.escape(table) + r"\b",
            sql,
            flags=re.IGNORECASE,
        ), table
    for index in EXPECTED_INDEXES:
        assert re.search(
            r"CREATE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+" + re.escape(index) + r"\b",
            sql,
            flags=re.IGNORECASE,
        ), index


def test_forward_migration_has_expected_contract_narrowing() -> None:
    sql = _read(FORWARD_SQL_PATH)
    assert "route_b_role_task_queue" in sql
    assert "CREATE TABLE IF NOT EXISTS route_b_role_tasks" not in sql
    assert "raw_content_hash text NOT NULL" in sql
    assert "decision_payload_json jsonb NOT NULL" in sql
    assert "authority_boundary_json jsonb NOT NULL" in sql
    assert "ck_route_b_role_task_queue_context_role_id_matches" in sql


def test_forward_migration_has_no_forbidden_destructive_operations() -> None:
    sql = _read(FORWARD_SQL_PATH)
    for pattern in FORWARD_FORBIDDEN_SQL_PATTERNS:
        assert not re.search(pattern, sql, flags=re.IGNORECASE), pattern


def test_rollback_migration_is_present_and_bounded() -> None:
    sql = _read(ROLLBACK_SQL_PATH)
    _assert_transactional(sql)
    assert "operator-run rollback only after PM L2 approval" in sql
    assert "rollback_scope: objects introduced by route_b_universal_role_runner_db_migration_execution.v0.1.sql only" in sql
    for table in EXPECTED_ROLLBACK_TABLES:
        assert re.search(
            r"DROP\s+TABLE\s+IF\s+EXISTS\s+" + re.escape(table) + r"\s*;",
            sql,
            flags=re.IGNORECASE,
        ), table


def test_rollback_migration_does_not_contain_broad_unsafe_operations() -> None:
    sql = _read(ROLLBACK_SQL_PATH)
    for pattern in ROLLBACK_FORBIDDEN_SQL_PATTERNS:
        assert not re.search(pattern, sql, flags=re.IGNORECASE), pattern


def test_sql_files_have_no_secrets_paths_dynamic_markers_or_placeholders() -> None:
    for path in (FORWARD_SQL_PATH, ROLLBACK_SQL_PATH):
        _assert_no_forbidden_text(_read(path))
