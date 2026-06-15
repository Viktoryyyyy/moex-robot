from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_worker_poller_db_adapter_contract.v0.1.yaml"
)
MIGRATION_SQL_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_execution.v0.1.sql"
)
DB_CONTRACT_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_universal_role_runner_db_contract.v0.1.yaml"
)
ROLE_TASK_QUEUE_CONTRACT_PATH = (
    REPO_ROOT / "docs/sot/context/schemas/route_b_role_task_queue.v0.1.yaml"
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


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


def _column_names(sql: str, table_name: str) -> set[str]:
    block = _table_block(sql, table_name)
    columns: set[str] = set()
    for line in block.splitlines():
        match = re.match(
            r"\s{4}([a-z_][a-z0-9_]*)\s+(?:text|integer|jsonb|timestamptz)\b",
            line,
        )
        if match:
            columns.add(match.group(1))
    assert columns, table_name
    return columns


def _constraint_values(sql: str, constraint_name: str) -> set[str]:
    match = re.search(
        re.escape(constraint_name) + r".*?IN\s*\((.*?)\)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match, constraint_name
    values = set(re.findall(r"'([^']+)'", match.group(1)))
    assert values, constraint_name
    return values


def _yaml_mapping_block(text: str, key: str, indent: int) -> str:
    prefix = " " * indent
    match = re.search(
        r"^"
        + re.escape(prefix + key)
        + r":\n.*?(?=^"
        + re.escape(prefix)
        + r"[a-z_][a-z0-9_]*:\n|\Z)",
        text,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match, key
    return match.group(0)


def _runtime_table_binding_section(spec: str, binding_name: str) -> str:
    bindings_block = _yaml_mapping_block(spec, "runtime_table_bindings", indent=0)
    section = _yaml_mapping_block(bindings_block, binding_name, indent=2)
    assert "runtime_table: public." in section, binding_name
    return section


def _changed_files() -> set[str]:
    base_ref = os.environ.get("GITHUB_BASE_REF", "").strip()
    commands: list[list[str]] = []
    if base_ref:
        subprocess.run(
            ["git", "fetch", "origin", base_ref, "--depth=1"],
            cwd=REPO_ROOT,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        commands.append(["git", "diff", "--name-only", "origin/" + base_ref + "...HEAD"])
    commands.append(["git", "diff", "--name-only", "HEAD^", "HEAD"])

    for command in commands:
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode == 0:
            return {line.strip() for line in result.stdout.splitlines() if line.strip()}
    return set()


def _is_route_b_n8n_workflow_json(path: str) -> bool:
    lower = path.lower()
    name = Path(path).name
    if not lower.endswith(".json"):
        return False
    return name.startswith("MOEX_ROUTE_B_") or ("route_b" in lower and "workflow" in lower) or (
        "n8n" in lower and "route_b" in lower
    )


def test_adapter_contract_file_exists_and_is_repo_only() -> None:
    spec = _read(SPEC_PATH)
    assert "schema_id: route_b_worker_poller_db_adapter_contract.v0.1" in spec
    assert "repo_only_contract: true" in spec
    assert "sql_execution_allowed: false" in spec
    assert "server_db_mutation_allowed: false" in spec
    assert "production_n8n_workflow_mutation_allowed: false" in spec
    assert "runtime_live_broker_trading_scope_allowed: false" in spec
    assert "secrets_or_auth_scope_allowed: false" in spec


def test_runtime_adapter_uses_exact_role_task_queue_table_name() -> None:
    spec = _read(SPEC_PATH)
    assert "runtime_table: public.route_b_role_task_queue" in spec
    assert "table: public.route_b_role_task_queue" in spec
    assert "public.route_b_role_outputs" in spec
    assert "public.route_b_pm_l3_decisions" in spec
    assert not re.search(
        r"(runtime_table|table)\s*:\s*public\.route_b_role_tasks\b",
        spec,
        flags=re.IGNORECASE,
    )


def test_adapter_references_accepted_db_contract_and_migration_artifacts() -> None:
    spec = _read(SPEC_PATH)
    assert DB_CONTRACT_PATH.is_file()
    assert ROLE_TASK_QUEUE_CONTRACT_PATH.is_file()
    assert MIGRATION_SQL_PATH.is_file()
    assert "docs/sot/context/schemas/route_b_universal_role_runner_db_contract.v0.1.yaml" in spec
    assert "docs/sot/context/schemas/route_b_role_task_queue.v0.1.yaml" in spec
    assert (
        "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_execution.v0.1.sql"
        in spec
    )


def test_required_columns_are_derived_from_accepted_db_migration() -> None:
    spec = _read(SPEC_PATH)
    sql = _read(MIGRATION_SQL_PATH)
    table_bindings = {
        "route_b_role_task_queue": "role_task_queue",
        "route_b_role_outputs": "role_outputs",
        "route_b_pm_l3_decisions": "pm_l3_decisions",
    }
    for table_name, binding_name in table_bindings.items():
        section = _runtime_table_binding_section(spec, binding_name)
        assert "runtime_table: public." + table_name in section, table_name
        for column_name in _column_names(sql, table_name):
            assert re.search(r"\b" + re.escape(column_name) + r"\b", section), (
                table_name,
                column_name,
            )


def test_allowed_statuses_are_derived_from_accepted_db_migration() -> None:
    spec = _read(SPEC_PATH)
    sql = _read(MIGRATION_SQL_PATH)
    status_constraints = {
        "ck_route_b_role_task_queue_status_lifecycle": "role_task_queue",
        "ck_route_b_role_outputs_validation_status": "role_outputs",
        "ck_route_b_pm_l3_decisions_type": "pm_l3_decisions",
    }
    for constraint_name, spec_section_name in status_constraints.items():
        values = _constraint_values(sql, constraint_name)
        section = _runtime_table_binding_section(spec, spec_section_name)
        for value in values:
            assert value in section, (constraint_name, value)


def test_claim_contract_is_single_row_deterministic_and_concurrency_safe() -> None:
    spec = _read(SPEC_PATH)
    sql = _read(MIGRATION_SQL_PATH)
    task_statuses = _constraint_values(sql, "ck_route_b_role_task_queue_status_lifecycle")
    assert "role_task_ready" in task_statuses
    assert "role_task_running" in task_statuses
    assert "max_rows_per_claim: 1" in spec
    assert "LIMIT 1" in spec
    assert "FOR UPDATE SKIP LOCKED" in spec
    assert "ORDER BY phase_run_id ASC, sequence_no ASC, created_at ASC, role_task_id ASC" in spec
    assert "status = 'role_task_ready'" in spec
    assert "status = 'role_task_running'" in spec
    assert "attempt < max_retries" in spec
    assert "attempt = q.attempt + 1" in spec
    assert "claimed_at" in spec


def test_claim_contract_binds_role_id_to_role_context_ref_without_pm_l3_only_hardcode() -> None:
    spec = _read(SPEC_PATH)
    role_queue_contract = _read(ROLE_TASK_QUEUE_CONTRACT_PATH)
    assert "role_task.role_id_equals_role_context_ref.role_id: true" in role_queue_contract
    assert "role_id_parameter: role_id" in spec
    assert "context_refs_json #>> '{role_context_ref,role_id}' = role_id" in spec
    assert "AND role_id = :role_id" in spec
    assert "pm_l3_only_hardcode_allowed: false" in spec


def test_write_role_output_contract_preserves_raw_untrusted_output() -> None:
    spec = _read(SPEC_PATH)
    assert "table: public.route_b_role_outputs" in spec
    assert "output_json_is_raw_untrusted_role_output: true" in spec
    assert "trusted_or_accepted_by_default: false" in spec
    assert "validation_status_default: not_validated" in spec
    assert "'not_validated'" in spec
    assert "raw_content_hash_required: true" in spec
    assert "server_filesystem_path_allowed: false" in spec
    assert "absolute_path_allowed: false" in spec
    assert "route_b_role_outputs.role_output_id={role_output_id};raw_content_hash={raw_content_hash}" in spec


def test_pm_l3_decision_contract_is_separate_and_preserves_authority_boundary() -> None:
    spec = _read(SPEC_PATH)
    assert "table: public.route_b_pm_l3_decisions" in spec
    assert "persisted_separately_from_raw_role_output: true" in spec
    required_boundary = (
        "merge_authority: PM_L2_ONLY",
        "pm_l2_review_required: true",
        "n8n_merge_allowed: false",
        "direct_main_write_allowed: false",
        "force_push_allowed: false",
        "file_delete_allowed: false",
        "executor_merge_allowed: false",
        "ci_passed_is_not_merge_approval: true",
        "pm_l2_approval_claimed: false",
        "executor_merge_enabled: false",
        "subchat_return_target: PM_L3_DELIVERY_VALIDATION_OWNER",
    )
    for item in required_boundary:
        assert item in spec, item
    assert "approved_for_merge: true" not in spec
    assert "executor_merge_allowed: true" not in spec


def test_adapter_contract_contains_no_server_filesystem_or_dynamic_path_markers() -> None:
    spec = _read(SPEC_PATH)
    forbidden_patterns = (
        r"(^|[\s'\"`])/(home|tmp|mnt|var|root|srv|opt)/",
        r"~/",
        r"file://",
        r"\b(latest|autodetect)\b",
    )
    for pattern in forbidden_patterns:
        assert not re.search(pattern, spec, flags=re.IGNORECASE | re.MULTILINE), pattern


def test_this_pr_does_not_change_production_n8n_workflow_json_files() -> None:
    changed_files = _changed_files()
    forbidden = sorted(path for path in changed_files if _is_route_b_n8n_workflow_json(path))
    assert forbidden == []
