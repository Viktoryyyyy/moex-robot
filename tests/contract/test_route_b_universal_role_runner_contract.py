from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "docs/sot/context/registry.route_b.v1.yaml"
SCHEMA_DIR = REPO_ROOT / "docs/sot/context/schemas"

NEW_SCHEMA_REFS = {
    "route_b_universal_role_runner.v0.1": "route_b_universal_role_runner.v0.1.yaml",
    "route_b_role_task_queue.v0.1": "route_b_role_task_queue.v0.1.yaml",
    "route_b_pm_l3_decision_loop.v0.1": "route_b_pm_l3_decision_loop.v0.1.yaml",
    "route_b_multi_role_phase_state_machine.v0.1": "route_b_multi_role_phase_state_machine.v0.1.yaml",
    "route_b_ollama_role_prompt_contract.v0.1": "route_b_ollama_role_prompt_contract.v0.1.yaml",
    "route_b_universal_role_runner_db_contract.v0.1": "route_b_universal_role_runner_db_contract.v0.1.yaml",
    "route_b_universal_role_runner_db_migration_proposal.v0.1": "route_b_universal_role_runner_db_migration_proposal.v0.1.sql",
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

MINIMUM_STATES = {
    "phase_queued",
    "pm_l3_planning",
    "role_task_ready",
    "role_task_running",
    "role_task_completed",
    "role_task_failed",
    "pm_l3_validating",
    "github_execution_requested",
    "github_executor_pr_opened_waiting_ci",
    "pm_l3_finalizing",
    "pm_l2_review_required",
    "blocked",
    "failed",
}

FORBIDDEN_AUTHORITY_PATTERNS = (
    r"(^|\n)\s*runtime_live_trading_allowed:\s*true",
    r"(^|\n)\s*broker_execution_allowed:\s*true",
    r"(^|\n)\s*server_apply_allowed:\s*true",
    r"(^|\n)\s*production_secret_access_allowed:\s*true",
    r"(^|\n)\s*ai_role_server_command_execution_allowed:\s*true",
    r"(^|\n)\s*ai_role_production_secret_access_allowed:\s*true",
    r"(^|\n)\s*direct_github_mutation_by_ai_role:\s*true",
    r"(^|\n)\s*n8n_merge_allowed:\s*true",
    r"(^|\n)\s*direct_main_write_allowed:\s*true",
    r"(^|\n)\s*force_push_allowed:\s*true",
    r"(^|\n)\s*file_delete_allowed:\s*true",
    r"(^|\n)\s*executor_merge_allowed:\s*true",
)


def _registry() -> dict[str, object]:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _schema_path(ref: str) -> Path:
    return SCHEMA_DIR / NEW_SCHEMA_REFS[ref]


def _schema_text(ref: str) -> str:
    return _schema_path(ref).read_text(encoding="utf-8")


def _all_new_schema_text() -> str:
    return "\n".join(_schema_text(ref) for ref in NEW_SCHEMA_REFS)


def test_all_new_yaml_contract_files_exist() -> None:
    for ref, filename in NEW_SCHEMA_REFS.items():
        path = SCHEMA_DIR / filename
        assert path.is_file(), ref


def test_registry_contains_all_new_schema_refs() -> None:
    registry = _registry()
    schema_refs = registry["schema_refs"]
    assert isinstance(schema_refs, dict)
    for ref, filename in NEW_SCHEMA_REFS.items():
        assert ref in schema_refs
        entry = schema_refs[ref]
        assert isinstance(entry, dict)
        assert entry["path"] == "docs/sot/context/schemas/" + filename
        assert entry["producer"] == "PM_L2_PHASE_OWNER"
        assert entry["consumer"] == "PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER"


def test_universal_role_runner_supported_role_allowlist_is_complete() -> None:
    text = _schema_text("route_b_universal_role_runner.v0.1")
    for role_id in SUPPORTED_ROLE_IDS:
        assert re.search(r"^\s*-\s*" + re.escape(role_id) + r"\s*$", text, re.MULTILINE), role_id


def test_contracts_define_role_ids_not_separate_workflows_and_one_runner() -> None:
    text = _all_new_schema_text()
    assert "roles_are_role_id_values_not_separate_workflows: true" in text
    assert "role_tasks_are_role_id_values_not_separate_workflows: true" in text
    assert "one_universal_runner_pattern: true" in text
    assert "one_workflow_per_role_allowed: false" in text
    assert "one_ai_node_per_role_allowed: false" in text


def test_role_task_role_id_must_match_role_context_ref_role_id() -> None:
    text = _all_new_schema_text()
    assert "role_task.role_id must equal role_context_ref.role_id" in text
    assert "invariant_role_task_role_id_equals_role_context_ref_role_id: true" in text
    assert "role_task.role_id_equals_role_context_ref.role_id: true" in text


def test_db_contract_is_proposal_only_and_no_db_changes_executed() -> None:
    text = _schema_text("route_b_universal_role_runner_db_contract.v0.1")
    assert re.search(r"^proposal_only:\s*true\s*$", text, re.MULTILINE)
    assert "no_production_migration: true" in text
    assert "no_server_apply: true" in text
    assert "no_executed_db_changes: true" in text
    assert "production_migration_executed: false" in text
    assert "server_apply_executed: false" in text
    assert "executed_db_changes: false" in text


def test_state_machine_supports_many_role_tasks_and_required_states() -> None:
    text = _schema_text("route_b_multi_role_phase_state_machine.v0.1")
    assert "supports_many_role_tasks_per_phase: true" in text
    assert "role_tasks_are_db_rows: true" in text
    for state in MINIMUM_STATES:
        assert re.search(r"^\s*-\s*" + re.escape(state) + r"\s*$", text, re.MULTILINE), state


def test_pm_l3_authority_boundaries_are_explicit() -> None:
    text = _schema_text("route_b_pm_l3_decision_loop.v0.1")
    assert "PM L3 cannot issue PM L2 final verdict" in text
    assert "PM L3 cannot approve merge" in text
    assert "PM L3 cannot bypass PM L2" in text
    assert "pm_l3_final_pm_l2_verdict_allowed: false" in text
    assert "pm_l3_merge_approval_allowed: false" in text


def test_subchat_return_and_github_executor_boundaries() -> None:
    text = _all_new_schema_text()
    assert "subchat_output_never_returns_directly_to_pm_l2: true" in text
    assert "subchat_cannot_return_directly_to_pm_l2: true" in text
    assert "subchat_direct_return_to_pm_l2_allowed: false" in text
    assert "github_executor_remains_separate_pr_only_executor: true" in text
    assert "source: separate_pr_only_github_executor" in text


def test_worker_poller_pm_l3_only_assumption_is_migration_target() -> None:
    text = _all_new_schema_text()
    assert "worker_poller_pm_l3_only_assumption" in text
    assert "existing_worker_poller_pm_l3_only_assumption" in text
    assert "status: migration_target_not_target_behavior" in text
    assert "target_replacement: DB-driven role_task.role_id selection through Universal Role Runner" in text


def test_no_runtime_live_broker_server_apply_or_secret_scope_is_exposed() -> None:
    text = _all_new_schema_text()
    for pattern in FORBIDDEN_AUTHORITY_PATTERNS:
        assert not re.search(pattern, text), pattern
    assert "no_runtime_live_trading_scope: true" in text
    assert "no_broker_execution_scope: true" in text
    assert "no_server_apply_scope: true" in text
    assert "no_production_secret_scope: true" in text


def test_ollama_prompt_contract_requires_strict_json_only() -> None:
    text = _schema_text("route_b_ollama_role_prompt_contract.v0.1")
    assert "strict_JSON_only: true" in text
    assert "no_markdown: true" in text
    assert "no_code_fences: true" in text
    assert "no_external_assumptions: true" in text
    assert "output_schema_binding:" in text
