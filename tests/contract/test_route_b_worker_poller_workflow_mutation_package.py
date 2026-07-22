from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_worker_poller_workflow_mutation_package.v0.1.yaml"
)
WORKFLOW_EXPORT_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json"
DB_ADAPTER_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_worker_poller_db_adapter_contract.v0.1.yaml"
)
REGISTRY_PATH = REPO_ROOT / "docs/sot/context/registry.route_b.v1.yaml"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_mutation_package_file_exists_as_historical_repo_only_evidence() -> None:
    assert PACKAGE_PATH.is_file()
    package = _read(PACKAGE_PATH)
    assert "schema_id: route_b_worker_poller_workflow_mutation_package.v0.1" in package
    assert "repo_only_package: true" in package
    assert "sql_execution_allowed: false" in package
    assert "server_db_mutation_allowed: false" in package
    assert "production_n8n_workflow_mutation_allowed: false" in package
    assert "endpoint_smoke_allowed: false" in package
    assert "ollama_production_call_allowed: false" in package
    assert "runtime_live_broker_trading_scope_allowed: false" in package
    assert "secrets_or_auth_scope_allowed: false" in package
    assert "workflow_export_mutation_mode: formal_mutation_package" in package


def test_worker_poller_export_is_fail_closed_historical_tombstone() -> None:
    workflow = json.loads(_read(WORKFLOW_EXPORT_PATH))

    assert workflow["name"] == "MOEX_ROUTE_B_WORKER_POLLER_V1_10_3"
    assert workflow["project"] == "MOEX_Bot"
    assert workflow["status"] == "deprecated_historical"
    assert workflow["active"] is False
    assert workflow["new_tasks_allowed"] is False
    assert workflow["new_runtime_execution_allowed"] is False
    assert workflow["nodes"] == []
    assert workflow["connections"] == {}
    assert workflow["historical_source"] is True
    assert workflow["superseded_by"] == [
        "browser_controlled_github_route",
        "flowise_automated_github_route",
    ]
    assert "deployed Applied State must be verified separately" in workflow["application_note"]


def test_historical_package_keeps_original_target_references_for_audit() -> None:
    package = _read(PACKAGE_PATH)

    assert DB_ADAPTER_PATH.is_file()
    assert (
        "target_workflow_export_artifact: docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json"
        in package
    )
    assert (
        "worker_poller_workflow_export: docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json"
        in package
    )


def test_claim_role_task_contract_preserves_historical_safety_boundaries() -> None:
    package = _read(PACKAGE_PATH)
    assert "read_table: public.route_b_role_task_queue" in package
    assert "role_task_queue: public.route_b_role_task_queue" in package
    assert "forbidden_runtime_table_names:" in package
    assert re.search(r"(?m)^\s*-\s+public\.route_b_role_tasks\s*$", package)
    assert not re.search(
        r"(?m)^\s*(?:read_table|write_table|table|role_task_queue|role_outputs|pm_l3_decisions):\s*public\.route_b_role_tasks\b",
        package,
    )
    assert "operation_kind: transactional_claim_one_role_task" in package
    assert "FOR UPDATE SKIP LOCKED" in package
    assert "LIMIT 1" in package
    assert "status = 'role_task_ready'" in package
    assert "status = 'role_task_running'" in package
    assert "attempt < max_retries" in package
    assert "attempt = q.attempt + 1" in package
    assert "claimed_at = now()" in package
    assert "ORDER BY phase_run_id ASC, sequence_no ASC, created_at ASC, role_task_id ASC" in package


def test_universal_role_input_contract_preserves_historical_reference_rules() -> None:
    package = _read(PACKAGE_PATH)
    required_tokens = (
        "step_id: build_universal_role_input",
        "role_id: claimed_role_task.role_id",
        "task_payload_json: claimed_role_task.task_payload_json",
        "context_refs_json: claimed_role_task.context_refs_json",
        "expected_output_schema_ref: claimed_role_task.expected_output_schema_ref",
        "source: context_refs_json.static_context_refs",
        "source: context_refs_json.role_context_ref",
        "role_id_must_equal_claimed_role_id: true",
        "context_refs_json.schema_refs",
        "expected_output_schema_ref",
        "repo_relative_only: true",
        "reject_unknown_ref: true",
        "implicit_path_autodetect",
    )
    for token in required_tokens:
        assert token in package, token


def test_prompt_contract_preserves_historical_strict_output_boundaries() -> None:
    package = _read(PACKAGE_PATH)
    required_tokens = (
        "target_node_kind: ai_call",
        "operation_kind: one_universal_role_prompt",
        "exactly_one_ai_node_in_worker_poller: true",
        "one_ai_node_per_role_allowed: false",
        "role_id_drives_role_behavior: true",
        "strict_json_output_only: true",
        "markdown_allowed: false",
        "code_fences_allowed: false",
        "prose_outside_json_allowed: false",
        "github_direct_call_instruction_allowed: false",
        "Do not use markdown.",
        "Do not use code fences.",
        "Do not call GitHub.",
    )
    for token in required_tokens:
        assert token in package, token


def test_persist_role_output_contract_preserves_untrusted_validation_fields() -> None:
    package = _read(PACKAGE_PATH)
    required_tokens = (
        "write_table: public.route_b_role_outputs",
        "include_role_task_id: true",
        "include_workflow_run_id: true",
        "include_role_id: true",
        "include_output_json: true",
        "include_output_schema_ref: true",
        "include_validation_status: true",
        "include_validation_errors_json: true",
        "include_raw_content_hash: true",
        "validation_status_on_insert: not_validated",
        "raw_output_trusted_on_insert: false",
        "output_not_trusted_until_this_step_passes: true",
        "'not_validated'",
        "raw_content_hash",
    )
    for token in required_tokens:
        assert token in package, token


def test_pm_l3_decision_contract_preserves_historical_authority_boundaries() -> None:
    package = _read(PACKAGE_PATH)
    required_tokens = (
        "role_id_equals: PM_L3_DELIVERY_VALIDATION_OWNER",
        "write_table: public.route_b_pm_l3_decisions",
        "create_next_role_task",
        "request_github_execution_package",
        "return_blocker",
        "return_final_pm_l3_evidence_package",
        "no_op",
        "github_execution_request_json_required_for_request_github_execution_package: true",
        "worker_poller_ai_calls_github_directly: false",
        "worker_poller_workflow_calls_github_directly: false",
        "github_execution_request_json_is_structured_handoff: true",
        "github_branch_pr_executor_only_mutation_executor: true",
        "github_branch_pr_executor_pr_only: true",
        "github_branch_pr_executor_merge_allowed: false",
    )
    for token in required_tokens:
        assert token in package, token


def test_historical_package_disallows_merge_direct_main_and_server_apply() -> None:
    package = _read(PACKAGE_PATH)
    required_tokens = (
        "merge_authority: PM_L2_ONLY",
        "pm_l2_review_required: true",
        "n8n_merge_allowed: false",
        "direct_main_write_allowed: false",
        "force_push_allowed: false",
        "file_delete_allowed: false",
        "executor_merge_allowed: false",
        "ci_passed_is_not_merge_approval: true",
        "worker_poller_may_merge_pr: false",
        "github_executor_may_merge_pr: false",
        "subchat_runtime_may_call_github: false",
        "server_apply_allowed: false",
    )
    for token in required_tokens:
        assert token in package, token
    assert "approved_for_merge: true" not in package
    assert "executor_merge_allowed: true" not in package
    assert "direct_main_write_allowed: true" not in package


def test_historical_mutation_package_is_not_published_by_active_registry() -> None:
    registry = json.loads(_read(REGISTRY_PATH))

    assert registry["status"] == "deprecated_historical"
    assert registry["new_tasks_allowed"] is False
    assert registry["new_runtime_execution_allowed"] is False
    assert registry["active_context_resolution_allowed"] is False
    assert "schema_refs" not in registry
    assert PACKAGE_PATH.is_file()
