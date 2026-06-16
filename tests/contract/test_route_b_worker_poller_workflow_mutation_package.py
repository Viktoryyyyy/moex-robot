from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_worker_poller_workflow_mutation_package.v0.1.yaml"
)
SOURCE_ARTIFACT_PATHS = (
    "docs/sot/context/schemas/route_b_worker_poller_db_adapter_contract.v0.1.yaml",
    "docs/sot/context/schemas/route_b_role_task_queue.v0.1.yaml",
    "docs/sot/context/schemas/route_b_universal_role_runner.v0.1.yaml",
    "docs/sot/context/schemas/route_b_ollama_role_prompt_contract.v0.1.yaml",
    "docs/sot/context/schemas/route_b_universal_role_runner_db_contract.v0.1.yaml",
    "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_execution.v0.1.sql",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


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


def test_workflow_mutation_package_exists_and_is_repo_only() -> None:
    spec = _read(PACKAGE_PATH)
    assert "schema_id: route_b_worker_poller_workflow_mutation_package.v0.1" in spec
    assert "status: repo_side_workflow_mutation_package" in spec
    assert "source_of_truth: github_repo" in spec
    assert "artifact_class: repo_relative" in spec
    assert (
        "artifact_path: docs/sot/context/schemas/route_b_worker_poller_workflow_mutation_package.v0.1.yaml"
        in spec
    )
    assert "producer: SUBCHAT_IMPLEMENTATION" in spec
    assert "consumer: PM_L3_DELIVERY_VALIDATION_OWNER_or_ROUTE_B_UNIVERSAL_ROLE_RUNNER" in spec
    assert "worker_poller_artifact_found_in_repo: false" in spec
    assert "worker_poller_artifact_paths: []" in spec
    assert "formal_mutation_package_required: true" in spec


def test_package_references_required_source_artifacts() -> None:
    spec = _read(PACKAGE_PATH)
    for relative_path in SOURCE_ARTIFACT_PATHS:
        assert relative_path in spec
        assert (REPO_ROOT / relative_path).is_file(), relative_path


def test_exact_runtime_table_names_are_present_and_wrong_table_is_absent() -> None:
    spec = _read(PACKAGE_PATH)
    assert "table: public.route_b_role_task_queue" in spec
    assert "table: public.route_b_role_outputs" in spec
    assert "table: public.route_b_pm_l3_decisions" in spec
    assert not re.search(r"\bpublic\.route_b_role_tasks\b", spec)


def test_claim_step_is_deterministic_single_row_transaction_safe_and_role_bound() -> None:
    spec = _read(PACKAGE_PATH)
    assert "operation: transactional_claim_one_ready_role_task" in spec
    assert "eligible_status: role_task_ready" in spec
    assert "claimed_status: role_task_running" in spec
    assert "max_rows_per_claim: 1" in spec
    assert "transaction_required: true" in spec
    assert "LIMIT 1" in spec
    assert "FOR UPDATE SKIP LOCKED" in spec
    assert "ORDER BY phase_run_id ASC, sequence_no ASC, created_at ASC, role_task_id ASC" in spec
    assert "attempt_increment_required: true" in spec
    assert "attempt = q.attempt + 1" in spec
    assert "role_binding_required: true" in spec
    assert "role_id_source: role_task.role_id" in spec
    assert "role_context_ref_source: role_task.context_refs_json.role_context_ref" in spec
    assert "invariant_role_id_equals_role_context_ref_role_id: true" in spec
    assert "context_refs_json #>> '{role_context_ref,role_id}' = role_id" in spec
    assert "pm_l3_only_hardcode_allowed: false" in spec
    for order_field in ("phase_run_id", "sequence_no", "created_at", "role_task_id"):
        assert re.search(r"^\s+- " + re.escape(order_field) + r"$", spec, re.MULTILINE), order_field


def test_context_prompt_ai_and_output_steps_preserve_universal_runner_boundaries() -> None:
    spec = _read(PACKAGE_PATH)
    assert "input_source: claimed_role_task_row" in spec
    assert "role_id" in spec
    assert "task_payload_json" in spec
    assert "context_refs_json" in spec
    assert "expected_output_schema_ref" in spec
    assert "static_context_refs: context_refs_json.static_context_refs" in spec
    assert "role_context_ref: context_refs_json.role_context_ref" in spec
    assert "schema_refs: context_refs_json.schema_refs" in spec
    assert "registry_path: docs/sot/context/registry.route_b.v1.yaml" in spec
    assert "implicit_path_discovery_allowed: false" in spec
    assert "full_static_context_paste_allowed: false" in spec
    assert "one_ai_node_only: true" in spec
    assert "inject_role_id: true" in spec
    assert "inject_role_context: true" in spec
    assert "inject_task_payload_json: true" in spec
    assert "inject_schema_refs: true" in spec
    assert "strict_json_output_only: true" in spec
    assert "markdown_allowed: false" in spec
    assert "code_fences_allowed: false" in spec
    assert "existing_ai_node_reused: true" in spec
    assert "additional_ai_nodes_allowed: false" in spec
    assert "production_live_run_in_this_phase_allowed: false" in spec
    assert "parsed_json_required: true" in spec
    assert "deterministic_validation_before_persist_required: true" in spec
    assert "ai_role_output_untrusted_until_validation_passes: true" in spec
    assert "raw_content_hash_required: true" in spec


def test_role_output_and_pm_l3_decision_persistence_are_untrusted_and_authority_bound() -> None:
    spec = _read(PACKAGE_PATH)
    assert "table: public.route_b_role_outputs" in spec
    assert "role_task_id" in spec
    assert "workflow_run_id" in spec
    assert "role_id" in spec
    assert "output_json" in spec
    assert "output_schema_ref" in spec
    assert "validation_status" in spec
    assert "validation_errors_json" in spec
    assert "output_json_is_raw_untrusted_role_output: true" in spec
    assert "validation_status_default: not_validated" in spec
    assert "trusted_or_accepted_by_default: false" in spec
    assert "applies_when_role_id: PM_L3_DELIVERY_VALIDATION_OWNER" in spec
    assert "table: public.route_b_pm_l3_decisions" in spec
    for decision_type in (
        "create_next_role_task",
        "request_github_execution_package",
        "return_blocker",
        "return_final_pm_l3_evidence_package",
        "no_op",
    ):
        assert decision_type in spec
    assert "authority_boundary_required: true" in spec
    assert "merge_authority: PM_L2_ONLY" in spec
    assert "pm_l2_review_required: true" in spec


def test_github_handoff_and_authority_boundaries_are_explicit() -> None:
    spec = _read(PACKAGE_PATH)
    assert "github_execution_request_json_required: true" in spec
    assert "github_execution_request_json_required_for_github_handoff: true" in spec
    assert "route_to_existing_github_executor_only: true" in spec
    assert "worker_poller_must_not_execute_github: true" in spec
    assert "ai_node_must_not_execute_github: true" in spec
    assert "existing_github_executor_pr_only: true" in spec
    assert "executor_merge_allowed: false" in spec
    assert "direct_main_write_allowed: false" in spec
    assert "merge_authority: PM_L2_ONLY" in spec
    assert "n8n_merge_allowed: false" in spec
    assert "force_push_allowed: false" in spec
    assert "file_delete_allowed: false" in spec
    assert "ci_passed_is_not_merge_approval: true" in spec
    assert "server_apply_allowed_without_pm_l2: false" in spec


def test_scope_boundary_forbids_out_of_scope_runtime_and_repo_actions() -> None:
    spec = _read(PACKAGE_PATH)
    forbidden_false_flags = (
        "production_n8n_workflow_mutation_allowed: false",
        "server_apply_allowed: false",
        "endpoint_smoke_allowed: false",
        "live_ollama_call_allowed: false",
        "db_migration_execution_allowed: false",
        "sql_execution_allowed: false",
        "secrets_or_auth_scope_allowed: false",
        "runtime_live_broker_trading_scope_allowed: false",
        "direct_github_mutation_by_ai_role_allowed: false",
        "direct_main_write_allowed: false",
        "force_push_allowed: false",
        "file_delete_allowed: false",
        "merge_allowed: false",
    )
    for flag in forbidden_false_flags:
        assert flag in spec, flag
    assert "production_n8n_workflow_mutation_allowed: true" not in spec
    assert "server_apply_allowed: true" not in spec
    assert "endpoint_smoke_allowed: true" not in spec
    assert "live_ollama_call_allowed: true" not in spec
    assert "secrets_or_auth_scope_allowed: true" not in spec
    assert "direct_main_write_allowed: true" not in spec
    assert "force_push_allowed: true" not in spec
    assert "file_delete_allowed: true" not in spec
    assert "merge_allowed: true" not in spec


def test_package_contains_no_server_filesystem_paths_or_dynamic_markers() -> None:
    spec = _read(PACKAGE_PATH)
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
