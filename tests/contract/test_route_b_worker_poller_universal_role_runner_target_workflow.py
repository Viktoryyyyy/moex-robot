from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json"
TARGET_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json"
MUTATION_PACKAGE_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_worker_poller_workflow_mutation_package.v0.1.yaml"
DB_MIGRATION_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_execution.v0.1.sql"

EXPECTED_TARGET_NAME = "MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET"
BASELINE_NAME = "MOEX_ROUTE_B_WORKER_POLLER_V1_10_3"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _walk_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        output: list[str] = []
        for key, item in value.items():
            output.extend(_walk_strings(key))
            output.extend(_walk_strings(item))
        return output
    if isinstance(value, list):
        output: list[str] = []
        for item in value:
            output.extend(_walk_strings(item))
        return output
    return []


def _workflow_text(workflow: dict[str, Any]) -> str:
    return "\n".join(_walk_strings(workflow))


def _nodes_by_name(workflow: dict[str, Any]) -> dict[str, dict[str, Any]]:
    nodes = workflow.get("nodes")
    assert isinstance(nodes, list)
    return {str(node["name"]): node for node in nodes if isinstance(node, dict)}


def _node_text(node: dict[str, Any]) -> str:
    return "\n".join(_walk_strings(node))


def test_target_workflow_artifact_exists_and_is_importable_without_replacing_baseline() -> None:
    assert BASELINE_PATH.is_file()
    assert TARGET_PATH.is_file()
    assert MUTATION_PACKAGE_PATH.is_file()
    assert DB_MIGRATION_PATH.is_file()
    baseline = _load_json(BASELINE_PATH)
    target = _load_json(TARGET_PATH)
    assert baseline["name"] == BASELINE_NAME
    assert target["name"] == EXPECTED_TARGET_NAME
    assert target["name"] != baseline["name"]
    assert TARGET_PATH.name != BASELINE_PATH.name
    assert isinstance(target.get("nodes"), list)
    assert isinstance(target.get("connections"), dict)
    assert target.get("settings", {}).get("executionOrder") == "v1"


def test_target_keeps_one_ai_call_and_no_direct_github_call_nodes() -> None:
    target = _load_json(TARGET_PATH)
    nodes = target["nodes"]
    assert isinstance(nodes, list)
    ai_nodes = [node for node in nodes if "ollama.com/api/chat" in json.dumps(node, ensure_ascii=False)]
    assert len(ai_nodes) == 1
    http_nodes = [node for node in nodes if node.get("type") == "n8n-nodes-base.httpRequest"]
    assert len(http_nodes) == 1
    text = _workflow_text(target)
    assert "api.github.com" not in text
    assert "github_pr_comment_moex_bot" not in text
    assert "contents/docs/sot/context" not in text
    assert "worker_poller_workflow_calls_github_directly:false" in text or "worker_poller_workflow_calls_github_directly',false" in text
    assert "Do not call GitHub" in text


def test_claim_query_uses_role_task_queue_with_skip_locked_and_one_task_limit() -> None:
    nodes = _nodes_by_name(_load_json(TARGET_PATH))
    query = str(nodes["Claim One Role Task From DB Queue"]["parameters"]["query"])
    assert "FROM public.route_b_role_task_queue" in query
    assert "UPDATE public.route_b_role_task_queue" in query
    assert "role_task_ready" in query
    assert "role_task_running" in query
    assert "FOR UPDATE SKIP LOCKED" in query
    assert "LIMIT 1" in query
    assert "ORDER BY q.phase_run_id ASC, q.sequence_no ASC, q.created_at ASC, q.role_task_id ASC" in query
    assert "q.context_refs_json #>> '{role_context_ref,role_id}' = q.role_id" in query
    assert "moex_n8n_workflow_runs" not in query
    assert "public.route_b_role_tasks" not in query


def test_prompt_is_role_id_driven_and_strict_json_only() -> None:
    nodes = _nodes_by_name(_load_json(TARGET_PATH))
    prompt_text = _node_text(nodes["Build Universal Role Input"])
    for required_input in ("role_id", "task_payload_json", "context_refs_json", "expected_output_schema_ref"):
        assert required_input in prompt_text
    assert "role_id is supplied by public.route_b_role_task_queue.role_id" in prompt_text
    assert "Return strict JSON only" in prompt_text
    assert "No markdown" in prompt_text
    assert "No code fences" in prompt_text
    assert "untrusted until deterministic validation passes" in prompt_text
    assert "GitHub execution is structured handoff only" in prompt_text
    assert "Do not merge" in prompt_text
    assert "Do not claim PM L2 approval" in prompt_text
    assert "PM_L2_ONLY" in prompt_text


def test_ai_output_is_untrusted_validated_and_persisted_with_required_fields() -> None:
    nodes = _nodes_by_name(_load_json(TARGET_PATH))
    validate_text = _node_text(nodes["Validate AI Role Output Deterministically"])
    persist_query = str(nodes["Persist Role Output And PM L3 Decision Handoff"]["parameters"]["query"])
    assert "ai_output_treated_as_untrusted_before_deterministic_validation" in validate_text
    assert "JSON.parse(raw)" in validate_text
    assert "role_output_not_valid_json" in validate_text
    assert "markdown_code_fence_not_allowed" in validate_text
    assert "raw_content_hash" in validate_text
    assert "public.route_b_role_outputs" in persist_query
    for column in ("role_output_id", "role_task_id", "workflow_run_id", "role_id", "output_json", "output_schema_ref", "validation_status", "validation_errors_json", "raw_content_hash"):
        assert column in persist_query


def test_pm_l3_decision_handoff_is_conditional_and_preserves_pm_l2_authority() -> None:
    target = _load_json(TARGET_PATH)
    nodes = _nodes_by_name(target)
    validate_text = _node_text(nodes["Validate AI Role Output Deterministically"])
    persist_query = str(nodes["Persist Role Output And PM L3 Decision Handoff"]["parameters"]["query"])
    full_text = _workflow_text(target)
    assert "PM_L3_DELIVERY_VALIDATION_OWNER" in validate_text
    assert "public.route_b_pm_l3_decisions" in persist_query
    assert "WHERE role_id='PM_L3_DELIVERY_VALIDATION_OWNER'" in persist_query
    for decision_type in ("create_next_role_task", "request_github_execution_package", "return_blocker", "return_final_pm_l3_evidence_package", "no_op"):
        assert decision_type in persist_query
    assert "github_execution_request_json_is_structured_handoff" in persist_query
    assert "worker_poller_workflow_calls_github_directly" in persist_query
    assert "'merge_authority','PM_L2_ONLY'" in persist_query
    assert "'n8n_merge_allowed',false" in persist_query
    assert "'direct_main_write_allowed',false" in persist_query
    assert "'executor_merge_allowed',false" in persist_query
    assert "worker_poller_may_merge_pr" in persist_query
    assert "merge_pull_request" not in full_text


def test_target_mentions_accepted_source_artifacts_and_table_names() -> None:
    text = _workflow_text(_load_json(TARGET_PATH))
    assert "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json" in text
    assert "docs/sot/context/schemas/route_b_worker_poller_workflow_mutation_package.v0.1.yaml" in text
    assert "public.route_b_role_task_queue" in text
    assert "public.route_b_role_outputs" in text
    assert "public.route_b_pm_l3_decisions" in text
    assert "role_task_ready" in text
    assert "role_task_running" in text
