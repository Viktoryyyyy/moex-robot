from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json"
BASELINE_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json"


def _workflow(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _nodes(workflow: dict) -> list[dict]:
    nodes = workflow.get("nodes")
    assert isinstance(nodes, list)
    return nodes


def _node(workflow: dict, name: str) -> dict:
    matches = [node for node in _nodes(workflow) if node.get("name") == name]
    assert len(matches) == 1, name
    return matches[0]


def _query(node: dict) -> str:
    query = node.get("parameters", {}).get("query", "")
    assert isinstance(query, str)
    return query


def _js(node: dict) -> str:
    js_code = node.get("parameters", {}).get("jsCode", "")
    assert isinstance(js_code, str)
    return js_code


def _assert_no_named_sql_placeholders(query: str) -> None:
    named_placeholders = re.findall(r"(?<!:):(?!:)[A-Za-z_][A-Za-z0-9_]*", query)
    assert named_placeholders == []


def test_target_workflow_artifact_exists_and_preserves_baseline_export() -> None:
    assert TARGET_PATH.is_file()
    assert BASELINE_PATH.is_file()

    target = _workflow(TARGET_PATH)
    baseline = _workflow(BASELINE_PATH)

    assert target["name"] == "MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET"
    assert baseline["name"] == "MOEX_ROUTE_B_WORKER_POLLER_V1_10_3"
    assert target["name"] != baseline["name"]
    assert target.get("active") is False
    assert baseline.get("active") is True or baseline.get("active") is False


def test_target_uses_one_ai_node_and_no_role_specific_ai_nodes() -> None:
    workflow = _workflow(TARGET_PATH)
    nodes = _nodes(workflow)

    ai_nodes = [
        node for node in nodes
        if node.get("type") == "n8n-nodes-base.httpRequest"
        and "ollama.com/api/chat" in json.dumps(node, ensure_ascii=False)
    ]

    assert len(ai_nodes) == 1
    assert ai_nodes[0]["name"] == "Universal Role AI Call"

    node_names = {node.get("name") for node in nodes}
    assert "SUBCHAT_IMPLEMENTATION AI Call" not in node_names
    assert "SUBCHAT_VALIDATION AI Call" not in node_names
    assert "PM L3 AI Call" not in node_names


def test_claim_role_task_uses_role_task_queue_and_lock_safe_semantics() -> None:
    workflow = _workflow(TARGET_PATH)
    query = _query(_node(workflow, "Claim One Role Task"))

    required_tokens = (
        "FROM public.route_b_role_task_queue",
        "WHERE q.status = 'role_task_ready'",
        "status = 'role_task_running'",
        "q.attempt < q.max_retries",
        "attempt = q.attempt + 1",
        "claimed_at = now()",
        "ORDER BY q.phase_run_id ASC, q.sequence_no ASC, q.created_at ASC, q.role_task_id ASC",
        "FOR UPDATE SKIP LOCKED",
        "LIMIT 1",
    )

    for token in required_tokens:
        assert token in query, token

    assert "public.route_b_role_tasks" not in query


def test_role_prompt_is_role_id_driven_strict_json_and_github_handoff_only() -> None:
    workflow = _workflow(TARGET_PATH)
    js_code = _js(_node(workflow, "Build Universal Role Prompt"))

    required_tokens = (
        "role_id is supplied by public.route_b_role_task_queue.role_id",
        "task_payload_json",
        "context_refs_json",
        "expected_output_schema_ref",
        "Return exactly one JSON object matching expected_output_schema_ref",
        "Return strict JSON output only",
        "no markdown",
        "no code fences",
        "Do not call GitHub directly",
        "structured github_execution_request_json only",
        "must not merge",
        "PM_L2_ONLY",
    )

    for token in required_tokens:
        assert token in js_code, token


def test_persist_role_output_uses_query_replacement_and_untrusted_output_contract() -> None:
    workflow = _workflow(TARGET_PATH)
    node = _node(workflow, "Persist Raw AI Role Output")
    query = _query(node)

    assert "$1::jsonb" in query
    assert node["parameters"]["options"]["queryReplacement"] == "={{ JSON.stringify($json) }}"
    _assert_no_named_sql_placeholders(query)

    required_tokens = (
        "INSERT INTO public.route_b_role_outputs",
        "role_output_id",
        "phase_run_id",
        "role_task_id",
        "workflow_run_id",
        "role_id",
        "output_json",
        "output_schema_ref",
        "validation_status",
        "validation_errors_json",
        "raw_content_hash",
        "'not_validated'",
        "ON CONFLICT (role_task_id, output_schema_ref) DO NOTHING",
    )

    for token in required_tokens:
        assert token in query, token


def test_validation_gate_rejects_boundary_violations_before_finalization() -> None:
    workflow = _workflow(TARGET_PATH)
    js_code = _js(_node(workflow, "Validate Untrusted Role Output"))

    required_tokens = (
        "role_id_mismatch",
        "markdown_code_fence_not_allowed",
        "worker_poller_boundary_violation",
        "pm_l2_approval_claim_forbidden",
        "request_github_execution_package_requires_github_execution_request_json_structured_handoff",
        "validation_status",
    )

    for token in required_tokens:
        assert token in js_code, token


def test_finalize_updates_validation_and_persists_pm_l3_decision_with_boundaries() -> None:
    workflow = _workflow(TARGET_PATH)
    node = _node(workflow, "Finalize Role Task and PM L3 Decision")
    query = _query(node)

    assert "$1::jsonb" in query
    assert node["parameters"]["options"]["queryReplacement"] == "={{ JSON.stringify($json) }}"
    _assert_no_named_sql_placeholders(query)

    required_tokens = (
        "UPDATE public.route_b_role_outputs",
        "INSERT INTO public.route_b_pm_l3_decisions",
        "UPDATE public.route_b_role_task_queue",
        "PM_L3_DELIVERY_VALIDATION_OWNER",
        "create_next_role_task",
        "request_github_execution_package",
        "return_blocker",
        "return_final_pm_l3_evidence_package",
        "no_op",
        "PM_L2_ONLY",
        "'n8n_merge_allowed',false",
        "'direct_main_write_allowed',false",
        "'force_push_allowed',false",
        "'file_delete_allowed',false",
        "'executor_merge_allowed',false",
        "'ci_passed_is_not_merge_approval',true",
        "'server_apply_allowed',false",
        "'pm_l2_approval_claimed',false",
        "role_task_completed",
        "role_task_failed",
    )

    for token in required_tokens:
        assert token in query, token


def test_target_workflow_has_required_credentials_without_new_secret_values() -> None:
    workflow = _workflow(TARGET_PATH)

    for name in (
        "Claim One Role Task",
        "Persist Raw AI Role Output",
        "Finalize Role Task and PM L3 Decision",
    ):
        node = _node(workflow, name)
        assert node["credentials"]["postgres"]["name"] == "postgres_moex_n8n"

    ai_node = _node(workflow, "Universal Role AI Call")
    assert ai_node["credentials"]["httpHeaderAuth"]["name"] == "ollama_cloud_moex_route_b"


def test_target_worker_does_not_call_github_or_claim_pm_l2_approval() -> None:
    workflow_text = TARGET_PATH.read_text(encoding="utf-8")

    forbidden_tokens = (
        "api.github.com",
        "github.com/Viktoryyyyy/moex-robot",
        "approved_for_merge\": true",
        "n8n_merge_allowed\": true",
        "executor_merge_allowed\": true",
        "direct_main_write_allowed\": true",
    )

    for token in forbidden_tokens:
        assert token not in workflow_text, token
