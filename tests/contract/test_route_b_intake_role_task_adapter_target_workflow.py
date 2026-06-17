from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET.json"
BASELINE_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_V1_10_3.json"
MIGRATION_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_execution.v0.1.sql"


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


def _workflow_text() -> str:
    return TARGET_PATH.read_text(encoding="utf-8")


def _assert_no_named_sql_placeholders(query: str) -> None:
    named_placeholders = re.findall(r"(?<!:):(?!:)[A-Za-z_][A-Za-z0-9_]*", query)
    assert named_placeholders == []


def test_target_workflow_json_exists_valid_and_baseline_preserved() -> None:
    assert TARGET_PATH.is_file()
    assert BASELINE_PATH.is_file()

    target = _workflow(TARGET_PATH)
    baseline = _workflow(BASELINE_PATH)

    assert target["name"] == "MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET"
    assert target.get("active") is False
    assert baseline["name"] == "MOEX_ROUTE_B_INTAKE_ACK_V1_10_3"
    assert baseline["name"] != target["name"]


def test_webhook_path_and_public_response_contract_are_preserved() -> None:
    workflow = _workflow(TARGET_PATH)
    webhook = _node(workflow, "Intake Webhook")
    response = _node(workflow, "Respond Accepted")

    assert webhook["parameters"]["httpMethod"] == "POST"
    assert webhook["parameters"]["path"] == "moex/route-b/intake"

    response_body = response["parameters"]["responseBody"]
    for token in (
        "accepted: true",
        "workflow_run_id",
        "status",
        "current_state",
        "repository_full_name",
        "target_branch_name",
        "idempotent_replay",
    ):
        assert token in response_body, token

    assert response["parameters"]["options"]["responseCode"] == 202


def test_sensitive_request_redaction_and_deterministic_idempotency_are_present() -> None:
    workflow = _workflow(TARGET_PATH)
    js_code = _js(_node(workflow, "Normalize Intake and Initial Role Task"))

    for token in (
        "isSensitiveKey",
        "redactValue",
        "[REDACTED]",
        "authorization",
        "cookie",
        "token",
        "secret",
        "password",
        "credential",
        "api-key",
        "clientsecret",
    ):
        assert token in js_code, token

    for token in (
        "clientIdempotencyKey",
        "idempotencySource",
        "hash32(idempotencySource)",
        "workflow_run_id:",
        "route_b_",
        "phase_run_",
        "role_task_",
        "targetBranchName",
    ):
        assert token in js_code, token


def test_postgres_query_uses_safe_jsonb_parameter_and_no_named_placeholders() -> None:
    workflow = _workflow(TARGET_PATH)
    node = _node(workflow, "Queue Legacy Run Phase and Initial Role Task")
    query = _query(node)

    assert "$1::jsonb" in query
    assert node["parameters"]["options"]["queryReplacement"] == "={{ JSON.stringify($json) }}"
    _assert_no_named_sql_placeholders(query)


def test_adapter_writes_legacy_compatibility_records() -> None:
    workflow = _workflow(TARGET_PATH)
    query = _query(_node(workflow, "Queue Legacy Run Phase and Initial Role Task"))

    for token in (
        "INSERT INTO moex_n8n_workflow_runs",
        "ON CONFLICT (workflow_run_id) DO UPDATE",
        "INSERT INTO moex_n8n_workflow_run_events",
        "event_type",
        "event_payload_json",
        "intake_ack",
    ):
        assert token in query or token in _workflow_text(), token


def test_adapter_writes_phase_run_with_repo_schema_columns() -> None:
    workflow = _workflow(TARGET_PATH)
    query = _query(_node(workflow, "Queue Legacy Run Phase and Initial Role Task"))
    migration = MIGRATION_PATH.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS route_b_phase_runs" in migration
    for token in (
        "INSERT INTO public.route_b_phase_runs",
        "phase_run_id",
        "workflow_run_id",
        "repository_full_name",
        "root_task_json",
        "status",
        "phase_run_created",
        "ON CONFLICT (workflow_run_id) DO UPDATE",
    ):
        assert token in query, token


def test_adapter_creates_exactly_one_initial_pm_l3_ready_role_task() -> None:
    workflow = _workflow(TARGET_PATH)
    query = _query(_node(workflow, "Queue Legacy Run Phase and Initial Role Task"))
    text = _workflow_text()

    assert query.count("INSERT INTO public.route_b_role_task_queue") == 1
    for token in (
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
        "PM_L3_DELIVERY_VALIDATION_OWNER",
        "pm_l3_initial_delivery_validation",
        "role_task_ready",
        "route_b_pm_l3_decision_loop.v0.1",
        "ON CONFLICT (role_task_id) DO UPDATE",
    ):
        assert token in query or token in text, token


def test_task_payload_contains_dynamic_handoff_and_pm_l2_request_data() -> None:
    workflow = _workflow(TARGET_PATH)
    js_code = _js(_node(workflow, "Normalize Intake and Initial Role Task"))

    for token in (
        "dynamic_handoff",
        "dynamicHandoff",
        "pm_l2_request_json",
        "pmL2RequestJson",
        "original_request_redacted",
        "authority_boundary",
        "PM_L2_ONLY",
        "required_next_output",
    ):
        assert token in js_code, token


def test_context_refs_and_expected_output_schema_are_populated_for_urr_worker() -> None:
    workflow = _workflow(TARGET_PATH)
    js_code = _js(_node(workflow, "Normalize Intake and Initial Role Task"))
    query = _query(_node(workflow, "Queue Legacy Run Phase and Initial Role Task"))

    for token in (
        "static_context_refs",
        "MOEX_Bot_Target_Architecture_2026_All_In_One",
        "MOEX_Bot_Role_Context_Operating_Model_v1",
        "github_commit_flow_subchats_v3",
        "role_context_ref",
        "role_context_version",
        "schema_refs",
        "route_b_universal_role_runner.v0.1",
        "route_b_role_task_queue.v0.1",
        "route_b_pm_l3_decision_loop.v0.1",
    ):
        assert token in js_code, token

    assert "expected_output_schema_ref" in query
    assert "context_refs_json" in query


def test_workflow_does_not_write_outputs_decisions_or_call_external_executors() -> None:
    workflow = _workflow(TARGET_PATH)
    text = _workflow_text()

    assert "public.route_b_role_outputs" not in text
    assert "public.route_b_pm_l3_decisions" not in text
    assert "ollama.com/api/chat" not in text
    assert "api.github.com" not in text
    assert "github.com/Viktoryyyyy/moex-robot" not in text

    http_nodes = [node for node in _nodes(workflow) if node.get("type") == "n8n-nodes-base.httpRequest"]
    assert http_nodes == []


def test_workflow_does_not_contain_forbidden_authority_claims() -> None:
    text = _workflow_text()

    forbidden_tokens = (
        '"n8n_merge_allowed": true',
        '"direct_main_write_allowed": true',
        '"force_push_allowed": true',
        '"file_delete_allowed": true',
        '"executor_merge_allowed": true',
        '"server_apply_allowed": true',
        '"broker_execution_allowed": true',
        '"runtime_live_trading_allowed": true',
        '"production_secret_access_allowed": true',
        '"approved_for_merge": true',
        "n8n_merge_allowed: true",
        "direct_main_write_allowed: true",
        "server_apply_allowed: true",
    )

    for token in forbidden_tokens:
        assert token not in text, token
