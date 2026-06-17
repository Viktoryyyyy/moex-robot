from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET.json"
BASELINE_INTAKE_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_V1_10_3.json"
BASELINE_WORKER_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json"
MIGRATION_SQL_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_universal_role_runner_db_migration_execution.v0.1.sql"
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _workflow(path: Path) -> dict:
    return json.loads(_read(path))


def _nodes(workflow: dict) -> list[dict]:
    nodes = workflow.get("nodes")
    assert isinstance(nodes, list)
    return nodes


def _node(workflow: dict, name: str) -> dict:
    matches = [node for node in _nodes(workflow) if node.get("name") == name]
    assert len(matches) == 1, name
    return matches[0]


def _postgres_nodes(workflow: dict) -> list[dict]:
    return [node for node in _nodes(workflow) if node.get("type") == "n8n-nodes-base.postgres"]


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


def test_target_workflow_json_exists_valid_name_inactive_and_path_preserved() -> None:
    assert TARGET_PATH.is_file()
    workflow = _workflow(TARGET_PATH)

    assert workflow["name"] == "MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET"
    assert workflow.get("active") is False

    webhook = _node(workflow, "Intake Webhook")
    assert webhook["type"] == "n8n-nodes-base.webhook"
    assert webhook["parameters"]["httpMethod"] == "POST"
    assert webhook["parameters"]["path"] == "moex/route-b/intake"
    assert webhook["parameters"]["responseMode"] == "responseNode"


def test_baseline_exports_remain_present_and_not_overwritten() -> None:
    assert BASELINE_INTAKE_PATH.is_file()
    assert BASELINE_WORKER_PATH.is_file()

    target = _workflow(TARGET_PATH)
    baseline_intake = _workflow(BASELINE_INTAKE_PATH)
    baseline_worker = _workflow(BASELINE_WORKER_PATH)

    assert baseline_intake["name"] == "MOEX_ROUTE_B_INTAKE_ACK_V1_10_3"
    assert baseline_worker["name"] == "MOEX_ROUTE_B_WORKER_POLLER_V1_10_3"
    assert target["name"] != baseline_intake["name"]
    assert target["name"] != baseline_worker["name"]


def test_response_behavior_preserves_ack_contract_fields() -> None:
    workflow = _workflow(TARGET_PATH)
    response = _node(workflow, "Respond Accepted")
    response_body = response["parameters"]["responseBody"]

    required_tokens = (
        "accepted: true",
        "workflow_run_id",
        "status",
        "current_state",
        "repository_full_name",
        "target_branch_name",
        "idempotent_replay",
    )

    for token in required_tokens:
        assert token in response_body, token

    assert response["parameters"]["options"]["responseCode"] == 202


def test_sensitive_request_redaction_remains_present() -> None:
    workflow = _workflow(TARGET_PATH)
    js_code = _js(_node(workflow, "Normalize Intake For Role Task Queue"))

    required_tokens = (
        "function isSensitiveKey",
        "function redactValue",
        "[REDACTED]",
        "authorization",
        "cookie",
        "token",
        "secret",
        "password",
        "credential",
        "api-key",
        "api_key",
        "clientsecret",
        "headers",
        "query",
        "body",
    )

    for token in required_tokens:
        assert token in js_code, token


def test_deterministic_idempotency_and_replay_semantics_remain_present() -> None:
    workflow = _workflow(TARGET_PATH)
    js_code = _js(_node(workflow, "Normalize Intake For Role Task Queue"))
    query = _query(_node(workflow, "Queue Legacy Run Event Phase And PM L3 Task"))

    required_js_tokens = (
        "stableStringify",
        "hash32",
        "idempotency-key",
        "x-idempotency-key",
        "idempotency_key",
        "workflowRunId",
        "phaseRunId",
        "roleTaskId",
        "n8n/route-b-",
    )

    for token in required_js_tokens:
        assert token in js_code, token

    assert "ON CONFLICT (workflow_run_id)" in query
    assert "ON CONFLICT (phase_run_id)" in query
    assert "ON CONFLICT (role_task_id) DO NOTHING" in query
    assert "idempotent_replay: !$json.inserted" in _node(workflow, "Respond Accepted")["parameters"]["responseBody"]


def test_legacy_status_result_compatibility_records_are_written() -> None:
    workflow = _workflow(TARGET_PATH)
    query = _query(_node(workflow, "Queue Legacy Run Event Phase And PM L3 Task"))

    required_tokens = (
        "INSERT INTO moex_n8n_workflow_runs",
        "INSERT INTO moex_n8n_workflow_run_events",
        "workflow_run_id",
        "idempotency_key",
        "status",
        "current_state",
        "repository_full_name",
        "current_phase",
        "target_branch_name",
        "event_payload_json",
        "WHERE u.inserted = true",
    )

    for token in required_tokens:
        assert token in query, token


def test_schema_contract_is_discoverable_and_target_uses_exact_db_columns() -> None:
    assert MIGRATION_SQL_PATH.is_file()
    migration_sql = _read(MIGRATION_SQL_PATH)
    query = _query(_node(_workflow(TARGET_PATH), "Queue Legacy Run Event Phase And PM L3 Task"))

    required_schema_tokens = (
        "CREATE TABLE IF NOT EXISTS route_b_phase_runs",
        "phase_run_id text PRIMARY KEY",
        "workflow_run_id text NOT NULL UNIQUE",
        "repository_full_name text NOT NULL",
        "root_task_json jsonb NOT NULL DEFAULT '{}'::jsonb",
        "status text NOT NULL DEFAULT 'phase_run_created'",
        "CONSTRAINT ck_route_b_phase_runs_status_lifecycle",
        "CREATE TABLE IF NOT EXISTS route_b_role_task_queue",
        "role_task_id text PRIMARY KEY",
        "sequence_no integer NOT NULL",
        "role_id text NOT NULL",
        "task_payload_json jsonb NOT NULL",
        "context_refs_json jsonb NOT NULL",
        "expected_output_schema_ref text NOT NULL",
        "CONSTRAINT uq_route_b_role_task_queue_phase_sequence",
        "CONSTRAINT ck_route_b_role_task_queue_context_refs_object",
        "CONSTRAINT ck_route_b_role_task_queue_context_role_id_matches",
    )

    for token in required_schema_tokens:
        assert token in migration_sql, token

    required_query_columns = (
        "phase_run_id",
        "workflow_run_id",
        "repository_full_name",
        "root_task_json",
        "role_task_id",
        "parent_role_task_id",
        "sequence_no",
        "role_id",
        "task_type",
        "task_payload_json",
        "context_refs_json",
        "expected_output_schema_ref",
        "attempt",
        "max_retries",
        "blocker_code",
        "claimed_at",
        "completed_at",
    )

    for column in required_query_columns:
        assert column in query, column


def test_workflow_writes_phase_run_and_exactly_one_initial_pm_l3_role_task() -> None:
    workflow = _workflow(TARGET_PATH)
    query = _query(_node(workflow, "Queue Legacy Run Event Phase And PM L3 Task"))

    assert "INSERT INTO public.route_b_phase_runs" in query
    assert "INSERT INTO public.route_b_role_task_queue" in query
    assert len(re.findall(r"INSERT\s+INTO\s+public\.route_b_role_task_queue", query, re.IGNORECASE)) == 1
    assert "'PM_L3_DELIVERY_VALIDATION_OWNER'" in query
    assert "'pm_l3_planning'" in query
    assert "'role_task_ready'" in query
    assert re.search(r"\b1,\s*'PM_L3_DELIVERY_VALIDATION_OWNER'", query), query
    assert "parent_role_task_id" in query
    assert "NULL" in query


def test_task_payload_contains_dynamic_handoff_and_pm_l2_request_data() -> None:
    workflow = _workflow(TARGET_PATH)
    js_code = _js(_node(workflow, "Normalize Intake For Role Task Queue"))

    required_tokens = (
        "taskPayloadJson",
        "dynamic_handoff",
        "pm_l2_request_json",
        "original_task",
        "acceptance_criteria",
        "approved_scope",
        "Route_B_Intake_To_Role_Task_Queue_Adapter_v0_1",
    )

    for token in required_tokens:
        assert token in js_code, token


def test_context_refs_and_expected_output_schema_are_populated_for_worker() -> None:
    workflow = _workflow(TARGET_PATH)
    js_code = _js(_node(workflow, "Normalize Intake For Role Task Queue"))
    query = _query(_node(workflow, "Queue Legacy Run Event Phase And PM L3 Task"))

    required_js_tokens = (
        "contextRefsJson",
        "static_context_refs",
        "role_context_ref",
        "schema_refs",
        "PM_L3_DELIVERY_VALIDATION_OWNER",
        "role_context_version",
        "route_b_pm_l3_decision_loop.v0.1",
        "route_b_role_task_queue.v0.1",
        "route_b_universal_role_runner.v0.1",
    )

    for token in required_js_tokens:
        assert token in js_code, token

    assert "context_refs_json" in query
    assert "expected_output_schema_ref" in query


def test_safe_postgres_query_pattern_uses_jsonb_replacement_and_no_named_placeholders() -> None:
    workflow = _workflow(TARGET_PATH)
    postgres_nodes = _postgres_nodes(workflow)

    assert len(postgres_nodes) == 1

    for node in postgres_nodes:
        query = _query(node)
        assert "$1::jsonb" in query
        assert node["parameters"]["options"]["queryReplacement"] == "={{ JSON.stringify($json) }}"
        _assert_no_named_sql_placeholders(query)


def test_no_forbidden_side_effect_tables_or_external_runtime_calls() -> None:
    workflow = _workflow(TARGET_PATH)
    workflow_text = _read(TARGET_PATH)

    forbidden_tokens = (
        "public.route_b_role_outputs",
        "public.route_b_pm_l3_decisions",
        "ollama",
        "Ollama",
        "api.github.com",
        "raw.githubusercontent.com",
        "github.com/Viktoryyyyy/moex-robot",
    )

    for token in forbidden_tokens:
        assert token not in workflow_text, token

    forbidden_node_types = (
        "n8n-nodes-base.httpRequest",
        "n8n-nodes-base.executeCommand",
    )

    for node_type in forbidden_node_types:
        assert all(node.get("type") != node_type for node in _nodes(workflow)), node_type


def test_no_merge_direct_main_or_server_apply_authority_claims() -> None:
    workflow_text = _read(TARGET_PATH)

    forbidden_tokens = (
        "approved_for_merge",
        "merge_authority",
        "merge_allowed",
        "n8n_merge_allowed",
        "direct_main_write_allowed",
        "executor_merge_allowed",
        "server_apply_allowed",
        "server_apply_authority",
        "server_apply_status",
        "direct-main",
        "direct main",
        "force_push_allowed",
    )

    for token in forbidden_tokens:
        assert token not in workflow_text, token
