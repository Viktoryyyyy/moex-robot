from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATH = (
    REPO_ROOT
    / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json"
)
BASELINE_PATH = REPO_ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json"
INTAKE_ADAPTER_PATH = (
    REPO_ROOT
    / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET.json"
)
BASELINE_BLOB_SHA = "513b70e52294d66e9478a55765fa2f72eec4baec"
INTAKE_ADAPTER_BLOB_SHA = "b2a19654add077f0ad72dd13b4e270bbcd21f44d"
APPROVED_PR_CHANGED_PATHS = {
    "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json",
    "tests/contract/test_route_b_worker_poller_universal_role_runner_target_workflow.py",
}


def _git_blob_sha(path: Path) -> str:
    content = path.read_bytes()
    header = f"blob {len(content)}\0".encode()
    return hashlib.sha1(header + content).hexdigest()


def _workflow(path: Path = TARGET_PATH) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _nodes(workflow: dict) -> list[dict]:
    nodes = workflow.get("nodes")
    assert isinstance(nodes, list)
    return nodes


def _node(workflow: dict, name: str) -> dict:
    matches = [node for node in _nodes(workflow) if node.get("name") == name]
    assert len(matches) == 1, name
    return matches[0]


def _query(workflow: dict, name: str) -> str:
    query = _node(workflow, name).get("parameters", {}).get("query", "")
    assert isinstance(query, str)
    return query


def _js(workflow: dict, name: str) -> str:
    js_code = _node(workflow, name).get("parameters", {}).get("jsCode", "")
    assert isinstance(js_code, str)
    return js_code


def _assert_tokens(text: str, tokens: tuple[str, ...]) -> None:
    for token in tokens:
        assert token in text, token


def _assert_no_named_sql_placeholders(query: str) -> None:
    named = re.findall(r"(?<!:):(?!:)[A-Za-z_][A-Za-z0-9_]*", query)
    assert named == []


def test_target_artifact_and_protected_exports_are_preserved() -> None:
    assert TARGET_PATH.is_file()
    assert BASELINE_PATH.is_file()
    assert INTAKE_ADAPTER_PATH.is_file()
    target = _workflow()
    baseline = _workflow(BASELINE_PATH)
    intake_adapter = _workflow(INTAKE_ADAPTER_PATH)
    assert target["name"] == (
        "MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET"
    )
    assert baseline["name"] == "MOEX_ROUTE_B_WORKER_POLLER_V1_10_3"
    assert intake_adapter["name"] == (
        "MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET"
    )
    assert target.get("active") is False
    assert _git_blob_sha(BASELINE_PATH) == BASELINE_BLOB_SHA
    assert _git_blob_sha(INTAKE_ADAPTER_PATH) == INTAKE_ADAPTER_BLOB_SHA
    assert APPROVED_PR_CHANGED_PATHS == {
        "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json",
        "tests/contract/test_route_b_worker_poller_universal_role_runner_target_workflow.py",
    }


def test_workflow_keeps_one_ai_call_and_no_direct_execution_nodes() -> None:
    workflow = _workflow()
    nodes = _nodes(workflow)
    ai_nodes = [
        node
        for node in nodes
        if node.get("type") == "n8n-nodes-base.httpRequest"
        and "ollama.com/api/chat" in json.dumps(node, ensure_ascii=False)
    ]
    assert len(ai_nodes) == 1
    assert ai_nodes[0]["name"] == "Universal Role AI Call"
    endpoint_text = "\n".join(
        str(node.get("parameters", {}).get("url", "")) for node in nodes
    ).lower()
    for forbidden in (
        "api.github.com",
        "github.com/Viktoryyyyy/moex-robot",
        "alor",
        "tinkoff",
        "broker",
        "live-trading",
        "live_trading",
        "runtime/execution",
    ):
        assert forbidden not in endpoint_text, forbidden
    for name in (
        "Claim One Role Task",
        "Persist Raw AI Role Output",
        "Finalize Role Task and PM L3 Decision",
    ):
        assert _node(workflow, name)["credentials"]["postgres"]["name"] == (
            "postgres_moex_n8n"
        )
    assert ai_nodes[0]["credentials"]["httpHeaderAuth"]["name"] == (
        "ollama_cloud_moex_route_b"
    )


def test_claim_is_lock_safe_and_context_compatible() -> None:
    query = _query(_workflow(), "Claim One Role Task")
    _assert_tokens(
        query,
        (
            "FROM public.route_b_role_task_queue",
            "WHERE q.status = 'role_task_ready'",
            "q.attempt < q.max_retries",
            "q.context_refs_json #>> '{role_context_ref,role_id}' = q.role_id",
            "ORDER BY q.phase_run_id ASC, q.sequence_no ASC",
            "FOR UPDATE SKIP LOCKED",
            "LIMIT 1",
            "status = 'role_task_running'",
            "attempt = q.attempt + 1",
            "claimed_at = now()",
        ),
    )


def test_pm_l3_prompt_contract_includes_controlled_safe_follow_up_schema() -> None:
    js_code = _js(_workflow(), "Build Universal Role Prompt")
    _assert_tokens(
        js_code,
        (
            "PM_L3_DELIVERY_VALIDATION_OWNER hard output contract",
            "Allowed decision_type values: create_next_role_task",
            "Controlled create_next_role_task smoke rule",
            "urr_create_next_role_task_smoke_only",
            "controlled_create_next_role_task_path",
            '"next_role_id":"PM_L3_DELIVERY_VALIDATION_OWNER"',
            '"next_task_payload_json"',
            '"next_expected_output_schema_ref":"route_b_pm_l3_decision_loop.v0.1"',
            '"required_decision_type":"no_op"',
            "Return decision_type no_op. Do not create another role task.",
            "Controlled DB queue orchestration smoke only",
            "no external execution is allowed",
            "must instruct the follow-up PM_L3 task to return no_op",
            "not create another task",
            "next_task_payload_json must be a JSON object",
            "Never inherit the parent expected_output_schema_ref",
            "Do not call GitHub directly",
            "PM_L2_ONLY",
        ),
    )


def test_validation_rejects_non_object_payload_and_unresolved_child_schema() -> None:
    js_code = _js(_workflow(), "Validate Untrusted Role Output")
    _assert_tokens(
        js_code,
        (
            "create_next_role_task_missing_decision_payload_json",
            "create_next_role_task_missing_next_role_id",
            "create_next_role_task_missing_next_task_payload_json",
            "else if(!obj(dp.next_task_payload_json))",
            "create_next_role_task_next_task_payload_must_be_object",
            "next_expected_output_schema_ref",
            "expected_output_schema_ref",
            "route_b_pm_l3_decision_loop.v0.1",
            "dp.next_role_id!=='PM_L3_DELIVERY_VALIDATION_OWNER'",
            "!explicitChildSchema",
            "create_next_role_task_missing_child_output_schema_ref",
            "create_next_role_task_child_schema_not_resolved",
            "if(e.length)validation_status='fail'",
        ),
    )


def test_validation_keeps_existing_boundary_and_decision_guards() -> None:
    js_code = _js(_workflow(), "Validate Untrusted Role Output")
    _assert_tokens(
        js_code,
        (
            "output_json_must_be_object",
            "role_id_mismatch",
            "markdown_code_fence_not_allowed",
            "worker_poller_boundary_violation",
            "pm_l2_approval_claim_forbidden",
            "decision_type_not_allowed",
            "request_github_execution_package_missing_github_execution_request_json",
            "return_blocker_missing_blocker_code",
            "return_final_pm_l3_evidence_package_missing_final_pm_l3_package_json",
        ),
    )


def test_finalization_is_parameterized_and_preserves_authority_boundary() -> None:
    workflow = _workflow()
    node = _node(workflow, "Finalize Role Task and PM L3 Decision")
    query = _query(workflow, node["name"])
    assert "$1::jsonb" in query
    assert node["parameters"]["options"]["queryReplacement"] == (
        "={{ JSON.stringify($json) }}"
    )
    _assert_no_named_sql_placeholders(query)
    _assert_tokens(
        query,
        (
            "INSERT INTO public.route_b_pm_l3_decisions",
            "'merge_authority','PM_L2_ONLY'",
            "'n8n_merge_allowed',false",
            "'direct_main_write_allowed',false",
            "'force_push_allowed',false",
            "'file_delete_allowed',false",
            "'executor_merge_allowed',false",
            "'ci_passed_is_not_merge_approval',true",
            "'server_apply_allowed',false",
            "'pm_l2_approval_claimed',false",
            "'runtime_live_trading_allowed',false",
            "'broker_execution_allowed',false",
            "'production_secret_access_allowed',false",
        ),
    )


def test_pm_l3_decision_and_enqueue_guards_require_object_child_payload() -> None:
    query = _query(_workflow(), "Finalize Role Task and PM L3 Decision")
    assert (
        query.count(
            "jsonb_typeof(d.decision_payload_json->'next_task_payload_json')='object'"
        )
        >= 2
    )
    _assert_tokens(
        query,
        (
            "d.role_id='PM_L3_DELIVERY_VALIDATION_OWNER'",
            "d.validation_status IN ('pass','conditional_pass')",
            "d.decision_type='create_next_role_task'",
            "jsonb_typeof(d.decision_payload_json)='object'",
            "NULLIF(d.decision_payload_json->>'next_role_id','') IS NOT NULL",
            "d.decision_payload_json ? 'next_task_payload_json'",
            "AND EXISTS(SELECT 1 FROM pm_l3_decision)",
        ),
    )


def test_child_schema_is_explicit_or_pm_l3_default_and_never_parent_inherited() -> None:
    query = _query(_workflow(), "Finalize Role Task and PM L3 Decision")
    queue_ctes = query.split("next_role_task_candidate AS", 1)[1].split(
        "task_update AS", 1
    )[0]
    schema_expression = (
        "COALESCE( "
        "NULLIF(d.decision_payload_json->>'next_expected_output_schema_ref',''), "
        "NULLIF(d.decision_payload_json->>'expected_output_schema_ref',''), "
        "CASE WHEN d.decision_payload_json->>'next_role_id' = "
        "'PM_L3_DELIVERY_VALIDATION_OWNER' "
        "THEN 'route_b_pm_l3_decision_loop.v0.1' ELSE NULL END )"
    )
    assert schema_expression in queue_ctes
    assert f"{schema_expression} AS expected_output_schema_ref" in queue_ctes
    assert f"{schema_expression} IS NOT NULL" in queue_ctes
    assert "n.expected_output_schema_ref IS NOT NULL" in queue_ctes
    assert "parent.expected_output_schema_ref" not in queue_ctes


def test_child_queue_row_preserves_ids_lineage_sequence_context_and_retries() -> None:
    query = _query(_workflow(), "Finalize Role Task and PM L3 Decision")
    queue_ctes = query.split("next_role_task_candidate AS", 1)[1].split(
        "task_update AS", 1
    )[0]
    _assert_tokens(
        queue_ctes,
        (
            "INSERT INTO public.route_b_role_task_queue",
            "'role_task_'||md5(",
            "d.role_task_id AS parent_role_task_id",
            "SELECT MAX(q.sequence_no)",
            "d.decision_payload_json->>'next_role_id' AS role_id",
            "d.decision_payload_json->'next_task_payload_json' AS task_payload_json",
            "jsonb_build_object( 'role_context_ref'",
            "jsonb_build_object( 'role_id', d.decision_payload_json->>'next_role_id' )",
            "WHERE n.context_refs_json #>> '{role_context_ref,role_id}' = n.role_id",
            "'role_task_ready', 0, n.max_retries",
            "LEAST(GREATEST(COALESCE(parent.max_retries,3),1),3)",
            "ON CONFLICT (role_task_id) DO NOTHING",
        ),
    )


def test_parent_terminal_and_legacy_state_paths_remain_safe() -> None:
    query = _query(_workflow(), "Finalize Role Task and PM L3 Decision")
    _assert_tokens(
        query,
        (
            "SET status=CASE WHEN d.validation_status IN ('pass','conditional_pass') THEN 'role_task_completed' ELSE 'role_task_failed' END",
            "d.validation_status IN ('fail','blocked') THEN 'failed'",
            "d.validation_status IN ('fail','blocked') THEN 'manual_review_required'",
            "d.decision_type='create_next_role_task' AND EXISTS(SELECT 1 FROM next_role_task) THEN 'queued'",
            "THEN 'next_role_task_queued' ELSE 'pm_l3_package_drafted' END",
            "ELSE 'ready_for_review' END",
            "THEN 'urr_create_next_role_task' ELSE 'urr_role_task_finalization' END",
        ),
    )


def test_workflow_has_no_direct_github_merge_server_or_runtime_permission() -> None:
    workflow_text = TARGET_PATH.read_text(encoding="utf-8")
    for forbidden in (
        "api.github.com",
        "github.com/Viktoryyyyy/moex-robot",
        'approved_for_merge": true',
        'n8n_merge_allowed": true',
        'executor_merge_allowed": true',
        'direct_main_write_allowed": true',
    ):
        assert forbidden not in workflow_text, forbidden
    _assert_tokens(
        workflow_text,
        (
            "'runtime_live_trading_allowed',false",
            "'broker_execution_allowed',false",
            "'production_secret_access_allowed',false",
        ),
    )
