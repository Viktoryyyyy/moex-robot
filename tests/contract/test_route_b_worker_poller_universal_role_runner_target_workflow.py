from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json"
BASELINE = ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json"
INTAKE = ROOT / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET.json"
APPROVED = {
    "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json",
    "tests/contract/test_route_b_worker_poller_universal_role_runner_target_workflow.py",
}


def workflow(path: Path = TARGET) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def node(name: str) -> dict:
    matches = [n for n in workflow()["nodes"] if n.get("name") == name]
    assert len(matches) == 1
    return matches[0]


def js(name: str) -> str:
    return node(name)["parameters"]["jsCode"]


def sql(name: str) -> str:
    return node(name)["parameters"]["query"]


def has(text: str, *tokens: str) -> None:
    for token in tokens:
        assert token in text, token


def blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


def test_artifact_scope_activation_and_protected_exports() -> None:
    target = workflow()
    baseline = workflow(BASELINE)
    assert target["name"] == "MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET"
    assert target["active"] is False
    assert APPROVED == {
        "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json",
        "tests/contract/test_route_b_worker_poller_universal_role_runner_target_workflow.py",
    }
    assert baseline["name"] == "MOEX_ROUTE_B_WORKER_POLLER_V1_10_3"
    assert baseline["project"] == "MOEX_Bot"
    assert baseline["status"] == "deprecated_historical"
    assert baseline["active"] is False
    assert baseline["new_tasks_allowed"] is False
    assert baseline["new_runtime_execution_allowed"] is False
    assert baseline["nodes"] == []
    assert baseline["connections"] == {}
    assert baseline["historical_source"] is True
    assert workflow(INTAKE)["name"] == "MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET"
    assert blob_sha(INTAKE) == "b2a19654add077f0ad72dd13b4e270bbcd21f44d"


def test_one_ai_call_and_no_direct_github_broker_live_runtime_node() -> None:
    nodes = workflow()["nodes"]
    ai = [
        n for n in nodes
        if n.get("type") == "n8n-nodes-base.httpRequest"
        and "ollama.com/api/chat" in json.dumps(n)
    ]
    assert len(ai) == 1
    assert ai[0]["name"] == "Universal Role AI Call"
    urls = "\n".join(str(n.get("parameters", {}).get("url", "")) for n in nodes).lower()
    for forbidden in (
        "api.github.com", "github.com/Viktoryyyyy/moex-robot", "alor", "tinkoff",
        "broker", "live-trading", "live_trading", "runtime/execution",
    ):
        assert forbidden not in urls
    assert ai[0]["credentials"]["httpHeaderAuth"]["name"] == "ollama_cloud_moex_route_b"
    for name in ("Claim One Role Task", "Persist Raw AI Role Output",
                 "Finalize Role Task and PM L3 Decision"):
        assert node(name)["credentials"]["postgres"]["name"] == "postgres_moex_n8n"


def test_claim_and_existing_create_next_contract_are_preserved() -> None:
    claim = sql("Claim One Role Task")
    has(
        claim, "FROM public.route_b_role_task_queue",
        "WHERE q.status = 'role_task_ready'", "q.attempt < q.max_retries",
        "q.context_refs_json #>> '{role_context_ref,role_id}' = q.role_id",
        "ORDER BY q.phase_run_id ASC, q.sequence_no ASC",
        "FOR UPDATE SKIP LOCKED", "LIMIT 1", "status = 'role_task_running'",
    )
    prompt = js("Build Universal Role Prompt")
    has(
        prompt, "Controlled create_next_role_task smoke rule",
        "urr_create_next_role_task_smoke_only", "controlled_create_next_role_task_path",
        '"next_role_id":"PM_L3_DELIVERY_VALIDATION_OWNER"',
        '"next_expected_output_schema_ref":"route_b_pm_l3_decision_loop.v0.1"',
        '"required_decision_type":"no_op"',
        "Return decision_type no_op. Do not create another role task.",
        "Never inherit the parent expected_output_schema_ref",
    )
    validation = js("Validate Untrusted Role Output")
    has(
        validation, "create_next_role_task_missing_decision_payload_json",
        "create_next_role_task_missing_next_role_id",
        "create_next_role_task_missing_next_task_payload_json",
        "create_next_role_task_next_task_payload_must_be_object",
        "create_next_role_task_missing_child_output_schema_ref",
        "create_next_role_task_child_schema_not_resolved",
    )


def test_controlled_request_prompt_and_complete_safe_example() -> None:
    prompt = js("Build Universal Role Prompt")
    has(
        prompt, "Controlled request_github_execution_package smoke rule",
        "urr_request_github_execution_package_smoke_only",
        "controlled_request_github_execution_package_path",
        "no direct GitHub execution", "no merge", "no server apply",
        "no broker/live/runtime", "only a structured GitHub execution request package",
        "no GitHub execution was performed",
    )
    marker = "valid request_github_execution_package JSON example: "
    line = next(line for line in prompt.splitlines() if marker in line)
    example = json.loads(line.split(marker, 1)[1][:-2])
    assert example["role_id"] == "PM_L3_DELIVERY_VALIDATION_OWNER"
    assert example["validation_status"] in {"pass", "conditional_pass"}
    assert example["decision_type"] == "request_github_execution_package"
    request = example["decision_payload_json"]["github_execution_request_json"]
    assert request == {
        "repository_full_name": "Viktoryyyyy/moex-robot",
        "lane": "route_b_n8n",
        "task_id": "route_b_urr_request_github_execution_package_path_v0_1_smoke",
        "requested_by_role": "PM_L3_DELIVERY_VALIDATION_OWNER",
        "execution_mode": "browser_chatgpt_github_direct",
        "branch_name": "route-b/request-github-execution-package-smoke-only",
        "base_ref": "origin/main",
        "approved_file_scope": [],
        "forbidden_file_scope": [
            "live trading", "broker", "production secrets", "server apply", "merge"
        ],
        "merge_authority": "PM_L2_ONLY",
        "server_apply_authority": "PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK",
        "runtime_allowed": False,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "direct_main_write_allowed": False,
        "force_push_allowed": False,
        "file_delete_allowed": False,
        "n8n_merge_allowed": False,
        "executor_merge_allowed": False,
        "production_secret_access_allowed": False,
        "ci_required": True,
        "expected_ci_workflow": "tests",
        "smoke_only": True,
        "server_apply_allowed": False,
        "pm_l2_approval_claimed": False,
    }


def test_request_validation_rejects_missing_invalid_and_unsafe_package() -> None:
    validation = js("Validate Untrusted Role Output")
    has(
        validation,
        "request_github_execution_package_missing_decision_payload_json",
        "request_github_execution_package_missing_github_execution_request_json",
        "else if(!obj(req))",
        "request_github_execution_package_github_execution_request_must_be_object",
        "request_github_execution_package_missing_repository_full_name",
        "request_github_execution_package_missing_branch_name",
        "request_github_execution_package_missing_base_ref",
        "request_github_execution_package_merge_authority_not_pm_l2_only",
        "req.runtime_allowed!==false",
        "request_github_execution_package_runtime_not_allowed",
        "req.broker_execution_allowed!==false",
        "request_github_execution_package_broker_not_allowed",
        "req.live_trading_allowed!==false",
        "request_github_execution_package_live_trading_not_allowed",
        "req.direct_main_write_allowed!==false",
        "request_github_execution_package_direct_main_write_not_allowed",
        "req.force_push_allowed!==false",
        "request_github_execution_package_force_push_not_allowed",
        "req.production_secret_access_allowed!==false",
        "request_github_execution_package_secret_access_not_allowed",
        "request_github_execution_package_execution_claim_not_allowed",
        "worker_poller_boundary_violation", "pm_l2_approval_claim_forbidden",
    )


def test_finalization_guards_authority_and_child_queue_isolation() -> None:
    query = sql("Finalize Role Task and PM L3 Decision")
    assert "$1::jsonb" in query
    assert node("Finalize Role Task and PM L3 Decision")["parameters"]["options"][
        "queryReplacement"
    ] == "={{ JSON.stringify($json) }}"
    assert re.findall(r"(?<!:):(?!:)[A-Za-z_][A-Za-z0-9_]*", query) == []
    has(
        query, "INSERT INTO public.route_b_pm_l3_decisions",
        "jsonb_typeof(d.decision_payload_json->'github_execution_request_json')='object'",
        "NULLIF(d.decision_payload_json->'github_execution_request_json'->>'repository_full_name','') IS NOT NULL",
        "NULLIF(d.decision_payload_json->'github_execution_request_json'->>'branch_name','') IS NOT NULL",
        "NULLIF(d.decision_payload_json->'github_execution_request_json'->>'base_ref','') IS NOT NULL",
        "->>'merge_authority'='PM_L2_ONLY'",
        "->'runtime_allowed'='false'::jsonb",
        "->'broker_execution_allowed'='false'::jsonb",
        "->'live_trading_allowed'='false'::jsonb",
        "->'direct_main_write_allowed'='false'::jsonb",
        "->'force_push_allowed'='false'::jsonb",
        "->'production_secret_access_allowed'='false'::jsonb",
        "'merge_authority','PM_L2_ONLY'", "'n8n_merge_allowed',false",
        "'direct_main_write_allowed',false", "'force_push_allowed',false",
        "'file_delete_allowed',false", "'executor_merge_allowed',false",
        "'ci_passed_is_not_merge_approval',true", "'server_apply_allowed',false",
        "'pm_l2_approval_claimed',false", "'runtime_live_trading_allowed',false",
        "'broker_execution_allowed',false", "'production_secret_access_allowed',false",
    )
    queue = query.split("next_role_task_candidate AS", 1)[1].split("task_update AS", 1)[0]
    assert "INSERT INTO public.route_b_role_task_queue" in queue
    assert "d.decision_type='create_next_role_task'" in queue
    assert "request_github_execution_package" not in queue
    assert "jsonb_typeof(d.decision_payload_json->'next_task_payload_json')='object'" in queue
    assert "parent.expected_output_schema_ref" not in queue


def test_request_finalization_and_existing_terminal_paths() -> None:
    query = sql("Finalize Role Task and PM L3 Decision")
    has(
        query,
        "SET status=CASE WHEN d.validation_status IN ('pass','conditional_pass') THEN 'role_task_completed' ELSE 'role_task_failed' END",
        "d.decision_type='request_github_execution_package' AND EXISTS(SELECT 1 FROM pm_l3_decision) THEN 'github_execution_package_drafted'",
        "THEN 'urr_request_github_execution_package'",
        "'github_execution_request_packaged'", "'repository_full_name'", "'branch_name'",
        "'merge_authority'", "'server_apply_authority'", "'pm_l3_decision_inserted'",
        "d.decision_type='create_next_role_task' AND EXISTS(SELECT 1 FROM next_role_task) THEN 'queued'",
        "THEN 'next_role_task_queued' ELSE 'pm_l3_package_drafted' END",
        "ELSE 'ready_for_review' END",
        "d.validation_status IN ('fail','blocked') THEN 'failed'",
        "d.validation_status IN ('fail','blocked') THEN 'manual_review_required'",
    )


def test_workflow_has_no_direct_execution_or_unsafe_true_authority() -> None:
    text = TARGET.read_text(encoding="utf-8")
    for forbidden in (
        "api.github.com", "github.com/Viktoryyyyy/moex-robot",
        'approved_for_merge": true', 'n8n_merge_allowed": true',
        'executor_merge_allowed": true', 'direct_main_write_allowed": true',
    ):
        assert forbidden not in text
    has(
        text, "'runtime_live_trading_allowed',false",
        "'broker_execution_allowed',false",
        "'production_secret_access_allowed',false",
    )
