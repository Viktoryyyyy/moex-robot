from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_CREDENTIALED_RUNTIME_V0_1_TARGET.json"
PROTECTED_EXPORTS = {
    ROOT / "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_EXECUTION_PATH_V0_1_TARGET.json":
        "4db1bb50726ca11f2d974a1197aae91b267770bc",
    ROOT / "docs/sot/MOEX_ROUTE_B_GITHUB_BRANCH_PR_EXECUTOR_V1_10_3.json":
        "aeaab1e677d1362624a05ff3d002ffa4d7a1bb04",
}
APPROVED_SCOPE = {
    "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_CREDENTIALED_RUNTIME_V0_1_TARGET.json",
    "tests/contract/test_route_b_github_executor_credentialed_runtime_target_workflow.py",
}
HTTP_NODES = {
    "Fetch Base Branch Ref",
    "Fetch Base Commit",
    "Create Feature Branch",
    "Create Git Tree",
    "Create Implementation Commit",
    "Update Feature Branch Ref",
    "Open Pull Request",
}
POSTGRES_NODES = {
    "Load Execution Evidence Registry",
    "Load Workflow Run Projection",
    "Load Step Run Idempotency Checkpoint",
    "Persist Accepted Execution Evidence",
    "Persist Blocked Execution Evidence",
    "Persist Final Execution Evidence",
}
TERMINAL_NODES = {
    "Persist Blocked Execution Evidence",
    "Return Runtime Result",
}
WEBHOOK_NODE = "GitHub Executor Runtime Webhook"
VALIDATOR_NODE = "Validate Normalized Runtime Request"
EXPECTED_CONNECTIONS = {
    WEBHOOK_NODE: [VALIDATOR_NODE],
    VALIDATOR_NODE: ["Load Execution Evidence Registry"],
    "Load Execution Evidence Registry": ["Load Workflow Run Projection"],
    "Load Workflow Run Projection": ["Load Step Run Idempotency Checkpoint"],
    "Load Step Run Idempotency Checkpoint": ["Resolve Execution Evidence State"],
    "Resolve Execution Evidence State": [
        "Persist Accepted Execution Evidence",
        "Persist Blocked Execution Evidence",
    ],
    "Persist Accepted Execution Evidence": ["Fetch Base Branch Ref"],
    "Fetch Base Branch Ref": ["Fetch Base Commit"],
    "Fetch Base Commit": ["Validate Base Ref"],
    "Validate Base Ref": ["Build Git Tree Elements From Exact File Changes"],
    "Build Git Tree Elements From Exact File Changes": ["Create Feature Branch"],
    "Create Feature Branch": ["Create Git Tree"],
    "Create Git Tree": ["Create Implementation Commit"],
    "Create Implementation Commit": ["Update Feature Branch Ref"],
    "Update Feature Branch Ref": ["Open Pull Request"],
    "Open Pull Request": ["Persist Final Execution Evidence"],
    "Persist Final Execution Evidence": ["Build Terminal Runtime Result"],
    "Build Terminal Runtime Result": ["Return Runtime Result"],
}
FORBIDDEN_FALSE_CONTROLS = {
    "direct_main_write_allowed",
    "force_push_allowed",
    "file_delete_allowed",
    "n8n_merge_allowed",
    "executor_merge_allowed",
    "server_apply_allowed",
    "broker_execution_allowed",
    "live_trading_allowed",
    "runtime_allowed",
    "production_secret_access_allowed",
    "pm_l2_approval_claimed",
}


def workflow() -> dict:
    return json.loads(TARGET.read_text(encoding="utf-8"))


def node(name: str) -> dict:
    matches = [item for item in workflow()["nodes"] if item.get("name") == name]
    assert len(matches) == 1
    return matches[0]


def blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


def serialized() -> str:
    return json.dumps(workflow(), sort_keys=True)


def outgoing_targets(target: dict, node_name: str) -> list[str]:
    outputs = target["connections"][node_name]["main"]
    assert len(outputs) == 1
    return [edge["node"] for edge in outputs[0]]


def reachable_nodes(target: dict, start_node: str) -> set[str]:
    seen: set[str] = set()
    stack = [start_node]

    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)

        for output in target.get("connections", {}).get(current, {}).get("main", []):
            stack.extend(edge["node"] for edge in output)

    return seen


def test_workflow_is_inactive_and_exactly_scoped() -> None:
    target = workflow()
    assert target["name"] == "MOEX_ROUTE_B_GITHUB_EXECUTOR_CREDENTIALED_RUNTIME_V0_1_TARGET"
    assert target["active"] is False
    assert set(target["meta"]["artifactChangeScope"]) == APPROVED_SCOPE
    assert target["meta"]["targetMode"] == "credentialed-runtime-evidence-registry-v0-1"
    assert target["meta"]["runtimeCreatedBranchPrefix"] == "n8n/"
    assert target["meta"]["exactFileChangesRequired"] is True
    assert target["meta"]["evidenceRegistryTable"] == "public.moex_n8n_workflow_run_events"
    assert target["meta"]["currentStateProjectionTable"] == "public.moex_n8n_workflow_runs"
    assert target["meta"]["idempotencyCheckpointTable"] == "public.moex_n8n_workflow_step_runs"
    assert target["meta"]["supportedFileCount"] == "multi"
    assert target["meta"]["normalizedIntakeOnly"] is True
    assert target["meta"]["secretsEmbedded"] is False
    assert target["meta"]["mergeAuthority"] == "PM_L2_ONLY"
    assert target["meta"]["mergeAllowed"] is False
    assert target["meta"]["serverApplyAllowed"] is False


def test_webhook_trigger_node_routes_runtime_requests_to_validator() -> None:
    target = workflow()
    webhook = node(WEBHOOK_NODE)
    assert webhook["type"] == "n8n-nodes-base.webhook"
    assert webhook["parameters"]["httpMethod"] == "POST"
    assert outgoing_targets(target, WEBHOOK_NODE) == [VALIDATOR_NODE]


def test_forbidden_merge_server_live_and_broker_controls_remain_false() -> None:
    target = workflow()
    controls = target["meta"]["forbiddenControls"]
    assert set(controls) == FORBIDDEN_FALSE_CONTROLS
    for control_name in FORBIDDEN_FALSE_CONTROLS:
        assert controls[control_name] is False

    script = node(VALIDATOR_NODE)["parameters"]["jsCode"]
    for control_name in FORBIDDEN_FALSE_CONTROLS:
        assert f"{control_name}_must_be_false" in script


def test_connections_are_not_empty_and_match_expected_sequential_graph() -> None:
    target = workflow()
    assert target["connections"]

    node_names = {item["name"] for item in target["nodes"]}
    non_terminal = node_names - TERMINAL_NODES
    assert non_terminal == set(EXPECTED_CONNECTIONS)

    for source, expected_targets in EXPECTED_CONNECTIONS.items():
        assert outgoing_targets(target, source) == expected_targets

    for terminal in TERMINAL_NODES:
        assert terminal not in target["connections"]


def test_expected_graph_is_connected_from_webhook_trigger() -> None:
    target = workflow()
    assert reachable_nodes(target, WEBHOOK_NODE) == {item["name"] for item in target["nodes"]}


def test_all_nodes_have_readable_non_collapsed_positions() -> None:
    target = workflow()
    positions = []

    for item in target["nodes"]:
        assert "position" in item, item["name"]
        position = item["position"]
        assert isinstance(position, list), item["name"]
        assert len(position) == 2, item["name"]
        assert all(isinstance(value, (int, float)) for value in position), item["name"]
        positions.append(tuple(position))

    assert any(position != (0, 0) for position in positions)
    assert len(set(positions)) > 1
    assert len(set(positions)) == len(target["nodes"])


def test_contains_real_github_http_nodes_and_postgres_registry_nodes_with_credential_refs_only() -> None:
    target = workflow()
    http_nodes = [item for item in target["nodes"] if item["type"] == "n8n-nodes-base.httpRequest"]
    postgres_nodes = [item for item in target["nodes"] if item["type"] == "n8n-nodes-base.postgres"]
    assert {item["name"] for item in http_nodes} == HTTP_NODES
    assert {item["name"] for item in postgres_nodes} == POSTGRES_NODES

    surface = serialized()
    assert "api.github.com/repos/" in surface
    assert "github_read_moex_bot" in surface
    assert "github_branch_write_moex_bot" in surface
    assert "route_b_executor_runtime_pg" in surface
    for forbidden in ("ghp_", "github_pat_", "Bearer ", "password", "process.env", "$env"):
        assert forbidden not in surface


def test_runtime_uses_event_payload_as_full_registry_workflow_runs_as_projection_and_step_runs_as_checkpoint() -> None:
    surface = serialized()
    for required in (
        "public.moex_n8n_workflow_run_events",
        "event_payload_json",
        "public.moex_n8n_workflow_runs",
        "current_state_json",
        "public.moex_n8n_workflow_step_runs",
        "input_json",
        "output_json",
        "route_b_github_executor_execution_accepted",
        "route_b_github_executor_execution_blocked",
        "route_b_github_executor_execution_result",
    ):
        assert required in surface
    assert "Load Execution Evidence Registry" in surface
    assert "Load Workflow Run Projection" in surface
    assert "Load Step Run Idempotency Checkpoint" in surface


def test_validator_enforces_normalized_intake_identity_exact_file_changes_and_no_task_id_fallback() -> None:
    script = node(VALIDATOR_NODE)["parameters"]["jsCode"]
    for token in (
        "raw_github_execution_request_json_forbidden",
        "validated_normalized_github_execution_request_required",
        "repository_full_name_not_allowed",
        "base_ref_must_equal_origin_main",
        "valid_base_sha_required",
        "branch_name_must_use_n8n_prefix",
        "branch_name_forbidden",
        "execution_request_id_required",
        "workflow_run_id_required",
        "role_task_id_required",
        "request_fingerprint_sha256_required",
        "execution_request_id_task_id_fallback_forbidden",
        "approved_file_scope_must_not_be_empty",
        "exact_file_changes_required",
        "file_change_count_must_equal_approved_file_scope",
        "file_change_path_must_equal_approved_file_scope",
        "file_change_operation_must_be_create_or_update",
        "merge_authority_must_equal_PM_L2_ONLY",
    ):
        assert token in script
    assert "request.execution_request_id || request.task_id" not in script
    assert "request.execution_request_id||request.task_id" not in script


def test_resolution_logic_is_idempotent_and_reuses_existing_commit_or_pr_deterministically() -> None:
    script = node("Resolve Execution Evidence State")["parameters"]["jsCode"]
    for token in (
        "same_execution_request_id_different_fingerprint",
        "resolution: 'blocked'",
        "resolution: 'return_existing_result'",
        "resolution: 'resume_or_return_existing_result'",
        "resolution: 'proceed_new_execution'",
        "request_fingerprint_sha256",
        "existing_result",
        "create_implementation_commit",
        "open_pull_request",
        "implementation_commit_sha",
        "pr_number",
        "pr_url",
        "approved_for_merge: false",
        "merge_performed: false",
    ):
        assert token in script


def test_runtime_surface_is_pr_only_without_merge_or_server_apply() -> None:
    surface = serialized().lower()
    for required in (
        "/git/ref/heads/main",
        "/git/commits/",
        "/git/refs",
        "/git/trees",
        "/git/commits",
        "/git/refs/heads/",
        "/pulls",
    ):
        assert required in surface
    for forbidden in (
        "/pulls/merge",
        "force:true",
        "approved_for_merge:true",
        "merge_performed:true",
        "server_apply_allowed:true",
        "direct_main_write_allowed:true",
        "git push",
        "git reset",
        "n8n-nodes-base.ssh",
        "n8n-nodes-base.executecommand",
    ):
        assert forbidden not in surface


def test_tree_creation_uses_exact_file_changes_with_base_tree_before_commit() -> None:
    assert node("Build Git Tree Elements From Exact File Changes")["type"] == "n8n-nodes-base.code"
    build_script = node("Build Git Tree Elements From Exact File Changes")["parameters"]["jsCode"]
    assert "request.file_changes.map" in build_script
    assert "path: change.path" in build_script
    assert "content: change.content" in build_script
    assert "type: 'blob'" in build_script
    assert "mode: '100644'" in build_script

    create_tree_body = node("Create Git Tree")["parameters"]["jsonBody"]
    assert "base_tree" in create_tree_body
    assert "base_tree_sha" in create_tree_body
    assert "tree:$node['Build Git Tree Elements From Exact File Changes'].json.tree_elements" in create_tree_body

    base_script = node("Validate Base Ref")["parameters"]["jsCode"]
    assert "base_sha_not_current_main" in base_script
    assert "missing_base_tree_sha" in base_script


def test_terminal_result_and_persisted_events_keep_merge_flags_explicitly_false() -> None:
    terminal_script = node("Build Terminal Runtime Result")["parameters"]["jsCode"]
    final_query = node("Persist Final Execution Evidence")["parameters"]["query"]
    for token in (
        "execution_request_id",
        "request_fingerprint_sha256",
        "implementation_commit_sha",
        "pr_number",
        "pr_url",
        "pr_head_sha",
        "ci_workflow: 'tests'",
        "approved_for_merge: false",
        "merge_performed: false",
        "github_mutation_performed",
    ):
        assert token in terminal_script
    for token in (
        "approved_for_merge: false",
        "merge_performed: false",
        "execution_request_id",
        "request_fingerprint_sha256",
        "file_changes",
        "implementation_commit_sha",
        "pr_number",
    ):
        assert token in final_query


def test_protected_route_b_exports_unchanged() -> None:
    for path, expected_sha in PROTECTED_EXPORTS.items():
        assert path.is_file(), path
        assert blob_sha(path) == expected_sha, path
