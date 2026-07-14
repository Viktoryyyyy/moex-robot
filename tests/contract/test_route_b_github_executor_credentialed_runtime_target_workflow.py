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
WEBHOOK_NODE = "GitHub Executor Runtime Webhook"
VALIDATOR_NODE = "Validate Normalized Runtime Request"
RESOLVE_NODE = "Resolve Execution Evidence State"
SWITCH_NODE = "Route Execution Resolution"
RETURN_NODE = "Return Runtime Result"
GITHUB_MUTATION_CHAIN = [
    "Fetch Base Branch Ref",
    "Fetch Base Commit",
    "Validate Base Ref",
    "Build Git Tree Elements From Exact File Changes",
    "Create Feature Branch",
    "Create Git Tree",
    "Create Implementation Commit",
    "Update Feature Branch Ref",
    "Open Pull Request",
    "Persist Final Execution Evidence",
]
GITHUB_MUTATION_NODES = set(GITHUB_MUTATION_CHAIN)
EXPECTED_CONNECTIONS = {
    WEBHOOK_NODE: [[VALIDATOR_NODE]],
    VALIDATOR_NODE: [["Load Execution Evidence Registry"]],
    "Load Execution Evidence Registry": [["Load Workflow Run Projection"]],
    "Load Workflow Run Projection": [["Load Step Run Idempotency Checkpoint"]],
    "Load Step Run Idempotency Checkpoint": [[RESOLVE_NODE]],
    RESOLVE_NODE: [[SWITCH_NODE]],
    SWITCH_NODE: [
        ["Persist Accepted Execution Evidence"],
        ["Persist Blocked Execution Evidence"],
        [RETURN_NODE],
        [RETURN_NODE],
    ],
    "Persist Accepted Execution Evidence": [["Fetch Base Branch Ref"]],
    "Fetch Base Branch Ref": [["Fetch Base Commit"]],
    "Fetch Base Commit": [["Validate Base Ref"]],
    "Validate Base Ref": [["Build Git Tree Elements From Exact File Changes"]],
    "Build Git Tree Elements From Exact File Changes": [["Create Feature Branch"]],
    "Create Feature Branch": [["Create Git Tree"]],
    "Create Git Tree": [["Create Implementation Commit"]],
    "Create Implementation Commit": [["Update Feature Branch Ref"]],
    "Update Feature Branch Ref": [["Open Pull Request"]],
    "Open Pull Request": [["Persist Final Execution Evidence"]],
    "Persist Final Execution Evidence": [["Build Terminal Runtime Result"]],
    "Build Terminal Runtime Result": [[RETURN_NODE]],
    "Persist Blocked Execution Evidence": [[RETURN_NODE]],
}
TERMINAL_NODES = {RETURN_NODE}
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


def connection_outputs(target: dict, node_name: str) -> list[list[str]]:
    outputs = target["connections"][node_name]["main"]
    return [[edge["node"] for edge in output] for output in outputs]


def outgoing_targets(target: dict, node_name: str) -> list[str]:
    outputs = connection_outputs(target, node_name)
    assert len(outputs) == 1
    return outputs[0]


def reachable_nodes_from_output(target: dict, start_node: str, output_index: int) -> set[str]:
    outputs = target["connections"][start_node]["main"]
    stack = [edge["node"] for edge in outputs[output_index]]
    seen: set[str] = set()

    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        for output in target.get("connections", {}).get(current, {}).get("main", []):
            stack.extend(edge["node"] for edge in output)

    return seen


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


def test_switch_node_routes_execution_resolution_by_json_resolution() -> None:
    target = workflow()
    switch = node(SWITCH_NODE)
    assert switch["type"] == "n8n-nodes-base.switch"

    rules = switch["parameters"]["rules"]["values"]
    assert [rule["outputKey"] for rule in rules] == [
        "proceed_new_execution",
        "blocked",
        "return_existing_result",
        "resume_or_return_existing_result",
    ]

    for rule in rules:
        conditions = rule["conditions"]["conditions"]
        assert len(conditions) == 1
        condition = conditions[0]
        assert condition["leftValue"] == "={{ $json.resolution }}"
        assert condition["operator"]["operation"] == "equals"
        assert condition["rightValue"] == rule["outputKey"]

    assert connection_outputs(target, SWITCH_NODE) == [
        ["Persist Accepted Execution Evidence"],
        ["Persist Blocked Execution Evidence"],
        [RETURN_NODE],
        [RETURN_NODE],
    ]


def test_resolve_evidence_state_routes_only_to_switch_and_has_no_unsafe_direct_fanout() -> None:
    target = workflow()
    assert outgoing_targets(target, RESOLVE_NODE) == [SWITCH_NODE]
    assert "Persist Accepted Execution Evidence" not in outgoing_targets(target, RESOLVE_NODE)
    assert "Persist Blocked Execution Evidence" not in outgoing_targets(target, RESOLVE_NODE)


def test_connections_are_not_empty_and_match_expected_conditional_graph() -> None:
    target = workflow()
    assert target["connections"]

    node_names = {item["name"] for item in target["nodes"]}
    non_terminal = node_names - TERMINAL_NODES
    assert non_terminal == set(EXPECTED_CONNECTIONS)

    for source, expected_outputs in EXPECTED_CONNECTIONS.items():
        assert connection_outputs(target, source) == expected_outputs

    for terminal in TERMINAL_NODES:
        assert terminal not in target["connections"]


def test_expected_graph_is_connected_from_webhook_trigger() -> None:
    target = workflow()
    assert reachable_nodes(target, WEBHOOK_NODE) == {item["name"] for item in target["nodes"]}


def test_only_switch_output_zero_can_reach_github_mutation_nodes() -> None:
    target = workflow()

    proceed = reachable_nodes_from_output(target, SWITCH_NODE, 0)
    assert GITHUB_MUTATION_NODES.issubset(proceed)

    for output_index in (1, 2, 3):
        branch_reachable = reachable_nodes_from_output(target, SWITCH_NODE, output_index)
        assert RETURN_NODE in branch_reachable
        assert not (branch_reachable & GITHUB_MUTATION_NODES), (
            output_index,
            branch_reachable & GITHUB_MUTATION_NODES,
        )


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
    script = node(RESOLVE_NODE)["parameters"]["jsCode"]
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


def test_owner_repo_are_derived_from_repository_full_name_and_token_required() -> None:
    script = node(VALIDATOR_NODE)["parameters"]["jsCode"]
    assert "repository_full_name_parts_required" in script
    assert "repository_full_name.split('/')" in script
    assert "const owner=repository_full_name_parts[0]" in script
    assert "const repo=repository_full_name_parts[1]" in script
    assert "repository_full_name!=='Viktoryyyyy/moex-robot'" in script
    assert "owner,repo,approved_for_merge:false,merge_performed:false" in script


def test_load_execution_evidence_registry_is_scoped_not_event_type_only() -> None:
    query = node("Load Execution Evidence Registry")["parameters"]["query"]
    assert "event_type IN" in query
    assert "workflow_run_id = '{{ $json.workflow_run_id }}'" in query
    assert "event_payload_json->>'execution_request_id'" in query
    assert "event_payload_json->>'request_fingerprint_sha256'" in query
    assert " OR " in query
    assert workflow()["meta"]["evidenceRegistryOrdering"] == "not_declared"
    assert (
        "deterministic_ordering_blocked_no_known_timestamp_or_id_column_in_scope"
        in workflow()["meta"]["remainingGaps"]
    )


def test_resolve_execution_evidence_state_has_no_stub_logic_and_reads_payload_rows() -> None:
    script = node(RESOLVE_NODE)["parameters"]["jsCode"]
    compact_script = "".join(script.split())
    for forbidden in ("constexisting_result=null", "if(false)"):
        assert forbidden not in compact_script
    for required in (
        "$items('LoadExecutionEvidenceRegistry')",
        "event_payload_json",
        "same_execution_request_id_different_fingerprint",
        "resolution:'blocked'",
        "resolution:'return_existing_result'",
        "resolution:'resume_or_return_existing_result'",
        "resolution:'proceed_new_execution'",
        "implementation_commit_sha",
        "pr_number",
        "pr_url",
        "approved_for_merge:false",
        "merge_performed:false",
    ):
        assert required in compact_script


def test_persist_accepted_and_blocked_events_are_not_empty_jsonb() -> None:
    accepted = node("Persist Accepted Execution Evidence")["parameters"]["query"]
    blocked = node("Persist Blocked Execution Evidence")["parameters"]["query"]
    assert "'{}'::jsonb" not in accepted
    assert "'{}'::jsonb" not in blocked
    for required in (
        "execution_request_id",
        "request_fingerprint_sha256",
        "workflow_run_id",
        "role_task_id",
        "status:'accepted'",
        "approved_for_merge:false",
        "merge_performed:false",
    ):
        assert required in accepted
    for required in (
        "execution_request_id",
        "request_fingerprint_sha256",
        "workflow_run_id",
        "role_task_id",
        "status:'blocked'",
        "blocker_code",
        "error",
        "approved_for_merge:false",
        "merge_performed:false",
    ):
        assert required in blocked


def test_blocked_existing_and_resume_paths_cannot_reach_github_mutation_nodes() -> None:
    target = workflow()
    assert connection_outputs(target, SWITCH_NODE) == [
        ["Persist Accepted Execution Evidence"],
        ["Persist Blocked Execution Evidence"],
        [RETURN_NODE],
        [RETURN_NODE],
    ]
    assert GITHUB_MUTATION_NODES.issubset(
        reachable_nodes_from_output(target, SWITCH_NODE, 0)
    )
    for output_index in (1, 2, 3):
        branch_reachable = reachable_nodes_from_output(target, SWITCH_NODE, output_index)
        assert RETURN_NODE in branch_reachable
        assert not (branch_reachable & GITHUB_MUTATION_NODES), (
            output_index,
            branch_reachable & GITHUB_MUTATION_NODES,
        )
