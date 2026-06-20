from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = (
    ROOT
    / "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_EXECUTION_PATH_V0_1_TARGET.json"
)
APPROVED_FILE_SCOPE = {
    "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_EXECUTION_PATH_V0_1_TARGET.json",
    "tests/contract/test_route_b_github_executor_execution_path_target_workflow.py",
}
PROTECTED_EXPORTS = {
    ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json":
        "513b70e52294d66e9478a55765fa2f72eec4baec",
    ROOT / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_V1_10_3.json":
        "2d41eed8e2c7964a0a2998010db219aa711e5018",
    ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json":
        "ef7daafdf22e92ba1546b892b69178cc9d74a852",
    ROOT / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET.json":
        "b2a19654add077f0ad72dd13b4e270bbcd21f44d",
    ROOT / "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_INTAKE_V0_1_TARGET.json":
        "e97dbf55d6dcb11bb36091082d8283d496386882",
}
REQUIRED_FALSE_FLAGS = {
    "runtime_allowed",
    "broker_execution_allowed",
    "live_trading_allowed",
    "direct_main_write_allowed",
    "force_push_allowed",
    "file_delete_allowed",
    "n8n_merge_allowed",
    "executor_merge_allowed",
    "production_secret_access_allowed",
    "server_apply_allowed",
    "pm_l2_approval_claimed",
}
ALLOWED_GITHUB_ACTIONS = {
    "create_branch",
    "create_or_update_approved_files",
    "open_pull_request",
    "read_ci_status",
}


def workflow() -> dict:
    return json.loads(TARGET.read_text(encoding="utf-8"))


def node(name: str) -> dict:
    matches = [item for item in workflow()["nodes"] if item.get("name") == name]
    assert len(matches) == 1
    return matches[0]


def executor_js() -> str:
    return node(
        "Revalidate and Build Controlled GitHub Execution Path"
    )["parameters"]["jsCode"]


def blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


def quoted_items(block: str) -> set[str]:
    return set(re.findall(r"'([^']+)'", block))


def test_workflow_exists_is_inactive_and_changed_scope_is_exact() -> None:
    assert TARGET.is_file()
    target = workflow()
    assert target["name"] == (
        "MOEX_ROUTE_B_GITHUB_EXECUTOR_EXECUTION_PATH_V0_1_TARGET"
    )
    assert target["active"] is False
    assert set(target["meta"]["artifactChangeScope"]) == APPROVED_FILE_SCOPE
    assert APPROVED_FILE_SCOPE == {
        "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_EXECUTION_PATH_V0_1_TARGET.json",
        "tests/contract/test_route_b_github_executor_execution_path_target_workflow.py",
    }


def test_consumes_intake_validated_request_and_revalidates_contract() -> None:
    script = executor_js()
    for token in (
        "envelope.accepted === true",
        "envelope.validation_status === 'pass'",
        "envelope.github_execution_request_json",
        "envelope.normalized_github_execution_request_json",
        "validated_github_execution_request_required",
        "github_execution_request_json_must_be_object",
        "repository_full_name_required",
        "repository_full_name_not_allowed",
        "Viktoryyyyy/moex-robot",
        "branch_name_required",
        "branch_name_not_route_b_executor_branch",
        "branch_name_direct_main_forbidden",
        "base_ref_required",
        "base_ref_must_equal_origin_main",
        "origin/main",
        "approved_file_scope_must_be_array",
        "approved_file_scope_must_not_be_empty",
        "approved_file_scope_invalid_repo_relative_path:",
        "forbidden_file_scope_must_be_array",
        "forbidden_file_scope_must_not_be_empty",
        "approved_file_scope_intersects_forbidden_file_scope:",
        "candidate.merge_authority !== 'PM_L2_ONLY'",
        "candidate.server_apply_authority !== "
        "'PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK'",
    ):
        assert token in script


def test_rejects_every_unsafe_true_flag() -> None:
    script = executor_js()
    match = re.search(
        r"const REQUIRED_FALSE_FLAGS = \[(.*?)\];",
        script,
        flags=re.DOTALL,
    )
    assert match is not None
    assert quoted_items(match.group(1)) == REQUIRED_FALSE_FLAGS
    assert "candidate[flag] !== false" in script
    assert "flag + '_must_be_false'" in script


def test_only_allowed_github_actions_are_structurally_defined() -> None:
    target = workflow()
    script = executor_js()
    match = re.search(
        r"const ALLOWED_GITHUB_ACTIONS = \[(.*?)\];",
        script,
        flags=re.DOTALL,
    )
    assert match is not None
    assert quoted_items(match.group(1)) == ALLOWED_GITHUB_ACTIONS
    assert set(target["meta"]["allowedGitHubActions"]) == ALLOWED_GITHUB_ACTIONS
    assert {item["type"] for item in target["nodes"]} == {
        "n8n-nodes-base.webhook",
        "n8n-nodes-base.code",
        "n8n-nodes-base.respondToWebhook",
    }
    assert all("credentials" not in item for item in target["nodes"])
    for token in (
        "execution_mode: 'structural_definition_only'",
        "real_github_api_nodes_present: false",
        "credentials_embedded: false",
        "github_mutation_enabled: false",
        "github_mutation_performed: false",
    ):
        assert token in script


def test_file_writes_are_scope_guarded_and_main_force_delete_are_forbidden() -> None:
    script = executor_js()
    for token in (
        "action: 'create_or_update_approved_files'",
        "file_paths: approvedFileScope",
        "path_guard: 'approved_file_scope'",
        "forbidden_scope_guard: 'forbidden_file_scope'",
        "branch_source: 'branch_name'",
        "delete_allowed: false",
        "branch_name_direct_main_forbidden",
        "direct_main_write_allowed: false",
        "force_push_allowed: false",
        "file_delete_allowed: false",
        "force: false",
    ):
        assert token in script


def test_no_merge_server_apply_broker_live_runtime_or_secret_execution() -> None:
    target = workflow()
    script = executor_js()
    actions_match = re.search(
        r"const ALLOWED_GITHUB_ACTIONS = \[(.*?)\];",
        script,
        flags=re.DOTALL,
    )
    assert actions_match is not None
    action_surface = "\n".join(sorted(quoted_items(actions_match.group(1)))).lower()
    for forbidden in (
        "merge",
        "server",
        "broker",
        "live",
        "runtime",
        "secret",
        "delete",
        "force",
    ):
        assert forbidden not in action_surface

    urls = [
        str(item.get("parameters", {}).get("url", "")).strip().lower()
        for item in target["nodes"]
        if str(item.get("parameters", {}).get("url", "")).strip()
    ]
    assert urls == []
    webhook_path = node(
        "GitHub Executor Execution Path Webhook"
    )["parameters"]["path"]
    endpoint_surface = "\n".join(urls + [webhook_path.lower()])
    for forbidden in (
        "api.github.com",
        "alor",
        "tinkoff",
        "broker-api",
        "live-trading",
        "live_trading",
        "runtime/execution",
        "server-apply",
        "pulls/merge",
    ):
        assert forbidden not in endpoint_surface

    for token in (
        "merge_allowed: false",
        "merge_performed: false",
        "n8n_merge_allowed: false",
        "executor_merge_allowed: false",
        "server_apply_allowed: false",
        "server_apply_performed: false",
        "broker_execution_allowed: false",
        "live_trading_allowed: false",
        "runtime_allowed: false",
        "production_secret_access_allowed: false",
        "production_secret_accessed: false",
        "pm_l2_approval_claimed: false",
    ):
        assert token in script

    serialized = json.dumps(target, sort_keys=True).lower()
    for forbidden in (
        "n8n-nodes-base.github",
        "n8n-nodes-base.httprequest",
        "n8n-nodes-base.ssh",
        "n8n-nodes-base.executecommand",
        "process.env",
        "$env",
    ):
        assert forbidden not in serialized


def test_pm_l2_authorities_and_ci_gate_are_preserved() -> None:
    target = workflow()
    script = executor_js()
    assert target["meta"]["mergeAuthority"] == "PM_L2_ONLY"
    assert target["meta"]["serverApplyAuthority"] == (
        "PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK"
    )
    for token in (
        "merge_authority: 'PM_L2_ONLY'",
        "server_apply_authority: "
        "'PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK'",
        "ci_required_must_be_true",
        "expected_ci_workflow_required",
        "expected_ci_workflow_must_equal_tests",
        "expected_ci_workflow: 'tests'",
        "action: 'read_ci_status'",
        "read_only: true",
    ):
        assert token in script


def test_old_route_b_exports_are_byte_for_byte_unchanged() -> None:
    for path, expected_sha in PROTECTED_EXPORTS.items():
        assert path.is_file(), path
        assert blob_sha(path) == expected_sha, path
