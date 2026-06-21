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
    "Create Feature Branch",
    "Create Git Blob",
    "Create Git Tree",
    "Create Implementation Commit",
    "Update Feature Branch Ref",
    "Open Pull Request",
    "Fetch CI Runs For Branch",
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


def test_workflow_is_inactive_and_exactly_scoped() -> None:
    target = workflow()
    assert target["name"] == "MOEX_ROUTE_B_GITHUB_EXECUTOR_CREDENTIALED_RUNTIME_V0_1_TARGET"
    assert target["active"] is False
    assert set(target["meta"]["artifactChangeScope"]) == APPROVED_SCOPE
    assert target["meta"]["targetMode"] == "credentialed-runtime-controlled-single-file-v0-1"
    assert target["meta"]["runtimeCreatedBranchPrefix"] == "n8n/"
    assert target["meta"]["supportedFileCount"] == 1
    assert target["meta"]["normalizedIntakeOnly"] is True
    assert target["meta"]["secretsEmbedded"] is False
    assert target["meta"]["mergeAuthority"] == "PM_L2_ONLY"


def test_contains_real_github_http_runtime_nodes_with_credential_refs_only() -> None:
    target = workflow()
    http_nodes = [item for item in target["nodes"] if item["type"] == "n8n-nodes-base.httpRequest"]
    assert {item["name"] for item in http_nodes} == HTTP_NODES
    surface = serialized()
    assert "api.github.com/repos/" in surface
    assert "github_read_moex_bot" in surface
    assert "github_branch_write_moex_bot" in surface
    for forbidden in ("ghp_", "github_pat_", "Bearer ", "process.env", "$env"):
        assert forbidden not in surface


def test_runtime_surface_is_pr_only_without_merge_or_server_apply() -> None:
    surface = serialized().lower()
    for required in (
        "/git/refs",
        "/git/blobs",
        "/git/trees",
        "/git/commits",
        "/git/refs/heads/",
        "/pulls",
        "/actions/runs",
    ):
        assert required in surface
    for forbidden in (
        "/pulls/merge",
        "force:true",
        "approved_for_merge:true",
        "merge_performed:true",
        "server_apply_allowed:true",
        "direct_main_write_allowed:true",
    ):
        assert forbidden not in surface


def test_validator_enforces_normalized_intake_n8n_branch_and_single_file_scope() -> None:
    script = node("Validate Normalized Runtime Request")["parameters"]["jsCode"]
    for token in (
        "raw_github_execution_request_json_forbidden",
        "validated_normalized_github_execution_request_required",
        "repository_full_name_not_allowed",
        "base_ref_must_equal_origin_main",
        "branch_name_must_use_n8n_prefix",
        "branch_name_forbidden",
        "exactly_one_file_change_required",
        "file_change_operation_must_be_create_or_update",
        "delete_operation_rejected",
        "merge_authority_must_equal_PM_L2_ONLY",
    ):
        assert token in script


def test_terminal_result_preserves_pm_l2_boundary() -> None:
    script = node("Build Terminal Runtime Result")["parameters"]["jsCode"]
    for token in (
        "implementation_commit_sha",
        "pr_number",
        "pr_url",
        "pr_head_sha",
        "ci_workflow:'tests'",
        "approved_for_merge:false",
        "merge_performed:false",
        "github_mutation_performed",
    ):
        assert token in script


def test_protected_route_b_exports_unchanged() -> None:
    for path, expected_sha in PROTECTED_EXPORTS.items():
        assert path.is_file(), path
        assert blob_sha(path) == expected_sha, path
