from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_RUNTIME_V0_1_TARGET.json"
APPROVED_SCOPE = {
    "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_RUNTIME_V0_1_TARGET.json",
    "tests/contract/test_route_b_github_executor_credentialed_runtime_target_workflow.py",
}
PROTECTED = {
    ROOT / "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_EXECUTION_PATH_V0_1_TARGET.json":
        "4db1bb50726ca11f2d974a1197aae91b267770bc",
    ROOT / "docs/sot/MOEX_ROUTE_B_GITHUB_BRANCH_PR_EXECUTOR_V1_10_3.json":
        "aeaab1e677d1362624a05ff3d002ffa4d7a1bb04",
    ROOT / "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_INTAKE_V0_1_TARGET.json":
        "e97dbf55d6dcb11bb36091082d8283d496386882",
}
WRITE_NODES = {"Create Branch", "Create File Commit", "Open PR"}
READ_NODES = {
    "Fetch Main Ref", "Fetch Existing Branch", "Fetch Existing PRs",
    "Verify Branch", "Fetch PR After Create", "Fetch CI Runs",
}


def workflow() -> dict:
    return json.loads(TARGET.read_text(encoding="utf-8"))


def node(name: str) -> dict:
    rows = [item for item in workflow()["nodes"] if item.get("name") == name]
    assert len(rows) == 1, name
    return rows[0]


def code(name: str) -> str:
    return node(name)["parameters"]["jsCode"]


def blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


def test_target_is_inactive_create_only_runtime() -> None:
    target = workflow()
    meta = target["meta"]
    assert target["name"] == "MOEX_ROUTE_B_GITHUB_EXECUTOR_RUNTIME_V0_1_TARGET"
    assert target["active"] is False
    assert set(meta["artifactChangeScope"]) == APPROVED_SCOPE
    assert meta["targetMode"] == "github-runtime-pr-only-create-file-v0.1"
    assert meta["sourceStructuralWorkflow"] == (
        "MOEX_ROUTE_B_GITHUB_EXECUTOR_EXECUTION_PATH_V0_1_TARGET"
    )
    assert meta["runtimeCreatedBranchPrefix"] == "n8n/"
    assert meta["singleFileCreateOnly"] is True
    assert meta["updateRequestsRejected"] is True
    assert meta["multiFileRequestsRejected"] is True
    assert meta["idempotencyRequired"] is True
    assert meta["duplicateCommitAllowed"] is False
    assert meta["duplicatePullRequestAllowed"] is False


def test_structural_result_and_exact_single_create_are_required() -> None:
    script = code("Validate Runtime Request")
    for token in (
        "structural_validation_result_json",
        "approved_mutation_payload_json",
        "raw_input_forbidden",
        "structural_result_invalid",
        "execution_request_id_mismatch",
        "revalidated_normalized_github_execution_request_json",
        "single_file_scope_required",
        "create_file_mutation_invalid",
        "scope_mismatch",
        "forbidden_scope",
    ):
        assert token in script
    assert "m.operation!=='create'" in script
    assert "['create','update']" not in script


def test_repo_branch_ci_and_authority_guards_are_present() -> None:
    script = code("Validate Runtime Request")
    for token in (
        "Viktoryyyyy/moex-robot",
        "origin/main",
        "branch_invalid",
        "^n8n\\/",
        "PM_L2_ONLY",
        "PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK",
        "runtime_allowed",
        "force_push_allowed",
        "file_delete_allowed",
        "executor_merge_allowed",
        "tests_ci_required",
    ):
        assert token in script


def test_idempotency_reuses_only_matching_branch_and_pr() -> None:
    validate = code("Validate Runtime Request")
    decide = code("Decide Runtime Mode")
    for token in ("runtime_idempotency_key", "pr_marker", "route-b-runtime-key"):
        assert token in validate
    for token in (
        "multiple_matching_prs",
        "existing_branch_not_owned",
        "match[0].head.sha===bs",
        "replayed:true",
        "includes(c.pr_marker)",
        "stale_base_sha",
    ):
        assert token in decide


def test_real_github_nodes_use_only_named_credentials() -> None:
    target = workflow()
    http = [n for n in target["nodes"] if n["type"] == "n8n-nodes-base.httpRequest"]
    write = {
        n["name"] for n in http
        if n["credentials"]["httpHeaderAuth"]["name"] == "github_branch_write_moex_bot"
    }
    read = {
        n["name"] for n in http
        if n["credentials"]["httpHeaderAuth"]["name"] == "github_read_moex_bot"
    }
    assert write == WRITE_NODES
    assert read == READ_NODES
    assert target["meta"]["productionSecretValuesEmbedded"] is False
    serialized = json.dumps(target).lower()
    for forbidden in ("github_pat_", "ghp_", "\"authorization\"", "bearer ", "process.env", "$env"):
        assert forbidden not in serialized


def test_pr_only_flow_has_no_merge_force_delete_server_or_broker_endpoint() -> None:
    target = workflow()
    http = [n for n in target["nodes"] if n["type"] == "n8n-nodes-base.httpRequest"]
    surface = "\n".join(str(n["parameters"].get("url", "")) for n in http).lower()
    for required in ("/git/refs", "/contents/", "/pulls", "/actions/runs"):
        assert required in surface
    for forbidden in (
        "/merge", "pulls/merge", "server-apply", "server_apply",
        "broker", "live-trading", "live_trading",
    ):
        assert forbidden not in surface
    assert all(n["parameters"].get("method", "GET") not in {"PATCH", "DELETE"} for n in http)
    assert "force:true" not in json.dumps(target).replace(" ", "")


def test_terminal_result_requires_verified_commit_pr_and_no_merge_claim() -> None:
    script = code("Build Runtime Result")
    for token in (
        "file_commit_missing",
        "branch_verification_failed",
        "pr_not_confirmed",
        "implementation_commit_sha:commit",
        "pr_number:p.number",
        "pr_url:p.html_url",
        "pr_head_sha:p.head.sha",
        "applied_file_count:1",
        "all_requested_files_applied:true",
        "approved_for_merge:false",
        "merge_performed:false",
        "ci_success_is_merge_approval:false",
        "partial_apply_success_allowed:false",
    ):
        assert token in script


def test_old_route_b_exports_are_unchanged() -> None:
    for path, expected in PROTECTED.items():
        assert path.is_file(), path
        assert blob_sha(path) == expected, path
