from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_INTAKE_V0_1_TARGET.json"
APPROVED_FILE_SCOPE = {
    "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_INTAKE_V0_1_TARGET.json",
    "tests/contract/test_route_b_github_executor_intake_target_workflow.py",
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
}
REQUIRED_FALSE_FLAGS = {
    "direct_main_write_allowed",
    "force_push_allowed",
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


def validator_js() -> str:
    return node("Validate and Normalize GitHub Execution Request")["parameters"]["jsCode"]


def blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


def test_artifact_exists_is_inactive_and_declares_exact_approved_scope() -> None:
    assert TARGET.is_file()
    target = workflow()
    assert target["name"] == "MOEX_ROUTE_B_GITHUB_EXECUTOR_INTAKE_V0_1_TARGET"
    assert target["active"] is False
    assert APPROVED_FILE_SCOPE == {
        "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_INTAKE_V0_1_TARGET.json",
        "tests/contract/test_route_b_github_executor_intake_target_workflow.py",
    }


def test_validates_github_execution_request_json_object_and_required_identity() -> None:
    script = validator_js()
    for token in (
        "const request = envelope.github_execution_request_json",
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
    ):
        assert token in script


def test_validates_approved_and_forbidden_file_scope() -> None:
    script = validator_js()
    for token in (
        "approved_file_scope_must_be_array",
        "approved_file_scope_must_contain_unique_non_empty_strings",
        "approved_file_scope_invalid_repo_relative_path:",
        "forbidden_file_scope_must_be_array",
        "forbidden_file_scope_must_not_be_empty",
        "forbidden_file_scope_must_contain_unique_non_empty_strings",
        "approved_file_scope_intersects_forbidden_file_scope:",
        "isSafeRepoRelativePath",
        "forbiddenRuleMatches",
    ):
        assert token in script


def test_requires_authority_contract_and_rejects_every_unsafe_true_flag() -> None:
    script = validator_js()
    assert "candidate.merge_authority !== 'PM_L2_ONLY'" in script
    assert "merge_authority_must_equal_PM_L2_ONLY" in script
    assert (
        "candidate.server_apply_authority !== "
        "'PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK'"
    ) in script
    assert (
        "server_apply_authority_must_equal_"
        "PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK"
    ) in script

    match = re.search(
        r"const REQUIRED_FALSE_FLAGS = \[(.*?)\];",
        script,
        flags=re.DOTALL,
    )
    assert match is not None
    assert set(re.findall(r"'([^']+)'", match.group(1))) == REQUIRED_FALSE_FLAGS
    assert "candidate[flag] !== false" in script
    assert "flag + '_must_be_false'" in script
    assert "file_delete_allowed_must_be_false" in script


def test_validation_only_workflow_has_no_github_n8n_merge_or_server_apply_action() -> None:
    target = workflow()
    assert {item["type"] for item in target["nodes"]} == {
        "n8n-nodes-base.webhook",
        "n8n-nodes-base.code",
        "n8n-nodes-base.respondToWebhook",
    }
    assert all("credentials" not in item for item in target["nodes"])
    script = validator_js()
    for token in (
        "executor_action: 'validation_and_normalization_only'",
        "github_mutation_allowed: false",
        "github_mutation_performed: false",
        "merge_allowed: false",
        "merge_performed: false",
        "server_apply_allowed: false",
        "server_apply_performed: false",
        "n8n_mutation_allowed: false",
        "n8n_mutation_performed: false",
        "production_secret_access_allowed: false",
        "production_secret_accessed: false",
    ):
        assert token in script

    serialized = json.dumps(target, sort_keys=True).lower()
    for forbidden in (
        "api.github.com",
        "merge_pull_request",
        "git push",
        "git reset",
        "n8n-nodes-base.github",
        "n8n-nodes-base.postgres",
        "n8n-nodes-base.ssh",
        "n8n-nodes-base.executecommand",
        "process.env",
        "$env",
    ):
        assert forbidden not in serialized


def test_contains_no_broker_live_or_runtime_endpoint() -> None:
    target = workflow()
    urls = "\n".join(
        str(item.get("parameters", {}).get("url", ""))
        for item in target["nodes"]
    ).lower()
    assert urls == ""
    webhook_path = node("GitHub Executor Intake Webhook")["parameters"]["path"]
    assert webhook_path == "moex/route-b/github-executor-intake-v0-1"
    for forbidden in (
        "alor",
        "tinkoff",
        "broker-api",
        "live-trading",
        "live_trading",
        "/runtime",
        "runtime/execution",
    ):
        assert forbidden not in urls
        assert forbidden not in webhook_path.lower()


def test_old_route_b_exports_are_byte_for_byte_unchanged() -> None:
    for path, expected_sha in PROTECTED_EXPORTS.items():
        assert path.is_file(), path
        assert blob_sha(path) == expected_sha, path
