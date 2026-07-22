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
CANONICAL_BRANCH_CONTRACT = (
    ROOT / "src/moex_core/contracts/route_b_github_execution.py"
)
CANONICAL_BRANCH_DOC = (
    ROOT / "docs/sot/route_b/github_branch_pr_executor.v1.md"
)
APPROVED_FILE_SCOPE = {
    "docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_EXECUTION_PATH_V0_1_TARGET.json",
    "tests/contract/test_route_b_github_executor_execution_path_target_workflow.py",
}
PROTECTED_EXPORTS = {
    ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_V1_10_3.json":
        "284d13f81ff20955185d59fab9bb6ec4d2a8f575",
    ROOT / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_V1_10_3.json":
        "2d41eed8e2c7964a0a2998010db219aa711e5018",
    ROOT / "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json":
        "ef7daafdf22e92ba1546b892b69178cc9d74a852",
    ROOT / "docs/sot/MOEX_ROUTE_B_INTAKE_ACK_ROLE_TASK_ADAPTER_V0_1_TARGET.json":
        "b2a19654add077f0ad72dd13b4e270bbcd21f44d",
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
ALLOWED_FILE_OPERATIONS = {"create", "update"}
TERMINAL_RESULT_REQUIRED_FIELDS = {
    "execution_request_id",
    "status",
    "repository_full_name",
    "base_ref",
    "base_sha",
    "feature_branch",
    "implementation_commit_sha",
    "pr_number",
    "pr_url",
    "pr_head_sha",
    "ci_workflow",
    "ci_run_id",
    "ci_conclusion",
    "requested_file_count",
    "applied_file_count",
    "all_requested_files_applied",
    "approved_for_merge",
    "merge_performed",
    "error",
}
TERMINAL_STATUSES = {
    "blocked",
    "failed",
    "pr_opened",
    "ci_failed",
    "ci_passed",
    "smoke_validated",
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


def array_items(script: str, constant_name: str) -> set[str]:
    match = re.search(
        rf"const {re.escape(constant_name)} = \[(.*?)\];",
        script,
        flags=re.DOTALL,
    )
    assert match is not None
    return quoted_items(match.group(1))


def test_workflow_is_inactive_scoped_and_has_no_bound_credentials() -> None:
    target = workflow()
    assert TARGET.is_file()
    assert target["name"] == (
        "MOEX_ROUTE_B_GITHUB_EXECUTOR_EXECUTION_PATH_V0_1_TARGET"
    )
    assert target["active"] is False
    assert set(target["meta"]["artifactChangeScope"]) == APPROVED_FILE_SCOPE
    assert set(target["meta"]["allowedGitHubActions"]) == ALLOWED_GITHUB_ACTIONS
    assert set(target["meta"]["allowedFileOperations"]) == ALLOWED_FILE_OPERATIONS
    assert target["meta"]["normalizedIntakeOnly"] is True
    assert target["meta"]["runtimeCreatedBranchPrefix"] == "n8n/"
    assert target["meta"]["idempotencyRequired"] is True
    assert target["meta"]["duplicateCommitAllowed"] is False
    assert target["meta"]["duplicatePullRequestAllowed"] is False
    assert target["meta"]["smokeEmptyScopeMutationAllowed"] is False
    assert target["meta"]["partialApplySuccessAllowed"] is False
    assert target["meta"]["terminalResultSchemaVersion"] == (
        "github_executor_execution_result.v0.1"
    )
    assert {item["type"] for item in target["nodes"]} == {
        "n8n-nodes-base.webhook",
        "n8n-nodes-base.code",
        "n8n-nodes-base.respondToWebhook",
    }
    assert all("credentials" not in item for item in target["nodes"])
    assert all(
        not str(item.get("parameters", {}).get("url", "")).strip()
        for item in target["nodes"]
    )


def test_consumes_only_intake_normalized_package_and_rejects_raw_input() -> None:
    script = executor_js()
    for token in (
        "envelope.accepted === true",
        "envelope.validation_status === 'pass'",
        "const request = envelope.normalized_github_execution_request_json",
        "validated_normalized_github_execution_request_required",
        "raw_github_execution_request_json_forbidden",
        "normalized_github_execution_request_json_must_be_object",
        "revalidated_normalized_github_execution_request_json",
        "normalized_intake_only: true",
        "raw_github_execution_request_allowed: false",
    ):
        assert token in script
    assert (
        "isPlainObject(envelope.github_execution_request_json)"
        not in script
    )
    assert (
        "? envelope.github_execution_request_json"
        not in script
    )


def test_runtime_branch_prefix_matches_canonical_n8n_boundary() -> None:
    script = executor_js()
    contract = CANONICAL_BRANCH_CONTRACT.read_text(encoding="utf-8")
    branch_doc = CANONICAL_BRANCH_DOC.read_text(encoding="utf-8")
    for token in (
        "branchName.startsWith('route-b/')",
        "route_b_branch_prefix_forbidden",
        "/^n8n\\/[A-Za-z0-9][A-Za-z0-9._/-]*$/",
        "branch_name_must_use_n8n_prefix",
        "branch_name_direct_main_or_master_forbidden",
        "required_prefix: 'n8n/'",
    ):
        assert token in script
    assert "^(route-b|n8n)" not in script
    assert "if value.startswith('route-b/')" in contract
    assert "if not value.startswith('n8n/')" in contract
    assert "The branch prefix must be `n8n/`." in branch_doc
    assert "`route-b/` is rejected." in branch_doc


def test_repository_base_and_authority_contract_are_revalidated() -> None:
    script = executor_js()
    for token in (
        "Viktoryyyyy/moex-robot",
        "repository_full_name_required",
        "repository_full_name_not_allowed",
        "task_id_required_for_idempotency",
        "base_ref_required",
        "base_ref_must_equal_origin_main",
        "base_ref: 'origin/main'",
        "candidate.merge_authority !== 'PM_L2_ONLY'",
        "candidate.server_apply_authority !== "
        "'PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK'",
        "merge_authority: 'PM_L2_ONLY'",
        "server_apply_authority: "
        "'PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK'",
    ):
        assert token in script


def test_mutation_scope_is_exact_nonempty_and_smoke_empty_scope_is_read_only() -> None:
    script = executor_js()
    for token in (
        "approved_file_scope_must_be_array",
        "approved_file_scope_must_contain_unique_non_empty_strings",
        "approved_file_scope_invalid_repo_relative_path:",
        "approved_file_scope_must_not_be_empty_before_mutation",
        "smoke_only_approved_file_scope_must_be_empty",
        "const requestedChangedPaths = approvedFileScope.slice()",
        "approved_file_scope_is_exact_requested_scope: true",
        "requested_changed_paths_source: 'approved_file_scope_exact'",
        "execution_mode: 'smoke_validation_only'",
        "github_actions: []",
        "github_mutation_allowed: false",
        "branch_creation_allowed: false",
        "commit_creation_allowed: false",
        "pull_request_creation_allowed: false",
        "smoke_empty_scope_mutation_allowed: false",
    ):
        assert token in script


def test_every_requested_write_is_revalidated_and_only_create_update_exist() -> None:
    script = executor_js()
    assert array_items(script, "ALLOWED_FILE_OPERATIONS") == (
        ALLOWED_FILE_OPERATIONS
    )
    for token in (
        "function validatePathBeforeWrite(",
        "requested_changed_path_outside_approved_file_scope:",
        "requested_changed_path_intersects_forbidden_file_scope:",
        "validatePathBeforeWrite(path, approvedFileScope, forbiddenFileScope)",
        "source: 'github_path_existence_at_pinned_base_sha'",
        "when_absent: 'create'",
        "when_present: 'update'",
        "explicit_operation_required_before_write: true",
        "path_must_be_in_approved_file_scope: true",
        "path_must_be_outside_forbidden_file_scope: true",
        "before_each_write_guard: 'validatePathBeforeWrite'",
        "delete_operation_rejected: true",
    ):
        assert token in script
    operation_surface = "\n".join(sorted(ALLOWED_FILE_OPERATIONS)).lower()
    assert "delete" not in operation_surface
    assert "remove" not in operation_surface


def test_deterministic_idempotency_and_replay_policy_prevent_duplicates() -> None:
    script = executor_js()
    for token in (
        "function stableStringify(",
        "function fnv1a32(",
        "function deriveExecutionRequestId(",
        "'route_b_exec_' + fnv1a32",
        "execution_request_id: executionRequestId",
        "lookup_required_before_any_mutation: true",
        "durable_state_store_required: true",
        "replay_same_key: "
        "'resume_from_recorded_step_or_return_existing_terminal_result'",
        "duplicate_commit_allowed: false",
        "duplicate_pull_request_allowed: false",
        "same_execution_request_id: 'reuse'",
        "different_execution_request_id: 'blocked'",
        "matching_open_pr: 'reuse'",
        "multiple_matching_open_prs: 'blocked'",
        "closed_or_merged_pr: "
        "'return_existing_terminal_result_or_blocked'",
        "existing_same_request: 'reuse'",
        "existing_different_request: 'blocked'",
        "existing_matching_open_pr: 'reuse'",
    ):
        assert token in script


def test_terminal_result_contract_is_complete_and_failure_safe() -> None:
    script = executor_js()
    assert array_items(script, "TERMINAL_STATUSES") == TERMINAL_STATUSES
    required_match = re.search(
        r"required_fields: \[(.*?)\],\n  allowed_statuses:",
        script,
        flags=re.DOTALL,
    )
    assert required_match is not None
    assert quoted_items(required_match.group(1)) == (
        TERMINAL_RESULT_REQUIRED_FIELDS
    )
    for token in (
        "schema_version: 'github_executor_execution_result.v0.1'",
        "mutation_result_required_non_null_fields",
        "'feature_branch'",
        "'implementation_commit_sha'",
        "'pr_number'",
        "'pr_url'",
        "'pr_head_sha'",
        "blocked_or_failed_requires_error_object: true",
        "pr_head_sha_must_equal_implementation_commit_sha: true",
        "blocked_or_failed_on_error: true",
        "partial_apply_reports_failed: true",
        "return terminal blocked result with validation_errors",
    ):
        assert token in script


def test_multi_file_batch_cannot_report_partial_success() -> None:
    script = executor_js()
    for token in (
        "stage_all_files_before_commit: true",
        "single_commit_for_batch: true",
        "before_each_write_revalidation: true",
        "partial_apply_success_allowed: false",
        "partial_apply_terminal_status: 'failed'",
        "partial_multi_file_apply_must_be_failed: true",
        "partial_multi_file_apply_can_report_success: false",
        "success_requires_applied_file_count_equals_requested_file_count: true",
        "success_requires_all_requested_files_applied: true",
    ):
        assert token in script


def test_all_unsafe_flags_are_required_false() -> None:
    script = executor_js()
    assert array_items(script, "REQUIRED_FALSE_FLAGS") == (
        REQUIRED_FALSE_FLAGS
    )
    assert "candidate[flag] !== false" in script
    assert "flag + '_required_false'" in script
    assert "flag + '_must_be_false'" in script


def test_ci_is_read_only_evidence_and_never_merge_approval() -> None:
    target = workflow()
    script = executor_js()
    assert target["meta"]["mergeAuthority"] == "PM_L2_ONLY"
    assert target["meta"]["serverApplyAuthority"] == (
        "PM_L2_ONLY_OR_EXPLICIT_PM_L2_TASK"
    )
    for token in (
        "ci_required_must_be_true",
        "expected_ci_workflow_required",
        "expected_ci_workflow_must_equal_tests",
        "expected_ci_workflow: 'tests'",
        "action: 'read_ci_status'",
        "read_only: true",
        "ci_success_is_merge_approval: false",
        "ci_passed_does_not_imply_merge_approval: true",
        "approved_for_merge_must_be_false: true",
        "merge_performed_must_be_false: true",
        "merge_allowed: false",
        "merge_performed: false",
        "pm_l2_approval_claimed: false",
    ):
        assert token in script


def test_no_merge_server_apply_runtime_broker_secret_or_real_github_nodes() -> None:
    target = workflow()
    script = executor_js()
    assert array_items(script, "ALLOWED_GITHUB_ACTIONS") == (
        ALLOWED_GITHUB_ACTIONS
    )
    action_surface = "\n".join(sorted(ALLOWED_GITHUB_ACTIONS)).lower()
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

    for token in (
        "real_github_api_nodes_present: false",
        "credentials_embedded: false",
        "github_mutation_enabled: false",
        "github_mutation_performed: false",
        "direct_main_write_allowed: false",
        "force_push_allowed: false",
        "file_delete_allowed: false",
        "n8n_merge_allowed: false",
        "executor_merge_allowed: false",
        "server_apply_allowed: false",
        "server_apply_performed: false",
        "broker_execution_allowed: false",
        "live_trading_allowed: false",
        "runtime_allowed: false",
        "production_secret_access_allowed: false",
        "production_secret_accessed: false",
    ):
        assert token in script

    serialized = json.dumps(target, sort_keys=True).lower()
    for forbidden in (
        "n8n-nodes-base.github",
        "n8n-nodes-base.httprequest",
        "n8n-nodes-base.ssh",
        "n8n-nodes-base.executecommand",
        "api.github.com",
        "pulls/merge",
        "server-apply",
        "process.env",
        "$env",
    ):
        assert forbidden not in serialized


def test_old_route_b_exports_are_byte_for_byte_unchanged() -> None:
    for path, expected_sha in PROTECTED_EXPORTS.items():
        assert path.is_file(), path
        assert blob_sha(path) == expected_sha, path
