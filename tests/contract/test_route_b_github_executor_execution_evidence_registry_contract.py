from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = (
    REPO_ROOT
    / "docs/sot/context/schemas/route_b_github_executor_execution_evidence_registry.v0.1.yaml"
)


def _read() -> str:
    return SPEC_PATH.read_text(encoding="utf-8")


def test_evidence_registry_contract_exists_and_is_repo_only() -> None:
    spec = _read()
    assert SPEC_PATH.is_file()
    assert "schema_id: route_b_github_executor_execution_evidence_registry.v0.1" in spec
    assert "repo_only_contract: true" in spec
    assert "db_migration_required: false" in spec
    assert "server_db_mutation_allowed: false" in spec
    assert "production_n8n_workflow_mutation_allowed: false" in spec
    assert "runtime_live_broker_trading_scope_allowed: false" in spec
    assert "production_secret_access_allowed: false" in spec


def test_registry_binding_uses_event_payload_as_full_registry_and_other_tables_only_for_projection_and_checkpoint() -> None:
    spec = _read()
    for token in (
        "runtime_table: public.moex_n8n_workflow_run_events",
        "payload_column: event_payload_json",
        "table_role: append_only_execution_evidence_registry",
        "runtime_table: public.moex_n8n_workflow_runs",
        "projection_only: true",
        "forbidden_as_full_execution_evidence_registry: true",
        "runtime_table: public.moex_n8n_workflow_step_runs",
        "checkpoint_only: true",
        "resume_detection",
        "duplicate_commit_prevention",
        "duplicate_pr_prevention",
    ):
        assert token in spec, token


def test_request_identity_contract_requires_exact_executor_fields_without_task_id_fallback() -> None:
    spec = _read()
    for token in (
        "- execution_request_id",
        "- workflow_run_id",
        "- role_task_id",
        "- base_sha",
        "- file_changes",
        "- request_fingerprint_sha256",
        "- role_output_id",
        "- pm_l3_decision_id",
        "execution_request_id_fallback_to_task_id_allowed: false",
        "base_sha_must_reference_origin_main: true",
        "file_changes_must_be_exact_request_payload: true",
        "request_fingerprint_algorithm: sha256",
    ):
        assert token in spec, token
    assert "task_id_fallback_allowed: true" not in spec


def test_idempotency_rules_enforce_resume_reuse_and_blocked_conflict() -> None:
    spec = _read()
    for token in (
        "same_execution_request_id_same_fingerprint: resume_or_return_existing_result",
        "same_execution_request_id_different_fingerprint: blocked",
        "duplicate_commit_creation: reuse_existing_commit_or_skip_new_commit",
        "duplicate_pr_creation: reuse_existing_pr_or_skip_new_pr",
        "branch_recreation_rule: reuse_existing_branch_when_checkpoint_exists",
        "request_identity_conflict_status: blocked",
        "public.moex_n8n_workflow_run_events.event_payload_json",
        "public.moex_n8n_workflow_step_runs",
        "public.moex_n8n_workflow_runs",
    ):
        assert token in spec, token


def test_result_projection_and_authority_boundary_keep_merge_flags_explicitly_false() -> None:
    spec = _read()
    for token in (
        "projection_table: public.moex_n8n_workflow_runs",
        "registry_table: public.moex_n8n_workflow_run_events",
        "- approved_for_merge",
        "- merge_performed",
        "merge_authority: PM_L2_ONLY",
        "n8n_merge_allowed: false",
        "direct_main_write_allowed: false",
        "executor_merge_allowed: false",
        "server_apply_allowed: false",
        "approved_for_merge_must_remain_false_until_pm_l2: true",
        "merge_performed_must_remain_false_until_pm_l2: true",
    ):
        assert token in spec, token
    assert not re.search(r"approved_for_merge\s*:\s*true", spec)
    assert not re.search(r"merge_performed\s*:\s*true", spec)
