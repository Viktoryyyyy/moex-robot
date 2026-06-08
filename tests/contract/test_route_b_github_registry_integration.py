from pathlib import Path

from moex_core.contracts.route_b_github_execution import (
    APPROVED_ROUTE_B_EXECUTOR_PATHS,
    ROUTE_B_CONTEXT_REGISTRY_PATH,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_route_b_context_registry_is_bound_to_github_executor_contracts() -> None:
    registry_path = REPO_ROOT / ROUTE_B_CONTEXT_REGISTRY_PATH
    registry_text = registry_path.read_text(encoding="utf-8")

    assert '"source_of_truth": "github_repo"' in registry_text
    assert '"path_mode": "repo_relative_only"' in registry_text
    assert '"SUBCHAT_IMPLEMENTATION"' in registry_text
    assert '"docs/sot/context/roles/SUBCHAT_IMPLEMENTATION.v1.yaml"' in registry_text


def test_route_b_github_executor_contract_files_exist_in_approved_scope() -> None:
    for relative_path in APPROVED_ROUTE_B_EXECUTOR_PATHS:
        assert (REPO_ROOT / relative_path).is_file(), relative_path


def test_route_b_github_executor_architecture_spec_documents_n8n_branch_boundary() -> None:
    spec_text = (REPO_ROOT / "docs/sot/route_b/github_branch_pr_executor.v1.md").read_text(encoding="utf-8")

    assert "Branch/PR Executor v1" in spec_text
    assert "`n8n/`" in spec_text
    assert "`route-b/` is rejected" in spec_text
    assert "must not merge" in spec_text
    assert "PM_L2_ONLY" in spec_text
    assert "branch_name: deterministic route-b/" not in spec_text


def test_route_b_github_executor_machine_readable_contracts_declare_required_schema_ids() -> None:
    request_text = (
        REPO_ROOT / "contracts/route_b/route_b_github_execution_request.v1.yaml"
    ).read_text(encoding="utf-8")
    result_text = (
        REPO_ROOT / "contracts/route_b/route_b_github_execution_result.v1.yaml"
    ).read_text(encoding="utf-8")
    validation_text = (
        REPO_ROOT / "contracts/route_b/route_b_pr_validation_package.v1.yaml"
    ).read_text(encoding="utf-8")

    assert "schema_version: route_b_github_execution_request.v1" in request_text
    assert "schema_version: route_b_github_execution_result.v1" in result_text
    assert "schema_version: route_b_pr_validation_package.v1" in validation_text
    assert "role_context_ref" in request_text
    assert "target_role_context_ref" in request_text
    assert "merge_authority: PM_L2_ONLY" in validation_text
