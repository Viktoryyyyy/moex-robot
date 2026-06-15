from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_pm_l2_console_actions_openapi.v0.1.yaml"
OWNER_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_pm_l2_console_owner_wiring.v0.1.yaml"

EXPECTED_OPERATION_IDS = {
    "submit_route_b_run",
    "get_route_b_status",
    "get_route_b_result",
}
EXPECTED_PATHS = {
    "/moex/route-b/intake",
    "/moex/route-b/status",
    "/moex/route-b/result",
}
EXPECTED_WORKFLOWS = {
    "MOEX_ROUTE_B_INTAKE_ACK_V1_10_3",
    "MOEX_ROUTE_B_STATUS_QUERY_V1_10_3",
    "MOEX_ROUTE_B_RESULT_QUERY_V1_10_3",
}
FORBIDDEN_TRUE_FLAGS = (
    "merge_authority_exposed_to_n8n",
    "ci_passed_is_pm_l2_approval",
    "runtime_live_trading_allowed",
    "broker_execution_allowed",
    "direct_main_write_allowed",
    "force_push_allowed",
    "file_delete_allowed",
    "executor_merge_allowed",
    "production_secrets_allowed",
    "production_secret_wiring_in_repo",
    "runtime_endpoint_smoke_in_scope",
    "server_first_assumptions_allowed",
    "workflow_json_relocation_allowed",
)


def _openapi_text() -> str:
    return OPENAPI_PATH.read_text(encoding="utf-8")


def _owner_text() -> str:
    return OWNER_PATH.read_text(encoding="utf-8")


def test_new_contract_files_exist() -> None:
    assert OPENAPI_PATH.is_file()
    assert OWNER_PATH.is_file()


def test_openapi_root_version_and_exact_operations() -> None:
    text = _openapi_text()
    assert re.search(r"^openapi:\s*3\.1\.0\s*$", text, re.MULTILINE)
    operation_ids = set(re.findall(r"^\s*operationId:\s*([A-Za-z0-9_]+)\s*$", text, re.MULTILINE))
    assert operation_ids == EXPECTED_OPERATION_IDS
    assert len(operation_ids) == 3


def test_openapi_paths_and_workflows_are_exact() -> None:
    text = _openapi_text()
    path_block = text.split("paths:", 1)[1].split("x-invariants:", 1)[0]
    paths = set(re.findall(r"^\s{2}(/moex/route-b/[a-z-]+):\s*$", path_block, re.MULTILINE))
    workflows = set(re.findall(r"x-maps-to-workflow:\s*(MOEX_ROUTE_B_[A-Z0-9_]+)", text))
    assert paths == EXPECTED_PATHS
    assert workflows == EXPECTED_WORKFLOWS


def test_server_url_and_auth_are_placeholder_contract_only() -> None:
    text = _openapi_text()
    assert "url: ${N8N_PUBLIC_WEBHOOK_BASE_URL}" in text
    assert "http://" not in text
    assert "https://" not in text
    assert "type: http" in text
    assert "scheme: bearer" in text
    assert not re.search(r"bearer\s+[A-Za-z0-9._~+/=-]{8,}", text, re.IGNORECASE)


def test_no_secret_values_are_present() -> None:
    combined = _openapi_text() + "\n" + _owner_text()
    lowered = combined.lower()
    forbidden_value_patterns = (
        r"authorization:\s*\S+",
        r"password:\s*\S+",
        r"secret:\s*\S+",
        r"api[_-]?key:\s*\S+",
        r"token:\s*\S+",
        r"bearer\s+[a-z0-9._~+/=-]{8,}",
    )
    for pattern in forbidden_value_patterns:
        assert not re.search(pattern, lowered)


def test_result_requires_pm_l2_role_and_rejects_status_only() -> None:
    text = _openapi_text()
    result_section = text.split("  /moex/route-b/result:", 1)[1]
    assert "x-rejects-status-only-semantics: true" in result_section
    assert "repository_full_name" in result_section
    assert "requested_by_role" in result_section
    assert "const: PM_L2_PHASE_OWNER" in result_section


def test_owner_wiring_references_openapi_and_manual_inputs() -> None:
    text = _owner_text()
    assert "owner_wiring_id: route_b_pm_l2_console_owner_wiring.v0.1" in text
    assert "openapi_artifact_ref: route_b_pm_l2_console_actions_openapi.v0.1" in text
    assert "source_adapter_ref: route_b_pm_l2_console_actions_adapter.v0.1" in text
    assert "N8N_PUBLIC_WEBHOOK_BASE_URL" in text
    assert "ROUTE_B_PM_L2_CONSOLE_BEARER_TOKEN" in text


def test_owner_wiring_disables_secret_wiring_and_runtime_smoke() -> None:
    text = _owner_text()
    assert re.search(r"^production_secret_wiring_in_repo:\s*false\s*$", text, re.MULTILINE)
    assert re.search(r"^runtime_endpoint_smoke_in_scope:\s*false\s*$", text, re.MULTILINE)


def test_no_forbidden_authority_is_exposed() -> None:
    combined = _openapi_text() + "\n" + _owner_text()
    for flag in FORBIDDEN_TRUE_FLAGS:
        assert not re.search(r"\b" + re.escape(flag) + r":\s*true\b", combined)
    forbidden_operation_ids = (
        "merge",
        "direct_main",
        "force_push",
        "file_delete",
        "runtime_live",
        "broker_execution",
    )
    operation_ids = set(re.findall(r"operationId:\s*([A-Za-z0-9_]+)", combined))
    for forbidden in forbidden_operation_ids:
        assert forbidden not in operation_ids
