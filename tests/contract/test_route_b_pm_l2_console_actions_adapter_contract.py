from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
ADAPTER_PATH = REPO_ROOT / "docs/sot/context/schemas/route_b_pm_l2_console_actions_adapter.v0.1.yaml"
EXPECTED_ADAPTER_ID = "route_b_pm_l2_console_actions_adapter.v0.1"
EXPECTED_SOURCE_SCHEMA_REF = "route_b_pm_l2_console_actions.v0.1"
EXPECTED_OPERATIONS = {
    "submit_route_b_run": {
        "method": "POST",
        "path": "/moex/route-b/intake",
        "maps_to_workflow": "MOEX_ROUTE_B_INTAKE_ACK_V1_10_3",
    },
    "get_route_b_status": {
        "method": "GET",
        "path": "/moex/route-b/status",
        "maps_to_workflow": "MOEX_ROUTE_B_STATUS_QUERY_V1_10_3",
    },
    "get_route_b_result": {
        "method": "GET",
        "path": "/moex/route-b/result",
        "maps_to_workflow": "MOEX_ROUTE_B_RESULT_QUERY_V1_10_3",
    },
}
FORBIDDEN_SECRET_MARKERS = (
    "secret_value:",
    "token:",
    "password:",
    "credential:",
    "authorization:",
    "bearer ",
    "api_key:",
    "api-key:",
)
FORBIDDEN_AUTHORITY_TRUE_FLAGS = (
    "merge_authority_exposed_to_n8n",
    "ci_passed_is_pm_l2_approval",
    "runtime_live_trading_allowed",
    "broker_execution_allowed",
    "direct_main_write_allowed",
    "force_push_allowed",
    "file_delete_allowed",
    "executor_merge_allowed",
)


def _load_adapter() -> dict[str, object]:
    return yaml.safe_load(ADAPTER_PATH.read_text(encoding="utf-8"))


def _walk(value: object) -> list[object]:
    output = [value]
    if isinstance(value, dict):
        for key, item in value.items():
            output.extend(_walk(key))
            output.extend(_walk(item))
    elif isinstance(value, list):
        for item in value:
            output.extend(_walk(item))
    return output


def _operations_by_id(adapter: dict[str, object]) -> dict[str, dict[str, object]]:
    operations = adapter["operations"]
    assert isinstance(operations, list)
    result = {}
    for operation in operations:
        assert isinstance(operation, dict)
        result[str(operation["operation_id"])] = operation
    return result


def test_adapter_file_exists_and_is_valid_yaml() -> None:
    assert ADAPTER_PATH.is_file()
    adapter = _load_adapter()
    assert isinstance(adapter, dict)


def test_adapter_identity_and_source_schema_ref() -> None:
    adapter = _load_adapter()
    assert adapter["adapter_id"] == EXPECTED_ADAPTER_ID
    assert adapter["source_schema_ref"] == EXPECTED_SOURCE_SCHEMA_REF
    assert adapter["artifact_class"] == "repo_relative"
    assert adapter["source_of_truth"] == "github_repo"
    assert adapter["intended_consumer"] == "PM_L2_CONSOLE"
    assert adapter["adapter_format"] == "openapi_3_1"


def test_adapter_exposes_exactly_three_operations() -> None:
    adapter = _load_adapter()
    operations = _operations_by_id(adapter)
    assert set(operations) == set(EXPECTED_OPERATIONS)
    assert len(operations) == 3
    for operation_id, expected in EXPECTED_OPERATIONS.items():
        operation = operations[operation_id]
        assert operation["method"] == expected["method"]
        assert operation["path"] == expected["path"]
        assert operation["maps_to_workflow"] == expected["maps_to_workflow"]


def test_adapter_paths_are_exactly_expected() -> None:
    adapter = _load_adapter()
    operations = _operations_by_id(adapter)
    assert {str(operation["path"]) for operation in operations.values()} == {
        "/moex/route-b/intake",
        "/moex/route-b/status",
        "/moex/route-b/result",
    }


def test_no_secret_values_are_present() -> None:
    text = ADAPTER_PATH.read_text(encoding="utf-8").lower()
    for marker in FORBIDDEN_SECRET_MARKERS:
        assert marker not in text
    adapter = _load_adapter()
    auth_contract = adapter["auth_contract"]
    assert isinstance(auth_contract, dict)
    assert auth_contract["class"] == "env_contract"
    assert auth_contract["secret_value_in_repo_allowed"] is False
    assert auth_contract["production_secret_wiring_in_scope"] is False


def test_server_url_uses_env_contract_only() -> None:
    adapter = _load_adapter()
    server_contract = adapter["server_url_contract"]
    assert isinstance(server_contract, dict)
    assert server_contract == {
        "class": "env_contract",
        "name": "N8N_PUBLIC_WEBHOOK_BASE_URL",
        "value_in_repo_allowed": False,
    }
    text = ADAPTER_PATH.read_text(encoding="utf-8")
    assert "http://" not in text
    assert "https://" not in text


def test_no_forbidden_authority_is_exposed() -> None:
    adapter = _load_adapter()
    invariants = adapter["invariants"]
    assert isinstance(invariants, dict)
    for flag in FORBIDDEN_AUTHORITY_TRUE_FLAGS:
        assert invariants[flag] is False
    for value in _walk(adapter):
        if isinstance(value, dict):
            for flag in FORBIDDEN_AUTHORITY_TRUE_FLAGS:
                if flag in value:
                    assert value[flag] is False


def test_result_operation_requires_pm_l2_repository_guard_and_rejects_status_only() -> None:
    adapter = _load_adapter()
    result_operation = _operations_by_id(adapter)["get_route_b_result"]
    assert result_operation["required_parameters"] == ["repository_full_name", "requested_by_role"]
    assert result_operation["status_only"] is False
    assert result_operation["rejects_status_only_semantics"] is True
    openapi_result = adapter["openapi_contract"]["paths"]["/moex/route-b/result"]["get"]
    assert openapi_result["x-status-only"] is False
    assert openapi_result["x-rejects-status-only-semantics"] is True
    assert openapi_result["x-status-only-redirect-action"] == "get_route_b_status"


def test_status_operation_remains_status_only() -> None:
    adapter = _load_adapter()
    status_operation = _operations_by_id(adapter)["get_route_b_status"]
    assert status_operation["status_only"] is True
    openapi_status = adapter["openapi_contract"]["paths"]["/moex/route-b/status"]["get"]
    assert openapi_status["x-status-only"] is True
