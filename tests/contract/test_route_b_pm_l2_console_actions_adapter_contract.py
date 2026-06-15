from __future__ import annotations

import re
from pathlib import Path


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


def _text() -> str:
    return ADAPTER_PATH.read_text(encoding="utf-8")


def _scalar(key: str) -> str:
    pattern = re.compile(r"^" + re.escape(key) + r":\s*(.+)$", re.MULTILINE)
    match = pattern.search(_text())
    assert match, key
    return match.group(1).strip().strip('"')


def _section(name: str) -> str:
    text = _text()
    start_match = re.search(r"^" + re.escape(name) + r":\s*$", text, flags=re.MULTILINE)
    assert start_match, name
    start = start_match.end()
    end_match = re.search(r"^[A-Za-z_][A-Za-z0-9_]*:\s*", text[start:], flags=re.MULTILINE)
    end = start + end_match.start() if end_match else len(text)
    return text[start:end]


def _operations() -> dict[str, dict[str, object]]:
    operations: dict[str, dict[str, object]] = {}
    current: dict[str, object] | None = None
    for raw_line in _section("operations").splitlines():
        line = raw_line.strip()
        if line.startswith("- operation_id:"):
            operation_id = line.split(":", 1)[1].strip()
            current = {"operation_id": operation_id}
            operations[operation_id] = current
        elif current is not None and ":" in line:
            key, value = line.split(":", 1)
            value = value.strip()
            if value == "true":
                current[key] = True
            elif value == "false":
                current[key] = False
            elif value:
                current[key] = value
    return operations


def _section_mapping(section_name: str) -> dict[str, object]:
    mapping: dict[str, object] = {}
    for raw_line in _section(section_name).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("-") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        value = value.strip()
        if value == "true":
            mapping[key] = True
        elif value == "false":
            mapping[key] = False
        elif value:
            mapping[key] = value.strip('"')
    return mapping


def test_adapter_file_exists_and_is_valid_yaml_subset() -> None:
    assert ADAPTER_PATH.is_file()
    text = _text()
    assert "\t" not in text
    for line in text.splitlines():
        if line.strip():
            indent = len(line) - len(line.lstrip(" "))
            assert indent % 2 == 0, line
    for required_key in (
        "adapter_id",
        "source_schema_ref",
        "artifact_class",
        "source_of_truth",
        "intended_consumer",
        "adapter_format",
        "server_url_contract",
        "auth_contract",
        "openapi_contract",
        "operations",
        "invariants",
    ):
        assert re.search(r"^" + re.escape(required_key) + r":", text, flags=re.MULTILINE)


def test_adapter_identity_and_source_schema_ref() -> None:
    assert _scalar("adapter_id") == EXPECTED_ADAPTER_ID
    assert _scalar("source_schema_ref") == EXPECTED_SOURCE_SCHEMA_REF
    assert _scalar("artifact_class") == "repo_relative"
    assert _scalar("source_of_truth") == "github_repo"
    assert _scalar("intended_consumer") == "PM_L2_CONSOLE"
    assert _scalar("adapter_format") == "openapi_3_1"


def test_adapter_exposes_exactly_three_operations() -> None:
    operations = _operations()
    assert set(operations) == set(EXPECTED_OPERATIONS)
    assert len(operations) == 3
    for operation_id, expected in EXPECTED_OPERATIONS.items():
        operation = operations[operation_id]
        assert operation["method"] == expected["method"]
        assert operation["path"] == expected["path"]
        assert operation["maps_to_workflow"] == expected["maps_to_workflow"]


def test_adapter_paths_are_exactly_expected() -> None:
    operations = _operations()
    assert {str(operation["path"]) for operation in operations.values()} == {
        "/moex/route-b/intake",
        "/moex/route-b/status",
        "/moex/route-b/result",
    }


def test_no_secret_values_are_present() -> None:
    lowered = _text().lower()
    for marker in FORBIDDEN_SECRET_MARKERS:
        assert marker not in lowered
    auth_contract = _section_mapping("auth_contract")
    assert auth_contract["class"] == "env_contract"
    assert auth_contract["secret_value_in_repo_allowed"] is False
    assert auth_contract["production_secret_wiring_in_scope"] is False


def test_server_url_uses_env_contract_only() -> None:
    server_contract = _section_mapping("server_url_contract")
    assert server_contract == {
        "class": "env_contract",
        "name": "N8N_PUBLIC_WEBHOOK_BASE_URL",
        "value_in_repo_allowed": False,
    }
    text = _text()
    assert "http://" not in text
    assert "https://" not in text


def test_no_forbidden_authority_is_exposed() -> None:
    invariants = _section_mapping("invariants")
    for flag in FORBIDDEN_AUTHORITY_TRUE_FLAGS:
        assert invariants[flag] is False
        assert not re.search(r"^\s*" + re.escape(flag) + r":\s*true\s*$", _text(), flags=re.MULTILINE)


def test_result_operation_requires_pm_l2_repository_guard_and_rejects_status_only() -> None:
    result_operation = _operations()["get_route_b_result"]
    assert result_operation["status_only"] is False
    assert result_operation["rejects_status_only_semantics"] is True
    result_section = re.search(
        r"/moex/route-b/result:[\s\S]+?^\s{4}/moex/|\Z",
        _text(),
        flags=re.MULTILINE,
    )
    assert result_section
    result_text = result_section.group(0)
    assert "required: true" in result_text
    assert "name: repository_full_name" in result_text
    assert "name: requested_by_role" in result_text
    assert "const: PM_L2_PHASE_OWNER" in result_text
    assert "x-rejects-status-only-semantics: true" in result_text
    assert "x-status-only-redirect-action: get_route_b_status" in result_text


def test_status_operation_remains_status_only() -> None:
    status_operation = _operations()["get_route_b_status"]
    assert status_operation["status_only"] is True
    status_section = re.search(
        r"/moex/route-b/status:[\s\S]+?^\s{4}/moex/route-b/result:",
        _text(),
        flags=re.MULTILINE,
    )
    assert status_section
    assert "x-status-only: true" in status_section.group(0)
