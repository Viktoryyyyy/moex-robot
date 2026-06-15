from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "docs/sot/context/registry.route_b.v1.yaml"
FORBIDDEN_MARKERS = ("latest", "current", "autodetect")
REQUIRED_STATIC_REFS = {
    "MOEX_Bot_Target_Architecture_2026_All_In_One",
    "MOEX_Bot_Role_Context_Operating_Model_v1",
    "github_commit_flow_subchats_v3",
}
REQUIRED_ROLE_REFS = {
    "PM_L2_PHASE_OWNER",
    "PM_L3_DELIVERY_VALIDATION_OWNER",
    "SUBCHAT_BASE",
    "SUBCHAT_IMPLEMENTATION",
    "SUBCHAT_VALIDATION",
    "SUBCHAT_REPO_AUDIT",
}
REQUIRED_SCHEMAS = {
    "pm_l2_to_pm_l3_request_envelope.v1",
    "pm_l3_to_subchat_task_package.v1",
    "subchat_to_pm_l3_return_package.v1",
    "pm_l3_to_pm_l2_validation_return_package.v1",
    "route_b_pm_l2_console_actions.v0.1",
    "route_b_pm_l2_console_actions_adapter.v0.1",
    "route_b_pm_l2_console_actions_openapi.v0.1",
    "route_b_pm_l2_console_owner_wiring.v0.1",
    "route_b_universal_role_runner.v0.1",
    "route_b_role_task_queue.v0.1",
    "route_b_pm_l3_decision_loop.v0.1",
    "route_b_multi_role_phase_state_machine.v0.1",
    "route_b_ollama_role_prompt_contract.v0.1",
    "route_b_universal_role_runner_db_contract.v0.1",
    "route_b_universal_role_runner_db_migration.v0.1",
}
REQUIRED_ROLE_FIELDS = (
    "mandate:",
    "authority:",
    "forbidden_actions:",
    "expected_output:",
    "relationship_to_chain:",
)


def _load_registry() -> dict[str, object]:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _walk_strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        output: list[str] = []
        for key, item in value.items():
            output.extend(_walk_strings(key))
            output.extend(_walk_strings(item))
        return output
    if isinstance(value, list):
        output = []
        for item in value:
            output.extend(_walk_strings(item))
        return output
    return []


def _registry_paths(registry: dict[str, object]) -> list[str]:
    paths: list[str] = []
    for section_name in ("static_context_refs", "schema_refs"):
        section = registry[section_name]
        assert isinstance(section, dict)
        for entry in section.values():
            assert isinstance(entry, dict)
            paths.append(str(entry["path"]))
    role_section = registry["role_context_refs"]
    assert isinstance(role_section, dict)
    for versions in role_section.values():
        assert isinstance(versions, dict)
        for entry in versions.values():
            assert isinstance(entry, dict)
            paths.append(str(entry["path"]))
    return paths


def test_route_b_registry_has_required_refs() -> None:
    registry = _load_registry()
    assert set(registry["static_context_refs"]) == REQUIRED_STATIC_REFS
    assert set(registry["role_context_refs"]) == REQUIRED_ROLE_REFS
    assert set(registry["schema_refs"]) == REQUIRED_SCHEMAS


def test_route_b_registry_paths_are_repo_relative_and_existing() -> None:
    registry = _load_registry()
    for path_value in _registry_paths(registry):
        path = Path(path_value)
        assert not path.is_absolute()
        assert ".." not in path.parts
        assert not str(path_value).startswith(("/", "~"))
        assert "\\" not in path_value
        assert (REPO_ROOT / path).is_file(), path_value


def test_route_b_registry_rejects_forbidden_markers() -> None:
    registry = _load_registry()
    for value in _walk_strings(registry):
        lowered = value.lower()
        for marker in FORBIDDEN_MARKERS:
            assert marker not in lowered, value


def test_role_specs_include_required_contract_fields() -> None:
    registry = _load_registry()
    role_section = registry["role_context_refs"]
    assert isinstance(role_section, dict)
    for versions in role_section.values():
        assert isinstance(versions, dict)
        role = versions["v1"]
        assert isinstance(role, dict)
        text = (REPO_ROOT / str(role["path"])).read_text(encoding="utf-8")
        for field in REQUIRED_ROLE_FIELDS:
            assert field in text


def test_route_b_schema_direction_is_explicit() -> None:
    registry = _load_registry()
    schemas = registry["schema_refs"]
    assert isinstance(schemas, dict)
    schema_text = {
        key: (REPO_ROOT / str(value["path"])).read_text(encoding="utf-8")
        for key, value in schemas.items()
        if isinstance(value, dict)
    }
    assert "producer: PM_L2_PHASE_OWNER" in schema_text["pm_l2_to_pm_l3_request_envelope.v1"]
    assert "consumer: PM_L3_DELIVERY_VALIDATION_OWNER" in schema_text["pm_l2_to_pm_l3_request_envelope.v1"]
    assert "target_subchat_role" in schema_text["pm_l3_to_subchat_task_package.v1"]
    assert "expected_return_to: PM_L3_DELIVERY_VALIDATION_OWNER" in schema_text["pm_l3_to_subchat_task_package.v1"]
    assert "return_to: PM_L3_DELIVERY_VALIDATION_OWNER" in schema_text["subchat_to_pm_l3_return_package.v1"]
    assert "return_to: PM_L2_PHASE_OWNER" in schema_text["pm_l3_to_pm_l2_validation_return_package.v1"]
