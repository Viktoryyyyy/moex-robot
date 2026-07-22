from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "docs/sot/context/registry.route_b.v1.yaml"
MANAGEMENT_REGISTRY_PATH = REPO_ROOT / "docs/MOEX_BOT_CONTEXT_CONFIGURATION_SOURCES.md"
MANAGEMENT_CANON_PATH = REPO_ROOT / "docs/MOEX_BOT_MANAGEMENT_CANON.md"


def _load_registry() -> dict[str, object]:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def test_route_b_registry_is_fail_closed_historical_source() -> None:
    registry = _load_registry()

    assert registry["registry_id"] == "route_b_context_registry.v1"
    assert registry["project"] == "MOEX_Bot"
    assert registry["status"] == "deprecated_historical"
    assert registry["new_tasks_allowed"] is False
    assert registry["new_runtime_execution_allowed"] is False
    assert registry["active_context_resolution_allowed"] is False
    assert registry["historical_source"] is True


def test_route_b_registry_points_to_current_management_sources() -> None:
    registry = _load_registry()
    superseded_by = registry["superseded_by"]

    assert isinstance(superseded_by, dict)
    assert superseded_by["management_registry"] == "docs/MOEX_BOT_CONTEXT_CONFIGURATION_SOURCES.md"
    assert superseded_by["management_canon"] == "docs/MOEX_BOT_MANAGEMENT_CANON.md"
    assert superseded_by["active_routes"] == [
        "browser_controlled_github_route",
        "flowise_automated_github_route",
    ]

    assert MANAGEMENT_REGISTRY_PATH.is_file()
    assert MANAGEMENT_CANON_PATH.is_file()


def test_route_b_registry_forbids_active_resolution() -> None:
    registry = _load_registry()
    policy = registry["resolution_policy"]
    resolver_rules = registry["resolver_rules"]

    assert isinstance(policy, dict)
    assert policy["active_resolution"] == "forbidden"
    assert policy["unknown_ref_policy"] == "reject"
    assert policy["historical_inspection"] == "read_only_when_explicitly_requested"

    assert isinstance(resolver_rules, dict)
    assert resolver_rules["active_resolution"] == "forbidden"
    assert resolver_rules["historical_inspection"] == "read_only_when_explicitly_requested"


def test_route_b_registry_preserves_refs_only_as_historical_evidence() -> None:
    registry = _load_registry()

    for section_name in ("static_context_refs", "schema_refs"):
        section = registry[section_name]
        assert isinstance(section, dict)
        assert section
        for entry in section.values():
            assert isinstance(entry, dict)
            assert entry["status"] == "historical_only"

    role_section = registry["role_context_refs"]
    assert isinstance(role_section, dict)
    assert role_section
    for versions in role_section.values():
        assert isinstance(versions, dict)
        for entry in versions.values():
            assert isinstance(entry, dict)
            assert entry["status"] == "historical_only"

    assert registry["route_b_chain"]
    assert "workflow_state_store" not in registry


def test_route_b_registry_records_applied_state_boundary() -> None:
    registry = _load_registry()
    note = registry["application_note"]

    assert isinstance(note, str)
    assert "must not be used" in note
    assert "Deployed n8n Applied State" in note
    assert "deactivated and verified separately" in note
