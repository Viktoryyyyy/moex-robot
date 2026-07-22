from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "docs/sot/context/registry.route_b.v1.yaml"
MANAGEMENT_REGISTRY_PATH = REPO_ROOT / "docs/MOEX_BOT_CONTEXT_CONFIGURATION_SOURCES.md"
MANAGEMENT_CANON_PATH = REPO_ROOT / "docs/MOEX_BOT_MANAGEMENT_CANON.md"


def _load_registry() -> dict[str, object]:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def test_route_b_registry_is_fail_closed_historical_tombstone() -> None:
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

    assert isinstance(policy, dict)
    assert policy["active_resolution"] == "forbidden"
    assert policy["unknown_ref_policy"] == "reject"
    assert policy["historical_inspection"] == "read_only_when_explicitly_requested"


def test_route_b_registry_does_not_publish_legacy_active_refs() -> None:
    registry = _load_registry()

    forbidden_active_sections = {
        "workflow_state_store",
        "static_context_refs",
        "role_context_refs",
        "schema_refs",
        "route_b_chain",
        "resolver_rules",
    }

    assert forbidden_active_sections.isdisjoint(registry)


def test_route_b_registry_records_applied_state_boundary() -> None:
    registry = _load_registry()
    note = registry["application_note"]

    assert isinstance(note, str)
    assert "must not be used" in note
    assert "Deployed n8n Applied State" in note
    assert "deactivated and verified separately" in note
