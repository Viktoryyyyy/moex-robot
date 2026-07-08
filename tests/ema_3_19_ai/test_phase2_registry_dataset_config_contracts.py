from __future__ import annotations

import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]

REGISTRY_PATH = REPO_ROOT / "configs/instruments/forts_instrument_registry.v1.yaml"
DATASET_CONFIG_PATH = REPO_ROOT / "configs/datasets/futures_data_lake.v1.yaml"

APPROVED_PHASE2_6_SOURCE_REFS = {
    "contracts/sources/internal/usdrubf_d1_ohlc_from_5m.v1.yaml",
    "contracts/sources/internal/usdrubf_d1_ema_3_19_cross_context.v1.yaml",
    "contracts/sources/internal/usdrubf_d1_classical_indicators.v1.yaml",
    "contracts/sources/futures/roll_expiry_mapping.v1.yaml",
    "contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml",
    "contracts/calendars/calendar/ru_tax_periods.v1.yaml",
    "contracts/calendars/calendar/ru_us_holidays.v1.yaml",
}

BLOCKED_SOURCE_REF_MARKERS = {
    "contracts/sources/futoi/participant_positioning.v1.yaml",
    "contracts/sources/oil/**",
    "contracts/sources/dollar_index/**",
    "contracts/sources/currency/**",
    "news_events.raw_ingestion",
    "news_events.llm_classification",
}

FALSE_READY_FLAGS = (
    "ingestion_ready",
    "runtime_ready",
    "loader_ready",
    "materialization_ready",
    "feature_computation_ready",
    "modeling_ready",
)

REGISTRY_FALSE_ENABLE_FLAGS = (
    "enabled_for_loading",
    "enabled_for_update",
    "enabled_for_retrieval",
    "enabled_for_raw_5m_materialization",
    "enabled_for_d1_derivation",
    "enabled_for_research",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _assert_repo_yaml_subset_parses(path: Path) -> str:
    """Validate the repository YAML subset without adding a PyYAML dependency."""
    text = _read(path)
    assert text.strip(), f"{path} is empty"
    stack: list[int] = [-1]
    block_scalar_parent_indent: int | None = None

    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        assert "\t" not in line, f"{path}:{line_no} contains a tab"
        indent = len(line) - len(line.lstrip(" "))
        assert indent % 2 == 0, f"{path}:{line_no} has non-2-space indentation"

        if block_scalar_parent_indent is not None:
            if indent > block_scalar_parent_indent:
                continue
            block_scalar_parent_indent = None

        stripped = line.strip()

        while stack and indent <= stack[-1]:
            stack.pop()
        assert stack, f"{path}:{line_no} invalid indentation stack"

        if stripped.startswith("- "):
            item = stripped[2:].strip()
            assert item, f"{path}:{line_no} has an empty list item"
            if ":" in item and not item.startswith(("http://", "https://")):
                key = item.split(":", 1)[0].strip()
                assert re.match(r"^[A-Za-z0-9_./${}-]+$", key), f"{path}:{line_no} invalid inline key"
            continue

        assert ":" in stripped, f"{path}:{line_no} is not a key/value YAML line"
        key, value = stripped.split(":", 1)
        assert re.match(r"^[A-Za-z0-9_./${}-]+$", key), f"{path}:{line_no} invalid key"
        value = value.strip()
        if not value:
            stack.append(indent)
        elif value in {">", "|"}:
            stack.append(indent)
            block_scalar_parent_indent = indent
        elif value[0] in {'"', "'"}:
            assert value[-1] == value[0], f"{path}:{line_no} has an unclosed quoted scalar"

    return text


def _yaml_load_for_shape(path: Path) -> Any:
    try:
        import yaml  # type: ignore[import-not-found]
    except ImportError:
        return {"__repo_yaml_subset_text__": _assert_repo_yaml_subset_parses(path)}

    loaded = yaml.safe_load(_read(path))
    assert isinstance(loaded, dict), f"{path} must parse as a YAML mapping"
    return loaded


def _section(text: str, section_header: str) -> str:
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() == section_header:
            base_indent = len(line) - len(line.lstrip(" "))
            collected = [line]
            for following in lines[idx + 1 :]:
                if following.strip() and len(following) - len(following.lstrip(" ")) <= base_indent:
                    break
                collected.append(following)
            return "\n".join(collected)
    raise AssertionError(f"section not found: {section_header}")


def _list_values(section_text: str) -> list[str]:
    return [
        line.strip()[2:].strip()
        for line in section_text.splitlines()
        if line.strip().startswith("- ")
    ]


def _assert_flag_false(text: str, flag: str) -> None:
    assert re.search(rf"^\s*{re.escape(flag)}:\s*false\s*$", text, re.MULTILINE), flag
    assert not re.search(rf"^\s*{re.escape(flag)}:\s*true\s*$", text, re.MULTILINE), flag


def test_registry_and_dataset_configs_exist_and_parse_as_yaml() -> None:
    for path in (REGISTRY_PATH, DATASET_CONFIG_PATH):
        assert path.is_file(), path
        _yaml_load_for_shape(path)

    registry_text = _read(REGISTRY_PATH)
    dataset_text = _read(DATASET_CONFIG_PATH)
    assert "phase2_6_binding:" in registry_text
    assert "phase2_6_source_bindings:" in dataset_text


def test_phase2_6_source_bindings_reference_only_approved_phase2_5_placeholders() -> None:
    dataset_text = _read(DATASET_CONFIG_PATH)
    binding = _section(dataset_text, "phase2_6_source_bindings:")
    approved_section = _section(binding, "  approved_source_contract_refs:")
    approved_refs = set(_list_values(approved_section))

    assert approved_refs == APPROVED_PHASE2_6_SOURCE_REFS
    for relative_path in approved_refs:
        assert (REPO_ROOT / relative_path).is_file(), relative_path

    assert "status: design_config_binding_only" in binding
    assert "availability_ts_utc_required: true" in binding
    assert "availability_ts_utc <= forecast_anchor_ts" in binding


def test_blocked_provider_refs_remain_excluded_from_approved_bindings() -> None:
    dataset_text = _read(DATASET_CONFIG_PATH)
    binding = _section(dataset_text, "phase2_6_source_bindings:")
    approved_section = _section(binding, "  approved_source_contract_refs:")
    blocked_section = _section(binding, "  blocked_source_refs:")

    approved_text = approved_section.lower()
    for marker in BLOCKED_SOURCE_REF_MARKERS:
        assert marker in blocked_section
        assert marker.lower() not in approved_text

    assert "contracts/sources/futoi/participant_positioning.v1.yaml" not in approved_text
    assert "news_events.raw_ingestion" not in approved_text
    assert "news_events.llm_classification" not in approved_text


def test_registry_and_dataset_readiness_flags_do_not_authorize_execution() -> None:
    registry_text = _read(REGISTRY_PATH)
    dataset_text = _read(DATASET_CONFIG_PATH)

    dataset_binding = _section(dataset_text, "phase2_6_source_bindings:")
    for flag in FALSE_READY_FLAGS:
        _assert_flag_false(dataset_binding, flag)

    for registry_binding in re.findall(
        r"phase2_6_binding:\n(?:\s{6,}.+\n?)+", registry_text
    ):
        for marker in (
            "ingestion_status: not_ready",
            "materialization_status: not_ready",
            "modeling_status: blocked",
            "runtime_status: not_ready",
        ):
            assert marker in registry_binding

    for flag in REGISTRY_FALSE_ENABLE_FLAGS:
        _assert_flag_false(registry_text, flag)


def test_no_generated_data_runtime_loader_or_current_contract_automation_is_authorized() -> None:
    registry_text = _read(REGISTRY_PATH)
    dataset_text = _read(DATASET_CONFIG_PATH)
    binding = _section(dataset_text, "phase2_6_source_bindings:")

    for marker in (
        "no generated data path",
        "no runtime loader",
        "no materialization job",
        "no feature computation",
        "no model fitting",
        "no prediction",
    ):
        assert marker in binding

    for marker in (
        "no current contract month selection automation",
        "no continuous contract runtime selection",
        "no loader readiness",
        "no materialization readiness",
        "no modeling readiness",
    ):
        assert marker in registry_text

    forbidden_authorizations = (
        "generated_data_path_authorized: true",
        "runtime_loader_authorized: true",
        "current_contract_month_selection_automation: true",
        "continuous_contract_runtime_selection: true",
    )
    combined = (registry_text + "\n" + dataset_text).lower()
    for forbidden in forbidden_authorizations:
        assert forbidden not in combined
