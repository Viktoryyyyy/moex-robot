from pathlib import Path

import pytest

from moex_core.contracts import (
    EXPECTED_REGISTRY_CONFIG_PATHS,
    REGISTRY_KINDS,
    RegistryContractError,
    validate_registry_entry_values,
    validate_registry_package_values,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def _parse_scalar(value: str) -> object:
    if value == "{}":
        return {}
    if value == "null":
        return None
    if value == "true":
        return True
    if value == "false":
        return False
    return value


def _yaml_lines(path: Path) -> tuple[tuple[int, str], ...]:
    rows: list[tuple[int, str]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        rows.append((indent, raw_line.strip()))
    return tuple(rows)


def _parse_yaml_block(rows: tuple[tuple[int, str], ...], start: int, indent: int) -> tuple[dict[str, object], int]:
    result: dict[str, object] = {}
    index = start
    while index < len(rows):
        row_indent, line = rows[index]
        if row_indent < indent:
            break
        if row_indent > indent:
            raise ValueError("unexpected yaml indent")
        if line.startswith("- "):
            raise ValueError("unexpected yaml list item")
        key, separator, raw_value = line.partition(":")
        if not separator:
            raise ValueError("invalid yaml line")
        value = raw_value.strip()
        if value:
            result[key] = _parse_scalar(value)
            index += 1
            continue
        if index + 1 >= len(rows) or rows[index + 1][0] <= row_indent:
            result[key] = None
            index += 1
            continue
        child_indent = rows[index + 1][0]
        if rows[index + 1][1].startswith("- "):
            items: list[object] = []
            index += 1
            while index < len(rows):
                item_indent, item_line = rows[index]
                if item_indent < child_indent:
                    break
                if item_indent != child_indent or not item_line.startswith("- "):
                    raise ValueError("invalid yaml list block")
                items.append(_parse_scalar(item_line.removeprefix("- ").strip()))
                index += 1
            result[key] = tuple(items)
            continue
        child, index = _parse_yaml_block(rows, index + 1, child_indent)
        result[key] = child
    return result, index


def _load_simple_yaml(relative_path: str) -> dict[str, object]:
    rows = _yaml_lines(REPO_ROOT / relative_path)
    result, index = _parse_yaml_block(rows, 0, 0)
    if index != len(rows):
        raise ValueError("unparsed yaml content")
    return result


def test_actual_registry_config_yaml_files_validate():
    entries = tuple(
        validate_registry_entry_values(_load_simple_yaml(relative_path))
        for relative_path in EXPECTED_REGISTRY_CONFIG_PATHS
    )
    package = validate_registry_package_values({"entries": entries})

    assert tuple(entry.registry_kind for entry in package.entries) == REGISTRY_KINDS
    assert package.entries[0].payload == {"family": "Si", "market": "futures", "timeframe_scope": ("D1",)}
    assert package.entries[1].payload == {
        "dataset_id": "futures_derived_d1",
        "contract_ref": "contracts/datasets/futures_derived_d1.v1.yaml",
    }


def test_wrong_payload_section_fails_closed():
    values = _load_simple_yaml("configs/datasets/dataset_registry.v1.yaml")
    values["feature"] = {"feature_id": "bad"}

    with pytest.raises(RegistryContractError):
        validate_registry_entry_values(values)


def test_payload_dynamic_marker_fails_closed():
    values = _load_simple_yaml("configs/datasets/dataset_registry.v1.yaml")
    values["dataset"] = {"contract_ref": "contracts/datasets/late" + "st.yaml"}

    with pytest.raises(RegistryContractError):
        validate_registry_entry_values(values)


def test_payload_promotion_marker_fails_closed():
    values = _load_simple_yaml("configs/datasets/dataset_registry.v1.yaml")
    values["dataset"] = {"appro" + "val_metric": "accepted"}

    with pytest.raises(RegistryContractError):
        validate_registry_entry_values(values)
