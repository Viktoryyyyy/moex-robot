from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from .config import validate_futures_data_lake_config_values
from .contracts import validate_dataset_contract_set
from .schemas import (
    EXPECTED_CONFIG_PATH,
    FuturesDataLakeConfig,
    FuturesDatasetContract,
)


class FuturesContractIoError(ValueError):
    pass


_FORBIDDEN_DYNAMIC_MARKERS: Final[tuple[str, ...]] = (
    "".join(chr(code) for code in (108, 97, 116, 101, 115, 116)),
    "".join(chr(code) for code in (99, 117, 114, 114, 101, 110, 116)),
    "".join(chr(code) for code in (97, 117, 116, 111, 100, 101, 116, 101, 99, 116)),
)


@dataclass(frozen=True)
class FuturesContractPackage:
    repo_root: Path
    config: FuturesDataLakeConfig
    contracts: tuple[FuturesDatasetContract, ...]
    contracts_by_id: Mapping[str, FuturesDatasetContract]
    contracts_by_dataset_id: Mapping[str, FuturesDatasetContract]


def _normalize_tokens(value: str) -> tuple[str, ...]:
    normalized = value.casefold()
    for separator in ("/", "\\", ".", "_", "-", ":", "{", "}", "$"):
        normalized = normalized.replace(separator, " ")
    return tuple(token for token in normalized.split() if token)


def reject_dynamic_markers(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FuturesContractIoError(f"{field_name} is required")
    tokens = _normalize_tokens(value)
    if any(token in _FORBIDDEN_DYNAMIC_MARKERS for token in tokens):
        raise FuturesContractIoError(f"{field_name} contains unsupported dynamic marker")
    return value


def _require_repo_relative_path(path: str | Path, field_name: str) -> Path:
    text = reject_dynamic_markers(str(path), field_name)
    candidate = Path(text)
    if candidate.is_absolute():
        raise FuturesContractIoError(f"{field_name} must be repo-relative")
    if any(part in ("", ".", "..") for part in candidate.parts):
        raise FuturesContractIoError(f"{field_name} must not contain path traversal")
    return candidate


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _parse_scalar(value: str) -> object:
    stripped = _strip_quotes(value.strip())
    if stripped == "true":
        return True
    if stripped == "false":
        return False
    if stripped == "null":
        return None
    return stripped


def _yaml_items(text: str) -> tuple[tuple[int, str], ...]:
    items: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        if indent % 2:
            raise FuturesContractIoError("yaml indentation must use two-space levels")
        items.append((indent, raw_line.strip()))
    if not items:
        raise FuturesContractIoError("yaml content must be non-empty")
    return tuple(items)


def _parse_block(items: Sequence[tuple[int, str]], index: int, indent: int) -> tuple[object, int]:
    if index >= len(items):
        raise FuturesContractIoError("yaml block is missing")
    item_indent, content = items[index]
    if item_indent != indent:
        raise FuturesContractIoError("yaml indentation is invalid")
    if content.startswith("- "):
        return _parse_list(items, index, indent)
    return _parse_mapping(items, index, indent)


def _parse_list(items: Sequence[tuple[int, str]], index: int, indent: int) -> tuple[tuple[object, ...], int]:
    result: list[object] = []
    while index < len(items):
        item_indent, content = items[index]
        if item_indent < indent:
            break
        if item_indent != indent:
            raise FuturesContractIoError("yaml list indentation is invalid")
        if not content.startswith("- "):
            break
        value = content[2:].strip()
        index += 1
        if value:
            result.append(_parse_scalar(value))
        else:
            child, index = _parse_block(items, index, indent + 2)
            result.append(child)
    if not result:
        raise FuturesContractIoError("yaml list must be non-empty")
    return tuple(result), index


def _parse_mapping(items: Sequence[tuple[int, str]], index: int, indent: int) -> tuple[dict[str, object], int]:
    result: dict[str, object] = {}
    while index < len(items):
        item_indent, content = items[index]
        if item_indent < indent:
            break
        if item_indent != indent:
            raise FuturesContractIoError("yaml mapping indentation is invalid")
        if content.startswith("- "):
            break
        key, separator, value = content.partition(":")
        if separator != ":" or not key.strip():
            raise FuturesContractIoError("yaml mapping key is invalid")
        guarded_key = reject_dynamic_markers(key.strip(), "yaml key")
        index += 1
        if value.strip():
            result[guarded_key] = _parse_scalar(value)
        else:
            child, index = _parse_block(items, index, indent + 2)
            result[guarded_key] = child
    if not result:
        raise FuturesContractIoError("yaml mapping must be non-empty")
    return result, index


def load_simple_yaml_mapping(repo_root: str | Path, repo_relative_path: str | Path) -> dict[str, object]:
    root = Path(repo_root)
    relative_path = _require_repo_relative_path(repo_relative_path, "repo_relative_path")
    full_path = root / relative_path
    items = _yaml_items(full_path.read_text(encoding="utf-8"))
    values, next_index = _parse_block(items, 0, 0)
    if next_index != len(items):
        raise FuturesContractIoError("yaml content contains trailing invalid block")
    if not isinstance(values, dict):
        raise FuturesContractIoError("yaml root must be a mapping")
    return values


def load_futures_data_lake_contract_package(repo_root: str | Path) -> FuturesContractPackage:
    root = Path(repo_root)
    config_values = load_simple_yaml_mapping(root, EXPECTED_CONFIG_PATH)
    config = validate_futures_data_lake_config_values(config_values)
    contract_values = tuple(load_simple_yaml_mapping(root, path) for path in config.dataset_contract_refs)
    contracts = validate_dataset_contract_set(contract_values)
    return FuturesContractPackage(
        repo_root=root,
        config=config,
        contracts=contracts,
        contracts_by_id={contract.contract_id: contract for contract in contracts},
        contracts_by_dataset_id={contract.dataset_id: contract for contract in contracts},
    )


def _require_placeholder_value(value: str | None, field_name: str) -> str:
    if value is None:
        raise FuturesContractIoError(f"{field_name} is required for this contract path")
    guarded = reject_dynamic_markers(value, field_name)
    if guarded.startswith(("/", "\\")) or "/" in guarded or "\\" in guarded or guarded in (".", ".."):
        raise FuturesContractIoError(f"{field_name} must be a single safe path token")
    return guarded


def expand_contract_path(
    path_pattern: str,
    env_root: str,
    placeholders: Mapping[str, str | None],
) -> Path:
    pattern = reject_dynamic_markers(path_pattern, "path_pattern")
    if pattern.startswith("/"):
        raise FuturesContractIoError("path_pattern must not be an absolute path")
    prefix = "${MOEX_DATA_ROOT}/"
    if not pattern.startswith(prefix):
        raise FuturesContractIoError("path_pattern must be rooted at MOEX_DATA_ROOT")
    if not env_root or not str(env_root).strip():
        raise FuturesContractIoError("MOEX_DATA_ROOT is required")
    expanded = pattern.removeprefix(prefix)
    for key, value in placeholders.items():
        token = "{" + key + "}"
        if token in expanded:
            expanded = expanded.replace(token, _require_placeholder_value(value, key))
    if "{" in expanded or "}" in expanded:
        raise FuturesContractIoError("path_pattern contains unresolved placeholders")
    return Path(env_root) / expanded
