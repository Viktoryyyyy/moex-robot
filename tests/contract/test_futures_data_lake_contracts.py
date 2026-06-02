import ast
from pathlib import Path

import pytest

from moex_data.futures import (
    EXPECTED_CONFIG_ID,
    EXPECTED_CONFIG_PATH,
    EXPECTED_DATASET_CONTRACT_IDS,
    EXPECTED_DATASET_CONTRACT_PATHS,
    EXPECTED_STORAGE_ROOT_REF,
    validate_dataset_contract_set,
    validate_dataset_contract_values,
    validate_futures_data_lake_config_values,
    validate_quality_report_rows,
    validate_quality_row_values,
    validate_refresh_manifest_values,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_CONTRACT_IDS = (
    "futures_raw_5m.v1",
    "futures_futoi_raw.v1",
    "futures_derived_d1.v1",
    "futures_derived_w1.v1",
    "futures_data_refresh_manifest.v1",
    "futures_quality_report.v1",
    "futures_continuous_5m.v1",
)
EXPECTED_CONTRACT_PATHS = (
    "contracts/datasets/futures_raw_5m.v1.yaml",
    "contracts/datasets/futures_futoi_raw.v1.yaml",
    "contracts/datasets/futures_derived_d1.v1.yaml",
    "contracts/datasets/futures_derived_w1.v1.yaml",
    "contracts/datasets/futures_data_refresh_manifest.v1.yaml",
    "contracts/datasets/futures_quality_report.v1.yaml",
    "contracts/datasets/futures_continuous_5m.v1.yaml",
)


def _dataset_contract(contract_id: str = "futures_raw_5m.v1", **overrides: object) -> dict[str, object]:
    dataset_id = contract_id.removesuffix(".v1")
    values: dict[str, object] = {
        "contract_id": contract_id,
        "dataset_id": dataset_id,
        "artifact_class": "external_pattern",
        "producer": "moex_data.futures.contract_test_builder",
        "consumers": ("moex_data.futures.contract_test_validator",),
        "format": "parquet",
        "schema_version": contract_id,
        "storage_root_ref": EXPECTED_STORAGE_ROOT_REF,
        "path_pattern": f"${{MOEX_DATA_ROOT}}/futures/{dataset_id}/trade_date={{YYYY-MM-DD}}/part.parquet",
        "partitioning": ("trade_date",),
    }
    values.update(overrides)
    return values


def _dataset_contracts() -> tuple[dict[str, object], ...]:
    return tuple(_dataset_contract(contract_id) for contract_id in EXPECTED_CONTRACT_IDS)


def _config(**overrides: object) -> dict[str, object]:
    artifact_class_index = {contract_id: "external_pattern" for contract_id in EXPECTED_CONTRACT_IDS}
    artifact_class_index["moex_data_root"] = "env_contract"
    artifact_class_index[EXPECTED_CONFIG_ID] = "repo_relative"
    values: dict[str, object] = {
        "config_id": EXPECTED_CONFIG_ID,
        "artifact_class": "repo_relative",
        "repo_path": EXPECTED_CONFIG_PATH,
        "external_storage_root": {
            "artifact_id": "moex_data_root",
            "artifact_class": "env_contract",
            "env_var": EXPECTED_STORAGE_ROOT_REF,
            "required": True,
            "hardcoded_server_path_allowed": False,
        },
        "dataset_contract_refs": EXPECTED_CONTRACT_PATHS,
        "artifact_class_index": artifact_class_index,
        "blocked_contracts": ("futures_continuous_5m.v1",),
        "path_rules": {
            "external_root_source": "env_contract",
            "hardcoded_server_path_allowed": False,
            "implicit_file_selection_allowed": False,
        },
    }
    values.update(overrides)
    return values


def _manifest(refresh_status: str = "succeeded", **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "run_id": "refresh_run.contract_test.v1",
        "run_date": "2026-06-02",
        "dataset_contract_refs": EXPECTED_CONTRACT_IDS,
        "partitions_written": ("${MOEX_DATA_ROOT}/futures/raw_5m/trade_date={YYYY-MM-DD}/part.parquet",),
        "partitions_skipped": (),
        "quality_report_ref": "${MOEX_DATA_ROOT}/futures/quality/run_date={YYYY-MM-DD}/quality_report.json",
        "refresh_status": refresh_status,
    }
    values.update(overrides)
    return values


def _quality_row(quality_status: str = "pass", **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "run_id": "refresh_run.contract_test.v1",
        "dataset_id": "futures_raw_5m",
        "family": "Si",
        "secid": "SiM6",
        "trade_date": "2026-06-02",
        "rows": 100,
        "duplicate_key_count": 0,
        "gap_count": 0,
        "null_ohlc_count": 0,
        "invalid_ohlc_count": 0,
        "futoi_missing_count": 0,
        "calendar_status": "trading_day",
        "quality_status": quality_status,
    }
    values.update(overrides)
    return values


def test_public_api_and_expected_ids_are_contract_stable():
    assert EXPECTED_DATASET_CONTRACT_IDS == EXPECTED_CONTRACT_IDS
    assert EXPECTED_DATASET_CONTRACT_PATHS == EXPECTED_CONTRACT_PATHS
    assert EXPECTED_CONFIG_ID == "futures_data_lake.v1"
    assert EXPECTED_CONFIG_PATH == "configs/datasets/futures_data_lake.v1.yaml"
    assert EXPECTED_STORAGE_ROOT_REF == "MOEX_DATA_ROOT"


def test_dataset_contracts_must_be_external_pattern_and_env_rooted():
    contracts = validate_dataset_contract_set(_dataset_contracts())

    assert tuple(contract.contract_id for contract in contracts) == EXPECTED_CONTRACT_IDS
    for contract in contracts:
        assert contract.artifact_class == "external_pattern"
        assert contract.storage_root_ref == "MOEX_DATA_ROOT"
        assert contract.path_pattern.startswith("${MOEX_DATA_ROOT}/")
        assert not contract.path_pattern.startswith("/")


def test_dataset_contract_membership_order_and_required_fields_fail_closed():
    with pytest.raises(ValueError):
        validate_dataset_contract_set(tuple(reversed(_dataset_contracts())))
    with pytest.raises(ValueError):
        validate_dataset_contract_set(_dataset_contracts()[:-1])

    missing = _dataset_contract()
    missing.pop("contract_id")
    with pytest.raises(ValueError):
        validate_dataset_contract_values(missing)


def test_dataset_contract_rejects_wrong_class_absolute_paths_and_dynamic_markers():
    invalid_cases = (
        _dataset_contract(artifact_class="repo_relative"),
        _dataset_contract(storage_root_ref="DATA_ROOT"),
        _dataset_contract(path_pattern="${DATA_ROOT}/futures/raw_5m/trade_date={YYYY-MM-DD}/part.parquet"),
        _dataset_contract(path_pattern="/home/trader/moex_bot/data/futures/raw_5m/trade_date={YYYY-MM-DD}/part.parquet"),
        _dataset_contract(path_pattern="${MOEX_DATA_ROOT}/futures/raw_5m/latest/trade_date={YYYY-MM-DD}/part.parquet"),
        _dataset_contract(path_pattern="${MOEX_DATA_ROOT}/futures/raw_5m/current/trade_date={YYYY-MM-DD}/part.parquet"),
        _dataset_contract(path_pattern="${MOEX_DATA_ROOT}/futures/raw_5m/autodetect/trade_date={YYYY-MM-DD}/part.parquet"),
    )

    for values in invalid_cases:
        with pytest.raises(ValueError):
            validate_dataset_contract_values(values)


def test_config_is_repo_relative_uses_env_contract_and_pr106_paths():
    config = validate_futures_data_lake_config_values(_config())

    assert config.config_id == "futures_data_lake.v1"
    assert config.artifact_class == "repo_relative"
    assert config.repo_path == "configs/datasets/futures_data_lake.v1.yaml"
    assert config.storage_root_env_var == "MOEX_DATA_ROOT"
    assert config.dataset_contract_refs == EXPECTED_CONTRACT_PATHS
    assert config.blocked_contracts == ("futures_continuous_5m.v1",)


def test_config_refs_external_root_blocked_contracts_and_required_fields_fail_closed():
    invalid_cases = (
        _config(artifact_class="external_pattern"),
        _config(repo_path="/home/trader/moex_bot/moex-robot/configs/datasets/futures_data_lake.v1.yaml"),
        _config(dataset_contract_refs=tuple(reversed(EXPECTED_CONTRACT_PATHS))),
        _config(blocked_contracts=()),
        _config(external_storage_root={"artifact_class": "repo_relative", "env_var": "MOEX_DATA_ROOT", "hardcoded_server_path_allowed": False}),
        _config(external_storage_root={"artifact_class": "env_contract", "env_var": "DATA_ROOT", "hardcoded_server_path_allowed": False}),
        _config(path_rules={"external_root_source": "env_contract", "hardcoded_server_path_allowed": True, "implicit_file_selection_allowed": False}),
    )

    for values in invalid_cases:
        with pytest.raises(ValueError):
            validate_futures_data_lake_config_values(values)

    missing = _config()
    missing.pop("path_rules")
    with pytest.raises(ValueError):
        validate_futures_data_lake_config_values(missing)


def test_refresh_manifest_status_and_dataset_refs_are_closed_sets():
    for status in ("succeeded", "failed", "partial"):
        assert validate_refresh_manifest_values(_manifest(status)).refresh_status == status

    with pytest.raises(ValueError):
        validate_refresh_manifest_values(_manifest("running"))
    with pytest.raises(ValueError):
        validate_refresh_manifest_values(_manifest(dataset_contract_refs=tuple(reversed(EXPECTED_CONTRACT_IDS))))

    missing = _manifest()
    missing.pop("refresh_status")
    with pytest.raises(ValueError):
        validate_refresh_manifest_values(missing)


def test_quality_status_and_counters_are_closed_sets():
    for status in ("pass", "warn", "fail"):
        assert validate_quality_row_values(_quality_row(status)).quality_status == status

    with pytest.raises(ValueError):
        validate_quality_row_values(_quality_row("running"))
    with pytest.raises(ValueError):
        validate_quality_row_values(_quality_row(rows=-1))
    with pytest.raises(ValueError):
        validate_quality_row_values(_quality_row(rows=1.5))
    with pytest.raises(ValueError):
        validate_quality_row_values(_quality_row(rows=True))

    missing = _quality_row()
    missing.pop("quality_status")
    with pytest.raises(ValueError):
        validate_quality_row_values(missing)


def test_quality_report_rows_must_be_non_empty_and_single_run():
    report = validate_quality_report_rows((_quality_row(), _quality_row(dataset_id="futures_futoi_raw")))

    assert report.run_id == "refresh_run.contract_test.v1"
    with pytest.raises(ValueError):
        validate_quality_report_rows(())
    with pytest.raises(ValueError):
        validate_quality_report_rows((_quality_row(), _quality_row(run_id="other_run")))


def test_futures_package_has_no_forbidden_imports():
    forbidden_prefixes = (
        "moex_" + "run" + "time",
        "moex_" + "back" + "test",
        "moex_" + "re" + "search",
        "strat" + "egies",
        "re" + "quests",
        "url" + "lib",
        "so" + "cket",
        "sub" + "process",
        "pan" + "das",
        "num" + "py",
        "pya" + "rrow",
    )
    for source_path in (REPO_ROOT / "src" / "moex_data" / "futures").glob("*.py"):
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_names = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported_names = (node.module or "",)
            else:
                continue
            for imported_name in imported_names:
                assert not imported_name.startswith(forbidden_prefixes), source_path
