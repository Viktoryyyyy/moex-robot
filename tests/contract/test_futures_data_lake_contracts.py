import ast
from pathlib import Path

import pytest

from moex_data.futures import (
    EXPECTED_CONFIG_ID,
    EXPECTED_CONFIG_PATH,
    EXPECTED_DATASET_CONTRACT_IDS,
    EXPECTED_DATASET_CONTRACT_PATHS,
    EXPECTED_STORAGE_ROOT_REF,
    FuturesConfigValidationError,
    FuturesContractValidationError,
    FuturesManifestValidationError,
    FuturesQualityValidationError,
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
CONFIG_VALIDATION_ERRORS = (FuturesConfigValidationError, FuturesContractValidationError)


def _dataset_id(contract_id: str) -> str:
    return contract_id.removesuffix(".v1")


def _dataset_contract(contract_id: str = "futures_raw_5m.v1", **overrides: object) -> dict[str, object]:
    dataset_id = _dataset_id(contract_id)
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
    if contract_id == "futures_continuous_5m.v1":
        values["implementation_status"] = "blocked_placeholder"
    values.update(overrides)
    return values


def _dataset_contracts() -> tuple[dict[str, object], ...]:
    return tuple(_dataset_contract(contract_id) for contract_id in EXPECTED_DATASET_CONTRACT_IDS)


def _config(**overrides: object) -> dict[str, object]:
    artifact_class_index = {contract_id: "external_pattern" for contract_id in EXPECTED_DATASET_CONTRACT_IDS}
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
        "dataset_contract_refs": EXPECTED_DATASET_CONTRACT_PATHS,
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


def _manifest(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "run_id": "refresh_run.contract_test.v1",
        "run_date": "2026-06-02",
        "dataset_contract_refs": EXPECTED_DATASET_CONTRACT_IDS,
        "partitions_written": ("${MOEX_DATA_ROOT}/futures/raw_5m/trade_date={YYYY-MM-DD}/part.parquet",),
        "partitions_skipped": (),
        "quality_report_ref": "${MOEX_DATA_ROOT}/futures/quality/run_date={YYYY-MM-DD}/quality_report.json",
        "refresh_status": "succeeded",
    }
    values.update(overrides)
    return values


def _quality_row(**overrides: object) -> dict[str, object]:
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
        "quality_status": "pass",
    }
    values.update(overrides)
    return values


def test_public_futures_api_exposes_expected_ids_config_and_validators():
    assert EXPECTED_DATASET_CONTRACT_IDS == EXPECTED_CONTRACT_IDS
    assert EXPECTED_DATASET_CONTRACT_PATHS == EXPECTED_CONTRACT_PATHS
    assert EXPECTED_CONFIG_ID == "futures_data_lake.v1"
    assert EXPECTED_CONFIG_PATH == "configs/datasets/futures_data_lake.v1.yaml"
    assert EXPECTED_STORAGE_ROOT_REF == "MOEX_DATA_ROOT"


@pytest.mark.parametrize("contract_id", EXPECTED_CONTRACT_IDS)
def test_dataset_contracts_use_external_pattern_and_env_rooted_paths(contract_id):
    contract = validate_dataset_contract_values(_dataset_contract(contract_id))

    assert contract.contract_id == contract_id
    assert contract.dataset_id == _dataset_id(contract_id)
    assert contract.artifact_class == "external_pattern"
    assert contract.storage_root_ref == "MOEX_DATA_ROOT"
    assert contract.path_pattern.startswith("${MOEX_DATA_ROOT}/")
    assert not contract.path_pattern.startswith("/")


def test_dataset_contract_set_membership_and_order_are_enforced():
    contracts = validate_dataset_contract_set(_dataset_contracts())

    assert tuple(contract.contract_id for contract in contracts) == EXPECTED_CONTRACT_IDS
    with pytest.raises(FuturesContractValidationError):
        validate_dataset_contract_set(tuple(reversed(_dataset_contracts())))
    with pytest.raises(FuturesContractValidationError):
        validate_dataset_contract_set(_dataset_contracts()[:-1])
    with pytest.raises(FuturesContractValidationError):
        validate_dataset_contract_set((_dataset_contract("unknown_dataset.v1"),) + _dataset_contracts()[1:])


def test_dataset_contract_artifact_class_must_be_external_pattern_only():
    with pytest.raises(FuturesContractValidationError):
        validate_dataset_contract_values(_dataset_contract(artifact_class="repo_relative"))
    with pytest.raises(FuturesContractValidationError):
        validate_dataset_contract_values(_dataset_contract(extra_artifact_class="external_pattern"))


@pytest.mark.parametrize(
    "overrides",
    [
        {"storage_root_ref": "DATA_ROOT"},
        {"path_pattern": "${DATA_ROOT}/futures/raw_5m/trade_date={YYYY-MM-DD}/part.parquet"},
        {"path_pattern": "${MOEX_DATA_ROOT}/mirror/${MOEX_DATA_ROOT}/trade_date={YYYY-MM-DD}/part.parquet"},
        {"path_pattern": "/home/trader/moex_bot/data/futures/raw_5m/trade_date={YYYY-MM-DD}/part.parquet"},
    ],
)
def test_dataset_contract_external_paths_must_be_moex_data_rooted_and_not_absolute(overrides):
    with pytest.raises(FuturesContractValidationError):
        validate_dataset_contract_values(_dataset_contract(**overrides))


@pytest.mark.parametrize(
    ("marker_field", "marker_value"),
    [
        ("path_pattern", "${MOEX_DATA_ROOT}/futures/raw_5m/latest/trade_date={YYYY-MM-DD}/part.parquet"),
        ("path_pattern", "${MOEX_DATA_ROOT}/futures/raw_5m/current/trade_date={YYYY-MM-DD}/part.parquet"),
        ("path_pattern", "${MOEX_DATA_ROOT}/futures/raw_5m/autodetect/trade_date={YYYY-MM-DD}/part.parquet"),
        ("producer", "moex_data.futures.latest_loader"),
    ],
)
def test_dataset_contract_latest_current_autodetect_markers_fail_closed(marker_field, marker_value):
    with pytest.raises(FuturesContractValidationError):
        validate_dataset_contract_values(_dataset_contract(**{marker_field: marker_value}))


@pytest.mark.parametrize(
    "missing_field",
    [
        "contract_id",
        "dataset_id",
        "artifact_class",
        "producer",
        "consumers",
        "format",
        "schema_version",
        "storage_root_ref",
        "path_pattern",
        "partitioning",
    ],
)
def test_dataset_contract_missing_required_fields_fail_closed(missing_field):
    values = _dataset_contract()
    values.pop(missing_field)

    with pytest.raises(FuturesContractValidationError):
        validate_dataset_contract_values(values)


def test_config_uses_repo_relative_artifact_class_and_pr106_contract_paths():
    config = validate_futures_data_lake_config_values(_config())

    assert config.config_id == "futures_data_lake.v1"
    assert config.artifact_class == "repo_relative"
    assert config.repo_path == "configs/datasets/futures_data_lake.v1.yaml"
    assert config.storage_root_env_var == "MOEX_DATA_ROOT"
    assert config.dataset_contract_refs == EXPECTED_CONTRACT_PATHS
    assert config.blocked_contracts == ("futures_continuous_5m.v1",)


@pytest.mark.parametrize(
    "overrides",
    [
        {"artifact_class": "external_pattern"},
        {"repo_path": "/home/trader/moex_bot/moex-robot/configs/datasets/futures_data_lake.v1.yaml"},
        {"repo_path": "configs/datasets/latest.yaml"},
        {"dataset_contract_refs": EXPECTED_CONTRACT_PATHS[:-1]},
        {"dataset_contract_refs": tuple(reversed(EXPECTED_CONTRACT_PATHS))},
        {"blocked_contracts": ()},
        {"blocked_contracts": ("futures_raw_5m.v1",)},
        {"path_rules": {"external_root_source": "env_contract", "hardcoded_server_path_allowed": True, "implicit_file_selection_allowed": False}},
        {"path_rules": {"external_root_source": "env_contract", "hardcoded_server_path_allowed": False, "implicit_file_selection_allowed": True}},
    ],
)
def test_config_invalid_artifact_paths_refs_and_blocked_contracts_fail_closed(overrides):
    with pytest.raises(CONFIG_VALIDATION_ERRORS):
        validate_futures_data_lake_config_values(_config(**overrides))


@pytest.mark.parametrize(
    "external_storage_root",
    [
        {"artifact_class": "repo_relative", "env_var": "MOEX_DATA_ROOT", "hardcoded_server_path_allowed": False},
        {"artifact_class": "env_contract", "env_var": "DATA_ROOT", "hardcoded_server_path_allowed": False},
        {"artifact_class": "env_contract", "env_var": "MOEX_DATA_ROOT", "hardcoded_server_path_allowed": True},
    ],
)
def test_config_external_storage_root_must_be_env_contract_moex_data_root(external_storage_root):
    with pytest.raises(CONFIG_VALIDATION_ERRORS):
        validate_futures_data_lake_config_values(_config(external_storage_root=external_storage_root))


@pytest.mark.parametrize(
    "missing_field",
    [
        "config_id",
        "artifact_class",
        "repo_path",
        "external_storage_root",
        "dataset_contract_refs",
        "artifact_class_index",
        "blocked_contracts",
    ],
)
def test_config_missing_required_fields_fail_closed(missing_field):
    values = _config()
    values.pop(missing_field)

    with pytest.raises(CONFIG_VALIDATION_ERRORS):
        validate_futures_data_lake_config_values(values)


@pytest.mark.parametrize("refresh_status", ("succeeded", "failed", "partial"))
def test_refresh_manifest_status_is_limited_to_accepted_values(refresh_status):
    manifest = validate_refresh_manifest_values(_manifest(refresh_status=refresh_status))

    assert manifest.refresh_status == refresh_status


@pytest.mark.parametrize("refresh_status", ("success", "warning", "running", "skipped"))
def test_refresh_manifest_rejects_unsupported_status(refresh_status):
    with pytest.raises(FuturesManifestValidationError):
        validate_refresh_manifest_values(_manifest(refresh_status=refresh_status))


def test_refresh_manifest_dataset_refs_must_match_expected_membership_and_order():
    validate_refresh_manifest_values(_manifest())
    with pytest.raises(FuturesManifestValidationError):
        validate_refresh_manifest_values(_manifest(dataset_contract_refs=tuple(reversed(EXPECTED_DATASET_CONTRACT_IDS))))
    with pytest.raises(FuturesManifestValidationError):
        validate_refresh_manifest_values(_manifest(dataset_contract_refs=EXPECTED_DATASET_CONTRACT_IDS[:-1]))


@pytest.mark.parametrize(
    "missing_field",
    (
        "run_id",
        "run_date",
        "dataset_contract_refs",
        "partitions_written",
        "partitions_skipped",
        "quality_report_ref",
        "refresh_status",
    ),
)
def test_refresh_manifest_missing_required_fields_fail_closed(missing_field):
    values = _manifest()
    values.pop(missing_field)

    with pytest.raises(FuturesManifestValidationError):
        validate_refresh_manifest_values(values)


@pytest.mark.parametrize("quality_status", ("pass", "warn", "fail"))
def test_quality_status_is_limited_to_accepted_values(quality_status):
    row = validate_quality_row_values(_quality_row(quality_status=quality_status))

    assert row.quality_status == quality_status


@pytest.mark.parametrize("quality_status", ("passed", "warning", "error", "skipped"))
def test_quality_rejects_unsupported_status(quality_status):
    with pytest.raises(FuturesQualityValidationError):
        validate_quality_row_values(_quality_row(quality_status=quality_status))


@pytest.mark.parametrize(
    "counter_field",
    (
        "rows",
        "duplicate_key_count",
        "gap_count",
        "null_ohlc_count",
        "invalid_ohlc_count",
        "futoi_missing_count",
    ),
)
def test_quality_counters_must_be_non_negative_integers(counter_field):
    validate_quality_row_values(_quality_row(**{counter_field: 0}))
    with pytest.raises(FuturesQualityValidationError):
        validate_quality_row_values(_quality_row(**{counter_field: -1}))
    with pytest.raises(FuturesQualityValidationError):
        validate_quality_row_values(_quality_row(**{counter_field: 1.5}))
    with pytest.raises(FuturesQualityValidationError):
        validate_quality_row_values(_quality_row(**{counter_field: True}))


@pytest.mark.parametrize(
    "missing_field",
    (
        "run_id",
        "dataset_id",
        "family",
        "secid",
        "trade_date",
        "rows",
        "duplicate_key_count",
        "gap_count",
        "null_ohlc_count",
        "invalid_ohlc_count",
        "futoi_missing_count",
        "calendar_status",
        "quality_status",
    ),
)
def test_quality_row_missing_required_fields_fail_closed(missing_field):
    values = _quality_row()
    values.pop(missing_field)

    with pytest.raises(FuturesQualityValidationError):
        validate_quality_row_values(values)


def test_quality_report_requires_rows_from_one_run():
    report = validate_quality_report_rows((_quality_row(), _quality_row(dataset_id="futures_futoi_raw")))

    assert report.run_id == "refresh_run.contract_test.v1"
    with pytest.raises(FuturesQualityValidationError):
        validate_quality_report_rows(())
    with pytest.raises(FuturesQualityValidationError):
        validate_quality_report_rows((_quality_row(), _quality_row(run_id="other_run")))


def test_futures_package_has_no_imports_from_forbidden_architecture_layers():
    forbidden_prefixes = ("moex_runtime", "moex_backtest", "moex_research", "strategies")
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


def test_futures_helper_source_has_no_forbidden_heavy_or_operational_dependencies():
    source = "\n".join(
        path.read_text(encoding="utf-8").casefold()
        for path in (REPO_ROOT / "src" / "moex_data" / "futures").glob("*.py")
    )
    forbidden_terms = ("requests", "urllib", "socket", "subprocess", "pandas", "numpy", "pyarrow")

    for term in forbidden_terms:
        assert term not in source
