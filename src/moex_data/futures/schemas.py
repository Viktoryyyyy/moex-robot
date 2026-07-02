from dataclasses import dataclass
from typing import Final


ALLOWED_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset(
    {"repo_relative", "external_pattern", "cli_argument", "env_contract"}
)
ALLOWED_DATASET_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset({"external_pattern"})
ALLOWED_CONFIG_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset({"repo_relative"})
ALLOWED_EXTERNAL_ROOT_ARTIFACT_CLASSES: Final[frozenset[str]] = frozenset({"env_contract"})

EXPECTED_DATASET_CONTRACT_IDS: Final[tuple[str, ...]] = (
    "futures_raw_5m.v1",
    "futures_futoi_raw.v1",
    "futures_derived_d1.v1",
    "futures_derived_w1.v1",
    "futures_data_refresh_manifest.v1",
    "futures_quality_report.v1",
    "futures_continuous_5m.v1",
)
EXPECTED_DATASET_CONTRACT_PATHS: Final[tuple[str, ...]] = (
    "contracts/datasets/futures_raw_5m.v1.yaml",
    "contracts/datasets/futures_futoi_raw.v1.yaml",
    "contracts/datasets/futures_derived_d1.v1.yaml",
    "contracts/datasets/futures_derived_w1.v1.yaml",
    "contracts/datasets/futures_data_refresh_manifest.v1.yaml",
    "contracts/datasets/futures_quality_report.v1.yaml",
    "contracts/datasets/futures_continuous_5m.v1.yaml",
)
EXPECTED_CONFIG_ID: Final[str] = "futures_data_lake.v1"
EXPECTED_CONFIG_PATH: Final[str] = "configs/datasets/futures_data_lake.v1.yaml"
EXPECTED_STORAGE_ROOT_REF: Final[str] = "MOEX_DATA_ROOT"


@dataclass(frozen=True)
class FuturesDatasetContract:
    contract_id: str
    dataset_id: str
    artifact_class: str
    producer: str
    consumers: tuple[str, ...]
    format: str
    schema_version: str
    storage_root_ref: str
    path_pattern: str
    partitioning: tuple[str, ...]
    implementation_status: str | None = None


@dataclass(frozen=True)
class FuturesDataLakeConfig:
    config_id: str
    artifact_class: str
    repo_path: str
    storage_root_env_var: str
    dataset_contract_refs: tuple[str, ...]
    artifact_class_index: dict[str, str]
    blocked_contracts: tuple[str, ...]


@dataclass(frozen=True)
class FuturesRefreshManifest:
    run_id: str
    run_date: str
    dataset_contract_refs: tuple[str, ...]
    partitions_written: tuple[str, ...]
    partitions_skipped: tuple[str, ...]
    quality_report_ref: str
    refresh_status: str
    instrument_scope: tuple[str, ...]
    source_scope: tuple[str, ...]
    accepted_manifest_ref: str


@dataclass(frozen=True)
class FuturesQualityRow:
    run_id: str
    dataset_id: str
    instrument_id: str
    source_id: str
    secid: str
    board: str
    market: str
    engine: str
    trade_date: str
    rows: int
    duplicate_key_count: int
    gap_count: int
    null_ohlc_count: int
    invalid_ohlc_count: int
    futoi_missing_count: int
    calendar_status: str
    quality_status: str
    family: str | None = None


@dataclass(frozen=True)
class FuturesQualityReport:
    run_id: str
    rows: tuple[FuturesQualityRow, ...]
