import json
from pathlib import Path

import pandas as pd
import pytest

import moex_data.futures.materialize_raw_5m as materializer


CONTRACT_IDS = (
    "futures_raw_5m.v1",
    "futures_futoi_raw.v1",
    "futures_derived_d1.v1",
    "futures_derived_w1.v1",
    "futures_data_refresh_manifest.v1",
    "futures_quality_report.v1",
    "futures_continuous_5m.v1",
)

CONTRACT_PATHS = (
    "contracts/datasets/futures_raw_5m.v1.yaml",
    "contracts/datasets/futures_futoi_raw.v1.yaml",
    "contracts/datasets/futures_derived_d1.v1.yaml",
    "contracts/datasets/futures_derived_w1.v1.yaml",
    "contracts/datasets/futures_data_refresh_manifest.v1.yaml",
    "contracts/datasets/futures_quality_report.v1.yaml",
    "contracts/datasets/futures_continuous_5m.v1.yaml",
)


def _dataset_id(contract_id):
    return contract_id.removesuffix(".v1")


def _path_pattern(contract_id):
    if contract_id == "futures_raw_5m.v1":
        return "${MOEX_DATA_ROOT}/futures/raw_5m/trade_date={YYYY-MM-DD}/family={FAMILY}/secid={SECID}/part.parquet"
    if contract_id == "futures_data_refresh_manifest.v1":
        return "${MOEX_DATA_ROOT}/futures/runs/raw_5m/trade_date={YYYY-MM-DD}/run_id={RUN_ID}/manifest.json"
    if contract_id == "futures_quality_report.v1":
        return "${MOEX_DATA_ROOT}/futures/quality/raw_5m/trade_date={YYYY-MM-DD}/run_id={RUN_ID}/quality.json"
    return "${MOEX_DATA_ROOT}/futures/unused/" + _dataset_id(contract_id) + "/trade_date={YYYY-MM-DD}/part.parquet"


def _write_contract_package(repo_root: Path):
    (repo_root / "configs/datasets").mkdir(parents=True)
    (repo_root / "contracts/datasets").mkdir(parents=True)
    config = """
config_id: futures_data_lake.v1
artifact_class: repo_relative
repo_path: configs/datasets/futures_data_lake.v1.yaml
external_storage_root:
  artifact_id: moex_data_root
  artifact_class: env_contract
  env_var: MOEX_DATA_ROOT
  required: true
  hardcoded_server_path_allowed: false
storage_backend:
  primary: parquet_partitioned_lake
  query_layer: duckdb
dataset_contract_refs:
  - contracts/datasets/futures_raw_5m.v1.yaml
  - contracts/datasets/futures_futoi_raw.v1.yaml
  - contracts/datasets/futures_derived_d1.v1.yaml
  - contracts/datasets/futures_derived_w1.v1.yaml
  - contracts/datasets/futures_data_refresh_manifest.v1.yaml
  - contracts/datasets/futures_quality_report.v1.yaml
  - contracts/datasets/futures_continuous_5m.v1.yaml
artifact_class_index:
  futures_raw_5m.v1: external_pattern
  futures_futoi_raw.v1: external_pattern
  futures_derived_d1.v1: external_pattern
  futures_derived_w1.v1: external_pattern
  futures_data_refresh_manifest.v1: external_pattern
  futures_quality_report.v1: external_pattern
  futures_continuous_5m.v1: external_pattern
  moex_data_root: env_contract
  futures_data_lake.v1: repo_relative
blocked_contracts:
  - futures_continuous_5m.v1
path_rules:
  external_root_source: env_contract
  hardcoded_server_path_allowed: false
  implicit_file_selection_allowed: false
""".lstrip()
    (repo_root / "configs/datasets/futures_data_lake.v1.yaml").write_text(config, encoding="utf-8")
    for contract_id in CONTRACT_IDS:
        content = (
            "contract_id: " + contract_id + "\n"
            "dataset_id: " + _dataset_id(contract_id) + "\n"
            "artifact_class: external_pattern\n"
            "producer: test\n"
            "consumers:\n"
            "  - test\n"
            "format: parquet\n"
            "schema_version: " + contract_id + "\n"
            "storage_root_ref: MOEX_DATA_ROOT\n"
            "path_pattern: \"" + _path_pattern(contract_id) + "\"\n"
            "partitioning:\n"
            "  - trade_date\n"
        )
        (repo_root / "contracts/datasets" / (contract_id + ".yaml")).write_text(content, encoding="utf-8")


def _call_kwargs(repo_root, data_root):
    return {
        "repo_root": repo_root,
        "dataset_id": materializer.TARGET_DATASET_ID,
        "contract_id": materializer.TARGET_CONTRACT_ID,
        "trade_date": materializer.TARGET_TRADE_DATE,
        "family": materializer.TARGET_FAMILY,
        "secid": materializer.TARGET_SECID,
        "source_path": None,
        "run_id": "run_apim",
        "env": {"MOEX_DATA_ROOT": str(data_root), "MOEX_API_URL": "https://apim.moex.test", "MOEX_UA": "pytest"},
        "source_candidate": materializer.SOURCE_CANDIDATE_APIM_TRADESTATS,
        "source_endpoint": materializer.SOURCE_ENDPOINT_APIM_FO_TRADESTATS,
        "market": materializer.TARGET_MARKET,
        "board": materializer.TARGET_BOARD,
        "series_type": materializer.TARGET_SERIES_TYPE,
        "granularity": materializer.TARGET_GRANULARITY,
        "timeout": 3.0,
        "apim_base_url": "https://apim.moex.test",
    }


def test_declared_apim_tradestats_materialization_writes_consumer_boundary(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    data_root = tmp_path / "data"
    repo_root.mkdir()
    _write_contract_package(repo_root)
    seen = {}

    class Response:
        url = "https://apim.moex.test/iss/datashop/algopack/fo/tradestats.json?date=2026-06-02&from=2026-06-02&till=2026-06-02&secid=SiM6&start=0&iss.meta=off&iss.only=tradestats"

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": {
                    "columns": ["tradedate", "tradetime", "secid", "pr_open", "pr_high", "pr_low", "pr_close", "vol", "val", "trades"],
                    "data": [
                        ["2026-06-02", "10:00:00", "SiM6", 100, 110, 99, 105, 10, 1000, 2],
                        ["2026-06-02", "10:05:00", "SiM6", 105, 111, 104, 108, 12, 1200, 3],
                    ],
                }
            }

    def fake_get(url, params, headers, timeout):
        seen["url"] = url
        seen["params"] = dict(params)
        seen["headers"] = dict(headers)
        seen["timeout"] = timeout
        return Response()

    monkeypatch.setattr(materializer.requests, "get", fake_get)
    result = materializer.materialize_single_raw_5m_partition(**_call_kwargs(repo_root, data_root))

    assert result.status == materializer.SUCCEEDED_STATUS
    assert result.rows == 2
    assert seen["url"] == "https://apim.moex.test/iss/datashop/algopack/fo/tradestats.json"
    assert "candles" not in seen["url"].casefold()
    assert seen["params"] == {
        "date": "2026-06-02",
        "from": "2026-06-02",
        "till": "2026-06-02",
        "secid": "SiM6",
        "start": 0,
        "iss.meta": "off",
        "iss.only": "tradestats",
    }
    partition = pd.read_parquet(result.partition_path)
    assert list(partition.columns) == list(materializer.RAW_5M_REQUIRED_COLUMNS)
    assert set(partition["source"]) == {materializer.SOURCE_CANDIDATE_APIM_TRADESTATS}
    assert set(partition["board"]) == {materializer.TARGET_BOARD}
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    source_contract = manifest["source_contract"]
    assert source_contract["source_candidate"] == materializer.SOURCE_CANDIDATE_APIM_TRADESTATS
    assert source_contract["source_endpoint"] == materializer.SOURCE_ENDPOINT_APIM_FO_TRADESTATS
    assert source_contract["market"] == materializer.TARGET_MARKET
    assert source_contract["board"] == materializer.TARGET_BOARD
    assert source_contract["granularity"] == materializer.TARGET_GRANULARITY
    assert source_contract["series_type"] == materializer.TARGET_SERIES_TYPE
    assert source_contract["failure_semantics"]["empty_response"] == "fail_closed"
    assert source_contract["failure_semantics"]["implicit_fallback"] == "forbidden"
    assert source_contract["failure_semantics"]["iss_candles_masking"] == "forbidden"


def test_declared_apim_empty_response_fails_closed(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    data_root = tmp_path / "data"
    repo_root.mkdir()
    _write_contract_package(repo_root)

    class Response:
        url = "https://apim.moex.test/iss/datashop/algopack/fo/tradestats.json"

        def raise_for_status(self):
            return None

        def json(self):
            return {"data": {"columns": ["tradedate"], "data": []}}

    monkeypatch.setattr(materializer.requests, "get", lambda *args, **kwargs: Response())
    with pytest.raises(materializer.FuturesRaw5mMaterializationError) as exc_info:
        materializer.materialize_single_raw_5m_partition(**_call_kwargs(repo_root, data_root))
    assert exc_info.value.status == materializer.VALIDATION_FAILED_STATUS
    assert "returned no rows" in exc_info.value.message
    assert not (data_root / "futures/raw_5m").exists()


def test_missing_source_contract_inputs_fail_closed(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    with pytest.raises(materializer.FuturesRaw5mMaterializationError) as exc_info:
        materializer.build_materialization_request(
            repo_root=repo_root,
            dataset_id=materializer.TARGET_DATASET_ID,
            contract_id=materializer.TARGET_CONTRACT_ID,
            trade_date=materializer.TARGET_TRADE_DATE,
            family=materializer.TARGET_FAMILY,
            secid=materializer.TARGET_SECID,
            source_path=None,
            run_id="run_missing_source_contract",
        )
    assert exc_info.value.status == materializer.VALIDATION_FAILED_STATUS
    assert "source_candidate" in exc_info.value.message
