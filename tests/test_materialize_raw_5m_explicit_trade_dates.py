import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import materialize_raw_5m as materializer


CONTRACT_IDS = (
    "futures_raw_5m.v1",
    "futures_futoi_raw.v1",
    "futures_derived_d1.v1",
    "futures_derived_w1.v1",
    "futures_data_refresh_manifest.v1",
    "futures_quality_report.v1",
    "futures_continuous_5m.v1",
)
FIXTURE_INSTRUMENT_ID = "forts.test.si"
FIXTURE_SOURCE_ID = "moex_algopack_fo_tradestats_snapshot.v1"
FIXTURE_SECID = "SiM6"
FIXTURE_FAMILY = "Si"
FIXTURE_MARKET = "FORTS"
FIXTURE_BOARD = "RFUD"
FIXTURE_ENGINE = "futures"


def _dataset_id(contract_id: str) -> str:
    return contract_id.removesuffix(".v1")


def _path_pattern(contract_id: str) -> str:
    if contract_id == "futures_raw_5m.v1":
        return (
            "${MOEX_DATA_ROOT}/market/raw/timeframe=5m/"
            "instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet"
        )
    if contract_id == "futures_data_refresh_manifest.v1":
        return "${MOEX_DATA_ROOT}/futures/runs/raw_5m/trade_date={YYYY-MM-DD}/run_id={RUN_ID}/manifest.json"
    if contract_id == "futures_quality_report.v1":
        return "${MOEX_DATA_ROOT}/futures/quality/raw_5m/trade_date={YYYY-MM-DD}/run_id={RUN_ID}/quality.json"
    return "${MOEX_DATA_ROOT}/futures/unused/" + _dataset_id(contract_id) + "/trade_date={YYYY-MM-DD}/part.parquet"


def _write_contract_package(repo_root: Path) -> None:
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


def _call_kwargs(repo_root: Path, data_root: Path, trade_date: str) -> dict[str, object]:
    return {
        "repo_root": repo_root,
        "dataset_id": materializer.TARGET_DATASET_ID,
        "contract_id": materializer.TARGET_CONTRACT_ID,
        "trade_date": trade_date,
        "instrument_id": FIXTURE_INSTRUMENT_ID,
        "source_id": FIXTURE_SOURCE_ID,
        "secid": FIXTURE_SECID,
        "source_path": None,
        "run_id": "run_apim_explicit_date",
        "env": {"MOEX_DATA_ROOT": str(data_root), "MOEX_API_URL": "https://apim.moex.test", "MOEX_UA": "pytest"},
        "source_candidate": materializer.SOURCE_CANDIDATE_APIM_TRADESTATS,
        "source_endpoint": materializer.SOURCE_ENDPOINT_APIM_FO_TRADESTATS,
        "market": FIXTURE_MARKET,
        "board": FIXTURE_BOARD,
        "engine": FIXTURE_ENGINE,
        "series_type": materializer.TARGET_SERIES_TYPE,
        "granularity": materializer.TARGET_GRANULARITY,
        "family": FIXTURE_FAMILY,
        "timeout": 3.0,
        "apim_base_url": "https://apim.moex.test",
    }


def test_non_default_explicit_trade_date_materializes_contract_partition(tmp_path, monkeypatch):
    trade_date = "2026-06-03"
    repo_root = tmp_path / "repo"
    data_root = tmp_path / "data"
    repo_root.mkdir()
    _write_contract_package(repo_root)
    seen = {}

    class Response:
        url = "https://apim.moex.test/iss/datashop/algopack/fo/tradestats.json?date=2026-06-03&from=2026-06-03&till=2026-06-03&secid=SiM6&start=0&iss.meta=off&iss.only=tradestats"

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": {
                    "columns": ["tradedate", "tradetime", "secid", "pr_open", "pr_high", "pr_low", "pr_close", "vol", "val", "trades"],
                    "data": [
                        [trade_date, "10:00:00", FIXTURE_SECID, 100, 110, 99, 105, 10, 1000, 2],
                        [trade_date, "10:05:00", FIXTURE_SECID, 105, 111, 104, 108, 12, 1200, 3],
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
    result = materializer.materialize_single_raw_5m_partition(**_call_kwargs(repo_root, data_root, trade_date))

    assert result.status == materializer.SUCCEEDED_STATUS
    assert result.rows == 2
    assert seen["params"] == {
        "date": trade_date,
        "from": trade_date,
        "till": trade_date,
        "secid": FIXTURE_SECID,
        "start": 0,
        "iss.meta": "off",
        "iss.only": "tradestats",
    }
    assert "instrument_id=forts.test.si" in result.partition_path.as_posix()
    assert "trade_date=2026-06-03" in result.partition_path.as_posix()
    assert "source=moex_algopack_fo_tradestats_snapshot.v1" in result.partition_path.as_posix()
    partition = pd.read_parquet(result.partition_path)
    assert set(partition["trade_date"]) == {trade_date}
    assert set(partition["instrument_id"]) == {FIXTURE_INSTRUMENT_ID}
    assert set(partition["source_id"]) == {FIXTURE_SOURCE_ID}
    assert set(partition["secid"]) == {FIXTURE_SECID}
    quality_report = json.loads(result.quality_report_path.read_text(encoding="utf-8"))
    assert quality_report["rows"][0]["quality_status"] == "pass"
    assert quality_report["rows"][0]["trade_date"] == trade_date
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["source_contract"]["trade_date"] == trade_date
    assert manifest["source_contract"]["instrument_id"] == FIXTURE_INSTRUMENT_ID
    assert manifest["source_contract"]["source_id"] == FIXTURE_SOURCE_ID
    assert manifest["source_contract"]["source_fetch_mode"] == "declared_apim_tradestats"


def test_non_explicit_trade_date_fails_closed(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    with pytest.raises(materializer.FuturesRaw5mMaterializationError, match="explicit YYYY-MM-DD"):
        materializer.build_materialization_request(
            repo_root=repo_root,
            dataset_id=materializer.TARGET_DATASET_ID,
            contract_id=materializer.TARGET_CONTRACT_ID,
            trade_date="2026-06",
            instrument_id=FIXTURE_INSTRUMENT_ID,
            source_id=FIXTURE_SOURCE_ID,
            secid=FIXTURE_SECID,
            source_path=None,
            run_id="run_bad_date",
            source_candidate=materializer.SOURCE_CANDIDATE_APIM_TRADESTATS,
            source_endpoint=materializer.SOURCE_ENDPOINT_APIM_FO_TRADESTATS,
            market=FIXTURE_MARKET,
            board=FIXTURE_BOARD,
            engine=FIXTURE_ENGINE,
            series_type=materializer.TARGET_SERIES_TYPE,
            granularity=materializer.TARGET_GRANULARITY,
            family=FIXTURE_FAMILY,
        )
