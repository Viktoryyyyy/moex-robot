import pandas as pd
import pytest

from moex_data.futures import resampler
from test_materialize_raw_5m_explicit_trade_dates import _write_contract_package


def _write_d1_ready_contract_package(repo_root):
    _write_contract_package(repo_root)
    (repo_root / "contracts" / "datasets" / "futures_derived_d1.v1.yaml").write_text(
        """
contract_id: futures_derived_d1.v1
dataset_id: futures_derived_d1
artifact_class: external_pattern
producer: moex_data.futures.resampler
consumers:
  - test
format: parquet
schema_version: futures_derived_d1.v1
storage_root_ref: MOEX_DATA_ROOT
path_pattern: "${MOEX_DATA_ROOT}/futures/derived_d1/series_type={SERIES_TYPE}/family={FAMILY}/part.parquet"
partitioning:
  - series_type
  - family
""".lstrip(),
        encoding="utf-8",
    )


def _raw_partition_path(data_root, trade_date):
    return data_root / "futures" / "raw_5m" / ("trade_date=" + trade_date) / "family=Si" / "secid=SiM6" / "part.parquet"


def _write_raw_partition(data_root, trade_date, base):
    path = _raw_partition_path(data_root, trade_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "ts": trade_date + " 10:00:00",
                "session_date": trade_date,
                "secid": "SiM6",
                "family": "Si",
                "board": "RFUD",
                "open": base,
                "high": base + 4,
                "low": base - 2,
                "close": base + 1,
                "volume": 10,
                "value": 1000,
                "num_trades": 2,
                "source": "MOEX_ALGOPACK_FO_TRADESTATS",
                "ingest_ts": trade_date + "T12:00:00+00:00",
            },
            {
                "trade_date": trade_date,
                "ts": trade_date + " 10:05:00",
                "session_date": trade_date,
                "secid": "SiM6",
                "family": "Si",
                "board": "RFUD",
                "open": base + 1,
                "high": base + 6,
                "low": base - 1,
                "close": base + 3,
                "volume": 20,
                "value": 2000,
                "num_trades": 3,
                "source": "MOEX_ALGOPACK_FO_TRADESTATS",
                "ingest_ts": trade_date + "T12:00:00+00:00",
            },
        ]
    ).to_parquet(path, index=False)
    return path


def _prepare_repo_and_inputs(tmp_path):
    repo_root = tmp_path / "repo"
    data_root = tmp_path / "data"
    repo_root.mkdir()
    _write_d1_ready_contract_package(repo_root)
    for index, trade_date in enumerate(resampler.APPROVED_TRADE_DATES):
        _write_raw_partition(data_root, trade_date, 100 + index * 10)
    return repo_root, data_root


def test_controlled_d1_readiness_derives_one_row_per_approved_trade_date(tmp_path):
    repo_root, data_root = _prepare_repo_and_inputs(tmp_path)
    env = {"MOEX_DATA_ROOT": str(data_root)}

    result = resampler.derive_d1_readiness_from_raw_5m_partitions(
        repo_root=repo_root,
        trade_dates=resampler.APPROVED_TRADE_DATES,
        family="Si",
        secid="SiM6",
        series_type="native",
        env=env,
    )

    assert result.status == resampler.SUCCEEDED_STATUS
    assert result.rows == 4
    assert result.trade_dates == resampler.APPROVED_TRADE_DATES
    assert result.symbols == ("SiM6",)
    assert result.rows_per_trade_date == {trade_date: 1 for trade_date in resampler.APPROVED_TRADE_DATES}
    assert result.output_partition_path == data_root / "futures" / "derived_d1" / "series_type=native" / "family=Si" / "part.parquet"

    output = pd.read_parquet(result.output_partition_path)
    assert list(output["trade_date"]) == list(resampler.APPROVED_TRADE_DATES)
    assert set(output["symbol"]) == {"SiM6"}
    assert set(output["secid"]) == {"SiM6"}
    assert set(output["series_type"]) == {"native"}
    assert set(output["source_schema_version"]) == {resampler.RAW_5M_CONTRACT_ID}
    assert set(output["input_manifest_quality_linkage_status"]) == {resampler.MANIFEST_QUALITY_LINKAGE_STATUS}

    row = output.loc[output["trade_date"] == "2026-06-03"].iloc[0]
    assert row["open"] == 110
    assert row["high"] == 116
    assert row["low"] == 108
    assert row["close"] == 113
    assert row["volume"] == 30
    assert row["value"] == 3000
    assert row["num_trades"] == 5

    first = output.copy()
    second_result = resampler.derive_d1_readiness_from_raw_5m_partitions(
        repo_root=repo_root,
        trade_dates=resampler.APPROVED_TRADE_DATES,
        family="Si",
        secid="SiM6",
        series_type="native",
        env=env,
    )
    second = pd.read_parquet(second_result.output_partition_path)
    pd.testing.assert_frame_equal(first, second)


def test_controlled_d1_readiness_rejects_unapproved_trade_date_scope(tmp_path):
    repo_root, data_root = _prepare_repo_and_inputs(tmp_path)
    with pytest.raises(resampler.FuturesD1ReadinessError, match="approved controlled D1 readiness slice"):
        resampler.derive_d1_readiness_from_raw_5m_partitions(
            repo_root=repo_root,
            trade_dates=("2026-06-02", "2026-06-03", "2026-06-04"),
            family="Si",
            secid="SiM6",
            series_type="native",
            env={"MOEX_DATA_ROOT": str(data_root)},
        )


def test_controlled_d1_readiness_rejects_mixed_secid_input(tmp_path):
    repo_root, data_root = _prepare_repo_and_inputs(tmp_path)
    bad_path = _raw_partition_path(data_root, "2026-06-02")
    bad = pd.read_parquet(bad_path)
    bad.loc[0, "secid"] = "BRM6"
    bad.to_parquet(bad_path, index=False)

    with pytest.raises(resampler.FuturesD1ReadinessError, match="secid values"):
        resampler.derive_d1_readiness_from_raw_5m_partitions(
            repo_root=repo_root,
            trade_dates=resampler.APPROVED_TRADE_DATES,
            family="Si",
            secid="SiM6",
            series_type="native",
            env={"MOEX_DATA_ROOT": str(data_root)},
        )
