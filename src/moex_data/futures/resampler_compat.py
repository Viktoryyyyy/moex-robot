from __future__ import annotations

from pathlib import Path

from . import resampler as _base

FuturesD1ReadinessError = _base.FuturesD1ReadinessError
FuturesD1ReadinessPaths = _base.FuturesD1ReadinessPaths
FuturesD1ReadinessResult = _base.FuturesD1ReadinessResult
RAW_5M_DATASET_ID = _base.RAW_5M_DATASET_ID
RAW_5M_CONTRACT_ID = _base.RAW_5M_CONTRACT_ID
D1_DATASET_ID = _base.D1_DATASET_ID
D1_CONTRACT_ID = _base.D1_CONTRACT_ID
TARGET_FAMILY = _base.TARGET_FAMILY
TARGET_SECID = _base.TARGET_SECID
TARGET_SERIES_TYPE = _base.TARGET_SERIES_TYPE
APPROVED_TRADE_DATES = _base.APPROVED_TRADE_DATES
SUCCEEDED_STATUS = _base.SUCCEEDED_STATUS
VALIDATION_FAILED_STATUS = _base.VALIDATION_FAILED_STATUS
MANIFEST_QUALITY_LINKAGE_STATUS = _base.MANIFEST_QUALITY_LINKAGE_STATUS
RAW_5M_REQUIRED_COLUMNS = _base.RAW_5M_REQUIRED_COLUMNS
D1_REQUIRED_COLUMNS = _base.D1_REQUIRED_COLUMNS
OHLC_COLUMNS = _base.OHLC_COLUMNS


def _legacy_raw_path(root: str, trade_date: str, family: str, secid: str) -> Path:
    return Path(root) / "futures" / "raw_5m" / ("trade_date=" + trade_date) / ("family=" + family) / ("secid=" + secid) / "part.parquet"


def d1_readiness_paths(repo_root, trade_dates, family, secid, series_type, env=None):
    result = _base.d1_readiness_paths(repo_root, trade_dates, family, secid, series_type, env)
    root = None if env is None else env.get("MOEX_DATA_ROOT")
    if not root:
        return result
    resolved = []
    for trade_date, path in zip(trade_dates, result.input_partition_paths):
        if path.exists():
            resolved.append(path)
            continue
        legacy_path = _legacy_raw_path(str(root), str(trade_date), str(family), str(secid))
        resolved.append(legacy_path if legacy_path.exists() else path)
    return FuturesD1ReadinessPaths(tuple(resolved), result.output_partition_path)


def derive_d1_readiness_from_raw_5m_partitions(repo_root, trade_dates, family, secid, series_type, env=None):
    checked_dates = _base._require_trade_dates(trade_dates)
    checked_family, checked_secid, checked_series_type = _base._require_target_scope(family, secid, series_type)
    paths = d1_readiness_paths(repo_root, checked_dates, checked_family, checked_secid, checked_series_type, env)
    rows = []
    for trade_date, input_path in zip(checked_dates, paths.input_partition_paths):
        raw_frame = _base._read_parquet(input_path)
        checked_frame, metrics = _base._validate_raw_partition(raw_frame, trade_date, checked_family, checked_secid)
        rows.append(_base._aggregate_day(checked_frame, metrics, input_path, checked_family, checked_secid, checked_series_type))
    output = _base.pd.DataFrame(rows).sort_values(["trade_date", "symbol", "series_type"], kind="mergesort").reset_index(drop=True)
    _base._validate_d1_output(output, checked_dates, checked_family, checked_secid, checked_series_type)
    _base._write_parquet_atomic(paths.output_partition_path, output)
    rows_per_trade_date = {trade_date: int(count) for trade_date, count in output.groupby("trade_date").size().items()}
    return FuturesD1ReadinessResult(
        status=SUCCEEDED_STATUS,
        rows=int(len(output.index)),
        output_partition_path=paths.output_partition_path,
        trade_dates=tuple(output["trade_date"].astype(str).tolist()),
        symbols=tuple(sorted(set(output["symbol"].astype(str)))),
        rows_per_trade_date=rows_per_trade_date,
        input_partition_paths=paths.input_partition_paths,
        manifest_quality_linkage_status=MANIFEST_QUALITY_LINKAGE_STATUS,
    )


def _result_payload(result):
    return _base._result_payload(result)


def _error_payload(error):
    return _base._error_payload(error)


def parse_args(argv=None):
    return _base.parse_args(argv)


def main(argv=None):
    return _base.main(argv)
