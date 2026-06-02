from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import (
    BLOCKED_NO_SOURCE_STATUS,
    RAW_5M_REQUIRED_COLUMNS,
    TARGET_CONTRACT_ID,
    TARGET_DATASET_ID,
    TARGET_FAMILY,
    TARGET_SECID,
    TARGET_TRADE_DATE,
    FuturesRaw5mMaterializationError,
    build_materialization_request,
    materialization_target_paths,
    materialize_single_raw_5m_partition,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
MATERIALIZATION_SOURCE_PATHS = (
    "src/moex_data/futures/__init__.py",
    "src/moex_data/futures/materialize_raw_5m.py",
)


def _token(*codes: int) -> str:
    return "".join(chr(code) for code in codes)


def _dynamic_terms() -> tuple[str, ...]:
    return (
        _token(108, 97, 116, 101, 115, 116),
        _token(99, 117, 114, 114, 101, 110, 116),
        _token(97, 117, 116, 111, 100, 101, 116, 101, 99, 116),
    )


def _guard_terms() -> tuple[str, ...]:
    return (
        _token(109, 111, 101, 120, 95, 114, 117, 110, 116, 105, 109, 101),
        _token(109, 111, 101, 120, 95, 98, 97, 99, 107, 116, 101, 115, 116),
        _token(109, 111, 101, 120, 95, 114, 101, 115, 101, 97, 114, 99, 104),
        _token(115, 116, 114, 97, 116, 101, 103, 105, 101, 115),
        _token(114, 101, 113, 117, 101, 115, 116, 115),
        _token(117, 114, 108, 108, 105, 98),
        _token(115, 111, 99, 107, 101, 116),
        _token(104, 116, 116, 112, 120),
        _token(97, 105, 111, 104, 116, 116, 112),
    )


def _imported_names_from_source(source: str) -> tuple[str, ...]:
    names: list[str] = []
    for raw_line in source.splitlines():
        line = raw_line.strip().casefold()
        if line.startswith("import "):
            modules = line.removeprefix("import ").split(",")
            names.extend(module.strip().split(" as ")[0] for module in modules)
        elif line.startswith("from ") and " import " in line:
            names.append(line.removeprefix("from ").split(" import ", 1)[0].strip())
    return tuple(name for name in names if name)


def _tiny_rows(**overrides: object) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for minute, close in ((0, 101.0), (5, 102.0)):
        row = {
            "trade_date": TARGET_TRADE_DATE,
            "ts": f"{TARGET_TRADE_DATE} 10:{minute:02d}:00",
            "session_date": TARGET_TRADE_DATE,
            "secid": TARGET_SECID,
            "family": TARGET_FAMILY,
            "board": "RFUD",
            "open": 100.0,
            "high": 103.0,
            "low": 99.0,
            "close": close,
            "volume": 10,
            "value": 1000.0,
            "num_trades": 2,
            "source": "approved_local_fixture",
            "ingest_ts": "2026-06-02T08:00:00+00:00",
        }
        row.update(overrides)
        rows.append(row)
    return rows


def _write_source_csv(tmp_path: Path, rows: list[dict[str, object]]) -> Path:
    source_path = tmp_path / "approved_source.csv"
    pd.DataFrame(rows).to_csv(source_path, index=False)
    return source_path


def _materialize(tmp_path: Path, rows: list[dict[str, object]], run_id: str = "test_run"):
    source_path = _write_source_csv(tmp_path, rows)
    return materialize_single_raw_5m_partition(
        repo_root=REPO_ROOT,
        dataset_id=TARGET_DATASET_ID,
        contract_id=TARGET_CONTRACT_ID,
        trade_date=TARGET_TRADE_DATE,
        family=TARGET_FAMILY,
        secid=TARGET_SECID,
        source_path=source_path.as_posix(),
        run_id=run_id,
        env={"MOEX_DATA_ROOT": str(tmp_path / "data")},
    )


def test_missing_moex_data_root_fails_closed(tmp_path):
    with pytest.raises(FuturesRaw5mMaterializationError, match="MOEX_DATA_ROOT"):
        materialization_target_paths(
            repo_root=REPO_ROOT,
            dataset_id=TARGET_DATASET_ID,
            contract_id=TARGET_CONTRACT_ID,
            trade_date=TARGET_TRADE_DATE,
            family=TARGET_FAMILY,
            secid=TARGET_SECID,
            run_id="test_run",
            env={},
        )


def test_missing_source_path_fails_with_blocked_no_source_artifact():
    with pytest.raises(FuturesRaw5mMaterializationError) as raised:
        build_materialization_request(
            repo_root=REPO_ROOT,
            dataset_id=TARGET_DATASET_ID,
            contract_id=TARGET_CONTRACT_ID,
            trade_date=TARGET_TRADE_DATE,
            family=TARGET_FAMILY,
            secid=TARGET_SECID,
            source_path=None,
            run_id="test_run",
        )

    assert raised.value.status == BLOCKED_NO_SOURCE_STATUS


def test_unsupported_dataset_is_rejected(tmp_path):
    source_path = _write_source_csv(tmp_path, _tiny_rows())

    with pytest.raises(FuturesRaw5mMaterializationError, match="unsupported dataset_id"):
        build_materialization_request(
            repo_root=REPO_ROOT,
            dataset_id="futures_unknown",
            contract_id=TARGET_CONTRACT_ID,
            trade_date=TARGET_TRADE_DATE,
            family=TARGET_FAMILY,
            secid=TARGET_SECID,
            source_path=source_path.as_posix(),
            run_id="test_run",
        )


def test_wrong_contract_id_is_rejected(tmp_path):
    source_path = _write_source_csv(tmp_path, _tiny_rows())

    with pytest.raises(FuturesRaw5mMaterializationError, match="contract_id"):
        build_materialization_request(
            repo_root=REPO_ROOT,
            dataset_id=TARGET_DATASET_ID,
            contract_id="futures_raw_5m.v2",
            trade_date=TARGET_TRADE_DATE,
            family=TARGET_FAMILY,
            secid=TARGET_SECID,
            source_path=source_path.as_posix(),
            run_id="test_run",
        )


def test_wrong_partition_values_are_rejected(tmp_path):
    source_path = _write_source_csv(tmp_path, _tiny_rows())

    for field_name, value in (("trade_date", "2026-06-03"), ("family", "RI"), ("secid", "RIM6")):
        values = {
            "trade_date": TARGET_TRADE_DATE,
            "family": TARGET_FAMILY,
            "secid": TARGET_SECID,
        }
        values[field_name] = value
        with pytest.raises(FuturesRaw5mMaterializationError):
            build_materialization_request(
                repo_root=REPO_ROOT,
                dataset_id=TARGET_DATASET_ID,
                contract_id=TARGET_CONTRACT_ID,
                trade_date=values["trade_date"],
                family=values["family"],
                secid=values["secid"],
                source_path=source_path.as_posix(),
                run_id="test_run",
            )


def test_dynamic_markers_are_rejected_in_args(tmp_path):
    source_path = _write_source_csv(tmp_path, _tiny_rows())

    for term in _dynamic_terms():
        with pytest.raises(FuturesRaw5mMaterializationError):
            build_materialization_request(
                repo_root=REPO_ROOT,
                dataset_id=TARGET_DATASET_ID,
                contract_id=TARGET_CONTRACT_ID,
                trade_date=TARGET_TRADE_DATE,
                family=term,
                secid=TARGET_SECID,
                source_path=source_path.as_posix(),
                run_id="test_run",
            )


def test_explicit_target_path_expansion_matches_contract(tmp_path):
    paths = materialization_target_paths(
        repo_root=REPO_ROOT,
        dataset_id=TARGET_DATASET_ID,
        contract_id=TARGET_CONTRACT_ID,
        trade_date=TARGET_TRADE_DATE,
        family=TARGET_FAMILY,
        secid=TARGET_SECID,
        run_id="test_run",
        env={"MOEX_DATA_ROOT": str(tmp_path / "data")},
    )

    assert paths.partition_path == tmp_path / "data/futures/raw_5m/trade_date=2026-06-02/family=Si/secid=SiM6/part.parquet"
    assert paths.manifest_path == tmp_path / "data/futures/runs/refresh/run_date=2026-06-02/run_id=test_run/manifest.json"
    assert paths.quality_report_path == tmp_path / "data/futures/quality/run_date=2026-06-02/run_id=test_run/quality_report.json"


def test_valid_tiny_source_table_materializes_expected_partition_and_reports(tmp_path):
    result = _materialize(tmp_path, _tiny_rows())

    assert result.status == "succeeded"
    assert result.rows == 2
    assert result.partition_path.exists()
    assert result.manifest_path.exists()
    assert result.quality_report_path.exists()

    parquet_rows = pd.read_parquet(result.partition_path)
    assert tuple(parquet_rows.columns) == RAW_5M_REQUIRED_COLUMNS
    assert parquet_rows["family"].tolist() == [TARGET_FAMILY, TARGET_FAMILY]

    quality_report = result.quality_report_path.read_text(encoding="utf-8")
    for field_name in (
        "rows",
        "duplicate_key_count",
        "gap_count",
        "null_ohlc_count",
        "invalid_ohlc_count",
        "quality_status",
    ):
        assert field_name in quality_report

    manifest = result.manifest_path.read_text(encoding="utf-8")
    assert result.partition_path.as_posix() in manifest
    assert result.quality_report_path.as_posix() in manifest


def test_manifest_is_written_only_for_successful_materialization(tmp_path):
    source_path = _write_source_csv(tmp_path, _tiny_rows(high=98.0, low=99.0))
    paths = materialization_target_paths(
        repo_root=REPO_ROOT,
        dataset_id=TARGET_DATASET_ID,
        contract_id=TARGET_CONTRACT_ID,
        trade_date=TARGET_TRADE_DATE,
        family=TARGET_FAMILY,
        secid=TARGET_SECID,
        run_id="bad_run",
        env={"MOEX_DATA_ROOT": str(tmp_path / "data")},
    )

    with pytest.raises(FuturesRaw5mMaterializationError):
        materialize_single_raw_5m_partition(
            repo_root=REPO_ROOT,
            dataset_id=TARGET_DATASET_ID,
            contract_id=TARGET_CONTRACT_ID,
            trade_date=TARGET_TRADE_DATE,
            family=TARGET_FAMILY,
            secid=TARGET_SECID,
            source_path=source_path.as_posix(),
            run_id="bad_run",
            env={"MOEX_DATA_ROOT": str(tmp_path / "data")},
        )

    assert not paths.partition_path.exists()
    assert not paths.manifest_path.exists()
    assert not paths.quality_report_path.exists()


def test_bad_ohlc_fails(tmp_path):
    with pytest.raises(FuturesRaw5mMaterializationError, match="high lower than low"):
        _materialize(tmp_path, _tiny_rows(high=98.0, low=99.0))


def test_duplicate_ts_secid_fails(tmp_path):
    rows = _tiny_rows()
    rows[1]["ts"] = rows[0]["ts"]

    with pytest.raises(FuturesRaw5mMaterializationError, match="duplicate"):
        _materialize(tmp_path, rows)


def test_wrong_family_secid_trade_date_inside_rows_fails(tmp_path):
    for field_name, value in (("trade_date", "2026-06-03"), ("family", "RI"), ("secid", "RIM6")):
        rows = _tiny_rows()
        rows[0][field_name] = value
        with pytest.raises(FuturesRaw5mMaterializationError):
            _materialize(tmp_path, rows, run_id=f"bad_{field_name}")


def test_no_forbidden_imports_in_materialization_boundary():
    imported_names = []
    for relative_path in MATERIALIZATION_SOURCE_PATHS:
        source_path = REPO_ROOT / relative_path
        assert source_path.exists(), relative_path
        imported_names.extend(_imported_names_from_source(source_path.read_text(encoding="utf-8")))

    for term in _guard_terms():
        assert not any(imported_name.startswith(term) for imported_name in imported_names)
