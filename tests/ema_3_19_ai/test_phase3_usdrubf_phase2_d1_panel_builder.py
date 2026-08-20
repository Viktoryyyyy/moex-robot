from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moex_data.futures import usdrubf_phase2_d1_panel_builder as builder  # noqa: E402


APPROVED_OUTPUT_ROOT_PARTS = (
    "research",
    "ema_3_19_ai",
    "usdrubf_phase2_d1_panel.v1",
)
FORBIDDEN_LABEL_COLUMNS = {
    "phase_label",
    "B",
    "S",
    "OUT",
    "target",
    "y",
    "future_return",
}


def _raw_partition(data_root: Path, trade_date: str) -> Path:
    return (
        data_root
        / "forts"
        / "raw_5m"
        / "tradestats"
        / f"trade_date={trade_date}"
        / "instrument_id=forts.usdrubf"
        / "secid=USDRUBF"
        / "part.parquet"
    )


def _write_raw_partition(data_root: Path, trade_date: str) -> Path:
    path = _raw_partition(data_root, trade_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "instrument_id": ["forts.usdrubf", "forts.usdrubf"],
            "trade_date": [trade_date, trade_date],
            "ts": [f"{trade_date} 10:00:00", f"{trade_date} 18:50:00"],
            "secid": ["USDRUBF", "USDRUBF"],
            "open": [100.0, 101.0],
            "high": [102.0, 104.0],
            "low": [99.0, 100.0],
            "close": [101.0, 103.0],
            "volume": [10, 20],
            "value": [1000.0, 2060.0],
            "num_trades": [1, 2],
            "phase_label": ["B", "S"],
            "future_return": [0.1, -0.2],
        }
    )
    frame.to_parquet(path, index=False)
    return path


def _request(
    data_root: Path,
    *,
    run_id: str = "test_run",
    start_date: str = "2026-06-11",
    end_date: str = "2026-06-11",
) -> builder.D1PanelBuildRequest:
    return builder.D1PanelBuildRequest(
        data_root=data_root,
        instrument_id="forts.usdrubf",
        secid="USDRUBF",
        start_date=builder._parse_iso_date(start_date, "start_date"),
        end_date=builder._parse_iso_date(end_date, "end_date"),
        run_id=run_id,
        no_overwrite=True,
    )


def test_contract_uses_moex_data_root_and_no_hardcoded_server_path() -> None:
    contract_path = ROOT / "contracts/datasets/usdrubf_phase2_d1_panel.v1.yaml"
    text = contract_path.read_text(encoding="utf-8")

    assert "${MOEX_DATA_ROOT}" in text
    assert "/home/trader" not in text
    assert "usdrubf_phase2_d1_panel.v1" in text
    assert "hardcoded_server_path_allowed: false" in text


def test_contract_marks_legacy_builder_compatibility_and_references_canonical_raw_5m() -> None:
    contract_path = ROOT / "contracts/datasets/usdrubf_phase2_d1_panel.v1.yaml"
    raw_contract_path = ROOT / "contracts/datasets/futures_raw_5m.v1.yaml"
    source_contract_path = ROOT / "contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml"

    contract = contract_path.read_text(encoding="utf-8")
    raw_contract = raw_contract_path.read_text(encoding="utf-8")
    source_contract = source_contract_path.read_text(encoding="utf-8")

    assert "status: legacy_research_compatibility" in contract
    assert "ingestion_source_of_truth: false" in contract
    assert "dataset_id: futures_raw_5m" in contract
    assert "contract_path: contracts/datasets/futures_raw_5m.v1.yaml" in contract
    assert "source_contract_path: contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml" in contract
    assert "implementation_compatibility_only_not_architecture" in contract
    assert "contract_id: futures_raw_5m.v1" in raw_contract
    assert "source_id: moex_algopack_fo_tradestats_5m" in source_contract


def test_builder_cli_parser_requires_explicit_date_range_and_no_overwrite() -> None:
    parser = builder.build_argument_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--data-root",
                "/tmp/data",
                "--instrument-id",
                "forts.usdrubf",
                "--secid",
                "USDRUBF",
                "--run-id",
                "missing_dates",
                "--no-overwrite",
            ]
        )

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--data-root",
                "/tmp/data",
                "--instrument-id",
                "forts.usdrubf",
                "--secid",
                "USDRUBF",
                "--start-date",
                "2026-06-11",
                "--end-date",
                "2026-06-11",
                "--run-id",
                "missing_no_overwrite",
            ]
        )

    args = parser.parse_args(
        [
            "--data-root",
            "/tmp/data",
            "--instrument-id",
            "forts.usdrubf",
            "--secid",
            "USDRUBF",
            "--start-date",
            "2026-06-11",
            "--end-date",
            "2026-06-11",
            "--run-id",
            "ok",
            "--no-overwrite",
        ]
    )
    assert isinstance(args, argparse.Namespace)
    assert args.no_overwrite is True


def test_builder_writes_only_panel_and_manifest_under_approved_target_root(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    input_path = _write_raw_partition(data_root, "2026-06-11")

    result = builder.build_panel(_request(data_root))

    expected_root = data_root.joinpath(*APPROVED_OUTPUT_ROOT_PARTS).resolve()
    assert result.output_path.resolve().relative_to(expected_root)
    assert result.manifest_path.resolve().relative_to(expected_root)
    assert result.output_path.name == "part.parquet"
    assert result.manifest_path.name == "manifest.json"

    output_files = sorted(path for path in expected_root.rglob("*") if path.is_file())
    assert output_files == sorted([result.output_path, result.manifest_path])
    assert input_path.exists()

    panel = pd.read_parquet(result.output_path)
    assert len(panel.index) == 1
    assert panel.loc[0, "trade_date"] == "2026-06-11"
    assert panel.loc[0, "instrument_id"] == "forts.usdrubf"
    assert panel.loc[0, "secid"] == "USDRUBF"
    assert panel.loc[0, "open"] == 100.0
    assert panel.loc[0, "high"] == 104.0
    assert panel.loc[0, "low"] == 99.0
    assert panel.loc[0, "close"] == 103.0
    assert panel.loc[0, "source_raw_5m_partition_count"] == 1
    assert panel.loc[0, "panel_schema_version"] == "usdrubf_phase2_d1_panel.v1"
    assert panel.loc[0, "build_run_id"] == "test_run"

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["row_count"] == 1
    assert manifest["input_partition_count"] == 1
    assert manifest["input_partitions"] == [input_path.resolve().as_posix()]
    assert manifest["side_effect_summary"]["writes"] == [
        result.output_path.as_posix(),
        result.manifest_path.as_posix(),
    ]
    assert manifest["side_effect_summary"]["network_calls"] is False


def test_builder_counts_source_partitions_per_d1_row_for_five_date_panel(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    trade_dates = [
        "2026-06-11",
        "2026-06-15",
        "2026-06-16",
        "2026-06-17",
        "2026-06-18",
    ]
    for trade_date in trade_dates:
        _write_raw_partition(data_root, trade_date)

    result = builder.build_panel(
        _request(
            data_root,
            run_id="five_date_panel",
            start_date="2026-06-11",
            end_date="2026-06-18",
        )
    )

    panel = pd.read_parquet(result.output_path)
    assert panel["trade_date"].tolist() == trade_dates
    assert panel["source_raw_5m_partition_count"].tolist() == [1, 1, 1, 1, 1]

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["row_count"] == 5
    assert manifest["input_partition_count"] == 5


def test_builder_refuses_overwrite_when_target_exists(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_raw_partition(data_root, "2026-06-11")

    request = _request(data_root, run_id="overwrite_guard")
    builder.build_panel(request)

    with pytest.raises(builder.D1PanelBuilderError, match="target output exists"):
        builder.build_panel(request)


def test_builder_fails_closed_on_missing_raw_5m_partition(tmp_path: Path) -> None:
    request = _request(tmp_path / "data", run_id="missing_raw")

    with pytest.raises(builder.D1PanelBuilderError, match="no raw 5m partitions"):
        builder.build_panel(request)


def test_builder_output_excludes_label_and_target_columns(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_raw_partition(data_root, "2026-06-11")

    result = builder.build_panel(_request(data_root, run_id="label_guard"))
    panel = pd.read_parquet(result.output_path)

    assert FORBIDDEN_LABEL_COLUMNS.isdisjoint(set(panel.columns))


def test_builder_source_has_no_network_subprocess_model_prediction_or_trading_side_effects() -> None:
    source_path = ROOT / "src/moex_data/futures/usdrubf_phase2_d1_panel_builder.py"
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    forbidden_import_roots = {"requests", "urllib", "subprocess", "socket"}
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call):
            function_name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            assert function_name not in {"fit", "predict"}

    assert forbidden_import_roots.isdisjoint(imported_roots)
    assert "broker" not in source.lower().replace("trading_or_broker_actions", "")
