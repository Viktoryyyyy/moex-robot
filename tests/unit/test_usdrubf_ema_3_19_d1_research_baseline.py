from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.moex_research.runners import usdrubf_ema_3_19_d1_research_baseline as runner


def _write_source_dataset(path: Path) -> None:
    closes: list[float] = []
    for _ in range(4):
        closes.extend(float(value) for value in pd.Series(range(25)).map(lambda i: 100.0 - (15.0 * i / 24.0)))
        closes.extend(float(value) for value in pd.Series(range(25)).map(lambda i: 85.0 + (40.0 * i / 24.0)))

    rows = []
    for index, close in enumerate(closes):
        end = pd.Timestamp("2024-01-01 18:50:00") + pd.Timedelta(days=index)
        rows.append(
            {
                "instrument_id": "usdrubf",
                "end": end.isoformat(),
                "open": close,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1000.0 + float(index),
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def _run(tmp_path: Path) -> Path:
    source = tmp_path / "source.csv"
    output_dir = tmp_path / "out"
    _write_source_dataset(source)
    assert runner.main(
        [
            "--source-dataset-path",
            str(source),
            "--output-dir",
            str(output_dir),
            "--run-id",
            "unit-test-run",
        ]
    ) == 0
    return output_dir


def test_required_cli_argument_validation(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    _write_source_dataset(source)

    with pytest.raises(SystemExit):
        runner.main(["--output-dir", str(tmp_path / "out"), "--run-id", "x"])

    with pytest.raises(SystemExit):
        runner.main(["--source-dataset-path", str(source), "--run-id", "x"])

    with pytest.raises(SystemExit):
        runner.main(["--source-dataset-path", str(source), "--output-dir", str(tmp_path / "out")])


def test_missing_source_dataset_rejection(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        runner.main(
            [
                "--source-dataset-path",
                str(tmp_path / "missing.csv"),
                "--output-dir",
                str(tmp_path / "out"),
                "--run-id",
                "missing-source",
            ]
        )


def test_output_dir_only_artifact_writes_and_declared_output_filenames(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    output_dir = tmp_path / "out"
    _write_source_dataset(source)

    runner.main(
        [
            "--source-dataset-path",
            str(source),
            "--output-dir",
            str(output_dir),
            "--run-id",
            "declared-files",
        ]
    )

    assert sorted(path.name for path in output_dir.iterdir()) == sorted(runner.DECLARED_OUTPUT_FILES)
    assert sorted(path.name for path in tmp_path.iterdir()) == ["out", "source.csv"]


def test_raw_baseline_summary_fields(tmp_path: Path) -> None:
    output_dir = _run(tmp_path)
    summary = pd.read_csv(output_dir / runner.OUTPUT_RAW_BASELINE_SUMMARY)

    required_columns = {
        "experiment_id",
        "group_type",
        "group_value",
        "row_count",
        "event_count",
        "cross_up_count",
        "cross_down_count",
        "signed_ret_o2o_h1_count",
        "signed_ret_o2o_h1_mean",
        "signed_ret_o2o_h2_count",
        "signed_ret_o2o_h2_mean",
        "signed_ret_o2o_h5_count",
        "signed_ret_o2o_h5_mean",
        "allow_trade_h5_count",
        "allow_trade_h5_rate",
        "max_adverse_excursion_h5_count",
        "max_adverse_excursion_h5_mean",
    }
    assert required_columns.issubset(set(summary.columns))
    assert {"total", "cross_dir", "year"}.issubset(set(summary["group_type"]))
    total = summary.loc[summary["group_type"] == "total"].iloc[0]
    assert int(total["event_count"]) > 0
    assert int(total["cross_up_count"]) > 0
    assert int(total["cross_down_count"]) > 0


def test_feature_context_vs_labels_separation(tmp_path: Path) -> None:
    output_dir = _run(tmp_path)

    context = pd.read_csv(output_dir / runner.OUTPUT_CROSS_CONTEXT)
    labels = pd.read_csv(output_dir / runner.OUTPUT_CROSS_LABELS)
    quality = json.loads((output_dir / runner.OUTPUT_QUALITY_REPORT).read_text(encoding="utf-8"))

    label_like_columns = [
        column
        for column in context.columns
        if column.startswith(("signed_ret_", "allow_trade_", "max_adverse_", "max_favorable_"))
    ]
    assert label_like_columns == []
    assert {"signed_ret_o2o_h1", "signed_ret_o2o_h2", "signed_ret_o2o_h5", "allow_trade_h5"}.issubset(labels.columns)

    feature_files = {
        item["filename"]
        for item in quality["artifact_groups"]["feature_context_artifacts"]
    }
    label_files = {
        item["filename"]
        for item in quality["artifact_groups"]["label_artifacts"]
    }
    assert runner.OUTPUT_CROSS_CONTEXT in feature_files
    assert runner.OUTPUT_CROSS_LABELS not in feature_files
    assert runner.OUTPUT_CROSS_LABELS in label_files
    assert quality["artifact_groups"]["label_artifacts"][0]["research_only"] is True


def test_no_d_plus_1_values_in_feature_context_rows(tmp_path: Path) -> None:
    output_dir = _run(tmp_path)

    d1 = pd.read_csv(output_dir / runner.OUTPUT_D1_OHLC)
    context = pd.read_csv(output_dir / runner.OUTPUT_CROSS_CONTEXT)
    quality = json.loads((output_dir / runner.OUTPUT_QUALITY_REPORT).read_text(encoding="utf-8"))

    for frame in (d1, context):
        lowered_columns = [str(column).lower() for column in frame.columns]
        assert not any("d+1" in column or "d_plus_1" in column for column in lowered_columns)
        assert set(frame["known_by_when"]) == {runner.D_CLOSE_KNOWN_BY_WHEN}

    context_text = context.to_csv(index=False).lower()
    assert "d+1 open" not in context_text
    assert "signed_ret_o2o" not in context_text
    assert quality["time_semantics"]["earliest_label_outcome_anchor"] == runner.EARLIEST_LABEL_OUTCOME_ANCHOR
    assert quality["time_semantics"]["feature_context_uses_d_plus_1_values"] is False
    assert quality["leakage_checks"]["no_d_plus_1_values_in_feature_context_rows"] is True
