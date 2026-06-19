from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.moex_features.daily.usdrubf_d1_ema_3_19_classical_indicators import INDICATOR_COLUMNS
from src.moex_features.labels.usdrubf_d1_ema_3_19_multi_horizon_labels import OUTPUT_COLUMNS
from src.moex_research.runners.usdrubf_ema_3_19_d1_indicators_horizons import (
    DECLARED_OUTPUT_FILES,
    _build_parser,
    _validate_cli_args,
    main,
)

ROOT = Path(__file__).resolve().parents[2]
APPROVED_FILES = (
    "src/moex_features/daily/usdrubf_d1_ema_3_19_classical_indicators.py",
    "src/moex_features/daily/usdrubf_d1_ema_3_19_indicator_context.py",
    "src/moex_features/labels/usdrubf_d1_ema_3_19_multi_horizon_labels.py",
    "src/moex_research/runners/usdrubf_ema_3_19_d1_indicators_horizons.py",
    "configs/features/usdrubf_d1_ema_3_19_classical_indicators.json",
    "configs/features/usdrubf_d1_ema_3_19_indicator_context.json",
    "contracts/features/usdrubf_d1_ema_3_19_classical_indicators.json",
    "contracts/features/usdrubf_d1_ema_3_19_indicator_context.json",
    "contracts/labels/usdrubf_d1_ema_3_19_multi_horizon_labels.json",
    "contracts/experiments/usdrubf_ema_3_19_d1_indicators_horizons_v1.json",
    "tests/unit/test_usdrubf_d1_ema_3_19_classical_indicators.py",
    "tests/unit/test_usdrubf_d1_ema_3_19_indicator_context.py",
    "tests/unit/test_usdrubf_d1_ema_3_19_multi_horizon_labels.py",
    "tests/contract/test_usdrubf_ema_3_19_d1_indicators_horizons_contract.py",
)
UNAPPROVED_GENERIC_FILES = (
    "src/moex_features/daily/usdrubf_d1_classical_indicators.py",
    "configs/features/usdrubf_d1_classical_indicators.json",
    "contracts/features/usdrubf_d1_classical_indicators.json",
    "tests/unit/test_usdrubf_d1_classical_indicators.py",
)


def _d1_frame(rows: int = 80) -> pd.DataFrame:
    values = []
    for index in range(rows):
        close = 100.0 + 0.2 * index + 4.0 * np.sin(index / 4.0)
        values.append(
            {
                "instrument_id": "usdrubf",
                "end": pd.Timestamp("2024-01-01 18:50:00") + pd.Timedelta(days=index),
                "open": close - 0.2,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1000.0 + index,
            }
        )
    return pd.DataFrame(values)


def _events(d1: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf",
                "end": d1.loc[20, "end"],
                "cross_dir": "cross_up",
                "ema3": 101.0,
                "ema19": 100.0,
                "ret_1d": 0.01,
            },
            {
                "instrument_id": "usdrubf",
                "end": d1.loc[35, "end"],
                "cross_dir": "cross_down",
                "ema3": 99.0,
                "ema19": 100.0,
                "ret_3d": -0.02,
            },
            {
                "instrument_id": "usdrubf",
                "end": d1.loc[55, "end"],
                "cross_dir": "cross_up",
                "ema3": 102.0,
                "ema19": 101.0,
                "allow_trade_h5": 1,
            },
        ]
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_exact_fourteen_approved_paths_are_present_and_generic_paths_are_absent() -> None:
    assert len(APPROVED_FILES) == 14
    assert all((ROOT / path).is_file() for path in APPROVED_FILES)
    assert all(not (ROOT / path).exists() for path in UNAPPROVED_GENERIC_FILES)


def test_feature_label_and_experiment_contracts_bind_exact_producers_and_columns() -> None:
    indicator_contract = json.loads(
        (ROOT / "contracts/features/usdrubf_d1_ema_3_19_classical_indicators.json").read_text(
            encoding="utf-8"
        )
    )
    context_contract = json.loads(
        (ROOT / "contracts/features/usdrubf_d1_ema_3_19_indicator_context.json").read_text(
            encoding="utf-8"
        )
    )
    label_contract = json.loads(
        (ROOT / "contracts/labels/usdrubf_d1_ema_3_19_multi_horizon_labels.json").read_text(
            encoding="utf-8"
        )
    )
    experiment_contract = json.loads(
        (ROOT / "contracts/experiments/usdrubf_ema_3_19_d1_indicators_horizons_v1.json").read_text(
            encoding="utf-8"
        )
    )

    assert indicator_contract["artifact_id"] == "usdrubf_d1_ema_3_19_classical_indicators"
    assert indicator_contract["producer_ref"] == (
        "src.moex_features.daily.usdrubf_d1_ema_3_19_classical_indicators:materialize_feature_frame"
    )
    assert indicator_contract["required_columns"] == [
        "instrument_id",
        "end",
        "session_index",
        *INDICATOR_COLUMNS,
        "indicator_ready",
    ]
    assert context_contract["source_indicator_ref"] == (
        "contracts/features/usdrubf_d1_ema_3_19_classical_indicators.json"
    )
    assert context_contract["join_keys"] == ["instrument_id", "end"]
    assert context_contract["join_cardinality"] == "one_to_one"
    assert context_contract["preserve_cross_dir"] is True
    assert label_contract["required_columns"] == list(OUTPUT_COLUMNS)
    assert label_contract["fixed_horizons"] == {
        "h1": "D+2 open",
        "h2": "D+3 open",
        "h3": "D+4 open",
        "h5": "D+6 open",
        "h10": "D+11 open",
    }
    assert experiment_contract["required_cli_args"] == [
        "--d1-ohlc-path",
        "--crossover-context-path",
        "--output-dir",
        "--run-id",
        "--git-commit-sha",
    ]
    assert [item["filename"] for item in experiment_contract["output_artifacts"]] == list(
        DECLARED_OUTPUT_FILES
    )
    assert experiment_contract["no_external_TA_dependency"] is True
    assert experiment_contract["no_model_training"] is True
    assert experiment_contract["no_runtime_consumption"] is True


def test_runner_contains_orchestration_not_indicator_or_label_formulas() -> None:
    source = (ROOT / "src/moex_research/runners/usdrubf_ema_3_19_d1_indicators_horizons.py").read_text(
        encoding="utf-8"
    )
    assert "usdrubf_d1_ema_3_19_classical_indicators" in source
    assert "build_classical_indicators_frame" in source
    assert "build_ema_3_19_indicator_context_frame" in source
    assert "build_multi_horizon_labels_frame" in source
    assert ".ewm(" not in source
    assert ".rolling(" not in source
    assert "def _rsi" not in source
    assert "def _signed_return" not in source


def test_cli_rejects_mutable_alias_glob_and_non_full_git_sha(tmp_path) -> None:
    d1 = tmp_path / "d1.csv"
    context = tmp_path / "context.csv"
    _d1_frame().to_csv(d1, index=False)
    _events(_d1_frame()).to_csv(context, index=False)
    parser = _build_parser()

    args = parser.parse_args(
        [
            "--d1-ohlc-path", str(d1),
            "--crossover-context-path", str(context),
            "--output-dir", str(tmp_path / "out"),
            "--run-id", "m4a-test",
            "--git-commit-sha", "abc123",
        ]
    )
    with pytest.raises(SystemExit):
        _validate_cli_args(args, parser)

    latest = tmp_path / "latest.csv"
    latest.write_text(d1.read_text(encoding="utf-8"), encoding="utf-8")
    args = parser.parse_args(
        [
            "--d1-ohlc-path", str(latest),
            "--crossover-context-path", str(context),
            "--output-dir", str(tmp_path / "out"),
            "--run-id", "m4a-test",
            "--git-commit-sha", "a" * 40,
        ]
    )
    with pytest.raises(SystemExit):
        _validate_cli_args(args, parser)

    args = parser.parse_args(
        [
            "--d1-ohlc-path", str(tmp_path / "*.csv"),
            "--crossover-context-path", str(context),
            "--output-dir", str(tmp_path / "out"),
            "--run-id", "m4a-test",
            "--git-commit-sha", "a" * 40,
        ]
    )
    with pytest.raises(SystemExit):
        _validate_cli_args(args, parser)


def test_runner_writes_exact_six_artifacts_with_input_hashes_and_quality_counts(tmp_path) -> None:
    d1_path = tmp_path / "d1.csv"
    context_path = tmp_path / "events.csv"
    output_dir = tmp_path / "output"
    d1 = _d1_frame()
    events = _events(d1)
    d1.to_csv(d1_path, index=False)
    events.to_csv(context_path, index=False)

    assert main(
        [
            "--d1-ohlc-path", str(d1_path),
            "--crossover-context-path", str(context_path),
            "--output-dir", str(output_dir),
            "--run-id", "m4a-contract-test",
            "--git-commit-sha", "b" * 40,
        ]
    ) == 0
    assert sorted(path.name for path in output_dir.iterdir()) == sorted(DECLARED_OUTPUT_FILES)

    metadata = json.loads((output_dir / "run_metadata.json").read_text(encoding="utf-8"))
    quality = json.loads(
        (output_dir / "usdrubf_ema_3_19_indicator_horizon_quality_report.json").read_text(
            encoding="utf-8"
        )
    )
    context_output = pd.read_csv(output_dir / "usdrubf_d1_ema_3_19_indicator_context.csv")
    labels = pd.read_csv(output_dir / "usdrubf_d1_ema_3_19_multi_horizon_labels.csv")

    assert metadata["git_commit_sha"] == "b" * 40
    assert metadata["inputs"]["d1_ohlc"]["sha256"] == _sha256(d1_path)
    assert metadata["inputs"]["crossover_context"]["sha256"] == _sha256(context_path)
    assert quality["input_artifacts"]["d1_ohlc"]["sha256"] == _sha256(d1_path)
    assert quality["input_artifacts"]["crossover_context"]["sha256"] == _sha256(context_path)
    assert quality["counts"]["d1_input_rows"] == len(d1)
    assert quality["counts"]["total_event_rows"] == len(events)
    assert quality["counts"]["cross_up_rows"] == 2
    assert quality["counts"]["cross_down_rows"] == 1
    assert "ret_1d" not in context_output.columns
    assert "ret_3d" not in context_output.columns
    assert "allow_trade_h5" not in context_output.columns
    assert list(labels.columns) == list(OUTPUT_COLUMNS)
    assert quality["model_training_performed"] is False
