from __future__ import annotations

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

from moex_research.runners import usdrubf_phase5_internal_manual_label_analysis as runner  # noqa: E402


REQUIRED_OUTPUTS = {
    "manifest.json",
    "analysis_report.md",
    "phase_summary.csv",
    "transition_counts.csv",
    "boundary_window_summary.csv",
    "joined_panel_preview.csv",
}


def _synthetic_panel_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": [
                "2025-09-09",
                "2025-09-10",
                "2025-09-11",
                "2025-09-12",
                "2025-09-15",
                "2025-09-24",
                "2025-09-25",
            ],
            "instrument_id": ["forts.usdrubf"] * 7,
            "secid": ["USDRUBF"] * 7,
            "open": [100.0, 101.0, 102.0, 101.5, 101.0, 99.0, 98.5],
            "high": [102.0, 103.0, 103.0, 102.0, 101.5, 100.0, 99.0],
            "low": [99.0, 100.0, 101.0, 100.0, 99.5, 97.5, 97.0],
            "close": [101.0, 102.0, 101.5, 101.0, 99.0, 98.5, 98.0],
            "volume": [10, 11, 12, 13, 14, 15, 16],
            "value": [1000.0, 1122.0, 1218.0, 1313.0, 1386.0, 1477.5, 1568.0],
            "num_trades": [1, 2, 2, 3, 3, 4, 4],
        }
    )


def _write_synthetic_panel(path: Path, extra_columns: dict[str, object] | None = None) -> None:
    frame = _synthetic_panel_frame()
    if extra_columns:
        for column, value in extra_columns.items():
            frame[column] = value
    frame.to_parquet(path, index=False)


def _write_synthetic_manifest(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "run_id": "phase3_4_full_exact_history",
                "row_count": 7,
                "input_partition_count": 7,
                "built_date_range": {
                    "min_trade_date": "2025-09-09",
                    "max_trade_date": "2025-09-25",
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_synthetic_label_contract(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "contract_id": "usdrubf_d1_manual_phase_labels_v1",
                "allowed_labels": ["B", "S", "OUT"],
                "provenance": {
                    "manual_hypothesis_label": True,
                    "gold_label_claim_allowed": False,
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_fixture_inputs(
    tmp_path: Path, *, extra_panel_columns: dict[str, object] | None = None
) -> tuple[Path, Path, Path]:
    panel_path = tmp_path / "panel.parquet"
    panel_manifest_path = tmp_path / "manifest.input.json"
    label_contract_path = tmp_path / "label_contract.json"

    _write_synthetic_panel(panel_path, extra_columns=extra_panel_columns)
    _write_synthetic_manifest(panel_manifest_path)
    _write_synthetic_label_contract(label_contract_path)

    return panel_path, panel_manifest_path, label_contract_path


def _runner_argv(
    *,
    panel_path: Path,
    panel_manifest_path: Path,
    label_contract_path: Path,
    output_dir: Path,
    run_id: str = "phase5_test_run",
) -> list[str]:
    return [
        "--panel-path",
        panel_path.as_posix(),
        "--panel-manifest-path",
        panel_manifest_path.as_posix(),
        "--label-contract-path",
        label_contract_path.as_posix(),
        "--output-dir",
        output_dir.as_posix(),
        "--run-id",
        run_id,
        "--internal-d1-only",
        "--no-external-data",
        "--no-model-fitting",
        "--no-prediction",
        "--no-trading",
    ]


def _run_fixture(tmp_path: Path, output_name: str = "out") -> Path:
    panel_path, panel_manifest_path, label_contract_path = _write_fixture_inputs(tmp_path)
    output_dir = tmp_path / output_name

    exit_code = runner.main(
        _runner_argv(
            panel_path=panel_path,
            panel_manifest_path=panel_manifest_path,
            label_contract_path=label_contract_path,
            output_dir=output_dir,
        )
    )

    assert exit_code == 0
    return output_dir


def test_cli_safety_gates_fail_before_reading_or_writing(tmp_path: Path) -> None:
    output_dir = tmp_path / "must_not_exist"
    parser = runner.build_argument_parser()
    args = parser.parse_args(
        [
            "--panel-path",
            (tmp_path / "missing_panel.parquet").as_posix(),
            "--panel-manifest-path",
            (tmp_path / "missing_manifest.json").as_posix(),
            "--label-contract-path",
            (tmp_path / "missing_contract.json").as_posix(),
            "--output-dir",
            output_dir.as_posix(),
            "--run-id",
            "missing_gate",
            "--internal-d1-only",
            "--no-external-data",
            "--no-model-fitting",
            "--no-prediction",
        ]
    )

    with pytest.raises(runner.Phase5RunnerError, match="--no-trading"):
        runner.run_analysis_from_args(args)

    assert not output_dir.exists()


def test_runner_rejects_target_like_input_panel_before_artifacts(tmp_path: Path) -> None:
    panel_path, panel_manifest_path, label_contract_path = _write_fixture_inputs(
        tmp_path, extra_panel_columns={"future_return": [0.0] * 7}
    )
    output_dir = tmp_path / "must_not_exist"

    with pytest.raises(runner.Phase5RunnerError, match="future_return"):
        runner.main(
            _runner_argv(
                panel_path=panel_path,
                panel_manifest_path=panel_manifest_path,
                label_contract_path=label_contract_path,
                output_dir=output_dir,
            )
        )

    assert not output_dir.exists()


def test_runner_rejects_phase_label_input_panel_before_artifacts(tmp_path: Path) -> None:
    panel_path, panel_manifest_path, label_contract_path = _write_fixture_inputs(
        tmp_path, extra_panel_columns={"phase_label": ["B"] * 7}
    )
    output_dir = tmp_path / "must_not_exist"

    with pytest.raises(runner.Phase5RunnerError, match="phase_label"):
        runner.main(
            _runner_argv(
                panel_path=panel_path,
                panel_manifest_path=panel_manifest_path,
                label_contract_path=label_contract_path,
                output_dir=output_dir,
            )
        )

    assert not output_dir.exists()


def test_runner_rejects_non_empty_output_directory_before_new_artifacts(tmp_path: Path) -> None:
    panel_path, panel_manifest_path, label_contract_path = _write_fixture_inputs(tmp_path)
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    stale_artifact = output_dir / "joined_panel.parquet"
    stale_artifact.write_text("stale", encoding="utf-8")

    with pytest.raises(runner.Phase5RunnerError, match="non-empty"):
        runner.main(
            _runner_argv(
                panel_path=panel_path,
                panel_manifest_path=panel_manifest_path,
                label_contract_path=label_contract_path,
                output_dir=output_dir,
            )
        )

    assert {path.name for path in output_dir.iterdir()} == {"joined_panel.parquet"}
    assert stale_artifact.read_text(encoding="utf-8") == "stale"
    assert REQUIRED_OUTPUTS.isdisjoint({path.name for path in output_dir.iterdir()})


def test_runner_produces_required_output_artifacts_only_by_default(tmp_path: Path) -> None:
    output_dir = _run_fixture(tmp_path)

    actual_outputs = {path.name for path in output_dir.iterdir() if path.is_file()}
    assert actual_outputs == REQUIRED_OUTPUTS
    assert not (output_dir / "joined_panel.parquet").exists()


def test_analysis_report_contains_manual_label_and_ema_baseline_statements(tmp_path: Path) -> None:
    output_dir = _run_fixture(tmp_path)
    report = (output_dir / "analysis_report.md").read_text(encoding="utf-8")

    assert "Manual labels are manual research labels and are not EMA-derived." in report
    assert "EMA 3/19 is baseline/context only, not label source." in report
    assert "EMA 3/19 baseline context is computed only from the internal D1 close series." in report
    assert "No external data ingestion, no model fitting, no prediction" in report


def test_outputs_have_stable_deterministic_content_structure(tmp_path: Path) -> None:
    first_output_dir = _run_fixture(tmp_path, "out_first")
    second_output_dir = _run_fixture(tmp_path, "out_second")

    for artifact_name in REQUIRED_OUTPUTS:
        first_bytes = (first_output_dir / artifact_name).read_bytes()
        second_bytes = (second_output_dir / artifact_name).read_bytes()
        assert first_bytes == second_bytes

    manifest = json.loads((first_output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["output_artifacts"] == list(runner.REQUIRED_OUTPUT_ARTIFACTS)
    assert manifest["optional_joined_panel_parquet_written"] is False
    assert manifest["side_effect_summary"] == {
        "external_data_ingestion": False,
        "model_fitting": False,
        "network_or_provider_api_calls": False,
        "prediction": False,
        "trading_or_broker_actions": False,
    }

    phase_summary = pd.read_csv(first_output_dir / "phase_summary.csv")
    assert {"B", "OUT", "S"}.issubset(set(phase_summary["phase"]))
    assert list(pd.read_csv(first_output_dir / "transition_counts.csv").columns) == [
        "from_phase",
        "to_phase",
        "transition_count",
    ]


def test_source_has_no_network_model_prediction_or_trading_implementation() -> None:
    source_path = ROOT / "src/moex_research/runners/usdrubf_phase5_internal_manual_label_analysis.py"
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    forbidden_import_roots = {
        "requests",
        "urllib",
        "socket",
        "subprocess",
        "sklearn",
        "statsmodels",
    }
    imported_roots: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call):
            function_name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            assert function_name not in {"fit", "predict", "order", "submit_order"}

    assert forbidden_import_roots.isdisjoint(imported_roots)
    assert "external_data_ingestion\": True" not in source
    assert "model_fitting\": True" not in source
    assert "prediction\": True" not in source
    assert "trading_or_broker_actions\": True" not in source
