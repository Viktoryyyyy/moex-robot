from __future__ import annotations

import argparse
import ast
import json
import math
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moex_research.runners import usdrubf_phase6_internal_modeling_dataset_builder as builder  # noqa: E402


FORBIDDEN_FEATURE_COLUMNS = {
    "phase_label",
    "B",
    "S",
    "OUT",
    "target",
    "y",
    "future_return",
    "source_interval_id",
    "phase_remaining_sessions",
    "next_regime_if_current_ends",
    "transition_exit_day",
    "boundary_distance",
    "future_phase",
    "future_volatility",
    "label_annotation",
    "annotator",
    "label_availability_ts",
}


def _panel_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": ["2026-06-10", "2026-06-11", "2026-06-15"],
            "instrument_id": ["forts.usdrubf", "forts.usdrubf", "forts.usdrubf"],
            "secid": ["USDRUBF", "USDRUBF", "USDRUBF"],
            "open": [100.0, 104.0, 106.0],
            "high": [105.0, 108.0, 111.0],
            "low": [99.0, 103.0, 105.0],
            "close": [104.0, 106.0, 110.0],
            "volume": [10.0, 20.0, 30.0],
            "value": [1000.0, 2200.0, 3300.0],
            "num_trades": [2.0, 3.0, 4.0],
        }
    )


def _multi_instrument_panel_frame() -> pd.DataFrame:
    second_instrument = _panel_frame().assign(
        instrument_id="forts.si",
        secid="Si",
        open=[1000.0, 2000.0, 3000.0],
        high=[1010.0, 2010.0, 3010.0],
        low=[990.0, 1990.0, 2990.0],
        close=[1005.0, 2005.0, 3005.0],
        volume=[1000.0, 2000.0, 3000.0],
        value=[100000.0, 200000.0, 300000.0],
        num_trades=[100.0, 200.0, 300.0],
    )
    return pd.concat([_panel_frame(), second_instrument], ignore_index=True)


def _label_contract() -> dict[str, object]:
    return {
        "contract_id": "usdrubf_d1_manual_phase_labels.v1",
        "allowed_labels": ["B", "S", "OUT"],
        "provenance": {"manual_hypothesis_label": True},
    }


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    panel_path = tmp_path / "panel.parquet"
    panel_manifest_path = tmp_path / "panel_manifest.json"
    label_contract_path = tmp_path / "manual_label_contract.json"

    _panel_frame().to_parquet(panel_path, index=False)
    panel_manifest_path.write_text(
        json.dumps({"run_id": "phase3_4_full_historical_d1_panel_v1"}) + "\n",
        encoding="utf-8",
    )
    label_contract_path.write_text(json.dumps(_label_contract()) + "\n", encoding="utf-8")
    return panel_path, panel_manifest_path, label_contract_path


def _request(tmp_path: Path, *, output_dir: Path | None = None) -> builder.Phase6DatasetBuildRequest:
    panel_path, panel_manifest_path, label_contract_path = _write_inputs(tmp_path)
    return builder.Phase6DatasetBuildRequest(
        panel_path=panel_path,
        panel_manifest_path=panel_manifest_path,
        label_contract_path=label_contract_path,
        output_dir=output_dir or (tmp_path / "out"),
        run_id="phase6_test_run",
        internal_d1_only=True,
        no_external_data=True,
        no_model_fitting=True,
        no_prediction=True,
        no_trading=True,
        no_overwrite=True,
    )


def test_dataset_contract_declares_internal_only_and_no_hardcoded_server_path() -> None:
    contract_path = ROOT / "contracts/datasets/usdrubf_phase6_internal_modeling_dataset.v1.yaml"
    text = contract_path.read_text(encoding="utf-8")

    assert "usdrubf_phase6_internal_modeling_dataset.v1" in text
    assert "internal_d1_only: true" in text
    assert "external_data_allowed: false" in text
    assert "model_fitting_allowed: false" in text
    assert "prediction_allowed: false" in text
    assert "trading_allowed: false" in text
    assert "/home/trader" not in text
    assert "moex_robot" not in text


def test_feature_contract_defines_required_batches_and_forbidden_columns() -> None:
    contract_path = ROOT / "contracts/features/usdrubf_phase6_internal_factor_batches_v1.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))

    required_batches = {
        "internal_price_return",
        "internal_volatility_range",
        "internal_volume_liquidity",
        "ema_3_19_baseline_context",
        "session_index_context",
    }
    assert set(contract["feature_batches"]) == required_batches
    assert "target_phase_label" in contract["target_columns"]
    assert "lag1_close_return_1d" in contract["feature_batches"]["internal_price_return"]["columns"]
    assert "lag1_hl_range_pct" in contract["feature_batches"]["internal_volatility_range"]["columns"]
    assert "lag1_volume" in contract["feature_batches"]["internal_volume_liquidity"]["columns"]
    assert "lag1_ema_3_19_state" in contract["feature_batches"]["ema_3_19_baseline_context"]["columns"]
    assert "target_trade_date" in contract["feature_batches"]["session_index_context"]["columns"]
    assert FORBIDDEN_FEATURE_COLUMNS.issubset(set(contract["forbidden_feature_columns"]))


def test_builder_cli_parser_requires_safety_flags_and_no_overwrite(tmp_path: Path) -> None:
    parser = builder.build_argument_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--panel-path",
                "/tmp/panel.parquet",
                "--panel-manifest-path",
                "/tmp/manifest.json",
                "--label-contract-path",
                "/tmp/labels.json",
                "--output-dir",
                "/tmp/out",
                "--run-id",
                "missing_no_overwrite",
                "--internal-d1-only",
                "--no-external-data",
                "--no-model-fitting",
                "--no-prediction",
                "--no-trading",
            ]
        )

    args = parser.parse_args(
        [
            "--panel-path",
            str(tmp_path / "panel.parquet"),
            "--panel-manifest-path",
            str(tmp_path / "manifest.json"),
            "--label-contract-path",
            str(tmp_path / "labels.json"),
            "--output-dir",
            str(tmp_path / "out"),
            "--run-id",
            "missing_safety_flags",
            "--no-overwrite",
        ]
    )
    assert isinstance(args, argparse.Namespace)
    with pytest.raises(builder.Phase6DatasetBuilderError, match="Missing required safety gate"):
        builder.build_dataset_from_args(args)


def test_builder_writes_only_approved_artifacts_and_manifest_side_effects(tmp_path: Path) -> None:
    result = builder.build_modeling_dataset(_request(tmp_path))

    expected_files = sorted(builder.REQUIRED_OUTPUT_ARTIFACTS)
    output_files = sorted(path.name for path in result.output_dir.iterdir() if path.is_file())
    assert output_files == expected_files

    dataset = pd.read_parquet(result.output_dir / "modeling_dataset.parquet")
    assert list(dataset.columns) == [*builder.TARGET_COLUMNS, *builder.FEATURE_COLUMNS]
    assert result.row_count == 3
    assert result.labeled_row_count == 3
    assert dataset["target_source"].unique().tolist() == ["manual_phase_labels_v1"]

    manifest = json.loads((result.output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["output_artifacts"] == list(builder.REQUIRED_OUTPUT_ARTIFACTS)
    assert manifest["side_effect_summary"]["network_or_provider_api_calls"] is False
    assert manifest["side_effect_summary"]["external_data_ingestion"] is False
    assert manifest["side_effect_summary"]["model_fitting"] is False
    assert manifest["side_effect_summary"]["prediction"] is False
    assert manifest["side_effect_summary"]["trading_or_broker_actions"] is False


def test_builder_refuses_non_empty_output_dir_or_existing_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    (output_dir / "stale.txt").write_text("stale", encoding="utf-8")

    request = builder.Phase6DatasetBuildRequest(
        panel_path=tmp_path / "missing_panel.parquet",
        panel_manifest_path=tmp_path / "missing_manifest.json",
        label_contract_path=tmp_path / "missing_labels.json",
        output_dir=output_dir,
        run_id="non_empty_output_guard",
        internal_d1_only=True,
        no_external_data=True,
        no_model_fitting=True,
        no_prediction=True,
        no_trading=True,
        no_overwrite=True,
    )

    with pytest.raises(builder.Phase6DatasetBuilderError, match="non-empty"):
        builder.build_modeling_dataset(request)


def test_builder_refuses_forbidden_target_like_columns_in_input_panel(tmp_path: Path) -> None:
    panel = _panel_frame()
    panel["phase_label"] = ["B", "B", "B"]

    with pytest.raises(
        builder.Phase6DatasetBuilderError,
        match="input panel must not contain target-like",
    ):
        builder.build_modeling_dataset_frame(
            panel=panel,
            label_contract=_label_contract(),
        )


def test_builder_refuses_multi_instrument_panel_before_lag_windows_can_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_diagnostics_run(panel: pd.DataFrame) -> pd.DataFrame:
        raise AssertionError("diagnostic feature generation must not run")

    monkeypatch.setattr(builder, "_add_past_only_diagnostics", fail_if_diagnostics_run)

    with pytest.raises(
        builder.Phase6DatasetBuilderError,
        match="single instrument_id only.*cross-instrument lag/rolling/EWM leakage",
    ):
        builder.build_modeling_dataset_frame(
            panel=_multi_instrument_panel_frame(),
            label_contract=_label_contract(),
        )


def test_features_are_lagged_and_do_not_include_same_day_ohlcv_or_label_metadata(tmp_path: Path) -> None:
    result = builder.build_modeling_dataset(_request(tmp_path))
    dataset = pd.read_parquet(result.output_dir / "modeling_dataset.parquet")

    assert {"open", "high", "low", "close", "volume", "value", "num_trades"}.isdisjoint(
        set(dataset.columns)
    )
    assert FORBIDDEN_FEATURE_COLUMNS.isdisjoint(set(dataset.columns))

    assert math.isnan(float(dataset.loc[0, "lag1_intraday_return"]))
    assert math.isnan(float(dataset.loc[1, "lag1_close_return_1d"]))
    expected_previous_close_return = (106.0 - 104.0) / 104.0
    assert dataset.loc[2, "lag1_close_return_1d"] == pytest.approx(
        expected_previous_close_return
    )
    assert dataset.loc[2, "lag1_intraday_return"] == pytest.approx((106.0 - 104.0) / 104.0)
    assert dataset.loc[2, "lag1_volume"] == pytest.approx(20.0)
    assert dataset.loc[2, "prior_trade_date"] == "2026-06-11"
    assert dataset.loc[2, "days_since_prior_trade_date"] == 4


def test_builder_source_has_no_external_provider_model_prediction_or_trading_patterns() -> None:
    source_path = ROOT / "src/moex_research/runners/usdrubf_phase6_internal_modeling_dataset_builder.py"
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

    normalized_source = source.lower().replace("trading_or_broker_actions", "")
    assert forbidden_import_roots.isdisjoint(imported_roots)
    assert "broker" not in normalized_source
    assert "create_order" not in normalized_source
    assert "place_order" not in normalized_source
