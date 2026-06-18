from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.moex_research.runners import usdrubf_ema_3_19_d1_logistic_screen as runner


def _synthetic_frames(event_count: int = 64) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    session_ends = pd.date_range("2023-01-02 18:50:00", periods=120, freq="D")
    d1 = pd.DataFrame(
        {
            "instrument_id": "usdrubf",
            "end": session_ends,
            "open": np.linspace(70.0, 95.0, len(session_ends)),
            "high": np.linspace(70.5, 95.5, len(session_ends)),
            "low": np.linspace(69.5, 94.5, len(session_ends)),
            "close": np.linspace(70.1, 95.1, len(session_ends)),
            "volume": np.arange(len(session_ends)) + 1000,
        }
    )

    rows: list[dict[str, object]] = []
    label_rows: list[dict[str, object]] = []
    for event_index in range(event_count):
        session_index = 20 + event_index
        cross_dir = "cross_up" if event_index % 2 == 0 else "cross_down"
        sign = 1.0 if cross_dir == "cross_up" else -1.0
        h5 = 0.012 if event_index % 3 else -0.009
        if event_index == event_count - 1:
            h5 = np.nan
        h1 = 0.006 if event_index % 4 in {1, 2} else -0.004
        rows.append(
            {
                "instrument_id": "usdrubf",
                "end": session_ends[session_index],
                "open": 80.0 + event_index / 10.0,
                "high": 80.5 + event_index / 10.0,
                "low": 79.5 + event_index / 10.0,
                "close": 80.1 + event_index / 10.0,
                "volume": 2000 + event_index,
                "ema3": 80.0 + sign * 0.4,
                "ema19": 80.0,
                "ema_diff": sign * (0.1 + event_index / 1000.0),
                "ema_diff_prev": -sign * (0.05 + event_index / 2000.0),
                "cross_dir": cross_dir,
                "bars_since_prev_cross": np.nan if event_index == 0 else 1 + event_index % 7,
                "ret_1d": sign * (0.001 + event_index / 100000.0),
                "ret_3d": sign * (0.002 + event_index / 90000.0),
                "ret_5d": sign * (0.003 + event_index / 80000.0),
                "rolling_vol_5d": 0.01 + event_index / 100000.0,
                "rolling_vol_20d": 0.02 + event_index / 100000.0,
                "known_by_when": "D close after finalized D1 bar",
            }
        )
        label_rows.append(
            {
                "instrument_id": "usdrubf",
                "end": session_ends[session_index],
                "cross_dir": cross_dir,
                "signed_ret_o2o_h1": h1,
                "signed_ret_o2o_h2": h1 * 1.2,
                "signed_ret_o2o_h5": h5,
                "allow_trade_h5": int(not pd.isna(h5) and h5 > 0.0),
                "max_adverse_excursion_h5": -0.02,
                "max_favorable_excursion_h5": 0.03,
            }
        )
    return d1, pd.DataFrame(rows), pd.DataFrame(label_rows)


def _write_inputs(
    root: Path,
    *,
    event_count: int = 64,
    d1: pd.DataFrame | None = None,
    context: pd.DataFrame | None = None,
    labels: pd.DataFrame | None = None,
) -> tuple[Path, Path, Path]:
    generated_d1, generated_context, generated_labels = _synthetic_frames(event_count)
    d1 = generated_d1 if d1 is None else d1
    context = generated_context if context is None else context
    labels = generated_labels if labels is None else labels
    root.mkdir(parents=True, exist_ok=True)
    d1_path = root / "d1.csv"
    context_path = root / "context.csv"
    labels_path = root / "labels.csv"
    d1.to_csv(d1_path, index=False)
    context.to_csv(context_path, index=False)
    labels.to_csv(labels_path, index=False)
    return d1_path, context_path, labels_path


def _cli_args(paths: tuple[Path, Path, Path], output_dir: Path, run_id: str = "synthetic-m3") -> list[str]:
    d1_path, context_path, labels_path = paths
    return [
        "--d1-ohlc-path",
        str(d1_path),
        "--context-path",
        str(context_path),
        "--labels-path",
        str(labels_path),
        "--output-dir",
        str(output_dir),
        "--run-id",
        run_id,
    ]


def _prepared(paths: tuple[Path, Path, Path]) -> dict[str, pd.DataFrame]:
    _, _, _, targets = runner.load_and_prepare_inputs(
        d1_ohlc_path=paths[0], context_path=paths[1], labels_path=paths[2]
    )
    return targets


def test_explicit_input_paths_create_exact_declared_outputs_and_hash_lineage(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path / "inputs")
    output_dir = tmp_path / "out"

    assert runner.main(_cli_args(paths, output_dir)) == 0
    assert sorted(path.name for path in output_dir.iterdir()) == sorted(runner.DECLARED_OUTPUT_FILES)
    assert not (output_dir / "model.pkl").exists()

    metadata = json.loads((output_dir / runner.OUTPUT_RUN_METADATA).read_text(encoding="utf-8"))
    assert metadata["result_status"] == "provisional_screening"
    assert metadata["no_model_artifact"] is True
    expected_hashes = {
        role: hashlib.sha256(path.read_bytes()).hexdigest()
        for role, path in zip(("d1_ohlc", "context", "labels"), paths, strict=True)
    }
    assert {item["artifact_role"]: item["sha256"] for item in metadata["input_artifacts"]} == expected_hashes


@pytest.mark.parametrize(
    ("argument_name", "path_index"),
    [
        ("--d1-ohlc-path", 0),
        ("--context-path", 1),
        ("--labels-path", 2),
    ],
)
@pytest.mark.parametrize("alias", ["LaTeSt", "CURRENT", "AutoDetect"])
def test_mutable_input_aliases_rejected_before_output_writes(
    tmp_path: Path,
    argument_name: str,
    path_index: int,
    alias: str,
) -> None:
    paths = list(_write_inputs(tmp_path / "inputs"))
    alias_dir = tmp_path / alias
    alias_dir.mkdir()
    alias_path = alias_dir / paths[path_index].name
    shutil.copyfile(paths[path_index], alias_path)
    paths[path_index] = alias_path
    output_dir = tmp_path / "out"

    with pytest.raises(SystemExit):
        runner.main(_cli_args(tuple(paths), output_dir))
    assert not output_dir.exists()


@pytest.mark.parametrize("artifact", ["context", "labels"])
def test_duplicate_event_keys_rejected(tmp_path: Path, artifact: str) -> None:
    d1, context, labels = _synthetic_frames()
    if artifact == "context":
        context = pd.concat([context, context.iloc[[0]]], ignore_index=True)
    else:
        labels = pd.concat([labels, labels.iloc[[0]]], ignore_index=True)
    paths = _write_inputs(tmp_path / "inputs", d1=d1, context=context, labels=labels)

    with pytest.raises(ValueError, match="duplicate event keys"):
        _prepared(paths)


def test_unmatched_context_label_keys_rejected(tmp_path: Path) -> None:
    d1, context, labels = _synthetic_frames()
    labels = labels.iloc[:-1].copy()
    paths = _write_inputs(tmp_path / "inputs", d1=d1, context=context, labels=labels)

    with pytest.raises(ValueError, match="do not match one-to-one"):
        _prepared(paths)


@pytest.mark.parametrize("column", ["mystery_numeric_feature", "signed_ret_o2o_h5", "year"])
def test_exact_feature_allowlist_and_future_or_label_like_rejection(tmp_path: Path, column: str) -> None:
    d1, context, labels = _synthetic_frames()
    context[column] = 1.0
    paths = _write_inputs(tmp_path / "inputs", d1=d1, context=context, labels=labels)

    with pytest.raises(ValueError):
        _prepared(paths)


def test_missing_h5_is_excluded_not_negative_and_stored_h5_consistency_is_asserted(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path / "inputs")
    targets = _prepared(paths)
    assert len(targets[runner.PRIMARY_TARGET]) == 63
    assert len(targets[runner.SECONDARY_TARGET]) == 64

    d1, context, labels = _synthetic_frames()
    labels.loc[0, "allow_trade_h5"] = 1 - int(labels.loc[0, "allow_trade_h5"])
    mismatch_paths = _write_inputs(
        tmp_path / "mismatch", d1=d1, context=context, labels=labels
    )
    with pytest.raises(ValueError, match="inconsistent"):
        _prepared(mismatch_paths)


def test_walk_forward_folds_are_chronological_and_enforce_six_session_purge(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path / "inputs")
    frame = _prepared(paths)[runner.PRIMARY_TARGET]
    folds = runner.build_walk_forward_folds(frame)

    assert folds
    for fold in folds:
        train = frame.iloc[list(fold.train_positions)]
        test = frame.iloc[list(fold.test_positions)]
        assert len(train) >= runner.MINIMUM_INITIAL_TRAIN_EVENTS
        assert 1 <= len(test) <= runner.TEST_BLOCK_EVENTS
        assert train["end"].max() < test["end"].min()
        assert ((train["_session_index"] + runner.PURGE_D1_SESSIONS) < fold.first_test_session_index).all()


def test_preprocessing_is_inside_fold_model_pipeline() -> None:
    pipeline = runner.build_model_pipeline(c_value=1.0)
    preprocessor = pipeline.named_steps["preprocessor"]
    numeric = dict((name, transformer) for name, transformer, _ in preprocessor.transformers)["numeric"]
    categorical = dict((name, transformer) for name, transformer, _ in preprocessor.transformers)["categorical"]

    assert isinstance(numeric, Pipeline)
    assert isinstance(numeric.named_steps["imputer"], SimpleImputer)
    assert numeric.named_steps["imputer"].strategy == "median"
    assert isinstance(numeric.named_steps["scaler"], StandardScaler)
    assert isinstance(categorical, OneHotEncoder)
    assert pipeline.named_steps["model"].class_weight == "balanced"


def test_each_model_fit_receives_only_its_fold_training_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _write_inputs(tmp_path / "inputs")
    frame = _prepared(paths)[runner.PRIMARY_TARGET]
    expected = [
        tuple(frame.iloc[list(fold.train_positions)]["end"])
        for fold in runner.build_walk_forward_folds(frame)
    ]
    observed: list[tuple[pd.Timestamp, ...]] = []
    original = runner._fit_fold_model

    def tracking_fit(train_frame: pd.DataFrame, *, c_value: float) -> Pipeline:
        observed.append(tuple(train_frame["end"]))
        return original(train_frame, c_value=c_value)

    monkeypatch.setattr(runner, "_fit_fold_model", tracking_fit)
    runner._evaluate_target(
        frame,
        target_name=runner.PRIMARY_TARGET,
        c_value=1.0,
        collect_coefficients=False,
    )
    assert observed == expected
    assert all(len(rows) < len(frame) for rows in observed)


def test_naive_probability_baseline_comes_from_corresponding_training_fold(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path / "inputs")
    frame = _prepared(paths)[runner.PRIMARY_TARGET]
    predictions, _, _ = runner._evaluate_target(
        frame,
        target_name=runner.PRIMARY_TARGET,
        c_value=1.0,
        collect_coefficients=False,
    )
    folds = {fold.fold: fold for fold in runner.build_walk_forward_folds(frame)}

    for fold_number, fold_predictions in predictions.groupby("fold"):
        train = frame.iloc[list(folds[int(fold_number)].train_positions)]
        expected_prevalence = float(train["target_value"].mean())
        assert set(fold_predictions["baseline_probability"].round(15)) == {round(expected_prevalence, 15)}


def test_single_class_split_metrics_are_null_with_explicit_reasons() -> None:
    predictions = pd.DataFrame(
        {
            "target_value": [1, 1, 1],
            "probability": [0.6, 0.7, 0.8],
            "baseline_probability": [0.5, 0.5, 0.5],
            "baseline_class": [0, 0, 0],
        }
    )
    metrics = runner._metric_bundle(predictions)
    assert metrics["roc_auc"] is None
    assert metrics["pr_auc"] is None
    assert metrics["undefined_metric_reasons"]["roc_auc"] == "split contains one target class"


def test_repeated_synthetic_execution_is_byte_deterministic(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path / "inputs")
    output_a = tmp_path / "out-a"
    output_b = tmp_path / "out-b"
    assert runner.main(_cli_args(paths, output_a, run_id="deterministic")) == 0
    assert runner.main(_cli_args(paths, output_b, run_id="deterministic")) == 0

    for filename in runner.DECLARED_OUTPUT_FILES:
        assert (output_a / filename).read_bytes() == (output_b / filename).read_bytes()


def test_primary_eligibility_and_one_class_stop_conditions(tmp_path: Path) -> None:
    short_paths = _write_inputs(tmp_path / "short", event_count=49)
    with pytest.raises(ValueError, match="minimum is 50"):
        _prepared(short_paths)

    d1, context, labels = _synthetic_frames()
    labels["signed_ret_o2o_h1"] = 0.01
    one_class_paths = _write_inputs(
        tmp_path / "one-class", d1=d1, context=context, labels=labels
    )
    with pytest.raises(ValueError, match="contains only one class"):
        _prepared(one_class_paths)
