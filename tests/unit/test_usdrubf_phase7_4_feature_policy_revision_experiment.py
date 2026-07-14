from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from moex_research.runners import usdrubf_phase7_4_feature_policy_revision_experiment as experiment
from moex_research.runners.usdrubf_phase7_4_feature_policy_revision_builder import (
    SOURCE_REQUIRED_COLUMNS,
)


def _source(rows: int = 390) -> pd.DataFrame:
    index = np.arange(rows, dtype=float)
    close = 100 + 0.08 * index + 2.2 * np.sin(index / 8) + 0.5 * np.cos(index / 3)
    spread = 0.008 + 0.003 * (1 + np.sin(index / 11))
    volume = 1000 + 2.5 * index + 45 * np.sin(index / 9)
    trades = 80 + (index % 23) + 0.07 * index
    dates = pd.Timestamp("2023-01-01") + pd.to_timedelta(np.cumsum(1 + (index.astype(int) % 7 == 0)) - 2, unit="D")
    return pd.DataFrame({
        "trade_date": dates,
        "instrument_id": "SiSynthetic",
        "open": close * (0.997 + 0.001 * np.sin(index / 5)),
        "high": close * (1 + spread), "low": close * (1 - spread * 0.8),
        "close": close, "volume": volume, "value": close * volume,
        "num_trades": trades,
    })


def _phase6(source: pd.DataFrame) -> pd.DataFrame:
    close = source["close"].astype(float)
    returns = close.pct_change()
    ranges = (source["high"] - source["low"]) / close
    ema3 = close.ewm(span=3, adjust=False).mean()
    ema19 = close.ewm(span=19, adjust=False).mean()
    records = []
    for index in range(1, len(source)):
        spread = ema3.iloc[index] - ema19.iloc[index]
        records.append({
            "target_phase_label": ("B", "S", "OUT")[index % 3],
            "target_is_labeled": True, "target_source": experiment.TARGET_SOURCE,
            "target_trade_date": (source["trade_date"].iloc[index] + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            "target_instrument_id": "SiSynthetic",
            "prior_trade_date": source["trade_date"].iloc[index].strftime("%Y-%m-%d"),
            "session_index": index + 0.1 * np.sin(index / 7),
            "days_since_prior_trade_date": 1 + (index % 7 == 0),
            "lag1_close_return_1d": returns.iloc[index],
            "lag1_intraday_return": close.iloc[index] / source["open"].iloc[index] - 1,
            "rolling_past_return_mean": returns.iloc[max(1, index - 4):index + 1].mean(),
            "rolling_past_return_std": returns.iloc[max(1, index - 4):index + 1].std(ddof=1),
            "lag1_hl_range_pct": ranges.iloc[index],
            "rolling_past_hl_range_mean": ranges.iloc[max(0, index - 4):index + 1].mean(),
            "rolling_past_hl_range_std": ranges.iloc[max(0, index - 4):index + 1].std(ddof=1),
            "lag1_volume": source["volume"].iloc[index],
            "lag1_value": source["value"].iloc[index],
            "lag1_num_trades": source["num_trades"].iloc[index],
            "rolling_past_volume_mean": source["volume"].iloc[max(0, index - 4):index + 1].mean(),
            "lag1_ema_3": ema3.iloc[index], "lag1_ema_19": ema19.iloc[index],
            "lag1_ema_3_19_spread": spread,
            "lag1_ema_3_19_state": "ema3_above_ema19" if spread > 0 else "ema3_below_ema19" if spread < 0 else "ema3_equal_ema19",
        })
    return pd.DataFrame(records)


def _contract_payload() -> dict[str, object]:
    return {
        "experiment_identity": {
            "contract_id": experiment.EXPERIMENT_CONTRACT_ID,
            "contract_version": experiment.EXPERIMENT_CONTRACT_VERSION,
            "design_recommendation_id": experiment.DESIGN_RECOMMENDATION_ID,
            "project": experiment.PROJECT,
            "lane": experiment.LANE,
            "task_id": experiment.TASK_ID,
            "execution_mode": experiment.EXECUTION_MODE,
        }
    }


def _write_inputs(tmp_path: Path) -> tuple[experiment.Phase74ExperimentRequest, pd.DataFrame]:
    source = _source()
    dataset = _phase6(source)
    source.to_pickle(tmp_path / "source.parquet")
    dataset.to_pickle(tmp_path / "dataset.parquet")
    (tmp_path / "source_manifest.json").write_text(json.dumps({
        "source_panel_id": "synthetic_source_panel_v1",
        "required_columns": list(SOURCE_REQUIRED_COLUMNS),
        "instruments": ["SiSynthetic"],
    }), encoding="utf-8")
    (tmp_path / "dataset_manifest.json").write_text(json.dumps({
        "dataset_id": experiment.DATASET_ID,
        "feature_schema_id": experiment.FEATURE_SCHEMA_ID,
        "target_source": experiment.TARGET_SOURCE,
        "target_columns": list(experiment.PHASE6_TARGET_COLUMNS),
        "feature_columns": list(experiment.PHASE6_FEATURE_COLUMNS),
    }), encoding="utf-8")
    (tmp_path / "feature_schema.json").write_text(json.dumps({
        "schema_id": experiment.FEATURE_SCHEMA_ID,
        "dataset_id": experiment.DATASET_ID,
        "target_columns": list(experiment.PHASE6_TARGET_COLUMNS),
        "feature_columns": list(experiment.PHASE6_FEATURE_COLUMNS),
    }), encoding="utf-8")
    (tmp_path / "readiness.yaml").write_text(
        f"contract_id: {experiment.READINESS_CONTRACT_ID}\ntarget_source: {experiment.TARGET_SOURCE}\n",
        encoding="utf-8",
    )
    (tmp_path / "contract.json").write_text(json.dumps(_contract_payload()), encoding="utf-8")
    eligible = experiment._prepare_experiment_eligible_rows(dataset)
    folds = experiment.build_chronological_folds(eligible)
    m0 = experiment._expected_validation_identities(eligible, folds)
    m0["candidate_y_pred"] = "B"
    m0["probability_B"] = 0.95
    m0["probability_S"] = 0.03
    m0["probability_OUT"] = 0.02
    m0.to_pickle(tmp_path / "m0.parquet")
    request = experiment.Phase74ExperimentRequest(
        source_panel_path=tmp_path / "source.parquet",
        source_panel_manifest_path=tmp_path / "source_manifest.json",
        modeling_dataset_path=tmp_path / "dataset.parquet",
        dataset_manifest_path=tmp_path / "dataset_manifest.json",
        feature_schema_path=tmp_path / "feature_schema.json",
        readiness_contract_path=tmp_path / "readiness.yaml",
        m0_validation_predictions_path=tmp_path / "m0.parquet",
        experiment_contract_path=tmp_path / "contract.json",
        output_dir=tmp_path / "output", run_id="synthetic_phase74",
        git_commit_sha="a" * 40,
    )
    return request, m0


def _patch_parquet(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pd, "read_parquet", lambda path, *args, **kwargs: pd.read_pickle(path))


def test_exact_eleven_argument_cli_and_path_rules(tmp_path: Path) -> None:
    parser = experiment.build_argument_parser()
    options = tuple(action.option_strings[0] for action in parser._actions if action.option_strings and action.option_strings[0] != "-h")
    assert options == experiment.REQUIRED_CLI_ARGS
    assert len(options) == 11
    names = ["source.parquet", "source_manifest.json", "dataset.parquet", "dataset_manifest.json", "feature_schema.json", "readiness.yaml", "m0.parquet", "contract.json"]
    for name in names:
        (tmp_path / name).write_bytes(b"x")
    values = [str(tmp_path / names[0]), str(tmp_path / names[1]), str(tmp_path / names[2]), str(tmp_path / names[3]), str(tmp_path / names[4]), str(tmp_path / names[5]), str(tmp_path / names[6]), str(tmp_path / names[7]), str(tmp_path / "output"), "run", "b" * 40]
    args = parser.parse_args([item for pair in zip(experiment.REQUIRED_CLI_ARGS, values) for item in pair])
    assert experiment.request_from_args(args).run_id == "run"
    args.source_panel_path = str(tmp_path / "latest_source.parquet")
    with pytest.raises(experiment.Phase74FeaturePolicyExperimentError, match="mutable alias"):
        experiment.request_from_args(args)
    args.source_panel_path = str(tmp_path / "*.parquet")
    with pytest.raises(experiment.Phase74FeaturePolicyExperimentError, match="glob syntax"):
        experiment.request_from_args(args)


def test_fixed_splitter_model_and_fold_training_classes() -> None:
    eligible = experiment._prepare_experiment_eligible_rows(_phase6(_source()))
    folds = experiment.build_chronological_folds(eligible)
    assert experiment.SPLITTER_CONSTRUCTOR == {"n_splits": 5, "test_size": 64, "gap": 0}
    assert isinstance(TimeSeriesSplit(**experiment.SPLITTER_CONSTRUCTOR), TimeSeriesSplit)
    assert len(folds) == 5 and sum(len(valid) for _, valid in folds) == 320
    assert all(set(eligible.iloc[train]["target_phase_label"]) == set(experiment.CLASS_ORDER) for train, _ in folds)
    pipeline = experiment.build_candidate_pipeline(experiment.M1_NUMERIC_FEATURES, experiment.M1_CATEGORICAL_FEATURES)
    classifier = pipeline.named_steps["classifier"]
    assert isinstance(classifier, LogisticRegression)
    assert (classifier.C, classifier.class_weight, classifier.solver, classifier.max_iter) == (1.0, "balanced", "lbfgs", 1000)
    preprocessor = pipeline.named_steps["preprocessor"]
    numeric = preprocessor.transformers[0][1]
    categorical = preprocessor.transformers[1][1]
    assert isinstance(numeric.named_steps["imputer"], SimpleImputer)
    assert numeric.named_steps["imputer"].strategy == "median"
    assert isinstance(numeric.named_steps["scaler"], StandardScaler)
    assert categorical.named_steps["imputer"].strategy == "most_frequent"
    assert isinstance(categorical.named_steps["encoder"], OneHotEncoder)
    assert categorical.named_steps["encoder"].handle_unknown == "ignore"


def test_m0_identity_exact_and_immutable_behavior(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    request, m0 = _write_inputs(tmp_path)
    dataset = pd.read_pickle(request.modeling_dataset_path)
    eligible = experiment._prepare_experiment_eligible_rows(dataset)
    folds = experiment.build_chronological_folds(eligible)
    identities = experiment._expected_validation_identities(eligible, folds)
    assert len(experiment.validate_m0_predictions(m0, expected_fold_identities=identities)) == 320
    moved = m0.copy()
    moved.loc[0, "fold_id"] = 2
    with pytest.raises(experiment.Phase74FeaturePolicyExperimentError, match="ordered fold identities"):
        experiment.validate_m0_predictions(moved, expected_fold_identities=identities)
    invalid = m0.copy()
    invalid.loc[:, ["probability_B", "probability_S", "probability_OUT"]] = 1 / 3
    monkeypatch.setattr(experiment, "build_candidate_pipeline", lambda *args: (_ for _ in ()).throw(AssertionError("M0 must stop before fit")))
    with pytest.raises(experiment.Phase74FeaturePolicyExperimentError, match="M0 highest-confidence bucket"):
        experiment._m0_metrics(invalid)


def test_confidence_bucket_inclusive_boundaries_gap_and_empty() -> None:
    frame = pd.DataFrame({
        "y_true": ["B", "S", "OUT"], "candidate_y_pred": ["B", "B", "OUT"],
        "probability_B": [0.90, 0.90, 0.0], "probability_S": [0.05, 0.05, 0.0],
        "probability_OUT": [0.05, 0.05, 1.0],
    })
    bucket = experiment.confidence_bucket(frame)
    assert bucket["bucket_count"] == 3
    assert bucket["bucket_accuracy"] == pytest.approx(2 / 3)
    assert bucket["bucket_mean_confidence"] == pytest.approx((0.9 + 0.9 + 1.0) / 3)
    assert bucket["bucket_gap"] == pytest.approx(bucket["bucket_mean_confidence"] - bucket["bucket_accuracy"])
    empty = frame.copy()
    empty.loc[:, ["probability_B", "probability_S", "probability_OUT"]] = [0.4, 0.3, 0.3]
    assert experiment.confidence_bucket(empty)["status"] == "undefined_empty"


def test_feature_smd_formula_and_undefined_fail_closed() -> None:
    train = pd.Series([1.0, 2.0, 3.0])
    validation = pd.Series([3.0, 4.0])
    expected = abs(3.5 - 2.0) / np.std([1.0, 2.0, 3.0], ddof=1)
    assert experiment.feature_smd(train, validation) == pytest.approx(expected)
    with pytest.raises(experiment.Phase74FeaturePolicyExperimentError, match="standard deviation"):
        experiment.feature_smd(pd.Series([1.0, 1.0]), pd.Series([2.0]))


def _metric_payload(value: float = 0.5) -> dict[str, object]:
    return {
        "B_recall": value, "zero_B_recall_fold_count": 0, "macro_f1": value,
        "balanced_accuracy": value, "accuracy": value,
        "multiclass_log_loss": 1.0, "fold_macro_f1_range": 0.1,
        "fold_macro_f1_population_standard_deviation": 0.05,
        "minimum_fold_macro_f1": 0.2, "S_to_OUT_rate": 0.2,
        "OUT_to_S_rate": 0.2, "mean_confidence_on_incorrect_predictions": 0.6,
        "confidence_bucket": {"bucket_count": 10, "bucket_gap": 0.1},
    }


def test_s1_s18_and_f1_f11_gate_inventory_and_roles() -> None:
    m0 = _metric_payload(0.35)
    m0["confidence_bucket"] = {"bucket_count": 10, "bucket_gap": 0.2}
    m1 = _metric_payload(0.5)
    folds0 = pd.DataFrame({"fold_id": range(1, 6), "macro_f1": [0.3] * 5})
    folds1 = pd.DataFrame({"fold_id": range(1, 6), "macro_f1": [0.32] * 5})
    shifts = [{"comparison_role": "revised_M1", "group": group, "ratio_to_mapped_M0": 0.7} for group in experiment.REVISED_GROUPS]
    identities = {"zero_eligible_row_loss": True, "same_eligible_identities_as_M0_basis": True, "same_320_validation_identities_as_M0": True, "no_all_null_training_fold_feature": True, "warmup_rates_within_limit": True, "denominator_rates_within_limit": True}
    gates = experiment.evaluate_gates(m0, m1, folds0, folds1, shifts, identities)
    assert set(gates["success_criteria"]) == {f"S{i}" for i in range(1, 19)}
    assert set(gates["failure_criteria"]) == {f"F{i}" for i in range(1, 12)}
    assert gates["M0_reference_status"] == "immutable_historical_control"
    assert gates["M2_M5_status"] == "diagnostic_only_not_eligible_for_acceptance"
    assert gates["no_promotion_declaration"]


def test_exact_nine_outputs_deterministic_no_serialization_or_external_actions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    request, _ = _write_inputs(tmp_path)
    _patch_parquet(monkeypatch)
    first = experiment.run_experiment(request)
    assert first.artifact_names == experiment.DECLARED_OUTPUT_ARTIFACTS
    assert first.validation_row_count == 320 and first.fold_count == 5
    assert sorted(path.name for path in request.output_dir.iterdir()) == sorted(experiment.DECLARED_OUTPUT_ARTIFACTS)
    assert not any(path.suffix in {".pkl", ".pickle", ".joblib"} for path in request.output_dir.iterdir())
    snapshot = {path.name: path.read_bytes() for path in request.output_dir.iterdir()}
    identity = json.loads((request.output_dir / "input_identity_verification.json").read_text())
    assert set(identity["inputs"]) == set(experiment._input_paths(request))
    assert all(set(value) == {"path", "sha256", "declared_identity"} for value in identity["inputs"].values())
    assert identity["inputs"]["modeling_dataset"]["declared_identity"]["eligible_identity_count"] == len(_phase6(_source()))
    assert identity["inputs"]["m0_validation_predictions"]["declared_identity"]["role"] == "immutable_historical_control_predictions"
    assert identity["inputs"]["m0_validation_predictions"]["declared_identity"]["row_count"] == 320
    second_request = experiment.Phase74ExperimentRequest(**{**request.__dict__, "output_dir": tmp_path / "output2"})
    experiment.run_experiment(second_request)
    assert snapshot == {path.name: path.read_bytes() for path in second_request.output_dir.iterdir()}
    source_text = Path(experiment.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import urllib", "import subprocess", "import socket", "pickle.dump", "joblib.dump"):
        assert forbidden not in source_text


def test_manifest_schema_and_contract_identity_mismatches_fail_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    request, _ = _write_inputs(tmp_path)
    _patch_parquet(monkeypatch)
    cases = [
        (request.dataset_manifest_path, "target_columns", None, "dataset manifest target_columns"),
        (request.dataset_manifest_path, "feature_columns", ["wrong"], "dataset manifest feature_columns"),
        (request.feature_schema_path, "target_columns", ["wrong"], "feature schema target_columns"),
        (request.feature_schema_path, "feature_columns", None, "feature schema feature_columns"),
    ]
    for path, key, value, message in cases:
        original = path.read_text()
        payload = json.loads(original)
        if value is None:
            payload.pop(key)
        else:
            payload[key] = value
        path.write_text(json.dumps(payload))
        with pytest.raises(experiment.Phase74FeaturePolicyExperimentError, match=message):
            experiment.run_experiment(request)
        path.write_text(original)
    for key in ("project", "lane", "task_id", "execution_mode", "design_recommendation_id"):
        original = request.experiment_contract_path.read_text()
        payload = json.loads(original)
        payload["experiment_identity"][key] = "wrong"
        request.experiment_contract_path.write_text(json.dumps(payload))
        with pytest.raises(experiment.Phase74FeaturePolicyExperimentError, match="experiment contract identity mismatch"):
            experiment.run_experiment(request)
        request.experiment_contract_path.write_text(original)


def test_nonempty_output_dir_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    request, _ = _write_inputs(tmp_path)
    request.output_dir.mkdir()
    (request.output_dir / "existing.txt").write_text("x")
    _patch_parquet(monkeypatch)
    with pytest.raises(experiment.Phase74FeaturePolicyExperimentError, match="absent or empty"):
        experiment.run_experiment(request)
