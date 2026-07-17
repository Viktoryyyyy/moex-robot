from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from moex_research.features import brent_incremental_features as features
from moex_research.runners import (
    usdrubf_phase8_3_external_factor_incremental_value_experiment as phase83,
)
from moex_research.runners import usdrubf_phase8_5_brent_incremental_value as runner


CONTRACT_PATH = Path(
    "contracts/experiments/usdrubf_phase8_5_brent_incremental_value_v1.json"
)


def _modeling_dataset() -> pd.DataFrame:
    count = runner.EXPECTED_ELIGIBLE_IDENTITIES
    dates = pd.date_range("2024-01-02", periods=count, freq="D")
    index = np.arange(count, dtype=float)
    frame = pd.DataFrame(
        {
            "target_trade_date": dates,
            "target_instrument_id": "forts.usdrubf",
            "prior_trade_date": dates - pd.Timedelta(days=1),
            "target_phase_label": np.resize(np.asarray(runner.CLASS_ORDER), count),
            "target_is_labeled": True,
            "target_source": "manual_phase_labels_v1",
        }
    )
    for offset, column in enumerate(runner.M0_NUMERIC_FEATURES, 1):
        frame[column] = index / (50.0 + offset) + offset
    frame[runner.M0_CATEGORICAL_FEATURES[0]] = np.resize(
        np.asarray(["above", "below", "flat"]), count
    )
    return frame


def _brent_matrix() -> pd.DataFrame:
    dataset = _modeling_dataset()
    count = len(dataset)
    index = np.arange(count, dtype=float)
    codes = np.where(index < 160, "BR00", np.where(index < 320, "BR01", "BR02"))
    changed = pd.Series(codes).ne(pd.Series(codes).shift())
    changed.iloc[0] = False
    previous = pd.Series(codes).shift()
    open_values = 75.0 + index / 200.0
    close_values = open_values + np.sin(index / 9.0) * 0.8
    high_values = np.maximum(open_values, close_values) + 1.0 + index / 10000.0
    low_values = np.minimum(open_values, close_values) - 1.0
    hashes = [f"{value + 1:064x}" for value in range(count)]
    return pd.DataFrame(
        {
            "target_trade_date": dataset["target_trade_date"],
            "target_instrument_id": dataset["target_instrument_id"],
            "prior_trade_date": dataset["prior_trade_date"],
            "brent_contract_code": codes,
            "brent_contract_changed": changed,
            "brent_previous_contract_code": previous,
            "brent_trade_date": dataset["prior_trade_date"],
            "brent_open": open_values,
            "brent_high": high_values,
            "brent_low": low_values,
            "brent_close": close_values,
            "brent_volume": 1000.0 + index * 3.0,
            "brent_value": 100000.0 + index * 101.0,
            "brent_candle_payload_sha256": hashes,
        }
    )


def _m0_predictions(dataset: pd.DataFrame) -> pd.DataFrame:
    eligible = features.prepare_eligible_modeling_rows(dataset)
    folds = runner.build_chronological_folds(eligible)
    rows: list[pd.DataFrame] = []
    for fold_id, (_, valid) in enumerate(folds, 1):
        frame = eligible.iloc[valid][
            [*runner.IDENTITY_COLUMNS, "target_phase_label"]
        ].copy()
        frame.insert(0, "fold_id", fold_id)
        frame = frame.rename(columns={"target_phase_label": "y_true"})
        frame["candidate_y_pred"] = frame["y_true"]
        for label in runner.CLASS_ORDER:
            frame[f"probability_{label}"] = np.where(
                frame["y_true"].eq(label), 0.92, 0.04
            )
        rows.append(frame)
    return pd.concat(rows, ignore_index=True)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_inputs(tmp_path: Path) -> tuple[runner.Phase85Request, dict[str, Path]]:
    dataset = _modeling_dataset()
    matrix = _brent_matrix()
    paths = {
        "modeling_dataset": tmp_path / "modeling_dataset.parquet",
        "dataset_manifest": tmp_path / "manifest.json",
        "feature_schema": tmp_path / "feature_schema.json",
        "m0_validation_predictions": tmp_path / "m0_validation_predictions.parquet",
        "brent_contract_universe": tmp_path / "brent_contract_universe.parquet",
        "brent_daily_candles_normalized": tmp_path
        / "brent_daily_candles_normalized.parquet",
        "brent_pit_acceptance_matrix": tmp_path / "brent_pit_acceptance_matrix.parquet",
        "contract_roll_diagnostics": tmp_path / "contract_roll_diagnostics.csv",
        "coverage_by_source": tmp_path / "coverage_by_source.csv",
        "phase84a_gate_results": tmp_path / "gate_results.json",
        "phase84a_input_identity": tmp_path / "input_identity_verification.json",
        "official_route_validation": tmp_path / "official_route_validation.json",
        "source_blocker_register": tmp_path / "source_blocker_register.json",
    }
    dataset.to_parquet(paths["modeling_dataset"], index=False)
    paths["dataset_manifest"].write_text("{}\n", encoding="utf-8")
    paths["feature_schema"].write_text("{}\n", encoding="utf-8")
    _m0_predictions(dataset).to_parquet(
        paths["m0_validation_predictions"], index=False
    )

    codes = [f"BR{value:02d}" for value in range(29)]
    universe_rows = []
    for value in range(len(matrix)):
        code = codes[value % len(codes)]
        universe_rows.append(
            {
                "contract_code": code,
                "asset_code": "BR",
                "board_id": "RFUD",
                "expiration_date": "2024-12-31" if value % 29 < 22 else "2030-12-31",
            }
        )
    pd.DataFrame(universe_rows).to_parquet(
        paths["brent_contract_universe"], index=False
    )
    candles = pd.DataFrame(
        {
            "contract_code": matrix["brent_contract_code"],
            "trade_date": matrix["brent_trade_date"],
            "raw_payload_sha256": matrix["brent_candle_payload_sha256"],
        }
    )
    candles.to_parquet(paths["brent_daily_candles_normalized"], index=False)
    matrix.to_parquet(paths["brent_pit_acceptance_matrix"], index=False)
    changed_rows = matrix.loc[matrix["brent_contract_changed"]].copy()
    pd.DataFrame(
        {
            "target_trade_date": changed_rows["target_trade_date"],
            "target_or_future_information_used": False,
            "cross_contract_return_calculated": False,
        }
    ).to_csv(paths["contract_roll_diagnostics"], index=False)
    pd.DataFrame(
        [
            {
                "source_id": "moex_brent_futures_daily",
                "eligible_identity_count": 472,
                "eligible_covered_count": 472,
                "eligible_missing_count": 0,
                "eligible_coverage_pct": 100.0,
                "validation_identity_count": 320,
                "validation_covered_count": 320,
                "validation_missing_count": 0,
                "validation_coverage_pct": 100.0,
            }
        ]
    ).to_csv(paths["coverage_by_source"], index=False)
    gate_payload = {f"G{value}_synthetic": {"passed": True} for value in range(1, 9)}
    gate_payload["G9_final_source_readiness"] = {
        "passed": True,
        "status": "moex_brent_source_candidate_for_phase8_5",
        "blocker_classification": None,
        "failed_gates": [],
    }
    paths["phase84a_gate_results"].write_text(
        json.dumps(gate_payload), encoding="utf-8"
    )
    paths["phase84a_input_identity"].write_text(
        json.dumps(
            {
                "run_id": runner.ACCEPTED_PHASE84A_RUN_ID,
                "source_git_commit_sha": runner.EXPECTED_SOURCE_COMMIT,
                "eligible_identity_count": 472,
                "validation_identity_count": 320,
            }
        ),
        encoding="utf-8",
    )
    paths["official_route_validation"].write_text(
        json.dumps(
            {
                "official_service": "MOEX ISS",
                "official_host": "iss.moex.com",
                "history_enumeration_uses_official_SECID": True,
                "contract_code_generation_or_guessing_used": False,
                "current_active_contract_route_used_as_historical_proof": False,
                "unique_explicit_contract_count": 29,
                "expired_explicit_contract_count": 22,
                "explicit_contract_candle_count": 472,
                "all_contracts_BR_RFUD": True,
                "all_routes_official_https": True,
                "retrieval_timestamp_origin": "per_payload_post_transport_utc_clock",
                "caller_provided_production_retrieval_timestamp_allowed": False,
                "metadata_and_candle_payload_provenance_distinguishable": True,
            }
        ),
        encoding="utf-8",
    )
    paths["source_blocker_register"].write_text(
        json.dumps(
            {
                "source_id": "moex_brent_futures_daily",
                "status": "candidate_for_phase8_5",
                "blocker_classification": None,
                "exact_blocker_reason": None,
                "existing_registry_modified": False,
                "promotion_authorized": False,
            }
        ),
        encoding="utf-8",
    )
    contract_copy = tmp_path / CONTRACT_PATH.name
    contract_copy.write_bytes(CONTRACT_PATH.read_bytes())
    request = runner.Phase85Request(
        modeling_dataset_path=paths["modeling_dataset"],
        dataset_manifest_path=paths["dataset_manifest"],
        feature_schema_path=paths["feature_schema"],
        m0_validation_predictions_path=paths["m0_validation_predictions"],
        brent_contract_universe_path=paths["brent_contract_universe"],
        brent_daily_candles_normalized_path=paths[
            "brent_daily_candles_normalized"
        ],
        brent_pit_acceptance_matrix_path=paths["brent_pit_acceptance_matrix"],
        contract_roll_diagnostics_path=paths["contract_roll_diagnostics"],
        coverage_by_source_path=paths["coverage_by_source"],
        phase84a_gate_results_path=paths["phase84a_gate_results"],
        phase84a_input_identity_path=paths["phase84a_input_identity"],
        official_route_validation_path=paths["official_route_validation"],
        source_blocker_register_path=paths["source_blocker_register"],
        experiment_contract_path=contract_copy,
        output_dir=tmp_path / "output",
        run_id="phase8_5_synthetic_v1",
        git_commit_sha="1" * 40,
    )
    return request, paths


def _patch_hashes(monkeypatch: pytest.MonkeyPatch, paths: dict[str, Path]) -> None:
    monkeypatch.setattr(
        runner,
        "EXPECTED_INPUT_SHA256",
        {name: _sha256(path) for name, path in paths.items()},
    )


@pytest.mark.parametrize("input_name", tuple(runner.EXPECTED_INPUT_SHA256))
def test_all_thirteen_immutable_hashes_are_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, input_name: str
) -> None:
    request, paths = _write_inputs(tmp_path)
    _patch_hashes(monkeypatch, paths)
    paths[input_name].write_bytes(paths[input_name].read_bytes() + b"tamper")
    with pytest.raises(runner.Phase85BrentIncrementalValueError, match=input_name):
        runner.run_experiment(request)
    assert not request.output_dir.exists()


def test_exact_472_and_320_identities_and_five_64_row_folds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, paths = _write_inputs(tmp_path)
    _patch_hashes(monkeypatch, paths)
    result = runner.run_experiment(request)
    assert result.eligible_identity_count == 472
    assert result.validation_identity_count == 320
    assert result.fold_count == 5
    evidence = json.loads((request.output_dir / "input_identity_verification.json").read_text())
    assert evidence["validation_rows_per_fold"] == [64] * 5


def test_E0_is_frozen_and_not_refit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    request, paths = _write_inputs(tmp_path)
    _patch_hashes(monkeypatch, paths)
    frozen = pd.read_parquet(request.m0_validation_predictions_path)
    runner.run_experiment(request)
    predictions = pd.read_parquet(
        request.output_dir / "validation_predictions_by_matrix.parquet"
    )
    e0 = predictions.loc[
        predictions["matrix_id"].eq("E0_FROZEN_PHASE7_2_CONTROL")
    ].drop(columns="matrix_id").reset_index(drop=True)
    pd.testing.assert_frame_equal(e0, frozen.loc[:, e0.columns].reset_index(drop=True))
    inventory = json.loads((request.output_dir / "feature_matrix_inventory.json").read_text())
    assert inventory["E0_FROZEN_PHASE7_2_CONTROL"]["refit"] is False


def test_E1_contains_internal_plus_all_six_Brent_features() -> None:
    assert runner.MATRIX_NUMERIC_FEATURES["E1_M0_PLUS_BRENT_FULL"] == (
        *runner.M0_NUMERIC_FEATURES,
        *runner.BRENT_FEATURES,
    )
    assert runner.MATRIX_CATEGORICAL_FEATURES["E1_M0_PLUS_BRENT_FULL"] == (
        runner.M0_CATEGORICAL_FEATURES
    )


def test_E2_contains_only_internal_plus_price_action_block() -> None:
    assert runner.MATRIX_NUMERIC_FEATURES["E2_M0_PLUS_BRENT_PRICE_ACTION"] == (
        *runner.M0_NUMERIC_FEATURES,
        *runner.PRICE_ACTION_FEATURES,
    )


def test_E3_contains_only_internal_plus_activity_block() -> None:
    assert runner.MATRIX_NUMERIC_FEATURES["E3_M0_PLUS_BRENT_ACTIVITY"] == (
        *runner.M0_NUMERIC_FEATURES,
        *runner.ACTIVITY_FEATURES,
    )


def test_E4_contains_Brent_features_only() -> None:
    assert runner.MATRIX_NUMERIC_FEATURES["E4_BRENT_ONLY"] == runner.BRENT_FEATURES
    assert runner.MATRIX_CATEGORICAL_FEATURES["E4_BRENT_ONLY"] == ()


def test_exact_phase83_estimator_and_split_protocol_is_reused() -> None:
    assert runner.SPLITTER_CONSTRUCTOR == phase83.SPLITTER_CONSTRUCTOR
    assert runner.MODEL_CONSTRUCTOR == phase83.MODEL_CONSTRUCTOR
    pipeline = runner.build_candidate_pipeline(("x",), ())
    classifier = pipeline.named_steps["classifier"]
    assert classifier.get_params()["C"] == 1.0
    assert classifier.get_params()["class_weight"] == "balanced"
    assert classifier.get_params()["solver"] == "lbfgs"
    assert classifier.get_params()["max_iter"] == 1000


def test_preprocessing_is_fit_inside_each_training_fold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, paths = _write_inputs(tmp_path)
    _patch_hashes(monkeypatch, paths)
    observed_rows: list[int] = []
    original_fit = runner.StandardScaler.fit

    def recording_fit(self: object, values: object, y: object = None, **kwargs: object) -> object:
        observed_rows.append(len(values))  # type: ignore[arg-type]
        return original_fit(self, values, y, **kwargs)

    monkeypatch.setattr(runner.StandardScaler, "fit", recording_fit)
    runner.run_experiment(request)
    assert observed_rows == [152, 216, 280, 344, 408] * 4
    assert max(observed_rows) < 472


def test_phase83_gate_thresholds_are_reused_without_change() -> None:
    assert runner.ABSOLUTE_LIMITS == phase83.ABSOLUTE_LIMITS


def test_exact_twelve_artifact_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, paths = _write_inputs(tmp_path)
    _patch_hashes(monkeypatch, paths)
    result = runner.run_experiment(request)
    assert result.artifact_names == runner.DECLARED_OUTPUT_ARTIFACTS
    assert sorted(path.name for path in request.output_dir.iterdir()) == sorted(
        runner.DECLARED_OUTPUT_ARTIFACTS
    )


def test_output_directory_must_not_preexist(tmp_path: Path) -> None:
    request, _ = _write_inputs(tmp_path)
    request.output_dir.mkdir()
    with pytest.raises(runner.Phase85BrentIncrementalValueError, match="pre-exist"):
        runner.run_experiment(request)


def test_no_network_access_path_exists() -> None:
    source = inspect.getsource(runner).lower()
    assert "requests" not in source
    assert "urlopen" not in source
    assert "urllib" not in source
    assert "fetch_bytes" not in source


def test_no_write_outside_output_directory(tmp_path: Path) -> None:
    payloads = {name: {} for name in runner.DECLARED_OUTPUT_ARTIFACTS}
    escaped = ("../escape.json", *runner.DECLARED_OUTPUT_ARTIFACTS[1:])
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(runner, "DECLARED_OUTPUT_ARTIFACTS", escaped)
        escaped_payloads = {name: payloads.get(name, {}) for name in escaped}
        with pytest.raises(runner.Phase85BrentIncrementalValueError, match="outside"):
            runner._write_exact_artifacts(tmp_path / "output", escaped_payloads)
    assert not (tmp_path / "escape.json").exists()


def test_no_model_serialization_or_promotion_path_exists() -> None:
    source = inspect.getsource(runner).lower()
    assert "joblib" not in source
    assert "pickle" not in source
    assert "subprocess" not in source
    assert ".dump(" not in source
    assert "promotion_performed\": true" not in source


def _synthetic_gate_inputs() -> tuple[dict[str, dict[str, object]], pd.DataFrame]:
    base = {
        "accuracy": 0.5,
        "balanced_accuracy": 0.5,
        "macro_f1": 0.5,
        "weighted_f1": 0.5,
        "multiclass_log_loss": 1.0,
        "B_recall": 0.5,
        "S_to_OUT_rate": 0.2,
        "OUT_to_S_rate": 0.2,
        "mean_confidence_on_incorrect_predictions": 0.5,
        "fold_macro_f1_range": 0.1,
        "fold_macro_f1_population_standard_deviation": 0.05,
        "minimum_fold_macro_f1": 0.4,
        "zero_B_recall_fold_count": 0,
        "confidence_bucket": {
            "bucket_count": 10,
            "bucket_accuracy": 0.8,
            "bucket_mean_confidence": 0.95,
            "bucket_gap": 0.15,
            "status": "defined",
        },
    }
    e1 = {**base, "multiclass_log_loss": 0.98}
    aggregates = {
        "E0_FROZEN_PHASE7_2_CONTROL": base,
        "E1_M0_PLUS_BRENT_FULL": e1,
    }
    rows = []
    for fold_id in range(1, 6):
        rows.extend(
            [
                {
                    "matrix_id": "E0_FROZEN_PHASE7_2_CONTROL",
                    "fold_id": fold_id,
                    "macro_f1": 0.45,
                },
                {
                    "matrix_id": "E1_M0_PLUS_BRENT_FULL",
                    "fold_id": fold_id,
                    "macro_f1": 0.47,
                },
            ]
        )
    return aggregates, pd.DataFrame(rows)


@pytest.mark.parametrize(
    "failed_flag",
    [
        "immutable_hashes_verified",
        "phase84a_source_verified",
        "identity_verified",
        "feature_integrity_verified",
        "protocol_verified",
        "distribution_verified",
        "scope_verified",
    ],
)
def test_G12_fails_when_any_required_integrity_gate_fails(failed_flag: str) -> None:
    aggregates, folds = _synthetic_gate_inputs()
    flags = {
        "immutable_hashes_verified": True,
        "phase84a_source_verified": True,
        "identity_verified": True,
        "feature_integrity_verified": True,
        "protocol_verified": True,
        "distribution_verified": True,
        "scope_verified": True,
    }
    flags[failed_flag] = False
    gates = runner.evaluate_gates(aggregates, folds, **flags)
    assert gates["G12_final_acceptance"]["passed"] is False
    assert gates["G12_final_acceptance"]["status"] == (
        "brent_incremental_value_not_supported"
    )


def test_exact_runner_cli_and_distinct_explicit_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, paths = _write_inputs(tmp_path)
    parser = runner.build_argument_parser()
    required = {
        action.option_strings[0]
        for action in parser._actions
        if action.required and action.option_strings
    }
    assert required == set(runner.REQUIRED_CLI_ARGS)
    values = {
        "--modeling-dataset-path": request.modeling_dataset_path,
        "--dataset-manifest-path": request.dataset_manifest_path,
        "--feature-schema-path": request.feature_schema_path,
        "--m0-validation-predictions-path": request.m0_validation_predictions_path,
        "--phase8-4a-brent-contract-universe-path": request.brent_contract_universe_path,
        "--phase8-4a-brent-daily-candles-normalized-path": request.brent_daily_candles_normalized_path,
        "--phase8-4a-brent-pit-acceptance-matrix-path": request.brent_pit_acceptance_matrix_path,
        "--phase8-4a-contract-roll-diagnostics-path": request.contract_roll_diagnostics_path,
        "--phase8-4a-coverage-by-source-path": request.coverage_by_source_path,
        "--phase8-4a-gate-results-path": request.phase84a_gate_results_path,
        "--phase8-4a-input-identity-verification-path": request.phase84a_input_identity_path,
        "--phase8-4a-official-route-validation-path": request.official_route_validation_path,
        "--phase8-4a-source-blocker-register-path": request.source_blocker_register_path,
        "--experiment-contract-path": request.experiment_contract_path,
        "--output-dir": request.output_dir,
        "--run-id": request.run_id,
        "--git-commit-sha": request.git_commit_sha,
    }
    argv = [str(item) for flag, value in values.items() for item in (flag, value)]
    parsed = runner.request_from_args(parser.parse_args(argv))
    assert parsed == request
    monkeypatch.setitem(values, "--feature-schema-path", request.dataset_manifest_path)
    duplicate = [str(item) for flag, value in values.items() for item in (flag, value)]
    with pytest.raises(runner.Phase85BrentIncrementalValueError, match="distinct"):
        runner.request_from_args(parser.parse_args(duplicate))
