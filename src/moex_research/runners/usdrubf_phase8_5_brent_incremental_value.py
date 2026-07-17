from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Iterable, Mapping

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from moex_research.features.brent_incremental_features import (
    ACTIVITY_FEATURES,
    BRENT_FEATURES,
    CLASS_ORDER,
    EXPECTED_ELIGIBLE_IDENTITIES,
    IDENTITY_COLUMNS,
    MATRIX_CATEGORICAL_FEATURES,
    MATRIX_NUMERIC_FEATURES,
    MATRIX_ROLES,
    M0_CATEGORICAL_FEATURES,
    M0_NUMERIC_FEATURES,
    PRICE_ACTION_FEATURES,
    BrentFeatureBuildResult,
    BrentIncrementalFeatureError,
    build_brent_feature_matrices,
    prepare_eligible_modeling_rows,
)
from moex_research.runners import (
    usdrubf_phase8_3_external_factor_incremental_value_experiment as phase83_protocol,
)


PROJECT: Final[str] = "MOEX Bot"
PHASE: Final[str] = "8.5"
LANE: Final[str] = "ema_3_19_ai"
TASK_ID: Final[str] = (
    "ema_3_19_ai_market_phase_phase_8_5_brent_incremental_value_experiment_v1"
)
EXECUTION_MODE: Final[str] = "browser_chatgpt_github_direct"
CONTRACT_ID: Final[str] = "usdrubf_phase8_5_brent_incremental_value_v1"
APPROVED_BRANCH: Final[str] = (
    "research/ema-3-19-ai/phase-8-5-brent-incremental-value"
)
APPROVED_FILES: Final[tuple[str, ...]] = (
    "contracts/experiments/usdrubf_phase8_5_brent_incremental_value_v1.json",
    "src/moex_research/features/brent_incremental_features.py",
    "src/moex_research/runners/usdrubf_phase8_5_brent_incremental_value.py",
    "tests/unit/test_usdrubf_phase8_5_brent_incremental_features.py",
    "tests/unit/test_usdrubf_phase8_5_brent_incremental_value.py",
    "tests/contract/test_usdrubf_phase8_5_brent_incremental_value_contract.py",
)
ACCEPTED_PHASE84A_RUN_ID: Final[str] = (
    "phase8_4a_moex_brent_source_validation_20260716_v2"
)
EXPECTED_SOURCE_COMMIT: Final[str] = "0e25691f8c665052c2d44b7bf66d8ef053543149"
EXPECTED_EXPERIMENT_CONTRACT_SHA256: Final[str] = (
    "c96aaf4a0ea8a415fc03c13aec43c83cb74815dd0d89c88f0a4f7eb8ed20f232"
)
EXPECTED_VALIDATION_IDENTITIES: Final[int] = 320
PROBABILITY_COLUMNS: Final[tuple[str, ...]] = (
    "probability_B",
    "probability_S",
    "probability_OUT",
)
M0_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "fold_id",
    *IDENTITY_COLUMNS,
    "y_true",
    "candidate_y_pred",
    *PROBABILITY_COLUMNS,
)
SPLITTER_CONSTRUCTOR: Final[dict[str, int]] = dict(
    phase83_protocol.SPLITTER_CONSTRUCTOR
)
MODEL_CONSTRUCTOR: Final[dict[str, Any]] = dict(phase83_protocol.MODEL_CONSTRUCTOR)
ABSOLUTE_LIMITS: Final[dict[str, tuple[str, float]]] = dict(
    phase83_protocol.ABSOLUTE_LIMITS
)
FROZEN_INPUT_SHA256: Final[dict[str, str]] = {
    "modeling_dataset": "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00",
    "dataset_manifest": "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082",
    "feature_schema": "8f08802c7fb0a4cc43ab4ba072ee22ff9edd92fe8d674ea0515545d20d143238",
    "m0_validation_predictions": "9769d00a49adeb54c016d965387774e46a3e09e09f895aa61d48a90bbf3568cf",
    "brent_contract_universe": "0b5b27d686cb99866a22ddbf52ff9713e25f8ac88531ac64b96b49361775bb83",
    "brent_daily_candles_normalized": "851f965944bcd60ff9d97e75d305fd43a6bb8eb00851e1da346adb07e139dff3",
    "brent_pit_acceptance_matrix": "78b60c9542fc08667267849b9ce03fdf161d2dc971fe99e1ab9d6a8d56266c43",
    "contract_roll_diagnostics": "cdeae7dc34f8845321bf3b4ca9279f5d50bf41464812fd8698e36dae47fc71dd",
    "coverage_by_source": "261c8bbd9235f8f5a5249d43e6195bf7ee44a29012fc57dbcb1d836be4cd3ded",
    "phase84a_gate_results": "aceaefb4d2e2a236539dd527c98464ddd1ea6bf5f1cdb8121662e1ce087f9c4c",
    "phase84a_input_identity": "3fa20b2daf45f196937064b5f1cc6a58b8009e544d6141e32a77d841d68b65ae",
    "official_route_validation": "e5f66ab37a270ed443ad513b7a84da249e9e9c7f032c6a194c7c056e70540fbe",
    "source_blocker_register": "60b6742ddaf7b3dde12c5128af2a2b765d3e29b57ec904ffaa97f301830fe946",
}
EXPECTED_INPUT_SHA256: dict[str, str] = dict(FROZEN_INPUT_SHA256)
DECLARED_OUTPUT_ARTIFACTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "feature_matrix_inventory.json",
    "brent_feature_definitions.json",
    "feature_nullness_by_matrix_and_fold.csv",
    "brent_feature_shift_by_fold.csv",
    "fold_metrics_by_matrix.csv",
    "aggregate_metrics_by_matrix.json",
    "per_class_metrics_by_matrix.csv",
    "validation_predictions_by_matrix.parquet",
    "ablation_effects.csv",
    "brent_feature_coefficients_by_fold.csv",
    "gate_results.json",
)
REQUIRED_CLI_ARGS: Final[tuple[str, ...]] = (
    "--modeling-dataset-path",
    "--dataset-manifest-path",
    "--feature-schema-path",
    "--m0-validation-predictions-path",
    "--phase8-4a-brent-contract-universe-path",
    "--phase8-4a-brent-daily-candles-normalized-path",
    "--phase8-4a-brent-pit-acceptance-matrix-path",
    "--phase8-4a-contract-roll-diagnostics-path",
    "--phase8-4a-coverage-by-source-path",
    "--phase8-4a-gate-results-path",
    "--phase8-4a-input-identity-verification-path",
    "--phase8-4a-official-route-validation-path",
    "--phase8-4a-source-blocker-register-path",
    "--experiment-contract-path",
    "--output-dir",
    "--run-id",
    "--git-commit-sha",
)
BRENT_FEATURE_DEFINITIONS: Final[dict[str, dict[str, str]]] = {
    "ext_brent_log_close": {
        "block": "PRICE_ACTION_BLOCK",
        "formula": "natural_log(prior_session_explicit_contract_close)",
    },
    "ext_brent_intraday_return": {
        "block": "PRICE_ACTION_BLOCK",
        "formula": "(close / open) - 1",
    },
    "ext_brent_range_pct": {
        "block": "PRICE_ACTION_BLOCK",
        "formula": "(high - low) / open",
    },
    "ext_brent_close_location": {
        "block": "PRICE_ACTION_BLOCK",
        "formula": "(close - low) / (high - low); 0.5 when high == low",
    },
    "ext_log1p_brent_volume": {
        "block": "ACTIVITY_BLOCK",
        "formula": "log1p(volume)",
    },
    "ext_log1p_brent_value": {
        "block": "ACTIVITY_BLOCK",
        "formula": "log1p(value)",
    },
}
_ALIAS_PATTERN = re.compile(r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.I)
_SHA_PATTERN = re.compile(r"^[0-9a-fA-F]{40}$")
_GLOB_CHARS = frozenset("*?[]")


class Phase85BrentIncrementalValueError(ValueError):
    """Raised when the frozen Phase 8.5 protocol must fail closed."""


@dataclass(frozen=True)
class Phase85Request:
    modeling_dataset_path: Path
    dataset_manifest_path: Path
    feature_schema_path: Path
    m0_validation_predictions_path: Path
    brent_contract_universe_path: Path
    brent_daily_candles_normalized_path: Path
    brent_pit_acceptance_matrix_path: Path
    contract_roll_diagnostics_path: Path
    coverage_by_source_path: Path
    phase84a_gate_results_path: Path
    phase84a_input_identity_path: Path
    official_route_validation_path: Path
    source_blocker_register_path: Path
    experiment_contract_path: Path
    output_dir: Path
    run_id: str
    git_commit_sha: str


@dataclass(frozen=True)
class Phase85Result:
    output_dir: Path
    artifact_names: tuple[str, ...]
    eligible_identity_count: int
    validation_identity_count: int
    fold_count: int
    final_status: str


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python -m moex_research.runners."
            "usdrubf_phase8_5_brent_incremental_value"
        )
    )
    for flag in REQUIRED_CLI_ARGS:
        parser.add_argument(flag, required=True)
    return parser


def _explicit_input_path(raw: object, flag: str, suffixes: tuple[str, ...]) -> Path:
    text = str(raw).strip()
    if not text:
        raise Phase85BrentIncrementalValueError(f"{flag} must be non-empty")
    if any(character in text for character in _GLOB_CHARS):
        raise Phase85BrentIncrementalValueError(f"{flag} must not contain glob syntax")
    if _ALIAS_PATTERN.search(text):
        raise Phase85BrentIncrementalValueError(f"{flag} must not use mutable alias")
    path = Path(text)
    if path.suffix.lower() not in suffixes:
        raise Phase85BrentIncrementalValueError(f"{flag} suffix mismatch")
    if not path.exists() or not path.is_file():
        raise Phase85BrentIncrementalValueError(f"{flag} must identify one existing file")
    return path


def _explicit_output_path(raw: object) -> Path:
    text = str(raw).strip()
    if (
        not text
        or any(character in text for character in _GLOB_CHARS)
        or _ALIAS_PATTERN.search(text)
    ):
        raise Phase85BrentIncrementalValueError(
            "--output-dir must be explicit and immutable"
        )
    return Path(text)


def request_from_args(args: argparse.Namespace) -> Phase85Request:
    request = Phase85Request(
        modeling_dataset_path=_explicit_input_path(
            args.modeling_dataset_path, "--modeling-dataset-path", (".parquet",)
        ),
        dataset_manifest_path=_explicit_input_path(
            args.dataset_manifest_path, "--dataset-manifest-path", (".json",)
        ),
        feature_schema_path=_explicit_input_path(
            args.feature_schema_path, "--feature-schema-path", (".json",)
        ),
        m0_validation_predictions_path=_explicit_input_path(
            args.m0_validation_predictions_path,
            "--m0-validation-predictions-path",
            (".parquet",),
        ),
        brent_contract_universe_path=_explicit_input_path(
            args.phase8_4a_brent_contract_universe_path,
            "--phase8-4a-brent-contract-universe-path",
            (".parquet",),
        ),
        brent_daily_candles_normalized_path=_explicit_input_path(
            args.phase8_4a_brent_daily_candles_normalized_path,
            "--phase8-4a-brent-daily-candles-normalized-path",
            (".parquet",),
        ),
        brent_pit_acceptance_matrix_path=_explicit_input_path(
            args.phase8_4a_brent_pit_acceptance_matrix_path,
            "--phase8-4a-brent-pit-acceptance-matrix-path",
            (".parquet",),
        ),
        contract_roll_diagnostics_path=_explicit_input_path(
            args.phase8_4a_contract_roll_diagnostics_path,
            "--phase8-4a-contract-roll-diagnostics-path",
            (".csv",),
        ),
        coverage_by_source_path=_explicit_input_path(
            args.phase8_4a_coverage_by_source_path,
            "--phase8-4a-coverage-by-source-path",
            (".csv",),
        ),
        phase84a_gate_results_path=_explicit_input_path(
            args.phase8_4a_gate_results_path,
            "--phase8-4a-gate-results-path",
            (".json",),
        ),
        phase84a_input_identity_path=_explicit_input_path(
            args.phase8_4a_input_identity_verification_path,
            "--phase8-4a-input-identity-verification-path",
            (".json",),
        ),
        official_route_validation_path=_explicit_input_path(
            args.phase8_4a_official_route_validation_path,
            "--phase8-4a-official-route-validation-path",
            (".json",),
        ),
        source_blocker_register_path=_explicit_input_path(
            args.phase8_4a_source_blocker_register_path,
            "--phase8-4a-source-blocker-register-path",
            (".json",),
        ),
        experiment_contract_path=_explicit_input_path(
            args.experiment_contract_path, "--experiment-contract-path", (".json",)
        ),
        output_dir=_explicit_output_path(args.output_dir),
        run_id=str(args.run_id).strip(),
        git_commit_sha=str(args.git_commit_sha).strip().lower(),
    )
    if not request.run_id:
        raise Phase85BrentIncrementalValueError("--run-id must be non-empty")
    if not _SHA_PATTERN.fullmatch(request.git_commit_sha):
        raise Phase85BrentIncrementalValueError(
            "--git-commit-sha must be 40 hexadecimal characters"
        )
    resolved = [path.resolve() for path in _input_paths(request).values()]
    if len(set(resolved)) != len(resolved):
        raise Phase85BrentIncrementalValueError(
            "all file inputs must resolve to distinct files"
        )
    return request


def _input_paths(request: Phase85Request) -> dict[str, Path]:
    return {
        "modeling_dataset": request.modeling_dataset_path,
        "dataset_manifest": request.dataset_manifest_path,
        "feature_schema": request.feature_schema_path,
        "m0_validation_predictions": request.m0_validation_predictions_path,
        "brent_contract_universe": request.brent_contract_universe_path,
        "brent_daily_candles_normalized": request.brent_daily_candles_normalized_path,
        "brent_pit_acceptance_matrix": request.brent_pit_acceptance_matrix_path,
        "contract_roll_diagnostics": request.contract_roll_diagnostics_path,
        "coverage_by_source": request.coverage_by_source_path,
        "phase84a_gate_results": request.phase84a_gate_results_path,
        "phase84a_input_identity": request.phase84a_input_identity_path,
        "official_route_validation": request.official_route_validation_path,
        "source_blocker_register": request.source_blocker_register_path,
        "experiment_contract": request.experiment_contract_path,
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_immutable_hashes(request: Phase85Request) -> dict[str, str]:
    paths = _input_paths(request)
    observed = {
        name: _sha256(paths[name]) for name in EXPECTED_INPUT_SHA256
    }
    mismatches = [
        name
        for name, expected in EXPECTED_INPUT_SHA256.items()
        if observed.get(name) != expected
    ]
    if mismatches:
        raise Phase85BrentIncrementalValueError(
            "immutable input hash mismatch: " + ", ".join(mismatches)
        )
    return observed


def verify_experiment_contract_hash(request: Phase85Request) -> str:
    observed = _sha256(request.experiment_contract_path)
    if observed != EXPECTED_EXPERIMENT_CONTRACT_SHA256:
        raise Phase85BrentIncrementalValueError(
            "experiment contract SHA256 mismatch"
        )
    return observed


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase85BrentIncrementalValueError(
            f"invalid JSON: {path.name}"
        ) from exc
    if not isinstance(value, dict):
        raise Phase85BrentIncrementalValueError(
            f"JSON must be an object: {path.name}"
        )
    return value


def validate_phase84a_acceptance_identity(
    input_identity: Mapping[str, Any],
    route_validation: Mapping[str, Any],
    blocker_register: Mapping[str, Any],
    gate_results: Mapping[str, Any],
) -> None:
    if input_identity.get("run_id") != ACCEPTED_PHASE84A_RUN_ID:
        raise Phase85BrentIncrementalValueError(
            "rejected or unknown Phase 8.4A run identity"
        )
    if input_identity.get("source_git_commit_sha") != EXPECTED_SOURCE_COMMIT:
        raise Phase85BrentIncrementalValueError("Phase 8.4A source commit mismatch")
    if (
        input_identity.get("eligible_identity_count") != EXPECTED_ELIGIBLE_IDENTITIES
        or input_identity.get("validation_identity_count")
        != EXPECTED_VALIDATION_IDENTITIES
    ):
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A eligible or validation coverage mismatch"
        )
    required_gates = tuple(f"G{i}" for i in range(1, 10))
    normalized_gate_names = {
        str(name).split("_", 1)[0]: payload
        for name, payload in gate_results.items()
        if isinstance(payload, Mapping)
    }
    if any(
        name not in normalized_gate_names
        or not bool(
            normalized_gate_names[name].get(
                "passed", normalized_gate_names[name].get("pass", False)
            )
        )
        for name in required_gates
    ):
        raise Phase85BrentIncrementalValueError("Phase 8.4A G1 through G9 must pass")
    final_gate = normalized_gate_names["G9"]
    if (
        final_gate.get("status") != "moex_brent_source_candidate_for_phase8_5"
        or final_gate.get("blocker_classification") is not None
        or final_gate.get("failed_gates") != []
    ):
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A final source readiness mismatch"
        )
    if blocker_register != {
        "source_id": "moex_brent_futures_daily",
        "status": "candidate_for_phase8_5",
        "blocker_classification": None,
        "exact_blocker_reason": None,
        "existing_registry_modified": False,
        "promotion_authorized": False,
    }:
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A blocker register or authority mismatch"
        )
    expected_routes = {
        "official_service": "MOEX ISS",
        "official_host": "iss.moex.com",
        "history_enumeration_uses_official_SECID": True,
        "contract_code_generation_or_guessing_used": False,
        "current_active_contract_route_used_as_historical_proof": False,
        "unique_explicit_contract_count": 29,
        "expired_explicit_contract_count": 22,
        "explicit_contract_candle_count": EXPECTED_ELIGIBLE_IDENTITIES,
        "all_contracts_BR_RFUD": True,
        "all_routes_official_https": True,
        "retrieval_timestamp_origin": "per_payload_post_transport_utc_clock",
        "caller_provided_production_retrieval_timestamp_allowed": False,
        "metadata_and_candle_payload_provenance_distinguishable": True,
    }
    if any(route_validation.get(key) != value for key, value in expected_routes.items()):
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A official route evidence mismatch"
        )


def validate_phase84a_artifacts(
    *,
    universe: pd.DataFrame,
    candles: pd.DataFrame,
    matrix: pd.DataFrame,
    coverage: pd.DataFrame,
    rolls: pd.DataFrame,
    eligible: pd.DataFrame,
) -> None:
    required_universe = ("contract_code", "asset_code", "board_id", "expiration_date")
    required_candles = (
        "contract_code",
        "trade_date",
        "raw_payload_sha256",
    )
    required_matrix = (
        *IDENTITY_COLUMNS,
        "prior_trade_date",
        "brent_contract_code",
        "brent_contract_changed",
        "brent_previous_contract_code",
        "brent_trade_date",
        "brent_candle_payload_sha256",
    )
    for frame, columns, label in (
        (universe, required_universe, "contract universe"),
        (candles, required_candles, "normalized candles"),
        (matrix, required_matrix, "PIT acceptance matrix"),
    ):
        missing = [column for column in columns if column not in frame.columns]
        if missing:
            raise Phase85BrentIncrementalValueError(
                f"Phase 8.4A {label} missing: " + ", ".join(missing)
            )
    if (
        universe["contract_code"].nunique() != 29
        or not universe["asset_code"].eq("BR").all()
        or not universe["board_id"].eq("RFUD").all()
    ):
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A explicit BR RFUD contract universe mismatch"
        )
    expirations = pd.to_datetime(universe["expiration_date"], errors="coerce")
    if expirations.isna().any():
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A contract expiration is invalid"
        )
    last_target = pd.to_datetime(eligible["target_trade_date"]).max()
    expired_codes = universe.loc[expirations.lt(last_target), "contract_code"].nunique()
    if expired_codes != 22:
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A expired explicit contract count mismatch"
        )
    if len(matrix) != EXPECTED_ELIGIBLE_IDENTITIES or len(candles) != EXPECTED_ELIGIBLE_IDENTITIES:
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A must contain one explicit candle per eligible identity"
        )

    matrix_identity = matrix.loc[:, IDENTITY_COLUMNS].copy()
    matrix_identity["target_trade_date"] = pd.to_datetime(
        matrix_identity["target_trade_date"], errors="raise"
    ).dt.strftime("%Y-%m-%d")
    matrix_identity["target_instrument_id"] = (
        matrix_identity["target_instrument_id"].astype("string").str.strip().astype(str)
    )
    if (
        matrix_identity.duplicated(list(IDENTITY_COLUMNS), keep=False).any()
        or not matrix_identity.reset_index(drop=True).equals(
            eligible.loc[:, IDENTITY_COLUMNS].reset_index(drop=True)
        )
    ):
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A PIT identity order differs from frozen Phase 6"
        )
    matrix_prior = pd.to_datetime(matrix["prior_trade_date"], errors="raise").dt.strftime(
        "%Y-%m-%d"
    )
    eligible_prior = eligible["prior_trade_date"].reset_index(drop=True)
    if not matrix_prior.reset_index(drop=True).equals(eligible_prior):
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A prior_trade_date differs from frozen Phase 6"
        )

    candle_keys = candles.loc[:, required_candles].copy()
    candle_keys["trade_date"] = pd.to_datetime(
        candle_keys["trade_date"], errors="raise"
    ).dt.strftime("%Y-%m-%d")
    candle_keys["contract_code"] = candle_keys["contract_code"].astype(str)
    if candle_keys.duplicated(["contract_code", "trade_date"], keep=False).any():
        raise Phase85BrentIncrementalValueError("Phase 8.4A candle identity is duplicated")
    matrix_keys = matrix.loc[
        :, ("brent_contract_code", "brent_trade_date", "brent_candle_payload_sha256")
    ].copy()
    matrix_keys["brent_trade_date"] = pd.to_datetime(
        matrix_keys["brent_trade_date"], errors="raise"
    ).dt.strftime("%Y-%m-%d")
    matrix_keys["brent_contract_code"] = matrix_keys["brent_contract_code"].astype(str)
    expected_keys = candle_keys.rename(
        columns={
            "contract_code": "brent_contract_code",
            "trade_date": "brent_trade_date",
            "raw_payload_sha256": "brent_candle_payload_sha256",
        }
    )
    matrix_index = pd.MultiIndex.from_frame(matrix_keys)
    candle_index = pd.MultiIndex.from_frame(expected_keys)
    if (
        not matrix_index.isin(candle_index).all()
        or not candle_index.isin(matrix_index).all()
        or not matrix["brent_contract_code"].isin(universe["contract_code"]).all()
    ):
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A matrix candle provenance or explicit contract mismatch"
        )

    coverage_required = (
        "source_id",
        "eligible_identity_count",
        "eligible_covered_count",
        "eligible_missing_count",
        "validation_identity_count",
        "validation_covered_count",
        "validation_missing_count",
    )
    if len(coverage) != 1 or any(column not in coverage for column in coverage_required):
        raise Phase85BrentIncrementalValueError("Phase 8.4A coverage evidence malformed")
    row = coverage.iloc[0]
    if (
        row["source_id"] != "moex_brent_futures_daily"
        or int(row["eligible_identity_count"]) != EXPECTED_ELIGIBLE_IDENTITIES
        or int(row["eligible_covered_count"]) != EXPECTED_ELIGIBLE_IDENTITIES
        or int(row["eligible_missing_count"]) != 0
        or int(row["validation_identity_count"]) != EXPECTED_VALIDATION_IDENTITIES
        or int(row["validation_covered_count"]) != EXPECTED_VALIDATION_IDENTITIES
        or int(row["validation_missing_count"]) != 0
    ):
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A exact eligible or validation coverage mismatch"
        )

    expected_changed = matrix["brent_contract_code"].ne(
        matrix["brent_contract_code"].shift()
    )
    expected_changed.iloc[0] = False
    expected_previous = matrix["brent_contract_code"].shift()
    if (
        not matrix["brent_contract_changed"].eq(expected_changed).all()
        or not matrix["brent_previous_contract_code"]
        .fillna("")
        .eq(expected_previous.fillna(""))
        .all()
        or len(rolls) != int(expected_changed.sum())
        or (
            not rolls.empty
            and (
                "cross_contract_return_calculated" not in rolls
                or rolls["cross_contract_return_calculated"].astype(bool).any()
                or "target_or_future_information_used" not in rolls
                or rolls["target_or_future_information_used"].astype(bool).any()
            )
        )
    ):
        raise Phase85BrentIncrementalValueError(
            "Phase 8.4A roll integrity or cross-contract-return policy mismatch"
        )


def validate_experiment_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("experiment_identity", {})
    expected_identity = {
        "contract_id": CONTRACT_ID,
        "project": PROJECT,
        "phase": PHASE,
        "lane": LANE,
        "task_id": TASK_ID,
        "execution_mode": EXECUTION_MODE,
    }
    if any(identity.get(key) != value for key, value in expected_identity.items()):
        raise Phase85BrentIncrementalValueError("experiment contract identity mismatch")
    if contract.get("approved_branch") != APPROVED_BRANCH:
        raise Phase85BrentIncrementalValueError("approved branch mismatch")
    scope = contract.get("approved_file_scope", {})
    if (
        tuple(scope.get("create_only", ())) != APPROVED_FILES
        or scope.get("exact_file_count") != 6
        or scope.get("existing_files_to_modify") != []
        or scope.get("scope_widening_allowed") is not False
    ):
        raise Phase85BrentIncrementalValueError("approved file scope mismatch")
    if contract.get("upstream_sha256") != FROZEN_INPUT_SHA256:
        raise Phase85BrentIncrementalValueError(
            "contract thirteen-hash inventory mismatch"
        )
    if tuple(contract.get("declared_output_artifacts", ())) != DECLARED_OUTPUT_ARTIFACTS:
        raise Phase85BrentIncrementalValueError("contract artifact inventory mismatch")
    protocol = contract.get("frozen_protocol", {})
    if protocol.get("splitter") != {
        "type": "sklearn.model_selection.TimeSeriesSplit",
        **SPLITTER_CONSTRUCTOR,
    }:
        raise Phase85BrentIncrementalValueError("contract splitter mismatch")
    if protocol.get("estimator") != {
        "type": "sklearn.linear_model.LogisticRegression",
        **MODEL_CONSTRUCTOR,
    }:
        raise Phase85BrentIncrementalValueError("contract estimator mismatch")
    if contract.get("absolute_limits") != {
        key: {"comparator": comparator, "threshold": threshold}
        for key, (comparator, threshold) in ABSOLUTE_LIMITS.items()
    }:
        raise Phase85BrentIncrementalValueError("contract absolute limits mismatch")
    accepted = contract.get("accepted_phase8_4a_input", {})
    if (
        accepted.get("run_id") != ACCEPTED_PHASE84A_RUN_ID
        or accepted.get("source_git_commit_sha") != EXPECTED_SOURCE_COMMIT
        or accepted.get("source_id") != "moex_brent_futures_daily"
        or accepted.get("status") != "candidate_for_phase8_5"
        or accepted.get("eligible_identities") != EXPECTED_ELIGIBLE_IDENTITIES
        or accepted.get("validation_identities") != EXPECTED_VALIDATION_IDENTITIES
    ):
        raise Phase85BrentIncrementalValueError("contract Phase 8.4A identity mismatch")
    identity_contract = contract.get("identity_contract", {})
    if (
        identity_contract.get("eligible_identities") != EXPECTED_ELIGIBLE_IDENTITIES
        or identity_contract.get("validation_identities")
        != EXPECTED_VALIDATION_IDENTITIES
        or identity_contract.get("instrument") != "forts.usdrubf"
        or identity_contract.get("target_source") != "manual_phase_labels_v1"
        or tuple(identity_contract.get("class_order", ())) != CLASS_ORDER
        or tuple(identity_contract.get("identity_columns", ())) != IDENTITY_COLUMNS
    ):
        raise Phase85BrentIncrementalValueError("contract identity policy mismatch")
    m0_features = contract.get("m0_internal_features", {})
    if (
        tuple(m0_features.get("numeric", ())) != M0_NUMERIC_FEATURES
        or tuple(m0_features.get("categorical", ())) != M0_CATEGORICAL_FEATURES
    ):
        raise Phase85BrentIncrementalValueError("contract M0 feature inventory mismatch")
    if contract.get("brent_feature_definitions") != BRENT_FEATURE_DEFINITIONS:
        raise Phase85BrentIncrementalValueError(
            "contract Brent feature definitions mismatch"
        )
    inventory = contract.get("matrix_inventory", {})
    if (
        tuple(inventory)
        != (
            "E0_FROZEN_PHASE7_2_CONTROL",
            "E1_M0_PLUS_BRENT_FULL",
            "E2_M0_PLUS_BRENT_PRICE_ACTION",
            "E3_M0_PLUS_BRENT_ACTIVITY",
            "E4_BRENT_ONLY",
        )
        or inventory.get("E0_FROZEN_PHASE7_2_CONTROL", {}).get("refit") is not False
        or [
            name
            for name, value in inventory.items()
            if isinstance(value, Mapping) and value.get("acceptance_eligible")
        ]
        != ["E1_M0_PLUS_BRENT_FULL"]
    ):
        raise Phase85BrentIncrementalValueError("contract matrix inventory mismatch")
    forbidden_protocol = (
        "model_family_change_allowed",
        "hyperparameter_search_allowed",
        "threshold_optimization_allowed",
        "calibration_allowed",
        "label_change_allowed",
        "fold_change_allowed",
        "feature_selection_search_allowed",
        "target_resampling_allowed",
        "post_result_feature_modification_allowed",
    )
    if any(protocol.get(name) is not False for name in forbidden_protocol):
        raise Phase85BrentIncrementalValueError("contract forbidden protocol mismatch")
    if protocol.get("reused_from_phase") != "8.3_unchanged":
        raise Phase85BrentIncrementalValueError("Phase 8.3 protocol reuse mismatch")
    restrictions = contract.get("feature_restrictions", {})
    if (
        restrictions.get("exact_feature_count") != 6
        or tuple(restrictions.get("authorized_features", ())) != BRENT_FEATURES
        or restrictions.get("cross_contract_return_allowed") is not False
        or restrictions.get("target_day_candle_allowed") is not False
        or restrictions.get("ruonia_or_key_rate_feature_allowed") is not False
    ):
        raise Phase85BrentIncrementalValueError("contract feature restriction mismatch")
    if tuple(contract.get("required_cli_args", ())) != REQUIRED_CLI_ARGS:
        raise Phase85BrentIncrementalValueError("contract CLI mismatch")
    artifact_policy = contract.get("artifact_policy", {})
    if (
        artifact_policy.get("exact_count") != 12
        or artifact_policy.get("undeclared_artifact_allowed") is not False
        or artifact_policy.get("output_outside_explicit_output_dir_allowed") is not False
        or artifact_policy.get("preexisting_output_directory_allowed") is not False
        or artifact_policy.get("model_file_allowed") is not False
        or artifact_policy.get("runtime_artifact_commit_allowed") is not False
    ):
        raise Phase85BrentIncrementalValueError("contract artifact policy mismatch")
    authority = contract.get("authority_boundary", {})
    forbidden_authority = (
        "direct_main_write_allowed",
        "server_apply_allowed",
        "real_runtime_allowed",
        "real_model_fit_allowed",
        "real_model_evaluation_allowed",
        "model_serialization_allowed",
        "model_promotion_allowed",
        "strategy_promotion_allowed",
        "live_prediction_allowed",
        "broker_action_allowed",
        "trading_allowed",
    )
    if any(authority.get(name) is not False for name in forbidden_authority):
        raise Phase85BrentIncrementalValueError("contract authority mismatch")


def build_chronological_folds(
    eligible: pd.DataFrame,
) -> list[tuple[np.ndarray, np.ndarray]]:
    try:
        folds = [
            (train.copy(), valid.copy())
            for train, valid in TimeSeriesSplit(**SPLITTER_CONSTRUCTOR).split(eligible)
        ]
    except ValueError as exc:
        raise Phase85BrentIncrementalValueError(
            "eligible rows cannot satisfy frozen folds"
        ) from exc
    seen: set[int] = set()
    parsed_dates = pd.to_datetime(eligible["target_trade_date"], errors="raise")
    for fold_id, (train, valid) in enumerate(folds, 1):
        if len(valid) != 64 or seen.intersection(valid.tolist()):
            raise Phase85BrentIncrementalValueError("validation fold identity mismatch")
        seen.update(int(index) for index in valid)
        if set(eligible.iloc[train]["target_phase_label"].astype(str)) != set(CLASS_ORDER):
            raise Phase85BrentIncrementalValueError(
                f"fold {fold_id} training data lacks B S or OUT"
            )
        if parsed_dates.iloc[train].max() >= parsed_dates.iloc[valid].min():
            raise Phase85BrentIncrementalValueError("fold violates chronology")
    if len(folds) != 5 or len(seen) != EXPECTED_VALIDATION_IDENTITIES:
        raise Phase85BrentIncrementalValueError(
            "frozen fold protocol must contain 320 validation identities"
        )
    return folds


def _expected_validation_identities(
    eligible: pd.DataFrame, folds: list[tuple[np.ndarray, np.ndarray]]
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for fold_id, (_, valid) in enumerate(folds, 1):
        frame = eligible.iloc[valid][
            [*IDENTITY_COLUMNS, "target_phase_label"]
        ].copy()
        frame.insert(0, "fold_id", fold_id)
        frames.append(frame.rename(columns={"target_phase_label": "y_true"}))
    return pd.concat(frames, ignore_index=True)


def validate_m0_predictions(
    predictions: pd.DataFrame, *, expected_fold_identities: pd.DataFrame
) -> pd.DataFrame:
    missing = [column for column in M0_REQUIRED_COLUMNS if column not in predictions.columns]
    if missing:
        raise Phase85BrentIncrementalValueError(
            "M0 predictions missing: " + ", ".join(missing)
        )
    observed = predictions.loc[:, M0_REQUIRED_COLUMNS].copy().reset_index(drop=True)
    dates = pd.to_datetime(observed["target_trade_date"], errors="coerce")
    if dates.isna().any():
        raise Phase85BrentIncrementalValueError(
            "M0 target_trade_date contains invalid values"
        )
    observed["target_trade_date"] = dates.dt.strftime("%Y-%m-%d")
    expected = expected_fold_identities.reset_index(drop=True)
    if len(observed) != EXPECTED_VALIDATION_IDENTITIES or not observed.loc[
        :, expected.columns
    ].equals(expected):
        raise Phase85BrentIncrementalValueError(
            "M0 ordered fold identities or labels differ from frozen basis"
        )
    probabilities = observed.loc[:, PROBABILITY_COLUMNS].to_numpy(float)
    if (
        not np.isfinite(probabilities).all()
        or (probabilities < 0).any()
        or not np.allclose(probabilities.sum(axis=1), 1.0)
    ):
        raise Phase85BrentIncrementalValueError(
            "M0 probabilities must be finite and sum to one"
        )
    if not observed["candidate_y_pred"].isin(CLASS_ORDER).all():
        raise Phase85BrentIncrementalValueError("M0 prediction class mismatch")
    return observed


def build_candidate_pipeline(
    numeric_features: tuple[str, ...], categorical_features: tuple[str, ...]
) -> Pipeline:
    transformers: list[tuple[str, Pipeline, list[str]]] = []
    if numeric_features:
        transformers.append(
            (
                "numeric",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                list(numeric_features),
            )
        )
    if categorical_features:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                list(categorical_features),
            )
        )
    return Pipeline(
        [
            (
                "preprocessor",
                ColumnTransformer(transformers=transformers, remainder="drop"),
            ),
            ("classifier", LogisticRegression(**MODEL_CONSTRUCTOR)),
        ]
    )


def _align_probabilities(raw: np.ndarray, observed_classes: Iterable[str]) -> np.ndarray:
    classes = tuple(str(item) for item in observed_classes)
    if set(classes) != set(CLASS_ORDER):
        raise Phase85BrentIncrementalValueError(
            "fitted class set differs from B S OUT"
        )
    positions = {label: index for index, label in enumerate(classes)}
    aligned = np.column_stack([raw[:, positions[label]] for label in CLASS_ORDER])
    if not np.isfinite(aligned).all() or not np.allclose(aligned.sum(axis=1), 1.0):
        raise Phase85BrentIncrementalValueError(
            "candidate probabilities must be finite and sum to one"
        )
    return aligned


def calculate_metrics(
    y_true: np.ndarray, y_pred: np.ndarray, probabilities: np.ndarray
) -> dict[str, Any]:
    matrix = confusion_matrix(y_true, y_pred, labels=list(CLASS_ORDER))
    support = matrix.sum(axis=1)
    predicted = matrix.sum(axis=0)
    precision: dict[str, float] = {}
    recall: dict[str, float] = {}
    f1: dict[str, float] = {}
    for index, label in enumerate(CLASS_ORDER):
        tp = int(matrix[index, index])
        precision[label] = tp / int(predicted[index]) if int(predicted[index]) else 0.0
        recall[label] = tp / int(support[index]) if int(support[index]) else 0.0
        denominator = precision[label] + recall[label]
        f1[label] = (
            2 * precision[label] * recall[label] / denominator if denominator else 0.0
        )
    incorrect = y_true != y_pred
    confidence = probabilities.max(axis=1)
    target_index = np.asarray([CLASS_ORDER.index(str(label)) for label in y_true])
    return {
        "validation_rows": int(len(y_true)),
        "accuracy": float(np.mean(y_true == y_pred)),
        "balanced_accuracy": float(np.mean(list(recall.values()))),
        "macro_f1": float(np.mean(list(f1.values()))),
        "weighted_f1": float(
            sum(f1[label] * support[index] for index, label in enumerate(CLASS_ORDER))
            / len(y_true)
        ),
        "multiclass_log_loss": float(
            -np.mean(
                np.log(
                    np.clip(
                        probabilities[np.arange(len(y_true)), target_index],
                        np.finfo(float).eps,
                        1.0,
                    )
                )
            )
        ),
        "B_recall": recall["B"],
        "S_to_OUT_rate": (
            float(matrix[1, 2] / support[1]) if support[1] else float("nan")
        ),
        "OUT_to_S_rate": (
            float(matrix[2, 1] / support[2]) if support[2] else float("nan")
        ),
        "mean_confidence_on_incorrect_predictions": (
            float(np.mean(confidence[incorrect])) if incorrect.any() else float("nan")
        ),
        "zero_B_recall": bool(recall["B"] == 0.0),
        "per_class_precision": precision,
        "per_class_recall": recall,
        "per_class_f1": f1,
        "per_class_support": {
            label: int(support[index]) for index, label in enumerate(CLASS_ORDER)
        },
        "confusion_matrix": matrix.astype(int).tolist(),
    }


def confidence_bucket(predictions: pd.DataFrame) -> dict[str, Any]:
    probabilities = predictions.loc[:, PROBABILITY_COLUMNS].to_numpy(float)
    confidence = probabilities.max(axis=1)
    mask = (confidence >= 0.90) & (confidence <= 1.00)
    if not mask.any():
        return {
            "bucket_count": 0,
            "bucket_accuracy": None,
            "bucket_mean_confidence": None,
            "bucket_gap": None,
            "status": "undefined_empty",
        }
    correct = (
        predictions["candidate_y_pred"].astype(str).to_numpy()
        == predictions["y_true"].astype(str).to_numpy()
    )
    accuracy = float(np.mean(correct[mask]))
    mean_confidence = float(np.mean(confidence[mask]))
    return {
        "bucket_count": int(mask.sum()),
        "bucket_accuracy": accuracy,
        "bucket_mean_confidence": mean_confidence,
        "bucket_gap": mean_confidence - accuracy,
        "status": "defined",
    }


def brent_feature_smd(train: pd.Series, validation: pd.Series) -> dict[str, float]:
    train_values = pd.to_numeric(train, errors="coerce").to_numpy(float)
    validation_values = pd.to_numeric(validation, errors="coerce").to_numpy(float)
    train_values = train_values[np.isfinite(train_values)]
    validation_values = validation_values[np.isfinite(validation_values)]
    if len(train_values) < 2 or len(validation_values) < 1:
        raise Phase85BrentIncrementalValueError(
            "Brent feature SMD has insufficient finite values"
        )
    standard_deviation = float(np.std(train_values, ddof=1))
    if not np.isfinite(standard_deviation) or standard_deviation <= 0:
        raise Phase85BrentIncrementalValueError(
            "Brent feature is constant in a training fold"
        )
    train_mean = float(np.mean(train_values))
    validation_mean = float(np.mean(validation_values))
    return {
        "train_finite_count": int(len(train_values)),
        "validation_finite_count": int(len(validation_values)),
        "train_mean": train_mean,
        "validation_mean": validation_mean,
        "train_standard_deviation": standard_deviation,
        "smd": abs(validation_mean - train_mean) / standard_deviation,
    }


def _per_class_rows(
    matrix_id: str,
    fold_id: int | str,
    metrics: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "matrix_id": matrix_id,
            "fold_id": fold_id,
            "class_label": label,
            "precision": metrics["per_class_precision"][label],
            "recall": metrics["per_class_recall"][label],
            "f1": metrics["per_class_f1"][label],
            "support": metrics["per_class_support"][label],
        }
        for label in CLASS_ORDER
    ]


def _aggregate_metrics(
    predictions: pd.DataFrame, fold_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    aggregate = calculate_metrics(
        predictions["y_true"].to_numpy(str),
        predictions["candidate_y_pred"].to_numpy(str),
        predictions.loc[:, PROBABILITY_COLUMNS].to_numpy(float),
    )
    fold_macro = np.asarray([row["macro_f1"] for row in fold_rows], dtype=float)
    aggregate.update(
        {
            "fold_macro_f1_range": float(fold_macro.max() - fold_macro.min()),
            "fold_macro_f1_population_standard_deviation": float(
                np.std(fold_macro, ddof=0)
            ),
            "minimum_fold_macro_f1": float(fold_macro.min()),
            "zero_B_recall_fold_count": int(
                sum(bool(row["zero_B_recall"]) for row in fold_rows)
            ),
            "confidence_bucket": confidence_bucket(predictions),
        }
    )
    return aggregate


def _evaluate_candidate_matrix(
    matrix_id: str,
    matrix: pd.DataFrame,
    eligible: pd.DataFrame,
    folds: list[tuple[np.ndarray, np.ndarray]],
) -> tuple[
    list[dict[str, Any]],
    pd.DataFrame,
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    numeric = MATRIX_NUMERIC_FEATURES[matrix_id]
    categorical = MATRIX_CATEGORICAL_FEATURES[matrix_id]
    feature_columns = (*numeric, *categorical)
    fold_rows: list[dict[str, Any]] = []
    prediction_frames: list[pd.DataFrame] = []
    null_rows: list[dict[str, Any]] = []
    per_class_rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []
    for fold_id, (train_index, valid_index) in enumerate(folds, 1):
        train = matrix.iloc[train_index]
        valid = matrix.iloc[valid_index]
        for split, frame in (("train", train), ("validation", valid)):
            for feature in feature_columns:
                null_count = int(frame[feature].isna().sum())
                null_rows.append(
                    {
                        "matrix_id": matrix_id,
                        "fold_id": fold_id,
                        "split": split,
                        "feature": feature,
                        "row_count": len(frame),
                        "null_count": null_count,
                        "null_rate": null_count / len(frame),
                        "all_null": bool(frame[feature].isna().all()),
                    }
                )
                if null_count:
                    raise Phase85BrentIncrementalValueError(
                        f"null value entered {matrix_id} fold {fold_id}"
                    )
        pipeline = build_candidate_pipeline(numeric, categorical)
        y_train = eligible.iloc[train_index]["target_phase_label"].astype(str)
        y_valid = eligible.iloc[valid_index]["target_phase_label"].astype(str).to_numpy()
        pipeline.fit(train.loc[:, feature_columns], y_train)
        predicted = pipeline.predict(valid.loc[:, feature_columns]).astype(str)
        classifier = pipeline.named_steps["classifier"]
        probabilities = _align_probabilities(
            pipeline.predict_proba(valid.loc[:, feature_columns]), classifier.classes_
        )
        metrics = calculate_metrics(y_valid, predicted, probabilities)
        row = {
            "matrix_id": matrix_id,
            "matrix_role": MATRIX_ROLES[matrix_id],
            "fold_id": fold_id,
            **metrics,
            "acceptance_eligible": matrix_id == "E1_M0_PLUS_BRENT_FULL",
        }
        fold_rows.append(row)
        per_class_rows.extend(_per_class_rows(matrix_id, fold_id, metrics))
        identity = eligible.iloc[valid_index][list(IDENTITY_COLUMNS)].reset_index(drop=True)
        identity.insert(0, "fold_id", fold_id)
        identity.insert(0, "matrix_id", matrix_id)
        identity["y_true"] = y_valid
        identity["candidate_y_pred"] = predicted
        for column_index, column in enumerate(PROBABILITY_COLUMNS):
            identity[column] = probabilities[:, column_index]
        prediction_frames.append(identity)
        if matrix_id == "E1_M0_PLUS_BRENT_FULL":
            transformed_names = pipeline.named_steps[
                "preprocessor"
            ].get_feature_names_out()
            positions = {
                name.split("__", 1)[-1]: index
                for index, name in enumerate(transformed_names)
            }
            class_positions = {
                str(label): index for index, label in enumerate(classifier.classes_)
            }
            for feature in BRENT_FEATURES:
                if feature not in positions:
                    raise Phase85BrentIncrementalValueError(
                        "E1 Brent coefficient mapping failed"
                    )
                for label in CLASS_ORDER:
                    coefficient_rows.append(
                        {
                            "matrix_id": matrix_id,
                            "fold_id": fold_id,
                            "class_label": label,
                            "brent_feature": feature,
                            "standardized_coefficient": float(
                                classifier.coef_[class_positions[label], positions[feature]]
                            ),
                        }
                    )
    predictions = pd.concat(prediction_frames, ignore_index=True)
    aggregate = _aggregate_metrics(predictions, fold_rows)
    per_class_rows.extend(_per_class_rows(matrix_id, "aggregate", aggregate))
    return (
        fold_rows,
        predictions,
        aggregate,
        null_rows,
        per_class_rows,
        coefficient_rows,
    )


def _evaluate_m0(
    m0: pd.DataFrame,
) -> tuple[list[dict[str, Any]], pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    fold_rows: list[dict[str, Any]] = []
    per_class_rows: list[dict[str, Any]] = []
    for fold_id, group in m0.groupby("fold_id", sort=True):
        metrics = calculate_metrics(
            group["y_true"].to_numpy(str),
            group["candidate_y_pred"].to_numpy(str),
            group.loc[:, PROBABILITY_COLUMNS].to_numpy(float),
        )
        fold_rows.append(
            {
                "matrix_id": "E0_FROZEN_PHASE7_2_CONTROL",
                "matrix_role": MATRIX_ROLES["E0_FROZEN_PHASE7_2_CONTROL"],
                "fold_id": int(fold_id),
                **metrics,
                "acceptance_eligible": False,
            }
        )
        per_class_rows.extend(
            _per_class_rows("E0_FROZEN_PHASE7_2_CONTROL", int(fold_id), metrics)
        )
    aggregate = _aggregate_metrics(m0, fold_rows)
    per_class_rows.extend(
        _per_class_rows("E0_FROZEN_PHASE7_2_CONTROL", "aggregate", aggregate)
    )
    predictions = m0.copy()
    predictions.insert(0, "matrix_id", "E0_FROZEN_PHASE7_2_CONTROL")
    return fold_rows, predictions, aggregate, per_class_rows


def _brent_shift_rows(
    e1: pd.DataFrame, folds: list[tuple[np.ndarray, np.ndarray]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for fold_id, (train, valid) in enumerate(folds, 1):
        for feature in BRENT_FEATURES:
            rows.append(
                {
                    "matrix_id": "E1_M0_PLUS_BRENT_FULL",
                    "fold_id": fold_id,
                    "brent_feature": feature,
                    **brent_feature_smd(e1.iloc[train][feature], e1.iloc[valid][feature]),
                    "status": "defined",
                }
            )
    return rows


def _coefficient_sign_consistency(rows: list[dict[str, Any]]) -> None:
    frame = pd.DataFrame(rows)
    for (feature, label), group in frame.groupby(
        ["brent_feature", "class_label"], sort=False
    ):
        coefficients = group["standardized_coefficient"].to_numpy(float)
        nonnegative = bool((coefficients >= 0).all())
        nonpositive = bool((coefficients <= 0).all())
        status = "consistent" if nonnegative or nonpositive else "mixed"
        for row in rows:
            if row["brent_feature"] == feature and row["class_label"] == label:
                row["coefficient_sign_consistency"] = status


def _scalar_metric_names() -> tuple[str, ...]:
    return (
        "accuracy",
        "balanced_accuracy",
        "macro_f1",
        "weighted_f1",
        "multiclass_log_loss",
        "B_recall",
        "S_to_OUT_rate",
        "OUT_to_S_rate",
        "mean_confidence_on_incorrect_predictions",
    )


def _aggregate_delta_metric_names() -> tuple[str, ...]:
    return (
        *_scalar_metric_names(),
        "fold_macro_f1_range",
        "fold_macro_f1_population_standard_deviation",
        "minimum_fold_macro_f1",
        "zero_B_recall_fold_count",
        "confidence_bucket_count",
        "confidence_bucket_accuracy",
        "confidence_bucket_mean_confidence",
        "confidence_bucket_gap",
    )


def _aggregate_metric_value(payload: Mapping[str, Any], metric: str) -> float:
    if metric.startswith("confidence_bucket_"):
        bucket_key = metric.removeprefix("confidence_bucket_")
        value = payload["confidence_bucket"]["bucket_" + bucket_key]
    else:
        value = payload[metric]
    return float(value) if value is not None else float("nan")


def _ablation_rows(
    aggregates: Mapping[str, Mapping[str, Any]], fold_metrics: pd.DataFrame
) -> list[dict[str, Any]]:
    e1_id = "E1_M0_PLUS_BRENT_FULL"
    rows: list[dict[str, Any]] = []
    for comparison in (
        "E0_FROZEN_PHASE7_2_CONTROL",
        "E2_M0_PLUS_BRENT_PRICE_ACTION",
        "E3_M0_PLUS_BRENT_ACTIVITY",
        "E4_BRENT_ONLY",
    ):
        for metric in _aggregate_delta_metric_names():
            candidate_value = _aggregate_metric_value(aggregates[e1_id], metric)
            comparison_value = _aggregate_metric_value(aggregates[comparison], metric)
            rows.append(
                {
                    "scope": "aggregate",
                    "fold_id": None,
                    "candidate_matrix_id": e1_id,
                    "comparison_matrix_id": comparison,
                    "metric": metric,
                    "candidate_value": candidate_value,
                    "comparison_value": comparison_value,
                    "E1_minus_comparison": candidate_value - comparison_value,
                }
            )
    e1_folds = fold_metrics.loc[fold_metrics["matrix_id"].eq(e1_id)].set_index(
        "fold_id"
    )
    e0_folds = fold_metrics.loc[
        fold_metrics["matrix_id"].eq("E0_FROZEN_PHASE7_2_CONTROL")
    ].set_index("fold_id")
    for fold_id in range(1, 6):
        for metric in _scalar_metric_names():
            rows.append(
                {
                    "scope": "fold",
                    "fold_id": fold_id,
                    "candidate_matrix_id": e1_id,
                    "comparison_matrix_id": "E0_FROZEN_PHASE7_2_CONTROL",
                    "metric": metric,
                    "candidate_value": e1_folds.loc[fold_id, metric],
                    "comparison_value": e0_folds.loc[fold_id, metric],
                    "E1_minus_comparison": (
                        e1_folds.loc[fold_id, metric] - e0_folds.loc[fold_id, metric]
                    ),
                }
            )
    return rows


def _limit_passes(value: Any, comparator: str, threshold: float) -> bool:
    try:
        observed = float(value)
    except (TypeError, ValueError):
        return False
    if not np.isfinite(observed):
        return False
    if comparator == ">=":
        return observed >= threshold
    if comparator == "<=":
        return observed <= threshold
    if comparator == "<":
        return observed < threshold
    raise Phase85BrentIncrementalValueError("unknown fixed comparator")


def evaluate_gates(
    aggregates: Mapping[str, Mapping[str, Any]],
    fold_metrics: pd.DataFrame,
    *,
    immutable_hashes_verified: bool,
    phase84a_source_verified: bool,
    identity_verified: bool,
    feature_integrity_verified: bool,
    protocol_verified: bool,
    distribution_verified: bool,
    scope_verified: bool = True,
) -> dict[str, Any]:
    e0_id = "E0_FROZEN_PHASE7_2_CONTROL"
    e1_id = "E1_M0_PLUS_BRENT_FULL"
    e0 = aggregates[e0_id]
    e1 = aggregates[e1_id]
    gates: dict[str, dict[str, Any]] = {}
    gates["G1_immutable_inputs"] = {
        "passed": bool(immutable_hashes_verified and phase84a_source_verified),
        "all_thirteen_hashes_match": bool(immutable_hashes_verified),
        "accepted_phase84a_source_verified": bool(phase84a_source_verified),
    }
    gates["G2_identities_and_folds"] = {
        "passed": bool(identity_verified),
        "eligible_identities": EXPECTED_ELIGIBLE_IDENTITIES,
        "validation_identities": EXPECTED_VALIDATION_IDENTITIES,
        "folds": 5,
        "validation_rows_per_fold": 64,
    }
    gates["G3_brent_feature_integrity"] = {
        "passed": bool(feature_integrity_verified),
        "brent_features": list(BRENT_FEATURES),
    }
    gates["G4_frozen_experiment_protocol"] = {
        "passed": bool(protocol_verified),
        "splitter": {"type": "TimeSeriesSplit", **SPLITTER_CONSTRUCTOR},
        "estimator": {"type": "LogisticRegression", **MODEL_CONSTRUCTOR},
        "E0_refitted": False,
    }
    absolute_checks = {
        metric: {
            "observed": e1[metric],
            "comparator": comparator,
            "threshold": threshold,
            "passed": _limit_passes(e1[metric], comparator, threshold),
        }
        for metric, (comparator, threshold) in ABSOLUTE_LIMITS.items()
    }
    gates["G5_absolute_performance"] = {
        "passed": all(item["passed"] for item in absolute_checks.values()),
        "checks": absolute_checks,
    }
    g6_loss = e1["multiclass_log_loss"] <= e0["multiclass_log_loss"] - 0.01
    g6_macro = e1["macro_f1"] >= e0["macro_f1"] - 0.01
    gates["G6_probability_improvement_versus_E0"] = {
        "passed": bool(g6_loss and g6_macro),
        "E1_multiclass_log_loss": e1["multiclass_log_loss"],
        "E0_multiclass_log_loss": e0["multiclass_log_loss"],
        "E1_macro_f1": e1["macro_f1"],
        "E0_macro_f1": e0["macro_f1"],
    }
    confusion_defined = all(
        np.isfinite(float(e0[metric])) and float(e0[metric]) > 0
        for metric in ("S_to_OUT_rate", "OUT_to_S_rate")
    ) and all(
        np.isfinite(float(e1[metric]))
        for metric in ("S_to_OUT_rate", "OUT_to_S_rate")
    )
    relative = (
        {
            metric: (e0[metric] - e1[metric]) / e0[metric]
            for metric in ("S_to_OUT_rate", "OUT_to_S_rate")
        }
        if confusion_defined
        else {}
    )
    g7 = bool(
        confusion_defined
        and any(value >= 0.05 for value in relative.values())
        and all(
            e1[metric] - e0[metric] < 0.03
            for metric in ("S_to_OUT_rate", "OUT_to_S_rate")
        )
    )
    gates["G7_S_versus_OUT_objective"] = {
        "passed": g7,
        "relative_improvements": relative,
        "denominators_defined": confusion_defined,
    }
    e0_bucket = e0["confidence_bucket"]
    e1_bucket = e1["confidence_bucket"]
    e0_gap = e0_bucket.get("bucket_gap")
    e1_gap = e1_bucket.get("bucket_gap")
    bucket_defined = bool(
        e0_bucket.get("bucket_count", 0) > 0
        and e0_gap is not None
        and np.isfinite(e0_gap)
        and e0_gap > 0
        and e1_bucket.get("bucket_count", 0) > 0
        and e1_gap is not None
        and np.isfinite(e1_gap)
    )
    bucket_limit = (
        float(np.nextafter(0.70 * e0_gap, np.inf)) if bucket_defined else None
    )
    gates["G8_high_confidence_calibration"] = {
        "passed": bool(bucket_defined and e1_gap <= bucket_limit),
        "E0_bucket": e0_bucket,
        "E1_bucket": e1_bucket,
        "E1_bucket_gap_limit": bucket_limit,
    }
    e1_folds = fold_metrics.loc[fold_metrics["matrix_id"].eq(e1_id)].sort_values(
        "fold_id"
    )
    e0_folds = fold_metrics.loc[fold_metrics["matrix_id"].eq(e0_id)].sort_values(
        "fold_id"
    )
    deltas = e1_folds["macro_f1"].to_numpy(float) - e0_folds[
        "macro_f1"
    ].to_numpy(float)
    improved = int(np.sum(deltas >= 0.01))
    degraded = int(np.sum(deltas < -0.03))
    stability = all(
        absolute_checks[metric]["passed"]
        for metric in (
            "fold_macro_f1_range",
            "fold_macro_f1_population_standard_deviation",
            "minimum_fold_macro_f1",
        )
    )
    gates["G9_fold_breadth"] = {
        "passed": bool(improved >= 3 and degraded <= 1 and stability),
        "improved_fold_count": improved,
        "degraded_fold_count": degraded,
        "macro_f1_deltas": deltas.tolist(),
        "absolute_stability_passed": stability,
    }
    gates["G10_distribution_integrity"] = {
        "passed": bool(distribution_verified),
        "all_brent_feature_SMD_defined": bool(distribution_verified),
    }
    gates["G11_leakage_and_scope"] = {
        "passed": bool(scope_verified),
        "target_or_probability_feature_used": False,
        "network_access_performed": False,
        "phase84a_artifact_modified": False,
        "threshold_or_fold_changed": False,
        "model_file_created": False,
        "production_prediction_performed": False,
        "output_outside_explicit_directory": False,
        "model_serialized": False,
        "promotion_performed": False,
    }
    prior = [f"G{i}" for i in range(1, 12)]
    normalized = {
        str(name).split("_", 1)[0]: payload for name, payload in gates.items()
    }
    final_passed = all(bool(normalized[name]["passed"]) for name in prior)
    failed = [name for name in prior if not bool(normalized[name]["passed"])]
    dimensions: list[str] = []
    if "G1" in failed:
        dimensions.append("immutable_inputs_or_phase84a_source")
    if "G2" in failed:
        dimensions.append("identity_or_fold_integrity")
    if "G3" in failed:
        dimensions.append("brent_feature_integrity")
    if "G4" in failed:
        dimensions.append("frozen_protocol")
    if "G5" in failed:
        dimensions.extend(
            f"absolute_{metric}"
            for metric, payload in absolute_checks.items()
            if not payload["passed"]
        )
    if "G6" in failed:
        dimensions.append("probability_quality")
    if "G7" in failed:
        dimensions.append("S_OUT_confusion")
    if "G8" in failed:
        dimensions.append("high_confidence_calibration")
    if "G9" in failed:
        dimensions.append("fold_breadth")
    if "G10" in failed:
        dimensions.append("source_shift")
    if "G11" in failed:
        dimensions.append("leakage_or_scope")
    status = (
        "brent_incremental_value_supported"
        if final_passed
        else "brent_incremental_value_not_supported"
    )
    gates["G12_final_acceptance"] = {
        "passed": final_passed,
        "requires": prior,
        "status": status,
        "failed_gates": failed,
        "failure_dimensions": dimensions,
        "production_or_strategy_promotion_authorized": False,
    }
    return gates


def _feature_inventory_payload() -> dict[str, Any]:
    return {
        "E0_FROZEN_PHASE7_2_CONTROL": {
            "role": MATRIX_ROLES["E0_FROZEN_PHASE7_2_CONTROL"],
            "refit": False,
            "acceptance_eligible": False,
            "numeric_features": list(M0_NUMERIC_FEATURES),
            "categorical_features": list(M0_CATEGORICAL_FEATURES),
        },
        **{
            matrix_id: {
                "role": MATRIX_ROLES[matrix_id],
                "acceptance_eligible": matrix_id == "E1_M0_PLUS_BRENT_FULL",
                "numeric_features": list(MATRIX_NUMERIC_FEATURES[matrix_id]),
                "categorical_features": list(MATRIX_CATEGORICAL_FEATURES[matrix_id]),
            }
            for matrix_id in MATRIX_NUMERIC_FEATURES
        },
    }


def _recommendation(gates: Mapping[str, Any], aggregates: Mapping[str, Any]) -> str:
    if gates["G12_final_acceptance"]["passed"]:
        return "brent_supported_for_separate_follow_on_authorization"
    if not gates["G6_probability_improvement_versus_E0"]["passed"]:
        return "do_not_promote_brent_under_current_design"
    e2 = aggregates["E2_M0_PLUS_BRENT_PRICE_ACTION"]["macro_f1"]
    e3 = aggregates["E3_M0_PLUS_BRENT_ACTIVITY"]["macro_f1"]
    if e2 > e3:
        return "retain_brent_price_action_for_diagnostic_record_only"
    if e3 > e2:
        return "retain_brent_activity_for_diagnostic_record_only"
    return "do_not_promote_brent_under_current_design"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, float) and not np.isfinite(value):
        return None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def _write_exact_artifacts(output_dir: Path, payloads: Mapping[str, object]) -> None:
    if tuple(payloads) != DECLARED_OUTPUT_ARTIFACTS:
        raise Phase85BrentIncrementalValueError(
            "undeclared runtime artifact inventory"
        )
    if output_dir.exists():
        raise Phase85BrentIncrementalValueError("output directory must not pre-exist")
    output_dir.mkdir(parents=True, exist_ok=False)
    for name in DECLARED_OUTPUT_ARTIFACTS:
        path = output_dir / name
        if path.parent != output_dir:
            raise Phase85BrentIncrementalValueError("write outside output directory refused")
        payload = payloads[name]
        if name.endswith(".json"):
            path.write_text(
                json.dumps(
                    _json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True
                )
                + "\n",
                encoding="utf-8",
            )
        elif name.endswith(".csv"):
            if not isinstance(payload, pd.DataFrame):
                raise Phase85BrentIncrementalValueError("CSV payload must be a DataFrame")
            payload.to_csv(path, index=False)
        elif name.endswith(".parquet"):
            if not isinstance(payload, pd.DataFrame):
                raise Phase85BrentIncrementalValueError(
                    "Parquet payload must be a DataFrame"
                )
            payload.to_parquet(path, index=False)
        else:  # pragma: no cover
            raise Phase85BrentIncrementalValueError("unsupported artifact suffix")
    observed = tuple(sorted(path.name for path in output_dir.iterdir() if path.is_file()))
    if observed != tuple(sorted(DECLARED_OUTPUT_ARTIFACTS)):
        raise Phase85BrentIncrementalValueError(
            "runtime artifact inventory differs from contract"
        )


def run_experiment(request: Phase85Request) -> Phase85Result:
    if request.output_dir.exists():
        raise Phase85BrentIncrementalValueError("output directory must not pre-exist")
    observed_hashes = verify_immutable_hashes(request)
    observed_contract_hash = verify_experiment_contract_hash(request)
    contract = _json(request.experiment_contract_path)
    validate_experiment_contract(contract)
    phase84a_input_identity = _json(request.phase84a_input_identity_path)
    route_validation = _json(request.official_route_validation_path)
    source_blockers = _json(request.source_blocker_register_path)
    phase84a_gates = _json(request.phase84a_gate_results_path)
    validate_phase84a_acceptance_identity(
        phase84a_input_identity,
        route_validation,
        source_blockers,
        phase84a_gates,
    )
    # Read all thirteen immutable inputs before feature construction or any fit.
    modeling_dataset = pd.read_parquet(request.modeling_dataset_path)
    m0_raw = pd.read_parquet(request.m0_validation_predictions_path)
    universe = pd.read_parquet(request.brent_contract_universe_path)
    candles = pd.read_parquet(request.brent_daily_candles_normalized_path)
    brent_matrix = pd.read_parquet(request.brent_pit_acceptance_matrix_path)
    rolls = pd.read_csv(request.contract_roll_diagnostics_path)
    coverage = pd.read_csv(request.coverage_by_source_path)
    _json(request.dataset_manifest_path)
    _json(request.feature_schema_path)
    try:
        eligible = prepare_eligible_modeling_rows(modeling_dataset)
        validate_phase84a_artifacts(
            universe=universe,
            candles=candles,
            matrix=brent_matrix,
            coverage=coverage,
            rolls=rolls,
            eligible=eligible,
        )
        built: BrentFeatureBuildResult = build_brent_feature_matrices(
            modeling_dataset, brent_matrix
        )
    except BrentIncrementalFeatureError as exc:
        raise Phase85BrentIncrementalValueError(str(exc)) from None
    folds = build_chronological_folds(built.eligible)
    expected_validation = _expected_validation_identities(built.eligible, folds)
    m0 = validate_m0_predictions(
        m0_raw, expected_fold_identities=expected_validation
    )
    shift_rows = _brent_shift_rows(
        built.matrices["E1_M0_PLUS_BRENT_FULL"], folds
    )
    all_fold_rows: list[dict[str, Any]] = []
    all_predictions: list[pd.DataFrame] = []
    all_null_rows: list[dict[str, Any]] = []
    all_per_class_rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []
    aggregates: dict[str, dict[str, Any]] = {}
    m0_folds, m0_predictions, m0_aggregate, m0_per_class = _evaluate_m0(m0)
    all_fold_rows.extend(m0_folds)
    all_predictions.append(m0_predictions)
    all_per_class_rows.extend(m0_per_class)
    aggregates["E0_FROZEN_PHASE7_2_CONTROL"] = m0_aggregate
    for matrix_id in MATRIX_NUMERIC_FEATURES:
        (
            fold_rows,
            predictions,
            aggregate,
            null_rows,
            per_class_rows,
            coefficients,
        ) = _evaluate_candidate_matrix(
            matrix_id, built.matrices[matrix_id], built.eligible, folds
        )
        all_fold_rows.extend(fold_rows)
        all_predictions.append(predictions)
        all_null_rows.extend(null_rows)
        all_per_class_rows.extend(per_class_rows)
        coefficient_rows.extend(coefficients)
        aggregates[matrix_id] = aggregate
    _coefficient_sign_consistency(coefficient_rows)
    fold_frame = pd.DataFrame(all_fold_rows)
    prediction_frame = pd.concat(all_predictions, ignore_index=True)
    gate_results = evaluate_gates(
        aggregates,
        fold_frame,
        immutable_hashes_verified=True,
        phase84a_source_verified=True,
        identity_verified=True,
        feature_integrity_verified=True,
        protocol_verified=True,
        distribution_verified=True,
        scope_verified=True,
    )
    gate_results["G12_final_acceptance"]["recommendation"] = _recommendation(
        gate_results, aggregates
    )
    input_verification = {
        "project": PROJECT,
        "phase": PHASE,
        "task_id": TASK_ID,
        "run_id": request.run_id,
        "source_git_commit_sha": request.git_commit_sha,
        "accepted_phase84a_run_id": ACCEPTED_PHASE84A_RUN_ID,
        "phase84a_source_git_commit_sha": EXPECTED_SOURCE_COMMIT,
        "phase84a_source_id": "moex_brent_futures_daily",
        "eligible_identity_count": len(built.eligible),
        "validation_identity_count": len(m0),
        "fold_count": len(folds),
        "validation_rows_per_fold": [len(valid) for _, valid in folds],
        "experiment_contract": {
            "expected_sha256": EXPECTED_EXPERIMENT_CONTRACT_SHA256,
            "observed_sha256": observed_contract_hash,
            "matches": observed_contract_hash
            == EXPECTED_EXPERIMENT_CONTRACT_SHA256,
        },
        "immutable_inputs": {
            name: {
                "expected_sha256": expected,
                "observed_sha256": observed_hashes[name],
                "matches": observed_hashes[name] == expected,
            }
            for name, expected in EXPECTED_INPUT_SHA256.items()
        },
    }
    aggregate_payload = {
        "project": PROJECT,
        "phase": PHASE,
        "matrices": aggregates,
        "final_status": gate_results["G12_final_acceptance"]["status"],
    }
    payloads: dict[str, object] = {
        "input_identity_verification.json": input_verification,
        "feature_matrix_inventory.json": _feature_inventory_payload(),
        "brent_feature_definitions.json": BRENT_FEATURE_DEFINITIONS,
        "feature_nullness_by_matrix_and_fold.csv": pd.DataFrame(all_null_rows),
        "brent_feature_shift_by_fold.csv": pd.DataFrame(shift_rows),
        "fold_metrics_by_matrix.csv": fold_frame,
        "aggregate_metrics_by_matrix.json": aggregate_payload,
        "per_class_metrics_by_matrix.csv": pd.DataFrame(all_per_class_rows),
        "validation_predictions_by_matrix.parquet": prediction_frame,
        "ablation_effects.csv": pd.DataFrame(
            _ablation_rows(aggregates, fold_frame)
        ),
        "brent_feature_coefficients_by_fold.csv": pd.DataFrame(coefficient_rows),
        "gate_results.json": gate_results,
    }
    _write_exact_artifacts(request.output_dir, payloads)
    return Phase85Result(
        output_dir=request.output_dir,
        artifact_names=DECLARED_OUTPUT_ARTIFACTS,
        eligible_identity_count=len(built.eligible),
        validation_identity_count=len(m0),
        fold_count=len(folds),
        final_status=gate_results["G12_final_acceptance"]["status"],
    )


def run_from_args(args: argparse.Namespace) -> Phase85Result:
    return run_experiment(request_from_args(args))


def main(argv: list[str] | None = None) -> int:
    run_from_args(build_argument_parser().parse_args(argv))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
