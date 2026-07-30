from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final

import pandas as pd

from moex_research.external_data.moex_cnyrubf_algopack_history import (
    ALGOPACK_BUCKET_MINUTES,
    ALGOPACK_HOST,
    ALGOPACK_TOKEN_ENV,
    ALGOPACK_TRADESTATS_ROUTE,
    ASSET_CODE,
    BOARD_ID,
    ENGINE,
    HISTORICAL_MODEL_USE_STATUS,
    MARKET,
    SECURITY_ID,
    SOURCE_ID,
    SOURCE_REVISION_STATUS,
    AlgoPackTransport,
    CnyrubfAlgoPackDailyCandle,
    CnyrubfAlgoPackError,
    CnyrubfSecurityIdentity,
    IssTransport,
    TokenLoader,
    UtcClock,
    build_security_metadata_url,
    fetch_algopack_bytes,
    fetch_iss_bytes,
    load_algopack_token,
    load_daily_history,
    load_security_identity,
    utc_now,
    validate_prior_session_candle,
)


PROJECT: Final[str] = "MOEX_Bot"
PHASE: Final[str] = "8.6A"
LANE: Final[str] = "ema_3_19_ai"
TASK_ID: Final[str] = "ema_3_19_ai_phase_8_6a_cnyrubf_fo_implementation_v1"
EXECUTION_MODE: Final[str] = "browser_controlled_github_route"
IMPLEMENTATION_BRANCH: Final[str] = (
    "research/ema-3-19-ai/phase-8-6a-cnyrubf-fo-implementation-v1"
)
CONTRACT_ID: Final[str] = (
    "usdrubf_phase8_6a_algopack_cnyrubf_fo_source_correction_v1"
)
CONTRACT_VERSION: Final[str] = "1.2"
CONTRACT_TASK_ID: Final[str] = (
    "ema_3_19_ai_phase_8_6a_cnyrubf_fo_source_correction_v1"
)
CONTRACT_BRANCH: Final[str] = (
    "research/ema-3-19-ai/phase-8-6a-cnyrubf-fo-source-correction-v1"
)

EXPECTED_IMMUTABLE_INPUT_DIGESTS: Final[dict[str, tuple[str, str]]] = {
    "modeling_dataset": (
        "sha256",
        "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00",
    ),
    "dataset_manifest": (
        "sha256",
        "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082",
    ),
    "feature_schema": (
        "sha256",
        "8f08802c7fb0a4cc43ab4ba072ee22ff9edd92fe8d674ea0515545d20d143238",
    ),
    "m0_validation_predictions": (
        "sha256",
        "9769d00a49adeb54c016d965387774e46a3e09e09f895aa61d48a90bbf3568cf",
    ),
    "phase83_aggregate_metrics": (
        "sha256",
        "d6ad4f6587dadb32431bd7b8f3bd59c5393e04d742efb4af459b316b417f8756",
    ),
    "phase83_gate_results": (
        "sha256",
        "d3f7e24022e550e725eae7ec5bc214d6b95e0e9c66393574c575e5d6f593f33c",
    ),
    "experiment_contract": (
        "git_blob_sha1",
        "8d7c0a8fcb50aa48d2c9f9c579e2c2ea62e01e35",
    ),
}
EXPECTED_INPUT_SHA256: Final[dict[str, str]] = {
    name: digest
    for name, (algorithm, digest) in EXPECTED_IMMUTABLE_INPUT_DIGESTS.items()
    if algorithm == "sha256"
}
EXPECTED_EXPERIMENT_CONTRACT_GIT_BLOB_SHA1: Final[str] = (
    EXPECTED_IMMUTABLE_INPUT_DIGESTS["experiment_contract"][1]
)
EXPECTED_ELIGIBLE_IDENTITIES: Final[int] = 472
EXPECTED_VALIDATION_IDENTITIES: Final[int] = 320
EXPECTED_FOLDS: Final[int] = 5
EXPECTED_VALIDATION_ROWS_PER_FOLD: Final[int] = 64
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"
TARGET_SOURCE: Final[str] = "manual_phase_labels_v1"
EXPECTED_FIRST_TARGET: Final[date] = date(2024, 8, 5)
EXPECTED_LAST_TARGET: Final[date] = date(2026, 6, 11)
EXPECTED_PHASE83_STATUS: Final[str] = (
    "external_factor_incremental_value_not_supported"
)
EXPECTED_PHASE83_RECOMMENDATION: Final[str] = (
    "prioritize_blocked_oil_and_liquidity_sources"
)
CLASS_ORDER: Final[tuple[str, ...]] = ("B", "S", "OUT")
IDENTITY_COLUMNS: Final[tuple[str, str]] = (
    "target_trade_date",
    "target_instrument_id",
)
FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "target_phase_label",
        "target_is_labeled",
        "target_source",
        "fold_id",
        "y_true",
        "candidate_y_pred",
        "prediction",
        "probability_B",
        "probability_S",
        "probability_OUT",
    }
)

DECLARED_OUTPUT_ARTIFACTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "official_route_validation.json",
    "cnyrubf_security_identity.json",
    "cnyrubf_daily_candles_normalized.parquet",
    "cnyrubf_pit_acceptance_matrix.parquet",
    "coverage_by_source.csv",
    "session_alignment_diagnostics.csv",
    "source_blocker_register.json",
    "gate_results.json",
)

NORMALIZED_SOURCE_COLUMNS: Final[tuple[str, ...]] = (
    "source_id",
    "security_id",
    "asset_code",
    "board_id",
    "engine",
    "market",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "volume_buy",
    "volume_sell",
    "volume_imbalance",
    "value",
    "value_buy",
    "value_sell",
    "trades",
    "trades_buy",
    "trades_sell",
    "initial_margin_close",
    "open_interest_open",
    "open_interest_high",
    "open_interest_low",
    "open_interest_close",
    "candle_begin",
    "candle_end",
    "source_available_at",
    "source_route",
    "retrieved_at_utc",
    "raw_payload_sha256",
    "source_revision_status",
    "historical_model_use_status",
)

ACCEPTANCE_MATRIX_COLUMNS: Final[tuple[str, ...]] = (
    "target_trade_date",
    "target_instrument_id",
    "prior_trade_date",
    "cnyrubf_security_id",
    "cnyrubf_asset_code",
    "cnyrubf_board_id",
    "cnyrubf_trade_date",
    "cnyrubf_open",
    "cnyrubf_high",
    "cnyrubf_low",
    "cnyrubf_close",
    "cnyrubf_volume",
    "cnyrubf_volume_buy",
    "cnyrubf_volume_sell",
    "cnyrubf_volume_imbalance",
    "cnyrubf_value",
    "cnyrubf_value_buy",
    "cnyrubf_value_sell",
    "cnyrubf_trades",
    "cnyrubf_trades_buy",
    "cnyrubf_trades_sell",
    "cnyrubf_initial_margin_close",
    "cnyrubf_open_interest_open",
    "cnyrubf_open_interest_high",
    "cnyrubf_open_interest_low",
    "cnyrubf_open_interest_close",
    "cnyrubf_candle_begin",
    "cnyrubf_candle_end",
    "cnyrubf_source_available_at",
    "cnyrubf_source_route",
    "cnyrubf_payload_sha256",
    "cnyrubf_retrieved_at_utc",
    "cnyrubf_source_revision_status",
)

DIAGNOSTIC_COLUMNS: Final[tuple[str, ...]] = (
    "target_trade_date",
    "prior_trade_date",
    "candidate_trade_date",
    "accepted",
    "reason",
    "blocker_classification",
    "same_day_or_future_used",
    "forward_fill_used",
    "backward_fill_used",
    "arbitrary_date_selection_used",
    "source_substitution_used",
    "target_derived_field_used",
)

BLOCKER_CLASSIFICATIONS: Final[tuple[str, ...]] = (
    "security_identity_not_reproducible",
    "token_env_not_configured",
    "algopack_authentication_failed",
    "algopack_subscription_not_entitled",
    "official_route_not_reproducible",
    "cnyrubf_not_available",
    "algopack_rate_limit_blocked",
    "algopack_tradestats_not_available",
    "algopack_schema_not_stable",
    "official_schema_not_stable",
    "point_in_time_cutoff_not_provable",
    "incomplete_identity_coverage",
    "numerical_or_chronology_integrity_failure",
    "provenance_not_sufficient",
    "target_derived_field_leakage",
    "other_fail_closed_with_exact_reason",
)

REQUIRED_CLI_ARGS: Final[tuple[str, ...]] = (
    "--modeling-dataset-path",
    "--dataset-manifest-path",
    "--feature-schema-path",
    "--m0-validation-predictions-path",
    "--phase8-3-aggregate-metrics-path",
    "--phase8-3-gate-results-path",
    "--experiment-contract-path",
    "--output-dir",
    "--run-id",
    "--git-commit-sha",
)

_RUN_ID: Final[re.Pattern[str]] = re.compile(
    r"^phase8_6a_algopack_cnyrubf_source_validation_[0-9]{8}_v[1-9][0-9]*$"
)
_SHA40: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")
_SHA256: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_ALIAS: Final[re.Pattern[str]] = re.compile(
    r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])",
    re.IGNORECASE,
)

IMPLEMENTATION_FILES: Final[tuple[str, ...]] = (
    "src/moex_research/external_data/moex_cnyrubf_algopack_history.py",
    "src/moex_research/runners/usdrubf_phase8_6a_algopack_cnyrubf_source_validation.py",
    "src/moex_research/runners/usdrubf_phase8_6a_algopack_cnyrubf_runtime.py",
    "tests/unit/test_usdrubf_phase8_6a_algopack_cnyrubf_history.py",
    "tests/unit/test_usdrubf_phase8_6a_algopack_cnyrubf_source_validation.py",
    "tests/contract/test_usdrubf_phase8_6a_algopack_cnyrubf_source_validation_contract.py",
)


class Phase86ACnyrubfSourceValidationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        blocker: str = "other_fail_closed_with_exact_reason",
    ) -> None:
        super().__init__(message)
        self.blocker = (
            blocker
            if blocker in BLOCKER_CLASSIFICATIONS
            else "other_fail_closed_with_exact_reason"
        )


@dataclass(frozen=True)
class Phase86ARequest:
    modeling_dataset_path: Path
    dataset_manifest_path: Path
    feature_schema_path: Path
    m0_validation_predictions_path: Path
    phase83_aggregate_metrics_path: Path
    phase83_gate_results_path: Path
    experiment_contract_path: Path
    output_dir: Path
    run_id: str
    git_commit_sha: str


@dataclass(frozen=True)
class Phase86AResult:
    output_dir: Path
    artifact_names: tuple[str, ...]
    eligible_identity_count: int
    validation_identity_count: int
    final_status: str
    blocker_classification: str | None


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python -m moex_research.runners."
            "usdrubf_phase8_6a_algopack_cnyrubf_source_validation"
        )
    )
    for flag in REQUIRED_CLI_ARGS:
        parser.add_argument(flag, required=True)
    return parser


def _file(value: object, flag: str, suffix: str) -> Path:
    text = str(value).strip()
    if not text or any(character in text for character in "*?[]") or _ALIAS.search(text):
        raise Phase86ACnyrubfSourceValidationError(
            f"{flag} must identify one immutable file"
        )
    path = Path(text)
    if path.suffix.lower() != suffix or not path.is_file():
        raise Phase86ACnyrubfSourceValidationError(
            f"{flag} file or suffix mismatch"
        )
    return path


def request_from_args(arguments: argparse.Namespace) -> Phase86ARequest:
    request = Phase86ARequest(
        modeling_dataset_path=_file(
            arguments.modeling_dataset_path,
            REQUIRED_CLI_ARGS[0],
            ".parquet",
        ),
        dataset_manifest_path=_file(
            arguments.dataset_manifest_path,
            REQUIRED_CLI_ARGS[1],
            ".json",
        ),
        feature_schema_path=_file(
            arguments.feature_schema_path,
            REQUIRED_CLI_ARGS[2],
            ".json",
        ),
        m0_validation_predictions_path=_file(
            arguments.m0_validation_predictions_path,
            REQUIRED_CLI_ARGS[3],
            ".parquet",
        ),
        phase83_aggregate_metrics_path=_file(
            getattr(arguments, "phase8_3_aggregate_metrics_path"),
            REQUIRED_CLI_ARGS[4],
            ".json",
        ),
        phase83_gate_results_path=_file(
            getattr(arguments, "phase8_3_gate_results_path"),
            REQUIRED_CLI_ARGS[5],
            ".json",
        ),
        experiment_contract_path=_file(
            arguments.experiment_contract_path,
            REQUIRED_CLI_ARGS[6],
            ".json",
        ),
        output_dir=Path(str(arguments.output_dir).strip()),
        run_id=str(arguments.run_id).strip(),
        git_commit_sha=str(arguments.git_commit_sha).strip().lower(),
    )
    _validate_request(request)
    return request


def _input_paths(request: Phase86ARequest) -> dict[str, Path]:
    return {
        "modeling_dataset": request.modeling_dataset_path,
        "dataset_manifest": request.dataset_manifest_path,
        "feature_schema": request.feature_schema_path,
        "m0_validation_predictions": request.m0_validation_predictions_path,
        "phase83_aggregate_metrics": request.phase83_aggregate_metrics_path,
        "phase83_gate_results": request.phase83_gate_results_path,
        "experiment_contract": request.experiment_contract_path,
    }


def _validate_request(request: Phase86ARequest) -> None:
    if len({path.resolve() for path in _input_paths(request).values()}) != 7:
        raise Phase86ACnyrubfSourceValidationError(
            "all input files must be distinct"
        )
    if not _RUN_ID.fullmatch(request.run_id):
        raise Phase86ACnyrubfSourceValidationError(
            "immutable CNYRUBF run id mismatch"
        )
    if not _SHA40.fullmatch(request.git_commit_sha):
        raise Phase86ACnyrubfSourceValidationError(
            "immutable git commit SHA mismatch"
        )
    text = str(request.output_dir)
    if (
        not text
        or any(character in text for character in "*?[]")
        or _ALIAS.search(text)
        or request.output_dir.exists()
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "--output-dir must be explicit and must not pre-exist"
        )


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_blob_sha1_bytes(payload: bytes) -> str:
    header = f"blob {len(payload)}\0".encode("ascii")
    return hashlib.sha1(header + payload, usedforsecurity=False).hexdigest()


def _git_blob_sha1(path: Path) -> str:
    return _git_blob_sha1_bytes(path.read_bytes())


def _immutable_input_digest(path: Path, algorithm: str) -> str:
    if algorithm == "sha256":
        return _hash(path)
    if algorithm == "git_blob_sha1":
        return _git_blob_sha1(path)
    raise Phase86ACnyrubfSourceValidationError(
        f"unsupported immutable input digest algorithm: {algorithm}"
    )


def _json_bytes(payload: bytes, name: str) -> dict[str, Any]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase86ACnyrubfSourceValidationError(
            f"invalid JSON: {name}"
        ) from exc
    if not isinstance(value, dict):
        raise Phase86ACnyrubfSourceValidationError(
            f"JSON must be object: {name}"
        )
    return value


def _json(path: Path) -> dict[str, Any]:
    return _json_bytes(path.read_bytes(), path.name)


def _verified_experiment_contract(
    path: Path,
) -> tuple[dict[str, Any], str]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise Phase86ACnyrubfSourceValidationError(
            f"cannot read immutable experiment contract: {path.name}"
        ) from exc
    observed = _git_blob_sha1_bytes(payload)
    expected = EXPECTED_IMMUTABLE_INPUT_DIGESTS["experiment_contract"][1]
    if observed != expected:
        raise Phase86ACnyrubfSourceValidationError(
            "immutable input hash mismatch: experiment_contract"
        )
    return _json_bytes(payload, path.name), observed


def verify_immutable_inputs(
    request: Phase86ARequest,
    *,
    experiment_contract_digest: str | None = None,
) -> dict[str, str]:
    paths = _input_paths(request)
    expected_names = set(EXPECTED_IMMUTABLE_INPUT_DIGESTS)
    if set(paths) != expected_names:
        missing = sorted(expected_names.difference(paths))
        unexpected = sorted(set(paths).difference(expected_names))
        raise Phase86ACnyrubfSourceValidationError(
            "immutable input digest inventory mismatch: "
            f"missing={missing}, unexpected={unexpected}"
        )
    found: dict[str, str] = {}
    for name, (algorithm, _expected) in EXPECTED_IMMUTABLE_INPUT_DIGESTS.items():
        if name == "experiment_contract" and experiment_contract_digest is not None:
            found[name] = experiment_contract_digest
        else:
            found[name] = _immutable_input_digest(paths[name], algorithm)
    bad = [
        name
        for name, (_algorithm, expected) in EXPECTED_IMMUTABLE_INPUT_DIGESTS.items()
        if found[name] != expected
    ]
    if bad:
        raise Phase86ACnyrubfSourceValidationError(
            "immutable input hash mismatch: " + ", ".join(bad)
        )
    return found


def _validate_phase83_evidence(
    aggregate: Mapping[str, Any],
    gates: Mapping[str, Any],
) -> None:
    final = gates.get("G12_final_acceptance", {})
    if (
        not isinstance(final, Mapping)
        or aggregate.get("final_status") != EXPECTED_PHASE83_STATUS
        or final.get("status") != EXPECTED_PHASE83_STATUS
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "Phase 8.3 status mismatch"
        )
    if final.get("recommendation") != EXPECTED_PHASE83_RECOMMENDATION:
        raise Phase86ACnyrubfSourceValidationError(
            "Phase 8.3 recommendation mismatch"
        )


def _validate_experiment_contract(contract: Mapping[str, Any]) -> None:
    expected_identity = {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": CONTRACT_TASK_ID,
        "lane": LANE,
        "phase": PHASE,
        "execution_mode": EXECUTION_MODE,
        "status": "source_correction_contract_pending_implementation",
    }
    if contract.get("contract_identity") != expected_identity:
        raise Phase86ACnyrubfSourceValidationError(
            "CNYRUBF correction contract identity mismatch"
        )
    if contract.get("approved_branch") != CONTRACT_BRANCH:
        raise Phase86ACnyrubfSourceValidationError(
            "CNYRUBF correction contract branch mismatch"
        )
    source = contract.get("source_identity", {})
    if tuple(
        source.get(key)
        for key in (
            "security_id",
            "asset_code",
            "board_id",
            "engine",
            "market",
            "algopack_market_code",
        )
    ) != (
        SECURITY_ID,
        ASSET_CODE,
        BOARD_ID,
        ENGINE,
        MARKET,
        "FO",
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "CNYRUBF correction source identity mismatch"
        )
    if (
        source.get("source_id") != SOURCE_ID
        or source.get("tradestats_route") != ALGOPACK_TRADESTATS_ROUTE
        or source.get("security_metadata_route") != build_security_metadata_url()
        or source.get("contract_roll_mapping_required") is not False
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "CNYRUBF correction route or perpetual identity mismatch"
        )
    metadata = contract.get("metadata_identity_policy", {})
    if (
        metadata.get("required_description_columns") != ["name", "value"]
        or metadata.get("required_description_value_keys") != ["SECID"]
        or metadata.get("secid_must_equal") != SECURITY_ID
        or metadata.get("boardid_must_equal") != BOARD_ID
        or metadata.get("engine_must_equal") != ENGINE
        or metadata.get("market_must_equal") != MARKET
        or metadata.get("is_primary_must_equal") != 1
        or metadata.get("is_traded_must_equal") != 1
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "CNYRUBF correction metadata policy mismatch"
        )
    leakage = contract.get("acceptance_matrix_leakage_policy", {})
    if (
        frozenset(leakage.get("forbidden_acceptance_matrix_fields", ()))
        != FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS
        or leakage.get("forbidden_fields_must_be_absent") is not True
        or leakage.get("failure_blocker") != "target_derived_field_leakage"
        or leakage.get("phase8_6b_entry_allowed_on_failure") is not False
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "CNYRUBF acceptance matrix leakage policy mismatch"
        )
    implementation = contract.get("implementation_scope_next_pr", {})
    if (
        tuple(implementation.get("required_new_files", ())) != IMPLEMENTATION_FILES
        or implementation.get("spot_files_may_be_imported_as_generic_primitives_only_after_refactor")
        is not True
        or implementation.get("spot_identity_constants_must_not_be_reused")
        is not True
        or implementation.get("implementation_requires_separate_pr") is not True
        or implementation.get("server_apply_allowed") is not False
        or implementation.get("controlled_runtime_allowed") is not False
        or implementation.get("controlled_runtime_requires_separate_explicit_authority")
        is not True
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "CNYRUBF implementation scope or authority mismatch"
        )
    if tuple(contract.get("required_runtime_artifacts", ())) != DECLARED_OUTPUT_ARTIFACTS:
        raise Phase86ACnyrubfSourceValidationError(
            "CNYRUBF runtime artifact contract mismatch"
        )


def _eligible_identities(frame: pd.DataFrame) -> pd.DataFrame:
    needed = (
        *IDENTITY_COLUMNS,
        "prior_trade_date",
        "target_phase_label",
        "target_is_labeled",
        "target_source",
    )
    if any(column not in frame for column in needed):
        raise Phase86ACnyrubfSourceValidationError(
            "modeling identity fields missing"
        )
    target = pd.to_datetime(frame.target_trade_date, errors="coerce")
    prior = pd.to_datetime(frame.prior_trade_date, errors="coerce")
    instrument = frame.target_instrument_id.astype("string").str.strip()
    mask = (
        frame.target_source.eq(TARGET_SOURCE)
        & frame.target_is_labeled.eq(True)
        & frame.target_phase_label.isin(CLASS_ORDER)
        & target.notna()
        & prior.notna()
        & instrument.eq(EXPECTED_INSTRUMENT)
    )
    result = frame.loc[mask, [*IDENTITY_COLUMNS, "prior_trade_date"]].copy()
    result.target_trade_date = target[mask].dt.strftime("%Y-%m-%d")
    result.prior_trade_date = prior[mask].dt.strftime("%Y-%m-%d")
    result.target_instrument_id = instrument[mask].astype(str)
    result = result.reset_index(drop=True)
    dates = pd.to_datetime(result.target_trade_date)
    priors = pd.to_datetime(result.prior_trade_date)
    if (
        len(result) != EXPECTED_ELIGIBLE_IDENTITIES
        or result.duplicated(list(IDENTITY_COLUMNS)).any()
        or not dates.is_monotonic_increasing
        or dates.iloc[0].date() != EXPECTED_FIRST_TARGET
        or dates.iloc[-1].date() != EXPECTED_LAST_TARGET
        or not (priors < dates).all()
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "frozen eligible identities mismatch"
        )
    return result


def _validation_identities(
    frame: pd.DataFrame,
    eligible: pd.DataFrame,
) -> pd.DataFrame:
    if any(column not in frame for column in IDENTITY_COLUMNS):
        raise Phase86ACnyrubfSourceValidationError(
            "validation identities missing"
        )
    result = frame.loc[:, IDENTITY_COLUMNS].copy()
    result.target_trade_date = pd.to_datetime(
        result.target_trade_date,
        errors="raise",
    ).dt.strftime("%Y-%m-%d")
    result.target_instrument_id = (
        result.target_instrument_id.astype("string").str.strip().astype(str)
    )
    result = result.reset_index(drop=True)
    if (
        len(result) != EXPECTED_VALIDATION_IDENTITIES
        or result.duplicated(list(IDENTITY_COLUMNS)).any()
        or not pd.MultiIndex.from_frame(result)
        .isin(pd.MultiIndex.from_frame(eligible.loc[:, IDENTITY_COLUMNS]))
        .all()
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "frozen validation identities mismatch"
        )
    return result


def normalized_candles(
    candles: Sequence[CnyrubfAlgoPackDailyCandle],
) -> pd.DataFrame:
    return pd.DataFrame(
        [candle.as_record() for candle in candles],
        columns=NORMALIZED_SOURCE_COLUMNS,
    )


def _empty_source_row() -> dict[str, object | None]:
    return {column: None for column in ACCEPTANCE_MATRIX_COLUMNS[3:]}


def _accepted_source_row(
    candle: CnyrubfAlgoPackDailyCandle,
) -> dict[str, object]:
    return {
        "cnyrubf_security_id": candle.security_id,
        "cnyrubf_asset_code": candle.asset_code,
        "cnyrubf_board_id": candle.board_id,
        "cnyrubf_trade_date": candle.trade_date.isoformat(),
        "cnyrubf_open": candle.open,
        "cnyrubf_high": candle.high,
        "cnyrubf_low": candle.low,
        "cnyrubf_close": candle.close,
        "cnyrubf_volume": candle.volume,
        "cnyrubf_volume_buy": candle.volume_buy,
        "cnyrubf_volume_sell": candle.volume_sell,
        "cnyrubf_volume_imbalance": candle.volume_imbalance,
        "cnyrubf_value": candle.value,
        "cnyrubf_value_buy": candle.value_buy,
        "cnyrubf_value_sell": candle.value_sell,
        "cnyrubf_trades": candle.trades,
        "cnyrubf_trades_buy": candle.trades_buy,
        "cnyrubf_trades_sell": candle.trades_sell,
        "cnyrubf_initial_margin_close": candle.initial_margin_close,
        "cnyrubf_open_interest_open": candle.open_interest_open,
        "cnyrubf_open_interest_high": candle.open_interest_high,
        "cnyrubf_open_interest_low": candle.open_interest_low,
        "cnyrubf_open_interest_close": candle.open_interest_close,
        "cnyrubf_candle_begin": candle.candle_begin.isoformat(),
        "cnyrubf_candle_end": candle.candle_end.isoformat(),
        "cnyrubf_source_available_at": candle.source_available_at.isoformat(),
        "cnyrubf_source_route": candle.source_route,
        "cnyrubf_payload_sha256": candle.raw_payload_sha256,
        "cnyrubf_retrieved_at_utc": candle.retrieved_at_utc.isoformat().replace(
            "+00:00",
            "Z",
        ),
        "cnyrubf_source_revision_status": candle.source_revision_status,
    }


def build_cnyrubf_pit_acceptance_matrix(
    eligible: pd.DataFrame,
    candles: Sequence[CnyrubfAlgoPackDailyCandle],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    keyed: dict[date, CnyrubfAlgoPackDailyCandle] = {}
    for candle in candles:
        if candle.trade_date in keyed:
            raise Phase86ACnyrubfSourceValidationError(
                "duplicate CNYRUBF daily source date",
                blocker="numerical_or_chronology_integrity_failure",
            )
        keyed[candle.trade_date] = candle

    rows: list[dict[str, object | None]] = []
    diagnostics: list[dict[str, object | None]] = []
    for identity in eligible.itertuples(index=False):
        target = date.fromisoformat(identity.target_trade_date)
        prior = date.fromisoformat(identity.prior_trade_date)
        candle = keyed.get(prior)
        accepted = False
        blocker: str | None = None
        candidate = None if candle is None else candle.trade_date.isoformat()
        if candle is None:
            source_row = _empty_source_row()
            reason = "missing_exact_prior_trade_date_cnyrubf_aggregate"
        else:
            try:
                validate_prior_session_candle(
                    candle,
                    target_trade_date=target,
                    prior_trade_date=prior,
                )
            except CnyrubfAlgoPackError as exc:
                if exc.blocker != "point_in_time_cutoff_not_provable":
                    raise Phase86ACnyrubfSourceValidationError(
                        str(exc),
                        blocker=exc.blocker,
                    ) from exc
                source_row = _empty_source_row()
                reason = str(exc)
                blocker = exc.blocker
            else:
                accepted = True
                reason = "accepted_exact_prior_trade_date_cnyrubf_aggregate"
                source_row = _accepted_source_row(candle)
        rows.append(
            {
                "target_trade_date": identity.target_trade_date,
                "target_instrument_id": identity.target_instrument_id,
                "prior_trade_date": identity.prior_trade_date,
                **source_row,
            }
        )
        diagnostics.append(
            {
                "target_trade_date": identity.target_trade_date,
                "prior_trade_date": identity.prior_trade_date,
                "candidate_trade_date": candidate,
                "accepted": accepted,
                "reason": reason,
                "blocker_classification": blocker,
                "same_day_or_future_used": False,
                "forward_fill_used": False,
                "backward_fill_used": False,
                "arbitrary_date_selection_used": False,
                "source_substitution_used": False,
                "target_derived_field_used": False,
            }
        )
    matrix = pd.DataFrame(rows, columns=ACCEPTANCE_MATRIX_COLUMNS)
    diagnostic_frame = pd.DataFrame(diagnostics, columns=DIAGNOSTIC_COLUMNS)
    if set(matrix.columns) & FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS:
        raise Phase86ACnyrubfSourceValidationError(
            "acceptance matrix contains target-derived fields",
            blocker="target_derived_field_leakage",
        )
    return matrix, diagnostic_frame


def _coverage(
    matrix: pd.DataFrame,
    validation: pd.DataFrame,
) -> pd.DataFrame:
    mask = pd.MultiIndex.from_frame(
        matrix.loc[:, IDENTITY_COLUMNS]
    ).isin(pd.MultiIndex.from_frame(validation))
    required_columns = tuple(
        column
        for column in ACCEPTANCE_MATRIX_COLUMNS[3:]
        if column != "cnyrubf_initial_margin_close"
    )
    complete = matrix.loc[:, required_columns].notna().all(axis=1)
    eligible_covered = int(complete.sum())
    validation_count = int(mask.sum())
    validation_covered = int(complete.to_numpy()[mask].sum())
    return pd.DataFrame(
        [
            {
                "source_id": SOURCE_ID,
                "eligible_identity_count": len(matrix),
                "eligible_covered_count": eligible_covered,
                "eligible_missing_count": len(matrix) - eligible_covered,
                "eligible_coverage_pct": (
                    eligible_covered / len(matrix) * 100 if len(matrix) else 0.0
                ),
                "validation_identity_count": validation_count,
                "validation_covered_count": validation_covered,
                "validation_missing_count": validation_count - validation_covered,
                "validation_coverage_pct": (
                    validation_covered / validation_count * 100
                    if validation_count
                    else 0.0
                ),
            }
        ]
    )


def _identity_record(
    identity: CnyrubfSecurityIdentity | None,
) -> dict[str, object]:
    if identity is None:
        return {
            "source_id": SOURCE_ID,
            "security_id": SECURITY_ID,
            "asset_code": ASSET_CODE,
            "board_id": BOARD_ID,
            "engine": ENGINE,
            "market": MARKET,
            "identity_verified": False,
            "historical_model_use_status": "blocked",
        }
    return {
        **asdict(identity),
        "identity_verified": True,
        "identity_service": "MOEX ISS metadata",
        "data_source_id": SOURCE_ID,
        "data_service": "MOEX AlgoPack subscription",
    }


def _official_route_validation(
    identity: CnyrubfSecurityIdentity | None,
    candles: pd.DataFrame,
    *,
    source_error: CnyrubfAlgoPackError | None,
) -> dict[str, object]:
    return {
        "official_service": "MOEX AlgoPack subscription",
        "official_hosts": ["apim.moex.com", "iss.moex.com"],
        "authorization_scheme": "Bearer",
        "token_environment_variable": ALGOPACK_TOKEN_ENV,
        "token_persisted_in_artifacts": False,
        "authorization_header_persisted": False,
        "response_body_persisted_in_blocker": False,
        "redirects_allowed": False,
        "security_metadata_route": build_security_metadata_url(),
        "tradestats_route": ALGOPACK_TRADESTATS_ROUTE,
        "bucket_interval_minutes": ALGOPACK_BUCKET_MINUTES,
        "tradetime_semantics": "completed_five_minute_interval_end",
        "provider_availability_field": "SYSTIME",
        "verified_security_id": identity.security_id if identity else None,
        "verified_asset_code": identity.asset_code if identity else None,
        "verified_board_id": identity.board_id if identity else None,
        "verified_engine": identity.engine if identity else None,
        "verified_market": identity.market if identity else None,
        "primary_board": bool(identity and identity.primary_board),
        "active_board": bool(identity and identity.active_board),
        "metadata_identity_verified": bool(identity),
        "pagination_complete": source_error is None,
        "schema_stable_within_run": source_error is None,
        "earliest_available_date": (
            None if candles.empty else str(candles.trade_date.min())
        ),
        "latest_available_date": (
            None if candles.empty else str(candles.trade_date.max())
        ),
        "daily_aggregate_count": len(candles),
        "directional_fields_present": bool(
            not candles.empty
            and {
                "volume_buy",
                "volume_sell",
                "value_buy",
                "value_sell",
                "trades_buy",
                "trades_sell",
            }.issubset(candles.columns)
        ),
        "open_interest_fields_present": bool(
            not candles.empty
            and {
                "open_interest_open",
                "open_interest_high",
                "open_interest_low",
                "open_interest_close",
            }.issubset(candles.columns)
        ),
        "source_availability_present": bool(
            not candles.empty and candles.source_available_at.notna().all()
        ),
        "fallback_used": False,
        "source_revision_status": SOURCE_REVISION_STATUS,
    }


def _finite(frame: pd.DataFrame, columns: Sequence[str]) -> bool:
    if frame.empty:
        return True
    numeric = frame.loc[:, columns].apply(pd.to_numeric, errors="coerce")
    return bool(numeric.map(math.isfinite).all().all())


def evaluate_gates(
    *,
    immutable_inputs_verified: bool,
    phase83_verified: bool,
    eligible: pd.DataFrame,
    validation: pd.DataFrame,
    identity: CnyrubfSecurityIdentity | None,
    candles: pd.DataFrame,
    matrix: pd.DataFrame,
    coverage: pd.DataFrame,
    diagnostics: pd.DataFrame,
    route_validation: Mapping[str, object],
    source_error: CnyrubfAlgoPackError | None = None,
) -> dict[str, dict[str, object]]:
    g1 = bool(
        immutable_inputs_verified
        and phase83_verified
        and len(eligible) == EXPECTED_ELIGIBLE_IDENTITIES
        and len(validation) == EXPECTED_VALIDATION_IDENTITIES
    )
    g2 = bool(
        identity
        and (
            identity.security_id,
            identity.asset_code,
            identity.board_id,
            identity.engine,
            identity.market,
        )
        == (SECURITY_ID, ASSET_CODE, BOARD_ID, ENGINE, MARKET)
        and identity.primary_board
        and identity.active_board
    )
    g3 = bool(
        route_validation.get("official_hosts")
        == ["apim.moex.com", "iss.moex.com"]
        and route_validation.get("tradestats_route") == ALGOPACK_TRADESTATS_ROUTE
        and route_validation.get("security_metadata_route")
        == build_security_metadata_url()
        and route_validation.get("bucket_interval_minutes")
        == ALGOPACK_BUCKET_MINUTES
        and route_validation.get("tradetime_semantics")
        == "completed_five_minute_interval_end"
        and route_validation.get("pagination_complete")
        and route_validation.get("schema_stable_within_run")
        and route_validation.get("directional_fields_present")
        and route_validation.get("open_interest_fields_present")
        and route_validation.get("source_availability_present")
        and route_validation.get("redirects_allowed") is False
    )

    accepted = matrix.cnyrubf_trade_date.notna()
    target = pd.to_datetime(matrix.loc[accepted, "target_trade_date"])
    prior = pd.to_datetime(matrix.loc[accepted, "prior_trade_date"])
    observed = pd.to_datetime(matrix.loc[accepted, "cnyrubf_trade_date"])
    available = pd.to_datetime(
        matrix.loc[accepted, "cnyrubf_source_available_at"],
        utc=True,
    ).dt.tz_convert("Europe/Moscow")
    no_fill_or_substitution = not diagnostics[
        [
            "same_day_or_future_used",
            "forward_fill_used",
            "backward_fill_used",
            "arbitrary_date_selection_used",
            "source_substitution_used",
            "target_derived_field_used",
        ]
    ].any().any()
    pit_rejection = diagnostics.blocker_classification.eq(
        "point_in_time_cutoff_not_provable"
    ).any()
    g4 = bool(
        not (
            source_error
            and source_error.blocker == "point_in_time_cutoff_not_provable"
        )
        and not pit_rejection
        and observed.eq(prior).all()
        and available.lt(
            target.dt.tz_localize("Europe/Moscow") + pd.Timedelta(hours=6)
        ).all()
        and no_fill_or_substitution
    )

    coverage_row = coverage.iloc[0]
    g5 = bool(
        len(matrix) == EXPECTED_ELIGIBLE_IDENTITIES
        and int(coverage_row.eligible_covered_count)
        == EXPECTED_ELIGIBLE_IDENTITIES
        and int(coverage_row.validation_covered_count)
        == EXPECTED_VALIDATION_IDENTITIES
        and not matrix.duplicated(list(IDENTITY_COLUMNS)).any()
        and matrix.loc[:, IDENTITY_COLUMNS].equals(
            eligible.loc[:, IDENTITY_COLUMNS]
        )
        and tuple(matrix.columns) == ACCEPTANCE_MATRIX_COLUMNS
    )

    numeric_columns = (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "volume_buy",
        "volume_sell",
        "volume_imbalance",
        "value",
        "value_buy",
        "value_sell",
        "trades",
        "trades_buy",
        "trades_sell",
        "open_interest_open",
        "open_interest_high",
        "open_interest_low",
        "open_interest_close",
    )
    volume_identity = bool(
        candles.empty
        or (
            candles.volume
            - (candles.volume_buy + candles.volume_sell)
        ).abs().le(1e-9).all()
    )
    value_identity = bool(
        candles.empty
        or (
            candles.value - (candles.value_buy + candles.value_sell)
        ).abs().le(candles.value.abs().mul(1e-6).clip(lower=1.0)).all()
    )
    trade_identity = bool(
        candles.empty
        or candles.trades.eq(candles.trades_buy + candles.trades_sell).all()
    )
    price_ohlc = bool(
        candles.empty
        or (
            candles.high.ge(
                candles[["open", "close", "low"]].max(axis=1)
            ).all()
            and candles.low.le(
                candles[["open", "close", "high"]].min(axis=1)
            ).all()
        )
    )
    open_interest_ohlc = bool(
        candles.empty
        or (
            candles.open_interest_high.ge(
                candles[
                    [
                        "open_interest_open",
                        "open_interest_close",
                        "open_interest_low",
                    ]
                ].max(axis=1)
            ).all()
            and candles.open_interest_low.le(
                candles[
                    [
                        "open_interest_open",
                        "open_interest_close",
                        "open_interest_high",
                    ]
                ].min(axis=1)
            ).all()
        )
    )
    g6 = bool(
        _finite(candles, numeric_columns)
        and (
            candles.empty
            or (
                pd.to_datetime(candles.trade_date).is_monotonic_increasing
                and not candles.duplicated(["trade_date"]).any()
            )
        )
        and price_ohlc
        and open_interest_ohlc
        and volume_identity
        and value_identity
        and trade_identity
        and (
            candles.empty
            or candles.volume_imbalance.between(-1.0, 1.0).all()
        )
    )

    provenance = matrix.loc[
        accepted,
        [
            "cnyrubf_security_id",
            "cnyrubf_asset_code",
            "cnyrubf_board_id",
            "cnyrubf_source_route",
            "cnyrubf_payload_sha256",
            "cnyrubf_retrieved_at_utc",
            "cnyrubf_source_revision_status",
        ],
    ]
    revision_valid = bool(
        matrix.cnyrubf_source_revision_status.notna().all()
        and matrix.cnyrubf_source_revision_status.eq(
            SOURCE_REVISION_STATUS
        ).all()
    )
    g7 = bool(
        provenance.notna().all().all()
        and provenance.cnyrubf_security_id.eq(SECURITY_ID).all()
        and provenance.cnyrubf_asset_code.eq(ASSET_CODE).all()
        and provenance.cnyrubf_board_id.eq(BOARD_ID).all()
        and provenance.cnyrubf_source_route.astype(str)
        .str.startswith(ALGOPACK_TRADESTATS_ROUTE + "?")
        .all()
        and provenance.cnyrubf_payload_sha256.astype(str)
        .map(lambda value: bool(_SHA256.fullmatch(value)))
        .all()
        and revision_valid
    )
    leakage_fields = set(matrix.columns) & FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS
    g8 = bool(
        not leakage_fields
        and not diagnostics.source_substitution_used.any()
        and not diagnostics.arbitrary_date_selection_used.any()
        and not diagnostics.target_derived_field_used.any()
        and route_validation.get("fallback_used") is False
    )

    passed = (g1, g2, g3, g4, g5, g6, g7, g8)
    gates: dict[str, dict[str, object]] = {
        "G1_immutable_inputs": {"passed": g1},
        "G2_official_security_identity": {"passed": g2},
        "G3_algopack_tradestats_route_and_schema": {"passed": g3},
        "G4_point_in_time_session_correctness": {
            "passed": g4,
            "pit_rejection_count": int(
                diagnostics.blocker_classification.eq(
                    "point_in_time_cutoff_not_provable"
                ).sum()
            ),
            "provider_availability_field": "SYSTIME",
            "tradetime_semantics": "completed_five_minute_interval_end",
            "fill_or_substitution_used": not no_fill_or_substitution,
        },
        "G5_exact_coverage": {"passed": g5},
        "G6_numerical_and_open_interest_integrity": {
            "passed": g6,
            "volume_equals_buy_plus_sell": volume_identity,
            "value_equals_buy_plus_sell": value_identity,
            "trades_equal_buy_plus_sell": trade_identity,
            "price_ohlc_valid": price_ohlc,
            "open_interest_ohlc_valid": open_interest_ohlc,
        },
        "G7_provenance": {
            "passed": g7,
            "source_revision_status_required": SOURCE_REVISION_STATUS,
            "source_revision_status_valid": revision_valid,
            "token_persisted_in_artifacts": False,
        },
        "G8_no_fallback_or_target_leakage": {
            "passed": g8,
            "forbidden_fields_present": sorted(leakage_fields),
            "target_prediction_or_probability_used": False,
            "source_fallback_used": False,
            "model_fit_or_evaluation_performed": False,
            "out_of_directory_write_performed": False,
            "promotion_performed": False,
            "broker_or_trading_action_performed": False,
        },
    }
    failed = [f"G{index}" for index, value in enumerate(passed, 1) if not value]
    blocker = None if not failed else _blocker_from_failure(failed, source_error)
    gates["G9_final_source_readiness"] = {
        "passed": not failed,
        "requires": [f"G{index}" for index in range(1, 9)],
        "failed_gates": failed,
        "status": (
            "moex_algopack_cnyrubf_source_candidate_for_phase8_6b"
            if not failed
            else "moex_algopack_cnyrubf_source_not_ready"
        ),
        "historical_model_use_status": (
            HISTORICAL_MODEL_USE_STATUS if not failed else "blocked"
        ),
        "blocker_classification": blocker,
    }
    return gates


def _blocker_from_failure(
    failed: list[str],
    source_error: CnyrubfAlgoPackError | None,
) -> str:
    if source_error and source_error.blocker in BLOCKER_CLASSIFICATIONS:
        return source_error.blocker
    mapping = {
        "G8": "target_derived_field_leakage",
        "G2": "security_identity_not_reproducible",
        "G3": "algopack_schema_not_stable",
        "G4": "point_in_time_cutoff_not_provable",
        "G5": "incomplete_identity_coverage",
        "G6": "numerical_or_chronology_integrity_failure",
        "G7": "provenance_not_sufficient",
    }
    return next(
        (value for gate, value in mapping.items() if gate in failed),
        "other_fail_closed_with_exact_reason",
    )


def _structured_reason(
    blocker: str | None,
    diagnostics: pd.DataFrame,
    source_error: CnyrubfAlgoPackError | None,
) -> str | None:
    if blocker is None:
        return None
    matching = diagnostics.loc[
        diagnostics.blocker_classification.eq(blocker),
        "reason",
    ]
    if not matching.empty:
        return str(matching.iloc[0])
    if source_error is not None:
        return str(source_error)
    return blocker


def _ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_ready(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _write_exact_artifacts(
    output_dir: Path,
    payloads: Mapping[str, object],
) -> None:
    if tuple(payloads) != DECLARED_OUTPUT_ARTIFACTS:
        raise Phase86ACnyrubfSourceValidationError(
            "undeclared runtime artifact inventory"
        )
    if output_dir.exists():
        raise Phase86ACnyrubfSourceValidationError(
            "output directory must not pre-exist"
        )
    output_dir.mkdir(parents=True, exist_ok=False)
    for name in DECLARED_OUTPUT_ARTIFACTS:
        path = output_dir / name
        payload = payloads[name]
        if path.parent != output_dir:
            raise Phase86ACnyrubfSourceValidationError(
                "write outside output directory refused"
            )
        if name.endswith(".json"):
            path.write_text(
                json.dumps(
                    _ready(payload),
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
        elif name.endswith(".csv") and isinstance(payload, pd.DataFrame):
            payload.to_csv(path, index=False)
        elif name.endswith(".parquet") and isinstance(payload, pd.DataFrame):
            payload.to_parquet(path, index=False)
        else:
            raise Phase86ACnyrubfSourceValidationError(
                "artifact payload or suffix mismatch"
            )
    if sorted(path.name for path in output_dir.iterdir()) != sorted(
        DECLARED_OUTPUT_ARTIFACTS
    ):
        raise Phase86ACnyrubfSourceValidationError(
            "runtime artifact inventory mismatch"
        )


def run_source_validation(
    request: Phase86ARequest,
    *,
    iss_transport: IssTransport = fetch_iss_bytes,
    algopack_transport: AlgoPackTransport = fetch_algopack_bytes,
    token_loader: TokenLoader = load_algopack_token,
    clock: UtcClock = utc_now,
    sleeper: Any = None,
    identity_loader: Any = load_security_identity,
    history_loader: Any = load_daily_history,
) -> Phase86AResult:
    _validate_request(request)
    contract, contract_digest = _verified_experiment_contract(
        request.experiment_contract_path
    )
    hashes = verify_immutable_inputs(
        request,
        experiment_contract_digest=contract_digest,
    )
    aggregate = _json(request.phase83_aggregate_metrics_path)
    phase83 = _json(request.phase83_gate_results_path)
    _validate_phase83_evidence(aggregate, phase83)
    _validate_experiment_contract(contract)
    _json(request.dataset_manifest_path)
    _json(request.feature_schema_path)

    eligible = _eligible_identities(
        pd.read_parquet(request.modeling_dataset_path)
    )
    validation = _validation_identities(
        pd.read_parquet(request.m0_validation_predictions_path),
        eligible,
    )

    identity: CnyrubfSecurityIdentity | None = None
    source_error: CnyrubfAlgoPackError | None = None
    candle_list: list[CnyrubfAlgoPackDailyCandle] = []
    loader_kwargs: dict[str, object] = {"clock": clock}
    if sleeper is not None:
        loader_kwargs["sleeper"] = sleeper
    try:
        identity = identity_loader(
            transport=iss_transport,
            **loader_kwargs,
        )
        first = min(map(date.fromisoformat, eligible.prior_trade_date))
        last = max(map(date.fromisoformat, eligible.prior_trade_date))
        candle_list = history_loader(
            identity,
            from_date=first,
            till_date=last,
            transport=algopack_transport,
            token_loader=token_loader,
            **loader_kwargs,
        )
    except CnyrubfAlgoPackError as exc:
        if exc.blocker not in BLOCKER_CLASSIFICATIONS:
            raise Phase86ACnyrubfSourceValidationError(
                str(exc),
                blocker=exc.blocker,
            ) from exc
        source_error = exc

    candles = normalized_candles(candle_list)
    matrix, diagnostics = build_cnyrubf_pit_acceptance_matrix(
        eligible,
        candle_list,
    )
    coverage = _coverage(matrix, validation)
    route_validation = _official_route_validation(
        identity,
        candles,
        source_error=source_error,
    )
    gates = evaluate_gates(
        immutable_inputs_verified=True,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        identity=identity,
        candles=candles,
        matrix=matrix,
        coverage=coverage,
        diagnostics=diagnostics,
        route_validation=route_validation,
        source_error=source_error,
    )
    final = gates["G9_final_source_readiness"]

    input_identity = {
        "project": PROJECT,
        "phase": PHASE,
        "task_id": TASK_ID,
        "implementation_branch": IMPLEMENTATION_BRANCH,
        "run_id": request.run_id,
        "source_git_commit_sha": request.git_commit_sha,
        "eligible_identity_count": len(eligible),
        "validation_identity_count": len(validation),
        "expected_folds": EXPECTED_FOLDS,
        "expected_validation_rows_per_fold": EXPECTED_VALIDATION_ROWS_PER_FOLD,
        "frozen_target_interval": [
            eligible.target_trade_date.iloc[0],
            eligible.target_trade_date.iloc[-1],
        ],
        "immutable_inputs": {
            name: {
                "algorithm": algorithm,
                "expected_digest": expected,
                "observed_digest": hashes[name],
                "matches": hashes[name] == expected,
                **(
                    {
                        "expected_sha256": expected,
                        "observed_sha256": hashes[name],
                    }
                    if algorithm == "sha256"
                    else {
                        "expected_git_blob_sha1": expected,
                        "observed_git_blob_sha1": hashes[name],
                    }
                ),
            }
            for name, (algorithm, expected) in EXPECTED_IMMUTABLE_INPUT_DIGESTS.items()
        },
    }
    blocker_register = {
        "source_id": SOURCE_ID,
        "status": final["status"],
        "historical_model_use_status": final["historical_model_use_status"],
        "blocker_classification": final["blocker_classification"],
        "exact_blocker_reason": _structured_reason(
            final["blocker_classification"],
            diagnostics,
            source_error,
        ),
        "failed_gates": final["failed_gates"],
        "offending_candle_used_in_acceptance_matrix": False,
        "fill_or_substitution_used": False,
        "source_fallback_used": False,
        "target_derived_field_used": False,
        "subscription_token_persisted": False,
        "authorization_header_persisted": False,
        "response_body_persisted_in_blocker": False,
        "model_fit_or_evaluation_performed": False,
        "promotion_authorized": False,
        "server_apply_performed": False,
    }
    payloads: dict[str, object] = {
        "input_identity_verification.json": input_identity,
        "official_route_validation.json": route_validation,
        "cnyrubf_security_identity.json": _identity_record(identity),
        "cnyrubf_daily_candles_normalized.parquet": candles,
        "cnyrubf_pit_acceptance_matrix.parquet": matrix,
        "coverage_by_source.csv": coverage,
        "session_alignment_diagnostics.csv": diagnostics,
        "source_blocker_register.json": blocker_register,
        "gate_results.json": gates,
    }
    _write_exact_artifacts(request.output_dir, payloads)
    return Phase86AResult(
        output_dir=request.output_dir,
        artifact_names=DECLARED_OUTPUT_ARTIFACTS,
        eligible_identity_count=len(eligible),
        validation_identity_count=len(validation),
        final_status=str(final["status"]),
        blocker_classification=final["blocker_classification"],
    )


def run_from_args(arguments: argparse.Namespace) -> Phase86AResult:
    return run_source_validation(request_from_args(arguments))


def main(argv: Sequence[str] | None = None) -> int:
    run_from_args(build_argument_parser().parse_args(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
