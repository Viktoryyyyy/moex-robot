from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final

import pandas as pd

from moex_research.external_data.models import HttpTransport, fetch_bytes
from moex_research.external_data.moex_brent_history import (
    ASSET_CODE,
    BOARD_ID,
    CANDLE_ROUTE_TEMPLATE,
    HISTORY_ROUTE,
    HISTORICAL_MODEL_USE_STATUS,
    MOEX_ISS_HOST,
    SECURITY_DESCRIPTION_ROUTE_TEMPLATE,
    SOURCE_ID,
    BrentContract,
    BrentHistoryError,
    UtcClock,
    enumerate_brent_contract_identities,
    load_contract_metadata,
    load_daily_candle,
    select_nearest_contract,
    utc_now,
    validate_prior_session_cutoff,
)


PROJECT: Final[str] = "MOEX Bot"
PHASE: Final[str] = "8.4A"
LANE: Final[str] = "ema_3_19_ai"
TASK_ID: Final[str] = (
    "ema_3_19_ai_market_phase_phase_8_4a_moex_brent_source_validation_v1"
)
EXECUTION_MODE: Final[str] = "browser_chatgpt_github_direct"
CONTRACT_ID: Final[str] = "usdrubf_phase8_4a_moex_brent_source_validation_v1"
CONTRACT_VERSION: Final[str] = "1.0"
APPROVED_BRANCH: Final[str] = (
    "research/ema-3-19-ai/phase-8-4a-moex-brent-source-validation"
)
PHASE81_CONTRACT_ID: Final[str] = "usdrubf_phase8_1_external_data_acquisition_v1"
EXPECTED_INPUT_SHA256: Final[dict[str, str]] = {
    "modeling_dataset": "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00",
    "dataset_manifest": "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082",
    "feature_schema": "8f08802c7fb0a4cc43ab4ba072ee22ff9edd92fe8d674ea0515545d20d143238",
    "m0_validation_predictions": "9769d00a49adeb54c016d965387774e46a3e09e09f895aa61d48a90bbf3568cf",
    "phase83_aggregate_metrics": "d6ad4f6587dadb32431bd7b8f3bd59c5393e04d742efb4af459b316b417f8756",
    "phase83_gate_results": "d3f7e24022e550e725eae7ec5bc214d6b95e0e9c66393574c575e5d6f593f33c",
}
EXPECTED_ELIGIBLE_IDENTITIES: Final[int] = 472
EXPECTED_VALIDATION_IDENTITIES: Final[int] = 320
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"
TARGET_SOURCE: Final[str] = "manual_phase_labels_v1"
CLASS_ORDER: Final[tuple[str, ...]] = ("B", "S", "OUT")
EXPECTED_FIRST_TARGET: Final[date] = date(2024, 8, 5)
EXPECTED_LAST_TARGET: Final[date] = date(2026, 6, 11)
EXPECTED_PHASE83_STATUS: Final[str] = "external_factor_incremental_value_not_supported"
EXPECTED_PHASE83_RECOMMENDATION: Final[str] = (
    "prioritize_blocked_oil_and_liquidity_sources"
)
IDENTITY_COLUMNS: Final[tuple[str, str]] = (
    "target_trade_date",
    "target_instrument_id",
)
ACCEPTANCE_MATRIX_COLUMNS: Final[tuple[str, ...]] = (
    "target_trade_date",
    "target_instrument_id",
    "prior_trade_date",
    "brent_contract_code",
    "brent_expiration_date",
    "brent_days_to_expiration",
    "brent_contract_changed",
    "brent_previous_contract_code",
    "brent_trade_date",
    "brent_open",
    "brent_high",
    "brent_low",
    "brent_close",
    "brent_volume",
    "brent_value",
    "brent_candle_begin",
    "brent_candle_end",
    "brent_contract_metadata_route",
    "brent_candle_route",
    "brent_contract_metadata_sha256",
    "brent_candle_payload_sha256",
    "brent_retrieved_at_utc",
)
DECLARED_OUTPUT_ARTIFACTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "official_route_validation.json",
    "brent_contract_universe.parquet",
    "brent_daily_candles_normalized.parquet",
    "brent_pit_acceptance_matrix.parquet",
    "coverage_by_source.csv",
    "contract_roll_diagnostics.csv",
    "source_blocker_register.json",
    "gate_results.json",
)
REQUIRED_CLI_ARGS: Final[tuple[str, ...]] = (
    "--modeling-dataset-path",
    "--dataset-manifest-path",
    "--feature-schema-path",
    "--m0-validation-predictions-path",
    "--phase8-3-aggregate-metrics-path",
    "--phase8-3-gate-results-path",
    "--phase8-1-source-contract-path",
    "--experiment-contract-path",
    "--output-dir",
    "--run-id",
    "--git-commit-sha",
)
BLOCKER_CLASSIFICATIONS: Final[tuple[str, ...]] = (
    "expired_contract_universe_not_reproducible",
    "expired_contract_candles_not_available",
    "point_in_time_cutoff_not_provable",
    "incomplete_identity_coverage",
    "official_schema_not_stable",
    "provenance_not_sufficient",
    "other_fail_closed_with_exact_reason",
)
_RUN_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SHA40_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")
_SHA256_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_ALIAS_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.IGNORECASE
)
_GLOB_CHARS: Final[frozenset[str]] = frozenset("*?[]")
_FORBIDDEN_MATRIX_FIELDS: Final[frozenset[str]] = frozenset(
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


class Phase84ABrentSourceValidationError(ValueError):
    """Raised when Phase 8.4A source validation must fail closed."""

    def __init__(
        self,
        message: str,
        *,
        blocker: str = "other_fail_closed_with_exact_reason",
    ) -> None:
        super().__init__(message)
        if blocker not in BLOCKER_CLASSIFICATIONS:
            blocker = "other_fail_closed_with_exact_reason"
        self.blocker = blocker


@dataclass(frozen=True)
class Phase84ARequest:
    modeling_dataset_path: Path
    dataset_manifest_path: Path
    feature_schema_path: Path
    m0_validation_predictions_path: Path
    phase83_aggregate_metrics_path: Path
    phase83_gate_results_path: Path
    phase81_source_contract_path: Path
    experiment_contract_path: Path
    output_dir: Path
    run_id: str
    git_commit_sha: str


@dataclass(frozen=True)
class Phase84AResult:
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
            "usdrubf_phase8_4a_moex_brent_source_validation"
        ),
        description=(
            "Validate an official, explicit-contract, prior-session MOEX Brent "
            "source without fitting or evaluating a predictive model."
        ),
    )
    for flag in REQUIRED_CLI_ARGS:
        parser.add_argument(flag, required=True)
    return parser


def _explicit_input_path(raw: object, flag: str, suffix: str) -> Path:
    text = str(raw).strip()
    if not text or any(char in text for char in _GLOB_CHARS):
        raise Phase84ABrentSourceValidationError(f"{flag} must identify one explicit file")
    if _ALIAS_PATTERN.search(text):
        raise Phase84ABrentSourceValidationError(f"{flag} must not use a mutable alias")
    path = Path(text)
    if path.suffix.lower() != suffix or not path.exists() or not path.is_file():
        raise Phase84ABrentSourceValidationError(f"{flag} file or suffix mismatch")
    return path


def _explicit_output_path(raw: object) -> Path:
    text = str(raw).strip()
    if not text or any(char in text for char in _GLOB_CHARS) or _ALIAS_PATTERN.search(text):
        raise Phase84ABrentSourceValidationError("--output-dir must be explicit and immutable")
    return Path(text)


def request_from_args(args: argparse.Namespace) -> Phase84ARequest:
    request = Phase84ARequest(
        modeling_dataset_path=_explicit_input_path(
            args.modeling_dataset_path, "--modeling-dataset-path", ".parquet"
        ),
        dataset_manifest_path=_explicit_input_path(
            args.dataset_manifest_path, "--dataset-manifest-path", ".json"
        ),
        feature_schema_path=_explicit_input_path(
            args.feature_schema_path, "--feature-schema-path", ".json"
        ),
        m0_validation_predictions_path=_explicit_input_path(
            args.m0_validation_predictions_path,
            "--m0-validation-predictions-path",
            ".parquet",
        ),
        phase83_aggregate_metrics_path=_explicit_input_path(
            getattr(args, "phase8_3_aggregate_metrics_path"),
            "--phase8-3-aggregate-metrics-path",
            ".json",
        ),
        phase83_gate_results_path=_explicit_input_path(
            getattr(args, "phase8_3_gate_results_path"),
            "--phase8-3-gate-results-path",
            ".json",
        ),
        phase81_source_contract_path=_explicit_input_path(
            getattr(args, "phase8_1_source_contract_path"),
            "--phase8-1-source-contract-path",
            ".json",
        ),
        experiment_contract_path=_explicit_input_path(
            args.experiment_contract_path, "--experiment-contract-path", ".json"
        ),
        output_dir=_explicit_output_path(args.output_dir),
        run_id=str(args.run_id).strip(),
        git_commit_sha=str(args.git_commit_sha).strip().lower(),
    )
    _validate_request(request)
    return request


def _input_paths(request: Phase84ARequest) -> dict[str, Path]:
    return {
        "modeling_dataset": request.modeling_dataset_path,
        "dataset_manifest": request.dataset_manifest_path,
        "feature_schema": request.feature_schema_path,
        "m0_validation_predictions": request.m0_validation_predictions_path,
        "phase83_aggregate_metrics": request.phase83_aggregate_metrics_path,
        "phase83_gate_results": request.phase83_gate_results_path,
        "phase81_source_contract": request.phase81_source_contract_path,
        "experiment_contract": request.experiment_contract_path,
    }


def _validate_request(request: Phase84ARequest) -> None:
    rules = (
        (request.modeling_dataset_path, "--modeling-dataset-path", ".parquet"),
        (request.dataset_manifest_path, "--dataset-manifest-path", ".json"),
        (request.feature_schema_path, "--feature-schema-path", ".json"),
        (
            request.m0_validation_predictions_path,
            "--m0-validation-predictions-path",
            ".parquet",
        ),
        (
            request.phase83_aggregate_metrics_path,
            "--phase8-3-aggregate-metrics-path",
            ".json",
        ),
        (
            request.phase83_gate_results_path,
            "--phase8-3-gate-results-path",
            ".json",
        ),
        (
            request.phase81_source_contract_path,
            "--phase8-1-source-contract-path",
            ".json",
        ),
        (request.experiment_contract_path, "--experiment-contract-path", ".json"),
    )
    for path, flag, suffix in rules:
        _explicit_input_path(path, flag, suffix)
    if len({path.resolve() for path in _input_paths(request).values()}) != 8:
        raise Phase84ABrentSourceValidationError("all input files must be distinct")
    _explicit_output_path(request.output_dir)
    if not _RUN_ID_PATTERN.fullmatch(request.run_id) or _ALIAS_PATTERN.search(request.run_id):
        raise Phase84ABrentSourceValidationError("--run-id must be immutable")
    if not _SHA40_PATTERN.fullmatch(request.git_commit_sha):
        raise Phase84ABrentSourceValidationError(
            "--git-commit-sha must be exactly 40 hexadecimal characters"
        )
    if request.output_dir.exists():
        raise Phase84ABrentSourceValidationError("--output-dir must not pre-exist")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase84ABrentSourceValidationError(f"invalid JSON input: {path.name}") from exc
    if not isinstance(payload, dict):
        raise Phase84ABrentSourceValidationError(f"JSON input must be an object: {path.name}")
    return payload


def verify_immutable_inputs(request: Phase84ARequest) -> dict[str, str]:
    observed = {
        name: _sha256_file(path)
        for name, path in _input_paths(request).items()
        if name in EXPECTED_INPUT_SHA256
    }
    for name, expected in EXPECTED_INPUT_SHA256.items():
        if observed.get(name) != expected:
            raise Phase84ABrentSourceValidationError(f"immutable input hash mismatch: {name}")
    return observed


def _validate_phase83_evidence(aggregate: Mapping[str, Any], gates: Mapping[str, Any]) -> None:
    final_gate = gates.get("G12_final_acceptance")
    if not isinstance(final_gate, Mapping):
        raise Phase84ABrentSourceValidationError("Phase 8.3 final gate is absent")
    if aggregate.get("final_status") != EXPECTED_PHASE83_STATUS:
        raise Phase84ABrentSourceValidationError("Phase 8.3 aggregate status mismatch")
    if final_gate.get("status") != EXPECTED_PHASE83_STATUS or final_gate.get("passed") is not False:
        raise Phase84ABrentSourceValidationError("Phase 8.3 final status mismatch")
    if final_gate.get("recommendation") != EXPECTED_PHASE83_RECOMMENDATION:
        raise Phase84ABrentSourceValidationError("Phase 8.3 recommendation mismatch")
    if set(final_gate.get("failed_gates", ())) != {"G5", "G6", "G7", "G8", "G9"}:
        raise Phase84ABrentSourceValidationError("Phase 8.3 failed-gate identity mismatch")


def _validate_phase81_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity", {})
    if identity.get("contract_id") != PHASE81_CONTRACT_ID:
        raise Phase84ABrentSourceValidationError("Phase 8.1 source contract mismatch")
    source = contract.get("sources", {}).get(SOURCE_ID, {})
    routes = source.get("official_routes", {})
    if (
        source.get("official_service") != "MOEX ISS"
        or source.get("historical_model_use_status") != "blocked_pending_source_validation"
        or routes.get("explicit_contract_daily_candles") != CANDLE_ROUTE_TEMPLATE
    ):
        raise Phase84ABrentSourceValidationError("Phase 8.1 MOEX Brent declaration mismatch")


def _validate_experiment_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity", {})
    if identity != {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": TASK_ID,
        "lane": LANE,
        "phase": PHASE,
        "execution_mode": EXECUTION_MODE,
        "status": "source_validation_only",
    }:
        raise Phase84ABrentSourceValidationError("Phase 8.4A contract identity mismatch")
    if contract.get("approved_branch") != APPROVED_BRANCH:
        raise Phase84ABrentSourceValidationError("Phase 8.4A branch mismatch")
    if tuple(contract.get("runtime_artifacts", ())) != DECLARED_OUTPUT_ARTIFACTS:
        raise Phase84ABrentSourceValidationError("Phase 8.4A artifact contract mismatch")


def _eligible_identities(dataset: pd.DataFrame) -> pd.DataFrame:
    required = (
        "target_phase_label",
        "target_is_labeled",
        "target_source",
        "target_trade_date",
        "target_instrument_id",
        "prior_trade_date",
    )
    missing = [column for column in required if column not in dataset.columns]
    if missing:
        raise Phase84ABrentSourceValidationError(
            "modeling dataset missing identity fields: " + ", ".join(missing)
        )
    target_dates = pd.to_datetime(dataset["target_trade_date"], errors="coerce")
    prior_dates = pd.to_datetime(dataset["prior_trade_date"], errors="coerce")
    instrument = dataset["target_instrument_id"].astype("string").str.strip()
    mask = (
        dataset["target_source"].eq(TARGET_SOURCE)
        & dataset["target_is_labeled"].eq(True)
        & dataset["target_phase_label"].isin(CLASS_ORDER)
        & target_dates.notna()
        & prior_dates.notna()
        & instrument.eq(EXPECTED_INSTRUMENT)
    )
    identities = dataset.loc[mask, [*IDENTITY_COLUMNS, "prior_trade_date"]].copy()
    identities["target_trade_date"] = target_dates.loc[mask].dt.strftime("%Y-%m-%d")
    identities["prior_trade_date"] = prior_dates.loc[mask].dt.strftime("%Y-%m-%d")
    identities["target_instrument_id"] = instrument.loc[mask].astype(str)
    identities = identities.reset_index(drop=True)
    if len(identities) != EXPECTED_ELIGIBLE_IDENTITIES:
        raise Phase84ABrentSourceValidationError("eligible identity count must equal 472")
    if identities.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise Phase84ABrentSourceValidationError("eligible identity is duplicated")
    dates = pd.to_datetime(identities["target_trade_date"])
    priors = pd.to_datetime(identities["prior_trade_date"])
    if (
        not dates.is_monotonic_increasing
        or dates.iloc[0].date() != EXPECTED_FIRST_TARGET
        or dates.iloc[-1].date() != EXPECTED_LAST_TARGET
        or not (priors < dates).all()
    ):
        raise Phase84ABrentSourceValidationError("frozen target interval or prior date mismatch")
    return identities


def _validation_identities(predictions: pd.DataFrame, eligible: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in IDENTITY_COLUMNS if column not in predictions.columns]
    if missing:
        raise Phase84ABrentSourceValidationError("M0 validation identity fields are missing")
    identities = predictions.loc[:, IDENTITY_COLUMNS].copy()
    identities["target_trade_date"] = pd.to_datetime(
        identities["target_trade_date"], errors="raise"
    ).dt.strftime("%Y-%m-%d")
    identities["target_instrument_id"] = (
        identities["target_instrument_id"].astype("string").str.strip().astype(str)
    )
    identities = identities.reset_index(drop=True)
    if len(identities) != EXPECTED_VALIDATION_IDENTITIES:
        raise Phase84ABrentSourceValidationError("validation identity count must equal 320")
    if identities.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise Phase84ABrentSourceValidationError("validation identity is duplicated")
    eligible_index = pd.MultiIndex.from_frame(eligible.loc[:, IDENTITY_COLUMNS])
    validation_index = pd.MultiIndex.from_frame(identities)
    if not validation_index.isin(eligible_index).all():
        raise Phase84ABrentSourceValidationError("validation identity is not Phase 6 eligible")
    return identities


def _contract_for_identity(
    identity: Any,
    *,
    metadata_cache: dict[str, BrentContract],
    transport: HttpTransport,
    clock: UtcClock,
) -> BrentContract:
    cached = metadata_cache.get(identity.contract_code)
    if cached is None:
        cached = load_contract_metadata(
            identity,
            transport=transport,
            clock=clock,
        )
        metadata_cache[identity.contract_code] = cached
    elif cached.short_name != identity.short_name:
        raise Phase84ABrentSourceValidationError(
            "official contract identity changed across historical enumerations",
            blocker="official_schema_not_stable",
        )
    if not (
        cached.first_verified_trade_date
        <= identity.enumerated_as_of_date
        <= cached.expiration_date
    ):
        raise Phase84ABrentSourceValidationError(
            "historical enumeration falls outside official contract life",
            blocker="expired_contract_universe_not_reproducible",
        )
    return replace(
        cached,
        enumerated_as_of_date=identity.enumerated_as_of_date,
        enumeration_route=identity.enumeration_route,
        enumeration_retrieved_at_utc=identity.enumeration_retrieved_at_utc,
        enumeration_raw_payload_sha256=identity.enumeration_raw_payload_sha256,
    )


def build_brent_pit_matrix(
    eligible: pd.DataFrame,
    *,
    transport: HttpTransport = fetch_bytes,
    clock: UtcClock = utc_now,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    metadata_cache: dict[str, BrentContract] = {}
    universe_observations: dict[str, list[BrentContract]] = {}
    universe_by_date: dict[date, list[BrentContract]] = {}
    candle_records: list[dict[str, object]] = []
    matrix_rows: list[dict[str, object]] = []
    roll_rows: list[dict[str, object]] = []
    previous_code: str | None = None
    for row in eligible.itertuples(index=False):
        target_date = date.fromisoformat(row.target_trade_date)
        prior_date = date.fromisoformat(row.prior_trade_date)
        contracts = universe_by_date.get(prior_date)
        if contracts is None:
            try:
                identities = enumerate_brent_contract_identities(
                    prior_date,
                    transport=transport,
                    clock=clock,
                )
                contracts = [
                    _contract_for_identity(
                        identity,
                        metadata_cache=metadata_cache,
                        transport=transport,
                        clock=clock,
                    )
                    for identity in identities
                ]
            except BrentHistoryError as exc:
                raise Phase84ABrentSourceValidationError(
                    str(exc), blocker=exc.blocker
                ) from None
            universe_by_date[prior_date] = contracts
            for contract in contracts:
                universe_observations.setdefault(contract.contract_code, []).append(contract)
        try:
            selected = select_nearest_contract(
                contracts,
                target_trade_date=target_date,
            )
            candle = load_daily_candle(
                selected,
                prior_date,
                transport=transport,
                clock=clock,
            )
            validate_prior_session_cutoff(
                candle,
                target_trade_date=target_date,
                prior_trade_date=prior_date,
            )
        except BrentHistoryError as exc:
            raise Phase84ABrentSourceValidationError(str(exc), blocker=exc.blocker) from None
        changed = previous_code is not None and previous_code != selected.contract_code
        days_to_expiration = (selected.expiration_date - target_date).days
        matrix_rows.append(
            {
                "target_trade_date": row.target_trade_date,
                "target_instrument_id": row.target_instrument_id,
                "prior_trade_date": row.prior_trade_date,
                "brent_contract_code": selected.contract_code,
                "brent_expiration_date": selected.expiration_date.isoformat(),
                "brent_days_to_expiration": days_to_expiration,
                "brent_contract_changed": changed,
                "brent_previous_contract_code": previous_code,
                "brent_trade_date": candle.trade_date.isoformat(),
                "brent_open": candle.open,
                "brent_high": candle.high,
                "brent_low": candle.low,
                "brent_close": candle.close,
                "brent_volume": candle.volume,
                "brent_value": candle.value,
                "brent_candle_begin": candle.candle_begin.isoformat(),
                "brent_candle_end": candle.candle_end.isoformat(),
                "brent_contract_metadata_route": selected.metadata_route,
                "brent_candle_route": candle.source_route,
                "brent_contract_metadata_sha256": selected.metadata_raw_payload_sha256,
                "brent_candle_payload_sha256": candle.raw_payload_sha256,
                "brent_retrieved_at_utc": candle.retrieved_at_utc.isoformat().replace(
                    "+00:00", "Z"
                ),
            }
        )
        candle_records.append(candle.as_record())
        if changed:
            roll_rows.append(
                {
                    "target_trade_date": row.target_trade_date,
                    "prior_trade_date": row.prior_trade_date,
                    "previous_contract_code": previous_code,
                    "new_contract_code": selected.contract_code,
                    "new_contract_expiration_date": selected.expiration_date.isoformat(),
                    "new_contract_days_to_expiration": days_to_expiration,
                    "roll_rule": "nearest_expiration_at_least_7_calendar_days_after_target",
                    "target_or_future_information_used": False,
                    "cross_contract_return_calculated": False,
                }
            )
        previous_code = selected.contract_code
    universe_rows: list[dict[str, object]] = []
    for _, observations in sorted(universe_observations.items()):
        dates = sorted(item.enumerated_as_of_date for item in observations)
        for observation in sorted(
            observations, key=lambda item: item.enumerated_as_of_date
        ):
            universe_rows.append(
                {
                    **observation.as_record(),
                    "first_enumerated_as_of_date": dates[0],
                    "last_enumerated_as_of_date": dates[-1],
                    "enumeration_date_count": len(set(dates)),
                }
            )
    return (
        pd.DataFrame(universe_rows),
        pd.DataFrame(candle_records),
        pd.DataFrame(matrix_rows, columns=ACCEPTANCE_MATRIX_COLUMNS),
        pd.DataFrame(
            roll_rows,
            columns=(
                "target_trade_date",
                "prior_trade_date",
                "previous_contract_code",
                "new_contract_code",
                "new_contract_expiration_date",
                "new_contract_days_to_expiration",
                "roll_rule",
                "target_or_future_information_used",
                "cross_contract_return_calculated",
            ),
        ),
    )


def _coverage(matrix: pd.DataFrame, validation: pd.DataFrame) -> pd.DataFrame:
    matrix_index = pd.MultiIndex.from_frame(matrix.loc[:, IDENTITY_COLUMNS])
    validation_index = pd.MultiIndex.from_frame(validation)
    validation_mask = matrix_index.isin(validation_index)
    required = [
        column for column in matrix.columns if column != "brent_previous_contract_code"
    ]
    first_identity = pd.Series(matrix.index == 0, index=matrix.index)
    previous_contract_valid = (
        first_identity | matrix["brent_previous_contract_code"].notna()
    )
    complete = matrix[required].notna().all(axis=1) & previous_contract_valid
    eligible_covered = int(complete.sum())
    validation_covered = int(complete.to_numpy()[validation_mask].sum())
    return pd.DataFrame(
        [
            {
                "source_id": SOURCE_ID,
                "eligible_identity_count": len(matrix),
                "eligible_covered_count": eligible_covered,
                "eligible_missing_count": len(matrix) - eligible_covered,
                "eligible_coverage_pct": eligible_covered / len(matrix) * 100.0,
                "validation_identity_count": int(validation_mask.sum()),
                "validation_covered_count": validation_covered,
                "validation_missing_count": int(validation_mask.sum()) - validation_covered,
                "validation_coverage_pct": validation_covered / int(validation_mask.sum()) * 100.0,
            }
        ]
    )


def _official_route_validation(
    universe: pd.DataFrame,
    candles: pd.DataFrame,
    eligible: pd.DataFrame,
) -> dict[str, Any]:
    expired = pd.to_datetime(universe["expiration_date"]).lt(
        pd.Timestamp(EXPECTED_LAST_TARGET)
    )
    return {
        "official_service": "MOEX ISS",
        "official_host": MOEX_ISS_HOST,
        "history_enumeration_route": HISTORY_ROUTE,
        "exact_security_description_route_template": SECURITY_DESCRIPTION_ROUTE_TEMPLATE,
        "explicit_contract_candle_route_template": CANDLE_ROUTE_TEMPLATE,
        "history_enumeration_uses_official_SECID": True,
        "contract_code_generation_or_guessing_used": False,
        "current_active_contract_route_used_as_historical_proof": False,
        "prior_trade_date_enumeration_count": int(eligible["prior_trade_date"].nunique()),
        "unique_explicit_contract_count": int(universe["contract_code"].nunique()),
        "expired_explicit_contract_count": int(
            universe.loc[expired, "contract_code"].nunique()
        ),
        "explicit_contract_candle_count": int(len(candles)),
        "all_contracts_BR_RFUD": bool(
            universe["asset_code"].eq(ASSET_CODE).all()
            and universe["board_id"].eq(BOARD_ID).all()
        ),
        "all_routes_official_https": True,
        "retrieval_timestamp_origin": "per_payload_post_transport_utc_clock",
        "caller_provided_production_retrieval_timestamp_allowed": False,
        "synthetic_clock_injection_scope": "tests_only",
        "metadata_and_candle_payload_provenance_distinguishable": True,
    }


def _utc_timestamp(value: object) -> datetime | None:
    if isinstance(value, pd.Timestamp):
        parsed = value.to_pydatetime()
    elif isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        return None
    return parsed.astimezone(timezone.utc)


def _timestamp_key(value: object) -> str | None:
    parsed = _utc_timestamp(value)
    return None if parsed is None else parsed.isoformat()


def evaluate_gates(
    *,
    immutable_inputs_verified: bool,
    phase83_verified: bool,
    eligible: pd.DataFrame,
    validation: pd.DataFrame,
    universe: pd.DataFrame,
    candles: pd.DataFrame,
    matrix: pd.DataFrame,
    coverage: pd.DataFrame,
    rolls: pd.DataFrame,
    route_validation: Mapping[str, Any],
) -> dict[str, Any]:
    g1 = immutable_inputs_verified and phase83_verified and len(eligible) == 472 and len(validation) == 320
    g2 = bool(
        route_validation.get("all_routes_official_https")
        and route_validation.get("history_enumeration_uses_official_SECID")
        and not route_validation.get("contract_code_generation_or_guessing_used")
        and route_validation.get("expired_explicit_contract_count", 0) > 0
        and not universe.duplicated(
            ["contract_code", "enumerated_as_of_date"], keep=False
        ).any()
        and universe["expiration_date"].notna().all()
    )
    g3 = bool(
        matrix["brent_days_to_expiration"].ge(7).all()
        and matrix["brent_contract_code"].isin(universe["contract_code"]).all()
    )
    target = pd.to_datetime(matrix["target_trade_date"])
    prior = pd.to_datetime(matrix["prior_trade_date"])
    candle_dates = pd.to_datetime(matrix["brent_trade_date"])
    ends = pd.to_datetime(matrix["brent_candle_end"], utc=True).dt.tz_convert("Europe/Moscow")
    cutoffs = target.dt.tz_localize("Europe/Moscow") + pd.Timedelta(hours=8, minutes=45)
    ohlc = (
        matrix["brent_high"].ge(matrix[["brent_open", "brent_close"]].max(axis=1))
        & matrix["brent_low"].le(matrix[["brent_open", "brent_close"]].min(axis=1))
        & matrix["brent_high"].ge(matrix["brent_low"])
    )
    g4 = bool(
        candle_dates.eq(prior).all()
        and ends.lt(cutoffs).all()
        and ohlc.all()
        and matrix[["brent_volume", "brent_value"]].ge(0).all().all()
    )
    cov = coverage.iloc[0]
    g5 = bool(
        len(matrix) == 472
        and int(cov["eligible_covered_count"]) == 472
        and int(cov["validation_covered_count"]) == 320
        and not matrix.duplicated(list(IDENTITY_COLUMNS), keep=False).any()
        and matrix.loc[:, IDENTITY_COLUMNS].equals(eligible.loc[:, IDENTITY_COLUMNS])
        and tuple(matrix.columns) == ACCEPTANCE_MATRIX_COLUMNS
    )
    changed = matrix["brent_contract_code"].ne(matrix["brent_contract_code"].shift())
    changed.iloc[0] = False
    expected_previous = matrix["brent_contract_code"].shift()
    g6 = bool(
        matrix["brent_contract_changed"].eq(changed).all()
        and matrix["brent_previous_contract_code"]
        .fillna("")
        .eq(expected_previous.fillna(""))
        .all()
        and len(rolls) == int(changed.sum())
        and (rolls.empty or (not rolls["target_or_future_information_used"].any()))
        and (rolls.empty or (not rolls["cross_contract_return_calculated"].any()))
    )
    hashes = pd.concat(
        [
            universe["metadata_raw_payload_sha256"].astype(str),
            universe["enumeration_raw_payload_sha256"].astype(str),
            candles["raw_payload_sha256"].astype(str),
        ],
        ignore_index=True,
    )
    enumeration_provenance_valid = bool(
        universe["enumeration_retrieved_at_utc"]
        .map(lambda value: _utc_timestamp(value) is not None)
        .all()
        and universe["enumeration_route"].astype(str).str.contains("/iss/history/").all()
        and universe["enumeration_raw_payload_sha256"]
        .astype(str)
        .map(lambda value: bool(_SHA256_PATTERN.fullmatch(value)))
        .all()
    )
    enumeration_provenance_records = {
        (
            str(row.enumeration_route),
            str(row.enumeration_raw_payload_sha256),
            _timestamp_key(row.enumeration_retrieved_at_utc),
        )
        for row in universe.itertuples(index=False)
    }
    enumeration_payload_keys = {
        record[:2] for record in enumeration_provenance_records
    }
    metadata_provenance_valid = bool(
        universe["metadata_retrieved_at_utc"]
        .map(lambda value: _utc_timestamp(value) is not None)
        .all()
        and universe["metadata_route"].astype(str).str.contains("/iss/securities/").all()
        and universe["metadata_raw_payload_sha256"]
        .astype(str)
        .map(lambda value: bool(_SHA256_PATTERN.fullmatch(value)))
        .all()
    )
    candle_provenance_valid = bool(
        candles["retrieved_at_utc"]
        .map(lambda value: _utc_timestamp(value) is not None)
        .all()
        and candles["source_route"].astype(str).str.contains("/candles.json").all()
        and candles["raw_payload_sha256"]
        .astype(str)
        .map(lambda value: bool(_SHA256_PATTERN.fullmatch(value)))
        .all()
    )
    candle_provenance_records = {
        (
            str(row.contract_code),
            str(row.trade_date),
            str(row.source_route),
            str(row.raw_payload_sha256),
            _timestamp_key(row.retrieved_at_utc),
        )
        for row in candles.itertuples(index=False)
    }
    metadata_provenance_records = {
        (
            str(row.contract_code),
            str(row.metadata_route),
            str(row.metadata_raw_payload_sha256),
            _timestamp_key(row.metadata_retrieved_at_utc),
        )
        for row in universe.itertuples(index=False)
    }
    metadata_payload_keys = {
        record[:3] for record in metadata_provenance_records
    }
    matrix_metadata_records = {
        (
            str(row.brent_contract_code),
            str(row.brent_contract_metadata_route),
            str(row.brent_contract_metadata_sha256),
        )
        for row in matrix.itertuples(index=False)
    }
    matrix_candle_records = {
        (
            str(row.brent_contract_code),
            str(row.brent_trade_date),
            str(row.brent_candle_route),
            str(row.brent_candle_payload_sha256),
            _timestamp_key(row.brent_retrieved_at_utc),
        )
        for row in matrix.itertuples(index=False)
    }
    g7 = bool(
        hashes.map(lambda value: bool(_SHA256_PATTERN.fullmatch(value))).all()
        and enumeration_provenance_valid
        and metadata_provenance_valid
        and candle_provenance_valid
        and None not in {record[-1] for record in enumeration_provenance_records}
        and len(enumeration_provenance_records) == len(enumeration_payload_keys)
        and None not in {record[-1] for record in matrix_candle_records}
        and None not in {record[-1] for record in metadata_provenance_records}
        and len(metadata_provenance_records) == len(metadata_payload_keys)
        and matrix_metadata_records.issubset(metadata_payload_keys)
        and matrix_candle_records.issubset(candle_provenance_records)
        and matrix["brent_contract_metadata_route"]
        .ne(matrix["brent_candle_route"])
        .all()
        and matrix["brent_contract_metadata_route"].str.contains("/iss/securities/").all()
        and matrix["brent_candle_route"].str.contains("/candles.json").all()
        and route_validation.get("retrieval_timestamp_origin")
        == "per_payload_post_transport_utc_clock"
        and route_validation.get(
            "caller_provided_production_retrieval_timestamp_allowed"
        )
        is False
        and route_validation.get("synthetic_clock_injection_scope") == "tests_only"
        and route_validation.get(
            "metadata_and_candle_payload_provenance_distinguishable"
        )
    )
    g8 = not bool(set(matrix.columns) & _FORBIDDEN_MATRIX_FIELDS)
    passed = (g1, g2, g3, g4, g5, g6, g7, g8)
    gates: dict[str, Any] = {
        "G1_immutable_inputs": {"passed": g1},
        "G2_official_expired_contract_universe": {"passed": g2},
        "G3_explicit_contract_selection": {"passed": g3},
        "G4_point_in_time_candle_correctness": {"passed": g4},
        "G5_exact_coverage": {"passed": g5},
        "G6_roll_integrity": {"passed": g6},
        "G7_provenance": {"passed": g7},
        "G8_leakage_and_scope": {
            "passed": g8,
            "target_prediction_or_probability_used": False,
            "non_MOEX_source_accessed": False,
            "model_fit_or_evaluation_performed": False,
            "model_file_created": False,
        },
    }
    final = all(passed)
    failed = [f"G{index}" for index, value in enumerate(passed, 1) if not value]
    blocker = None if final else _blocker_from_failed_gates(failed)
    gates["G9_final_source_readiness"] = {
        "passed": final,
        "requires": [f"G{index}" for index in range(1, 9)],
        "failed_gates": failed,
        "status": (
            "moex_brent_source_candidate_for_phase8_5"
            if final
            else "moex_brent_source_remains_blocked"
        ),
        "blocker_classification": blocker,
    }
    return gates


def _blocker_from_failed_gates(failed: list[str]) -> str:
    for gate, blocker in (
        ("G2", "expired_contract_universe_not_reproducible"),
        ("G4", "point_in_time_cutoff_not_provable"),
        ("G5", "incomplete_identity_coverage"),
        ("G7", "provenance_not_sufficient"),
    ):
        if gate in failed:
            return blocker
    return "other_fail_closed_with_exact_reason"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _write_exact_artifacts(output_dir: Path, payloads: Mapping[str, object]) -> None:
    if tuple(payloads) != DECLARED_OUTPUT_ARTIFACTS:
        raise Phase84ABrentSourceValidationError("undeclared runtime artifact inventory")
    if output_dir.exists():
        raise Phase84ABrentSourceValidationError("output directory must not pre-exist")
    output_dir.mkdir(parents=True, exist_ok=False)
    for name in DECLARED_OUTPUT_ARTIFACTS:
        path = output_dir / name
        if path.parent != output_dir:
            raise Phase84ABrentSourceValidationError("write outside output directory refused")
        payload = payloads[name]
        if name.endswith(".json"):
            path.write_text(
                json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )
        elif name.endswith(".csv"):
            if not isinstance(payload, pd.DataFrame):
                raise Phase84ABrentSourceValidationError("CSV payload must be a DataFrame")
            payload.to_csv(path, index=False)
        elif name.endswith(".parquet"):
            if not isinstance(payload, pd.DataFrame):
                raise Phase84ABrentSourceValidationError("Parquet payload must be a DataFrame")
            payload.to_parquet(path, index=False)
        else:  # pragma: no cover
            raise Phase84ABrentSourceValidationError("unsupported artifact suffix")
    observed = sorted(path.name for path in output_dir.iterdir() if path.is_file())
    if observed != sorted(DECLARED_OUTPUT_ARTIFACTS):
        raise Phase84ABrentSourceValidationError("runtime artifact inventory mismatch")


def run_source_validation(
    request: Phase84ARequest,
    *,
    transport: HttpTransport = fetch_bytes,
    clock: UtcClock = utc_now,
) -> Phase84AResult:
    _validate_request(request)
    observed_hashes = verify_immutable_inputs(request)
    aggregate = _json(request.phase83_aggregate_metrics_path)
    phase83_gates = _json(request.phase83_gate_results_path)
    _validate_phase83_evidence(aggregate, phase83_gates)
    _validate_phase81_contract(_json(request.phase81_source_contract_path))
    _validate_experiment_contract(_json(request.experiment_contract_path))
    # Dataset contract files are immutable evidence and are read before any network call.
    _json(request.dataset_manifest_path)
    _json(request.feature_schema_path)
    dataset = pd.read_parquet(request.modeling_dataset_path)
    predictions = pd.read_parquet(request.m0_validation_predictions_path)
    eligible = _eligible_identities(dataset)
    validation = _validation_identities(predictions, eligible)
    universe, candles, matrix, rolls = build_brent_pit_matrix(
        eligible,
        transport=transport,
        clock=clock,
    )
    coverage = _coverage(matrix, validation)
    route_validation = _official_route_validation(universe, candles, eligible)
    gates = evaluate_gates(
        immutable_inputs_verified=True,
        phase83_verified=True,
        eligible=eligible,
        validation=validation,
        universe=universe,
        candles=candles,
        matrix=matrix,
        coverage=coverage,
        rolls=rolls,
        route_validation=route_validation,
    )
    final = gates["G9_final_source_readiness"]
    blocker = final["blocker_classification"]
    input_verification = {
        "project": PROJECT,
        "phase": PHASE,
        "task_id": TASK_ID,
        "run_id": request.run_id,
        "source_git_commit_sha": request.git_commit_sha,
        "eligible_identity_count": len(eligible),
        "validation_identity_count": len(validation),
        "frozen_target_interval": [
            eligible["target_trade_date"].iloc[0],
            eligible["target_trade_date"].iloc[-1],
        ],
        "phase83_final_status": EXPECTED_PHASE83_STATUS,
        "phase83_recommendation": EXPECTED_PHASE83_RECOMMENDATION,
        "immutable_inputs": {
            name: {
                "expected_sha256": expected,
                "observed_sha256": observed_hashes[name],
                "matches": observed_hashes[name] == expected,
            }
            for name, expected in EXPECTED_INPUT_SHA256.items()
        },
    }
    blocker_register = {
        "source_id": SOURCE_ID,
        "status": (
            "candidate_for_phase8_5"
            if final["passed"]
            else "blocked_pending_source_validation"
        ),
        "blocker_classification": blocker,
        "exact_blocker_reason": None if final["passed"] else blocker,
        "existing_registry_modified": False,
        "promotion_authorized": False,
    }
    payloads: dict[str, object] = {
        "input_identity_verification.json": input_verification,
        "official_route_validation.json": route_validation,
        "brent_contract_universe.parquet": universe,
        "brent_daily_candles_normalized.parquet": candles,
        "brent_pit_acceptance_matrix.parquet": matrix,
        "coverage_by_source.csv": coverage,
        "contract_roll_diagnostics.csv": rolls,
        "source_blocker_register.json": blocker_register,
        "gate_results.json": gates,
    }
    _write_exact_artifacts(request.output_dir, payloads)
    return Phase84AResult(
        output_dir=request.output_dir,
        artifact_names=DECLARED_OUTPUT_ARTIFACTS,
        eligible_identity_count=len(eligible),
        validation_identity_count=len(validation),
        final_status=str(final["status"]),
        blocker_classification=blocker,
    )


def run_from_args(args: argparse.Namespace) -> Phase84AResult:
    return run_source_validation(request_from_args(args))


def main(argv: list[str] | None = None) -> int:
    run_from_args(build_argument_parser().parse_args(argv))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
