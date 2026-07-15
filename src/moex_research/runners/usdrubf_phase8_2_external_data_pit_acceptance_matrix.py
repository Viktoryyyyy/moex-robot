from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final
from urllib.parse import parse_qsl, urlsplit

import numpy as np
import pandas as pd

from moex_research.external_data.cbr import (
    KEY_RATE_ROUTE,
    RUONIA_ROUTE,
    load_key_rate_daily,
    load_ruonia_daily,
)
from moex_research.external_data.models import ExternalDataError, HttpTransport, fetch_bytes
from moex_research.external_data.pit_alignment import (
    IDENTITY_COLUMNS,
    KEY_RATE_MATRIX_COLUMNS,
    MATRIX_COLUMNS,
    RUONIA_MATRIX_COLUMNS,
    PITAlignmentError,
    build_external_pit_matrix,
)
from moex_research.external_data.registry import SOURCE_REGISTRY, require_phase8_2_ready


PROJECT: Final[str] = "MOEX Bot"
LANE: Final[str] = "ema_3_19_ai"
TASK_ID: Final[str] = (
    "ema_3_19_ai_market_phase_phase_8_2_external_data_pit_acceptance_matrix_v1"
)
PHASE: Final[str] = "8.2"
EXECUTION_MODE: Final[str] = "browser_chatgpt_github_direct"
CONTRACT_ID: Final[str] = (
    "usdrubf_phase8_2_external_data_pit_acceptance_matrix_v1"
)
CONTRACT_VERSION: Final[str] = "1.0"
PHASE81_CONTRACT_ID: Final[str] = "usdrubf_phase8_1_external_data_acquisition_v1"
APPROVED_BRANCH: Final[str] = (
    "research/ema-3-19-ai/phase-8-2-external-data-pit-acceptance-matrix"
)

EXPECTED_INPUT_SHA256: Final[dict[str, str]] = {
    "modeling_dataset": "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00",
    "dataset_manifest": "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082",
    "feature_schema": "8f08802c7fb0a4cc43ab4ba072ee22ff9edd92fe8d674ea0515545d20d143238",
    "m0_validation_predictions": "9769d00a49adeb54c016d965387774e46a3e09e09f895aa61d48a90bbf3568cf",
}
EXPECTED_ELIGIBLE_IDENTITIES: Final[int] = 472
EXPECTED_VALIDATION_IDENTITIES: Final[int] = 320
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"
TARGET_SOURCE: Final[str] = "manual_phase_labels_v1"
CLASS_ORDER: Final[tuple[str, ...]] = ("B", "S", "OUT")
DATASET_ID: Final[str] = "usdrubf_phase6_internal_modeling_dataset.v1"
FEATURE_SCHEMA_ID: Final[str] = "usdrubf_phase6_internal_factor_batches_v1"
HISTORY_BUFFER_CALENDAR_DAYS: Final[int] = 31
KEY_RATE_HISTORY_START: Final[date] = date(2014, 2, 3)
KEY_RATE_MAX_NORMALIZED_ROW_FRACTION: Final[float] = 0.05

ACCEPTED_SOURCES: Final[tuple[str, str]] = (
    "cbr_ruonia_daily",
    "cbr_key_rate_daily",
)
BLOCKED_SOURCE_STATUSES: Final[dict[str, str]] = {
    "moex_brent_futures_daily": "blocked_pending_source_validation",
    "cme_wti_pre_moex": "blocked_pending_license",
    "cbr_banking_liquidity_daily": "blocked_pending_vintage_policy",
    "ine_shanghai_crude_pre_moex": "blocked_pending_historical_intraday_source",
}
SOURCE_DATE_FIELDS: Final[dict[str, str]] = {
    "cbr_ruonia_daily": "observation_date",
    "cbr_key_rate_daily": "effective_date",
}
SOURCE_REVISION_STATUSES: Final[dict[str, str]] = {
    "cbr_ruonia_daily": "official_published_history",
    "cbr_key_rate_daily": "official_change_date_history",
}
SOURCE_REQUIRED_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "cbr_ruonia_daily": (
        "observation_date",
        "publication_date",
        "ruonia_rate_pct",
        "transaction_volume_rub_bn",
        "transaction_count",
        "participant_count",
        "minimum_rate_pct",
        "percentile_25_rate_pct",
        "percentile_75_rate_pct",
        "maximum_rate_pct",
        "calculation_status",
        "source_id",
        "source_route",
        "retrieved_at_utc",
        "raw_payload_sha256",
        "source_revision_status",
        "historical_model_use_status",
    ),
    "cbr_key_rate_daily": (
        "effective_date",
        "key_rate_pct",
        "source_id",
        "source_route",
        "retrieved_at_utc",
        "raw_payload_sha256",
        "source_revision_status",
        "historical_model_use_status",
    ),
}
DECLARED_OUTPUT_ARTIFACTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "source_fetch_manifest.json",
    "ruonia_normalized.parquet",
    "key_rate_normalized.parquet",
    "external_pit_acceptance_matrix.parquet",
    "coverage_by_source.csv",
    "staleness_by_source.csv",
    "source_blocker_register.json",
    "gate_results.json",
)
REQUIRED_CLI_ARGS: Final[tuple[str, ...]] = (
    "--modeling-dataset-path",
    "--dataset-manifest-path",
    "--feature-schema-path",
    "--m0-validation-predictions-path",
    "--phase8-1-source-contract-path",
    "--experiment-contract-path",
    "--output-dir",
    "--run-id",
    "--git-commit-sha",
)
FORBIDDEN_MATRIX_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "target_phase_label",
        "target_is_labeled",
        "target_source",
        "fold_id",
        "y_true",
        "candidate_y_pred",
        "probability_B",
        "probability_S",
        "probability_OUT",
    }
)
FORBIDDEN_MATRIX_TOKENS: Final[tuple[str, ...]] = (
    "prediction",
    "probability",
    "cme",
    "wti",
    "brent",
    "liquidity",
    "ine_",
    "lag1_",
    "rolling_",
    "ema_",
)
_ALIAS_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])",
    re.IGNORECASE,
)
_RUN_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SHA40_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-fA-F]{40}$")
_SHA256_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_GLOB_CHARS: Final[frozenset[str]] = frozenset("*?[]")
_SENSITIVE_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"authorization\s*[:=]", re.IGNORECASE),
    re.compile(r"api[_-]?key\s*[:=]", re.IGNORECASE),
    re.compile(r"access[_-]?token\s*[:=]", re.IGNORECASE),
    re.compile(r"client[_-]?secret\s*[:=]", re.IGNORECASE),
    re.compile(r"password\s*[:=]", re.IGNORECASE),
    re.compile(r"bearer\s+[A-Za-z0-9._~+/-]+", re.IGNORECASE),
)


class Phase82AcceptanceMatrixError(ValueError):
    """Raised when the Phase 8.2 controlled acceptance run must fail closed."""


@dataclass(frozen=True)
class Phase82AcceptanceRequest:
    modeling_dataset_path: Path
    dataset_manifest_path: Path
    feature_schema_path: Path
    m0_validation_predictions_path: Path
    phase81_source_contract_path: Path
    experiment_contract_path: Path
    output_dir: Path
    run_id: str
    git_commit_sha: str
    retrieved_at_utc: datetime | None = None


@dataclass(frozen=True)
class Phase82AcceptanceResult:
    output_dir: Path
    artifact_names: tuple[str, ...]
    eligible_identity_count: int
    validation_identity_count: int
    final_gate_passed: bool


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python -m moex_research.runners."
            "usdrubf_phase8_2_external_data_pit_acceptance_matrix"
        ),
        description=(
            "Build the controlled RUONIA/key-rate point-in-time acceptance matrix; "
            "no model fitting or evaluation is performed."
        ),
    )
    for flag in REQUIRED_CLI_ARGS:
        parser.add_argument(flag, required=True)
    parser.add_argument("--retrieved-at-utc")
    return parser


def request_from_args(args: argparse.Namespace) -> Phase82AcceptanceRequest:
    request = Phase82AcceptanceRequest(
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
        phase81_source_contract_path=_explicit_input_path(
            getattr(args, "phase8_1_source_contract_path"),
            "--phase8-1-source-contract-path",
            (".json",),
        ),
        experiment_contract_path=_explicit_input_path(
            args.experiment_contract_path,
            "--experiment-contract-path",
            (".json",),
        ),
        output_dir=_explicit_output_path(args.output_dir),
        run_id=_validate_run_id(args.run_id),
        git_commit_sha=_validate_git_sha(args.git_commit_sha),
        retrieved_at_utc=_parse_optional_retrieved_at(args.retrieved_at_utc),
    )
    _validate_distinct_inputs(request)
    return request


def run_from_args(args: argparse.Namespace) -> Phase82AcceptanceResult:
    return run_acceptance_matrix(request_from_args(args))


def run_acceptance_matrix(
    request: Phase82AcceptanceRequest,
    *,
    ruonia_transport: HttpTransport = fetch_bytes,
    key_rate_transport: HttpTransport = fetch_bytes,
) -> Phase82AcceptanceResult:
    _validate_request(request)
    _assert_output_dir_absent(request.output_dir)
    retrieved_at = request.retrieved_at_utc or datetime.now(timezone.utc).replace(
        microsecond=0
    )

    input_paths = _input_paths(request)
    observed_hashes = {name: _sha256_file(path) for name, path in input_paths.items()}
    _verify_immutable_hashes(observed_hashes)

    experiment_contract = _read_json_object(request.experiment_contract_path)
    phase81_contract = _read_json_object(request.phase81_source_contract_path)
    _validate_experiment_contract(experiment_contract)
    _validate_phase81_contract(phase81_contract)
    _validate_phase81_registry()
    _validate_dataset_contracts(
        _read_json_object(request.dataset_manifest_path),
        _read_json_object(request.feature_schema_path),
    )

    modeling_dataset = pd.read_parquet(request.modeling_dataset_path)
    m0_predictions = pd.read_parquet(request.m0_validation_predictions_path)
    eligible_identities = _eligible_identities(modeling_dataset)
    validation_identities = _validation_identities(
        m0_predictions, eligible_identities=eligible_identities
    )

    first_target = date.fromisoformat(eligible_identities["target_trade_date"].iloc[0])
    last_target = date.fromisoformat(eligible_identities["target_trade_date"].iloc[-1])
    ruonia_requested_start = first_target - timedelta(
        days=HISTORY_BUFFER_CALENDAR_DAYS
    )
    key_rate_requested_start = KEY_RATE_HISTORY_START
    requested_end = last_target

    ruonia_records = _load_source_safely(
        lambda: load_ruonia_daily(
            ruonia_requested_start,
            requested_end,
            retrieved_at_utc=retrieved_at,
            transport=ruonia_transport,
        )
    )
    key_rate_records = _load_source_safely(
        lambda: load_key_rate_daily(
            key_rate_requested_start,
            requested_end,
            retrieved_at_utc=retrieved_at,
            transport=key_rate_transport,
        )
    )
    ruonia_normalized = _normalized_source_frame(
        ruonia_records,
        source_id="cbr_ruonia_daily",
        expected_retrieved_at=retrieved_at,
    )
    key_rate_normalized = _normalized_source_frame(
        key_rate_records,
        source_id="cbr_key_rate_daily",
        expected_retrieved_at=retrieved_at,
    )

    try:
        matrix = build_external_pit_matrix(
            eligible_identities,
            ruonia_records=ruonia_records,
            key_rate_records=key_rate_records,
        )
    except PITAlignmentError as exc:
        raise Phase82AcceptanceMatrixError(str(exc)) from None
    _validate_matrix(matrix, eligible_identities=eligible_identities)

    source_fetch_manifest = _build_source_fetch_manifest(
        normalized_sources={
            "cbr_ruonia_daily": ruonia_normalized,
            "cbr_key_rate_daily": key_rate_normalized,
        },
        requested_intervals={
            "cbr_ruonia_daily": (ruonia_requested_start, requested_end),
            "cbr_key_rate_daily": (key_rate_requested_start, requested_end),
        },
        requested_end=requested_end,
        retrieved_at=retrieved_at,
    )
    coverage = _build_coverage(
        matrix,
        validation_identities=validation_identities,
    )
    staleness = _build_staleness(matrix)
    blocker_register = _build_source_blocker_register()
    input_verification = _build_input_identity_verification(
        request=request,
        observed_hashes=observed_hashes,
        eligible_identities=eligible_identities,
        validation_identities=validation_identities,
    )
    gates = _build_gate_results(
        observed_hashes=observed_hashes,
        eligible_identities=eligible_identities,
        validation_identities=validation_identities,
        matrix=matrix,
        ruonia_normalized=ruonia_normalized,
        key_rate_normalized=key_rate_normalized,
        coverage=coverage,
        source_fetch_manifest=source_fetch_manifest,
        blocker_register=blocker_register,
    )
    if not gates["G9_final_acceptance"]["passed"]:
        raise Phase82AcceptanceMatrixError("Phase 8.2 final acceptance gate failed")

    payloads: dict[str, object] = {
        "input_identity_verification.json": input_verification,
        "source_fetch_manifest.json": source_fetch_manifest,
        "ruonia_normalized.parquet": ruonia_normalized,
        "key_rate_normalized.parquet": key_rate_normalized,
        "external_pit_acceptance_matrix.parquet": matrix,
        "coverage_by_source.csv": coverage,
        "staleness_by_source.csv": staleness,
        "source_blocker_register.json": blocker_register,
        "gate_results.json": gates,
    }
    _write_exact_artifacts(request.output_dir, payloads)

    return Phase82AcceptanceResult(
        output_dir=request.output_dir,
        artifact_names=DECLARED_OUTPUT_ARTIFACTS,
        eligible_identity_count=len(eligible_identities),
        validation_identity_count=len(validation_identities),
        final_gate_passed=True,
    )


def _explicit_input_path(raw: object, flag: str, suffixes: tuple[str, ...]) -> Path:
    text = str(raw).strip()
    if not text:
        raise Phase82AcceptanceMatrixError(f"{flag} must be non-empty")
    if any(character in text for character in _GLOB_CHARS):
        raise Phase82AcceptanceMatrixError(f"{flag} must not contain glob syntax")
    if _ALIAS_PATTERN.search(text):
        raise Phase82AcceptanceMatrixError(f"{flag} must not use a mutable alias")
    path = Path(text)
    if path.suffix.lower() not in suffixes:
        raise Phase82AcceptanceMatrixError(f"{flag} suffix mismatch")
    if not path.exists() or not path.is_file():
        raise Phase82AcceptanceMatrixError(f"{flag} must identify one existing file")
    return path


def _explicit_output_path(raw: object) -> Path:
    text = str(raw).strip()
    if not text:
        raise Phase82AcceptanceMatrixError("--output-dir must be non-empty")
    if any(character in text for character in _GLOB_CHARS) or _ALIAS_PATTERN.search(text):
        raise Phase82AcceptanceMatrixError("--output-dir must be explicit and immutable")
    return Path(text)


def _validate_run_id(raw: object) -> str:
    run_id = str(raw).strip()
    if not _RUN_ID_PATTERN.fullmatch(run_id) or _ALIAS_PATTERN.search(run_id):
        raise Phase82AcceptanceMatrixError("--run-id must be non-empty and immutable")
    return run_id


def _validate_git_sha(raw: object) -> str:
    value = str(raw).strip().lower()
    if not _SHA40_PATTERN.fullmatch(value):
        raise Phase82AcceptanceMatrixError(
            "--git-commit-sha must be exactly 40 hexadecimal characters"
        )
    return value


def _parse_optional_retrieved_at(raw: object) -> datetime | None:
    if raw is None or str(raw).strip() == "":
        return None
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        value = datetime.fromisoformat(text)
    except ValueError as exc:
        raise Phase82AcceptanceMatrixError(
            "--retrieved-at-utc must be an ISO-8601 timestamp"
        ) from exc
    if value.tzinfo is None or value.utcoffset() is None:
        raise Phase82AcceptanceMatrixError("--retrieved-at-utc must include timezone UTC")
    normalized = value.astimezone(timezone.utc)
    if value.utcoffset() != timedelta(0):
        raise Phase82AcceptanceMatrixError("--retrieved-at-utc must be expressed in UTC")
    return normalized


def _input_paths(request: Phase82AcceptanceRequest) -> dict[str, Path]:
    return {
        "modeling_dataset": request.modeling_dataset_path,
        "dataset_manifest": request.dataset_manifest_path,
        "feature_schema": request.feature_schema_path,
        "m0_validation_predictions": request.m0_validation_predictions_path,
        "phase81_source_contract": request.phase81_source_contract_path,
        "phase82_experiment_contract": request.experiment_contract_path,
    }


def _validate_distinct_inputs(request: Phase82AcceptanceRequest) -> None:
    resolved = [path.resolve() for path in _input_paths(request).values()]
    if len(set(resolved)) != 6:
        raise Phase82AcceptanceMatrixError("all six input artifacts must be distinct")


def _validate_request(request: Phase82AcceptanceRequest) -> None:
    rules = (
        (request.modeling_dataset_path, "--modeling-dataset-path", (".parquet",)),
        (request.dataset_manifest_path, "--dataset-manifest-path", (".json",)),
        (request.feature_schema_path, "--feature-schema-path", (".json",)),
        (
            request.m0_validation_predictions_path,
            "--m0-validation-predictions-path",
            (".parquet",),
        ),
        (
            request.phase81_source_contract_path,
            "--phase8-1-source-contract-path",
            (".json",),
        ),
        (request.experiment_contract_path, "--experiment-contract-path", (".json",)),
    )
    for path, flag, suffixes in rules:
        _explicit_input_path(path, flag, suffixes)
    _explicit_output_path(request.output_dir)
    _validate_run_id(request.run_id)
    _validate_git_sha(request.git_commit_sha)
    if request.retrieved_at_utc is not None:
        _parse_optional_retrieved_at(request.retrieved_at_utc.isoformat())
    _validate_distinct_inputs(request)


def _assert_output_dir_absent(output_dir: Path) -> None:
    if output_dir.exists():
        raise Phase82AcceptanceMatrixError("--output-dir must not already exist")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_immutable_hashes(observed_hashes: Mapping[str, str]) -> None:
    for name, expected in EXPECTED_INPUT_SHA256.items():
        if observed_hashes.get(name) != expected:
            raise Phase82AcceptanceMatrixError(f"{name} SHA256 mismatch")


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase82AcceptanceMatrixError("input JSON artifact is invalid") from exc
    if not isinstance(payload, dict):
        raise Phase82AcceptanceMatrixError("input JSON artifact must be an object")
    return payload


def _validate_experiment_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity")
    if identity != {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": TASK_ID,
        "lane": LANE,
        "phase": PHASE,
        "execution_mode": EXECUTION_MODE,
        "status": "controlled_runtime_contract",
    }:
        raise Phase82AcceptanceMatrixError("Phase 8.2 experiment contract identity mismatch")
    if contract.get("approved_branch") != APPROVED_BRANCH:
        raise Phase82AcceptanceMatrixError("Phase 8.2 approved branch mismatch")
    if tuple(contract.get("accepted_sources", ())) != ACCEPTED_SOURCES:
        raise Phase82AcceptanceMatrixError("Phase 8.2 accepted source set mismatch")
    if contract.get("blocked_sources") != BLOCKED_SOURCE_STATUSES:
        raise Phase82AcceptanceMatrixError("Phase 8.2 blocked source set mismatch")
    cutoff = contract.get("availability_policy", {})
    if not isinstance(cutoff, Mapping) or cutoff.get("timezone") != "Europe/Moscow":
        raise Phase82AcceptanceMatrixError("Phase 8.2 cutoff timezone mismatch")
    if cutoff.get("decision_cutoff_local_time") != "08:45:00":
        raise Phase82AcceptanceMatrixError("Phase 8.2 decision cutoff mismatch")
    if cutoff.get("ruonia_eligibility") != "publication_date < target_trade_date":
        raise Phase82AcceptanceMatrixError("Phase 8.2 RUONIA policy mismatch")
    if cutoff.get("key_rate_eligibility") != "effective_date <= target_trade_date":
        raise Phase82AcceptanceMatrixError("Phase 8.2 key-rate policy mismatch")
    history = contract.get("historical_request_range", {})
    if history != {
        "ruonia": {
            "start_date": "minimum eligible target_trade_date minus 31 calendar days",
            "end_date": "maximum eligible target_trade_date",
            "history_buffer_calendar_days": HISTORY_BUFFER_CALENDAR_DAYS,
        },
        "key_rate_change_history": {
            "start_date": KEY_RATE_HISTORY_START.isoformat(),
            "end_date": "maximum eligible target_trade_date",
            "source_semantics": "official_key_rate_change_date_history",
        },
        "hidden_dynamic_widening_allowed": False,
        "missing_first_row_policy": "fail_closed",
    }:
        raise Phase82AcceptanceMatrixError("Phase 8.2 source request range mismatch")
    change_policy = contract.get("key_rate_change_point_policy", {})
    if change_policy != {
        "required_response_columns": ["Date effective", "Key rate"],
        "strictly_increasing_effective_dates": True,
        "duplicate_effective_date_allowed": False,
        "consecutive_identical_rate_allowed": False,
        "daily_date_rate_schema_allowed": False,
        "maximum_normalized_row_fraction_of_request_calendar_days": (
            KEY_RATE_MAX_NORMALIZED_ROW_FRACTION
        ),
        "all_zero_key_rate_age_allowed": False,
        "selected_effective_date_must_exist_in_normalized_source": True,
    }:
        raise Phase82AcceptanceMatrixError("Phase 8.2 key-rate change-point policy mismatch")
    if tuple(contract.get("runtime_artifacts", ())) != DECLARED_OUTPUT_ARTIFACTS:
        raise Phase82AcceptanceMatrixError("Phase 8.2 artifact inventory mismatch")
    immutable = contract.get("immutable_input_sha256")
    if immutable != EXPECTED_INPUT_SHA256:
        raise Phase82AcceptanceMatrixError("Phase 8.2 immutable input hashes mismatch")
    authority = contract.get("authority_boundary", {})
    required_false = (
        "server_apply_allowed",
        "real_external_data_acquisition_during_repository_implementation_allowed",
        "model_fit_allowed",
        "model_evaluation_allowed",
        "model_promotion_allowed",
        "strategy_promotion_allowed",
        "live_prediction_allowed",
        "broker_action_allowed",
        "trading_allowed",
    )
    if any(authority.get(field) is not False for field in required_false):
        raise Phase82AcceptanceMatrixError("Phase 8.2 authority boundary mismatch")


def _validate_phase81_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity", {})
    if identity.get("contract_id") != PHASE81_CONTRACT_ID or identity.get(
        "contract_version"
    ) != "1.0":
        raise Phase82AcceptanceMatrixError("Phase 8.1 source contract identity mismatch")
    sources = contract.get("sources", {})
    for source_id in ACCEPTED_SOURCES:
        source = sources.get(source_id, {})
        if source.get("historical_model_use_status") != "candidate_for_phase8_2":
            raise Phase82AcceptanceMatrixError(
                "Phase 8.1 accepted source status differs from candidate_for_phase8_2"
            )
    observed_blockers = {
        "moex_brent_futures_daily": sources.get("moex_brent_futures_daily", {}).get(
            "historical_model_use_status"
        ),
        "cbr_banking_liquidity_daily": sources.get(
            "cbr_banking_liquidity_daily", {}
        ).get("historical_model_use_status"),
        "cme_wti_pre_moex": sources.get("pre_moex_global_oil_market", {})
        .get("selection", {})
        .get("selected_historical_readiness_status"),
        "ine_shanghai_crude_pre_moex": sources.get("pre_moex_global_oil_market", {})
        .get("selection", {})
        .get("rejected_historical_readiness_status"),
    }
    if observed_blockers != BLOCKED_SOURCE_STATUSES:
        raise Phase82AcceptanceMatrixError("Phase 8.1 blocked source status mismatch")
    status_policy = contract.get("source_status_policy", {})
    if status_policy.get("blocked_source_can_enter_phase8_2_matrix") is not False:
        raise Phase82AcceptanceMatrixError("Phase 8.1 blocked-source policy mismatch")


def _validate_phase81_registry() -> None:
    for source_id in ACCEPTED_SOURCES:
        definition = require_phase8_2_ready(source_id)
        if definition.source_revision_status != SOURCE_REVISION_STATUSES[source_id]:
            raise Phase82AcceptanceMatrixError("source revision status mismatch")
    if (
        SOURCE_REGISTRY["cbr_key_rate_daily"].source_semantics
        != "official_key_rate_change_date_history"
    ):
        raise Phase82AcceptanceMatrixError("key-rate source semantics mismatch")
    for source_id, status in BLOCKED_SOURCE_STATUSES.items():
        definition = SOURCE_REGISTRY[source_id]
        if definition.historical_model_use_status != status:
            raise Phase82AcceptanceMatrixError("blocked source registry status mismatch")
        try:
            require_phase8_2_ready(source_id)
        except ExternalDataError:
            continue
        raise Phase82AcceptanceMatrixError("blocked source was marked Phase 8.2 ready")


def _validate_dataset_contracts(
    manifest: Mapping[str, Any], feature_schema: Mapping[str, Any]
) -> None:
    expected_targets = (
        "target_phase_label",
        "target_is_labeled",
        "target_source",
        "target_trade_date",
        "target_instrument_id",
    )
    if manifest.get("dataset_id") != DATASET_ID:
        raise Phase82AcceptanceMatrixError("dataset manifest identity mismatch")
    if manifest.get("feature_schema_id") != FEATURE_SCHEMA_ID:
        raise Phase82AcceptanceMatrixError("dataset manifest feature schema mismatch")
    if manifest.get("target_source") != TARGET_SOURCE:
        raise Phase82AcceptanceMatrixError("dataset manifest target source mismatch")
    if tuple(manifest.get("target_columns", ())) != expected_targets:
        raise Phase82AcceptanceMatrixError("dataset manifest target columns mismatch")
    if feature_schema.get("schema_id") != FEATURE_SCHEMA_ID:
        raise Phase82AcceptanceMatrixError("feature schema identity mismatch")
    if feature_schema.get("dataset_id") != DATASET_ID:
        raise Phase82AcceptanceMatrixError("feature schema dataset mismatch")
    if tuple(feature_schema.get("target_columns", ())) != expected_targets:
        raise Phase82AcceptanceMatrixError("feature schema target columns mismatch")


def _eligible_identities(dataset: pd.DataFrame) -> pd.DataFrame:
    required = (
        "target_phase_label",
        "target_is_labeled",
        "target_source",
        *IDENTITY_COLUMNS,
    )
    missing = [column for column in required if column not in dataset.columns]
    if missing:
        raise Phase82AcceptanceMatrixError(
            "modeling dataset missing identity fields: " + ", ".join(missing)
        )
    dates = pd.to_datetime(dataset["target_trade_date"], errors="coerce")
    instruments = dataset["target_instrument_id"].astype("string").str.strip()
    mask = (
        dataset["target_source"].eq(TARGET_SOURCE)
        & dataset["target_is_labeled"].eq(True)
        & dataset["target_phase_label"].isin(CLASS_ORDER)
        & dates.notna()
        & instruments.notna()
        & instruments.ne("")
    )
    identities = dataset.loc[mask, list(IDENTITY_COLUMNS)].copy()
    identities["target_trade_date"] = dates.loc[mask].dt.strftime("%Y-%m-%d")
    identities["target_instrument_id"] = instruments.loc[mask].astype(str)
    identities = identities.sort_values(list(IDENTITY_COLUMNS), kind="mergesort").reset_index(
        drop=True
    )
    if len(identities) != EXPECTED_ELIGIBLE_IDENTITIES:
        raise Phase82AcceptanceMatrixError("eligible identity count must equal 472")
    if identities.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise Phase82AcceptanceMatrixError("eligible target identity is duplicated")
    if set(identities["target_instrument_id"]) != {EXPECTED_INSTRUMENT}:
        raise Phase82AcceptanceMatrixError("eligible instrument identity mismatch")
    return identities


def _validation_identities(
    predictions: pd.DataFrame,
    *,
    eligible_identities: pd.DataFrame,
) -> pd.DataFrame:
    missing = [column for column in IDENTITY_COLUMNS if column not in predictions.columns]
    if missing:
        raise Phase82AcceptanceMatrixError(
            "M0 predictions missing identity fields: " + ", ".join(missing)
        )
    identities = predictions.loc[:, IDENTITY_COLUMNS].copy()
    dates = pd.to_datetime(identities["target_trade_date"], errors="coerce")
    instruments = identities["target_instrument_id"].astype("string").str.strip()
    if dates.isna().any() or instruments.isna().any() or instruments.eq("").any():
        raise Phase82AcceptanceMatrixError("M0 validation identity is invalid")
    identities["target_trade_date"] = dates.dt.strftime("%Y-%m-%d")
    identities["target_instrument_id"] = instruments.astype(str)
    identities = identities.sort_values(list(IDENTITY_COLUMNS), kind="mergesort").reset_index(
        drop=True
    )
    if len(identities) != EXPECTED_VALIDATION_IDENTITIES:
        raise Phase82AcceptanceMatrixError("M0 validation identity count must equal 320")
    if identities.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise Phase82AcceptanceMatrixError("M0 validation identity is duplicated")
    if set(identities["target_instrument_id"]) != {EXPECTED_INSTRUMENT}:
        raise Phase82AcceptanceMatrixError("M0 validation instrument identity mismatch")
    eligible_index = pd.MultiIndex.from_frame(eligible_identities.loc[:, IDENTITY_COLUMNS])
    validation_index = pd.MultiIndex.from_frame(identities.loc[:, IDENTITY_COLUMNS])
    if not validation_index.isin(eligible_index).all():
        raise Phase82AcceptanceMatrixError(
            "M0 validation identities differ from eligible Phase 6 identities"
        )
    return identities


def _load_source_safely(loader: Callable[[], list[dict[str, object]]]) -> list[dict[str, object]]:
    try:
        records = loader()
    except Exception:
        raise Phase82AcceptanceMatrixError("external source acquisition failed") from None
    if not records:
        raise Phase82AcceptanceMatrixError("normalized external source is empty")
    return records


def _normalized_source_frame(
    records: list[dict[str, object]],
    *,
    source_id: str,
    expected_retrieved_at: datetime,
) -> pd.DataFrame:
    required = SOURCE_REQUIRED_COLUMNS[source_id]
    frame = pd.DataFrame(records)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise Phase82AcceptanceMatrixError(
            f"{source_id} normalized fields missing: " + ", ".join(missing)
        )
    frame = frame.loc[:, required].copy()
    if not frame["source_id"].eq(source_id).all():
        raise Phase82AcceptanceMatrixError("normalized source_id mismatch")
    if not frame["historical_model_use_status"].eq("candidate_for_phase8_2").all():
        raise Phase82AcceptanceMatrixError("normalized source readiness status mismatch")
    if not frame["source_revision_status"].eq(SOURCE_REVISION_STATUSES[source_id]).all():
        raise Phase82AcceptanceMatrixError("normalized source revision status mismatch")
    if frame["raw_payload_sha256"].nunique(dropna=False) != 1:
        raise Phase82AcceptanceMatrixError("normalized source raw hash is ambiguous")
    raw_hash = str(frame["raw_payload_sha256"].iloc[0])
    if not _SHA256_PATTERN.fullmatch(raw_hash):
        raise Phase82AcceptanceMatrixError("normalized source raw hash is invalid")
    if frame["source_route"].nunique(dropna=False) != 1:
        raise Phase82AcceptanceMatrixError("normalized source route is ambiguous")
    route = str(frame["source_route"].iloc[0])
    expected_base = RUONIA_ROUTE if source_id == "cbr_ruonia_daily" else KEY_RATE_ROUTE
    _validate_public_source_route(route, expected_base=expected_base)
    expected_timestamp = expected_retrieved_at.astimezone(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    if not frame["retrieved_at_utc"].eq(expected_timestamp).all():
        raise Phase82AcceptanceMatrixError("normalized source retrieval timestamp mismatch")
    sort_fields = (
        ["publication_date", "observation_date"]
        if source_id == "cbr_ruonia_daily"
        else ["effective_date"]
    )
    frame = frame.sort_values(sort_fields, kind="mergesort").reset_index(drop=True)
    if source_id == "cbr_key_rate_daily":
        _validate_key_rate_change_history(frame)
    _assert_secret_free(frame)
    return frame


def _validate_key_rate_change_history(frame: pd.DataFrame) -> None:
    effective_dates = pd.to_datetime(frame["effective_date"], errors="raise")
    if effective_dates.duplicated().any():
        raise Phase82AcceptanceMatrixError("duplicate key-rate effective date")
    if len(effective_dates) > 1 and not effective_dates.diff().iloc[1:].gt(
        pd.Timedelta(0)
    ).all():
        raise Phase82AcceptanceMatrixError(
            "key-rate effective dates are not strictly increasing"
        )
    rates = pd.to_numeric(frame["key_rate_pct"], errors="raise").astype(float)
    if not np.isfinite(rates.to_numpy()).all():
        raise Phase82AcceptanceMatrixError("key-rate change history contains invalid rates")
    if len(rates) > 1 and rates.eq(rates.shift()).iloc[1:].any():
        raise Phase82AcceptanceMatrixError(
            "consecutive key-rate change points have identical rates"
        )


def _validate_public_source_route(route: str, *, expected_base: str) -> None:
    parsed = urlsplit(route)
    if not route.startswith(expected_base) or parsed.scheme != "https":
        raise Phase82AcceptanceMatrixError("normalized source route mismatch")
    if parsed.username or parsed.password:
        raise Phase82AcceptanceMatrixError("source route must not contain credentials")
    query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
    if query_keys - {"unidbquery.posted", "unidbquery.from", "unidbquery.to"}:
        raise Phase82AcceptanceMatrixError("source route contains an undeclared query field")


def _validate_matrix(matrix: pd.DataFrame, *, eligible_identities: pd.DataFrame) -> None:
    if tuple(matrix.columns) != MATRIX_COLUMNS:
        raise Phase82AcceptanceMatrixError("acceptance matrix column contract mismatch")
    if len(matrix) != EXPECTED_ELIGIBLE_IDENTITIES:
        raise Phase82AcceptanceMatrixError("acceptance matrix row count must equal 472")
    if matrix.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise Phase82AcceptanceMatrixError("acceptance matrix identity is duplicated")
    if not matrix.loc[:, IDENTITY_COLUMNS].equals(eligible_identities):
        raise Phase82AcceptanceMatrixError("acceptance matrix identity order mismatch")
    forbidden = set(matrix.columns) & FORBIDDEN_MATRIX_FIELDS
    token_fields = [
        column
        for column in matrix.columns
        if any(token in column.lower() for token in FORBIDDEN_MATRIX_TOKENS)
    ]
    if forbidden or token_fields:
        raise Phase82AcceptanceMatrixError("forbidden field entered acceptance matrix")
    if matrix.isna().any().any():
        raise Phase82AcceptanceMatrixError("acceptance matrix contains missing values")
    target_dates = pd.to_datetime(matrix["target_trade_date"])
    publications = pd.to_datetime(matrix["ruonia_publication_date"])
    observations = pd.to_datetime(matrix["ruonia_observation_date"])
    effective_dates = pd.to_datetime(matrix["key_rate_effective_date"])
    if not (publications < target_dates).all():
        raise Phase82AcceptanceMatrixError("RUONIA publication cutoff violation")
    if not (observations <= publications).all():
        raise Phase82AcceptanceMatrixError("RUONIA observation chronology violation")
    if not (effective_dates <= target_dates).all():
        raise Phase82AcceptanceMatrixError("key-rate effective-date violation")
    _assert_secret_free(matrix)


def _build_source_fetch_manifest(
    *,
    normalized_sources: Mapping[str, pd.DataFrame],
    requested_intervals: Mapping[str, tuple[date, date]],
    requested_end: date,
    retrieved_at: datetime,
) -> dict[str, Any]:
    sources: list[dict[str, Any]] = []
    for source_id in ACCEPTED_SOURCES:
        frame = normalized_sources[source_id]
        requested_start, source_requested_end = requested_intervals[source_id]
        if source_requested_end != requested_end:
            raise Phase82AcceptanceMatrixError("source request end-date mismatch")
        date_field = SOURCE_DATE_FIELDS[source_id]
        dates = pd.to_datetime(frame[date_field], errors="raise")
        if (
            dates.lt(pd.Timestamp(requested_start)).any()
            or dates.gt(pd.Timestamp(requested_end)).any()
        ):
            raise Phase82AcceptanceMatrixError(
                "normalized source row falls outside the declared request interval"
            )
        sources.append(
            {
                "source_id": source_id,
                "exact_requested_route": str(frame["source_route"].iloc[0]),
                "requested_start_date": requested_start.isoformat(),
                "requested_end_date": requested_end.isoformat(),
                "retrieved_at_utc": retrieved_at.astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "raw_payload_sha256": str(frame["raw_payload_sha256"].iloc[0]),
                "normalized_row_count": int(len(frame)),
                "first_business_date": dates.min().strftime("%Y-%m-%d"),
                "last_business_date": dates.max().strftime("%Y-%m-%d"),
                "source_revision_status": SOURCE_REVISION_STATUSES[source_id],
                "historical_model_use_status": "candidate_for_phase8_2",
            }
        )
    result = {
        "ruonia_history_buffer_calendar_days": HISTORY_BUFFER_CALENDAR_DAYS,
        "key_rate_fixed_start_date": KEY_RATE_HISTORY_START.isoformat(),
        "sources": sources,
    }
    _assert_secret_free(result)
    return result


def _build_coverage(
    matrix: pd.DataFrame,
    *,
    validation_identities: pd.DataFrame,
) -> pd.DataFrame:
    validation_index = pd.MultiIndex.from_frame(validation_identities)
    matrix_index = pd.MultiIndex.from_frame(matrix.loc[:, IDENTITY_COLUMNS])
    validation_mask = matrix_index.isin(validation_index)
    source_columns = {
        "cbr_ruonia_daily": RUONIA_MATRIX_COLUMNS,
        "cbr_key_rate_daily": KEY_RATE_MATRIX_COLUMNS,
    }
    rows = []
    for source_id in ACCEPTED_SOURCES:
        present = matrix.loc[:, source_columns[source_id]].notna().all(axis=1).to_numpy()
        eligible_covered = int(present.sum())
        validation_covered = int(present[validation_mask].sum())
        rows.append(
            {
                "source_id": source_id,
                "eligible_identity_count": len(matrix),
                "eligible_covered_count": eligible_covered,
                "eligible_missing_count": len(matrix) - eligible_covered,
                "eligible_coverage_pct": eligible_covered / len(matrix) * 100.0,
                "validation_identity_count": int(validation_mask.sum()),
                "validation_covered_count": validation_covered,
                "validation_missing_count": int(validation_mask.sum())
                - validation_covered,
                "validation_coverage_pct": validation_covered
                / int(validation_mask.sum())
                * 100.0,
            }
        )
    return pd.DataFrame(rows)


def _build_staleness(matrix: pd.DataFrame) -> pd.DataFrame:
    definitions = (
        (
            "cbr_ruonia_daily",
            "observation_date",
            "ruonia_observation_age_calendar_days",
            (("1 calendar day", 1, 1), ("2-3 calendar days", 2, 3), ("4-7 calendar days", 4, 7), ("more than 7 calendar days", 8, None)),
        ),
        (
            "cbr_ruonia_daily",
            "publication_date",
            "ruonia_publication_age_calendar_days",
            (("1 calendar day", 1, 1), ("2-3 calendar days", 2, 3), ("4-7 calendar days", 4, 7), ("more than 7 calendar days", 8, None)),
        ),
        (
            "cbr_key_rate_daily",
            "effective_date",
            "key_rate_age_calendar_days",
            (("0-30 calendar days", 0, 30), ("31-90 calendar days", 31, 90), ("91-180 calendar days", 91, 180), ("more than 180 calendar days", 181, None)),
        ),
    )
    rows: list[dict[str, Any]] = []
    for source_id, age_basis, column, buckets in definitions:
        values = matrix[column].astype("int64")
        summary = {
            "minimum_age_calendar_days": int(values.min()),
            "median_age_calendar_days": float(values.median()),
            "p90_age_calendar_days": float(values.quantile(0.90)),
            "p95_age_calendar_days": float(values.quantile(0.95)),
            "maximum_age_calendar_days": int(values.max()),
        }
        for label, minimum, maximum in buckets:
            mask = values.ge(minimum)
            if maximum is not None:
                mask &= values.le(maximum)
            rows.append(
                {
                    "source_id": source_id,
                    "age_basis": age_basis,
                    **summary,
                    "age_bucket": label,
                    "row_count": int(mask.sum()),
                }
            )
    return pd.DataFrame(rows)


def _build_source_blocker_register() -> dict[str, Any]:
    if set(ACCEPTED_SOURCES) & set(BLOCKED_SOURCE_STATUSES):
        raise Phase82AcceptanceMatrixError("blocked source entered accepted source set")
    return {
        "accepted_sources": list(ACCEPTED_SOURCES),
        "blocked_sources": [
            {
                "source_id": source_id,
                "historical_model_use_status": status,
                "entered_normalized_accepted_dataset": False,
                "entered_acceptance_matrix": False,
                "entered_readiness_gates": False,
                "entered_future_model_feature_allowlist": False,
            }
            for source_id, status in BLOCKED_SOURCE_STATUSES.items()
        ],
    }


def _build_input_identity_verification(
    *,
    request: Phase82AcceptanceRequest,
    observed_hashes: Mapping[str, str],
    eligible_identities: pd.DataFrame,
    validation_identities: pd.DataFrame,
) -> dict[str, Any]:
    immutable = {
        name: {
            "expected_sha256": expected,
            "observed_sha256": observed_hashes[name],
            "matches": observed_hashes[name] == expected,
        }
        for name, expected in EXPECTED_INPUT_SHA256.items()
    }
    return {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": request.run_id,
        "source_git_commit_sha": request.git_commit_sha,
        "immutable_inputs": immutable,
        "phase81_source_contract": {
            "contract_id": PHASE81_CONTRACT_ID,
            "observed_sha256": observed_hashes["phase81_source_contract"],
            "sha256_computed_at_runtime": True,
        },
        "phase82_experiment_contract": {
            "contract_id": CONTRACT_ID,
            "observed_sha256": observed_hashes["phase82_experiment_contract"],
        },
        "eligible_identity_count": len(eligible_identities),
        "validation_identity_count": len(validation_identities),
        "instrument": EXPECTED_INSTRUMENT,
        "validation_identities_are_subset_of_eligible": True,
        "identity_order": list(IDENTITY_COLUMNS),
    }


def _build_gate_results(
    *,
    observed_hashes: Mapping[str, str],
    eligible_identities: pd.DataFrame,
    validation_identities: pd.DataFrame,
    matrix: pd.DataFrame,
    ruonia_normalized: pd.DataFrame,
    key_rate_normalized: pd.DataFrame,
    coverage: pd.DataFrame,
    source_fetch_manifest: Mapping[str, Any],
    blocker_register: Mapping[str, Any],
) -> dict[str, Any]:
    target_dates = pd.to_datetime(matrix["target_trade_date"])
    publication_dates = pd.to_datetime(matrix["ruonia_publication_date"])
    observation_dates = pd.to_datetime(matrix["ruonia_observation_date"])
    effective_dates = pd.to_datetime(matrix["key_rate_effective_date"])
    validation_index = pd.MultiIndex.from_frame(validation_identities)
    matrix_index = pd.MultiIndex.from_frame(matrix.loc[:, IDENTITY_COLUMNS])
    manifest_sources = source_fetch_manifest.get("sources", [])
    manifest_provenance = (
        isinstance(manifest_sources, list)
        and len(manifest_sources) == 2
        and all(
            isinstance(item, Mapping)
            and _SHA256_PATTERN.fullmatch(str(item.get("raw_payload_sha256", "")))
            and str(item.get("exact_requested_route", "")).startswith("https://")
            and str(item.get("retrieved_at_utc", "")).endswith("Z")
            and item.get("historical_model_use_status") == "candidate_for_phase8_2"
            for item in manifest_sources
        )
    )
    g1 = (
        all(observed_hashes.get(name) == expected for name, expected in EXPECTED_INPUT_SHA256.items())
        and len(eligible_identities) == EXPECTED_ELIGIBLE_IDENTITIES
        and len(validation_identities) == EXPECTED_VALIDATION_IDENTITIES
        and set(eligible_identities["target_instrument_id"]) == {EXPECTED_INSTRUMENT}
    )
    g2 = (
        len(matrix) == EXPECTED_ELIGIBLE_IDENTITIES
        and not matrix.duplicated(list(IDENTITY_COLUMNS)).any()
        and matrix.loc[:, IDENTITY_COLUMNS].equals(eligible_identities)
        and validation_index.isin(matrix_index).all()
    )
    g3 = bool(
        (publication_dates < target_dates).all()
        and (observation_dates <= publication_dates).all()
        and matrix.loc[:, RUONIA_MATRIX_COLUMNS].notna().all().all()
        and _latest_ruonia_selection_matches(matrix, ruonia_normalized)
    )
    g4 = bool(
        (effective_dates <= target_dates).all()
        and np.isfinite(matrix["key_rate_pct"].to_numpy(float)).all()
        and _latest_key_rate_selection_matches(matrix, key_rate_normalized)
        and _key_rate_semantic_integrity(
            matrix,
            key_rate_normalized,
            source_fetch_manifest=source_fetch_manifest,
        )
    )
    g5 = bool(
        coverage["eligible_coverage_pct"].eq(100.0).all()
        and coverage["validation_coverage_pct"].eq(100.0).all()
        and not matrix.isna().any().any()
    )
    blocked_rows = blocker_register.get("blocked_sources", [])
    g6 = bool(
        len(blocked_rows) == 4
        and {item.get("source_id") for item in blocked_rows}
        == set(BLOCKED_SOURCE_STATUSES)
        and all(
            not item.get("entered_normalized_accepted_dataset")
            and not item.get("entered_acceptance_matrix")
            and not item.get("entered_readiness_gates")
            and not item.get("entered_future_model_feature_allowlist")
            for item in blocked_rows
        )
        and not any(
            token in column.lower()
            for column in matrix.columns
            for token in ("cme", "wti", "brent", "liquidity", "ine_")
        )
    )
    normalized_provenance = all(
        set(
            (
                "source_id",
                "source_route",
                "retrieved_at_utc",
                "raw_payload_sha256",
                "source_revision_status",
                "historical_model_use_status",
            )
        ).issubset(frame.columns)
        and frame["source_id"].eq(source_id).all()
        and frame["historical_model_use_status"].eq("candidate_for_phase8_2").all()
        for source_id, frame in (
            ("cbr_ruonia_daily", ruonia_normalized),
            ("cbr_key_rate_daily", key_rate_normalized),
        )
    )
    g7 = bool(manifest_provenance and normalized_provenance)
    g8 = bool(
        tuple(matrix.columns) == MATRIX_COLUMNS
        and not (set(matrix.columns) & FORBIDDEN_MATRIX_FIELDS)
        and not any(
            token in column.lower()
            for column in matrix.columns
            for token in FORBIDDEN_MATRIX_TOKENS
        )
    )
    gates: dict[str, Any] = {
        "G1_input_identity": {"passed": bool(g1)},
        "G2_exact_identity_preservation": {"passed": bool(g2)},
        "G3_ruonia_point_in_time_correctness": {"passed": bool(g3)},
        "G4_key_rate_point_in_time_correctness": {"passed": bool(g4)},
        "G5_coverage": {"passed": bool(g5)},
        "G6_blocked_source_exclusion": {"passed": bool(g6)},
        "G7_provenance": {"passed": bool(g7)},
        "G8_leakage_and_scope": {
            "passed": bool(g8),
            "model_fit_performed": False,
            "model_evaluation_performed": False,
            "threshold_label_calibration_or_fold_changed": False,
        },
    }
    return _finalize_gate_results(gates)


def _latest_ruonia_selection_matches(
    matrix: pd.DataFrame, normalized: pd.DataFrame
) -> bool:
    source = normalized.loc[:, ["observation_date", "publication_date"]].copy()
    source["publication_date"] = pd.to_datetime(source["publication_date"])
    source = source.sort_values("publication_date", kind="mergesort").reset_index(drop=True)
    for row in matrix.loc[
        :, ["target_trade_date", "ruonia_observation_date", "ruonia_publication_date"]
    ].itertuples(index=False):
        target = pd.Timestamp(row.target_trade_date)
        eligible = source.loc[source["publication_date"] < target]
        if eligible.empty:
            return False
        selected = eligible.iloc[-1]
        if str(selected["observation_date"]) != row.ruonia_observation_date:
            return False
        if pd.Timestamp(selected["publication_date"]).strftime("%Y-%m-%d") != row.ruonia_publication_date:
            return False
    return True


def _latest_key_rate_selection_matches(
    matrix: pd.DataFrame, normalized: pd.DataFrame
) -> bool:
    source = normalized.loc[:, ["effective_date"]].copy()
    source["effective_date"] = pd.to_datetime(source["effective_date"])
    source = source.sort_values("effective_date", kind="mergesort").reset_index(drop=True)
    for row in matrix.loc[
        :, ["target_trade_date", "key_rate_effective_date"]
    ].itertuples(index=False):
        target = pd.Timestamp(row.target_trade_date)
        eligible = source.loc[source["effective_date"] <= target]
        if eligible.empty:
            return False
        selected = pd.Timestamp(eligible["effective_date"].iloc[-1]).strftime("%Y-%m-%d")
        if selected != row.key_rate_effective_date:
            return False
    return True


def _key_rate_semantic_integrity(
    matrix: pd.DataFrame,
    normalized: pd.DataFrame,
    *,
    source_fetch_manifest: Mapping[str, Any],
) -> bool:
    try:
        effective_dates = pd.to_datetime(normalized["effective_date"], errors="raise")
        rates = pd.to_numeric(normalized["key_rate_pct"], errors="raise").astype(float)
        ages = pd.to_numeric(
            matrix["key_rate_age_calendar_days"], errors="raise"
        ).astype("int64")
    except (KeyError, TypeError, ValueError):
        return False
    if normalized.empty or ages.empty:
        return False
    strict_dates = bool(
        not effective_dates.duplicated().any()
        and (
            len(effective_dates) == 1
            or effective_dates.diff().iloc[1:].gt(pd.Timedelta(0)).all()
        )
    )
    changing_rates = bool(
        np.isfinite(rates.to_numpy()).all()
        and (len(rates) == 1 or not rates.eq(rates.shift()).iloc[1:].any())
    )
    selected_dates = set(matrix["key_rate_effective_date"].astype(str))
    normalized_dates = set(effective_dates.dt.strftime("%Y-%m-%d"))
    selected_dates_exist = selected_dates.issubset(normalized_dates)
    key_manifest = next(
        (
            item
            for item in source_fetch_manifest.get("sources", [])
            if isinstance(item, Mapping)
            and item.get("source_id") == "cbr_key_rate_daily"
        ),
        None,
    )
    if not isinstance(key_manifest, Mapping):
        return False
    try:
        requested_start = date.fromisoformat(str(key_manifest["requested_start_date"]))
        requested_end = date.fromisoformat(str(key_manifest["requested_end_date"]))
    except (KeyError, TypeError, ValueError):
        return False
    request_calendar_days = (requested_end - requested_start).days + 1
    sparse_change_points = bool(
        request_calendar_days > 0
        and len(normalized)
        <= request_calendar_days * KEY_RATE_MAX_NORMALIZED_ROW_FRACTION
    )
    return bool(
        strict_dates
        and changing_rates
        and sparse_change_points
        and ages.max() > 0
        and not ages.eq(0).all()
        and selected_dates_exist
    )


def _finalize_gate_results(gates: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    finalized = {name: dict(result) for name, result in gates.items()}
    preceding = [name for name in finalized if name != "G9_final_acceptance"]
    finalized.pop("G9_final_acceptance", None)
    finalized["G9_final_acceptance"] = {
        "passed": all(bool(finalized[name].get("passed")) for name in preceding),
        "requires": preceding,
    }
    return finalized


def _assert_secret_free(payload: object) -> None:
    if isinstance(payload, pd.DataFrame):
        text = payload.to_json(orient="records", date_format="iso")
    else:
        text = json.dumps(payload, ensure_ascii=False, default=str)
    if any(pattern.search(text) for pattern in _SENSITIVE_PATTERNS):
        raise Phase82AcceptanceMatrixError("sensitive credential-like content refused")


def _write_exact_artifacts(output_dir: Path, payloads: Mapping[str, object]) -> None:
    if tuple(payloads) != DECLARED_OUTPUT_ARTIFACTS:
        raise Phase82AcceptanceMatrixError("undeclared runtime artifact inventory")
    _assert_output_dir_absent(output_dir)
    for payload in payloads.values():
        _assert_secret_free(payload)
    output_dir.mkdir(parents=True, exist_ok=False)
    for name in DECLARED_OUTPUT_ARTIFACTS:
        path = output_dir / name
        payload = payloads[name]
        if name.endswith(".json"):
            path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )
        elif name.endswith(".parquet"):
            assert isinstance(payload, pd.DataFrame)
            payload.to_parquet(path, index=False)
        elif name.endswith(".csv"):
            assert isinstance(payload, pd.DataFrame)
            payload.to_csv(path, index=False)
        else:  # pragma: no cover - constant inventory prevents this branch
            raise Phase82AcceptanceMatrixError("unsupported declared artifact suffix")
    observed = tuple(sorted(path.name for path in output_dir.iterdir() if path.is_file()))
    if observed != tuple(sorted(DECLARED_OUTPUT_ARTIFACTS)):
        raise Phase82AcceptanceMatrixError("runtime artifact inventory differs from contract")


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    run_from_args(args)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
