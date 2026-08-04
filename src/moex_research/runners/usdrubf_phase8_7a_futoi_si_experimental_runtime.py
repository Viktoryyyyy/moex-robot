from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final, Mapping

import pandas as pd

from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_runtime as base,
)

PROJECT: Final[str] = base.PROJECT
TASK_ID: Final[str] = (
    "ema_3_19_ai_phase_8_7a_futoi_si_historical_runtime_enablement_v1"
)
POLICY_CONTRACT_ID: Final[str] = (
    "usdrubf_phase8_7a_futoi_si_experimental_runtime_policy_v2"
)
POLICY_CONTRACT_VERSION: Final[str] = "2.0"
AUTHORITY_MODE: Final[str] = "futoi_si_historical_experimental_only"
POLICY_FLAG: Final[str] = "--experimental-authority-contract-path"
RUNTIME_AUTHORITY_FLAG: Final[str] = "--runtime-authority-evidence-path"
POLICY_REPO_PATH: Final[str] = (
    "contracts/experiments/"
    "usdrubf_phase8_7a_futoi_si_experimental_runtime_authority_v1.json"
)
AUTHORIZED_DATA_ROOT: Final[Path] = Path("/home/trader/moex_bot/data")
OUTPUT_PARENT_RELATIVE: Final[Path] = Path(
    "research/ema_3_19_ai/phase8_7a_futoi_si_source_validation"
)
EXPERIMENTAL_STATUS: Final[str] = (
    "moex_futoi_si_experimental_dataset_materialized"
)
FAIL_STATUS: Final[str] = "moex_futoi_si_source_not_ready"
TECHNICAL_GATES: Final[tuple[str, ...]] = (
    "G1_immutable_inputs",
    "G2_exact_route_and_transport",
    "G4_schema_and_pairing",
    "G6_exact_coverage",
    "G7_numerical_and_chronology",
    "G8_provenance_and_no_leakage",
)
AUTHORIZATION_ID_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.:-]{7,127}$"
)
SHA40_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class ExperimentalRuntimeRequest:
    base_request: base.RuntimeRequest
    policy_contract_path: Path
    runtime_authority_evidence_path: Path


def build_argument_parser() -> argparse.ArgumentParser:
    parser = base.build_argument_parser()
    parser.prog = (
        "python -m moex_research.runners."
        "usdrubf_phase8_7a_futoi_si_experimental_runtime"
    )
    parser.add_argument(POLICY_FLAG, required=True)
    parser.add_argument(RUNTIME_AUTHORITY_FLAG, required=True)
    return parser


def request_from_args(args: argparse.Namespace) -> ExperimentalRuntimeRequest:
    base_request = base.request_from_args(args)
    policy_path = base._input_file(
        getattr(args, "experimental_authority_contract_path"),
        ".json",
        POLICY_FLAG,
    )
    authority_path = base._input_file(
        getattr(args, "runtime_authority_evidence_path"),
        ".json",
        RUNTIME_AUTHORITY_FLAG,
    )
    paths = {
        path.resolve()
        for name, path in base_request.__dict__.items()
        if name.endswith("_path")
    }
    if policy_path.resolve() in paths or authority_path.resolve() in paths:
        raise base.validation.FutoiSiSourceValidationError(
            "experimental policy and runtime authority must be distinct inputs",
            blocker="provenance_not_sufficient",
        )
    if policy_path.resolve() == authority_path.resolve():
        raise base.validation.FutoiSiSourceValidationError(
            "runtime authority evidence cannot equal the checked-in policy",
            blocker="provenance_not_sufficient",
        )
    return ExperimentalRuntimeRequest(
        base_request=base_request,
        policy_contract_path=policy_path,
        runtime_authority_evidence_path=authority_path,
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _data_root() -> Path:
    raw = str(os.environ.get("MOEX_DATA_ROOT") or "").strip()
    if not raw:
        raise base.validation.FutoiSiSourceValidationError(
            "MOEX_DATA_ROOT is required for experimental runtime",
            blocker="provenance_not_sufficient",
        )
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute() or candidate.resolve() != AUTHORIZED_DATA_ROOT:
        raise base.validation.FutoiSiSourceValidationError(
            "MOEX_DATA_ROOT differs from the canonical data root",
            blocker="provenance_not_sufficient",
        )
    try:
        mode = AUTHORIZED_DATA_ROOT.lstat().st_mode
    except OSError as exc:
        raise base.validation.FutoiSiSourceValidationError(
            "canonical data root is not accessible",
            blocker="provenance_not_sufficient",
        ) from exc
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise base.validation.FutoiSiSourceValidationError(
            "canonical data root must be a physical directory",
            blocker="provenance_not_sufficient",
        )
    return AUTHORIZED_DATA_ROOT


def _validate_descendant(
    root: Path,
    relative: Path,
    *,
    leaf_must_not_exist: bool,
) -> Path:
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise base.validation.FutoiSiSourceValidationError(
            "canonical output descendant is malformed",
            blocker="provenance_not_sufficient",
        )
    current = root
    for index, part in enumerate(relative.parts):
        current = current / part
        is_leaf = index == len(relative.parts) - 1
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise base.validation.FutoiSiSourceValidationError(
                "canonical output descendant cannot be inspected",
                blocker="provenance_not_sufficient",
            ) from exc
        if stat.S_ISLNK(mode):
            raise base.validation.FutoiSiSourceValidationError(
                "symlinked canonical output descendant is forbidden",
                blocker="provenance_not_sufficient",
            )
        if not is_leaf and not stat.S_ISDIR(mode):
            raise base.validation.FutoiSiSourceValidationError(
                "canonical output ancestor must be a directory",
                blocker="provenance_not_sufficient",
            )
        if is_leaf and leaf_must_not_exist:
            raise base.validation.FutoiSiSourceValidationError(
                "experimental output directory must not pre-exist",
                blocker="provenance_not_sufficient",
            )
        try:
            resolved = current.resolve(strict=True)
        except OSError as exc:
            raise base.validation.FutoiSiSourceValidationError(
                "canonical output descendant cannot be resolved",
                blocker="provenance_not_sufficient",
            ) from exc
        if not resolved.is_relative_to(root):
            raise base.validation.FutoiSiSourceValidationError(
                "canonical output descendant escapes the data root",
                blocker="provenance_not_sufficient",
            )
    return root / relative


def _canonical_output_path(run_id: str, *, require_absent: bool) -> Path:
    relative = OUTPUT_PARENT_RELATIVE / f"run_id={run_id}"
    return _validate_descendant(
        _data_root(),
        relative,
        leaf_must_not_exist=require_absent,
    )


def _run_git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    value = completed.stdout.strip()
    if completed.returncode != 0 or not value:
        raise base.validation.FutoiSiSourceValidationError(
            "applied git state cannot prove runtime provenance",
            blocker="provenance_not_sufficient",
        )
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_policy_contract(request: ExperimentalRuntimeRequest) -> dict[str, Any]:
    path = request.policy_contract_path
    repo_root = _repo_root()
    canonical_path = (repo_root / POLICY_REPO_PATH).resolve()
    if path.resolve() != canonical_path:
        raise base.validation.FutoiSiSourceValidationError(
            "experimental policy must be read from its canonical repository path",
            blocker="provenance_not_sufficient",
        )
    head = _run_git(repo_root, "rev-parse", "HEAD").lower()
    if head != request.base_request.git_commit_sha:
        raise base.validation.FutoiSiSourceValidationError(
            "runtime git SHA differs from applied repository HEAD",
            blocker="provenance_not_sufficient",
        )
    tracked_blob = _run_git(
        repo_root,
        "rev-parse",
        f"{head}:{POLICY_REPO_PATH}",
    ).lower()
    actual_blob = base._git_blob_sha1(path)
    if tracked_blob != actual_blob:
        raise base.validation.FutoiSiSourceValidationError(
            "experimental policy differs from the applied tracked blob",
            blocker="provenance_not_sufficient",
        )
    payload = base._json(path)
    identity = payload.get("contract_identity")
    parent = payload.get("parent_contract")
    boundary = payload.get("policy_boundary")
    authority = payload.get("authority_boundaries")
    gates = payload.get("gate_policy")
    runtime = payload.get("runtime_policy")
    if not all(
        isinstance(item, Mapping)
        for item in (identity, parent, boundary, authority, gates, runtime)
    ):
        raise base.validation.FutoiSiSourceValidationError(
            "experimental runtime policy structure mismatch",
            blocker="provenance_not_sufficient",
        )
    assert isinstance(identity, Mapping)
    assert isinstance(parent, Mapping)
    assert isinstance(boundary, Mapping)
    assert isinstance(authority, Mapping)
    assert isinstance(gates, Mapping)
    assert isinstance(runtime, Mapping)
    forbidden = (
        "phase8_7b_feature_computation_allowed",
        "model_fitting_allowed",
        "production_prediction_allowed",
        "model_or_strategy_promotion_allowed",
        "raw_payload_redistribution_allowed",
        "broker_action_allowed",
        "trading_action_allowed",
    )
    if (
        identity.get("project") != PROJECT
        or identity.get("task_id") != TASK_ID
        or identity.get("contract_id") != POLICY_CONTRACT_ID
        or identity.get("contract_version") != POLICY_CONTRACT_VERSION
        or identity.get("phase") != "8.7A"
        or identity.get("status") != "experimental_runtime_policy_active"
        or parent.get("git_blob_sha1") != base.CONTRACT_GIT_BLOB_SHA1
        or parent.get("source_ticker") != base.validation.SOURCE_TICKER
        or parent.get("target_instrument_id")
        != base.validation.TARGET_INSTRUMENT_ID
        or boundary.get("checked_in_policy_is_runtime_authority") is not False
        or boundary.get("separate_runtime_authority_evidence_required") is not True
        or boundary.get("required_runtime_authority_flag")
        != RUNTIME_AUTHORITY_FLAG
        or boundary.get("runtime_authority_must_not_be_stored_in_repository")
        is not True
        or boundary.get("canonical_data_root")
        != AUTHORIZED_DATA_ROOT.as_posix()
        or boundary.get("module_claims_global_single_use") is not False
        or authority.get("mode") != AUTHORITY_MODE
        or authority.get("approved_by") != "PM_L2_PHASE_OWNER"
        or authority.get("historical_authenticated_retrieval_allowed") is not True
        or authority.get("phase8_7a_source_validation_allowed") is not True
        or any(authority.get(name) is not False for name in forbidden)
        or tuple(
            gates.get("experimental_dataset_status_requires_technical_gates")
            or ()
        )
        != TECHNICAL_GATES
        or gates.get("g3_or_g5_must_not_be_forced_to_pass") is not True
        or gates.get("experimental_dataset_status") != EXPERIMENTAL_STATUS
        or gates.get("failure_status") != FAIL_STATUS
        or runtime.get("required_policy_flag") != POLICY_FLAG
        or runtime.get("output_artifact_count") != 10
        or runtime.get("fallback_or_substitution_allowed") is not False
        or runtime.get("raw_response_persistence_allowed") is not False
    ):
        raise base.validation.FutoiSiSourceValidationError(
            "experimental runtime policy mismatch",
            blocker="provenance_not_sufficient",
        )
    return {
        "contract_id": POLICY_CONTRACT_ID,
        "contract_version": POLICY_CONTRACT_VERSION,
        "git_blob_sha1": actual_blob,
        "sha256": _sha256(path),
    }


def _aware_timestamp(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _verify_runtime_authority(
    request: ExperimentalRuntimeRequest,
) -> dict[str, Any]:
    path = request.runtime_authority_evidence_path
    try:
        mode = path.lstat().st_mode
    except OSError as exc:
        raise base.validation.FutoiSiSourceValidationError(
            "runtime authority evidence is not accessible",
            blocker="provenance_not_sufficient",
        ) from exc
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise base.validation.FutoiSiSourceValidationError(
            "runtime authority evidence must be a physical regular file",
            blocker="provenance_not_sufficient",
        )
    repo_root = _repo_root().resolve()
    if path.resolve().is_relative_to(repo_root):
        raise base.validation.FutoiSiSourceValidationError(
            "runtime authority evidence must not be stored in the repository",
            blocker="provenance_not_sufficient",
        )
    payload = base._json(path)
    base_request = request.base_request
    expected_output = _canonical_output_path(
        base_request.run_id,
        require_absent=True,
    )
    forbidden = (
        "phase8_7b_feature_computation_allowed",
        "model_fitting_allowed",
        "production_prediction_allowed",
        "model_or_strategy_promotion_allowed",
        "raw_payload_redistribution_allowed",
        "broker_action_allowed",
        "trading_action_allowed",
    )
    authorization_id = str(payload.get("authorization_id") or "").strip()
    authority_sha = str(payload.get("git_commit_sha") or "").strip().lower()
    if (
        payload.get("project") != PROJECT
        or payload.get("task_id") != TASK_ID
        or not AUTHORIZATION_ID_PATTERN.fullmatch(authorization_id)
        or payload.get("approved_by") != "PM_L2_PHASE_OWNER"
        or payload.get("mode") != AUTHORITY_MODE
        or not SHA40_PATTERN.fullmatch(authority_sha)
        or authority_sha != base_request.git_commit_sha
        or payload.get("run_id") != base_request.run_id
        or payload.get("data_root") != AUTHORIZED_DATA_ROOT.as_posix()
        or payload.get("output_dir") != expected_output.as_posix()
        or not _aware_timestamp(payload.get("issued_at"))
        or payload.get("historical_authenticated_retrieval_allowed") is not True
        or payload.get("phase8_7a_source_validation_allowed") is not True
        or any(payload.get(name) is not False for name in forbidden)
    ):
        raise base.validation.FutoiSiSourceValidationError(
            "runtime authority evidence does not match the exact invocation",
            blocker="provenance_not_sufficient",
        )
    if base_request.output_dir.absolute() != expected_output:
        raise base.validation.FutoiSiSourceValidationError(
            "runtime output directory differs from exact authority evidence",
            blocker="provenance_not_sufficient",
        )
    return {
        "authorization_id": authorization_id,
        "approved_by": "PM_L2_PHASE_OWNER",
        "mode": AUTHORITY_MODE,
        "git_commit_sha": authority_sha,
        "run_id": base_request.run_id,
        "data_root": AUTHORIZED_DATA_ROOT.as_posix(),
        "output_dir": expected_output.as_posix(),
        "issued_at": str(payload.get("issued_at")),
        "evidence_sha256": _sha256(path),
        "global_single_use_claimed": False,
        "production_use_allowed": False,
        "feature_computation_allowed": False,
        "model_fitting_allowed": False,
        "promotion_allowed": False,
        "trading_allowed": False,
    }


def _source_error_record(
    *,
    trade_date_value: str | None,
    blocker: str,
    reason: str,
) -> dict[str, object]:
    return {
        "trade_date": trade_date_value,
        "blocker": blocker,
        "reason": reason,
    }


def execute(request: ExperimentalRuntimeRequest) -> dict[str, object]:
    policy_summary = _verify_policy_contract(request)
    authority_summary = _verify_runtime_authority(request)
    base_request = request.base_request
    input_hashes = base._verify_frozen_inputs(base_request)
    input_hashes["experimental_runtime_policy"] = _sha256(
        request.policy_contract_path
    )
    input_hashes["runtime_authority_evidence"] = _sha256(
        request.runtime_authority_evidence_path
    )

    modeling = pd.read_parquet(base_request.modeling_dataset_path)
    validation_predictions = pd.read_parquet(
        base_request.m0_validation_predictions_path
    )
    eligible, validation_ids = base._identity_frames(
        modeling,
        validation_predictions,
    )
    license_passed, license_validation = base._license_access_validation(
        base._json(base_request.license_access_evidence_path)
    )
    pit_evidence = base._json(base_request.pit_semantics_evidence_path)
    pit_semantics_verified = base._pit_semantics_passed(pit_evidence)

    pairs: list[base.validation.FutoiDailyPair] = []
    schema_columns: tuple[str, ...] | None = None
    source_errors: list[dict[str, object]] = []
    if not license_passed:
        source_errors.append(
            _source_error_record(
                trade_date_value=None,
                blocker="provider_license_and_access_terms_not_documented",
                reason=(
                    "retrieval is authorized only for the exact experimental "
                    "invocation; production use and redistribution remain prohibited"
                ),
            )
        )
    if not pit_semantics_verified:
        source_errors.append(
            _source_error_record(
                trade_date_value=None,
                blocker="historical_pit_revision_semantics_not_proven",
                reason=(
                    "historical measurement may proceed, but G5 and production "
                    "entry remain blocked"
                ),
            )
        )

    token: str | None = None
    try:
        token = base.validation.algopack_http.load_algopack_token()
    except base.validation.algopack_http.AlgoPackHttpError as exc:
        source_errors.append(
            _source_error_record(
                trade_date_value=None,
                blocker=exc.transport_outcome,
                reason=str(exc),
            )
        )

    if token is not None:
        for value in sorted(eligible.prior_trade_date.unique()):
            source_date = date.fromisoformat(str(value))
            try:
                pair, columns = base.validation.load_futoi_daily_pair(
                    source_date,
                    bearer_token=token,
                )
            except base.validation.FutoiSiSourceValidationError as exc:
                source_errors.append(
                    _source_error_record(
                        trade_date_value=source_date.isoformat(),
                        blocker=exc.blocker,
                        reason=str(exc),
                    )
                )
                continue
            if schema_columns is None:
                schema_columns = columns
            elif schema_columns != columns:
                source_errors.append(
                    _source_error_record(
                        trade_date_value=source_date.isoformat(),
                        blocker="official_schema_not_stable",
                        reason="FUTOI schema changed across requests",
                    )
                )
                break
            pairs.append(pair)

    matrix, diagnostics = base.validation.build_futoi_pit_acceptance_matrix(
        eligible,
        pairs,
    )
    coverage = base.validation.coverage_by_source(matrix, validation_ids)
    transport_exercised = bool(pairs)
    route_validation = {
        "official_service": base.EXPECTED_LICENSE_PROVIDER,
        "host": base.validation.ALGOPACK_HOST,
        "exact_path": base.validation.FUTOI_PATH,
        "source_ticker": base.validation.SOURCE_TICKER,
        "target_security_id": base.validation.TARGET_SECURITY_ID,
        "storage_family_code": base.validation.STORAGE_FAMILY_CODE,
        "token_environment_variable": "MOEX_ALGOPACK_TOKEN",
        "moex_api_key_alias_allowed": False,
        "redirects_allowed": False,
        "fallback_used": False,
        "one_trade_date_per_request": True,
        "latest": 1,
        "request_attempted": token is not None,
        "successful_request_count": len(pairs),
        "transport_exercised": transport_exercised,
        "route_validated": transport_exercised,
        "runtime_mode": AUTHORITY_MODE,
        "runtime_authorization_id": authority_summary["authorization_id"],
    }
    schema_profile = {
        "required_fields": list(base.validation.RAW_REQUIRED_FIELDS),
        "observed_columns": list(schema_columns or ()),
        "participant_groups": list(base.validation.PARTICIPANT_GROUPS),
        "pair_key": ["trade_date", "moment", "sess_id"],
        "cross_group_seqnum_equality_required": False,
        "canonical_normalizer": (
            "moex_data.futures.futoi_raw_loader.normalize_futoi"
        ),
        "canonical_schema_version": "futures_futoi_5m_raw.v1",
        "daily_pair_count": len(pairs),
        "schema_stable": bool(schema_columns)
        and not any(
            item["blocker"] == "official_schema_not_stable"
            for item in source_errors
        ),
        "pit_evidence": pit_evidence,
        "runtime_mode": AUTHORITY_MODE,
    }
    numerical_integrity = not any(
        item["blocker"] == "numerical_or_chronology_integrity_failure"
        for item in source_errors
    )
    provenance_passed = not any(
        item["blocker"]
        in {"provenance_not_sufficient", "official_route_not_reproducible"}
        for item in source_errors
    )
    gates = base.validation.evaluate_gates(
        immutable_inputs_verified=True,
        eligible_identity_count=len(eligible),
        validation_identity_count=len(validation_ids),
        route_validated=transport_exercised,
        license_access_passed=license_passed,
        schema_stable=bool(schema_profile["schema_stable"]),
        pit_semantics_verified=pit_semantics_verified,
        matrix=matrix,
        coverage=coverage,
        diagnostics=diagnostics,
        numerical_integrity_passed=numerical_integrity,
        provenance_passed=provenance_passed,
    )
    failed_gates = [
        name for name, result in gates.items() if not bool(result["passed"])
    ]
    technical_gates_passed = all(
        bool(gates[name]["passed"]) for name in TECHNICAL_GATES
    )
    final_status = (
        EXPERIMENTAL_STATUS if technical_gates_passed else FAIL_STATUS
    )
    blockers = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": base_request.run_id,
        "final_status": final_status,
        "failed_gates": failed_gates,
        "technical_gates_passed": technical_gates_passed,
        "experimental_runtime_policy": policy_summary,
        "runtime_authority": authority_summary,
        "blockers": source_errors,
        "historical_model_use_status": (
            "experimental_only" if technical_gates_passed else "blocked"
        ),
    }
    input_verification = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": base_request.run_id,
        "git_commit_sha": base_request.git_commit_sha,
        "eligible_identity_count": len(eligible),
        "validation_identity_count": len(validation_ids),
        "input_hashes": input_hashes,
        "expected_frozen_sha256": base.EXPECTED_FROZEN_SHA256,
        "expected_parent_contract_git_blob_sha1": base.CONTRACT_GIT_BLOB_SHA1,
        "experimental_runtime_policy": policy_summary,
        "runtime_authority": authority_summary,
        "immutable_inputs_verified": True,
    }
    _canonical_output_path(base_request.run_id, require_absent=True)
    artifact_names = base.validation.write_validation_artifacts(
        base_request.output_dir,
        input_identity_verification=input_verification,
        route_validation=route_validation,
        license_validation=license_validation,
        schema_profile=schema_profile,
        pairs=pairs,
        matrix=matrix,
        coverage=coverage,
        diagnostics=diagnostics,
        blockers=blockers,
        gates={
            "project": PROJECT,
            "task_id": TASK_ID,
            "run_id": base_request.run_id,
            "final_status": final_status,
            "failed_gates": failed_gates,
            "technical_gates_passed": technical_gates_passed,
            "experimental_runtime_policy": policy_summary,
            "runtime_authority": authority_summary,
            "gates": gates,
        },
    )
    if len(artifact_names) != 10:
        raise base.validation.FutoiSiSourceValidationError(
            "experimental runtime artifact inventory mismatch",
            blocker="provenance_not_sufficient",
        )
    return {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": base_request.run_id,
        "output_dir": str(base_request.output_dir),
        "artifact_names": list(artifact_names),
        "artifact_count": len(artifact_names),
        "eligible_identity_count": len(eligible),
        "validation_identity_count": len(validation_ids),
        "daily_pair_count": len(pairs),
        "final_status": final_status,
        "failed_gates": failed_gates,
        "technical_gates_passed": technical_gates_passed,
        "runtime_authorization_id": authority_summary["authorization_id"],
        "production_use_allowed": False,
    }


def main(argv: list[str] | None = None) -> int:
    request = request_from_args(build_argument_parser().parse_args(argv))
    result = execute(request)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
