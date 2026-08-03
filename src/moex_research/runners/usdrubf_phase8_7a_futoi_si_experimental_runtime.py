from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import date
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
AUTHORITY_CONTRACT_ID: Final[str] = (
    "usdrubf_phase8_7a_futoi_si_experimental_runtime_authority_v1"
)
AUTHORITY_CONTRACT_VERSION: Final[str] = "1.0"
AUTHORITY_MODE: Final[str] = "futoi_si_historical_experimental_only"
AUTHORITY_CONTRACT_GIT_BLOB_SHA1: Final[str] = "ee4e0bedb90085dbcea883b9cb2cda09ec37e3c0"
AUTHORITY_FLAG: Final[str] = "--experimental-authority-contract-path"
EXPERIMENTAL_STATUS: Final[str] = (
    "moex_futoi_si_experimental_dataset_materialized"
)
FAIL_STATUS: Final[str] = "moex_futoi_si_source_not_ready"
TECHNICAL_GATES: Final[tuple[str, ...]] = (
    "G1",
    "G2",
    "G4",
    "G6",
    "G7",
    "G8",
)


@dataclass(frozen=True)
class ExperimentalRuntimeRequest:
    base_request: base.RuntimeRequest
    authority_contract_path: Path


def build_argument_parser() -> argparse.ArgumentParser:
    parser = base.build_argument_parser()
    parser.prog = (
        "python -m moex_research.runners."
        "usdrubf_phase8_7a_futoi_si_experimental_runtime"
    )
    parser.add_argument(AUTHORITY_FLAG, required=True)
    return parser


def request_from_args(args: argparse.Namespace) -> ExperimentalRuntimeRequest:
    base_request = base.request_from_args(args)
    authority_path = base._input_file(
        getattr(args, "experimental_authority_contract_path"),
        ".json",
        AUTHORITY_FLAG,
    )
    existing_inputs = {
        path.resolve()
        for name, path in base_request.__dict__.items()
        if name.endswith("_path")
    }
    if authority_path.resolve() in existing_inputs:
        raise base.validation.FutoiSiSourceValidationError(
            "experimental authority contract must be a distinct immutable input",
            blocker="provenance_not_sufficient",
        )
    return ExperimentalRuntimeRequest(
        base_request=base_request,
        authority_contract_path=authority_path,
    )


def _verify_experimental_authority(path: Path) -> dict[str, Any]:
    payload = base._json(path)
    identity = payload.get("contract_identity")
    parent = payload.get("parent_contract")
    authority = payload.get("authority")
    gate_policy = payload.get("gate_policy")
    runtime_policy = payload.get("runtime_policy")
    if not all(
        isinstance(item, Mapping)
        for item in (identity, parent, authority, gate_policy, runtime_policy)
    ):
        raise base.validation.FutoiSiSourceValidationError(
            "experimental authority contract structure mismatch",
            blocker="provenance_not_sufficient",
        )
    assert isinstance(identity, Mapping)
    assert isinstance(parent, Mapping)
    assert isinstance(authority, Mapping)
    assert isinstance(gate_policy, Mapping)
    assert isinstance(runtime_policy, Mapping)

    expected_false = (
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
        or identity.get("contract_id") != AUTHORITY_CONTRACT_ID
        or identity.get("contract_version") != AUTHORITY_CONTRACT_VERSION
        or identity.get("task_id") != TASK_ID
        or identity.get("phase") != "8.7A"
        or identity.get("status") != "experimental_runtime_authority_active"
        or parent.get("git_blob_sha1") != base.CONTRACT_GIT_BLOB_SHA1
        or parent.get("source_ticker") != base.validation.SOURCE_TICKER
        or parent.get("target_instrument_id")
        != base.validation.TARGET_INSTRUMENT_ID
        or parent.get("target_security_id")
        != base.validation.TARGET_SECURITY_ID
        or authority.get("mode") != AUTHORITY_MODE
        or authority.get("authority_owner") != "PM_L2_PHASE_OWNER"
        or authority.get("historical_authenticated_retrieval_allowed") is not True
        or authority.get("local_research_artifact_storage_allowed") is not True
        or authority.get("phase8_7a_source_validation_allowed") is not True
        or any(authority.get(name) is not False for name in expected_false)
        or gate_policy.get("license_access_gate_must_reflect_actual_evidence")
        is not True
        or gate_policy.get("pit_revision_gate_must_reflect_actual_evidence")
        is not True
        or gate_policy.get("g3_or_g5_must_not_be_forced_to_pass") is not True
        or tuple(gate_policy.get("experimental_dataset_status_requires_technical_gates") or ())
        != TECHNICAL_GATES
        or gate_policy.get("experimental_dataset_status") != EXPERIMENTAL_STATUS
        or gate_policy.get("failure_status") != FAIL_STATUS
        or runtime_policy.get("required_authority_flag") != AUTHORITY_FLAG
        or runtime_policy.get("output_artifact_count") != 10
        or runtime_policy.get("fallback_or_substitution_allowed") is not False
        or runtime_policy.get("raw_response_persistence_allowed") is not False
    ):
        raise base.validation.FutoiSiSourceValidationError(
            "experimental authority contract policy mismatch",
            blocker="provenance_not_sufficient",
        )
    if base._git_blob_sha1(path) != AUTHORITY_CONTRACT_GIT_BLOB_SHA1:
        raise base.validation.FutoiSiSourceValidationError(
            "experimental authority contract digest mismatch",
            blocker="provenance_not_sufficient",
        )
    return dict(payload)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
    _verify_experimental_authority(request.authority_contract_path)
    base_request = request.base_request
    input_hashes = base._verify_frozen_inputs(base_request)
    input_hashes["experimental_authority_contract"] = _sha256(
        request.authority_contract_path
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
                    "historical retrieval is authorized only for the closed "
                    "experimental Phase 8.7A run; production use and raw "
                    "redistribution remain prohibited"
                ),
            )
        )
    if not pit_semantics_verified:
        source_errors.append(
            _source_error_record(
                trade_date_value=None,
                blocker="historical_pit_revision_semantics_not_proven",
                reason=(
                    "historical retrieval may proceed for experimental "
                    "measurement, but G5 and production entry remain blocked"
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
    coverage = base.validation.coverage_by_source(
        matrix,
        validation_ids,
    )
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
        name
        for name, result in gates.items()
        if not bool(result["passed"])
    ]
    technical_gates_passed = all(
        bool(gates[name]["passed"]) for name in TECHNICAL_GATES
    )
    final_status = (
        EXPERIMENTAL_STATUS if technical_gates_passed else FAIL_STATUS
    )

    authority_summary = {
        "contract_id": AUTHORITY_CONTRACT_ID,
        "contract_version": AUTHORITY_CONTRACT_VERSION,
        "mode": AUTHORITY_MODE,
        "git_blob_sha1": AUTHORITY_CONTRACT_GIT_BLOB_SHA1,
        "technical_gates": list(TECHNICAL_GATES),
        "production_use_allowed": False,
        "feature_computation_allowed": False,
        "model_fitting_allowed": False,
        "promotion_allowed": False,
        "trading_allowed": False,
    }
    blockers = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": base_request.run_id,
        "final_status": final_status,
        "failed_gates": failed_gates,
        "technical_gates_passed": technical_gates_passed,
        "experimental_authority": authority_summary,
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
        "experimental_authority": authority_summary,
        "immutable_inputs_verified": True,
    }
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
            "experimental_authority": authority_summary,
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
        "production_use_allowed": False,
    }


def main(argv: list[str] | None = None) -> int:
    request = request_from_args(build_argument_parser().parse_args(argv))
    result = execute(request)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
