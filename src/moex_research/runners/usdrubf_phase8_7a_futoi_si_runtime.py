from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Final, Mapping

import pandas as pd

from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_source_validation as validation,
)

PROJECT: Final[str] = "MOEX_Bot"
RUN_ID_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^phase8_7a_futoi_si_source_validation_[0-9]{8}_v[1-9][0-9]*$"
)
SHA40_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")
CONTRACT_GIT_BLOB_SHA1: Final[str] = "eeb64cb20e8a122bc1d962077d4ca748c19d5fad"
EXPECTED_FROZEN_SHA256: Final[dict[str, str]] = {
    "modeling_dataset": "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00",
    "dataset_manifest": "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082",
    "feature_schema": "8f08802c7fb0a4cc43ab4ba072ee22ff9edd92fe8d674ea0515545d20d143238",
    "m0_validation_predictions": "9769d00a49adeb54c016d965387774e46a3e09e09f895aa61d48a90bbf3568cf",
    "phase83_aggregate_metrics": "d6ad4f6587dadb32431bd7b8f3bd59c5393e04d742efb4af459b316b417f8756",
    "phase83_gate_results": "d3f7e24022e550e725eae7ec5bc214d6b95e0e9c66393574c575e5d6f593f33c",
}
EXPECTED_PHASE83_STATUS: Final[str] = (
    "external_factor_incremental_value_not_supported"
)
EXPECTED_PHASE83_RECOMMENDATION: Final[str] = (
    "prioritize_blocked_oil_and_liquidity_sources"
)
EXPECTED_LICENSE_PROVIDER: Final[str] = "MOEX AlgoPack FUTOI"
EXPECTED_LICENSE_PRODUCT: Final[str] = "AlgoPack FUTOI"
FROZEN_ELIGIBLE_TARGET_DATE_FROM: Final[str] = "2024-08-05"
FROZEN_ELIGIBLE_TARGET_DATE_TILL: Final[str] = "2026-06-11"
REQUIRED_ARGS: Final[tuple[str, ...]] = (
    "--modeling-dataset-path",
    "--dataset-manifest-path",
    "--feature-schema-path",
    "--m0-validation-predictions-path",
    "--phase8-3-aggregate-metrics-path",
    "--phase8-3-gate-results-path",
    "--experiment-contract-path",
    "--license-access-evidence-path",
    "--pit-semantics-evidence-path",
    "--output-dir",
    "--run-id",
    "--git-commit-sha",
)


@dataclass(frozen=True)
class RuntimeRequest:
    modeling_dataset_path: Path
    dataset_manifest_path: Path
    feature_schema_path: Path
    m0_validation_predictions_path: Path
    phase83_aggregate_metrics_path: Path
    phase83_gate_results_path: Path
    experiment_contract_path: Path
    license_access_evidence_path: Path
    pit_semantics_evidence_path: Path
    output_dir: Path
    run_id: str
    git_commit_sha: str


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python -m moex_research.runners."
            "usdrubf_phase8_7a_futoi_si_runtime"
        )
    )
    for flag in REQUIRED_ARGS:
        parser.add_argument(flag, required=True)
    return parser


def _input_file(value: object, suffix: str, flag: str) -> Path:
    text = str(value or "").strip()
    if not text or any(char in text for char in "*?[]"):
        raise validation.FutoiSiSourceValidationError(
            f"{flag} must identify one explicit immutable file",
            blocker="provenance_not_sufficient",
        )
    path = Path(text)
    if path.suffix.lower() != suffix or not path.is_file():
        raise validation.FutoiSiSourceValidationError(
            f"{flag} file or suffix mismatch",
            blocker="provenance_not_sufficient",
        )
    return path


def request_from_args(args: argparse.Namespace) -> RuntimeRequest:
    request = RuntimeRequest(
        modeling_dataset_path=_input_file(args.modeling_dataset_path, ".parquet", REQUIRED_ARGS[0]),
        dataset_manifest_path=_input_file(args.dataset_manifest_path, ".json", REQUIRED_ARGS[1]),
        feature_schema_path=_input_file(args.feature_schema_path, ".json", REQUIRED_ARGS[2]),
        m0_validation_predictions_path=_input_file(
            args.m0_validation_predictions_path, ".parquet", REQUIRED_ARGS[3]
        ),
        phase83_aggregate_metrics_path=_input_file(
            getattr(args, "phase8_3_aggregate_metrics_path"), ".json", REQUIRED_ARGS[4]
        ),
        phase83_gate_results_path=_input_file(
            getattr(args, "phase8_3_gate_results_path"), ".json", REQUIRED_ARGS[5]
        ),
        experiment_contract_path=_input_file(
            args.experiment_contract_path, ".json", REQUIRED_ARGS[6]
        ),
        license_access_evidence_path=_input_file(
            args.license_access_evidence_path, ".json", REQUIRED_ARGS[7]
        ),
        pit_semantics_evidence_path=_input_file(
            args.pit_semantics_evidence_path, ".json", REQUIRED_ARGS[8]
        ),
        output_dir=Path(str(args.output_dir).strip()),
        run_id=str(args.run_id).strip(),
        git_commit_sha=str(args.git_commit_sha).strip().lower(),
    )
    if not RUN_ID_PATTERN.fullmatch(request.run_id):
        raise validation.FutoiSiSourceValidationError(
            "immutable FUTOI run id mismatch",
            blocker="provenance_not_sufficient",
        )
    if not SHA40_PATTERN.fullmatch(request.git_commit_sha):
        raise validation.FutoiSiSourceValidationError(
            "immutable git SHA mismatch",
            blocker="provenance_not_sufficient",
        )
    if request.output_dir.exists():
        raise validation.FutoiSiSourceValidationError(
            "output directory must not pre-exist",
            blocker="provenance_not_sufficient",
        )
    paths = [value for name, value in request.__dict__.items() if name.endswith("_path")]
    if len({path.resolve() for path in paths}) != len(paths):
        raise validation.FutoiSiSourceValidationError(
            "runtime input files must be distinct",
            blocker="provenance_not_sufficient",
        )
    return request


def _json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise validation.FutoiSiSourceValidationError(
            f"invalid JSON input: {path.name}",
            blocker="provenance_not_sufficient",
        ) from exc
    if not isinstance(payload, dict):
        raise validation.FutoiSiSourceValidationError(
            f"JSON input must be object: {path.name}",
            blocker="provenance_not_sufficient",
        )
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_blob_sha1(path: Path) -> str:
    payload = path.read_bytes()
    header = f"blob {len(payload)}\0".encode("ascii")
    return hashlib.sha1(header + payload, usedforsecurity=False).hexdigest()


def _frozen_input_inventory(request: RuntimeRequest) -> dict[str, Path]:
    return {
        "modeling_dataset": request.modeling_dataset_path,
        "dataset_manifest": request.dataset_manifest_path,
        "feature_schema": request.feature_schema_path,
        "m0_validation_predictions": request.m0_validation_predictions_path,
        "phase83_aggregate_metrics": request.phase83_aggregate_metrics_path,
        "phase83_gate_results": request.phase83_gate_results_path,
    }


def _verify_contract(path: Path) -> dict[str, Any]:
    contract = _json(path)
    identity = contract.get("contract_identity")
    source = contract.get("source_identity")
    if not isinstance(identity, dict) or not isinstance(source, dict):
        raise validation.FutoiSiSourceValidationError(
            "FUTOI contract identity is malformed",
            blocker="provenance_not_sufficient",
        )
    if (
        identity.get("project") != PROJECT
        or identity.get("phase") != "8.7A"
        or identity.get("contract_version") != "1.6"
        or source.get("source_ticker") != "Si"
        or source.get("exact_path") != validation.FUTOI_PATH
        or source.get("target_security_id") != validation.TARGET_SECURITY_ID
    ):
        raise validation.FutoiSiSourceValidationError(
            "FUTOI experiment contract mismatch",
            blocker="provenance_not_sufficient",
        )
    if _git_blob_sha1(path) != CONTRACT_GIT_BLOB_SHA1:
        raise validation.FutoiSiSourceValidationError(
            "FUTOI experiment contract digest mismatch",
            blocker="provenance_not_sufficient",
        )
    return contract


def _validate_phase83_evidence(
    aggregate: Mapping[str, Any],
    gates: Mapping[str, Any],
) -> None:
    final = gates.get("G12_final_acceptance")
    if (
        not isinstance(final, Mapping)
        or aggregate.get("final_status") != EXPECTED_PHASE83_STATUS
        or final.get("status") != EXPECTED_PHASE83_STATUS
        or final.get("recommendation") != EXPECTED_PHASE83_RECOMMENDATION
    ):
        raise validation.FutoiSiSourceValidationError(
            "Phase 8.3 frozen evidence mismatch",
            blocker="provenance_not_sufficient",
        )


def _verify_frozen_inputs(request: RuntimeRequest) -> dict[str, str]:
    inventory = _frozen_input_inventory(request)
    observed = {name: _sha256(path) for name, path in inventory.items()}
    bad = [
        name
        for name, expected in EXPECTED_FROZEN_SHA256.items()
        if observed.get(name) != expected
    ]
    if bad:
        raise validation.FutoiSiSourceValidationError(
            "immutable input hash mismatch: " + ", ".join(sorted(bad)),
            blocker="provenance_not_sufficient",
        )
    if not _json(request.dataset_manifest_path) or not _json(request.feature_schema_path):
        raise validation.FutoiSiSourceValidationError(
            "frozen manifest or feature schema is empty",
            blocker="provenance_not_sufficient",
        )
    _validate_phase83_evidence(
        _json(request.phase83_aggregate_metrics_path),
        _json(request.phase83_gate_results_path),
    )
    _verify_contract(request.experiment_contract_path)
    return {
        **observed,
        "experiment_contract": _git_blob_sha1(request.experiment_contract_path),
        "license_access_evidence": _sha256(request.license_access_evidence_path),
        "pit_semantics_evidence": _sha256(request.pit_semantics_evidence_path),
    }


def _identity_frames(
    modeling: pd.DataFrame,
    validation_predictions: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    identity_columns = ["target_trade_date", "target_instrument_id", "prior_trade_date"]
    if not set(identity_columns).issubset(modeling.columns):
        raise validation.FutoiSiSourceValidationError(
            "modeling dataset lacks frozen identity fields",
            blocker="provenance_not_sufficient",
        )
    all_identities = (
        modeling.loc[:, identity_columns]
        .assign(
            target_trade_date=lambda x: pd.to_datetime(x.target_trade_date).dt.strftime("%Y-%m-%d"),
            prior_trade_date=lambda x: pd.to_datetime(x.prior_trade_date).dt.strftime("%Y-%m-%d"),
            target_instrument_id=lambda x: x.target_instrument_id.astype(str),
        )
        .drop_duplicates()
        .sort_values(["target_trade_date", "target_instrument_id"])
        .reset_index(drop=True)
    )
    eligible = (
        all_identities.loc[
            all_identities.target_instrument_id.eq(validation.TARGET_INSTRUMENT_ID)
            & all_identities.target_trade_date.between(
                FROZEN_ELIGIBLE_TARGET_DATE_FROM,
                FROZEN_ELIGIBLE_TARGET_DATE_TILL,
                inclusive="both",
            )
        ]
        .sort_values(["target_trade_date", "target_instrument_id"])
        .reset_index(drop=True)
    )
    validation_columns = ["target_trade_date", "target_instrument_id"]
    if not set(validation_columns).issubset(validation_predictions.columns):
        raise validation.FutoiSiSourceValidationError(
            "validation predictions lack frozen identity fields",
            blocker="provenance_not_sufficient",
        )
    validation_ids = (
        validation_predictions.loc[:, validation_columns]
        .assign(
            target_trade_date=lambda x: pd.to_datetime(x.target_trade_date).dt.strftime("%Y-%m-%d"),
            target_instrument_id=lambda x: x.target_instrument_id.astype(str),
        )
        .drop_duplicates()
        .sort_values(["target_trade_date", "target_instrument_id"])
        .reset_index(drop=True)
    )
    if len(eligible) != validation.EXPECTED_ELIGIBLE_IDENTITIES:
        raise validation.FutoiSiSourceValidationError(
            "eligible identity count mismatch", blocker="provenance_not_sufficient"
        )
    if (
        eligible.target_trade_date.iloc[0] != FROZEN_ELIGIBLE_TARGET_DATE_FROM
        or eligible.target_trade_date.iloc[-1] != FROZEN_ELIGIBLE_TARGET_DATE_TILL
    ):
        raise validation.FutoiSiSourceValidationError(
            "eligible identity date window mismatch",
            blocker="provenance_not_sufficient",
        )
    if len(validation_ids) != validation.EXPECTED_VALIDATION_IDENTITIES:
        raise validation.FutoiSiSourceValidationError(
            "validation identity count mismatch", blocker="provenance_not_sufficient"
        )
    if not eligible.target_instrument_id.eq(validation.TARGET_INSTRUMENT_ID).all():
        raise validation.FutoiSiSourceValidationError(
            "eligible identities contain another instrument",
            blocker="provenance_not_sufficient",
        )
    if not validation_ids.target_instrument_id.eq(validation.TARGET_INSTRUMENT_ID).all():
        raise validation.FutoiSiSourceValidationError(
            "validation identities contain another instrument",
            blocker="provenance_not_sufficient",
        )
    return eligible, validation_ids


def _license_access_validation(
    evidence: dict[str, Any],
) -> tuple[bool, dict[str, object]]:
    return validation.validate_license_access_evidence(evidence)


def _pit_semantics_passed(evidence: dict[str, Any]) -> bool:
    return bool(
        evidence.get("provider") == EXPECTED_LICENSE_PROVIDER
        and evidence.get("availability_field") == "systime"
        and evidence.get("historical_original_publication_time_verified") is True
        and evidence.get("revision_behavior_documented") is True
        and str(evidence.get("evidence_source") or "").strip()
        and validation._aware_iso_timestamp(evidence.get("verified_at"))
    )


def _source_error_record(
    *,
    trade_date_value: str | None,
    blocker: str,
    reason: str,
) -> dict[str, object]:
    return {"trade_date": trade_date_value, "blocker": blocker, "reason": reason}


def execute(request: RuntimeRequest) -> dict[str, object]:
    input_hashes = _verify_frozen_inputs(request)
    modeling = pd.read_parquet(request.modeling_dataset_path)
    validation_predictions = pd.read_parquet(request.m0_validation_predictions_path)
    eligible, validation_ids = _identity_frames(modeling, validation_predictions)
    license_passed, license_validation = _license_access_validation(
        _json(request.license_access_evidence_path)
    )
    pit_evidence = _json(request.pit_semantics_evidence_path)
    pit_semantics_verified = _pit_semantics_passed(pit_evidence)

    pairs: list[validation.FutoiDailyPair] = []
    schema_columns: tuple[str, ...] | None = None
    source_errors: list[dict[str, object]] = []
    token: str | None = None
    if license_passed:
        try:
            token = validation.algopack_http.load_algopack_token()
        except validation.algopack_http.AlgoPackHttpError as exc:
            source_errors.append(
                _source_error_record(
                    trade_date_value=None,
                    blocker=exc.transport_outcome,
                    reason=str(exc),
                )
            )
    else:
        source_errors.append(
            _source_error_record(
                trade_date_value=None,
                blocker="provider_license_and_access_terms_not_documented",
                reason=(
                    "network retrieval not performed because license/access "
                    "evidence is not approved for MOEX AlgoPack FUTOI"
                ),
            )
        )

    if token is not None:
        for value in sorted(eligible.prior_trade_date.unique()):
            source_date = date.fromisoformat(str(value))
            try:
                pair, columns = validation.load_futoi_daily_pair(
                    source_date, bearer_token=token
                )
            except validation.FutoiSiSourceValidationError as exc:
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

    matrix, diagnostics = validation.build_futoi_pit_acceptance_matrix(eligible, pairs)
    coverage = validation.coverage_by_source(matrix, validation_ids)
    transport_exercised = bool(pairs)
    route_validation = {
        "official_service": EXPECTED_LICENSE_PROVIDER,
        "host": validation.ALGOPACK_HOST,
        "exact_path": validation.FUTOI_PATH,
        "source_ticker": validation.SOURCE_TICKER,
        "target_security_id": validation.TARGET_SECURITY_ID,
        "storage_family_code": validation.STORAGE_FAMILY_CODE,
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
    }
    schema_profile = {
        "required_fields": list(validation.RAW_REQUIRED_FIELDS),
        "observed_columns": list(schema_columns or ()),
        "participant_groups": list(validation.PARTICIPANT_GROUPS),
        "pair_key": ["trade_date", "moment", "sess_id"],
        "cross_group_seqnum_equality_required": False,
        "canonical_normalizer": "moex_data.futures.futoi_raw_loader.normalize_futoi",
        "canonical_schema_version": "futures_futoi_5m_raw.v1",
        "daily_pair_count": len(pairs),
        "schema_stable": bool(schema_columns)
        and not any(
            item["blocker"] == "official_schema_not_stable" for item in source_errors
        ),
        "pit_evidence": pit_evidence,
    }
    numerical_integrity = not any(
        item["blocker"] == "numerical_or_chronology_integrity_failure"
        for item in source_errors
    )
    provenance_passed = not any(
        item["blocker"] in {"provenance_not_sufficient", "official_route_not_reproducible"}
        for item in source_errors
    )
    gates = validation.evaluate_gates(
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
    failed_gates = [name for name, result in gates.items() if not bool(result["passed"])]
    final_status = (
        "moex_futoi_si_source_candidate_for_phase8_7b"
        if not failed_gates
        else "moex_futoi_si_source_not_ready"
    )
    blockers = {
        "project": PROJECT,
        "task_id": validation.TASK_ID,
        "run_id": request.run_id,
        "final_status": final_status,
        "failed_gates": failed_gates,
        "blockers": source_errors,
        "historical_model_use_status": (
            "source_validation_only" if not failed_gates else "blocked"
        ),
    }
    input_verification = {
        "project": PROJECT,
        "run_id": request.run_id,
        "git_commit_sha": request.git_commit_sha,
        "eligible_identity_count": len(eligible),
        "validation_identity_count": len(validation_ids),
        "input_hashes": input_hashes,
        "expected_frozen_sha256": EXPECTED_FROZEN_SHA256,
        "expected_contract_git_blob_sha1": CONTRACT_GIT_BLOB_SHA1,
        "immutable_inputs_verified": True,
    }
    artifact_names = validation.write_validation_artifacts(
        request.output_dir,
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
            "task_id": validation.TASK_ID,
            "run_id": request.run_id,
            "final_status": final_status,
            "failed_gates": failed_gates,
            "gates": gates,
        },
    )
    return {
        "project": PROJECT,
        "task_id": validation.TASK_ID,
        "run_id": request.run_id,
        "output_dir": str(request.output_dir),
        "artifact_names": list(artifact_names),
        "artifact_count": len(artifact_names),
        "eligible_identity_count": len(eligible),
        "validation_identity_count": len(validation_ids),
        "daily_pair_count": len(pairs),
        "final_status": final_status,
        "failed_gates": failed_gates,
    }


def main(argv: list[str] | None = None) -> int:
    request = request_from_args(build_argument_parser().parse_args(argv))
    result = execute(request)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
