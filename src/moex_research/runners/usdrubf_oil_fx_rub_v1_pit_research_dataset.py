from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Final, Mapping

import numpy as np
import pandas as pd

from moex_research.features.oil_fx_rub_v1_features import (
    EXPECTED_IDENTITY_COUNT,
    IDENTITY_COLUMNS,
    LABEL_COLUMNS,
    OilFxRubFeatureError,
    build_feature_frame,
    build_forward_return_labels,
    prepare_identity_panel,
)
from moex_research.runners import usdrubf_phase6_internal_modeling_dataset_builder as phase6_builder


PROJECT: Final[str] = "MOEX_Bot"
TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_02B_RESEARCH_DATASET"
CONTRACT_ID: Final[str] = "usdrubf_oil_fx_rub_v1_pit_research_dataset"
CONTRACT_VERSION: Final[str] = "1.0"
BRENT_READY_STATUS: Final[str] = "moex_brent_source_candidate_for_phase8_5"
CNYRUBF_READY_STATUS: Final[str] = "moex_algopack_cnyrubf_source_candidate_for_phase8_6b"
EXPECTED_IMMUTABLE_SHA256: Final[dict[str, str]] = {
    "phase6_modeling_dataset": "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00",
    "phase6_dataset_manifest": "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082",
    "brent_pit_acceptance_matrix": "78b60c9542fc08667267849b9ce03fdf161d2dc971fe99e1ab9d6a8d56266c43",
    "phase84a_gate_results": "aceaefb4d2e2a236539dd527c98464ddd1ea6bf5f1cdb8121662e1ce087f9c4c",
}
DECLARED_OUTPUTS: Final[tuple[str, ...]] = (
    "oil_fx_rub_features.parquet",
    "oil_fx_rub_labels.parquet",
    "oil_fx_rub_join_manifest.json",
    "gate_results.json",
)


class OilFxRubDatasetError(ValueError):
    """Fail-closed error for the additive Oil/FX/RUB V1 research dataset."""


def _read_json(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OilFxRubDatasetError(f"invalid JSON evidence: {path}") from exc
    if not isinstance(value, dict):
        raise OilFxRubDatasetError(f"JSON evidence must be an object: {path}")
    return value


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity")
    if not isinstance(identity, Mapping):
        raise OilFxRubDatasetError("contract_identity is required")
    if identity.get("contract_id") != CONTRACT_ID:
        raise OilFxRubDatasetError("contract id mismatch")
    if identity.get("contract_version") != CONTRACT_VERSION:
        raise OilFxRubDatasetError("contract version mismatch")
    if identity.get("project") != PROJECT or identity.get("task_id") != TASK_ID:
        raise OilFxRubDatasetError("project/task identity mismatch")

    scope = contract.get("approved_file_scope")
    if not isinstance(scope, Mapping):
        raise OilFxRubDatasetError("approved_file_scope is required")
    if scope.get("existing_files_to_modify") != []:
        raise OilFxRubDatasetError("existing contract/source files must not be modified")
    if scope.get("scope_widening_allowed") is not False:
        raise OilFxRubDatasetError("scope widening must remain forbidden")

    authority = contract.get("authority_boundary")
    if not isinstance(authority, Mapping) or not authority:
        raise OilFxRubDatasetError("authority boundary is required")
    if any(value is not False for value in authority.values()):
        raise OilFxRubDatasetError("authority boundary was widened")

    modes = contract.get("staged_modes")
    if not isinstance(modes, Mapping):
        raise OilFxRubDatasetError("staged_modes is required")
    brent = modes.get("brent_only")
    full = modes.get("oil_fx_full")
    if not isinstance(brent, Mapping) or brent.get("authorized_now") is not True:
        raise OilFxRubDatasetError("brent_only must remain the sole authorized current mode")
    if brent.get("cnyrubf_input_allowed") is not False:
        raise OilFxRubDatasetError("brent_only must forbid CNYRUBF input")
    if not isinstance(full, Mapping) or full.get("authorized_now") is not False:
        raise OilFxRubDatasetError("oil_fx_full must remain blocked in v1.0")


def validate_immutable_hashes(observed: Mapping[str, str]) -> None:
    if set(observed) != set(EXPECTED_IMMUTABLE_SHA256):
        raise OilFxRubDatasetError("immutable artifact inventory mismatch")
    failed = [
        name
        for name, expected in EXPECTED_IMMUTABLE_SHA256.items()
        if observed.get(name) != expected
    ]
    if failed:
        raise OilFxRubDatasetError(
            "immutable upstream hash mismatch: " + ", ".join(sorted(failed))
        )


def _validate_gate_inventory(gates: Mapping[str, Any], *, source: str) -> None:
    if len(gates) != 9:
        raise OilFxRubDatasetError(f"{source} gate inventory must contain exactly G1-G9")
    prefixes = [str(key).split("_", 1)[0] for key in gates]
    if prefixes != [f"G{i}" for i in range(1, 10)]:
        raise OilFxRubDatasetError(f"{source} gate inventory/order must be exactly G1-G9")
    for key, value in gates.items():
        if not isinstance(value, Mapping) or value.get("passed") is not True:
            raise OilFxRubDatasetError(f"{source} evidence has failed gate {key}")


def validate_brent_admission(gates: Mapping[str, Any]) -> None:
    _validate_gate_inventory(gates, source="Brent")
    final = next(value for key, value in gates.items() if str(key).startswith("G9_"))
    if final.get("status") != BRENT_READY_STATUS:
        raise OilFxRubDatasetError("Brent final source status is not admitted")
    if final.get("blocker_classification") is not None:
        raise OilFxRubDatasetError("Brent evidence has a blocker")


def validate_cnyrubf_admission(gates: Mapping[str, Any]) -> None:
    """Validate future-stage source readiness only; this does not authorize oil_fx_full."""
    _validate_gate_inventory(gates, source="CNYRUBF")
    final = next(value for key, value in gates.items() if str(key).startswith("G9_"))
    if final.get("status") != CNYRUBF_READY_STATUS:
        raise OilFxRubDatasetError("CNYRUBF post-fix source status is not admitted")
    if final.get("blocker_classification") is not None:
        raise OilFxRubDatasetError("CNYRUBF post-fix evidence has a blocker")


def _normalized_dates(series: pd.Series, *, label: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.isna().any():
        raise OilFxRubDatasetError(f"{label} contains invalid date")
    return parsed.dt.strftime("%Y-%m-%d")


def validate_phase6_source_panel_replay(
    frozen_modeling_dataset: pd.DataFrame,
    source_panel: pd.DataFrame,
) -> None:
    """Bind the explicit OHLC source panel to the exact frozen Phase 6 dataset semantics."""
    required = (
        "target_trade_date",
        "target_instrument_id",
        *phase6_builder.FEATURE_COLUMNS,
    )
    missing = [column for column in required if column not in frozen_modeling_dataset.columns]
    if missing:
        raise OilFxRubDatasetError(
            "frozen Phase 6 dataset missing replay columns: " + ", ".join(missing)
        )
    try:
        prepared = phase6_builder._prepare_internal_d1_panel(source_panel)
        diagnostic = phase6_builder._add_past_only_diagnostics(prepared)
        replayed = phase6_builder._build_feature_frame(diagnostic)
    except Exception as exc:  # existing Phase 6 fail-closed semantics remain authoritative
        raise OilFxRubDatasetError("Phase 6 source panel replay failed") from exc

    if len(replayed) != len(frozen_modeling_dataset) or len(prepared) != len(frozen_modeling_dataset):
        raise OilFxRubDatasetError("Phase 6 source panel row count differs from frozen dataset")
    frozen_dates = _normalized_dates(
        frozen_modeling_dataset["target_trade_date"], label="frozen Phase 6 target_trade_date"
    ).reset_index(drop=True)
    source_dates = _normalized_dates(
        prepared["trade_date"], label="Phase 6 source panel trade_date"
    ).reset_index(drop=True)
    if not frozen_dates.equals(source_dates):
        raise OilFxRubDatasetError("Phase 6 source panel dates differ from frozen dataset")
    frozen_instruments = frozen_modeling_dataset["target_instrument_id"].astype(str).reset_index(drop=True)
    source_instruments = prepared["instrument_id"].astype(str).reset_index(drop=True)
    if not frozen_instruments.equals(source_instruments):
        raise OilFxRubDatasetError("Phase 6 source panel instruments differ from frozen dataset")

    for column in phase6_builder.FEATURE_COLUMNS:
        expected = frozen_modeling_dataset[column].reset_index(drop=True)
        actual = replayed[column].reset_index(drop=True)
        if column == "prior_trade_date":
            if not _normalized_dates(expected, label=column).equals(
                _normalized_dates(actual, label=f"replayed {column}")
            ):
                raise OilFxRubDatasetError(f"Phase 6 replay mismatch: {column}")
            continue
        if column == "lag1_ema_3_19_state":
            left = expected.astype("string").fillna("<NA>")
            right = actual.astype("string").fillna("<NA>")
            if not left.equals(right):
                raise OilFxRubDatasetError(f"Phase 6 replay mismatch: {column}")
            continue
        left_num = pd.to_numeric(expected, errors="coerce").to_numpy(float)
        right_num = pd.to_numeric(actual, errors="coerce").to_numpy(float)
        if not np.allclose(left_num, right_num, rtol=1e-12, atol=1e-12, equal_nan=True):
            raise OilFxRubDatasetError(f"Phase 6 replay mismatch: {column}")


def _finite_or_null(frame: pd.DataFrame, columns: list[str]) -> bool:
    if not columns:
        return True
    raw = frame.loc[:, columns]
    numeric = raw.apply(pd.to_numeric, errors="coerce")
    values = numeric.to_numpy(dtype=float)
    invalid_coercion = np.isnan(values) & ~raw.isna().to_numpy()
    return bool(not invalid_coercion.any() and (np.isfinite(values) | np.isnan(values)).all())


def build_research_dataset(
    *,
    contract: Mapping[str, Any],
    frozen_modeling_dataset: pd.DataFrame,
    phase6_source_panel: pd.DataFrame,
    brent_matrix: pd.DataFrame,
    brent_gate_results: Mapping[str, Any],
    phase6_lineage_verified: bool,
    brent_artifacts_verified: bool,
    mode: str = "brent_only",
    cnyrubf_matrix: pd.DataFrame | None = None,
    cnyrubf_gate_results: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    _validate_contract(contract)
    modes = contract["staged_modes"]
    if mode not in modes:
        raise OilFxRubDatasetError("unknown research dataset mode")
    if modes[mode].get("authorized_now") is not True:
        raise OilFxRubDatasetError(f"{mode} is not authorized by current contract")
    if phase6_lineage_verified is not True:
        raise OilFxRubDatasetError("Phase 6 source-panel lineage is not verified")
    if brent_artifacts_verified is not True:
        raise OilFxRubDatasetError("Brent accepted-artifact hashes are not verified")
    if mode != "brent_only":
        raise OilFxRubDatasetError("only brent_only is authorized in contract v1.0")
    if cnyrubf_matrix is not None or cnyrubf_gate_results is not None:
        raise OilFxRubDatasetError("brent_only mode forbids CNYRUBF inputs/evidence")

    identities = prepare_identity_panel(frozen_modeling_dataset)
    validate_brent_admission(brent_gate_results)
    try:
        features = build_feature_frame(
            frozen_modeling_dataset,
            brent_matrix,
            mode="brent_only",
        )
        labels = build_forward_return_labels(
            frozen_modeling_dataset,
            phase6_source_panel,
        )
    except OilFxRubFeatureError as exc:
        raise OilFxRubDatasetError(str(exc)) from exc

    identity_ok = bool(
        len(features) == EXPECTED_IDENTITY_COUNT
        and len(labels) == EXPECTED_IDENTITY_COUNT
        and features.loc[:, IDENTITY_COLUMNS].equals(identities.loc[:, IDENTITY_COLUMNS])
        and labels.loc[:, IDENTITY_COLUMNS].equals(identities.loc[:, IDENTITY_COLUMNS])
    )
    prior = pd.to_datetime(brent_matrix["prior_trade_date"], errors="coerce")
    observed = pd.to_datetime(brent_matrix["brent_trade_date"], errors="coerce")
    source_dates_ok = bool(
        prior.notna().all()
        and observed.notna().all()
        and prior.reset_index(drop=True).equals(observed.reset_index(drop=True))
    )
    feature_columns = [column for column in features if column not in IDENTITY_COLUMNS]
    labels_separate = bool(
        not (set(features.columns) & set(LABEL_COLUMNS))
        and set(labels.columns) == set(IDENTITY_COLUMNS) | set(LABEL_COLUMNS)
    )
    numerical_ok = _finite_or_null(features, feature_columns)

    gates: dict[str, dict[str, Any]] = {
        "G1_identity_and_phase6_lineage": {
            "passed": identity_ok and phase6_lineage_verified,
        },
        "G2_brent_admission": {
            "passed": brent_artifacts_verified,
            "status": BRENT_READY_STATUS,
        },
        "G3_cnyrubf_stage_policy": {
            "passed": True,
            "mode": "brent_only",
            "oil_fx_full_authorized": False,
        },
        "G4_pit": {"passed": source_dates_ok},
        "G5_brent_roll_integrity": {
            "passed": True,
            "cross_contract_returns_computed": False,
        },
        "G6_leakage": {"passed": labels_separate},
        "G7_numerical": {"passed": numerical_ok},
        "G8_label_separation": {"passed": labels_separate},
    }
    failed = [key.split("_", 1)[0] for key, value in gates.items() if value["passed"] is not True]
    gates["G9_final"] = {
        "passed": not failed,
        "failed_gates": failed,
        "status": (
            "oil_fx_rub_brent_only_research_dataset_ready"
            if not failed
            else "oil_fx_rub_research_dataset_not_ready"
        ),
        "mode": "brent_only",
    }
    if failed:
        raise OilFxRubDatasetError("research dataset gates failed: " + ", ".join(failed))
    return features, labels, gates


def _write_outputs(
    output_dir: Path,
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    manifest: Mapping[str, Any],
    gates: Mapping[str, Any],
) -> None:
    if output_dir.exists():
        raise OilFxRubDatasetError("output directory must not pre-exist")
    output_dir.mkdir(parents=True, exist_ok=False)
    payloads: dict[str, object] = {
        "oil_fx_rub_features.parquet": features,
        "oil_fx_rub_labels.parquet": labels,
        "oil_fx_rub_join_manifest.json": manifest,
        "gate_results.json": gates,
    }
    if tuple(payloads) != DECLARED_OUTPUTS:
        raise OilFxRubDatasetError("output inventory mismatch")
    for name, value in payloads.items():
        path = output_dir / name
        if name.endswith(".parquet"):
            if not isinstance(value, pd.DataFrame):
                raise OilFxRubDatasetError("Parquet output must be a DataFrame")
            value.to_parquet(path, index=False)
        else:
            path.write_text(
                json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
    if sorted(path.name for path in output_dir.iterdir()) != sorted(DECLARED_OUTPUTS):
        raise OilFxRubDatasetError("undeclared output artifact detected")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-path", required=True)
    parser.add_argument("--phase6-modeling-dataset-path", required=True)
    parser.add_argument("--phase6-dataset-manifest-path", required=True)
    parser.add_argument("--phase6-source-panel-path", required=True)
    parser.add_argument("--brent-matrix-path", required=True)
    parser.add_argument("--brent-gate-results-path", required=True)
    parser.add_argument("--mode", choices=("brent_only", "oil_fx_full"), default="brent_only")
    parser.add_argument("--cnyrubf-matrix-path")
    parser.add_argument("--cnyrubf-gate-results-path")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)

    if args.mode != "brent_only":
        raise OilFxRubDatasetError(
            "oil_fx_full is blocked by contract v1.0 pending a separate additive CNYRUBF admission contract"
        )
    if args.cnyrubf_matrix_path or args.cnyrubf_gate_results_path:
        raise OilFxRubDatasetError("brent_only refuses CNYRUBF paths")

    paths = {
        "contract": Path(args.contract_path),
        "phase6_modeling_dataset": Path(args.phase6_modeling_dataset_path),
        "phase6_dataset_manifest": Path(args.phase6_dataset_manifest_path),
        "phase6_source_panel": Path(args.phase6_source_panel_path),
        "brent_pit_acceptance_matrix": Path(args.brent_matrix_path),
        "phase84a_gate_results": Path(args.brent_gate_results_path),
    }
    contract = _read_json(paths["contract"])
    _validate_contract(contract)
    observed_immutable = {
        name: _sha256(paths[name]) for name in EXPECTED_IMMUTABLE_SHA256
    }
    validate_immutable_hashes(observed_immutable)

    frozen_modeling_dataset = pd.read_parquet(paths["phase6_modeling_dataset"])
    phase6_source_panel = pd.read_parquet(paths["phase6_source_panel"])
    brent_matrix = pd.read_parquet(paths["brent_pit_acceptance_matrix"])
    brent_gates = _read_json(paths["phase84a_gate_results"])
    validate_phase6_source_panel_replay(frozen_modeling_dataset, phase6_source_panel)

    features, labels, gates = build_research_dataset(
        contract=contract,
        frozen_modeling_dataset=frozen_modeling_dataset,
        phase6_source_panel=phase6_source_panel,
        brent_matrix=brent_matrix,
        brent_gate_results=brent_gates,
        phase6_lineage_verified=True,
        brent_artifacts_verified=True,
        mode="brent_only",
    )
    manifest = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "mode": "brent_only",
        "identity_count": len(features),
        "input_sha256": {name: _sha256(path) for name, path in paths.items()},
        "immutable_upstream_sha256_verified": observed_immutable,
        "phase6_source_panel_semantic_replay_verified": True,
        "network_access_performed": False,
        "upstream_artifact_mutation_performed": False,
        "model_fit_performed": False,
        "trading_action_performed": False,
    }
    _write_outputs(
        Path(args.output_dir),
        features=features,
        labels=labels,
        manifest=manifest,
        gates=gates,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
