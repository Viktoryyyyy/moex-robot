from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Final, Mapping

import numpy as np
import pandas as pd

from moex_research.features.oil_fx_rub_v1_features import (
    CNYRUBF_FEATURES,
    EXPECTED_IDENTITY_COUNT,
    IDENTITY_COLUMNS,
    INTERACTION_FEATURES,
    LABEL_COLUMNS,
    BRENT_FEATURES,
    OilFxRubFeatureError,
    build_feature_frame,
    build_forward_return_labels,
    prepare_identity_panel,
)


PROJECT: Final[str] = "MOEX_Bot"
TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_02B_RESEARCH_DATASET"
CONTRACT_ID: Final[str] = "usdrubf_oil_fx_rub_v1_pit_research_dataset"
CONTRACT_VERSION: Final[str] = "1.0"
BRENT_READY_STATUS: Final[str] = "moex_brent_source_candidate_for_phase8_5"
CNYRUBF_READY_STATUS: Final[str] = "moex_algopack_cnyrubf_source_candidate_for_phase8_6b"
DECLARED_OUTPUTS: Final[tuple[str, ...]] = (
    "oil_fx_rub_features.parquet",
    "oil_fx_rub_labels.parquet",
    "oil_fx_rub_join_manifest.json",
    "gate_results.json",
)
EXPECTED_GATE_NAMES: Final[tuple[str, ...]] = tuple(
    [f"G{i}_placeholder" for i in range(1, 9)] + ["G9_final_source_readiness"]
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
    _validate_gate_inventory(gates, source="CNYRUBF")
    final = next(value for key, value in gates.items() if str(key).startswith("G9_"))
    if final.get("status") != CNYRUBF_READY_STATUS:
        raise OilFxRubDatasetError("CNYRUBF post-fix source status is not admitted")
    if final.get("blocker_classification") is not None:
        raise OilFxRubDatasetError("CNYRUBF post-fix evidence has a blocker")


def _finite_or_null(frame: pd.DataFrame, columns: list[str]) -> bool:
    if not columns:
        return True
    numeric = frame.loc[:, columns].apply(pd.to_numeric, errors="coerce")
    raw_nulls = frame.loc[:, columns].isna().to_numpy()
    numeric_values = numeric.to_numpy(dtype=float)
    invalid = np.isnan(numeric_values) & ~raw_nulls
    if invalid.any():
        return False
    return bool((np.isfinite(numeric_values) | np.isnan(numeric_values)).all())


def build_research_dataset(
    *,
    contract: Mapping[str, Any],
    identity_panel: pd.DataFrame,
    brent_matrix: pd.DataFrame,
    brent_gate_results: Mapping[str, Any],
    usdrubf_d1: pd.DataFrame,
    mode: str = "brent_only",
    cnyrubf_matrix: pd.DataFrame | None = None,
    cnyrubf_gate_results: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    _validate_contract(contract)
    identities = prepare_identity_panel(identity_panel)
    validate_brent_admission(brent_gate_results)

    g3 = True
    if mode == "brent_only":
        if cnyrubf_matrix is not None or cnyrubf_gate_results is not None:
            raise OilFxRubDatasetError("brent_only mode forbids CNYRUBF inputs/evidence")
    elif mode == "oil_fx_full":
        if cnyrubf_matrix is None or cnyrubf_gate_results is None:
            raise OilFxRubDatasetError("oil_fx_full requires CNYRUBF matrix and post-fix gates")
        validate_cnyrubf_admission(cnyrubf_gate_results)
    else:
        raise OilFxRubDatasetError("unknown research dataset mode")

    try:
        features = build_feature_frame(
            identity_panel,
            brent_matrix,
            mode=mode,
            cnyrubf_matrix=cnyrubf_matrix,
        )
        labels = build_forward_return_labels(identity_panel, usdrubf_d1)
    except OilFxRubFeatureError as exc:
        raise OilFxRubDatasetError(str(exc)) from exc

    identity_ok = bool(
        len(features) == EXPECTED_IDENTITY_COUNT
        and len(labels) == EXPECTED_IDENTITY_COUNT
        and features.loc[:, IDENTITY_COLUMNS].equals(identities.loc[:, IDENTITY_COLUMNS])
        and labels.loc[:, IDENTITY_COLUMNS].equals(identities.loc[:, IDENTITY_COLUMNS])
    )
    feature_columns = [column for column in features if column not in IDENTITY_COLUMNS]
    labels_separate = bool(
        not (set(features.columns) & set(LABEL_COLUMNS))
        and all(column in labels.columns for column in LABEL_COLUMNS)
        and set(labels.columns) == set(IDENTITY_COLUMNS) | set(LABEL_COLUMNS)
    )
    source_dates_ok = True
    for matrix, trade_column in (
        (brent_matrix, "brent_trade_date"),
        (cnyrubf_matrix, "cnyrubf_trade_date"),
    ):
        if matrix is None:
            continue
        prior = pd.to_datetime(matrix["prior_trade_date"], errors="coerce")
        observed = pd.to_datetime(matrix[trade_column], errors="coerce")
        source_dates_ok = bool(
            source_dates_ok
            and prior.notna().all()
            and observed.notna().all()
            and prior.equals(observed)
        )
    numerical_ok = _finite_or_null(features, feature_columns)

    gates: dict[str, dict[str, Any]] = {
        "G1_identity": {"passed": identity_ok},
        "G2_brent_admission": {"passed": True, "status": BRENT_READY_STATUS},
        "G3_cnyrubf_admission": {
            "passed": g3,
            "mode": mode,
            "status": None if mode == "brent_only" else CNYRUBF_READY_STATUS,
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
        "status": "oil_fx_rub_research_dataset_ready" if not failed else "oil_fx_rub_research_dataset_not_ready",
        "mode": mode,
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
            assert isinstance(value, pd.DataFrame)
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
    parser.add_argument("--identity-panel-path", required=True)
    parser.add_argument("--brent-matrix-path", required=True)
    parser.add_argument("--brent-gate-results-path", required=True)
    parser.add_argument("--usdrubf-d1-path", required=True)
    parser.add_argument("--mode", choices=("brent_only", "oil_fx_full"), default="brent_only")
    parser.add_argument("--cnyrubf-matrix-path")
    parser.add_argument("--cnyrubf-gate-results-path")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)

    paths = {
        "contract": Path(args.contract_path),
        "identity_panel": Path(args.identity_panel_path),
        "brent_matrix": Path(args.brent_matrix_path),
        "brent_gate_results": Path(args.brent_gate_results_path),
        "usdrubf_d1": Path(args.usdrubf_d1_path),
    }
    if args.mode == "oil_fx_full":
        if not args.cnyrubf_matrix_path or not args.cnyrubf_gate_results_path:
            raise OilFxRubDatasetError("oil_fx_full requires explicit CNYRUBF paths")
        paths["cnyrubf_matrix"] = Path(args.cnyrubf_matrix_path)
        paths["cnyrubf_gate_results"] = Path(args.cnyrubf_gate_results_path)
    elif args.cnyrubf_matrix_path or args.cnyrubf_gate_results_path:
        raise OilFxRubDatasetError("brent_only refuses CNYRUBF paths")

    contract = _read_json(paths["contract"])
    identity_panel = pd.read_parquet(paths["identity_panel"])
    brent_matrix = pd.read_parquet(paths["brent_matrix"])
    brent_gates = _read_json(paths["brent_gate_results"])
    usdrubf_d1 = pd.read_parquet(paths["usdrubf_d1"])
    cny_matrix = (
        pd.read_parquet(paths["cnyrubf_matrix"])
        if "cnyrubf_matrix" in paths
        else None
    )
    cny_gates = (
        _read_json(paths["cnyrubf_gate_results"])
        if "cnyrubf_gate_results" in paths
        else None
    )
    features, labels, gates = build_research_dataset(
        contract=contract,
        identity_panel=identity_panel,
        brent_matrix=brent_matrix,
        brent_gate_results=brent_gates,
        usdrubf_d1=usdrubf_d1,
        mode=args.mode,
        cnyrubf_matrix=cny_matrix,
        cnyrubf_gate_results=cny_gates,
    )
    manifest = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "mode": args.mode,
        "identity_count": len(features),
        "input_sha256": {name: _sha256(path) for name, path in paths.items()},
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
