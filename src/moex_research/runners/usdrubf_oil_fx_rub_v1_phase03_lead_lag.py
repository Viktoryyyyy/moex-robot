from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Final, Mapping

import numpy as np
import pandas as pd

PROJECT: Final[str] = "MOEX_Bot"
TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_03_LEAD_LAG_RESEARCH"
CONTRACT_ID: Final[str] = "usdrubf_oil_fx_rub_v1_phase03_lead_lag_research"
CONTRACT_VERSION: Final[str] = "1.0"
UPSTREAM_TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_02B_RESEARCH_DATASET"
UPSTREAM_READY_STATUS: Final[str] = "oil_fx_rub_brent_only_research_dataset_ready"
EXPECTED_IDENTITY_COUNT: Final[int] = 472
BOOTSTRAP_SAMPLES: Final[int] = 1000
BOOTSTRAP_BLOCK_LENGTH: Final[int] = 5
BOOTSTRAP_SEED: Final[int] = 20260916
H2_MIN_ABS_RHO_IMPROVEMENT: Final[float] = 0.03
IDENTITY_COLUMNS: Final[tuple[str, str]] = (
    "target_trade_date",
    "target_instrument_id",
)
FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "ext_brent_intraday_return",
    "ext_brent_range_pct",
    "ext_brent_close_location",
    "ext_log1p_brent_volume",
    "ext_brent_same_contract_close_return_1session",
    "ext_brent_same_contract_close_return_3session",
    "ext_brent_same_contract_close_return_5session",
)
PRIMARY_IMPULSE_FEATURES: Final[tuple[str, ...]] = (
    "ext_brent_intraday_return",
    "ext_brent_same_contract_close_return_1session",
    "ext_brent_same_contract_close_return_3session",
    "ext_brent_same_contract_close_return_5session",
)
LABEL_COLUMNS: Final[tuple[str, ...]] = (
    "fwd_usdrubf_close_return_1session",
    "fwd_usdrubf_close_return_3session",
    "fwd_usdrubf_close_return_5session",
    "fwd_usdrubf_close_return_10session",
)
HORIZON_BY_LABEL: Final[dict[str, int]] = {
    "fwd_usdrubf_close_return_1session": 1,
    "fwd_usdrubf_close_return_3session": 3,
    "fwd_usdrubf_close_return_5session": 5,
    "fwd_usdrubf_close_return_10session": 10,
}
DECLARED_OUTPUTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "lead_lag_pairwise_metrics.csv",
    "lead_lag_quantile_effects.csv",
    "lead_lag_temporal_stability.csv",
    "hypothesis_summary.json",
    "research_manifest.json",
    "gate_results.json",
)
_ALIAS_PATTERN = re.compile(r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.I)
_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class Phase03LeadLagError(ValueError):
    """Fail-closed error for Oil/FX/RUB Phase 03 lead-lag research."""


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Phase03LeadLagError(f"invalid JSON evidence: {path}") from exc
    if not isinstance(value, dict):
        raise Phase03LeadLagError(f"JSON evidence must be an object: {path}")
    return value


def _validate_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity")
    if not isinstance(identity, Mapping):
        raise Phase03LeadLagError("contract_identity is required")
    expected = {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": TASK_ID,
    }
    for key, value in expected.items():
        if identity.get(key) != value:
            raise Phase03LeadLagError(f"contract identity mismatch: {key}")
    scope = contract.get("scope")
    if not isinstance(scope, Mapping):
        raise Phase03LeadLagError("scope is required")
    if scope.get("brent_only") is not True or scope.get("cnyrubf_allowed") is not False:
        raise Phase03LeadLagError("Phase 03 must remain brent_only")
    if scope.get("model_fit_allowed") is not False or scope.get("trading_rule_design_allowed") is not False:
        raise Phase03LeadLagError("model fitting/trading-rule design must remain forbidden")
    method = contract.get("methodology")
    if not isinstance(method, Mapping):
        raise Phase03LeadLagError("methodology is required")
    if method.get("bootstrap_samples") != BOOTSTRAP_SAMPLES:
        raise Phase03LeadLagError("bootstrap sample count mismatch")
    if method.get("bootstrap_block_length_sessions") != BOOTSTRAP_BLOCK_LENGTH:
        raise Phase03LeadLagError("bootstrap block length mismatch")
    if method.get("bootstrap_seed") != BOOTSTRAP_SEED:
        raise Phase03LeadLagError("bootstrap seed mismatch")
    if method.get("h2_min_abs_spearman_improvement") != H2_MIN_ABS_RHO_IMPROVEMENT:
        raise Phase03LeadLagError("H2 strengthening threshold mismatch")
    outputs = contract.get("runtime_artifacts")
    if outputs != list(DECLARED_OUTPUTS):
        raise Phase03LeadLagError("runtime artifact inventory mismatch")


def _validate_input_bundle_paths(
    features_path: Path,
    labels_path: Path,
    manifest_path: Path,
    gates_path: Path,
) -> None:
    parents = {
        path.resolve().parent
        for path in (features_path, labels_path, manifest_path, gates_path)
    }
    if len(parents) != 1:
        raise Phase03LeadLagError(
            "Phase 02B runtime inputs must come from one materialization directory"
        )


def _validate_inputs(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    manifest: Mapping[str, Any],
    gates: Mapping[str, Any],
) -> pd.DataFrame:
    if list(features.columns) != [*IDENTITY_COLUMNS, *FEATURE_COLUMNS]:
        raise Phase03LeadLagError("feature schema mismatch")
    if list(labels.columns) != [*IDENTITY_COLUMNS, *LABEL_COLUMNS]:
        raise Phase03LeadLagError("label schema mismatch")
    if len(features) != EXPECTED_IDENTITY_COUNT or len(labels) != EXPECTED_IDENTITY_COUNT:
        raise Phase03LeadLagError("identity count mismatch")
    left_ids = features.loc[:, IDENTITY_COLUMNS].astype(str).reset_index(drop=True)
    right_ids = labels.loc[:, IDENTITY_COLUMNS].astype(str).reset_index(drop=True)
    if not left_ids.equals(right_ids):
        raise Phase03LeadLagError("feature/label identity mismatch")
    dates = pd.to_datetime(features["target_trade_date"], errors="coerce")
    if dates.isna().any() or not dates.is_monotonic_increasing:
        raise Phase03LeadLagError("target dates must be valid/increasing")
    if left_ids.duplicated(list(IDENTITY_COLUMNS)).any():
        raise Phase03LeadLagError("duplicate identity")
    if manifest.get("project") != PROJECT or manifest.get("task_id") != UPSTREAM_TASK_ID:
        raise Phase03LeadLagError("upstream manifest project/task mismatch")
    if manifest.get("mode") != "brent_only" or manifest.get("identity_count") != EXPECTED_IDENTITY_COUNT:
        raise Phase03LeadLagError("upstream manifest mode/count mismatch")
    for key in (
        "network_access_performed",
        "upstream_artifact_mutation_performed",
        "model_fit_performed",
        "trading_action_performed",
    ):
        if manifest.get(key) is not False:
            raise Phase03LeadLagError(f"upstream manifest unsafe side effect: {key}")
    g9 = gates.get("G9_final")
    if not isinstance(g9, Mapping):
        raise Phase03LeadLagError("upstream G9 evidence missing")
    if g9.get("passed") is not True or g9.get("status") != UPSTREAM_READY_STATUS:
        raise Phase03LeadLagError("upstream dataset is not ready")
    research = pd.concat(
        [features.loc[:, FEATURE_COLUMNS], labels.loc[:, LABEL_COLUMNS]], axis=1
    )
    numeric = research.apply(pd.to_numeric, errors="coerce")
    values = numeric.to_numpy(float)
    if not (np.isfinite(values) | np.isnan(values)).all():
        raise Phase03LeadLagError("input contains infinite value")
    coercion = np.isnan(values) & ~research.isna().to_numpy()
    if coercion.any():
        raise Phase03LeadLagError("input contains non-numeric research value")
    joined = features.merge(labels, on=list(IDENTITY_COLUMNS), how="inner", validate="one_to_one")
    if len(joined) != EXPECTED_IDENTITY_COUNT:
        raise Phase03LeadLagError("joined identity count mismatch")
    return joined


def _pearson(x: np.ndarray, y: np.ndarray) -> float:
    if len(x) < 3 or np.std(x) == 0.0 or np.std(y) == 0.0:
        return float("nan")
    return float(np.corrcoef(x, y)[0, 1])


def _spearman(x: np.ndarray, y: np.ndarray) -> float:
    xr = pd.Series(x).rank(method="average").to_numpy(float)
    yr = pd.Series(y).rank(method="average").to_numpy(float)
    return _pearson(xr, yr)


def _sign(value: float, *, eps: float = 1e-15) -> int:
    if not np.isfinite(value) or abs(value) <= eps:
        return 0
    return 1 if value > 0 else -1


def _ci_excludes_zero(low: float, high: float) -> bool:
    return bool(np.isfinite(low) and np.isfinite(high) and (low > 0.0 or high < 0.0))


def _circular_block_indices(n: int, rng: np.random.Generator, block_length: int) -> np.ndarray:
    blocks = int(math.ceil(n / block_length))
    starts = rng.integers(0, n, size=blocks)
    offsets = np.arange(block_length)
    return np.concatenate([(start + offsets) % n for start in starts])[:n]


def _bootstrap_ci(
    x: np.ndarray,
    y: np.ndarray,
    *,
    metric: str,
    samples: int,
    seed: int,
    block_length: int,
) -> tuple[float, float]:
    rng = np.random.default_rng(seed)
    values: list[float] = []
    for _ in range(samples):
        idx = _circular_block_indices(len(x), rng, block_length)
        if metric == "spearman":
            value = _spearman(x[idx], y[idx])
        elif metric == "quantile_spread":
            value = _quantile_spread(x[idx], y[idx])[2]
        else:  # pragma: no cover
            raise Phase03LeadLagError(f"unknown bootstrap metric: {metric}")
        if np.isfinite(value):
            values.append(float(value))
    if len(values) < max(100, samples // 2):
        return float("nan"), float("nan")
    low, high = np.quantile(np.asarray(values), [0.025, 0.975])
    return float(low), float(high)


def _quantile_spread(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float, int, int]:
    if len(x) < 10:
        return float("nan"), float("nan"), float("nan"), 0, 0
    order = np.argsort(x, kind="mergesort")
    groups = np.array_split(order, 5)
    low_idx = groups[0]
    high_idx = groups[-1]
    low_mean = float(np.mean(y[low_idx]))
    high_mean = float(np.mean(y[high_idx]))
    return low_mean, high_mean, high_mean - low_mean, len(low_idx), len(high_idx)


def _pair_arrays(frame: pd.DataFrame, feature: str, label: str) -> tuple[np.ndarray, np.ndarray]:
    x = pd.to_numeric(frame[feature], errors="coerce").to_numpy(float)
    y = pd.to_numeric(frame[label], errors="coerce").to_numpy(float)
    mask = np.isfinite(x) & np.isfinite(y)
    return x[mask], y[mask]


def analyze_lead_lag(
    frame: pd.DataFrame,
    *,
    bootstrap_samples: int = BOOTSTRAP_SAMPLES,
    bootstrap_block_length: int = BOOTSTRAP_BLOCK_LENGTH,
    bootstrap_seed: int = BOOTSTRAP_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    pair_rows: list[dict[str, Any]] = []
    quantile_rows: list[dict[str, Any]] = []
    temporal_rows: list[dict[str, Any]] = []
    pair_index = 0
    for feature in FEATURE_COLUMNS:
        for label in LABEL_COLUMNS:
            x, y = _pair_arrays(frame, feature, label)
            n = len(x)
            pearson = _pearson(x, y)
            spearman = _spearman(x, y)
            spearman_low, spearman_high = _bootstrap_ci(
                x,
                y,
                metric="spearman",
                samples=bootstrap_samples,
                seed=bootstrap_seed + pair_index * 2,
                block_length=bootstrap_block_length,
            )
            q1, q5, spread, q1_n, q5_n = _quantile_spread(x, y)
            spread_low, spread_high = _bootstrap_ci(
                x,
                y,
                metric="quantile_spread",
                samples=bootstrap_samples,
                seed=bootstrap_seed + pair_index * 2 + 1,
                block_length=bootstrap_block_length,
            )
            segments = np.array_split(np.arange(n), 3)
            segment_rhos = [_spearman(x[idx], y[idx]) for idx in segments]
            overall_sign = _sign(spearman)
            same_sign_count = sum(_sign(value) == overall_sign and overall_sign != 0 for value in segment_rhos)
            corr_sig = _ci_excludes_zero(spearman_low, spearman_high)
            spread_sig = _ci_excludes_zero(spread_low, spread_high)
            coherent = _sign(spearman) == _sign(spread) and _sign(spearman) != 0
            if corr_sig and spread_sig and coherent and same_sign_count == 3:
                evidence_status = "robust"
            elif (corr_sig or spread_sig) and coherent and same_sign_count >= 2:
                evidence_status = "suggestive"
            else:
                evidence_status = "not_supported"
            pair_rows.append(
                {
                    "feature": feature,
                    "label": label,
                    "horizon_sessions": HORIZON_BY_LABEL[label],
                    "primary_impulse_feature": feature in PRIMARY_IMPULSE_FEATURES,
                    "valid_pair_count": n,
                    "pearson": pearson,
                    "spearman": spearman,
                    "spearman_ci95_low": spearman_low,
                    "spearman_ci95_high": spearman_high,
                    "spearman_ci_excludes_zero": corr_sig,
                    "evidence_status": evidence_status,
                }
            )
            quantile_rows.append(
                {
                    "feature": feature,
                    "label": label,
                    "horizon_sessions": HORIZON_BY_LABEL[label],
                    "valid_pair_count": n,
                    "q1_count": q1_n,
                    "q5_count": q5_n,
                    "q1_label_mean": q1,
                    "q5_label_mean": q5,
                    "q5_minus_q1_label_mean": spread,
                    "spread_ci95_low": spread_low,
                    "spread_ci95_high": spread_high,
                    "spread_ci_excludes_zero": spread_sig,
                }
            )
            temporal_rows.append(
                {
                    "feature": feature,
                    "label": label,
                    "horizon_sessions": HORIZON_BY_LABEL[label],
                    "valid_pair_count": n,
                    "early_count": len(segments[0]),
                    "middle_count": len(segments[1]),
                    "late_count": len(segments[2]),
                    "early_spearman": segment_rhos[0],
                    "middle_spearman": segment_rhos[1],
                    "late_spearman": segment_rhos[2],
                    "same_sign_segment_count": same_sign_count,
                    "all_three_same_sign": same_sign_count == 3,
                }
            )
            pair_index += 1
    pairwise = pd.DataFrame(pair_rows)
    quantiles = pd.DataFrame(quantile_rows)
    temporal = pd.DataFrame(temporal_rows)

    primary = pairwise[pairwise["primary_impulse_feature"]].copy()
    robust_count = int(primary["evidence_status"].eq("robust").sum())
    suggestive_count = int(primary["evidence_status"].eq("suggestive").sum())
    if robust_count:
        h1_status = "robust_evidence_present"
    elif suggestive_count:
        h1_status = "suggestive_evidence_only"
    else:
        h1_status = "not_supported_in_phase03"

    lag_details: list[dict[str, Any]] = []
    robust_lag_count = 0
    suggestive_lag_count = 0
    for feature in PRIMARY_IMPULSE_FEATURES:
        rows = primary[primary["feature"].eq(feature)].sort_values("horizon_sessions")
        one = rows[rows["horizon_sessions"].eq(1)]
        if len(one) != 1 or not np.isfinite(float(one.iloc[0]["spearman"])):
            continue
        rho1 = abs(float(one.iloc[0]["spearman"]))
        later = rows[rows["horizon_sessions"].gt(1)].copy()
        later["abs_spearman"] = later["spearman"].abs()
        best = later.sort_values(["abs_spearman", "horizon_sessions"], ascending=[False, True]).iloc[0]
        delta = float(best["abs_spearman"] - rho1)
        qualifies = delta >= H2_MIN_ABS_RHO_IMPROVEMENT
        status = str(best["evidence_status"])
        if qualifies and status == "robust":
            robust_lag_count += 1
        elif qualifies and status == "suggestive":
            suggestive_lag_count += 1
        lag_details.append(
            {
                "feature": feature,
                "spearman_abs_h1": rho1,
                "strongest_later_horizon_sessions": int(best["horizon_sessions"]),
                "strongest_later_abs_spearman": float(best["abs_spearman"]),
                "abs_spearman_improvement_vs_1session": delta,
                "later_pair_evidence_status": status,
                "meets_h2_strengthening_rule": bool(qualifies),
            }
        )
    if robust_lag_count:
        h2_status = "robust_delayed_strengthening_present"
    elif suggestive_lag_count:
        h2_status = "suggestive_delayed_strengthening_only"
    else:
        h2_status = "not_supported_in_phase03"

    strongest = primary.assign(abs_spearman=primary["spearman"].abs()).sort_values(
        ["abs_spearman", "horizon_sessions"], ascending=[False, True]
    ).iloc[0]
    summary = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "analysis_scope": "brent_only_H1_H2",
        "exploratory_not_causal": True,
        "trading_rule_design_performed": False,
        "model_fit_performed": False,
        "H1_oil_impulse": {
            "status": h1_status,
            "robust_primary_pair_count": robust_count,
            "suggestive_primary_pair_count": suggestive_count,
            "primary_pair_count": int(len(primary)),
        },
        "H2_oil_lag": {
            "status": h2_status,
            "min_abs_spearman_improvement": H2_MIN_ABS_RHO_IMPROVEMENT,
            "robust_feature_count": robust_lag_count,
            "suggestive_feature_count": suggestive_lag_count,
            "details": lag_details,
        },
        "strongest_primary_pair_by_abs_spearman": {
            "feature": str(strongest["feature"]),
            "label": str(strongest["label"]),
            "horizon_sessions": int(strongest["horizon_sessions"]),
            "spearman": float(strongest["spearman"]),
            "evidence_status": str(strongest["evidence_status"]),
        },
        "interpretation_boundary": (
            "Phase 03 measures reproducible association/lead-lag evidence only. "
            "It does not establish causality, a tradable threshold, a position size, or a production signal."
        ),
    }
    return pairwise, quantiles, temporal, summary


def _build_gates(
    pairwise: pd.DataFrame,
    quantiles: pd.DataFrame,
    temporal: pd.DataFrame,
    summary: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    expected_pairs = len(FEATURE_COLUMNS) * len(LABEL_COLUMNS)
    min_valid = int(pairwise["valid_pair_count"].min())
    finite_metrics = bool(
        np.isfinite(pairwise[["pearson", "spearman", "spearman_ci95_low", "spearman_ci95_high"]].to_numpy(float)).all()
        and np.isfinite(quantiles[["q1_label_mean", "q5_label_mean", "q5_minus_q1_label_mean", "spread_ci95_low", "spread_ci95_high"]].to_numpy(float)).all()
        and np.isfinite(temporal[["early_spearman", "middle_spearman", "late_spearman"]].to_numpy(float)).all()
    )
    gates: dict[str, dict[str, Any]] = {
        "G1_upstream_readiness": {"passed": True, "status": UPSTREAM_READY_STATUS},
        "G2_identity_and_schema": {"passed": len(pairwise) == expected_pairs, "expected_pairs": expected_pairs},
        "G3_pair_coverage": {"passed": min_valid >= 300, "minimum_valid_pair_count": min_valid},
        "G4_descriptive_metrics": {"passed": finite_metrics},
        "G5_block_bootstrap_uncertainty": {
            "passed": finite_metrics,
            "samples": BOOTSTRAP_SAMPLES,
            "block_length_sessions": BOOTSTRAP_BLOCK_LENGTH,
            "seed": BOOTSTRAP_SEED,
        },
        "G6_quantile_effects": {"passed": len(quantiles) == expected_pairs},
        "G7_temporal_stability": {
            "passed": len(temporal) == expected_pairs and int(temporal[["early_count", "middle_count", "late_count"]].min().min()) >= 90
        },
        "G8_scope_and_leakage": {
            "passed": summary.get("model_fit_performed") is False and summary.get("trading_rule_design_performed") is False,
            "network_access_performed": False,
            "model_fit_performed": False,
            "trading_rule_design_performed": False,
            "broker_action_performed": False,
        },
    }
    failed = [key.split("_", 1)[0] for key, value in gates.items() if value["passed"] is not True]
    gates["G9_final"] = {
        "passed": not failed,
        "failed_gates": failed,
        "status": "oil_fx_rub_phase03_lead_lag_research_complete" if not failed else "oil_fx_rub_phase03_lead_lag_research_blocked",
    }
    return gates


def _write_outputs(
    output_dir: Path,
    *,
    input_identity: Mapping[str, Any],
    pairwise: pd.DataFrame,
    quantiles: pd.DataFrame,
    temporal: pd.DataFrame,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    gates: Mapping[str, Any],
) -> None:
    if output_dir.exists():
        raise Phase03LeadLagError("output directory must not pre-exist")
    output_dir.mkdir(parents=True, exist_ok=False)
    payloads: dict[str, Any] = {
        "input_identity_verification.json": input_identity,
        "lead_lag_pairwise_metrics.csv": pairwise,
        "lead_lag_quantile_effects.csv": quantiles,
        "lead_lag_temporal_stability.csv": temporal,
        "hypothesis_summary.json": summary,
        "research_manifest.json": manifest,
        "gate_results.json": gates,
    }
    if tuple(payloads) != DECLARED_OUTPUTS:
        raise Phase03LeadLagError("declared output inventory mismatch")
    for name, value in payloads.items():
        path = output_dir / name
        if name.endswith(".csv"):
            value.to_csv(path, index=False)
        else:
            path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if sorted(path.name for path in output_dir.iterdir()) != sorted(DECLARED_OUTPUTS):
        raise Phase03LeadLagError("undeclared runtime artifact detected")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_oil_fx_rub_v1_phase03_lead_lag"
    )
    parser.add_argument("--contract-path", required=True)
    parser.add_argument("--features-path", required=True)
    parser.add_argument("--labels-path", required=True)
    parser.add_argument("--dataset-manifest-path", required=True)
    parser.add_argument("--dataset-gates-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--git-commit-sha", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    if _ALIAS_PATTERN.search(str(output_dir)):
        raise Phase03LeadLagError("latest/current/autodetect output paths are forbidden")
    if not _SHA_PATTERN.fullmatch(args.git_commit_sha):
        raise Phase03LeadLagError("git commit SHA must be exact 40-char lowercase hex")
    contract_path = Path(args.contract_path)
    features_path = Path(args.features_path)
    labels_path = Path(args.labels_path)
    manifest_path = Path(args.dataset_manifest_path)
    gates_path = Path(args.dataset_gates_path)
    contract = _read_json(contract_path)
    _validate_contract(contract)
    _validate_input_bundle_paths(
        features_path, labels_path, manifest_path, gates_path
    )
    features = pd.read_parquet(features_path)
    labels = pd.read_parquet(labels_path)
    upstream_manifest = _read_json(manifest_path)
    upstream_gates = _read_json(gates_path)
    joined = _validate_inputs(features, labels, upstream_manifest, upstream_gates)
    pairwise, quantiles, temporal, summary = analyze_lead_lag(joined)
    gates = _build_gates(pairwise, quantiles, temporal, summary)
    if gates["G9_final"]["passed"] is not True:
        raise Phase03LeadLagError("Phase 03 gates failed: " + ", ".join(gates["G9_final"]["failed_gates"]))
    input_paths = {
        "contract": contract_path,
        "features": features_path,
        "labels": labels_path,
        "dataset_manifest": manifest_path,
        "dataset_gates": gates_path,
    }
    input_sha256 = {name: _sha256(path) for name, path in input_paths.items()}
    input_identity = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "upstream_task_id": UPSTREAM_TASK_ID,
        "upstream_ready_status": UPSTREAM_READY_STATUS,
        "identity_count": EXPECTED_IDENTITY_COUNT,
        "input_sha256": input_sha256,
    }
    manifest = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": args.run_id,
        "git_commit_sha": args.git_commit_sha,
        "analysis_scope": "brent_only_H1_H2",
        "identity_count": EXPECTED_IDENTITY_COUNT,
        "feature_count": len(FEATURE_COLUMNS),
        "label_count": len(LABEL_COLUMNS),
        "pair_count": len(pairwise),
        "bootstrap_samples": BOOTSTRAP_SAMPLES,
        "bootstrap_block_length_sessions": BOOTSTRAP_BLOCK_LENGTH,
        "bootstrap_seed": BOOTSTRAP_SEED,
        "input_sha256": input_sha256,
        "network_access_performed": False,
        "upstream_artifact_mutation_performed": False,
        "model_fit_performed": False,
        "trading_rule_design_performed": False,
        "broker_action_performed": False,
    }
    _write_outputs(
        output_dir,
        input_identity=input_identity,
        pairwise=pairwise,
        quantiles=quantiles,
        temporal=temporal,
        summary=summary,
        manifest=manifest,
        gates=gates,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
