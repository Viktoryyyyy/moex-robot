from __future__ import annotations

import argparse
import math
import re
from pathlib import Path
from typing import Any, Final, Mapping

import numpy as np
import pandas as pd

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase04_regime_research as phase04
from moex_research.runners import usdrubf_oil_fx_rub_v1_phase05_signal_design as phase05
from moex_research.runners import usdrubf_oil_fx_rub_v1_phase06_backtest as phase06


PROJECT: Final[str] = "MOEX_Bot"
TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_07_RISK_MODEL"
CONTRACT_ID: Final[str] = "usdrubf_oil_fx_rub_v1_phase07_risk_model"
CONTRACT_VERSION: Final[str] = "1.0"
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"
HORIZONS: Final[tuple[int, ...]] = (5, 10, 20)
STOP_GRID: Final[tuple[float, ...]] = (0.01, 0.02, 0.03, 0.04, 0.05)
RISK_BUDGET_GRID: Final[tuple[float, ...]] = (0.0025, 0.005, 0.01)
ROUND_TRIP_COST_BPS: Final[int] = 20
COST_RETURN: Final[float] = ROUND_TRIP_COST_BPS / 10000.0
EXPECTED_TRADES: Final[int] = 3

DECLARED_OUTPUTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "risk_excursions.csv",
    "stop_sensitivity.csv",
    "sizing_sensitivity.csv",
    "risk_summary.json",
    "research_manifest.json",
    "gate_results.json",
)

_ALIAS_PATTERN = re.compile(r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.I)
_SHA40_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_GLOB_CHARS = frozenset("*?[]")


class Phase07RiskModelError(ValueError):
    """Fail-closed error for Oil/FX/RUB Phase 07 risk research."""


def _read_json(path: str | Path) -> dict[str, Any]:
    return phase05._read_json(path)


def _explicit_file(raw: object, flag: str, suffix: str) -> Path:
    return phase05._explicit_file(raw, flag, suffix)


def _explicit_output_dir(raw: object) -> Path:
    return phase05._explicit_output_dir(raw)


def _validate_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity")
    expected_identity = {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": TASK_ID,
    }
    if not isinstance(identity, Mapping):
        raise Phase07RiskModelError("contract_identity is required")
    for key, value in expected_identity.items():
        if identity.get(key) != value:
            raise Phase07RiskModelError(f"contract identity mismatch: {key}")

    purpose = contract.get("purpose")
    if not isinstance(purpose, Mapping):
        raise Phase07RiskModelError("purpose is required")
    for key in (
        "risk_research_allowed",
        "mae_mfe_analysis_allowed",
        "stop_sensitivity_allowed",
        "normalized_position_sizing_allowed",
    ):
        if purpose.get(key) is not True:
            raise Phase07RiskModelError(f"required purpose permission missing: {key}")
    for key in (
        "parameter_optimization_allowed",
        "best_stop_selection_allowed",
        "statistical_inference_allowed",
        "strategy_promotion_allowed",
        "broker_action_allowed",
        "trading_allowed",
    ):
        if purpose.get(key) is not False:
            raise Phase07RiskModelError(f"purpose boundary widened: {key}")

    inputs = contract.get("inputs")
    if not isinstance(inputs, Mapping):
        raise Phase07RiskModelError("inputs are required")
    hash_key_map = {
        "phase6_modeling_dataset_sha256": "phase6_modeling_dataset",
        "phase6_dataset_manifest_sha256": "phase6_dataset_manifest",
        "brent_pit_acceptance_matrix_sha256": "brent_pit_acceptance_matrix",
        "phase84a_gate_results_sha256": "phase84a_gate_results",
        "phase84a_input_identity_sha256": "phase84a_input_identity",
    }
    for contract_key, upstream_key in hash_key_map.items():
        if inputs.get(contract_key) != phase04.EXPECTED_SHA256[upstream_key]:
            raise Phase07RiskModelError(f"immutable hash metadata mismatch: {contract_key}")
    if inputs.get("recompute_from_pinned_upstreams") is not True:
        raise Phase07RiskModelError("recompute_from_pinned_upstreams must be true")
    if inputs.get("phase06_runtime_artifact_required") is not False:
        raise Phase07RiskModelError("Phase06 runtime artifact must not be required")
    if inputs.get("phase06a_runtime_artifact_required") is not False:
        raise Phase07RiskModelError("Phase06A runtime artifact must not be required")

    baseline = contract.get("baseline_reference")
    expected_baseline = {
        "rolling_window_sessions": 126,
        "high_threshold": 0.75,
        "cooldown_source_sessions": 21,
        "entry_execution": "target_trade_date open",
        "exit_horizons_sessions": list(HORIZONS),
        "round_trip_cost_bps": ROUND_TRIP_COST_BPS,
        "expected_independent_entry_dates": [row[0] for row in phase06.FROZEN_SCHEDULE],
    }
    if not isinstance(baseline, Mapping):
        raise Phase07RiskModelError("baseline_reference is required")
    for key, value in expected_baseline.items():
        if baseline.get(key) != value:
            raise Phase07RiskModelError(f"baseline mismatch: {key}")

    risk = contract.get("risk_definition")
    if not isinstance(risk, Mapping):
        raise Phase07RiskModelError("risk_definition is required")
    expected_risk = {
        "direction": "short_usdrubf_long_rub",
        "mae_formula": "max(max_session_high / entry_open - 1, 0)",
        "mfe_formula": "max(1 - min_session_low / entry_open, 0)",
        "mark_to_market_path": "entry_open to each session close",
        "intratrade_max_drawdown_formula": "max((running_peak_mark_equity - mark_equity) / running_peak_mark_equity)",
        "stop_grid_pct": list(STOP_GRID),
        "stop_price_formula": "entry_open * (1 + stop_pct)",
        "stop_trigger": "session_high >= stop_price",
        "gap_aware_stop_fill": "max(stop_price, session_open)",
        "stop_checked_from_entry_session": True,
        "fixed_exit_if_not_stopped": "horizon_session_close",
        "round_trip_cost_bps": ROUND_TRIP_COST_BPS,
        "risk_budget_pct_nav": list(RISK_BUDGET_GRID),
        "normalized_exposure_multiple_formula": "risk_budget_pct_nav / (stop_pct + round_trip_cost_bps / 10000)",
        "gap_risk_budget_breach_reported": True,
        "contract_count_sizing_allowed": False,
    }
    for key, value in expected_risk.items():
        if risk.get(key) != value:
            raise Phase07RiskModelError(f"risk definition mismatch: {key}")

    method = contract.get("methodology")
    if not isinstance(method, Mapping):
        raise Phase07RiskModelError("methodology is required")
    for key in (
        "full_stop_grid_must_be_reported",
        "full_horizon_grid_must_be_reported",
        "best_stop_must_not_be_selected",
        "no_post_hoc_grid_expansion",
        "sample_limited_status_required",
        "phase06_baseline_schedule_must_reproduce",
    ):
        if method.get(key) is not True:
            raise Phase07RiskModelError(f"methodology guard missing: {key}")
    for key in ("terminal_source_ohlc_allowed", "overlapping_positions_allowed"):
        if method.get(key) is not False:
            raise Phase07RiskModelError(f"methodology boundary widened: {key}")

    if contract.get("runtime_artifacts") != list(DECLARED_OUTPUTS):
        raise Phase07RiskModelError("runtime artifact inventory mismatch")
    authority = contract.get("authority_boundary")
    if not isinstance(authority, Mapping) or any(value is not False for value in authority.values()):
        raise Phase07RiskModelError("authority boundary widened")


def _validate_upstream_contracts(
    phase06_contract: Mapping[str, Any],
    phase05_contract: Mapping[str, Any],
    phase04_contract: Mapping[str, Any],
) -> None:
    phase06._validate_contract(phase06_contract)
    phase06._validate_upstream_semantics(
        phase05_contract=phase05_contract,
        phase04_contract=phase04_contract,
    )


def _safe_ohlc_path(panel: pd.DataFrame, entry_idx: int, exit_idx: int) -> pd.DataFrame:
    terminal_index = len(panel) - 1
    if entry_idx < 0 or exit_idx < entry_idx or exit_idx >= terminal_index:
        raise Phase07RiskModelError("risk path must use non-terminal bound OHLC rows")
    path = panel.iloc[entry_idx : exit_idx + 1].copy().reset_index(drop=True)
    for column in ("open", "high", "low", "close"):
        values = pd.to_numeric(path[column], errors="coerce").to_numpy(float)
        if not np.isfinite(values).all() or np.any(values <= 0):
            raise Phase07RiskModelError(f"risk path {column} must be finite positive")
        path[column] = values
    return path


def _intratrade_max_drawdown(entry_open: float, closes: np.ndarray) -> float:
    entry = phase06._safe_positive_price(entry_open, "entry_open")
    values = np.asarray(closes, dtype=float)
    if values.size == 0 or not np.isfinite(values).all() or np.any(values <= 0):
        raise Phase07RiskModelError("intratrade drawdown requires finite positive closes")
    mark_returns = (entry - values) / entry
    equity = np.concatenate(([1.0], 1.0 + mark_returns))
    if np.any(equity <= 0):
        raise Phase07RiskModelError("intratrade mark equity must remain positive")
    peaks = np.maximum.accumulate(equity)
    return float(np.max(1.0 - equity / peaks))


def _excursion_row(signal: pd.Series, panel: pd.DataFrame, horizon: int) -> dict[str, Any]:
    entry_idx = int(signal["entry_source_session_index"])
    exit_idx = entry_idx + horizon
    path = _safe_ohlc_path(panel, entry_idx, exit_idx)
    entry_open = phase06._safe_positive_price(path.iloc[0]["open"], "entry_open")
    highs = path["high"].to_numpy(float)
    lows = path["low"].to_numpy(float)
    closes = path["close"].to_numpy(float)
    mae_path = np.maximum(highs / entry_open - 1.0, 0.0)
    mfe_path = np.maximum(1.0 - lows / entry_open, 0.0)
    mae_pos = int(np.argmax(mae_path))
    mfe_pos = int(np.argmax(mfe_path))
    exit_close = phase06._safe_positive_price(path.iloc[-1]["close"], "exit_close")
    gross = phase06._gross_short_return(entry_open, exit_close)
    net = gross - COST_RETURN
    panel_dates = panel["trade_date"].astype(str).tolist()
    return {
        "signal_id": str(signal["signal_id"]),
        "entry_trade_date": str(signal["target_trade_date"]),
        "prior_trade_date": str(signal["prior_trade_date"]),
        "horizon_sessions": horizon,
        "exit_trade_date": panel_dates[exit_idx],
        "entry_price": entry_open,
        "fixed_exit_price": exit_close,
        "mae_adverse_excursion": float(np.max(mae_path)),
        "mae_trade_date": panel_dates[entry_idx + mae_pos],
        "mae_sessions_from_entry": mae_pos,
        "mfe_favorable_excursion": float(np.max(mfe_path)),
        "mfe_trade_date": panel_dates[entry_idx + mfe_pos],
        "mfe_sessions_from_entry": mfe_pos,
        "intratrade_max_drawdown": _intratrade_max_drawdown(entry_open, closes),
        "fixed_exit_gross_short_return": gross,
        "fixed_exit_net_short_return_20bps": net,
    }


def _materialize_excursions(candidates: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    phase06._validate_price_binding(candidates, panel)
    eligible = candidates.loc[candidates["execution_eligible"]].copy()
    eligible = eligible.sort_values("entry_source_session_index", kind="mergesort")
    if len(eligible) != EXPECTED_TRADES:
        raise Phase07RiskModelError("independent trade count mismatch")
    rows = [_excursion_row(signal, panel, horizon) for _, signal in eligible.iterrows() for horizon in HORIZONS]
    result = pd.DataFrame(rows)
    if len(result) != EXPECTED_TRADES * len(HORIZONS):
        raise Phase07RiskModelError("excursion row count mismatch")
    return result


def _simulate_stop(signal: pd.Series, panel: pd.DataFrame, horizon: int, stop_pct: float) -> dict[str, Any]:
    if stop_pct not in STOP_GRID:
        raise Phase07RiskModelError("unsupported stop pct")
    entry_idx = int(signal["entry_source_session_index"])
    exit_idx = entry_idx + horizon
    path = _safe_ohlc_path(panel, entry_idx, exit_idx)
    entry_open = phase06._safe_positive_price(path.iloc[0]["open"], "entry_open")
    stop_price = entry_open * (1.0 + stop_pct)
    panel_dates = panel["trade_date"].astype(str).tolist()

    stopped = False
    actual_exit_idx = exit_idx
    actual_exit_price = phase06._safe_positive_price(path.iloc[-1]["close"], "fixed_exit_close")
    for offset, row in path.iterrows():
        session_high = phase06._safe_positive_price(row["high"], "session_high")
        if session_high >= stop_price:
            session_open = phase06._safe_positive_price(row["open"], "session_open")
            stopped = True
            actual_exit_idx = entry_idx + int(offset)
            actual_exit_price = max(stop_price, session_open)
            break

    gross = phase06._gross_short_return(entry_open, actual_exit_price)
    net = gross - COST_RETURN
    return {
        "signal_id": str(signal["signal_id"]),
        "entry_trade_date": str(signal["target_trade_date"]),
        "horizon_sessions": horizon,
        "stop_pct": stop_pct,
        "stop_price": stop_price,
        "stop_triggered": stopped,
        "actual_exit_trade_date": panel_dates[actual_exit_idx],
        "actual_exit_execution": "gap_aware_stop" if stopped else "fixed_horizon_close",
        "actual_exit_price": actual_exit_price,
        "holding_source_sessions": actual_exit_idx - entry_idx,
        "round_trip_cost_bps": ROUND_TRIP_COST_BPS,
        "gross_short_return": gross,
        "net_short_return": net,
        "net_profitable": bool(net > 0.0),
    }


def _materialize_stop_paths(candidates: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    eligible = candidates.loc[candidates["execution_eligible"]].copy()
    eligible = eligible.sort_values("entry_source_session_index", kind="mergesort")
    rows = [
        _simulate_stop(signal, panel, horizon, stop_pct)
        for _, signal in eligible.iterrows()
        for horizon in HORIZONS
        for stop_pct in STOP_GRID
    ]
    result = pd.DataFrame(rows)
    expected = EXPECTED_TRADES * len(HORIZONS) * len(STOP_GRID)
    if len(result) != expected:
        raise Phase07RiskModelError("stop simulation row count mismatch")
    return result


def _stop_sensitivity(stop_paths: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        for stop_pct in STOP_GRID:
            group = stop_paths.loc[
                stop_paths["horizon_sessions"].eq(horizon)
                & np.isclose(stop_paths["stop_pct"], stop_pct)
            ].sort_values("entry_trade_date", kind="mergesort")
            if len(group) != EXPECTED_TRADES:
                raise Phase07RiskModelError("stop metric group count mismatch")
            net = group["net_short_return"].to_numpy(float)
            if not np.isfinite(net).all():
                raise Phase07RiskModelError("stop metrics contain non-finite returns")
            rows.append(
                {
                    "horizon_sessions": horizon,
                    "stop_pct": stop_pct,
                    "round_trip_cost_bps": ROUND_TRIP_COST_BPS,
                    "trade_count": EXPECTED_TRADES,
                    "stop_out_count": int(group["stop_triggered"].sum()),
                    "stop_out_rate": float(group["stop_triggered"].mean()),
                    "mean_net_return": float(np.mean(net)),
                    "median_net_return": float(np.median(net)),
                    "hit_rate_net_positive": float(np.mean(net > 0.0)),
                    "compounded_net_return": float(np.prod(1.0 + net) - 1.0),
                    "worst_trade_net_return": float(np.min(net)),
                    "trade_sequence_max_drawdown": phase06._trade_sequence_max_drawdown(net.tolist()),
                }
            )
    return pd.DataFrame(rows)


def _sizing_sensitivity(stop_metrics: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, metric in stop_metrics.iterrows():
        stop_pct = float(metric["stop_pct"])
        worst_loss = max(-float(metric["worst_trade_net_return"]), 0.0)
        for risk_budget in RISK_BUDGET_GRID:
            exposure = risk_budget / (stop_pct + COST_RETURN)
            observed_worst_loss_nav = exposure * worst_loss
            rows.append(
                {
                    "horizon_sessions": int(metric["horizon_sessions"]),
                    "stop_pct": stop_pct,
                    "risk_budget_pct_nav": risk_budget,
                    "round_trip_cost_bps": ROUND_TRIP_COST_BPS,
                    "normalized_exposure_multiple": exposure,
                    "nominal_exact_stop_loss_pct_nav": exposure * (stop_pct + COST_RETURN),
                    "observed_worst_trade_loss_pct_nav": observed_worst_loss_nav,
                    "gap_risk_budget_breached_in_sample": bool(observed_worst_loss_nav > risk_budget + 1e-12),
                }
            )
    result = pd.DataFrame(rows)
    expected = len(HORIZONS) * len(STOP_GRID) * len(RISK_BUDGET_GRID)
    if len(result) != expected:
        raise Phase07RiskModelError("sizing sensitivity row count mismatch")
    return result


def _summary(excursions: pd.DataFrame, stop_metrics: pd.DataFrame, sizing: pd.DataFrame) -> dict[str, Any]:
    by_horizon: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        group = excursions.loc[excursions["horizon_sessions"].eq(horizon)]
        by_horizon.append(
            {
                "horizon_sessions": horizon,
                "trade_count": int(len(group)),
                "median_mae": float(group["mae_adverse_excursion"].median()),
                "max_mae": float(group["mae_adverse_excursion"].max()),
                "median_mfe": float(group["mfe_favorable_excursion"].median()),
                "max_mfe": float(group["mfe_favorable_excursion"].max()),
                "median_intratrade_max_drawdown": float(group["intratrade_max_drawdown"].median()),
                "max_intratrade_max_drawdown": float(group["intratrade_max_drawdown"].max()),
                "mean_fixed_exit_net_return_20bps": float(group["fixed_exit_net_short_return_20bps"].mean()),
            }
        )
    return {
        "project": PROJECT,
        "task_id": TASK_ID,
        "status": "risk_model_complete_sample_limited",
        "independent_trade_count": EXPECTED_TRADES,
        "horizons_sessions": list(HORIZONS),
        "stop_grid_pct": list(STOP_GRID),
        "risk_budget_pct_nav": list(RISK_BUDGET_GRID),
        "round_trip_cost_bps": ROUND_TRIP_COST_BPS,
        "excursion_diagnostics": by_horizon,
        "stop_metric_variant_count": int(len(stop_metrics)),
        "sizing_variant_count": int(len(sizing)),
        "gap_budget_breach_variant_count": int(sizing["gap_risk_budget_breached_in_sample"].sum()),
        "sample_limitation": "Three independent entries only; risk metrics and stop/sizing sensitivities are descriptive and do not establish robustness.",
        "parameter_optimization_performed": False,
        "best_stop_selection_performed": False,
        "statistical_inference_performed": False,
        "contract_count_sizing_performed": False,
        "strategy_promotion_performed": False,
        "broker_action_performed": False,
        "trading_action_performed": False,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_oil_fx_rub_v1_phase07_risk_model"
    )
    for flag in (
        "--contract-path",
        "--phase06-contract-path",
        "--phase05-contract-path",
        "--phase04-contract-path",
        "--phase6-modeling-dataset-path",
        "--phase6-dataset-manifest-path",
        "--phase6-source-panel-path",
        "--brent-matrix-path",
        "--brent-gate-results-path",
        "--brent-input-identity-path",
        "--output-dir",
        "--run-id",
        "--git-commit-sha",
    ):
        parser.add_argument(flag, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    contract_path = _explicit_file(args.contract_path, "--contract-path", ".json")
    phase06_contract_path = _explicit_file(args.phase06_contract_path, "--phase06-contract-path", ".json")
    phase05_contract_path = _explicit_file(args.phase05_contract_path, "--phase05-contract-path", ".json")
    phase04_contract_path = _explicit_file(args.phase04_contract_path, "--phase04-contract-path", ".json")
    phase6_modeling_path = _explicit_file(args.phase6_modeling_dataset_path, "--phase6-modeling-dataset-path", ".parquet")
    phase6_manifest_path = _explicit_file(args.phase6_dataset_manifest_path, "--phase6-dataset-manifest-path", ".json")
    phase6_source_path = _explicit_file(args.phase6_source_panel_path, "--phase6-source-panel-path", ".parquet")
    brent_matrix_path = _explicit_file(args.brent_matrix_path, "--brent-matrix-path", ".parquet")
    brent_gates_path = _explicit_file(args.brent_gate_results_path, "--brent-gate-results-path", ".json")
    brent_identity_path = _explicit_file(args.brent_input_identity_path, "--brent-input-identity-path", ".json")
    output_dir = _explicit_output_dir(args.output_dir)
    run_id = str(args.run_id).strip()
    git_sha = str(args.git_commit_sha).strip().lower()
    if not run_id or any(char in run_id for char in _GLOB_CHARS) or _ALIAS_PATTERN.search(run_id):
        raise Phase07RiskModelError("--run-id must be explicit and immutable")
    if not _SHA40_PATTERN.fullmatch(git_sha):
        raise Phase07RiskModelError("--git-commit-sha must be exactly 40 lowercase hex characters")

    contract = _read_json(contract_path)
    phase06_contract = _read_json(phase06_contract_path)
    phase05_contract = _read_json(phase05_contract_path)
    phase04_contract = _read_json(phase04_contract_path)
    _validate_contract(contract)
    _validate_upstream_contracts(phase06_contract, phase05_contract, phase04_contract)

    immutable_paths = {
        "phase6_modeling_dataset": phase6_modeling_path,
        "phase6_dataset_manifest": phase6_manifest_path,
        "brent_pit_acceptance_matrix": brent_matrix_path,
        "phase84a_gate_results": brent_gates_path,
        "phase84a_input_identity": brent_identity_path,
    }
    observed_hashes = phase04._validate_immutable_hashes(immutable_paths)

    modeling = pd.read_parquet(phase6_modeling_path)
    source_panel = pd.read_parquet(phase6_source_path)
    brent_matrix = pd.read_parquet(brent_matrix_path)
    brent_gates = _read_json(brent_gates_path)
    brent_identity = _read_json(brent_identity_path)
    phase04._validate_brent_evidence(brent_gates)
    if not brent_identity:
        raise Phase07RiskModelError("Brent input identity evidence must be non-empty")

    candidates, panel = phase06._recompute_frozen_candidates(
        modeling=modeling,
        source_panel=source_panel,
        brent_matrix=brent_matrix,
    )
    if phase06._schedule_from_candidates(candidates) != phase06.FROZEN_SCHEDULE:
        raise Phase07RiskModelError("Phase06 frozen schedule reproduction failed")

    excursions = _materialize_excursions(candidates, panel)
    stop_paths = _materialize_stop_paths(candidates, panel)
    stop_metrics = _stop_sensitivity(stop_paths)
    sizing = _sizing_sensitivity(stop_metrics)
    summary = _summary(excursions, stop_metrics, sizing)

    gates: dict[str, dict[str, Any]] = {
        "G1_lineage": {"passed": True, "immutable_input_sha256": observed_hashes},
        "G2_frozen_baseline": {
            "passed": True,
            "entry_dates": [row[0] for row in phase06.FROZEN_SCHEDULE],
            "horizons_sessions": list(HORIZONS),
        },
        "G3_ohlc_binding": {
            "passed": True,
            "all_consumed_ohlc_rows_nonterminal": True,
            "phase6_source_panel_replayed": True,
        },
        "G4_excursion_math": {
            "passed": len(excursions) == EXPECTED_TRADES * len(HORIZONS),
            "excursion_row_count": int(len(excursions)),
        },
        "G5_stop_grid": {
            "passed": len(stop_metrics) == len(HORIZONS) * len(STOP_GRID),
            "stop_grid_pct": list(STOP_GRID),
            "gap_aware_fill": True,
        },
        "G6_sizing_grid": {
            "passed": len(sizing) == len(HORIZONS) * len(STOP_GRID) * len(RISK_BUDGET_GRID),
            "risk_budget_pct_nav": list(RISK_BUDGET_GRID),
            "cost_aware": True,
            "contract_count_sizing_performed": False,
        },
        "G7_no_selection_or_inference": {
            "passed": True,
            "parameter_optimization_performed": False,
            "best_stop_selection_performed": False,
            "statistical_inference_performed": False,
        },
        "G8_research_only": {
            "passed": True,
            "network_access_performed": False,
            "source_mutation_performed": False,
            "strategy_promotion_performed": False,
            "broker_action_performed": False,
            "trading_action_performed": False,
        },
    }
    failed = [key for key, value in gates.items() if value.get("passed") is not True]
    gates["G9_final"] = {
        "passed": not failed,
        "failed_gates": failed,
        "status": "oil_fx_rub_phase07_risk_model_complete" if not failed else "blocked",
    }

    manifest = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": run_id,
        "git_commit_sha": git_sha,
        "input_sha256": observed_hashes,
        "independent_trade_count": EXPECTED_TRADES,
        "excursion_row_count": int(len(excursions)),
        "stop_metric_variant_count": int(len(stop_metrics)),
        "sizing_variant_count": int(len(sizing)),
        "runtime_artifacts": list(DECLARED_OUTPUTS),
        "network_access_performed": False,
        "source_mutation_performed": False,
        "parameter_optimization_performed": False,
        "best_stop_selection_performed": False,
        "strategy_promotion_performed": False,
        "trading_action_performed": False,
    }
    identity = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "instrument_id": EXPECTED_INSTRUMENT,
        "phase06_frozen_schedule_reproduced": True,
        "independent_trade_count": EXPECTED_TRADES,
        "phase6_lineage_replayed": True,
        "brent_immutable_artifacts_verified": True,
    }

    output_dir.mkdir(parents=True, exist_ok=False)
    phase04._write_json(output_dir / "input_identity_verification.json", identity)
    excursions.to_csv(output_dir / "risk_excursions.csv", index=False)
    stop_metrics.to_csv(output_dir / "stop_sensitivity.csv", index=False)
    sizing.to_csv(output_dir / "sizing_sensitivity.csv", index=False)
    phase04._write_json(output_dir / "risk_summary.json", summary)
    phase04._write_json(output_dir / "research_manifest.json", manifest)
    phase04._write_json(output_dir / "gate_results.json", gates)
    if tuple(sorted(path.name for path in output_dir.iterdir())) != tuple(sorted(DECLARED_OUTPUTS)):
        raise Phase07RiskModelError("output artifact inventory mismatch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
