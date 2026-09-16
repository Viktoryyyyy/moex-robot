from __future__ import annotations

import argparse
import math
import re
from itertools import product
from pathlib import Path
from typing import Any, Final, Mapping

import numpy as np
import pandas as pd

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase04_regime_research as phase04
from moex_research.runners import usdrubf_oil_fx_rub_v1_phase05_signal_design as phase05
from moex_research.runners import usdrubf_oil_fx_rub_v1_phase06_backtest as phase06


PROJECT: Final[str] = "MOEX_Bot"
TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_06A_ROBUSTNESS_SENSITIVITY"
CONTRACT_ID: Final[str] = "usdrubf_oil_fx_rub_v1_phase06a_robustness_sensitivity"
CONTRACT_VERSION: Final[str] = "1.0"
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"

WINDOW_GRID: Final[tuple[int, ...]] = (63, 126, 252)
MIN_HISTORY_BY_WINDOW: Final[dict[int, int]] = {63: 32, 126: 63, 252: 126}
THRESHOLD_GRID: Final[tuple[float, ...]] = (0.70, 0.75, 0.80)
COOLDOWN_GRID: Final[tuple[int, ...]] = (15, 21, 30)
HORIZON_GRID: Final[tuple[int, ...]] = (5, 10, 15, 20, 30)
PRIMARY_COST_BPS: Final[int] = 20
EXPECTED_CONFIG_COUNT: Final[int] = 27
EXPECTED_METRIC_CELL_COUNT: Final[int] = 135
BASELINE_WINDOW: Final[int] = 126
BASELINE_MIN_HISTORY: Final[int] = 63
BASELINE_THRESHOLD: Final[float] = 0.75
BASELINE_COOLDOWN: Final[int] = 21

DECLARED_OUTPUTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "sensitivity_trade_results.csv",
    "sensitivity_metrics.csv",
    "sensitivity_summary.json",
    "research_manifest.json",
    "gate_results.json",
)

_ALIAS_PATTERN = re.compile(r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.I)
_SHA40_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_GLOB_CHARS = frozenset("*?[]")


class Phase06ARobustnessError(ValueError):
    """Fail-closed error for Oil/FX/RUB Phase 06A robustness sensitivity."""


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
        raise Phase06ARobustnessError("contract_identity is required")
    for key, value in expected_identity.items():
        if identity.get(key) != value:
            raise Phase06ARobustnessError(f"contract identity mismatch: {key}")

    purpose = contract.get("purpose")
    if not isinstance(purpose, Mapping) or purpose.get("robustness_sensitivity_allowed") is not True:
        raise Phase06ARobustnessError("robustness sensitivity must be explicitly allowed")
    for key in (
        "parameter_optimization_allowed",
        "winner_selection_allowed",
        "parameter_ranking_allowed",
        "statistical_inference_allowed",
        "model_fit_allowed",
        "position_sizing_allowed",
        "strategy_promotion_allowed",
        "trading_allowed",
    ):
        if purpose.get(key) is not False:
            raise Phase06ARobustnessError(f"purpose boundary widened: {key}")

    inputs = contract.get("inputs")
    if not isinstance(inputs, Mapping):
        raise Phase06ARobustnessError("inputs are required")
    hash_key_map = {
        "phase6_modeling_dataset_sha256": "phase6_modeling_dataset",
        "phase6_dataset_manifest_sha256": "phase6_dataset_manifest",
        "brent_pit_acceptance_matrix_sha256": "brent_pit_acceptance_matrix",
        "phase84a_gate_results_sha256": "phase84a_gate_results",
        "phase84a_input_identity_sha256": "phase84a_input_identity",
    }
    for contract_key, upstream_key in hash_key_map.items():
        if inputs.get(contract_key) != phase04.EXPECTED_SHA256[upstream_key]:
            raise Phase06ARobustnessError(f"immutable hash metadata mismatch: {contract_key}")
    if inputs.get("phase06_runtime_artifact_required") is not False:
        raise Phase06ARobustnessError("Phase06 runtime artifact must not be required")
    if inputs.get("recompute_from_pinned_upstreams") is not True:
        raise Phase06ARobustnessError("recompute_from_pinned_upstreams must be true")

    baseline = contract.get("baseline_reference")
    expected_baseline = {
        "rolling_window_sessions": BASELINE_WINDOW,
        "minimum_history_sessions": BASELINE_MIN_HISTORY,
        "high_threshold": BASELINE_THRESHOLD,
        "cooldown_source_sessions": BASELINE_COOLDOWN,
        "exit_horizons_sessions": [5, 10, 20],
        "round_trip_cost_bps": PRIMARY_COST_BPS,
        "expected_independent_entry_dates": [row[0] for row in phase06.FROZEN_SCHEDULE],
    }
    if not isinstance(baseline, Mapping):
        raise Phase06ARobustnessError("baseline_reference is required")
    for key, value in expected_baseline.items():
        if baseline.get(key) != value:
            raise Phase06ARobustnessError(f"baseline mismatch: {key}")

    grid = contract.get("sensitivity_grid")
    if not isinstance(grid, Mapping):
        raise Phase06ARobustnessError("sensitivity_grid is required")
    expected_grid = {
        "rolling_window_sessions": list(WINDOW_GRID),
        "minimum_history_by_window": {str(k): v for k, v in MIN_HISTORY_BY_WINDOW.items()},
        "high_threshold": list(THRESHOLD_GRID),
        "cooldown_source_sessions": list(COOLDOWN_GRID),
        "exit_horizons_sessions": list(HORIZON_GRID),
        "round_trip_cost_bps": PRIMARY_COST_BPS,
        "configuration_count": EXPECTED_CONFIG_COUNT,
        "metric_cell_count": EXPECTED_METRIC_CELL_COUNT,
        "entry_execution": "target_trade_date open",
        "exit_execution": "fixed future source-session close",
    }
    for key, value in expected_grid.items():
        if grid.get(key) != value:
            raise Phase06ARobustnessError(f"sensitivity grid mismatch: {key}")

    methodology = contract.get("methodology")
    if not isinstance(methodology, Mapping):
        raise Phase06ARobustnessError("methodology is required")
    required_true = (
        "cooldown_applied_in_source_panel_sessions",
        "baseline_must_reproduce_phase06_schedule",
        "full_grid_must_be_reported",
        "best_parameter_combination_must_not_be_selected",
        "no_post_hoc_grid_expansion",
    )
    if any(methodology.get(key) is not True for key in required_true):
        raise Phase06ARobustnessError("methodology guard missing")
    if methodology.get("pyramiding_allowed") is not False:
        raise Phase06ARobustnessError("pyramiding must remain disabled")
    if methodology.get("terminal_source_ohlc_allowed") is not False:
        raise Phase06ARobustnessError("terminal source OHLC must remain forbidden")
    if methodology.get("gross_short_return_formula") != "(entry_open - exit_close) / entry_open":
        raise Phase06ARobustnessError("gross return formula mismatch")
    if methodology.get("net_short_return_formula") != "gross_short_return - cost_return":
        raise Phase06ARobustnessError("net return formula mismatch")

    aggregate = contract.get("aggregate_diagnostics")
    if not isinstance(aggregate, Mapping) or aggregate.get("robustness_claim_allowed") is not False:
        raise Phase06ARobustnessError("robustness claim must remain forbidden")

    if contract.get("runtime_artifacts") != list(DECLARED_OUTPUTS):
        raise Phase06ARobustnessError("runtime artifact inventory mismatch")

    authority = contract.get("authority_boundary")
    if not isinstance(authority, Mapping) or any(value is not False for value in authority.values()):
        raise Phase06ARobustnessError("authority boundary was widened")


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


def _execution_eligibility(indices: list[int], cooldown: int) -> list[bool]:
    if cooldown not in COOLDOWN_GRID:
        raise Phase06ARobustnessError("unsupported cooldown")
    accepted: list[bool] = []
    last_accepted: int | None = None
    for index in indices:
        eligible = last_accepted is None or index - last_accepted >= cooldown
        accepted.append(eligible)
        if eligible:
            last_accepted = index
    return accepted


def _config_id(window: int, threshold: float, cooldown: int) -> str:
    return f"w{window}_t{int(round(threshold * 100)):02d}_c{cooldown}"


def _build_config_candidates(
    observations: pd.DataFrame,
    panel: pd.DataFrame,
    *,
    window: int,
    threshold: float,
    cooldown: int,
) -> tuple[pd.DataFrame, int, int]:
    min_history = MIN_HISTORY_BY_WINDOW[window]
    oil_pct = phase04._rolling_percentile(
        pd.to_numeric(observations["brent_prior_close"], errors="coerce").to_numpy(float),
        window=window,
        min_history=min_history,
    )
    usd_pct = phase04._rolling_percentile(
        pd.to_numeric(observations["usdrubf_prior_close"], errors="coerce").to_numpy(float),
        window=window,
        min_history=min_history,
    )
    mask = np.isfinite(oil_pct) & np.isfinite(usd_pct) & (oil_pct >= threshold) & (usd_pct >= threshold)
    raw = observations.loc[mask].copy()
    raw["brent_percentile"] = oil_pct[mask]
    raw["usdrubf_percentile"] = usd_pct[mask]
    raw = raw.sort_values("entry_source_session_index", kind="mergesort").reset_index(drop=True)
    raw_signal_count = int(len(raw))

    terminal_index = len(panel) - 1
    executable = raw.loc[raw["entry_source_session_index"].astype(int) < terminal_index].copy()
    unbound_entry_excluded_count = raw_signal_count - int(len(executable))
    if executable.empty:
        executable["execution_eligible"] = pd.Series(dtype=bool)
        return executable, raw_signal_count, unbound_entry_excluded_count

    indices = executable["entry_source_session_index"].astype(int).tolist()
    executable["execution_eligible"] = _execution_eligibility(indices, cooldown)
    return executable, raw_signal_count, unbound_entry_excluded_count


def _baseline_schedule(candidates: pd.DataFrame, panel: pd.DataFrame) -> tuple[tuple[str, str, str, str, str], ...]:
    eligible = candidates.loc[candidates["execution_eligible"]].copy()
    eligible = eligible.sort_values("entry_source_session_index", kind="mergesort")
    panel_dates = panel["trade_date"].astype(str).tolist()
    rows: list[tuple[str, str, str, str, str]] = []
    for _, row in eligible.iterrows():
        entry_idx = int(row["entry_source_session_index"])
        endpoints = [entry_idx + horizon for horizon in phase06.EXIT_HORIZONS]
        if any(endpoint >= len(panel_dates) - 1 for endpoint in endpoints):
            raise Phase06ARobustnessError("baseline exit would consume terminal OHLC")
        rows.append(
            (
                str(row["target_trade_date"]),
                str(row["prior_trade_date"]),
                panel_dates[endpoints[0]],
                panel_dates[endpoints[1]],
                panel_dates[endpoints[2]],
            )
        )
    return tuple(rows)


def _trade_rows_for_config(
    candidates: pd.DataFrame,
    panel: pd.DataFrame,
    *,
    window: int,
    threshold: float,
    cooldown: int,
) -> list[dict[str, Any]]:
    eligible = candidates.loc[candidates["execution_eligible"]].copy()
    eligible = eligible.sort_values("entry_source_session_index", kind="mergesort")
    panel_dates = panel["trade_date"].astype(str).tolist()
    panel_open = pd.to_numeric(panel["open"], errors="coerce").to_numpy(float)
    panel_close = pd.to_numeric(panel["close"], errors="coerce").to_numpy(float)
    terminal_index = len(panel) - 1
    config_id = _config_id(window, threshold, cooldown)
    rows: list[dict[str, Any]] = []
    for trade_ordinal, (_, candidate) in enumerate(eligible.iterrows(), start=1):
        entry_idx = int(candidate["entry_source_session_index"])
        if entry_idx >= terminal_index:
            raise Phase06ARobustnessError("terminal entry OHLC consumption blocked")
        entry_open = phase06._safe_positive_price(panel_open[entry_idx], "entry_open")
        for horizon in HORIZON_GRID:
            exit_idx = entry_idx + horizon
            if exit_idx >= terminal_index:
                continue
            exit_close = phase06._safe_positive_price(panel_close[exit_idx], "exit_close")
            gross = phase06._gross_short_return(entry_open, exit_close)
            net = phase06._net_short_return(gross, PRIMARY_COST_BPS)
            rows.append(
                {
                    "config_id": config_id,
                    "rolling_window_sessions": window,
                    "minimum_history_sessions": MIN_HISTORY_BY_WINDOW[window],
                    "high_threshold": threshold,
                    "cooldown_source_sessions": cooldown,
                    "trade_ordinal": trade_ordinal,
                    "entry_trade_date": str(candidate["target_trade_date"]),
                    "prior_trade_date": str(candidate["prior_trade_date"]),
                    "entry_source_session_index": entry_idx,
                    "brent_percentile": float(candidate["brent_percentile"]),
                    "usdrubf_percentile": float(candidate["usdrubf_percentile"]),
                    "horizon_sessions": horizon,
                    "exit_trade_date": panel_dates[exit_idx],
                    "entry_price": entry_open,
                    "exit_price": exit_close,
                    "round_trip_cost_bps": PRIMARY_COST_BPS,
                    "gross_short_return": gross,
                    "net_short_return": net,
                    "net_profitable": bool(net > 0.0),
                }
            )
    return rows


def _metric_row(
    trade_frame: pd.DataFrame,
    *,
    window: int,
    threshold: float,
    cooldown: int,
    horizon: int,
    raw_signal_count: int,
    independent_trade_count: int,
    unbound_entry_excluded_count: int,
) -> dict[str, Any]:
    subset = trade_frame.loc[
        (trade_frame["config_id"] == _config_id(window, threshold, cooldown))
        & (trade_frame["horizon_sessions"] == horizon)
    ].sort_values("entry_trade_date", kind="mergesort") if not trade_frame.empty else pd.DataFrame()
    trade_count = int(len(subset))
    row: dict[str, Any] = {
        "config_id": _config_id(window, threshold, cooldown),
        "rolling_window_sessions": window,
        "minimum_history_sessions": MIN_HISTORY_BY_WINDOW[window],
        "high_threshold": threshold,
        "cooldown_source_sessions": cooldown,
        "horizon_sessions": horizon,
        "round_trip_cost_bps": PRIMARY_COST_BPS,
        "raw_signal_count": raw_signal_count,
        "independent_trade_count": independent_trade_count,
        "unbound_entry_excluded_count": unbound_entry_excluded_count,
        "trade_count": trade_count,
        "mean_net_return": np.nan,
        "median_net_return": np.nan,
        "hit_rate_net_positive": np.nan,
        "compounded_net_return": np.nan,
        "worst_trade_net_return": np.nan,
        "trade_sequence_max_drawdown": np.nan,
    }
    if trade_count == 0:
        return row
    net = pd.to_numeric(subset["net_short_return"], errors="coerce").to_numpy(float)
    if not np.isfinite(net).all():
        raise Phase06ARobustnessError("non-finite net return")
    row.update(
        {
            "mean_net_return": float(np.mean(net)),
            "median_net_return": float(np.median(net)),
            "hit_rate_net_positive": float(np.mean(net > 0.0)),
            "compounded_net_return": float(np.prod(1.0 + net) - 1.0),
            "worst_trade_net_return": float(np.min(net)),
            "trade_sequence_max_drawdown": phase06._trade_sequence_max_drawdown(net.tolist()),
        }
    )
    return row


def _build_aggregate_summary(metrics: pd.DataFrame) -> dict[str, Any]:
    evaluable = metrics.loc[metrics["trade_count"] > 0].copy()
    at_least_three = metrics.loc[metrics["trade_count"] >= 3].copy()

    def share(frame: pd.DataFrame, column: str, predicate: Any) -> float | None:
        if frame.empty:
            return None
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(float)
        finite = np.isfinite(values)
        if not finite.any():
            return None
        return float(np.mean(predicate(values[finite])))

    by_horizon: list[dict[str, Any]] = []
    for horizon in HORIZON_GRID:
        subset = metrics.loc[(metrics["horizon_sessions"] == horizon) & (metrics["trade_count"] > 0)]
        by_horizon.append(
            {
                "horizon_sessions": horizon,
                "evaluable_configuration_count": int(len(subset)),
                "positive_mean_cell_share": share(subset, "mean_net_return", lambda x: x > 0.0),
                "positive_median_cell_share": share(subset, "median_net_return", lambda x: x > 0.0),
                "hit_rate_majority_cell_share": share(subset, "hit_rate_net_positive", lambda x: x >= 0.5),
                "configuration_cells_with_at_least_3_trades": int((subset["trade_count"] >= 3).sum()),
            }
        )

    baseline = metrics.loc[
        (metrics["rolling_window_sessions"] == BASELINE_WINDOW)
        & np.isclose(metrics["high_threshold"], BASELINE_THRESHOLD)
        & (metrics["cooldown_source_sessions"] == BASELINE_COOLDOWN)
        & metrics["horizon_sessions"].isin(phase06.EXIT_HORIZONS)
    ].sort_values("horizon_sessions")

    return {
        "project": PROJECT,
        "task_id": TASK_ID,
        "status": "robustness_map_complete_sample_limited",
        "configuration_count": EXPECTED_CONFIG_COUNT,
        "metric_cell_count": EXPECTED_METRIC_CELL_COUNT,
        "evaluable_metric_cell_count": int(len(evaluable)),
        "metric_cells_with_at_least_3_trades": int(len(at_least_three)),
        "positive_mean_cell_share": share(evaluable, "mean_net_return", lambda x: x > 0.0),
        "positive_median_cell_share": share(evaluable, "median_net_return", lambda x: x > 0.0),
        "hit_rate_majority_cell_share": share(evaluable, "hit_rate_net_positive", lambda x: x >= 0.5),
        "horizon_diagnostics": by_horizon,
        "baseline_reference_metrics_20bps": baseline.to_dict(orient="records"),
        "sample_limitation": "Sensitivity cells reuse a small historical sample; descriptive map only, no robustness or optimization claim.",
        "parameter_optimization_performed": False,
        "parameter_ranking_performed": False,
        "winner_selection_performed": False,
        "statistical_inference_performed": False,
        "model_fit_performed": False,
        "position_sizing_performed": False,
        "strategy_promotion_performed": False,
        "broker_action_performed": False,
        "trading_action_performed": False,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_oil_fx_rub_v1_phase06a_robustness_sensitivity"
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
    if not run_id or _ALIAS_PATTERN.search(run_id):
        raise Phase06ARobustnessError("--run-id must be explicit and immutable")
    if not _SHA40_PATTERN.fullmatch(git_sha):
        raise Phase06ARobustnessError("--git-commit-sha must be exactly 40 lowercase hex characters")

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
        raise Phase06ARobustnessError("Brent input identity evidence must be non-empty")

    observations, panel = phase05._prepare_signal_observations(modeling, source_panel, brent_matrix)
    if len(observations) != phase04.EXPECTED_IDENTITY_COUNT:
        raise Phase06ARobustnessError("identity count mismatch")

    baseline_candidates, _, _ = _build_config_candidates(
        observations,
        panel,
        window=BASELINE_WINDOW,
        threshold=BASELINE_THRESHOLD,
        cooldown=BASELINE_COOLDOWN,
    )
    if _baseline_schedule(baseline_candidates, panel) != phase06.FROZEN_SCHEDULE:
        raise Phase06ARobustnessError("baseline sensitivity configuration does not reproduce Phase06 schedule")

    trade_rows: list[dict[str, Any]] = []
    config_stats: dict[tuple[int, float, int], tuple[int, int, int]] = {}
    for window, threshold, cooldown in product(WINDOW_GRID, THRESHOLD_GRID, COOLDOWN_GRID):
        candidates, raw_count, unbound_count = _build_config_candidates(
            observations,
            panel,
            window=window,
            threshold=threshold,
            cooldown=cooldown,
        )
        independent_count = int(candidates["execution_eligible"].sum()) if "execution_eligible" in candidates else 0
        config_stats[(window, threshold, cooldown)] = (raw_count, independent_count, unbound_count)
        trade_rows.extend(
            _trade_rows_for_config(
                candidates,
                panel,
                window=window,
                threshold=threshold,
                cooldown=cooldown,
            )
        )

    trade_columns = [
        "config_id", "rolling_window_sessions", "minimum_history_sessions", "high_threshold",
        "cooldown_source_sessions", "trade_ordinal", "entry_trade_date", "prior_trade_date",
        "entry_source_session_index", "brent_percentile", "usdrubf_percentile", "horizon_sessions",
        "exit_trade_date", "entry_price", "exit_price", "round_trip_cost_bps",
        "gross_short_return", "net_short_return", "net_profitable",
    ]
    trades = pd.DataFrame(trade_rows, columns=trade_columns)

    metric_rows: list[dict[str, Any]] = []
    for window, threshold, cooldown in product(WINDOW_GRID, THRESHOLD_GRID, COOLDOWN_GRID):
        raw_count, independent_count, unbound_count = config_stats[(window, threshold, cooldown)]
        for horizon in HORIZON_GRID:
            metric_rows.append(
                _metric_row(
                    trades,
                    window=window,
                    threshold=threshold,
                    cooldown=cooldown,
                    horizon=horizon,
                    raw_signal_count=raw_count,
                    independent_trade_count=independent_count,
                    unbound_entry_excluded_count=unbound_count,
                )
            )
    metrics = pd.DataFrame(metric_rows)
    if len(config_stats) != EXPECTED_CONFIG_COUNT:
        raise Phase06ARobustnessError("configuration count mismatch")
    if len(metrics) != EXPECTED_METRIC_CELL_COUNT:
        raise Phase06ARobustnessError("metric cell count mismatch")
    if set(metrics["round_trip_cost_bps"].astype(int)) != {PRIMARY_COST_BPS}:
        raise Phase06ARobustnessError("cost grid drift")

    summary = _build_aggregate_summary(metrics)
    gates: dict[str, dict[str, Any]] = {
        "G1_lineage": {"passed": True, "immutable_input_sha256": observed_hashes},
        "G2_frozen_grid": {
            "passed": True,
            "configuration_count": EXPECTED_CONFIG_COUNT,
            "metric_cell_count": EXPECTED_METRIC_CELL_COUNT,
        },
        "G3_baseline_reproduction": {
            "passed": True,
            "baseline_schedule": [list(row) for row in phase06.FROZEN_SCHEDULE],
        },
        "G4_full_parameter_map": {
            "passed": len(config_stats) == EXPECTED_CONFIG_COUNT,
            "windows": list(WINDOW_GRID),
            "thresholds": list(THRESHOLD_GRID),
            "cooldowns": list(COOLDOWN_GRID),
            "horizons": list(HORIZON_GRID),
        },
        "G5_metric_inventory": {
            "passed": len(metrics) == EXPECTED_METRIC_CELL_COUNT,
            "metric_cell_count": int(len(metrics)),
        },
        "G6_execution_and_cost_math": {
            "passed": True,
            "entry_execution": "target_trade_date_open",
            "exit_execution": "future_source_session_close",
            "terminal_source_ohlc_consumed": False,
            "round_trip_cost_bps": PRIMARY_COST_BPS,
        },
        "G7_no_selection_or_inference": {
            "passed": True,
            "parameter_optimization_performed": False,
            "parameter_ranking_performed": False,
            "winner_selection_performed": False,
            "statistical_inference_performed": False,
            "robustness_claim_performed": False,
        },
        "G8_research_only": {
            "passed": True,
            "network_access_performed": False,
            "upstream_artifact_mutation_performed": False,
            "model_fit_performed": False,
            "position_sizing_performed": False,
            "strategy_promotion_performed": False,
            "broker_action_performed": False,
            "trading_action_performed": False,
        },
    }
    failed = [key for key, value in gates.items() if value.get("passed") is not True]
    gates["G9_final"] = {
        "passed": not failed,
        "failed_gates": failed,
        "status": "oil_fx_rub_phase06a_robustness_sensitivity_complete" if not failed else "blocked",
    }

    manifest = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": run_id,
        "git_commit_sha": git_sha,
        "identity_count": int(len(observations)),
        "input_sha256": observed_hashes,
        "configuration_count": EXPECTED_CONFIG_COUNT,
        "metric_cell_count": EXPECTED_METRIC_CELL_COUNT,
        "trade_result_row_count": int(len(trades)),
        "runtime_artifacts": list(DECLARED_OUTPUTS),
        "network_access_performed": False,
        "upstream_artifact_mutation_performed": False,
        "parameter_optimization_performed": False,
        "winner_selection_performed": False,
        "strategy_promotion_performed": False,
        "trading_action_performed": False,
    }
    identity_verification = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "eligible_identity_count": int(len(observations)),
        "instrument_id": EXPECTED_INSTRUMENT,
        "phase6_lineage_replayed": True,
        "brent_immutable_artifacts_verified": True,
        "baseline_phase06_schedule_reproduced": True,
    }

    output_dir.mkdir(parents=True, exist_ok=False)
    phase04._write_json(output_dir / "input_identity_verification.json", identity_verification)
    trades.to_csv(output_dir / "sensitivity_trade_results.csv", index=False)
    metrics.to_csv(output_dir / "sensitivity_metrics.csv", index=False)
    phase04._write_json(output_dir / "sensitivity_summary.json", summary)
    phase04._write_json(output_dir / "research_manifest.json", manifest)
    phase04._write_json(output_dir / "gate_results.json", gates)

    if tuple(sorted(path.name for path in output_dir.iterdir())) != tuple(sorted(DECLARED_OUTPUTS)):
        raise Phase06ARobustnessError("output artifact inventory mismatch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
