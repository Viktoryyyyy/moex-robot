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


PROJECT: Final[str] = "MOEX_Bot"
TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_06_BACKTEST"
CONTRACT_ID: Final[str] = "usdrubf_oil_fx_rub_v1_phase06_backtest"
CONTRACT_VERSION: Final[str] = "1.0"
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"
EXIT_HORIZONS: Final[tuple[int, ...]] = (5, 10, 20)
COST_GRID_BPS: Final[tuple[int, ...]] = (0, 5, 10, 20)
EXPECTED_INDEPENDENT_TRADES: Final[int] = 3

FROZEN_SCHEDULE: Final[tuple[tuple[str, str, str, str, str], ...]] = (
    ("2025-01-13", "2025-01-10", "2025-01-20", "2025-01-27", "2025-02-10"),
    ("2025-09-29", "2025-09-26", "2025-10-06", "2025-10-13", "2025-10-27"),
    ("2026-03-17", "2026-03-16", "2026-03-24", "2026-03-31", "2026-04-14"),
)

DECLARED_OUTPUTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "trade_results.csv",
    "backtest_metrics.csv",
    "backtest_summary.json",
    "research_manifest.json",
    "gate_results.json",
)

_ALIAS_PATTERN = re.compile(r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.I)
_SHA40_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_GLOB_CHARS = frozenset("*?[]")


class Phase06BacktestError(ValueError):
    """Fail-closed error for Oil/FX/RUB Phase 06 backtest."""


def _read_json(path: str | Path) -> dict[str, Any]:
    return phase05._read_json(path)


def _explicit_file(raw: object, flag: str, suffix: str) -> Path:
    return phase05._explicit_file(raw, flag, suffix)


def _explicit_output_dir(raw: object) -> Path:
    return phase05._explicit_output_dir(raw)


def _contract_schedule_rows(contract: Mapping[str, Any]) -> tuple[tuple[str, str, str, str, str], ...]:
    rows = contract.get("frozen_phase05_schedule")
    if not isinstance(rows, list):
        raise Phase06BacktestError("frozen_phase05_schedule is required")
    normalized: list[tuple[str, str, str, str, str]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise Phase06BacktestError("frozen Phase05 schedule row must be an object")
        normalized.append(
            (
                str(row.get("target_trade_date", "")),
                str(row.get("prior_trade_date", "")),
                str(row.get("exit_5session_trade_date", "")),
                str(row.get("exit_10session_trade_date", "")),
                str(row.get("exit_20session_trade_date", "")),
            )
        )
    return tuple(normalized)


def _validate_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity")
    if not isinstance(identity, Mapping):
        raise Phase06BacktestError("contract_identity is required")
    expected_identity = {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": TASK_ID,
    }
    for key, value in expected_identity.items():
        if identity.get(key) != value:
            raise Phase06BacktestError(f"contract identity mismatch: {key}")

    purpose = contract.get("purpose")
    if not isinstance(purpose, Mapping) or purpose.get("performance_evaluation_allowed") is not True:
        raise Phase06BacktestError("performance evaluation must be explicitly allowed")
    for key in (
        "model_fit_allowed",
        "parameter_optimization_allowed",
        "horizon_ranking_allowed",
        "position_sizing_allowed",
        "strategy_promotion_allowed",
        "trading_allowed",
    ):
        if purpose.get(key) is not False:
            raise Phase06BacktestError(f"purpose boundary widened: {key}")

    inputs = contract.get("inputs")
    if not isinstance(inputs, Mapping):
        raise Phase06BacktestError("inputs are required")
    hash_key_map = {
        "phase6_modeling_dataset_sha256": "phase6_modeling_dataset",
        "phase6_dataset_manifest_sha256": "phase6_dataset_manifest",
        "brent_pit_acceptance_matrix_sha256": "brent_pit_acceptance_matrix",
        "phase84a_gate_results_sha256": "phase84a_gate_results",
        "phase84a_input_identity_sha256": "phase84a_input_identity",
    }
    for contract_key, phase04_key in hash_key_map.items():
        if inputs.get(contract_key) != phase04.EXPECTED_SHA256[phase04_key]:
            raise Phase06BacktestError(f"immutable hash metadata mismatch: {contract_key}")
    if inputs.get("phase05_runtime_artifact_required") is not False:
        raise Phase06BacktestError("Phase05 runtime artifact must not be required")

    if _contract_schedule_rows(contract) != FROZEN_SCHEDULE:
        raise Phase06BacktestError("frozen Phase05 schedule mismatch")

    backtest = contract.get("backtest_definition")
    if not isinstance(backtest, Mapping):
        raise Phase06BacktestError("backtest_definition is required")
    expected_backtest = {
        "direction": "short_usdrubf_long_rub",
        "entry_execution": "target_trade_date open",
        "entry_price_field": "open",
        "fixed_exit_horizons_sessions": list(EXIT_HORIZONS),
        "exit_execution": "close",
        "exit_price_field": "close",
        "round_trip_cost_bps_sensitivity": list(COST_GRID_BPS),
        "trade_overlap_allowed": False,
        "pyramiding_allowed": False,
        "stop_loss_allowed_in_phase06": False,
        "position_sizing_allowed_in_phase06": False,
    }
    for key, value in expected_backtest.items():
        if backtest.get(key) != value:
            raise Phase06BacktestError(f"backtest definition mismatch: {key}")
    if backtest.get("gross_short_return_formula") != "(entry_open - exit_close) / entry_open":
        raise Phase06BacktestError("gross return formula mismatch")
    if backtest.get("cost_return_formula") != "round_trip_cost_bps / 10000":
        raise Phase06BacktestError("cost formula mismatch")
    if backtest.get("net_short_return_formula") != "gross_short_return - cost_return":
        raise Phase06BacktestError("net return formula mismatch")

    sample = contract.get("sample_policy")
    if not isinstance(sample, Mapping):
        raise Phase06BacktestError("sample_policy is required")
    if sample.get("frozen_independent_trade_count") != EXPECTED_INDEPENDENT_TRADES:
        raise Phase06BacktestError("frozen independent trade count mismatch")
    if sample.get("statistical_inference_allowed") is not False:
        raise Phase06BacktestError("statistical inference must remain disabled")
    if sample.get("robustness_claim_allowed") is not False:
        raise Phase06BacktestError("robustness claims must remain disabled")

    metrics = contract.get("metrics_definition")
    if not isinstance(metrics, Mapping):
        raise Phase06BacktestError("metrics_definition is required")
    if metrics.get("horizon_ranking_allowed") is not False:
        raise Phase06BacktestError("horizon ranking must remain disabled")
    if metrics.get("best_variant_selection_allowed") is not False:
        raise Phase06BacktestError("best variant selection must remain disabled")

    if contract.get("runtime_artifacts") != list(DECLARED_OUTPUTS):
        raise Phase06BacktestError("runtime artifact inventory mismatch")

    authority = contract.get("authority_boundary")
    if not isinstance(authority, Mapping) or any(value is not False for value in authority.values()):
        raise Phase06BacktestError("authority boundary was widened")


def _validate_upstream_semantics(
    *, phase05_contract: Mapping[str, Any], phase04_contract: Mapping[str, Any]
) -> None:
    phase05._validate_contract(phase05_contract)
    phase05._validate_phase04_semantics(phase04_contract)
    if phase05.EXIT_HORIZONS != EXIT_HORIZONS:
        raise Phase06BacktestError("Phase05 exit horizons changed")
    if phase05.COST_GRID_BPS != COST_GRID_BPS:
        raise Phase06BacktestError("Phase05 cost grid changed")
    if phase05.MIN_ENTRY_SEPARATION != 21:
        raise Phase06BacktestError("Phase05 entry separation changed")


def _schedule_from_candidates(candidates: pd.DataFrame) -> tuple[tuple[str, str, str, str, str], ...]:
    eligible = candidates.loc[candidates["execution_eligible"]].copy()
    eligible = eligible.sort_values("entry_source_session_index", kind="mergesort")
    if len(eligible) != EXPECTED_INDEPENDENT_TRADES:
        raise Phase06BacktestError("execution-eligible Phase05 trade count mismatch")
    if not (eligible["direction"] == "short_usdrubf_long_rub").all():
        raise Phase06BacktestError("Phase05 direction mismatch")
    if not (eligible["signal_regime"] == "high_high").all():
        raise Phase06BacktestError("Phase05 regime mismatch")

    rows: list[tuple[str, str, str, str, str]] = []
    for _, row in eligible.iterrows():
        if not all(bool(row[f"exit_{h}session_available"]) for h in EXIT_HORIZONS):
            raise Phase06BacktestError("frozen Phase05 trade has unavailable exit horizon")
        rows.append(
            (
                str(row["target_trade_date"]),
                str(row["prior_trade_date"]),
                str(row["exit_5session_trade_date"]),
                str(row["exit_10session_trade_date"]),
                str(row["exit_20session_trade_date"]),
            )
        )
    return tuple(rows)


def _recompute_frozen_candidates(
    *,
    modeling: pd.DataFrame,
    source_panel: pd.DataFrame,
    brent_matrix: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    observations, panel = phase05._prepare_signal_observations(modeling, source_panel, brent_matrix)
    candidates = phase05._build_signal_candidates(observations, panel)
    if _schedule_from_candidates(candidates) != FROZEN_SCHEDULE:
        raise Phase06BacktestError("recomputed Phase05 schedule differs from frozen Phase06 schedule")
    return candidates, panel


def _safe_positive_price(value: object, label: str) -> float:
    try:
        price = float(value)
    except (TypeError, ValueError) as exc:
        raise Phase06BacktestError(f"{label} is not numeric") from exc
    if not math.isfinite(price) or price <= 0:
        raise Phase06BacktestError(f"{label} must be finite positive")
    return price


def _gross_short_return(entry_open: float, exit_close: float) -> float:
    entry = _safe_positive_price(entry_open, "entry_open")
    exit_price = _safe_positive_price(exit_close, "exit_close")
    return (entry - exit_price) / entry


def _net_short_return(gross_return: float, cost_bps: int) -> float:
    if cost_bps not in COST_GRID_BPS:
        raise Phase06BacktestError("unsupported cost bps")
    if not math.isfinite(float(gross_return)):
        raise Phase06BacktestError("gross return must be finite")
    return float(gross_return) - (float(cost_bps) / 10000.0)


def _trade_sequence_max_drawdown(net_returns: list[float]) -> float:
    values = np.asarray(net_returns, dtype=float)
    if values.size == 0 or not np.isfinite(values).all():
        raise Phase06BacktestError("drawdown requires finite non-empty returns")
    if np.any(values <= -1.0):
        raise Phase06BacktestError("trade return <= -100% makes compounded equity invalid")
    equity = np.concatenate(([1.0], np.cumprod(1.0 + values)))
    running_peak = np.maximum.accumulate(equity)
    drawdowns = 1.0 - (equity / running_peak)
    return float(np.max(drawdowns))


def _validate_price_binding(candidates: pd.DataFrame, panel: pd.DataFrame) -> None:
    eligible = candidates.loc[candidates["execution_eligible"]].copy()
    eligible = eligible.sort_values("entry_source_session_index", kind="mergesort")
    panel_dates = panel["trade_date"].astype(str).tolist()
    date_to_index = {date: idx for idx, date in enumerate(panel_dates)}
    terminal_index = len(panel_dates) - 1

    for _, row in eligible.iterrows():
        entry_date = str(row["target_trade_date"])
        entry_idx = date_to_index.get(entry_date)
        if entry_idx is None or entry_idx != int(row["entry_source_session_index"]):
            raise Phase06BacktestError("entry date/index mismatch")
        if entry_idx >= terminal_index:
            raise Phase06BacktestError("entry open is not bound by next-row Phase6 replay")
        _safe_positive_price(panel.iloc[entry_idx]["open"], f"entry open {entry_date}")

        for horizon in EXIT_HORIZONS:
            exit_date = str(row[f"exit_{horizon}session_trade_date"])
            exit_idx = date_to_index.get(exit_date)
            if exit_idx is None or exit_idx != entry_idx + horizon:
                raise Phase06BacktestError("exit date/index mismatch")
            if exit_idx >= terminal_index:
                raise Phase06BacktestError("exit close is not bound by next-row Phase6 replay")
            _safe_positive_price(panel.iloc[exit_idx]["close"], f"exit close {exit_date}")


def _materialize_trade_results(candidates: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    _validate_price_binding(candidates, panel)
    eligible = candidates.loc[candidates["execution_eligible"]].copy()
    eligible = eligible.sort_values("entry_source_session_index", kind="mergesort")
    panel_dates = panel["trade_date"].astype(str).tolist()
    date_to_index = {date: idx for idx, date in enumerate(panel_dates)}

    rows: list[dict[str, Any]] = []
    for _, signal in eligible.iterrows():
        entry_date = str(signal["target_trade_date"])
        entry_idx = date_to_index[entry_date]
        entry_open = _safe_positive_price(panel.iloc[entry_idx]["open"], "entry_open")
        for horizon in EXIT_HORIZONS:
            exit_date = str(signal[f"exit_{horizon}session_trade_date"])
            exit_idx = date_to_index[exit_date]
            exit_close = _safe_positive_price(panel.iloc[exit_idx]["close"], "exit_close")
            gross = _gross_short_return(entry_open, exit_close)
            for cost_bps in COST_GRID_BPS:
                net = _net_short_return(gross, cost_bps)
                rows.append(
                    {
                        "signal_id": str(signal["signal_id"]),
                        "entry_trade_date": entry_date,
                        "prior_trade_date": str(signal["prior_trade_date"]),
                        "horizon_sessions": horizon,
                        "exit_trade_date": exit_date,
                        "direction": "short_usdrubf_long_rub",
                        "entry_execution": "open",
                        "exit_execution": "close",
                        "entry_price": entry_open,
                        "exit_price": exit_close,
                        "gross_short_return": gross,
                        "round_trip_cost_bps": cost_bps,
                        "cost_return": float(cost_bps) / 10000.0,
                        "net_short_return": net,
                        "net_profitable": bool(net > 0.0),
                    }
                )
    result = pd.DataFrame(rows)
    expected_rows = EXPECTED_INDEPENDENT_TRADES * len(EXIT_HORIZONS) * len(COST_GRID_BPS)
    if len(result) != expected_rows:
        raise Phase06BacktestError("trade result row count mismatch")
    return result


def _build_backtest_metrics(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for horizon in EXIT_HORIZONS:
        for cost_bps in COST_GRID_BPS:
            group = trades.loc[
                trades["horizon_sessions"].eq(horizon)
                & trades["round_trip_cost_bps"].eq(cost_bps)
            ].sort_values("entry_trade_date", kind="mergesort")
            if len(group) != EXPECTED_INDEPENDENT_TRADES:
                raise Phase06BacktestError("metric group trade count mismatch")
            gross = group["gross_short_return"].to_numpy(float)
            net = group["net_short_return"].to_numpy(float)
            if not np.isfinite(gross).all() or not np.isfinite(net).all():
                raise Phase06BacktestError("metrics contain non-finite returns")
            compounded = float(np.prod(1.0 + net) - 1.0)
            rows.append(
                {
                    "horizon_sessions": horizon,
                    "round_trip_cost_bps": cost_bps,
                    "trade_count": int(len(group)),
                    "mean_gross_return": float(np.mean(gross)),
                    "mean_net_return": float(np.mean(net)),
                    "median_net_return": float(np.median(net)),
                    "hit_rate_net_positive": float(np.mean(net > 0.0)),
                    "compounded_net_return": compounded,
                    "worst_trade_net_return": float(np.min(net)),
                    "trade_sequence_max_drawdown": _trade_sequence_max_drawdown(net.tolist()),
                }
            )
    return pd.DataFrame(rows)


def _build_summary(trades: pd.DataFrame, metrics: pd.DataFrame) -> dict[str, Any]:
    return {
        "project": PROJECT,
        "task_id": TASK_ID,
        "status": "backtest_complete_sample_limited",
        "signal_id": "oil_fx_rub_v1_high_high_short",
        "independent_trade_count": EXPECTED_INDEPENDENT_TRADES,
        "trade_result_row_count": int(len(trades)),
        "metric_variant_count": int(len(metrics)),
        "direction": "short_usdrubf_long_rub",
        "entry_execution": "target_trade_date open",
        "fixed_exit_horizons_sessions": list(EXIT_HORIZONS),
        "round_trip_cost_bps_sensitivity": list(COST_GRID_BPS),
        "sample_limitation": "Three independent entries only; results are descriptive and do not establish robustness.",
        "statistical_inference_performed": False,
        "horizon_ranking_performed": False,
        "best_variant_selection_performed": False,
        "parameter_optimization_performed": False,
        "position_sizing_performed": False,
        "model_fit_performed": False,
        "strategy_promotion_performed": False,
        "broker_action_performed": False,
        "trading_action_performed": False,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_oil_fx_rub_v1_phase06_backtest"
    )
    for flag in (
        "--contract-path",
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
    phase05_contract_path = _explicit_file(
        args.phase05_contract_path, "--phase05-contract-path", ".json"
    )
    phase04_contract_path = _explicit_file(
        args.phase04_contract_path, "--phase04-contract-path", ".json"
    )
    phase6_modeling = _explicit_file(
        args.phase6_modeling_dataset_path, "--phase6-modeling-dataset-path", ".parquet"
    )
    phase6_manifest = _explicit_file(
        args.phase6_dataset_manifest_path, "--phase6-dataset-manifest-path", ".json"
    )
    phase6_source = _explicit_file(
        args.phase6_source_panel_path, "--phase6-source-panel-path", ".parquet"
    )
    brent_matrix_path = _explicit_file(
        args.brent_matrix_path, "--brent-matrix-path", ".parquet"
    )
    brent_gates_path = _explicit_file(
        args.brent_gate_results_path, "--brent-gate-results-path", ".json"
    )
    brent_identity_path = _explicit_file(
        args.brent_input_identity_path, "--brent-input-identity-path", ".json"
    )
    output_dir = _explicit_output_dir(args.output_dir)

    run_id = str(args.run_id).strip()
    git_sha = str(args.git_commit_sha).strip().lower()
    if not run_id or any(char in run_id for char in _GLOB_CHARS) or _ALIAS_PATTERN.search(run_id):
        raise Phase06BacktestError("--run-id must be explicit and immutable")
    if not _SHA40_PATTERN.fullmatch(git_sha):
        raise Phase06BacktestError("--git-commit-sha must be exactly 40 lowercase hex characters")

    contract = _read_json(contract_path)
    phase05_contract = _read_json(phase05_contract_path)
    phase04_contract = _read_json(phase04_contract_path)
    _validate_contract(contract)
    _validate_upstream_semantics(
        phase05_contract=phase05_contract,
        phase04_contract=phase04_contract,
    )

    immutable_paths = {
        "phase6_modeling_dataset": phase6_modeling,
        "phase6_dataset_manifest": phase6_manifest,
        "brent_pit_acceptance_matrix": brent_matrix_path,
        "phase84a_gate_results": brent_gates_path,
        "phase84a_input_identity": brent_identity_path,
    }
    observed_hashes = phase04._validate_immutable_hashes(immutable_paths)

    modeling = pd.read_parquet(phase6_modeling)
    source_panel = pd.read_parquet(phase6_source)
    brent_matrix = pd.read_parquet(brent_matrix_path)
    brent_gates = _read_json(brent_gates_path)
    brent_identity = _read_json(brent_identity_path)
    phase04._validate_brent_evidence(brent_gates)
    if not brent_identity:
        raise Phase06BacktestError("Brent input identity evidence must be non-empty")

    candidates, panel = _recompute_frozen_candidates(
        modeling=modeling,
        source_panel=source_panel,
        brent_matrix=brent_matrix,
    )
    trades = _materialize_trade_results(candidates, panel)
    metrics = _build_backtest_metrics(trades)
    summary = _build_summary(trades, metrics)

    gates: dict[str, dict[str, Any]] = {
        "G1_lineage": {"passed": True, "input_sha256": observed_hashes},
        "G2_frozen_signal_schedule": {
            "passed": True,
            "independent_trade_count": EXPECTED_INDEPENDENT_TRADES,
            "entry_dates": [row[0] for row in FROZEN_SCHEDULE],
        },
        "G3_price_binding": {
            "passed": True,
            "phase6_source_panel_replayed": True,
            "all_consumed_price_rows_nonterminal": True,
        },
        "G4_execution_semantics": {
            "passed": True,
            "direction": "short_usdrubf_long_rub",
            "entry_execution": "open",
            "exit_execution": "close",
            "horizons_sessions": list(EXIT_HORIZONS),
        },
        "G5_return_math": {
            "passed": True,
            "formula": "(entry_open - exit_close) / entry_open",
        },
        "G6_cost_math": {
            "passed": True,
            "round_trip_cost_bps_sensitivity": list(COST_GRID_BPS),
            "cost_applied_once_per_trade": True,
        },
        "G7_metrics": {
            "passed": True,
            "statistical_inference_performed": False,
            "horizon_ranking_performed": False,
            "best_variant_selection_performed": False,
            "parameter_optimization_performed": False,
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
        "status": "oil_fx_rub_phase06_backtest_complete" if not failed else "blocked",
    }

    manifest = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": run_id,
        "git_commit_sha": git_sha,
        "input_sha256": observed_hashes,
        "frozen_independent_trade_count": EXPECTED_INDEPENDENT_TRADES,
        "trade_result_row_count": int(len(trades)),
        "metric_variant_count": int(len(metrics)),
        "runtime_artifacts": list(DECLARED_OUTPUTS),
        "network_access_performed": False,
        "upstream_artifact_mutation_performed": False,
        "model_fit_performed": False,
        "parameter_optimization_performed": False,
        "horizon_ranking_performed": False,
        "position_sizing_performed": False,
        "strategy_promotion_performed": False,
        "trading_action_performed": False,
    }
    identity_verification = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "instrument_id": EXPECTED_INSTRUMENT,
        "phase6_lineage_replayed": True,
        "brent_immutable_artifacts_verified": True,
        "phase05_schedule_recomputed": True,
        "phase05_schedule_exact_match": True,
        "consumed_price_rows_nonterminal": True,
    }

    output_dir.mkdir(parents=True, exist_ok=False)
    trades.to_csv(output_dir / "trade_results.csv", index=False)
    metrics.to_csv(output_dir / "backtest_metrics.csv", index=False)
    phase04._write_json(output_dir / "input_identity_verification.json", identity_verification)
    phase04._write_json(output_dir / "backtest_summary.json", summary)
    phase04._write_json(output_dir / "research_manifest.json", manifest)
    phase04._write_json(output_dir / "gate_results.json", gates)

    if tuple(sorted(path.name for path in output_dir.iterdir())) != tuple(sorted(DECLARED_OUTPUTS)):
        raise Phase06BacktestError("output artifact inventory mismatch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
