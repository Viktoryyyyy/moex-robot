"""Rework of the multi-session USDRUBF reversal research prototype.

This engine removes fixed holding-period exits, separates execution costs, and
performs expanding-window selection before each out-of-sample fold.  It has no
alerting, order, deployment, or operational-monitor integration.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.usdrubf_multisession_sequence_20261110 import study as stage1  # noqa: E402


@dataclass(frozen=True)
class Costs:
    commission_per_side: float = 0.005
    half_spread_per_side: float = 0.010
    slippage_per_side: float = 0.010

    @property
    def friction_per_side(self) -> float:
        return self.half_spread_per_side + self.slippage_per_side

    @property
    def all_in_per_side(self) -> float:
        return self.commission_per_side + self.friction_per_side


BASE_COSTS = Costs()
STRESS_COSTS = Costs(slippage_per_side=0.035)


@dataclass(frozen=True)
class Variant:
    pattern: str
    formation: int
    confirmation: str
    life: int
    stop_buffer: float
    exit_mode: str
    trail_atr: float = 0.0
    structure_lookback: int = 0

    @property
    def key(self) -> str:
        return (
            f"{self.pattern}|formation={self.formation}|confirm={self.confirmation}"
            f"|life={self.life}|buffer={self.stop_buffer:g}|exit={self.exit_mode}"
            f"|trail={self.trail_atr:g}|structure={self.structure_lookback}"
        )


@dataclass(frozen=True)
class Fold:
    name: str
    train_end: str
    validate_start: str
    validate_end: str


FOLDS = (
    Fold("2025", "2024-12-31", "2025-01-01", "2025-12-31"),
    Fold("2026_h1", "2025-12-31", "2026-01-01", "2026-06-30"),
    Fold("2026_q3", "2026-06-30", "2026-07-01", "2026-09-30"),
)
HISTORIC_HOLDOUT = ("2026-10-01", "2026-11-09")
FUTURE_HOLDOUT_START = "2026-11-10"
EXIT_SPECS = (
    ("atr_trailing", 1.5, 0),
    ("atr_trailing", 2.5, 0),
    ("structure", 0.0, 1),
    ("structure", 0.0, 2),
    ("hybrid", 2.0, 2),
)


_WORKER_MARKET: tuple[pd.DataFrame, pd.DataFrame] | None = None
_WORKER_FUNDING: dict[str, float | None] | None = None
_WORKER_CATALOGS: dict[tuple[str, int], list[dict[str, Any]]] | None = None


def variants() -> list[Variant]:
    return [
        Variant(pattern, formation, confirmation, life, buffer, mode, trail, structure)
        for pattern in stage1.PATTERNS
        for formation in (3, 4, 5)
        for confirmation in stage1.CONFIRMATIONS
        for life in (3, 5)
        for buffer in (0.1, 0.25)
        for mode, trail, structure in EXIT_SPECS
    ]


def _execution_price(raw_price: float, side: int, entry: bool, costs: Costs) -> float:
    direction = side if entry else -side
    return float(raw_price + direction * costs.friction_per_side)


def _close_trade(
    position: Mapping[str, Any],
    exit_index: int,
    raw_exit: float,
    reason: str,
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    funding: Mapping[str, float | None],
    costs: Costs,
) -> dict[str, Any]:
    side = int(position["side"])
    entry = float(position["entry"])
    exit_price = _execution_price(raw_exit, side, False, costs)
    path = bars.iloc[int(position["entry_index"]) : exit_index + 1]
    favorable = float(path["high"].max() - entry) if side == 1 else float(entry - path["low"].min())
    adverse = float(path["low"].min() - entry) if side == 1 else float(entry - path["high"].max())
    funding_cost, missing = stage1.base._funding_cost(
        pd.Timestamp(position["entry_ts"]),
        pd.Timestamp(bars.iloc[exit_index]["ts"]),
        side,
        entry,
        daily,
        funding,
    )
    execution_gross = side * (exit_price - entry)
    commission = 2.0 * costs.commission_per_side
    net = execution_gross - commission - funding_cost
    risk = float(position["risk"])
    return {
        **position,
        "exit_index": int(exit_index),
        "exit_date": str(bars.iloc[exit_index]["date"]),
        "exit_ts": str(bars.iloc[exit_index]["ts"]),
        "exit_raw": float(raw_exit),
        "exit": exit_price,
        "exit_reason": reason,
        "commission_points": commission,
        "half_spread_points": 2.0 * costs.half_spread_per_side,
        "slippage_points": 2.0 * costs.slippage_per_side,
        "explicit_all_in_points": 2.0 * costs.all_in_per_side,
        "funding_cost_points": funding_cost,
        "missing_funding_events": missing,
        "gross_points_before_execution": side * (float(raw_exit) - float(position["entry_raw"])),
        "gross_points_after_spread_slippage": execution_gross,
        "net_points": net,
        "net_r": net / risk,
        "net_return_pct": 100.0 * net / entry,
        "mae_points": adverse,
        "mfe_points": favorable,
        "mae_r": adverse / risk,
        "mfe_r": favorable / risk,
        "net_rub": 1000.0 * net,
    }


def _trail_stop(
    variant: Variant,
    side: int,
    current_stop: float,
    entry_session: int,
    current_session: int,
    daily: pd.DataFrame,
) -> tuple[float, str]:
    stop = current_stop
    source = "initial_stop"
    atr_known = float(daily.iloc[current_session]["atr20_prior"])
    if variant.exit_mode in ("atr_trailing", "hybrid") and np.isfinite(atr_known):
        completed = daily.iloc[entry_session:current_session]
        if not completed.empty:
            if side == 1:
                candidate = float(completed["high"].max()) - variant.trail_atr * atr_known
                if candidate > stop:
                    stop = candidate
                    source = "atr_trailing"
            else:
                candidate = float(completed["low"].min()) + variant.trail_atr * atr_known
                if candidate < stop:
                    stop = candidate
                    source = "atr_trailing"
    if variant.exit_mode in ("structure", "hybrid"):
        left = max(entry_session, current_session - variant.structure_lookback)
        completed = daily.iloc[left:current_session]
        if not completed.empty:
            if side == 1:
                candidate = float(completed["low"].min()) - 0.01
                if candidate > stop:
                    stop = candidate
                    source = "structure"
            else:
                candidate = float(completed["high"].max()) + 0.01
                if candidate < stop:
                    stop = candidate
                    source = "structure"
    return stage1.base._round_stop(stop, side), source


def _simulate_exit(
    position: dict[str, Any],
    variant: Variant,
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    funding: Mapping[str, float | None],
    costs: Costs,
    final_index: int,
    market: Mapping[str, np.ndarray],
    session_ends: np.ndarray,
) -> dict[str, Any] | None:
    side = int(position["side"])
    stop = float(position["initial_stop"])
    stop_source = "initial_stop"
    entry_index = int(position["entry_index"])
    entry_session = int(position["entry_session"])
    previous_session = entry_session
    bar_index = entry_index
    while bar_index <= final_index:
        session = int(market["session"][bar_index])
        if session != previous_session:
            stop, stop_source = _trail_stop(
                variant, side, stop, entry_session, session, daily
            )
            previous_session = session
        right = min(int(session_ends[session]), final_index)
        if side == 1:
            positions = np.flatnonzero(market["low"][bar_index : right + 1] <= stop)
        else:
            positions = np.flatnonzero(market["high"][bar_index : right + 1] >= stop)
        if len(positions):
            exit_index = bar_index + int(positions[0])
            gap = side * (float(market["open"][exit_index]) - stop) <= 0
            raw_exit = float(market["open"][exit_index]) if gap else stop
            reason = ("gap_" if gap else "") + stop_source
            closed = _close_trade(
                {**position, "last_stop": stop},
                exit_index,
                raw_exit,
                reason,
                bars,
                daily,
                funding,
                costs,
            )
            return closed
        bar_index = right + 1
    return None


def run_variant(
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    funding: Mapping[str, float | None],
    variant: Variant,
    setups: Sequence[Mapping[str, Any]],
    *,
    costs: Costs = BASE_COSTS,
    simulation_start: str | None = None,
    simulation_end: str | None = None,
    close_data_end: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    candidates = stage1._candidate_confirmations(bars, daily, variant, setups)
    consumed = [False] * len(candidates)
    signals: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    incomplete = 0
    dates = bars["date"].astype(str).to_numpy()
    sessions = bars["session"].to_numpy(dtype=int)
    starts = np.flatnonzero(np.r_[True, sessions[1:] != sessions[:-1]])
    ends = np.r_[starts[1:] - 1, len(bars) - 1]
    market = {
        "session": sessions,
        "open": bars["open"].to_numpy(dtype=float),
        "high": bars["high"].to_numpy(dtype=float),
        "low": bars["low"].to_numpy(dtype=float),
    }
    if simulation_start is None:
        first_index = 0
    else:
        values = np.flatnonzero(dates >= simulation_start)
        if not len(values):
            return [], [], 0
        first_index = int(values[0])
    if simulation_end is None:
        final_index = len(bars) - 1
    else:
        values = np.flatnonzero(dates <= simulation_end)
        if not len(values):
            return [], [], 0
        final_index = int(values[-1])
    cursor = first_index

    while cursor < final_index:
        choices: list[tuple[int, int]] = []
        for candidate_index, candidate in enumerate(candidates):
            if consumed[candidate_index]:
                continue
            indices = candidate["indices"]
            offset = int(np.searchsorted(indices, cursor, side="left"))
            if offset < len(indices):
                index = int(indices[offset])
                if index <= final_index:
                    choices.append((index, candidate_index))
        if not choices:
            break
        confirmation_index = min(item[0] for item in choices)
        matches = [item[1] for item in choices if item[0] == confirmation_index]
        confirmation_bar = bars.iloc[confirmation_index]
        if len(matches) > 1:
            signals.append(
                {
                    "variant": variant.key,
                    "side": "conflict",
                    "confirmation_date": str(confirmation_bar["date"]),
                    "confirmation_ts": str(confirmation_bar["ts"]),
                    "status": "skipped_simultaneous_opposites",
                }
            )
            cursor = confirmation_index + 1
            continue

        candidate_index = matches[0]
        consumed[candidate_index] = True
        setup = candidates[candidate_index]["setup"]
        side = int(setup["side"])
        entry_index = confirmation_index + 1
        if entry_index > final_index:
            break
        raw_entry = float(bars.iloc[entry_index]["open"])
        entry = _execution_price(raw_entry, side, True, costs)
        raw_initial_stop = (
            float(setup["formation_low"]) - variant.stop_buffer * float(setup["setup_atr"])
            if side == 1
            else float(setup["formation_high"]) + variant.stop_buffer * float(setup["setup_atr"])
        )
        initial_stop = stage1.base._round_stop(raw_initial_stop, side)
        projected_stop_execution = _execution_price(initial_stop, side, False, costs)
        risk = side * (entry - projected_stop_execution) + 2.0 * costs.commission_per_side
        signal = {
            "variant": variant.key,
            "side": "long" if side == 1 else "short",
            "setup_date": setup["setup_date"],
            "formation_start_date": setup["formation_start_date"],
            "confirmation_date": str(confirmation_bar["date"]),
            "confirmation_ts": str(confirmation_bar["ts"]),
            "status": "scheduled",
        }
        signals.append(signal)
        if risk <= 0 or risk > 3.0 * float(setup["setup_atr"]):
            signals[-1]["status"] = "rejected_risk"
            cursor = entry_index
            continue
        position = {
            **setup,
            "variant": variant.key,
            "side": side,
            "entry_index": entry_index,
            "entry_session": int(sessions[entry_index]),
            "entry_date": str(bars.iloc[entry_index]["date"]),
            "entry_ts": str(bars.iloc[entry_index]["ts"]),
            "entry_raw": raw_entry,
            "entry": entry,
            "initial_stop": initial_stop,
            "risk": risk,
            "confirmation_date": str(confirmation_bar["date"]),
            "confirmation_ts": str(confirmation_bar["ts"]),
        }
        closed = _simulate_exit(
            position,
            variant,
            bars,
            daily,
            funding,
            costs,
            final_index,
            market,
            ends,
        )
        if closed is None:
            incomplete += 1
            signals[-1]["status"] = "open_at_period_end"
            if close_data_end and simulation_end is None:
                closed = _close_trade(
                    position,
                    final_index,
                    float(bars.iloc[final_index]["close"]),
                    "data_end",
                    bars,
                    daily,
                    funding,
                    costs,
                )
                trades.append(closed)
            break
        signals[-1]["status"] = "entered"
        trades.append(closed)
        cursor = int(closed["exit_index"])
    return trades, signals, incomplete


def metrics(
    trades: Iterable[Mapping[str, Any]], start: str | None = None, end: str | None = None
) -> dict[str, Any]:
    return stage1.metrics(trades, start, end)


def _training_rank(row: Mapping[str, Any]) -> tuple[float, float, float, str]:
    score = float(row["net_r"]) / max(3.0, float(row["max_drawdown_r"]))
    return score, float(row["without_best_r"]), -float(row["max_drawdown_r"]), str(row["variant"])


def select_on_training(
    all_variants: Sequence[Variant],
    trade_map: Mapping[str, Sequence[Mapping[str, Any]]],
    train_end: str,
) -> tuple[Variant, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    for variant in all_variants:
        value = metrics(trade_map[variant.key], None, train_end)
        row = {"variant": variant.key, **asdict(variant), **value}
        rows.append(row)
        by_key[variant.key] = row
    for row, variant in zip(rows, all_variants):
        neighbors = [
            item
            for item in all_variants
            if sum(
                left != right
                for left, right in (
                    (item.pattern, variant.pattern),
                    (item.formation, variant.formation),
                    (item.confirmation, variant.confirmation),
                    (item.life, variant.life),
                    (item.stop_buffer, variant.stop_buffer),
                    (item.exit_mode, variant.exit_mode),
                    (item.trail_atr, variant.trail_atr),
                    (item.structure_lookback, variant.structure_lookback),
                )
            )
            == 1
        ]
        values = [float(by_key[item.key]["net_r"]) for item in neighbors]
        positive_fraction = float(np.mean(np.asarray(values) > 0)) if values else 0.0
        row["neighbor_count"] = len(values)
        row["positive_neighbor_fraction"] = positive_fraction
        row["neighbor_median_net_r"] = float(np.median(values)) if values else None
        row["eligible"] = bool(
            int(row["trades"]) >= 20
            and int(row["long_trades"]) >= 5
            and int(row["short_trades"]) >= 5
            and float(row["without_best_r"]) > 0
            and row["top3_positive_profit_share"] is not None
            and float(row["top3_positive_profit_share"]) <= 0.7
            and positive_fraction >= 0.5
        )
        row["training_score"] = _training_rank(row)[0]
    table = pd.DataFrame(rows)
    eligible = table.loc[table["eligible"]].copy()
    ranked = eligible if not eligible.empty else table
    ranked = ranked.sort_values(
        ["training_score", "without_best_r", "max_drawdown_r", "variant"],
        ascending=[False, False, True, True],
    )
    winner_key = str(ranked.iloc[0]["variant"])
    return next(item for item in all_variants if item.key == winner_key), table


def _worker_init(input_paths: Sequence[str], history_paths: Sequence[str]) -> None:
    global _WORKER_MARKET, _WORKER_FUNDING, _WORKER_CATALOGS
    _WORKER_MARKET = stage1.build_market([Path(value) for value in input_paths])
    _WORKER_FUNDING = stage1.base.load_funding([Path(value) for value in history_paths])
    _WORKER_CATALOGS = {
        (pattern, formation): stage1.setup_catalog(_WORKER_MARKET[1], pattern, formation)
        for pattern in stage1.PATTERNS
        for formation in (3, 4, 5)
    }


def _worker_run(variant: Variant) -> tuple[str, list[dict[str, Any]]]:
    if _WORKER_MARKET is None or _WORKER_FUNDING is None or _WORKER_CATALOGS is None:
        raise RuntimeError("worker not initialized")
    bars, daily = _WORKER_MARKET
    trades, _signals, _incomplete = run_variant(
        bars,
        daily,
        _WORKER_FUNDING,
        variant,
        _WORKER_CATALOGS[(variant.pattern, variant.formation)],
    )
    return variant.key, trades


def _json_value(value: Any) -> Any:
    return stage1._json_value(value)


def write_json(path: Path, payload: Any) -> None:
    stage1.write_json(path, payload)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tradestats", action="append", required=True)
    parser.add_argument("--history", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1))
    args = parser.parse_args(argv)
    input_paths = [Path(value).resolve(strict=True) for value in args.tradestats]
    history_paths = [Path(value).resolve(strict=True) for value in args.history]
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise FileExistsError("output directory must be empty: " + str(output))

    bars, daily = stage1.build_market(input_paths)
    funding = stage1.base.load_funding(history_paths)
    catalogs = {
        (pattern, formation): stage1.setup_catalog(daily, pattern, formation)
        for pattern in stage1.PATTERNS
        for formation in (3, 4, 5)
    }
    all_variants = variants()
    trade_map: dict[str, list[dict[str, Any]]] = {}
    if args.workers <= 1:
        for index, variant in enumerate(all_variants, start=1):
            trades, _signals, _incomplete = run_variant(
                bars, daily, funding, variant, catalogs[(variant.pattern, variant.formation)]
            )
            trade_map[variant.key] = trades
            if index % 40 == 0:
                print(f"completed {index}/{len(all_variants)} variants", file=sys.stderr, flush=True)
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers,
            initializer=_worker_init,
            initargs=(tuple(str(path) for path in input_paths), tuple(str(path) for path in history_paths)),
        ) as executor:
            for index, (key, trades) in enumerate(executor.map(_worker_run, all_variants), start=1):
                trade_map[key] = trades
                if index % 40 == 0:
                    print(f"completed {index}/{len(all_variants)} variants", file=sys.stderr, flush=True)

    wf_trades: list[dict[str, Any]] = []
    fold_rows: list[dict[str, Any]] = []
    selection_tables: list[pd.DataFrame] = []
    for fold in FOLDS:
        winner, table = select_on_training(all_variants, trade_map, fold.train_end)
        table.insert(0, "selection_fold", fold.name)
        selection_tables.append(table)
        validation, signals, incomplete = run_variant(
            bars,
            daily,
            funding,
            winner,
            catalogs[(winner.pattern, winner.formation)],
            simulation_start=fold.validate_start,
            simulation_end=fold.validate_end,
            close_data_end=False,
        )
        value = metrics(validation)
        fold_rows.append(
            {
                "fold": fold.name,
                "train_end": fold.train_end,
                "validate_start": fold.validate_start,
                "validate_end": fold.validate_end,
                "selected_variant": winner.key,
                "incomplete_positions_excluded": incomplete,
                **value,
            }
        )
        for trade in validation:
            trade["walk_forward_fold"] = fold.name
        wf_trades.extend(validation)
        pd.DataFrame(signals).to_csv(output / f"signals_{fold.name}.csv", index=False)

    pd.concat(selection_tables, ignore_index=True).to_csv(
        output / "training_selection_by_fold.csv", index=False
    )
    pd.DataFrame(fold_rows).to_csv(output / "walk_forward_folds.csv", index=False)
    pd.DataFrame(wf_trades).to_csv(output / "walk_forward_trades.csv", index=False)

    final_candidate, final_table = select_on_training(all_variants, trade_map, "2026-09-30")
    final_table.to_csv(output / "final_training_selection.csv", index=False)
    holdout_trades, holdout_signals, holdout_incomplete = run_variant(
        bars,
        daily,
        funding,
        final_candidate,
        catalogs[(final_candidate.pattern, final_candidate.formation)],
        simulation_start=HISTORIC_HOLDOUT[0],
        simulation_end=HISTORIC_HOLDOUT[1],
        close_data_end=False,
    )
    pd.DataFrame(holdout_trades).to_csv(output / "historic_holdout_trades.csv", index=False)
    pd.DataFrame(holdout_signals).to_csv(output / "historic_holdout_signals.csv", index=False)

    stress_wf: list[dict[str, Any]] = []
    for fold_row, fold in zip(fold_rows, FOLDS):
        winner_key = str(fold_row["selected_variant"])
        winner = next(item for item in all_variants if item.key == winner_key)
        validation, _signals, _incomplete = run_variant(
            bars,
            daily,
            funding,
            winner,
            catalogs[(winner.pattern, winner.formation)],
            costs=STRESS_COSTS,
            simulation_start=fold.validate_start,
            simulation_end=fold.validate_end,
            close_data_end=False,
        )
        stress_wf.extend(validation)

    future_gate = {
        "schema_version": "usdrubf_multisession_future_holdout_gate.v1",
        "status": "COLLECTING_NOT_ELIGIBLE",
        "frozen_candidate": asdict(final_candidate),
        "frozen_candidate_key": final_candidate.key,
        "start": FUTURE_HOLDOUT_START,
        "observed_closed_trades": 0,
        "minimum_closed_trades": 20,
        "minimum_each_direction": 5,
        "requires_positive_without_best_trade": True,
        "may_change_ema_signals": False,
        "may_change_nightly_reversal_monitor": False,
        "promotion_allowed": False,
    }
    write_json(output / "future_holdout_gate.json", future_gate)
    summary = {
        "schema_version": "usdrubf_multisession_rework_study.v1",
        "status": "REWORK_REQUIRED_RESEARCH_ONLY",
        "data_range": [str(daily.iloc[0]["date"]), str(daily.iloc[-1]["date"])],
        "bars": len(bars),
        "sessions": len(daily),
        "variants": len(all_variants),
        "fixed_time_exit_used": False,
        "costs": asdict(BASE_COSTS),
        "stress_costs": asdict(STRESS_COSTS),
        "walk_forward": metrics(wf_trades),
        "walk_forward_stress": metrics(stress_wf),
        "walk_forward_folds": fold_rows,
        "final_candidate": asdict(final_candidate),
        "final_candidate_key": final_candidate.key,
        "historic_holdout": metrics(holdout_trades),
        "historic_holdout_incomplete_positions_excluded": holdout_incomplete,
        "future_holdout_gate": future_gate,
        "operational_changes": [],
    }
    write_json(output / "summary.json", summary)
    write_json(
        output / "data_audit.json",
        {
            "inputs": {
                str(path): stage1.base.sha256(path)
                for path in [*input_paths, *history_paths]
            },
            "first_bar": str(bars.iloc[0]["ts"]),
            "last_bar": str(bars.iloc[-1]["ts"]),
            "bars": len(bars),
            "sessions": len(daily),
            "true_walk_forward_selection_uses_only_prior_dates": True,
            "historic_holdout_used_in_selection": False,
            "future_holdout_start": FUTURE_HOLDOUT_START,
        },
    )
    print(json.dumps(_json_value(summary), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
