"""Compare exit families with fold-local selection from an existing rework run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

import pandas as pd

from research.usdrubf_multisession_sequence_20261110 import rework, study


def _variant(row: pd.Series) -> rework.Variant:
    return rework.Variant(
        pattern=str(row["pattern"]),
        formation=int(row["formation"]),
        confirmation=str(row["confirmation"]),
        life=int(row["life"]),
        stop_buffer=float(row["stop_buffer"]),
        exit_mode=str(row["exit_mode"]),
        trail_atr=float(row["trail_atr"]),
        structure_lookback=int(row["structure_lookback"]),
    )


def _winner(table: pd.DataFrame, family: str) -> rework.Variant:
    family_rows = table.loc[table["exit_mode"].eq(family)].copy()
    eligible = family_rows.loc[family_rows["eligible"].astype(str).str.lower().eq("true")]
    ranked = eligible if not eligible.empty else family_rows
    ranked = ranked.sort_values(
        ["training_score", "without_best_r", "max_drawdown_r", "variant"],
        ascending=[False, False, True, True],
    )
    return _variant(ranked.iloc[0])


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tradestats", action="append", required=True)
    parser.add_argument("--history", action="append", required=True)
    parser.add_argument("--selection-table", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args(argv)
    bars, daily = study.build_market([Path(value) for value in args.tradestats])
    funding = study.base.load_funding([Path(value) for value in args.history])
    catalogs = {
        (pattern, formation): study.setup_catalog(daily, pattern, formation)
        for pattern in study.PATTERNS
        for formation in (3, 4, 5)
    }
    selections = pd.read_csv(args.selection_table)
    rows: list[dict[str, object]] = []
    summary: dict[str, object] = {}
    for family in ("atr_trailing", "structure", "hybrid"):
        combined: list[dict[str, object]] = []
        fold_payload: list[dict[str, object]] = []
        for fold in rework.FOLDS:
            table = selections.loc[selections["selection_fold"].eq(fold.name)]
            winner = _winner(table, family)
            trades, _signals, incomplete = rework.run_variant(
                bars,
                daily,
                funding,
                winner,
                catalogs[(winner.pattern, winner.formation)],
                simulation_start=fold.validate_start,
                simulation_end=fold.validate_end,
                close_data_end=False,
            )
            value = rework.metrics(trades)
            row = {
                "exit_family": family,
                "fold": fold.name,
                "selected_variant": winner.key,
                "incomplete_positions_excluded": incomplete,
                **value,
            }
            rows.append(row)
            fold_payload.append(row)
            combined.extend(trades)
        combined_value = rework.metrics(combined)
        rows.append(
            {
                "exit_family": family,
                "fold": "combined",
                "selected_variant": "fold_specific",
                "incomplete_positions_excluded": sum(
                    int(item["incomplete_positions_excluded"]) for item in fold_payload
                ),
                **combined_value,
            }
        )
        summary[family] = {"folds": fold_payload, "combined": combined_value}
    pd.DataFrame(rows).to_csv(args.output_csv, index=False)
    Path(args.output_json).write_text(
        json.dumps(rework._json_value(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(rework._json_value(summary), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
