from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Mapping, Sequence

import numpy as np
import pandas as pd

from moex_backtest.engine.canonical import (
    CanonicalBacktestEngine,
    CanonicalBacktestInput,
    CostConfig,
    ExecutionConfig,
)

EXPERIMENT_ID: Final = "usdrubf_ema_3_19_d1_economic_cost_capacity_v1"
PRODUCER: Final = "src.moex_research.runners.usdrubf_ema_3_19_d1_economic_cost_capacity"
M2_EXPERIMENT_ID: Final = "usdrubf_ema_3_19_d1_research_baseline_v1"
M4B_EXPERIMENT_ID: Final = "usdrubf_ema_3_19_d1_rule_gate_benchmark_v1"
M4C_EXPERIMENT_ID: Final = "usdrubf_ema_3_19_d1_technical_ml_benchmark_v1"
INSTRUMENT_ID: Final = "usdrubf"
CANONICAL_ENGINE_ID: Final = "canonical_backtest_engine_minimal_v1"

OUTPUT_RUN_METADATA: Final = "run_metadata.json"
OUTPUT_SIGNALS: Final = "m5a_signal_table.csv"
OUTPUT_FILLS: Final = "m5a_fill_table.csv"
OUTPUT_COSTS: Final = "m5a_cost_table.csv"
OUTPUT_POSITION_PATH: Final = "m5a_position_path.csv"
OUTPUT_ECONOMIC_METRICS: Final = "m5a_economic_metrics.json"
OUTPUT_COST_CAPACITY: Final = "m5a_cost_capacity.json"
OUTPUT_QUALITY: Final = "m5a_quality_report.json"
OUTPUT_DECISION: Final = "m5a_decision.json"
DECLARED_OUTPUT_FILES: Final = (
    OUTPUT_RUN_METADATA,
    OUTPUT_SIGNALS,
    OUTPUT_FILLS,
    OUTPUT_COSTS,
    OUTPUT_POSITION_PATH,
    OUTPUT_ECONOMIC_METRICS,
    OUTPUT_COST_CAPACITY,
    OUTPUT_QUALITY,
    OUTPUT_DECISION,
)

_REQUIRED_D1_COLUMNS: Final = ("instrument_id", "end", "open", "high", "low", "close")
_REQUIRED_CONTEXT_COLUMNS: Final = ("instrument_id", "end", "cross_dir")
_VALID_DIRECTIONS: Final = {"cross_up": 1, "cross_down": -1}
_ALIAS_TOKENS: Final = ("latest", "current", "autodetect")
_GLOB_CHARACTERS: Final = frozenset("*?[]")
_SHA_RE: Final = re.compile(r"^[0-9a-fA-F]{40}$")
_TOKEN_SPLIT_RE: Final = re.compile(r"[^A-Za-z0-9]+")
_FORBIDDEN_CONTEXT_PREFIXES: Final = (
    "signed_ret_",
    "signed_return_",
    "allow_trade",
    "max_adverse_",
    "max_favorable_",
    "entry_session_",
    "exit_session_",
    "completion_",
    "opposite_cross_",
    "h1_",
    "h2_",
    "h3_",
    "h5_",
    "h10_",
    "reverse_",
)
_FORBIDDEN_CONTEXT_MARKERS: Final = (
    "future_outcome",
    "target_value",
    "label_censored",
    "entry_open",
    "exit_open",
    "outcome_open",
)

_FILL_COLUMNS: Final = (
    "engine_id",
    "signal_timestamp",
    "execution_timestamp",
    "raw_price",
    "fill_price",
    "mark_price",
    "quantity",
    "side",
    "position_before",
    "position_after",
    "transition_type",
    "reason",
)
_COST_COLUMNS: Final = (
    "engine_id",
    "execution_timestamp",
    "quantity",
    "raw_price",
    "fill_price",
    "commission",
    "slippage_cost",
    "total_cost",
    "reason",
)
_POSITION_COLUMNS: Final = (
    "engine_id",
    "execution_timestamp",
    "position",
    "cash",
    "mark_price",
    "equity",
    "reason",
    "mark_scope",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Measure gross execution economics and break-even all-in cost capacity for the "
            "unfiltered USDRUBF D1 EMA(3/19) crossover baseline."
        )
    )
    parser.add_argument("--d1-ohlc-path", required=True)
    parser.add_argument("--cross-context-path", required=True)
    parser.add_argument("--m2-quality-report-path", required=True)
    parser.add_argument("--m4b-decision-path", required=True)
    parser.add_argument("--m4c-decision-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--git-commit-sha", required=True)
    return parser


def _alias_tokens(path_value: str) -> list[str]:
    components = [
        part.lower()
        for part in re.split(r"[\\/]+", path_value)
        if part and part not in {".", ".."}
    ]
    stem_tokens = set(_TOKEN_SPLIT_RE.split(Path(components[-1]).stem)) if components else set()
    tokens = set(components) | {token.lower() for token in stem_tokens if token}
    return [token for token in _ALIAS_TOKENS if token in tokens]


def _validate_explicit_input(
    raw_value: str,
    argument_name: str,
    allowed_suffixes: set[str],
    parser: argparse.ArgumentParser,
) -> Path:
    raw_path = raw_value.strip()
    if not raw_path:
        parser.error(f"{argument_name} must be non-empty")
    if any(character in raw_path for character in _GLOB_CHARACTERS):
        parser.error(f"{argument_name} must reference one explicit file and must not contain glob syntax")
    candidates = [raw_path]
    try:
        resolved = str(Path(raw_path).expanduser().resolve(strict=False))
    except (OSError, RuntimeError):
        resolved = str(Path(raw_path).expanduser().absolute())
    if resolved != raw_path:
        candidates.append(resolved)
    aliases = sorted({token for candidate in candidates for token in _alias_tokens(candidate)})
    if aliases:
        parser.error(f"{argument_name} must not use mutable alias token(s): " + ", ".join(aliases))
    path = Path(raw_path)
    if not path.exists():
        parser.error(f"{argument_name} must reference an existing file")
    if not path.is_file():
        parser.error(f"{argument_name} must reference a file")
    if path.suffix.lower() not in allowed_suffixes:
        parser.error(f"{argument_name} must end with " + " or ".join(sorted(allowed_suffixes)))
    return path


def _validate_cli_args(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> tuple[Path, Path, Path, Path, Path, Path, str, str]:
    d1_path = _validate_explicit_input(
        str(args.d1_ohlc_path), "--d1-ohlc-path", {".csv", ".parquet"}, parser
    )
    context_path = _validate_explicit_input(
        str(args.cross_context_path),
        "--cross-context-path",
        {".csv", ".parquet"},
        parser,
    )
    m2_quality_path = _validate_explicit_input(
        str(args.m2_quality_report_path), "--m2-quality-report-path", {".json"}, parser
    )
    m4b_path = _validate_explicit_input(
        str(args.m4b_decision_path), "--m4b-decision-path", {".json"}, parser
    )
    m4c_path = _validate_explicit_input(
        str(args.m4c_decision_path), "--m4c-decision-path", {".json"}, parser
    )
    if len(
        {
            d1_path.resolve(),
            context_path.resolve(),
            m2_quality_path.resolve(),
            m4b_path.resolve(),
            m4c_path.resolve(),
        }
    ) != 5:
        parser.error("the five input artifact paths must be distinct")
    output_raw = str(args.output_dir).strip()
    if not output_raw:
        parser.error("--output-dir must be non-empty")
    output_dir = Path(output_raw)
    if output_dir.exists() and not output_dir.is_dir():
        parser.error("--output-dir must reference a directory")
    if output_dir.exists() and any(output_dir.iterdir()):
        parser.error("--output-dir must be absent or empty to prevent stale evidence")
    run_id = str(args.run_id).strip()
    if not run_id:
        parser.error("--run-id must be non-empty")
    git_sha = str(args.git_commit_sha).strip().lower()
    if not _SHA_RE.fullmatch(git_sha):
        parser.error("--git-commit-sha must be an explicit 40-character hexadecimal commit SHA")
    return d1_path, context_path, m2_quality_path, m4b_path, m4c_path, output_dir, run_id, git_sha


def _read_table(path: Path, artifact_name: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path) if path.suffix.lower() == ".csv" else pd.read_parquet(path)
    except Exception as exc:  # pragma: no cover - parser-specific details vary
        raise ValueError(f"failed to read {artifact_name}: {exc}") from exc


def _read_json(path: Path, artifact_name: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"failed to read {artifact_name}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{artifact_name} must contain a JSON object")
    return payload


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_safe(value: Any) -> Any:
    if value is pd.NA:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    frame.to_csv(path, index=False, lineterminator="\n", float_format="%.12g")


def _output_paths(output_dir: Path) -> dict[str, Path]:
    root = output_dir.resolve()
    paths = {filename: root / filename for filename in DECLARED_OUTPUT_FILES}
    if any(path.resolve().parent != root for path in paths.values()):
        raise ValueError("declared output path escaped output directory")
    return paths


def _normalize_end(values: pd.Series, artifact_name: str) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    if timestamps.isna().any():
        raise ValueError(f"{artifact_name} contains invalid end timestamps")
    if getattr(timestamps.dt, "tz", None) is not None:
        timestamps = timestamps.dt.tz_convert("Europe/Moscow").dt.tz_localize(None)
    return timestamps


def _numeric_finite(series: pd.Series, column: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().any() or not np.isfinite(numeric).all():
        raise ValueError(f"column {column} must contain finite numeric values")
    return numeric.astype("float64")


def _normalize_d1(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in _REQUIRED_D1_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("D1 OHLC artifact is missing required columns: " + ", ".join(missing))
    work = frame[list(_REQUIRED_D1_COLUMNS)].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != INSTRUMENT_ID).any():
        raise ValueError("all D1 OHLC instrument_id values must equal 'usdrubf'")
    work["end"] = _normalize_end(work["end"], "D1 OHLC artifact")
    if work["end"].duplicated().any() or not work["end"].is_monotonic_increasing:
        raise ValueError("D1 OHLC end timestamps must be unique and strictly increasing")
    for column in ("open", "high", "low", "close"):
        work[column] = _numeric_finite(work[column], column)
        if (work[column] <= 0.0).any():
            raise ValueError(f"D1 OHLC column {column} must contain positive prices")
    if (work["high"] < work[["open", "close", "low"]].max(axis=1)).any():
        raise ValueError("D1 OHLC high is below another OHLC value")
    if (work["low"] > work[["open", "close", "high"]].min(axis=1)).any():
        raise ValueError("D1 OHLC low is above another OHLC value")
    if work.empty:
        raise ValueError("D1 OHLC artifact has zero rows")
    return work.reset_index(drop=True)


def _forbidden_context_columns(frame: pd.DataFrame) -> list[str]:
    forbidden: list[str] = []
    for column in frame.columns:
        normalized = str(column).strip().lower()
        if normalized.startswith(_FORBIDDEN_CONTEXT_PREFIXES) or any(
            marker in normalized for marker in _FORBIDDEN_CONTEXT_MARKERS
        ):
            forbidden.append(str(column))
    return sorted(forbidden)


def _normalize_context(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in _REQUIRED_CONTEXT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("cross context artifact is missing required columns: " + ", ".join(missing))
    forbidden = _forbidden_context_columns(frame)
    if forbidden:
        raise ValueError(
            "cross context contains future or label fields: " + ", ".join(forbidden)
        )
    work = frame[list(_REQUIRED_CONTEXT_COLUMNS)].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != INSTRUMENT_ID).any():
        raise ValueError("all cross context instrument_id values must equal 'usdrubf'")
    work["end"] = _normalize_end(work["end"], "cross context artifact")
    if work["end"].duplicated().any() or not work["end"].is_monotonic_increasing:
        raise ValueError("cross context end timestamps must be unique and strictly increasing")
    work["cross_dir"] = work["cross_dir"].astype(str)
    invalid = sorted(set(work["cross_dir"]) - set(_VALID_DIRECTIONS))
    if invalid:
        raise ValueError("unsupported cross_dir values: " + ", ".join(invalid))
    if len(work) > 1:
        repeated = work["cross_dir"].eq(work["cross_dir"].shift(1)).fillna(False)
        if repeated.any():
            raise ValueError("consecutive EMA crossover directions must alternate")
    if work.empty:
        raise ValueError("cross context artifact has zero rows")
    return work.reset_index(drop=True)


def validate_source_artifacts(
    d1_ohlc: pd.DataFrame,
    cross_context: pd.DataFrame,
    m2_quality_report: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    d1 = _normalize_d1(d1_ohlc)
    context = _normalize_context(cross_context)
    if not set(context["end"]).issubset(set(d1["end"])):
        raise ValueError("every crossover timestamp must exist in the D1 OHLC artifact")
    if m2_quality_report.get("experiment_id") != M2_EXPERIMENT_ID:
        raise ValueError(f"M2 quality report experiment_id must equal {M2_EXPERIMENT_ID!r}")
    if m2_quality_report.get("d1_ohlc_row_count") != len(d1):
        raise ValueError("M2 quality report D1 row count does not match D1 OHLC artifact")
    if m2_quality_report.get("event_count") != len(context):
        raise ValueError("M2 quality report event count does not match cross context artifact")
    row_counts = m2_quality_report.get("row_counts")
    if not isinstance(row_counts, Mapping):
        raise ValueError("M2 quality report row_counts must be an object")
    if row_counts.get("d1_ohlc") != len(d1) or row_counts.get("cross_context") != len(context):
        raise ValueError("M2 quality report row_counts do not match source artifacts")
    time_semantics = m2_quality_report.get("time_semantics")
    if not isinstance(time_semantics, Mapping):
        raise ValueError("M2 quality report time_semantics must be an object")
    if time_semantics.get("feature_context_uses_d_plus_1_values") is not False:
        raise ValueError("M2 feature context must state that D+1 values are not used")
    leakage = m2_quality_report.get("leakage_checks")
    if not isinstance(leakage, Mapping):
        raise ValueError("M2 quality report leakage_checks must be an object")
    if leakage.get("no_d_plus_1_values_in_feature_context_rows") is not True:
        raise ValueError("M2 quality report must confirm no D+1 feature-context values")
    if leakage.get("labels_kept_research_only") is not True:
        raise ValueError("M2 quality report must confirm labels are research-only")
    for key in (
        "feature_context_label_like_columns",
        "feature_context_future_outcome_columns",
    ):
        payload = leakage.get(key)
        if not isinstance(payload, Mapping) or any(payload.values()):
            raise ValueError(f"M2 quality report {key} must contain only empty lists")
    return d1, context


def validate_prior_decisions(
    m4b_decision: Mapping[str, Any],
    m4c_decision: Mapping[str, Any],
) -> None:
    if m4b_decision.get("experiment_id") != M4B_EXPERIMENT_ID:
        raise ValueError(f"M4B decision experiment_id must equal {M4B_EXPERIMENT_ID!r}")
    if m4b_decision.get("result_status") != "rule_gate_not_supported":
        raise ValueError("M5A requires M4B result_status=rule_gate_not_supported")
    if m4b_decision.get("selected_rule") is not None:
        raise ValueError("M5A requires M4B selected_rule to be null")
    for field in (
        "model_training_performed",
        "threshold_sweep_performed",
        "post_hoc_rule_search_performed",
        "runtime_or_trading_action_performed",
        "strategy_promotion_allowed",
    ):
        if m4b_decision.get(field) is not False:
            raise ValueError(f"M4B decision must state {field}=false")

    if m4c_decision.get("experiment_id") != M4C_EXPERIMENT_ID:
        raise ValueError(f"M4C decision experiment_id must equal {M4C_EXPERIMENT_ID!r}")
    if m4c_decision.get("result_status") != "technical_ml_not_supported":
        raise ValueError("M5A requires M4C result_status=technical_ml_not_supported")
    if m4c_decision.get("selected_feature_group") is not None:
        raise ValueError("M5A requires M4C selected_feature_group to be null")
    for field in (
        "persistent_model_artifact_emitted",
        "hyperparameter_search_performed",
        "threshold_tuning_performed",
        "post_hoc_feature_selection_performed",
        "model_promotion_allowed",
        "strategy_promotion_allowed",
        "runtime_or_trading_action_performed",
    ):
        if m4c_decision.get(field) is not False:
            raise ValueError(f"M4C decision must state {field}=false")


def prepare_backtest_inputs(
    d1_ohlc: pd.DataFrame,
    cross_context: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    d1 = _normalize_d1(d1_ohlc)
    context = _normalize_context(cross_context)
    if not set(context["end"]).issubset(set(d1["end"])):
        raise ValueError("every crossover timestamp must exist in the D1 OHLC artifact")
    bars = pd.DataFrame(
        {
            "timestamp": d1["end"],
            "open": d1["open"],
            "close": d1["close"],
            "valid": True,
        }
    )
    signals = pd.DataFrame(
        {
            "signal_timestamp": context["end"],
            "timestamp": context["end"],
            "cross_dir": context["cross_dir"],
            "target_position": context["cross_dir"].map(_VALID_DIRECTIONS).astype("int64"),
            "source_context_index": np.arange(len(context), dtype=int),
        }
    )
    return bars.reset_index(drop=True), signals.reset_index(drop=True)


def _frame_from_rows(rows: Sequence[Mapping[str, object]], columns: Sequence[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=list(columns))
    frame = pd.DataFrame([dict(row) for row in rows])
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[list(columns)]


def run_canonical_gross_backtest(
    bars: pd.DataFrame,
    signals: pd.DataFrame,
) -> tuple[Any, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    engine = CanonicalBacktestEngine()
    result = engine.run(
        CanonicalBacktestInput(
            bars=bars.to_dict("records"),
            signals=signals[["timestamp", "target_position"]].to_dict("records"),
            cost_config=CostConfig(commission_bps=0.0, slippage_bps=0.0),
            execution_config=ExecutionConfig(
                fill_model_id="next_bar_open",
                terminal_close=True,
                initial_cash=0.0,
            ),
        )
    )
    if result.metrics.get("engine_id") != CANONICAL_ENGINE_ID:
        raise ValueError("canonical backtest engine_id mismatch")
    fills = _frame_from_rows(result.fills, _FILL_COLUMNS)
    costs = _frame_from_rows(result.costs, _COST_COLUMNS)
    position_path = _frame_from_rows(result.position_path, _POSITION_COLUMNS[:-1])
    position_path["mark_scope"] = "fill_event_only"
    position_path = position_path[list(_POSITION_COLUMNS)]
    if not costs.empty:
        for column in ("commission", "slippage_cost", "total_cost"):
            values = pd.to_numeric(costs[column], errors="coerce")
            if values.isna().any() or not values.eq(0.0).all():
                raise ValueError("M5A gross engine run must contain zero commission and slippage")
    return result, fills, costs, position_path


def build_economic_measurements(
    result: Any,
    fills: pd.DataFrame,
    costs: pd.DataFrame,
    position_path: pd.DataFrame,
) -> tuple[dict[str, Any], dict[str, Any]]:
    del costs
    if fills.empty:
        turnover_units = 0.0
        turnover_notional = 0.0
    else:
        quantity = pd.to_numeric(fills["quantity"], errors="coerce")
        raw_price = pd.to_numeric(fills["raw_price"], errors="coerce")
        if quantity.isna().any() or raw_price.isna().any():
            raise ValueError("canonical fill table contains invalid quantity or raw_price values")
        turnover_units = float(quantity.abs().sum())
        turnover_notional = float((quantity.abs() * raw_price).sum())
    gross_pnl = float(result.metrics["total_pnl"])
    break_even_bps = None if turnover_notional <= 0.0 else gross_pnl / turnover_notional * 10_000.0
    pnl_per_bp = None if turnover_notional <= 0.0 else -turnover_notional / 10_000.0
    reversal_count = (
        0
        if fills.empty
        else int(fills["transition_type"].astype(str).str.contains("reversal").sum())
    )
    signal_fill_count = 0 if fills.empty else int(fills["reason"].eq("signal").sum())
    forced_close_count = (
        0 if fills.empty else int(fills["reason"].eq("forced_terminal_close").sum())
    )
    metrics = {
        "experiment_id": EXPERIMENT_ID,
        "engine_id": result.metrics["engine_id"],
        "pnl_unit": "underlying_price_units_at_contract_multiplier_one",
        "signal_count": int(result.metrics["signal_count"]),
        "signal_fill_count": signal_fill_count,
        "rejected_signal_count": int(result.metrics["rejected_signal_count"]),
        "fill_count": int(len(fills)),
        "reversal_count": reversal_count,
        "forced_terminal_close_count": forced_close_count,
        "gross_turnover_units": turnover_units,
        "gross_turnover_notional": turnover_notional,
        "gross_total_pnl_price_units": gross_pnl,
        "gross_pnl_bps_of_turnover": break_even_bps,
        "ending_position": int(result.metrics["final_position"]),
        "fill_event_max_drawdown_price_units": float(result.metrics["max_drawdown"]),
        "fill_event_position_path_rows": int(len(position_path)),
        "cash_baseline_pnl_price_units": 0.0,
        "gross_pnl_minus_cash_baseline": gross_pnl,
        "commission_bps": 0.0,
        "slippage_bps": 0.0,
        "total_recorded_cost": float(result.metrics["total_cost"]),
    }
    capacity = {
        "experiment_id": EXPERIMENT_ID,
        "cost_capacity_scope": "combined commission plus slippage bps per traded notional",
        "gross_pnl_price_units": gross_pnl,
        "gross_turnover_notional": turnover_notional,
        "break_even_all_in_bps_per_traded_notional": break_even_bps,
        "pnl_change_per_one_all_in_bp": pnl_per_bp,
        "net_pnl_equation": (
            "gross_pnl_price_units - gross_turnover_notional * all_in_bps / 10000"
        ),
        "net_pnl_at_break_even_bps": (
            None
            if break_even_bps is None
            else gross_pnl - turnover_notional * break_even_bps / 10_000.0
        ),
        "actual_market_costs_bound": False,
        "contract_multiplier_bound": False,
        "funding_or_roll_bound": False,
        "actual_net_profitability_claim_allowed": False,
    }
    return metrics, capacity


def build_decision(
    metrics: Mapping[str, Any],
    cost_capacity: Mapping[str, Any],
    *,
    run_id: str,
    git_commit_sha: str,
) -> dict[str, Any]:
    gross_pnl = float(metrics["gross_total_pnl_price_units"])
    gross_positive = gross_pnl > 0.0
    result_status = (
        "economic_cost_binding_required"
        if gross_positive
        else "economic_baseline_not_supported_gross"
    )
    return {
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "git_commit_sha": git_commit_sha,
        "result_status": result_status,
        "gross_supported_vs_cash": gross_positive,
        "gross_total_pnl_price_units": gross_pnl,
        "break_even_all_in_bps_per_traded_notional": cost_capacity.get(
            "break_even_all_in_bps_per_traded_notional"
        ),
        "selected_economic_configuration": None,
        "full_economic_support_available": False,
        "full_economic_support_reason": (
            "actual broker/exchange costs, executable slippage, multiplier, roll, and funding are not bound"
        ),
        "required_next_evidence": [
            "broker commission schedule",
            "exchange and clearing fees",
            "empirical bid-ask or executable slippage",
            "USDRUBF contract multiplier and monetary point value",
            "contract roll and expiry handling",
            "funding or carry semantics when applicable",
        ],
        "custom_strategy_pnl_engine_used": False,
        "machine_learning_used": False,
        "threshold_search_performed": False,
        "strategy_promotion_allowed": False,
        "runtime_or_trading_action_performed": False,
    }


def _input_records(paths: Mapping[str, Path]) -> dict[str, dict[str, str]]:
    return {
        name: {"path": str(path), "sha256": _sha256_file(path)}
        for name, path in paths.items()
    }


def run_research_package(
    *,
    d1_ohlc_path: Path,
    cross_context_path: Path,
    m2_quality_report_path: Path,
    m4b_decision_path: Path,
    m4c_decision_path: Path,
    output_dir: Path,
    run_id: str,
    git_commit_sha: str,
) -> dict[str, Any]:
    if output_dir.exists() and (not output_dir.is_dir() or any(output_dir.iterdir())):
        raise ValueError("output_dir must be absent or empty to prevent stale evidence")
    started_at = _utc_now_iso()
    input_paths = {
        "d1_ohlc": d1_ohlc_path,
        "cross_context": cross_context_path,
        "m2_quality_report": m2_quality_report_path,
        "m4b_decision": m4b_decision_path,
        "m4c_decision": m4c_decision_path,
    }
    inputs = _input_records(input_paths)
    raw_d1 = _read_table(d1_ohlc_path, "M2.1 D1 OHLC")
    raw_context = _read_table(cross_context_path, "M2.1 crossover context")
    m2_quality = _read_json(m2_quality_report_path, "M2.1 quality report")
    m4b_decision = _read_json(m4b_decision_path, "M4B decision")
    m4c_decision = _read_json(m4c_decision_path, "M4C decision")
    d1, context = validate_source_artifacts(raw_d1, raw_context, m2_quality)
    validate_prior_decisions(m4b_decision, m4c_decision)
    bars, signals = prepare_backtest_inputs(d1, context)
    result, fills, costs, position_path = run_canonical_gross_backtest(bars, signals)
    economic_metrics, cost_capacity = build_economic_measurements(
        result, fills, costs, position_path
    )
    decision = build_decision(
        economic_metrics,
        cost_capacity,
        run_id=run_id,
        git_commit_sha=git_commit_sha,
    )
    rejected_signals = list(result.artifacts.get("rejected_signals", ()))
    metadata = {
        "experiment_id": EXPERIMENT_ID,
        "producer": PRODUCER,
        "run_id": run_id,
        "git_commit_sha": git_commit_sha,
        "started_at": started_at,
        "completed_at": _utc_now_iso(),
        "instrument_id": INSTRUMENT_ID,
        "inputs": inputs,
        "lineage": {
            "m2_experiment_id": M2_EXPERIMENT_ID,
            "m4b_experiment_id": M4B_EXPERIMENT_ID,
            "m4b_result_status": m4b_decision["result_status"],
            "m4c_experiment_id": M4C_EXPERIMENT_ID,
            "m4c_result_status": m4c_decision["result_status"],
        },
        "strategy_signal_semantics": {
            "signal_timestamp": "finalized crossover day D close",
            "cross_up_target_position": 1,
            "cross_down_target_position": -1,
            "position_rule": "hold until reversal or terminal close",
        },
        "canonical_backtest_semantics": {
            "engine_id": CANONICAL_ENGINE_ID,
            "fill_model_id": "next_bar_open",
            "commission_bps": 0.0,
            "slippage_bps": 0.0,
            "terminal_close": True,
            "position_size": "one normalized contract unit",
        },
        "outputs": list(DECLARED_OUTPUT_FILES),
        "result_status": decision["result_status"],
        "custom_strategy_pnl_engine_used": False,
        "actual_market_costs_bound": False,
        "strategy_promotion_allowed": False,
    }
    quality = {
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "git_commit_sha": git_commit_sha,
        "input_artifacts": inputs,
        "counts": {
            "d1_rows": int(len(d1)),
            "cross_events": int(len(context)),
            "signal_rows": int(len(signals)),
            "fill_rows": int(len(fills)),
            "cost_rows": int(len(costs)),
            "position_path_rows": int(len(position_path)),
            "rejected_signal_rows": int(len(rejected_signals)),
        },
        "source_validation": {
            "m2_quality_report_validated": True,
            "m4b_result_status": m4b_decision["result_status"],
            "m4b_selected_rule": m4b_decision["selected_rule"],
            "m4c_result_status": m4c_decision["result_status"],
            "m4c_selected_feature_group": m4c_decision["selected_feature_group"],
        },
        "canonical_engine": {
            "engine_id": result.metrics["engine_id"],
            "fill_model_id": result.metrics["fill_model_id"],
            "custom_strategy_pnl_engine_used": False,
            "zero_cost_gross_run": True,
            "ending_position_is_flat": result.metrics["final_position"] == 0,
            "fill_event_marks_only": True,
        },
        "anti_leakage": {
            "signal_source": "M2.1 finalized D1 crossover context only",
            "market_data_source": "M2.1 finalized D1 OHLC only",
            "label_or_future_fields_used": [],
            "signal_trade_execution": "strict next D1 bar open",
            "terminal_close": "last valid D1 close",
        },
        "economic_scope": {
            "pnl_unit": "underlying price units at contract multiplier one",
            "actual_market_costs_bound": False,
            "contract_multiplier_bound": False,
            "roll_or_funding_bound": False,
            "actual_net_profitability_claim_allowed": False,
        },
        "rejected_signals": rejected_signals,
        "declared_outputs": list(DECLARED_OUTPUT_FILES),
        "result_status": decision["result_status"],
        "strategy_promotion_allowed": False,
        "runtime_or_trading_logic_present": False,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = _output_paths(output_dir)
    _write_json(paths[OUTPUT_RUN_METADATA], metadata)
    _write_csv(paths[OUTPUT_SIGNALS], signals)
    _write_csv(paths[OUTPUT_FILLS], fills)
    _write_csv(paths[OUTPUT_COSTS], costs)
    _write_csv(paths[OUTPUT_POSITION_PATH], position_path)
    _write_json(paths[OUTPUT_ECONOMIC_METRICS], economic_metrics)
    _write_json(paths[OUTPUT_COST_CAPACITY], cost_capacity)
    _write_json(paths[OUTPUT_QUALITY], quality)
    _write_json(paths[OUTPUT_DECISION], decision)
    return {
        "metadata": metadata,
        "economic_metrics": economic_metrics,
        "cost_capacity": cost_capacity,
        "quality_report": quality,
        "decision": decision,
        "output_paths": {name: str(path) for name, path in paths.items()},
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    (
        d1_path,
        context_path,
        m2_quality_path,
        m4b_path,
        m4c_path,
        output_dir,
        run_id,
        git_sha,
    ) = _validate_cli_args(args, parser)
    run_research_package(
        d1_ohlc_path=d1_path,
        cross_context_path=context_path,
        m2_quality_report_path=m2_quality_path,
        m4b_decision_path=m4b_path,
        m4c_decision_path=m4c_path,
        output_dir=output_dir,
        run_id=run_id,
        git_commit_sha=git_sha,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
