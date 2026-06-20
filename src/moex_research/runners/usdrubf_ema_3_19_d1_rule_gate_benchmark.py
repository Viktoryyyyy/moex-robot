from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Sequence

import numpy as np
import pandas as pd

EXPERIMENT_ID: Final = "usdrubf_ema_3_19_d1_rule_gate_benchmark_v1"
SOURCE_EXPERIMENT_ID: Final = "usdrubf_ema_3_19_d1_indicators_horizons_v1"
PRODUCER: Final = "src.moex_research.runners.usdrubf_ema_3_19_d1_rule_gate_benchmark"
INSTRUMENT_ID: Final = "usdrubf"

INDICATOR_COLUMNS: Final = (
    "rsi_14",
    "roc_10",
    "stoch_k_14",
    "stoch_d_3",
    "adx_14",
    "di_spread_14",
    "macd_hist_12_26_9_pct",
    "atr_14_pct",
    "bb_percent_b_20_2",
    "bb_bandwidth_20_2",
)
DIRECTION_VALUES: Final = {"cross_up": 1, "cross_down": -1}
RULE_NAMES: Final = (
    "no_gate",
    "adx_di",
    "adx_di_momentum",
    "moderate_trend_confirmation",
)
CANDIDATE_RULES: Final = RULE_NAMES[1:]
FORMAL_HORIZONS: Final = ("h1", "h2", "h3", "h5", "h10", "reverse")

RANDOM_SEED: Final = 319
RANDOM_REPETITIONS: Final = 10_000
BOOTSTRAP_SEED: Final = 319
BOOTSTRAP_REPETITIONS: Final = 5_000
BOOTSTRAP_CONFIDENCE_LEVEL: Final = 0.90
MINIMUM_ACCEPTED_EVENTS: Final = 12
MINIMUM_ACCEPTANCE_RATE: Final = 0.20
MAXIMUM_ACCEPTANCE_RATE: Final = 0.60
MAX_ADJUSTED_P_VALUE: Final = 0.10
MAX_POSITIVE_YEAR_CONTRIBUTION: Final = 0.70

OUTPUT_RUN_METADATA: Final = "run_metadata.json"
OUTPUT_FORMAL_DIAGNOSTICS: Final = "m4b_formal_horizon_diagnostics.csv"
OUTPUT_GATE_METRICS: Final = "m4b_rule_gate_metrics.csv"
OUTPUT_YEAR_METRICS: Final = "m4b_rule_gate_year_metrics.csv"
OUTPUT_DIRECTION_METRICS: Final = "m4b_rule_gate_direction_metrics.csv"
OUTPUT_RANDOM_NULL: Final = "m4b_random_gate_null.json"
OUTPUT_BOOTSTRAP: Final = "m4b_bootstrap_intervals.json"
OUTPUT_QUALITY: Final = "m4b_quality_report.json"
OUTPUT_DECISION: Final = "m4b_decision.json"
DECLARED_OUTPUT_FILES: Final = (
    OUTPUT_RUN_METADATA,
    OUTPUT_FORMAL_DIAGNOSTICS,
    OUTPUT_GATE_METRICS,
    OUTPUT_YEAR_METRICS,
    OUTPUT_DIRECTION_METRICS,
    OUTPUT_RANDOM_NULL,
    OUTPUT_BOOTSTRAP,
    OUTPUT_QUALITY,
    OUTPUT_DECISION,
)

_ALIAS_TOKENS: Final = ("latest", "current", "autodetect")
_GLOB_CHARACTERS: Final = frozenset("*?[]")
_SHA_RE: Final = re.compile(r"^[0-9a-fA-F]{40}$")
_TOKEN_SPLIT_RE: Final = re.compile(r"[^A-Za-z0-9]+")
_CONTEXT_FORBIDDEN_EXACT: Final = {
    "entry_open",
    "exit_open",
    "outcome_open",
    "next_open",
    "reverse_label_censored",
    "holding_sessions_to_reverse_exit",
}
_CONTEXT_FORBIDDEN_PREFIXES: Final = tuple(
    "ret_ return_ signed_ret_ signed_return_ allow_trade max_adverse_ max_favorable_ "
    "entry_session_ exit_session_ completion_ reverse_ opposite_cross_ h1_ h2_ h3_ h5_ h10_".split()
)
_CONTEXT_FORBIDDEN_MARKERS: Final = ("future_outcome", "target_value", "label_censored")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark frozen interpretable gates for USDRUBF D1 EMA(3/19) events."
    )
    parser.add_argument("--indicator-context-path", required=True)
    parser.add_argument("--labels-path", required=True)
    parser.add_argument("--quality-report-path", required=True)
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
        parser.error(
            f"{argument_name} must end with " + " or ".join(sorted(allowed_suffixes))
        )
    return path


def _validate_cli_args(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> tuple[Path, Path, Path, Path, str, str]:
    context_path = _validate_explicit_input(
        str(args.indicator_context_path),
        "--indicator-context-path",
        {".csv", ".parquet"},
        parser,
    )
    labels_path = _validate_explicit_input(
        str(args.labels_path), "--labels-path", {".csv", ".parquet"}, parser
    )
    quality_path = _validate_explicit_input(
        str(args.quality_report_path), "--quality-report-path", {".json"}, parser
    )
    if len({context_path.resolve(), labels_path.resolve(), quality_path.resolve()}) != 3:
        parser.error("the three input artifact paths must be distinct")
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
    return context_path, labels_path, quality_path, output_dir, run_id, git_sha


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_table(path: Path, artifact_name: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path) if path.suffix.lower() == ".csv" else pd.read_parquet(path)
    except Exception as exc:  # pragma: no cover - parser-specific detail
        raise ValueError(f"failed to read {artifact_name}: {exc}") from exc


def _read_json(path: Path, artifact_name: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"failed to read {artifact_name}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{artifact_name} must contain a JSON object")
    return payload


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
        raise ValueError(f"invalid timestamp values in {artifact_name} column end")
    if getattr(timestamps.dt, "tz", None) is not None:
        timestamps = timestamps.dt.tz_convert("Europe/Moscow").dt.tz_localize(None)
    return timestamps


def _numeric_series(series: pd.Series, column: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    invalid = series.notna() & numeric.isna()
    if invalid.any():
        raise ValueError(f"column {column} contains non-numeric values")
    finite_or_null = numeric.isna() | np.isfinite(numeric)
    if not finite_or_null.all():
        raise ValueError(f"column {column} contains infinite values")
    return numeric.astype("float64")


def _nullable_boolean(series: pd.Series, column: str) -> pd.Series:
    def parse(value: object) -> object:
        if pd.isna(value):
            return pd.NA
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        if isinstance(value, (int, np.integer)) and int(value) in (0, 1):
            return bool(int(value))
        if isinstance(value, (float, np.floating)) and np.isfinite(value) and float(value) in (0.0, 1.0):
            return bool(int(value))
        text = str(value).strip().lower()
        if text in {"true", "1"}:
            return True
        if text in {"false", "0"}:
            return False
        raise ValueError(f"column {column} contains a non-boolean value: {value!r}")

    return pd.Series(pd.array([parse(value) for value in series], dtype="boolean"), index=series.index)


def _context_forbidden_columns(frame: pd.DataFrame) -> list[str]:
    forbidden: list[str] = []
    for column in frame.columns:
        value = str(column).strip().lower()
        if (
            value in _CONTEXT_FORBIDDEN_EXACT
            or value.startswith(_CONTEXT_FORBIDDEN_PREFIXES)
            or any(marker in value for marker in _CONTEXT_FORBIDDEN_MARKERS)
        ):
            forbidden.append(str(column))
    return sorted(forbidden)


def _normalize_context(frame: pd.DataFrame) -> pd.DataFrame:
    required = [
        "instrument_id",
        "end",
        "cross_dir",
        "session_index",
        *INDICATOR_COLUMNS,
        "indicator_ready",
    ]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError("indicator context is missing required columns: " + ", ".join(missing))
    forbidden = _context_forbidden_columns(frame)
    if forbidden:
        raise ValueError(
            "indicator context contains label, return, or future-outcome fields: " + ", ".join(forbidden)
        )
    work = frame[required].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != INSTRUMENT_ID).any():
        raise ValueError("all indicator context instrument_id values must equal 'usdrubf'")
    work["end"] = _normalize_end(work["end"], "indicator context")
    if work.duplicated(["instrument_id", "end"]).any():
        raise ValueError("duplicate indicator context instrument_id/end keys found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("indicator context rows must be chronological by end")
    directions = work["cross_dir"].astype(str)
    invalid_directions = sorted(set(directions) - set(DIRECTION_VALUES))
    if invalid_directions:
        raise ValueError("unsupported cross_dir values: " + ", ".join(invalid_directions))
    work["cross_dir"] = directions
    session_index = pd.to_numeric(work["session_index"], errors="coerce")
    if session_index.isna().any() or not np.isfinite(session_index).all():
        raise ValueError("indicator context session_index must be finite integers")
    if not session_index.eq(np.floor(session_index)).all() or (session_index < 0).any():
        raise ValueError("indicator context session_index must contain non-negative integers")
    if not session_index.is_monotonic_increasing or session_index.duplicated().any():
        raise ValueError("indicator context session_index must be strictly increasing")
    work["session_index"] = session_index.astype("int64")
    for column in INDICATOR_COLUMNS:
        work[column] = _numeric_series(work[column], column)
    serialized_ready = _nullable_boolean(work["indicator_ready"], "indicator_ready")
    if serialized_ready.isna().any():
        raise ValueError("indicator_ready must not contain null values")
    computed_ready = work[list(INDICATOR_COLUMNS)].notna().all(axis=1)
    if not serialized_ready.astype(bool).equals(computed_ready):
        raise ValueError("indicator_ready does not equal non-null readiness across all ten indicators")
    work["indicator_ready"] = computed_ready.astype(bool)
    return work.reset_index(drop=True)


def _label_required_columns() -> list[str]:
    columns = ["instrument_id", "end", "cross_dir", "event_session_index", "entry_session_index"]
    for horizon in ("h1", "h2", "h3", "h5", "h10"):
        columns.extend(
            [
                f"{horizon}_completion_index",
                f"{horizon}_signed_return",
                f"{horizon}_allow_trade",
                f"{horizon}_opposite_cross_before_exit",
            ]
        )
    columns.extend(
        [
            "reverse_event_session_index",
            "reverse_completion_index",
            "holding_sessions_to_reverse_exit",
            "reverse_signed_return",
            "reverse_allow_trade",
            "reverse_label_censored",
        ]
    )
    return columns


def _normalize_labels(frame: pd.DataFrame) -> pd.DataFrame:
    required = _label_required_columns()
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError("labels artifact is missing required columns: " + ", ".join(missing))
    work = frame[required].copy()
    work["instrument_id"] = work["instrument_id"].astype(str)
    if (work["instrument_id"] != INSTRUMENT_ID).any():
        raise ValueError("all labels instrument_id values must equal 'usdrubf'")
    work["end"] = _normalize_end(work["end"], "labels artifact")
    if work.duplicated(["instrument_id", "end"]).any():
        raise ValueError("duplicate labels instrument_id/end keys found")
    if not work["end"].is_monotonic_increasing:
        raise ValueError("labels rows must be chronological by end")
    directions = work["cross_dir"].astype(str)
    invalid_directions = sorted(set(directions) - set(DIRECTION_VALUES))
    if invalid_directions:
        raise ValueError("unsupported labels cross_dir values: " + ", ".join(invalid_directions))
    work["cross_dir"] = directions
    event_index = pd.to_numeric(work["event_session_index"], errors="coerce")
    if event_index.isna().any() or not np.isfinite(event_index).all():
        raise ValueError("event_session_index must contain finite integers")
    if not event_index.eq(np.floor(event_index)).all() or (event_index < 0).any():
        raise ValueError("event_session_index must contain non-negative integers")
    work["event_session_index"] = event_index.astype("int64")

    for horizon in ("h1", "h2", "h3", "h5", "h10"):
        return_column = f"{horizon}_signed_return"
        allow_column = f"{horizon}_allow_trade"
        opposite_column = f"{horizon}_opposite_cross_before_exit"
        work[return_column] = _numeric_series(work[return_column], return_column)
        allow = _numeric_series(work[allow_column], allow_column)
        invalid_allow = allow.notna() & ~allow.isin([0.0, 1.0])
        if invalid_allow.any():
            raise ValueError(f"{allow_column} must contain only 0, 1, or null")
        observed = work[return_column].notna()
        if allow[observed].isna().any() or allow[~observed].notna().any():
            raise ValueError(f"{allow_column} availability must match {return_column}")
        if observed.any() and not allow[observed].astype(int).eq(
            work.loc[observed, return_column].gt(0.0).astype(int)
        ).all():
            raise ValueError(f"{allow_column} must equal int({return_column} > 0)")
        work[allow_column] = allow
        opposite = _nullable_boolean(work[opposite_column], opposite_column)
        if opposite[observed].isna().any() or opposite[~observed].notna().any():
            raise ValueError(f"{opposite_column} availability must match {return_column}")
        work[opposite_column] = opposite

    work["reverse_signed_return"] = _numeric_series(
        work["reverse_signed_return"], "reverse_signed_return"
    )
    reverse_allow = _numeric_series(work["reverse_allow_trade"], "reverse_allow_trade")
    if (reverse_allow.notna() & ~reverse_allow.isin([0.0, 1.0])).any():
        raise ValueError("reverse_allow_trade must contain only 0, 1, or null")
    reverse_censored = _nullable_boolean(work["reverse_label_censored"], "reverse_label_censored")
    if reverse_censored.isna().any():
        raise ValueError("reverse_label_censored must not contain null values")
    censored = reverse_censored.astype(bool)
    if work.loc[censored, "reverse_signed_return"].notna().any() or reverse_allow[censored].notna().any():
        raise ValueError("censored reverse labels must have null return and allow_trade")
    uncensored = ~censored
    if work.loc[uncensored, "reverse_signed_return"].isna().any() or reverse_allow[uncensored].isna().any():
        raise ValueError("uncensored reverse labels must have observed return and allow_trade")
    if uncensored.any() and not reverse_allow[uncensored].astype(int).eq(
        work.loc[uncensored, "reverse_signed_return"].gt(0.0).astype(int)
    ).all():
        raise ValueError("reverse_allow_trade must equal int(reverse_signed_return > 0)")
    work["reverse_allow_trade"] = reverse_allow
    work["reverse_label_censored"] = reverse_censored
    return work.reset_index(drop=True)


def build_analysis_frame(
    indicator_context: pd.DataFrame,
    labels: pd.DataFrame,
) -> pd.DataFrame:
    context = _normalize_context(indicator_context)
    normalized_labels = _normalize_labels(labels)
    keys = ["instrument_id", "end", "cross_dir"]
    merged = context.merge(
        normalized_labels,
        on=keys,
        how="outer",
        validate="one_to_one",
        indicator=True,
        sort=False,
    )
    if (merged["_merge"] != "both").any():
        raise ValueError("indicator context and labels must contain exactly the same event keys")
    merged = merged.drop(columns="_merge").sort_values("end", kind="stable").reset_index(drop=True)
    if not merged["session_index"].eq(merged["event_session_index"]).all():
        raise ValueError("indicator context session_index must equal labels event_session_index")
    direction = merged["cross_dir"].map(DIRECTION_VALUES)
    if direction.isna().any():  # pragma: no cover - normalized above
        raise ValueError("cross_dir direction mapping failed")
    merged["direction"] = direction.astype("int64")
    merged["dir_di_spread"] = merged["direction"] * merged["di_spread_14"]
    merged["dir_roc_10"] = merged["direction"] * merged["roc_10"]
    merged["dir_macd_hist"] = merged["direction"] * merged["macd_hist_12_26_9_pct"]
    merged["dir_rsi_centered"] = merged["direction"] * (merged["rsi_14"] - 50.0)
    merged["dir_bb_position"] = merged["direction"] * (merged["bb_percent_b_20_2"] - 0.5)
    merged["calendar_year"] = merged["end"].dt.year.astype("int64")
    h10_opposite = merged["h10_opposite_cross_before_exit"].astype("boolean")
    merged["h10_no_opposite_cross"] = ~h10_opposite
    return merged


def build_rule_masks(frame: pd.DataFrame) -> dict[str, pd.Series]:
    required = {
        "indicator_ready",
        "adx_14",
        "dir_di_spread",
        "dir_roc_10",
        "dir_rsi_centered",
        "dir_macd_hist",
        "dir_bb_position",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("analysis frame is missing frozen gate fields: " + ", ".join(missing))
    ready = frame["indicator_ready"].astype(bool)
    masks = {
        "no_gate": ready,
        "adx_di": ready & frame["adx_14"].ge(25.0) & frame["dir_di_spread"].gt(0.0),
        "adx_di_momentum": (
            ready
            & frame["adx_14"].ge(25.0)
            & frame["dir_di_spread"].gt(0.0)
            & frame["dir_roc_10"].gt(0.0)
            & frame["dir_rsi_centered"].gt(0.0)
        ),
        "moderate_trend_confirmation": (
            ready
            & frame["adx_14"].ge(20.0)
            & frame["dir_di_spread"].gt(0.0)
            & frame["dir_macd_hist"].gt(0.0)
            & frame["dir_bb_position"].gt(0.0)
        ),
    }
    for name, mask in masks.items():
        masks[name] = pd.Series(mask.fillna(False).astype(bool), index=frame.index, name=name)
        if name != "no_gate" and (masks[name] & ~masks["no_gate"]).any():
            raise ValueError(f"rule {name} accepted an indicator_non_ready event")
    return masks


def _finite_values(series: pd.Series) -> np.ndarray:
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    return values[np.isfinite(values)]


def _return_stats(frame: pd.DataFrame, return_column: str, allow_column: str) -> dict[str, Any]:
    observed = frame[return_column].notna()
    values = _finite_values(frame.loc[observed, return_column])
    allow = pd.to_numeric(frame.loc[observed, allow_column], errors="coerce").dropna()
    return {
        "observed_return_rows": int(len(values)),
        "unavailable_return_rows": int((~observed).sum()),
        "mean_signed_return": None if len(values) == 0 else float(np.mean(values)),
        "median_signed_return": None if len(values) == 0 else float(np.median(values)),
        "std_signed_return": None if len(values) < 2 else float(np.std(values, ddof=1)),
        "win_rate": None if allow.empty else float(allow.mean()),
        "positive_return_rows": int((allow == 1.0).sum()),
    }


def _difference(value: Any, baseline: Any) -> float | None:
    if value is None or baseline is None or pd.isna(value) or pd.isna(baseline):
        return None
    return float(value) - float(baseline)


def _diagnostic_row(frame: pd.DataFrame, mask: pd.Series, gate_name: str, horizon: str) -> dict[str, Any]:
    accepted = frame.loc[mask]
    if horizon == "reverse":
        row = _return_stats(accepted, "reverse_signed_return", "reverse_allow_trade")
        row.update(
            {
                "no_opposite_observed_rows": None,
                "no_opposite_cross_rate": None,
                "opposite_cross_rate": None,
                "censored_rows": int(
                    accepted["reverse_label_censored"].astype("boolean").fillna(True).sum()
                ),
            }
        )
    else:
        row = _return_stats(accepted, f"{horizon}_signed_return", f"{horizon}_allow_trade")
        opposite = accepted[f"{horizon}_opposite_cross_before_exit"].astype("boolean")
        observed_opposite = opposite.dropna()
        no_opposite = (~observed_opposite).astype(bool)
        row.update(
            {
                "no_opposite_observed_rows": int(len(observed_opposite)),
                "no_opposite_cross_rate": (
                    None if observed_opposite.empty else float(no_opposite.mean())
                ),
                "opposite_cross_rate": (
                    None if observed_opposite.empty else float(observed_opposite.astype(bool).mean())
                ),
                "censored_rows": 0,
            }
        )
    return {
        "experiment_id": EXPERIMENT_ID,
        "gate_name": gate_name,
        "horizon": horizon,
        "accepted_events": int(mask.sum()),
        **row,
    }


def build_horizon_diagnostics(
    frame: pd.DataFrame,
    masks: dict[str, pd.Series],
) -> pd.DataFrame:
    rows = [
        _diagnostic_row(frame, masks[gate_name], gate_name, horizon)
        for gate_name in RULE_NAMES
        for horizon in FORMAL_HORIZONS
    ]
    baseline = {
        row["horizon"]: row for row in rows if row["gate_name"] == "no_gate"
    }
    for row in rows:
        base = baseline[row["horizon"]]
        row["mean_signed_return_uplift_vs_no_gate"] = _difference(
            row["mean_signed_return"], base["mean_signed_return"]
        )
        row["median_signed_return_uplift_vs_no_gate"] = _difference(
            row["median_signed_return"], base["median_signed_return"]
        )
        row["win_rate_uplift_vs_no_gate"] = _difference(row["win_rate"], base["win_rate"])
        row["no_opposite_rate_uplift_vs_no_gate"] = _difference(
            row["no_opposite_cross_rate"], base["no_opposite_cross_rate"]
        )
    return pd.DataFrame(rows)


def _primary_group_metrics(group: pd.DataFrame) -> dict[str, Any]:
    stats = _return_stats(group, "h2_signed_return", "h2_allow_trade")
    return {
        "h2_observed_rows": stats["observed_return_rows"],
        "h2_mean_signed_return": stats["mean_signed_return"],
        "h2_median_signed_return": stats["median_signed_return"],
        "h2_win_rate": stats["win_rate"],
    }


def build_year_metrics(frame: pd.DataFrame, masks: dict[str, pd.Series]) -> pd.DataFrame:
    years = sorted(frame.loc[masks["no_gate"], "calendar_year"].unique().tolist())
    rows: list[dict[str, Any]] = []
    for year in years:
        year_mask = frame["calendar_year"].eq(year)
        no_gate_group = frame.loc[masks["no_gate"] & year_mask]
        baseline = _primary_group_metrics(no_gate_group)
        for gate_name in RULE_NAMES:
            accepted_mask = masks[gate_name] & year_mask
            accepted = frame.loc[accepted_mask]
            metrics = _primary_group_metrics(accepted)
            uplift = _difference(
                metrics["h2_mean_signed_return"], baseline["h2_mean_signed_return"]
            )
            cumulative_uplift = (
                None
                if uplift is None
                else float(uplift * int(metrics["h2_observed_rows"]))
            )
            rows.append(
                {
                    "experiment_id": EXPERIMENT_ID,
                    "gate_name": gate_name,
                    "calendar_year": int(year),
                    "eligible_events": int((masks["no_gate"] & year_mask).sum()),
                    "accepted_events": int(accepted_mask.sum()),
                    "acceptance_rate": (
                        None
                        if int((masks["no_gate"] & year_mask).sum()) == 0
                        else float(
                            accepted_mask.sum() / (masks["no_gate"] & year_mask).sum()
                        )
                    ),
                    **metrics,
                    "no_gate_h2_mean_signed_return": baseline["h2_mean_signed_return"],
                    "no_gate_h2_median_signed_return": baseline["h2_median_signed_return"],
                    "no_gate_h2_win_rate": baseline["h2_win_rate"],
                    "h2_mean_uplift_vs_no_gate": uplift,
                    "h2_median_uplift_vs_no_gate": _difference(
                        metrics["h2_median_signed_return"],
                        baseline["h2_median_signed_return"],
                    ),
                    "h2_win_rate_uplift_vs_no_gate": _difference(
                        metrics["h2_win_rate"], baseline["h2_win_rate"]
                    ),
                    "h2_cumulative_uplift_vs_no_gate": cumulative_uplift,
                    "h2_positive_cumulative_uplift": (
                        None if cumulative_uplift is None else max(cumulative_uplift, 0.0)
                    ),
                }
            )
    return pd.DataFrame(rows)


def build_direction_metrics(frame: pd.DataFrame, masks: dict[str, pd.Series]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for cross_dir in ("cross_up", "cross_down"):
        direction_mask = frame["cross_dir"].eq(cross_dir)
        no_gate_group = frame.loc[masks["no_gate"] & direction_mask]
        baseline = _primary_group_metrics(no_gate_group)
        for gate_name in RULE_NAMES:
            accepted_mask = masks[gate_name] & direction_mask
            metrics = _primary_group_metrics(frame.loc[accepted_mask])
            rows.append(
                {
                    "experiment_id": EXPERIMENT_ID,
                    "gate_name": gate_name,
                    "cross_dir": cross_dir,
                    "eligible_events": int((masks["no_gate"] & direction_mask).sum()),
                    "accepted_events": int(accepted_mask.sum()),
                    "acceptance_rate": (
                        None
                        if int((masks["no_gate"] & direction_mask).sum()) == 0
                        else float(
                            accepted_mask.sum() / (masks["no_gate"] & direction_mask).sum()
                        )
                    ),
                    **metrics,
                    "no_gate_h2_mean_signed_return": baseline["h2_mean_signed_return"],
                    "no_gate_h2_median_signed_return": baseline["h2_median_signed_return"],
                    "no_gate_h2_win_rate": baseline["h2_win_rate"],
                    "h2_mean_uplift_vs_no_gate": _difference(
                        metrics["h2_mean_signed_return"], baseline["h2_mean_signed_return"]
                    ),
                    "h2_median_uplift_vs_no_gate": _difference(
                        metrics["h2_median_signed_return"],
                        baseline["h2_median_signed_return"],
                    ),
                    "h2_win_rate_uplift_vs_no_gate": _difference(
                        metrics["h2_win_rate"], baseline["h2_win_rate"]
                    ),
                }
            )
    return pd.DataFrame(rows)


def _nanmean(values: np.ndarray) -> float:
    finite = values[np.isfinite(values)]
    return float(np.mean(finite)) if len(finite) else float("nan")


def _nanmedian(values: np.ndarray) -> float:
    finite = values[np.isfinite(values)]
    return float(np.median(finite)) if len(finite) else float("nan")


def _summary(values: np.ndarray) -> dict[str, Any]:
    finite = values[np.isfinite(values)]
    if len(finite) == 0:
        return {
            "valid_repetitions": 0,
            "mean": None,
            "std": None,
            "q50": None,
            "q90": None,
            "q95": None,
            "q99": None,
        }
    return {
        "valid_repetitions": int(len(finite)),
        "mean": float(np.mean(finite)),
        "std": None if len(finite) < 2 else float(np.std(finite, ddof=1)),
        "q50": float(np.quantile(finite, 0.50)),
        "q90": float(np.quantile(finite, 0.90)),
        "q95": float(np.quantile(finite, 0.95)),
        "q99": float(np.quantile(finite, 0.99)),
    }


def _adjusted_p_value(max_null: np.ndarray, observed: float | None) -> float | None:
    if observed is None or not np.isfinite(observed):
        return None
    finite = max_null[np.isfinite(max_null)]
    if len(finite) == 0:
        return None
    return float((1 + np.count_nonzero(finite >= observed)) / (len(finite) + 1))


def compute_random_gate_null(
    frame: pd.DataFrame,
    masks: dict[str, pd.Series],
    *,
    seed: int = RANDOM_SEED,
    repetitions: int = RANDOM_REPETITIONS,
) -> dict[str, Any]:
    if repetitions <= 0:
        raise ValueError("random gate repetitions must be positive")
    ready_index = frame.index[masks["no_gate"]]
    universe = frame.loc[ready_index].reset_index(drop=True)
    if universe.empty:
        raise ValueError("random gate requires at least one indicator_ready event")
    h2_values = pd.to_numeric(universe["h2_signed_return"], errors="coerce").to_numpy(dtype=float)
    persistence_values = (
        universe["h10_no_opposite_cross"].astype("boolean").astype("Float64").to_numpy(dtype=float)
    )
    h2_baseline = _nanmean(h2_values)
    persistence_baseline = _nanmean(persistence_values)
    candidate_membership = {
        gate_name: masks[gate_name].loc[ready_index].to_numpy(dtype=bool)
        for gate_name in CANDIDATE_RULES
    }
    stratum_positions: dict[tuple[int, str], np.ndarray] = {}
    for key, positions in universe.groupby(["calendar_year", "cross_dir"], sort=True).indices.items():
        stratum_positions[(int(key[0]), str(key[1]))] = np.asarray(positions, dtype=int)
    plans: dict[str, list[tuple[tuple[int, str], np.ndarray, int]]] = {}
    for gate_name, membership in candidate_membership.items():
        plan: list[tuple[tuple[int, str], np.ndarray, int]] = []
        for key, positions in stratum_positions.items():
            count = int(membership[positions].sum())
            if count > len(positions):  # pragma: no cover - impossible subset guard
                raise ValueError(f"random gate stratum count is infeasible for {gate_name}")
            if count:
                plan.append((key, positions, count))
        plans[gate_name] = plan

    rng = np.random.default_rng(seed)
    h2_null = {gate_name: np.full(repetitions, np.nan) for gate_name in CANDIDATE_RULES}
    persistence_null = {
        gate_name: np.full(repetitions, np.nan) for gate_name in CANDIDATE_RULES
    }
    max_h2 = np.full(repetitions, np.nan)
    max_persistence = np.full(repetitions, np.nan)
    for repetition in range(repetitions):
        h2_rep_values: list[float] = []
        persistence_rep_values: list[float] = []
        for gate_name in CANDIDATE_RULES:
            selected_parts = [
                rng.choice(positions, size=count, replace=False)
                for _, positions, count in plans[gate_name]
            ]
            selected = (
                np.concatenate(selected_parts)
                if selected_parts
                else np.asarray([], dtype=int)
            )
            h2_uplift = (
                _nanmean(h2_values[selected]) - h2_baseline
                if len(selected) and np.isfinite(h2_baseline)
                else float("nan")
            )
            persistence_uplift = (
                _nanmean(persistence_values[selected]) - persistence_baseline
                if len(selected) and np.isfinite(persistence_baseline)
                else float("nan")
            )
            h2_null[gate_name][repetition] = h2_uplift
            persistence_null[gate_name][repetition] = persistence_uplift
            if np.isfinite(h2_uplift):
                h2_rep_values.append(h2_uplift)
            if np.isfinite(persistence_uplift):
                persistence_rep_values.append(persistence_uplift)
        if h2_rep_values:
            max_h2[repetition] = max(h2_rep_values)
        if persistence_rep_values:
            max_persistence[repetition] = max(persistence_rep_values)

    candidates: dict[str, Any] = {}
    for gate_name in CANDIDATE_RULES:
        membership = candidate_membership[gate_name]
        accepted_h2 = _nanmean(h2_values[membership])
        accepted_persistence = _nanmean(persistence_values[membership])
        observed_h2_uplift = (
            accepted_h2 - h2_baseline
            if np.isfinite(accepted_h2) and np.isfinite(h2_baseline)
            else None
        )
        observed_persistence_uplift = (
            accepted_persistence - persistence_baseline
            if np.isfinite(accepted_persistence) and np.isfinite(persistence_baseline)
            else None
        )
        candidates[gate_name] = {
            "accepted_events": int(membership.sum()),
            "matching_status": "exact_year_x_cross_dir_strata",
            "stratum_counts": {
                f"{key[0]}|{key[1]}": int(membership[positions].sum())
                for key, positions in stratum_positions.items()
            },
            "h2_mean_uplift": {
                "observed": observed_h2_uplift,
                "unadjusted_null": _summary(h2_null[gate_name]),
                "max_stat_adjusted_p_value": _adjusted_p_value(max_h2, observed_h2_uplift),
            },
            "h10_persistence_uplift": {
                "observed": observed_persistence_uplift,
                "unadjusted_null": _summary(persistence_null[gate_name]),
                "max_stat_adjusted_p_value": _adjusted_p_value(
                    max_persistence, observed_persistence_uplift
                ),
            },
        }
    return {
        "experiment_id": EXPERIMENT_ID,
        "seed": int(seed),
        "repetitions": int(repetitions),
        "matching": {
            "accepted_count": "exact",
            "strata": ["calendar_year", "cross_dir"],
            "preserve_where_feasible": True,
            "all_candidate_plans_feasible": True,
        },
        "adjustment": "one-sided max-statistic across the three nontrivial gates",
        "no_gate_baseline": {
            "indicator_ready_events": int(len(universe)),
            "h2_mean_signed_return": h2_baseline,
            "h10_persistence_rate": persistence_baseline,
        },
        "max_statistic_null": {
            "h2_mean_uplift": _summary(max_h2),
            "h10_persistence_uplift": _summary(max_persistence),
        },
        "candidates": candidates,
    }


def _bootstrap_point_metrics(
    h2_values: np.ndarray,
    h10_returns: np.ndarray,
    persistence_values: np.ndarray,
    membership: np.ndarray,
    no_gate_h2_mean: float,
    no_gate_h2_median: float,
    no_gate_h2_win: float,
    no_gate_persistence: float,
) -> dict[str, float]:
    selected_h2 = h2_values[membership]
    selected_h10 = h10_returns[membership]
    selected_persistence = persistence_values[membership]
    h2_mean = _nanmean(selected_h2)
    h2_median = _nanmedian(selected_h2)
    finite_h2 = selected_h2[np.isfinite(selected_h2)]
    h2_win = float(np.mean(finite_h2 > 0.0)) if len(finite_h2) else float("nan")
    persistence = _nanmean(selected_persistence)
    return {
        "acceptance_rate": float(np.mean(membership)),
        "h2_mean_signed_return": h2_mean,
        "h2_median_signed_return": h2_median,
        "h2_win_rate": h2_win,
        "h2_mean_uplift": h2_mean - no_gate_h2_mean,
        "h2_median_uplift": h2_median - no_gate_h2_median,
        "h2_win_rate_uplift": h2_win - no_gate_h2_win,
        "h10_persistence_rate": persistence,
        "h10_persistence_uplift": persistence - no_gate_persistence,
        "h10_mean_signed_return": _nanmean(selected_h10),
    }


def compute_bootstrap_intervals(
    frame: pd.DataFrame,
    masks: dict[str, pd.Series],
    *,
    seed: int = BOOTSTRAP_SEED,
    repetitions: int = BOOTSTRAP_REPETITIONS,
    confidence_level: float = BOOTSTRAP_CONFIDENCE_LEVEL,
) -> dict[str, Any]:
    if repetitions <= 0:
        raise ValueError("bootstrap repetitions must be positive")
    if not 0.0 < confidence_level < 1.0:
        raise ValueError("bootstrap confidence_level must be between zero and one")
    ready_index = frame.index[masks["no_gate"]]
    universe = frame.loc[ready_index].reset_index(drop=True)
    if universe.empty:
        raise ValueError("bootstrap requires at least one indicator_ready event")
    h2_values = pd.to_numeric(universe["h2_signed_return"], errors="coerce").to_numpy(dtype=float)
    h10_returns = pd.to_numeric(universe["h10_signed_return"], errors="coerce").to_numpy(dtype=float)
    persistence_values = (
        universe["h10_no_opposite_cross"].astype("boolean").astype("Float64").to_numpy(dtype=float)
    )
    memberships = {
        gate_name: (
            np.ones(len(universe), dtype=bool)
            if gate_name == "no_gate"
            else masks[gate_name].loc[ready_index].to_numpy(dtype=bool)
        )
        for gate_name in RULE_NAMES
    }
    metric_names = (
        "acceptance_rate",
        "h2_mean_signed_return",
        "h2_median_signed_return",
        "h2_win_rate",
        "h2_mean_uplift",
        "h2_median_uplift",
        "h2_win_rate_uplift",
        "h10_persistence_rate",
        "h10_persistence_uplift",
        "h10_mean_signed_return",
    )
    samples = {
        gate_name: {metric: np.full(repetitions, np.nan) for metric in metric_names}
        for gate_name in RULE_NAMES
    }
    rng = np.random.default_rng(seed)
    for repetition in range(repetitions):
        sampled = rng.integers(0, len(universe), size=len(universe))
        sampled_h2 = h2_values[sampled]
        sampled_persistence = persistence_values[sampled]
        no_gate_h2_mean = _nanmean(sampled_h2)
        no_gate_h2_median = _nanmedian(sampled_h2)
        finite_h2 = sampled_h2[np.isfinite(sampled_h2)]
        no_gate_h2_win = (
            float(np.mean(finite_h2 > 0.0)) if len(finite_h2) else float("nan")
        )
        no_gate_persistence = _nanmean(sampled_persistence)
        for gate_name in RULE_NAMES:
            metrics = _bootstrap_point_metrics(
                sampled_h2,
                h10_returns[sampled],
                sampled_persistence,
                memberships[gate_name][sampled],
                no_gate_h2_mean,
                no_gate_h2_median,
                no_gate_h2_win,
                no_gate_persistence,
            )
            for metric, value in metrics.items():
                samples[gate_name][metric][repetition] = value

    actual_no_gate_h2_mean = _nanmean(h2_values)
    actual_no_gate_h2_median = _nanmedian(h2_values)
    finite_actual_h2 = h2_values[np.isfinite(h2_values)]
    actual_no_gate_h2_win = (
        float(np.mean(finite_actual_h2 > 0.0)) if len(finite_actual_h2) else float("nan")
    )
    actual_no_gate_persistence = _nanmean(persistence_values)
    alpha = (1.0 - confidence_level) / 2.0
    intervals: dict[str, Any] = {}
    for gate_name in RULE_NAMES:
        point = _bootstrap_point_metrics(
            h2_values,
            h10_returns,
            persistence_values,
            memberships[gate_name],
            actual_no_gate_h2_mean,
            actual_no_gate_h2_median,
            actual_no_gate_h2_win,
            actual_no_gate_persistence,
        )
        intervals[gate_name] = {}
        for metric in metric_names:
            finite = samples[gate_name][metric][np.isfinite(samples[gate_name][metric])]
            intervals[gate_name][metric] = {
                "point_estimate": point[metric],
                "lower": None if len(finite) == 0 else float(np.quantile(finite, alpha)),
                "upper": None if len(finite) == 0 else float(np.quantile(finite, 1.0 - alpha)),
                "valid_repetitions": int(len(finite)),
            }
    return {
        "experiment_id": EXPERIMENT_ID,
        "seed": int(seed),
        "repetitions": int(repetitions),
        "confidence_level": float(confidence_level),
        "method": "paired percentile bootstrap over the indicator_ready event universe",
        "resampling_unit": "event row",
        "intervals": intervals,
    }


def _diagnostic_lookup(diagnostics: pd.DataFrame, gate_name: str, horizon: str) -> dict[str, Any]:
    rows = diagnostics.loc[
        diagnostics["gate_name"].eq(gate_name) & diagnostics["horizon"].eq(horizon)
    ]
    if len(rows) != 1:
        raise ValueError(f"expected one diagnostic row for {gate_name}/{horizon}")
    return rows.iloc[0].to_dict()


def build_decision_and_gate_metrics(
    frame: pd.DataFrame,
    masks: dict[str, pd.Series],
    diagnostics: pd.DataFrame,
    year_metrics: pd.DataFrame,
    random_null: dict[str, Any],
    *,
    run_id: str,
    git_commit_sha: str,
) -> tuple[dict[str, Any], pd.DataFrame]:
    ready_events = int(masks["no_gate"].sum())
    no_gate_h2 = _diagnostic_lookup(diagnostics, "no_gate", "h2")
    no_gate_h10 = _diagnostic_lookup(diagnostics, "no_gate", "h10")
    metric_rows: list[dict[str, Any]] = []
    candidate_decisions: dict[str, Any] = {}
    for gate_name in RULE_NAMES:
        h2 = _diagnostic_lookup(diagnostics, gate_name, "h2")
        h10 = _diagnostic_lookup(diagnostics, gate_name, "h10")
        accepted_events = int(masks[gate_name].sum())
        acceptance_rate = None if ready_events == 0 else float(accepted_events / ready_events)
        minimum_count_pass = accepted_events >= MINIMUM_ACCEPTED_EVENTS
        acceptance_rate_pass = bool(
            acceptance_rate is not None
            and MINIMUM_ACCEPTANCE_RATE <= acceptance_rate <= MAXIMUM_ACCEPTANCE_RATE
        )
        candidate_null = random_null.get("candidates", {}).get(gate_name, {})
        h2_p_value = candidate_null.get("h2_mean_uplift", {}).get(
            "max_stat_adjusted_p_value"
        )
        persistence_p_value = candidate_null.get("h10_persistence_uplift", {}).get(
            "max_stat_adjusted_p_value"
        )
        gate_years = year_metrics.loc[year_metrics["gate_name"].eq(gate_name)].copy()
        positive_years = int(
            (
                pd.to_numeric(gate_years["h2_mean_uplift_vs_no_gate"], errors="coerce")
                > 0.0
            ).sum()
        )
        positive_contributions = pd.to_numeric(
            gate_years["h2_positive_cumulative_uplift"], errors="coerce"
        ).fillna(0.0)
        positive_total = float(positive_contributions.sum())
        max_positive_share = (
            None if positive_total <= 0.0 else float(positive_contributions.max() / positive_total)
        )
        conditions_h2 = {
            "accepted_h2_mean_gt_no_gate": bool(
                h2["mean_signed_return"] is not None
                and no_gate_h2["mean_signed_return"] is not None
                and h2["mean_signed_return"] > no_gate_h2["mean_signed_return"]
            ),
            "accepted_h2_median_gt_no_gate": bool(
                h2["median_signed_return"] is not None
                and no_gate_h2["median_signed_return"] is not None
                and h2["median_signed_return"] > no_gate_h2["median_signed_return"]
            ),
            "accepted_h2_win_rate_gt_no_gate": bool(
                h2["win_rate"] is not None
                and no_gate_h2["win_rate"] is not None
                and h2["win_rate"] > no_gate_h2["win_rate"]
            ),
            "max_stat_adjusted_h2_mean_uplift_p_le_0_10": bool(
                h2_p_value is not None and h2_p_value <= MAX_ADJUSTED_P_VALUE
            ),
            "positive_h2_mean_uplift_in_at_least_two_years": positive_years >= 2,
            "no_single_year_over_70pct_positive_cumulative_uplift": bool(
                max_positive_share is not None
                and max_positive_share <= MAX_POSITIVE_YEAR_CONTRIBUTION
            ),
        }
        conditions_persistence = {
            "accepted_persistence_rate_gt_no_gate": bool(
                h10["no_opposite_cross_rate"] is not None
                and no_gate_h10["no_opposite_cross_rate"] is not None
                and h10["no_opposite_cross_rate"] > no_gate_h10["no_opposite_cross_rate"]
            ),
            "accepted_h10_mean_signed_return_gt_zero": bool(
                h10["mean_signed_return"] is not None and h10["mean_signed_return"] > 0.0
            ),
            "max_stat_adjusted_persistence_p_le_0_10": bool(
                persistence_p_value is not None
                and persistence_p_value <= MAX_ADJUSTED_P_VALUE
            ),
            "accepted_events_at_least_12": minimum_count_pass,
            "acceptance_rate_within_0_20_to_0_60": acceptance_rate_pass,
        }
        is_candidate = gate_name in CANDIDATE_RULES
        h2_supported = bool(is_candidate and minimum_count_pass and acceptance_rate_pass and all(conditions_h2.values()))
        persistence_supported = bool(is_candidate and all(conditions_persistence.values()))
        rule_supported = h2_supported or persistence_supported
        row = {
            "experiment_id": EXPERIMENT_ID,
            "gate_name": gate_name,
            "is_candidate_gate": is_candidate,
            "indicator_ready_events": ready_events,
            "accepted_events": accepted_events,
            "acceptance_rate": acceptance_rate,
            "minimum_accepted_events_pass": minimum_count_pass,
            "acceptance_rate_limits_pass": acceptance_rate_pass,
            "h2_observed_rows": h2["observed_return_rows"],
            "h2_mean_signed_return": h2["mean_signed_return"],
            "h2_median_signed_return": h2["median_signed_return"],
            "h2_win_rate": h2["win_rate"],
            "no_gate_h2_mean_signed_return": no_gate_h2["mean_signed_return"],
            "no_gate_h2_median_signed_return": no_gate_h2["median_signed_return"],
            "no_gate_h2_win_rate": no_gate_h2["win_rate"],
            "h2_mean_uplift_vs_no_gate": h2["mean_signed_return_uplift_vs_no_gate"],
            "h2_median_uplift_vs_no_gate": h2["median_signed_return_uplift_vs_no_gate"],
            "h2_win_rate_uplift_vs_no_gate": h2["win_rate_uplift_vs_no_gate"],
            "h2_max_stat_adjusted_p_value": h2_p_value,
            "positive_h2_uplift_years": positive_years,
            "positive_h2_cumulative_uplift": positive_total,
            "maximum_positive_year_contribution_share": max_positive_share,
            **{f"h2_condition_{key}": value for key, value in conditions_h2.items()},
            "h2_rule_supported": h2_supported,
            "h10_observed_rows": h10["observed_return_rows"],
            "h10_mean_signed_return": h10["mean_signed_return"],
            "h10_persistence_observed_rows": h10["no_opposite_observed_rows"],
            "h10_persistence_rate": h10["no_opposite_cross_rate"],
            "no_gate_h10_persistence_rate": no_gate_h10["no_opposite_cross_rate"],
            "h10_persistence_uplift_vs_no_gate": h10["no_opposite_rate_uplift_vs_no_gate"],
            "h10_persistence_max_stat_adjusted_p_value": persistence_p_value,
            **{
                f"persistence_condition_{key}": value
                for key, value in conditions_persistence.items()
            },
            "h10_persistence_supported": persistence_supported,
            "rule_supported": rule_supported,
        }
        metric_rows.append(row)
        if is_candidate:
            candidate_decisions[gate_name] = {
                "accepted_events": accepted_events,
                "acceptance_rate": acceptance_rate,
                "candidate_limits_pass": minimum_count_pass and acceptance_rate_pass,
                "h2_rule_supported": h2_supported,
                "h2_conditions": conditions_h2,
                "h10_persistence_supported": persistence_supported,
                "h10_persistence_conditions": conditions_persistence,
                "rule_supported": rule_supported,
                "h2_max_stat_adjusted_p_value": h2_p_value,
                "h10_persistence_max_stat_adjusted_p_value": persistence_p_value,
                "positive_h2_uplift_years": positive_years,
                "maximum_positive_year_contribution_share": max_positive_share,
            }
    metrics = pd.DataFrame(metric_rows)
    h2_supported_rows = metrics.loc[metrics["h2_rule_supported"].astype(bool)]
    persistence_supported_rows = metrics.loc[
        metrics["h10_persistence_supported"].astype(bool)
    ]
    if not h2_supported_rows.empty:
        selected = h2_supported_rows.sort_values(
            ["h2_mean_uplift_vs_no_gate", "gate_name"], ascending=[False, True]
        ).iloc[0]
        selected_rule = str(selected["gate_name"])
        selection_basis = "primary_h2_rule_support_then_highest_h2_mean_uplift"
    elif not persistence_supported_rows.empty:
        selected = persistence_supported_rows.sort_values(
            ["h10_persistence_uplift_vs_no_gate", "gate_name"], ascending=[False, True]
        ).iloc[0]
        selected_rule = str(selected["gate_name"])
        selection_basis = "secondary_h10_persistence_support_then_highest_persistence_uplift"
    else:
        selected_rule = None
        selection_basis = None
    result_status = "rule_gate_supported" if selected_rule is not None else "rule_gate_not_supported"
    decision = {
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "git_commit_sha": git_commit_sha,
        "result_status": result_status,
        "fallback_result": "rule_gate_not_supported",
        "selected_rule": selected_rule,
        "selection_basis": selection_basis,
        "primary_target": "h2",
        "secondary_target": "h10_no_opposite_cross",
        "no_gate_baseline": {
            "accepted_events": ready_events,
            "h2_mean_signed_return": no_gate_h2["mean_signed_return"],
            "h2_median_signed_return": no_gate_h2["median_signed_return"],
            "h2_win_rate": no_gate_h2["win_rate"],
            "h10_persistence_rate": no_gate_h10["no_opposite_cross_rate"],
            "h10_mean_signed_return": no_gate_h10["mean_signed_return"],
        },
        "candidate_limits": {
            "minimum_accepted_events": MINIMUM_ACCEPTED_EVENTS,
            "minimum_acceptance_rate": MINIMUM_ACCEPTANCE_RATE,
            "maximum_acceptance_rate": MAXIMUM_ACCEPTANCE_RATE,
        },
        "year_contribution_definition": (
            "max(year_h2_mean_uplift, 0) multiplied by accepted observed H2 rows in that year"
        ),
        "candidate_decisions": candidate_decisions,
        "model_training_performed": False,
        "threshold_sweep_performed": False,
        "post_hoc_rule_search_performed": False,
        "runtime_or_trading_action_performed": False,
        "strategy_promotion_allowed": False,
    }
    return decision, metrics


def _validate_quality_report(
    quality: dict[str, Any],
    frame: pd.DataFrame,
) -> None:
    if quality.get("experiment_id") != SOURCE_EXPERIMENT_ID:
        raise ValueError(
            f"quality report experiment_id must equal {SOURCE_EXPERIMENT_ID!r}"
        )
    counts = quality.get("counts")
    if not isinstance(counts, dict):
        raise ValueError("quality report counts must be a JSON object")
    expected = {
        "total_event_rows": int(len(frame)),
        "indicator_ready_event_rows": int(frame["indicator_ready"].sum()),
        "indicator_non_ready_event_rows": int((~frame["indicator_ready"].astype(bool)).sum()),
        "cross_up_rows": int(frame["cross_dir"].eq("cross_up").sum()),
        "cross_down_rows": int(frame["cross_dir"].eq("cross_down").sum()),
    }
    for key, value in expected.items():
        if counts.get(key) != value:
            raise ValueError(
                f"quality report count {key}={counts.get(key)!r} does not match input artifacts {value}"
            )
    if quality.get("model_training_performed") is not False:
        raise ValueError("source quality report must state model_training_performed=false")


def _frozen_rule_definition() -> dict[str, Any]:
    return {
        "direction": {"cross_up": 1, "cross_down": -1},
        "derived_signal_time_values": {
            "dir_di_spread": "direction * di_spread_14",
            "dir_roc_10": "direction * roc_10",
            "dir_macd_hist": "direction * macd_hist_12_26_9_pct",
            "dir_rsi_centered": "direction * (rsi_14 - 50)",
            "dir_bb_position": "direction * (bb_percent_b_20_2 - 0.5)",
        },
        "rules": {
            "no_gate": ["indicator_ready"],
            "adx_di": ["adx_14 >= 25", "dir_di_spread > 0"],
            "adx_di_momentum": [
                "adx_14 >= 25",
                "dir_di_spread > 0",
                "dir_roc_10 > 0",
                "dir_rsi_centered > 0",
            ],
            "moderate_trend_confirmation": [
                "adx_14 >= 20",
                "dir_di_spread > 0",
                "dir_macd_hist > 0",
                "dir_bb_position > 0",
            ],
        },
    }


def run_research_package(
    *,
    indicator_context_path: Path,
    labels_path: Path,
    quality_report_path: Path,
    output_dir: Path,
    run_id: str,
    git_commit_sha: str,
) -> dict[str, Any]:
    if output_dir.exists() and (not output_dir.is_dir() or any(output_dir.iterdir())):
        raise ValueError("output_dir must be absent or empty to prevent stale evidence")
    started_at = _utc_now_iso()
    input_hashes = {
        "indicator_context": _sha256_file(indicator_context_path),
        "labels": _sha256_file(labels_path),
        "quality_report": _sha256_file(quality_report_path),
    }
    indicator_context = _read_table(indicator_context_path, "M4A indicator context")
    labels = _read_table(labels_path, "M4A multi-horizon labels")
    source_quality = _read_json(quality_report_path, "M4A quality report")
    frame = build_analysis_frame(indicator_context, labels)
    _validate_quality_report(source_quality, frame)
    masks = build_rule_masks(frame)
    diagnostics = build_horizon_diagnostics(frame, masks)
    year_metrics = build_year_metrics(frame, masks)
    direction_metrics = build_direction_metrics(frame, masks)
    random_null = compute_random_gate_null(
        frame,
        masks,
        seed=RANDOM_SEED,
        repetitions=RANDOM_REPETITIONS,
    )
    bootstrap = compute_bootstrap_intervals(
        frame,
        masks,
        seed=BOOTSTRAP_SEED,
        repetitions=BOOTSTRAP_REPETITIONS,
        confidence_level=BOOTSTRAP_CONFIDENCE_LEVEL,
    )
    decision, gate_metrics = build_decision_and_gate_metrics(
        frame,
        masks,
        diagnostics,
        year_metrics,
        random_null,
        run_id=run_id,
        git_commit_sha=git_commit_sha,
    )
    inputs = {
        "indicator_context": {
            "path": str(indicator_context_path),
            "sha256": input_hashes["indicator_context"],
            "source_experiment_id": SOURCE_EXPERIMENT_ID,
        },
        "labels": {
            "path": str(labels_path),
            "sha256": input_hashes["labels"],
            "source_experiment_id": SOURCE_EXPERIMENT_ID,
        },
        "quality_report": {
            "path": str(quality_report_path),
            "sha256": input_hashes["quality_report"],
            "source_experiment_id": SOURCE_EXPERIMENT_ID,
        },
    }
    quality_report = {
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "git_commit_sha": git_commit_sha,
        "instrument_id": INSTRUMENT_ID,
        "source_experiment_id": SOURCE_EXPERIMENT_ID,
        "input_artifacts": inputs,
        "join_validation": {
            "keys": ["instrument_id", "end", "cross_dir"],
            "cardinality": "one_to_one",
            "same_event_set_required": True,
            "session_index_matches_event_session_index": True,
        },
        "counts": {
            "event_rows": int(len(frame)),
            "indicator_ready_events": int(masks["no_gate"].sum()),
            "indicator_non_ready_events": int((~masks["no_gate"]).sum()),
            "cross_up_events": int(frame["cross_dir"].eq("cross_up").sum()),
            "cross_down_events": int(frame["cross_dir"].eq("cross_down").sum()),
            "formal_horizon_diagnostic_rows": int(len(diagnostics)),
            "rule_gate_metric_rows": int(len(gate_metrics)),
            "year_metric_rows": int(len(year_metrics)),
            "direction_metric_rows": int(len(direction_metrics)),
        },
        "accepted_events_by_rule": {
            gate_name: int(mask.sum()) for gate_name, mask in masks.items()
        },
        "frozen_rule_definition": _frozen_rule_definition(),
        "anti_leakage": {
            "gate_source": "indicator context only",
            "label_or_future_fields_used_in_gate": [],
            "h10_no_opposite_cross_use": "evaluation label only",
            "threshold_sweep_performed": False,
            "post_hoc_rule_search_performed": False,
        },
        "random_gate_control": {
            "seed": RANDOM_SEED,
            "repetitions": RANDOM_REPETITIONS,
            "matching": ["same accepted count", "calendar_year x cross_dir strata"],
            "adjustment": "max-statistic across the three nontrivial gates",
        },
        "bootstrap_control": {
            "seed": BOOTSTRAP_SEED,
            "repetitions": BOOTSTRAP_REPETITIONS,
            "confidence_level": BOOTSTRAP_CONFIDENCE_LEVEL,
        },
        "declared_outputs": list(DECLARED_OUTPUT_FILES),
        "result_status": decision["result_status"],
        "model_training_performed": False,
        "runtime_or_trading_logic_present": False,
    }
    metadata = {
        "experiment_id": EXPERIMENT_ID,
        "producer": PRODUCER,
        "run_id": run_id,
        "git_commit_sha": git_commit_sha,
        "started_at": started_at,
        "completed_at": _utc_now_iso(),
        "instrument_id": INSTRUMENT_ID,
        "source_experiment_id": SOURCE_EXPERIMENT_ID,
        "inputs": inputs,
        "frozen_rule_definition": _frozen_rule_definition(),
        "targets": {
            "primary": ["h2_signed_return", "h2_allow_trade"],
            "secondary": {
                "name": "h10_no_opposite_cross",
                "derived_as": "not h10_opposite_cross_before_exit",
                "use": "evaluation label only",
            },
            "diagnostic_horizons": ["h1", "h3", "h5", "h10", "reverse"],
        },
        "controls": {
            "random_gate": {
                "seed": RANDOM_SEED,
                "repetitions": RANDOM_REPETITIONS,
                "max_statistic_adjustment": True,
            },
            "bootstrap": {
                "seed": BOOTSTRAP_SEED,
                "repetitions": BOOTSTRAP_REPETITIONS,
                "confidence_level": BOOTSTRAP_CONFIDENCE_LEVEL,
            },
        },
        "outputs": list(DECLARED_OUTPUT_FILES),
        "result_status": decision["result_status"],
        "model_training_performed": False,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = _output_paths(output_dir)
    _write_csv(paths[OUTPUT_FORMAL_DIAGNOSTICS], diagnostics)
    _write_csv(paths[OUTPUT_GATE_METRICS], gate_metrics)
    _write_csv(paths[OUTPUT_YEAR_METRICS], year_metrics)
    _write_csv(paths[OUTPUT_DIRECTION_METRICS], direction_metrics)
    _write_json(paths[OUTPUT_RANDOM_NULL], random_null)
    _write_json(paths[OUTPUT_BOOTSTRAP], bootstrap)
    _write_json(paths[OUTPUT_QUALITY], quality_report)
    _write_json(paths[OUTPUT_DECISION], decision)
    _write_json(paths[OUTPUT_RUN_METADATA], metadata)
    return {
        "metadata": metadata,
        "quality_report": quality_report,
        "decision": decision,
        "output_paths": {name: str(path) for name, path in paths.items()},
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    context_path, labels_path, quality_path, output_dir, run_id, git_sha = _validate_cli_args(
        args, parser
    )
    run_research_package(
        indicator_context_path=context_path,
        labels_path=labels_path,
        quality_report_path=quality_path,
        output_dir=output_dir,
        run_id=run_id,
        git_commit_sha=git_sha,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
