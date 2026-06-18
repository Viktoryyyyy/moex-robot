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

from src.moex_features.daily.usdrubf_d1_ema_3_19_classical_indicators import (
    CALENDAR_CONTRACT,
    INDICATOR_COLUMNS,
    INSTRUMENT_ID,
    TIMEZONE_NAME,
    build_classical_indicators_frame,
)
from src.moex_features.daily.usdrubf_d1_ema_3_19_indicator_context import (
    build_ema_3_19_indicator_context_frame,
    forbidden_context_columns,
)
from src.moex_features.labels.usdrubf_d1_ema_3_19_multi_horizon_labels import (
    HORIZONS,
    build_multi_horizon_labels_frame,
)

EXPERIMENT_ID: Final = "usdrubf_ema_3_19_d1_indicators_horizons_v1"
PRODUCER: Final = "src.moex_research.runners.usdrubf_ema_3_19_d1_indicators_horizons"
OUTPUT_RUN_METADATA: Final = "run_metadata.json"
OUTPUT_INDICATORS: Final = "usdrubf_d1_classical_indicators.csv"
OUTPUT_INDICATOR_CONTEXT: Final = "usdrubf_d1_ema_3_19_indicator_context.csv"
OUTPUT_LABELS: Final = "usdrubf_d1_ema_3_19_multi_horizon_labels.csv"
OUTPUT_BASELINE_SUMMARY: Final = "usdrubf_ema_3_19_horizon_baseline_summary.csv"
OUTPUT_QUALITY_REPORT: Final = "usdrubf_ema_3_19_indicator_horizon_quality_report.json"
DECLARED_OUTPUT_FILES: Final = (
    OUTPUT_RUN_METADATA,
    OUTPUT_INDICATORS,
    OUTPUT_INDICATOR_CONTEXT,
    OUTPUT_LABELS,
    OUTPUT_BASELINE_SUMMARY,
    OUTPUT_QUALITY_REPORT,
)
_ALIAS_TOKENS: Final = ("latest", "current", "autodetect")
_GLOB_CHARACTERS: Final = frozenset("*?[]")
_SHA_RE: Final = re.compile(r"^[0-9a-fA-F]{40}$")
_TOKEN_SPLIT_RE: Final = re.compile(r"[^A-Za-z0-9]+")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize the USDRUBF D1 EMA(3/19) indicator and horizon research pack."
    )
    parser.add_argument("--d1-ohlc-path", required=True)
    parser.add_argument("--crossover-context-path", required=True)
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


def _validate_input(raw_value: str, argument_name: str, parser: argparse.ArgumentParser) -> Path:
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
    if path.suffix.lower() not in {".csv", ".parquet"}:
        parser.error(f"{argument_name} must end with .csv or .parquet")
    return path


def _validate_cli_args(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> tuple[Path, Path, Path, str, str]:
    d1_path = _validate_input(str(args.d1_ohlc_path), "--d1-ohlc-path", parser)
    context_path = _validate_input(
        str(args.crossover_context_path), "--crossover-context-path", parser
    )
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
    return d1_path, context_path, output_dir, run_id, git_sha


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_table(path: Path, artifact_name: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path) if path.suffix.lower() == ".csv" else pd.read_parquet(path)
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"failed to read {artifact_name}: {exc}") from exc


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


def _return_stats(series: pd.Series) -> dict[str, Any]:
    observed = pd.to_numeric(series, errors="coerce").dropna()
    return {
        "observed_rows": int(len(observed)),
        "mean_signed_return": None if observed.empty else float(observed.mean()),
        "median_signed_return": None if observed.empty else float(observed.median()),
        "std_signed_return": None if len(observed) < 2 else float(observed.std()),
        "min_signed_return": None if observed.empty else float(observed.min()),
        "max_signed_return": None if observed.empty else float(observed.max()),
    }


def _summary_row(
    frame: pd.DataFrame,
    horizon: str,
    group_type: str,
    group_value: str,
) -> dict[str, Any]:
    if horizon == "reverse":
        return_column = "reverse_signed_return"
        allow_column = "reverse_allow_trade"
        observed = frame[return_column].notna()
        censored_rows: int | None = int(
            frame["reverse_label_censored"].astype("boolean").fillna(True).sum()
        )
        no_opposite_rows: int | None = None
        no_opposite_rate: float | None = None
    else:
        return_column = f"{horizon}_signed_return"
        allow_column = f"{horizon}_allow_trade"
        observed = frame[return_column].notna()
        no_opposite = (
            frame.loc[observed, f"{horizon}_opposite_cross_before_exit"]
            .astype("boolean")
            .eq(False)
            .fillna(False)
        )
        no_opposite_rows = int(no_opposite.sum())
        no_opposite_rate = None if not observed.any() else float(no_opposite_rows / int(observed.sum()))
        censored_rows = 0
    allow = pd.to_numeric(frame.loc[observed, allow_column], errors="coerce").dropna()
    row: dict[str, Any] = {
        "experiment_id": EXPERIMENT_ID,
        "horizon": horizon,
        "group_type": group_type,
        "group_value": group_value,
        "event_rows": int(len(frame)),
        "unavailable_rows": int((~observed).sum()),
        "allow_trade_positive_rows": int((allow == 1).sum()),
        "allow_trade_rate": None if allow.empty else float(allow.mean()),
        "no_opposite_cross_before_exit_rows": no_opposite_rows,
        "no_opposite_cross_before_exit_rate": no_opposite_rate,
        "censored_rows": censored_rows,
    }
    row.update(_return_stats(frame[return_column]))
    return row


def build_horizon_baseline_summary(labels: pd.DataFrame) -> pd.DataFrame:
    groups = [("total", "all", labels)] + [
        ("cross_dir", direction, labels.loc[labels["cross_dir"] == direction])
        for direction in ("cross_up", "cross_down")
    ]
    return pd.DataFrame(
        [
            _summary_row(group, horizon, group_type, group_value)
            for group_type, group_value, group in groups
            for horizon in (*HORIZONS.keys(), "reverse")
        ]
    )


def _quality_counts(
    d1: pd.DataFrame,
    events: pd.DataFrame,
    indicators: pd.DataFrame,
    context: pd.DataFrame,
    labels: pd.DataFrame,
) -> dict[str, int]:
    counts = {
        "d1_input_rows": int(len(d1)),
        "d1_indicator_rows": int(len(indicators)),
        "total_event_rows": int(len(events)),
        "cross_up_rows": int((events["cross_dir"] == "cross_up").sum()),
        "cross_down_rows": int((events["cross_dir"] == "cross_down").sum()),
        "indicator_ready_event_rows": int(context["indicator_ready"].sum()),
        "indicator_non_ready_event_rows": int((~context["indicator_ready"].astype(bool)).sum()),
    }
    for horizon in HORIZONS:
        observed = labels[f"{horizon}_signed_return"].notna()
        no_reverse = (
            labels.loc[observed, f"{horizon}_opposite_cross_before_exit"]
            .astype("boolean")
            .eq(False)
            .fillna(False)
        )
        counts[f"{horizon}_observed_rows"] = int(observed.sum())
        counts[f"{horizon}_no_reverse_before_exit_rows"] = int(no_reverse.sum())
    censored = labels["reverse_label_censored"].astype("boolean").fillna(True)
    counts["reverse_uncensored_rows"] = int((~censored).sum())
    counts["reverse_censored_rows"] = int(censored.sum())
    return counts


def run_research_package(
    *,
    d1_ohlc_path: Path,
    crossover_context_path: Path,
    output_dir: Path,
    run_id: str,
    git_commit_sha: str,
) -> dict[str, Any]:
    started_at = _utc_now_iso()
    d1_hash = _sha256_file(d1_ohlc_path)
    context_hash = _sha256_file(crossover_context_path)
    d1 = _read_table(d1_ohlc_path, "D1 OHLC artifact")
    events = _read_table(crossover_context_path, "crossover context artifact")
    indicators = build_classical_indicators_frame(d1)
    context = build_ema_3_19_indicator_context_frame(events, indicators)
    labels = build_multi_horizon_labels_frame(events, d1)
    summary = build_horizon_baseline_summary(labels)
    forbidden = forbidden_context_columns(context)
    if forbidden:
        raise ValueError("indicator context contains forbidden return/label fields: " + ", ".join(forbidden))
    inputs = {
        "d1_ohlc": {"path": str(d1_ohlc_path), "sha256": d1_hash},
        "crossover_context": {"path": str(crossover_context_path), "sha256": context_hash},
    }
    quality = {
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "git_commit_sha": git_commit_sha,
        "instrument_id": INSTRUMENT_ID,
        "calendar_contract": CALENDAR_CONTRACT,
        "timezone": TIMEZONE_NAME,
        "input_artifacts": inputs,
        "join_validation": {
            "keys": ["instrument_id", "end"],
            "cardinality": "one_to_one",
            "all_events_matched_exactly_one_d1_session": True,
            "authoritative_event_stream": "supplied_crossover_context",
        },
        "anti_leakage": {
            "centered_windows_used": False,
            "forward_fill_used": False,
            "backward_fill_used": False,
            "feature_known_no_later_than": "D close",
            "entry": "D+1 open",
            "indicator_context_forbidden_columns": forbidden,
            "indicator_ready_requires_all_ten_indicators": True,
        },
        "session_index": {
            "base": "zero_based",
            "construction": "chronological finalized D1 order",
            "minimum": 0 if len(d1) else None,
            "maximum": len(d1) - 1 if len(d1) else None,
        },
        "counts": _quality_counts(d1, events, indicators, context, labels),
        "indicator_columns": list(INDICATOR_COLUMNS),
        "summary_rows": int(len(summary)),
        "declared_outputs": list(DECLARED_OUTPUT_FILES),
        "result_status": "repository_research_foundation_only",
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
        "calendar_contract": CALENDAR_CONTRACT,
        "timezone": TIMEZONE_NAME,
        "inputs": inputs,
        "canonical_semantics": {
            "crossover_known": "D close",
            "entry": "D+1 open",
            "fixed_horizons": {
                horizon: f"D+{holding_sessions + 1} open"
                for horizon, holding_sessions in HORIZONS.items()
            },
            "reverse_cross_known": "R close",
            "reverse_exit": "R+1 open",
            "allow_trade_observed": "int(signed_return > 0)",
            "allow_trade_unavailable": None,
        },
        "outputs": list(DECLARED_OUTPUT_FILES),
        "model_training_performed": False,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = _output_paths(output_dir)
    _write_csv(paths[OUTPUT_INDICATORS], indicators)
    _write_csv(paths[OUTPUT_INDICATOR_CONTEXT], context)
    _write_csv(paths[OUTPUT_LABELS], labels)
    _write_csv(paths[OUTPUT_BASELINE_SUMMARY], summary)
    _write_json(paths[OUTPUT_QUALITY_REPORT], quality)
    _write_json(paths[OUTPUT_RUN_METADATA], metadata)
    return {
        "metadata": metadata,
        "quality_report": quality,
        "output_paths": {name: str(path) for name, path in paths.items()},
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    d1_path, context_path, output_dir, run_id, git_sha = _validate_cli_args(args, parser)
    run_research_package(
        d1_ohlc_path=d1_path,
        crossover_context_path=context_path,
        output_dir=output_dir,
        run_id=run_id,
        git_commit_sha=git_sha,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
