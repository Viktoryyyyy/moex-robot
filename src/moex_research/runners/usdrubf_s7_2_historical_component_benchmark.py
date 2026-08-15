from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from src.moex_features.daily.usdrubf_d1_ohlc_from_5m import (
    build_d1_ohlc_from_5m_frame,
    normalize_intraday_5m_frame,
)
from src.moex_research.intelligence.usdrubf_intelligence_benchmark import (
    BenchmarkObservation,
    evaluate_intelligence_quality,
    realized_bias,
)
from src.moex_research.intelligence.usdrubf_live_shadow_bridge import (
    LiveShadowBridgeError,
    MOSCOW,
    build_live_decision_input,
)


PROJECT = "MOEX_Bot"
MODE = "s7_2_historical_component_benchmark"
EXPERIMENT_ID = "usdrubf_rub_intelligence_s7_2_historical_component_benchmark_v1"
DEFAULT_HORIZONS = (1, 3, 5, 10)
SOURCE_PATH_ALIAS_TOKENS = ("latest", "current", "autodetect")
_FILENAME_STEM_TOKEN_SPLIT_RE = re.compile(r"[^A-Za-z0-9]+")
PHASE3_PANEL_ID = "usdrubf_phase2_d1_panel.v1"
PHASE3_INSTRUMENT_ID = "forts.usdrubf"
PHASE3_SECID = "USDRUBF"

OUTPUT_RUN_METADATA = "run_metadata.json"
OUTPUT_REPLAY_ROWS = "historical_replay_rows.csv"
OUTPUT_REPLAY_EXCLUSIONS = "replay_exclusions.csv"
OUTPUT_EMA_BIAS_ONLY = "ema_bias_only_metrics.json"
OUTPUT_EMA_ALWAYS_ACTIVE = "ema_always_active_metrics.json"
OUTPUT_STRUCTURE_SUMMARY = "structure_forward_summary.csv"
OUTPUT_QUALITY_REPORT = "quality_report.json"
DECLARED_OUTPUTS = (
    OUTPUT_RUN_METADATA,
    OUTPUT_REPLAY_ROWS,
    OUTPUT_REPLAY_EXCLUSIONS,
    OUTPUT_EMA_BIAS_ONLY,
    OUTPUT_EMA_ALWAYS_ACTIVE,
    OUTPUT_STRUCTURE_SUMMARY,
    OUTPUT_QUALITY_REPORT,
)
_EXCLUSION_COLUMNS = (
    "trade_date",
    "prior_trade_date",
    "error_type",
    "reason",
    "current_bar_count",
    "prior_bar_count",
)


class HistoricalComponentBenchmarkError(ValueError):
    """Raised when S7.2 historical replay or artifact boundaries are violated."""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay historical USDRUBF 5m data through the current deterministic "
            "input bridge and benchmark component signals."
        ),
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--source-dataset-path")
    source.add_argument("--source-panel-manifest-path")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--neutral-band-bps", type=float, default=0.0)
    parser.add_argument("--high-confidence-threshold", type=float, default=0.75)
    parser.add_argument(
        "--horizons",
        default=",".join(str(value) for value in DEFAULT_HORIZONS),
        help="Comma-separated positive trading-day horizons; default 1,3,5,10",
    )
    return parser


def _path_alias_tokens(path_value: str) -> list[str]:
    components = [
        component.lower()
        for component in re.split(r"[\\/]+", path_value)
        if component and component not in {".", ".."}
    ]
    filename = components[-1] if components else ""
    stem_tokens = {
        token.lower()
        for token in _FILENAME_STEM_TOKEN_SPLIT_RE.split(Path(filename).stem)
        if token
    }
    component_tokens = set(components)
    return [
        token
        for token in SOURCE_PATH_ALIAS_TOKENS
        if token in component_tokens or token in stem_tokens
    ]


def _validate_source_path(raw_value: object, *, field: str, suffix: str) -> Path:
    raw = str(raw_value or "").strip()
    if not raw:
        raise HistoricalComponentBenchmarkError(f"{field} must be non-empty")
    aliases = _path_alias_tokens(raw)
    if aliases:
        raise HistoricalComponentBenchmarkError(
            f"{field} must not use mutable alias token(s): " + ", ".join(aliases)
        )
    path = Path(raw).expanduser()
    if not path.exists() or not path.is_file() or path.is_symlink():
        raise HistoricalComponentBenchmarkError(
            f"{field} must be an existing regular non-symlink file"
        )
    if path.suffix.lower() != suffix:
        raise HistoricalComponentBenchmarkError(f"{field} must have {suffix} suffix")
    return path.resolve()


def _parse_date(value: str | None, field: str) -> pd.Timestamp | None:
    if value is None or not value.strip():
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise HistoricalComponentBenchmarkError(f"{field} must be a valid date")
    return pd.Timestamp(parsed).normalize()


def _parse_horizons(value: str) -> tuple[int, ...]:
    parts = [item.strip() for item in value.split(",") if item.strip()]
    if not parts:
        raise HistoricalComponentBenchmarkError("horizons must not be empty")
    try:
        horizons = tuple(int(item) for item in parts)
    except ValueError as exc:
        raise HistoricalComponentBenchmarkError("horizons must contain integers") from exc
    if any(item <= 0 for item in horizons):
        raise HistoricalComponentBenchmarkError("horizons must be positive")
    if len(horizons) != len(set(horizons)):
        raise HistoricalComponentBenchmarkError("horizons must be unique")
    return tuple(sorted(horizons))


def _validate_cli(
    args: argparse.Namespace,
) -> tuple[str, Path, Path, str, pd.Timestamp | None, pd.Timestamp | None, tuple[int, ...]]:
    csv_value = getattr(args, "source_dataset_path", None)
    manifest_value = getattr(args, "source_panel_manifest_path", None)
    if bool(csv_value) == bool(manifest_value):
        raise HistoricalComponentBenchmarkError(
            "exactly one source_dataset_path or source_panel_manifest_path is required"
        )
    if csv_value:
        source_mode = "explicit_csv"
        source = _validate_source_path(
            csv_value,
            field="source_dataset_path",
            suffix=".csv",
        )
    else:
        source_mode = "phase3_panel_manifest"
        source = _validate_source_path(
            manifest_value,
            field="source_panel_manifest_path",
            suffix=".json",
        )

    raw_output = str(args.output_dir).strip()
    if not raw_output:
        raise HistoricalComponentBenchmarkError("output_dir must be non-empty")
    output_dir = Path(raw_output).expanduser()
    if output_dir.exists() and (not output_dir.is_dir() or output_dir.is_symlink()):
        raise HistoricalComponentBenchmarkError("output_dir must be a regular directory")

    run_id = str(args.run_id).strip()
    if not run_id:
        raise HistoricalComponentBenchmarkError("run_id must be non-empty")

    start = _parse_date(args.start_date, "start_date")
    end = _parse_date(args.end_date, "end_date")
    if start is not None and end is not None and start > end:
        raise HistoricalComponentBenchmarkError("start_date must not be after end_date")
    horizons = _parse_horizons(str(args.horizons))
    return source_mode, source, output_dir, run_id, start, end, horizons


def _json_safe(value: Any) -> Any:
    if value is pd.NA:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(
        json.dumps(
            _json_safe(dict(payload)),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_source_frame(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    normalized = normalize_intraday_5m_frame(
        raw,
        instrument_id="usdrubf",
        timezone_name="Europe/Moscow",
    )
    if "volume" not in normalized.columns:
        raise HistoricalComponentBenchmarkError(
            "historical replay requires volume in the 5m source dataset"
        )
    daily = build_d1_ohlc_from_5m_frame(
        normalized,
        instrument_id="usdrubf",
        timezone_name="Europe/Moscow",
    )
    normalized = normalized.copy()
    normalized["trade_date"] = normalized["end"].dt.normalize()
    complete_dates = set(pd.to_datetime(daily["end"]).dt.normalize())
    normalized = normalized[
        normalized["trade_date"].isin(complete_dates)
    ].reset_index(drop=True)
    return daily.reset_index(drop=True), normalized


def _complete_daily_and_intraday(source: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _normalize_source_frame(pd.read_csv(source))


def _require_manifest_mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise HistoricalComponentBenchmarkError(f"{field} must be a JSON object")
    return value


def _phase3_manifest_source(
    manifest_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, Mapping[str, object]]:
    try:
        manifest = _require_manifest_mapping(
            json.loads(manifest_path.read_text(encoding="utf-8")),
            "phase3 panel manifest",
        )
    except json.JSONDecodeError as exc:
        raise HistoricalComponentBenchmarkError("phase3 panel manifest is invalid JSON") from exc

    if manifest.get("panel_id") != PHASE3_PANEL_ID:
        raise HistoricalComponentBenchmarkError("phase3 panel manifest panel_id mismatch")
    if manifest.get("panel_schema_version") != PHASE3_PANEL_ID:
        raise HistoricalComponentBenchmarkError(
            "phase3 panel manifest schema version mismatch"
        )
    if manifest.get("instrument_id") != PHASE3_INSTRUMENT_ID:
        raise HistoricalComponentBenchmarkError(
            "phase3 panel manifest instrument_id mismatch"
        )
    if manifest.get("secid") != PHASE3_SECID:
        raise HistoricalComponentBenchmarkError("phase3 panel manifest secid mismatch")
    run_id = str(manifest.get("run_id") or "").strip()
    if not run_id:
        raise HistoricalComponentBenchmarkError("phase3 panel manifest run_id missing")

    raw_partitions = manifest.get("input_partitions")
    if isinstance(raw_partitions, (str, bytes)) or not isinstance(raw_partitions, list):
        raise HistoricalComponentBenchmarkError(
            "phase3 panel manifest input_partitions must be a list"
        )
    if not raw_partitions:
        raise HistoricalComponentBenchmarkError(
            "phase3 panel manifest input_partitions must be non-empty"
        )
    declared_count = manifest.get("input_partition_count")
    if isinstance(declared_count, bool) or not isinstance(declared_count, int):
        raise HistoricalComponentBenchmarkError(
            "phase3 panel manifest input_partition_count must be integer"
        )
    if declared_count != len(raw_partitions):
        raise HistoricalComponentBenchmarkError(
            "phase3 panel manifest input partition count mismatch"
        )

    partition_paths: list[Path] = []
    seen: set[Path] = set()
    for index, raw_path in enumerate(raw_partitions):
        path = _validate_source_path(
            raw_path,
            field=f"input_partitions[{index}]",
            suffix=".parquet",
        )
        if path in seen:
            raise HistoricalComponentBenchmarkError(
                "phase3 panel manifest contains duplicate input partition"
            )
        seen.add(path)
        partition_paths.append(path)

    frames: list[pd.DataFrame] = []
    for path in partition_paths:
        frame = pd.read_parquet(path)
        if frame.empty:
            raise HistoricalComponentBenchmarkError(
                "phase3 raw 5m input partition is empty"
            )
        if "instrument_id" in frame.columns:
            identity = set(frame["instrument_id"].astype(str).str.strip())
            if identity != {PHASE3_INSTRUMENT_ID}:
                raise HistoricalComponentBenchmarkError(
                    "phase3 raw input instrument_id mismatch"
                )
        if "secid" in frame.columns:
            identity = set(frame["secid"].astype(str).str.strip())
            if identity != {PHASE3_SECID}:
                raise HistoricalComponentBenchmarkError(
                    "phase3 raw input secid mismatch"
                )
        if "end" in frame.columns:
            timestamp_column = "end"
        elif "ts" in frame.columns:
            timestamp_column = "ts"
        else:
            raise HistoricalComponentBenchmarkError(
                "phase3 raw input requires end or ts timestamp column"
            )
        required = [timestamp_column, "open", "high", "low", "close", "volume"]
        missing = [column for column in required if column not in frame.columns]
        if missing:
            raise HistoricalComponentBenchmarkError(
                "phase3 raw input missing required columns: " + ", ".join(missing)
            )
        selected = frame[required].copy().rename(columns={timestamp_column: "end"})
        frames.append(selected)

    raw = pd.concat(frames, ignore_index=True)
    raw["end"] = pd.to_datetime(raw["end"], errors="coerce")
    if raw["end"].isna().any():
        raise HistoricalComponentBenchmarkError(
            "phase3 raw input contains invalid timestamps"
        )
    raw = raw.sort_values("end", kind="stable").reset_index(drop=True)
    if raw["end"].duplicated().any():
        raise HistoricalComponentBenchmarkError(
            "phase3 raw input contains duplicate timestamps"
        )

    daily, intraday = _normalize_source_frame(raw)
    provenance = {
        "source_mode": "phase3_panel_manifest",
        "panel_manifest_path": str(manifest_path),
        "panel_manifest_sha256": _sha256(manifest_path),
        "panel_id": PHASE3_PANEL_ID,
        "panel_run_id": run_id,
        "panel_instrument_id": PHASE3_INSTRUMENT_ID,
        "panel_secid": PHASE3_SECID,
        "input_partition_count": len(partition_paths),
        "input_partition_paths_recorded_in_manifest": True,
        "directory_scan_used": False,
    }
    return daily, intraday, provenance


def _filter_prediction_rows(
    replay: pd.DataFrame,
    *,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> pd.DataFrame:
    """Filter frozen prediction rows after full-history replay and label join.

    The prior session before `start` remains available to build the first requested
    day's causal Level/Structure context. Trading days after `end` remain available
    only as post-hoc forward labels. They never enter prediction construction.
    """

    work = replay.copy()
    trade_dates = pd.to_datetime(work["trade_date"], errors="coerce").dt.normalize()
    if trade_dates.isna().any():
        raise HistoricalComponentBenchmarkError("replay contains invalid trade_date values")
    mask = pd.Series(True, index=work.index)
    if start is not None:
        mask &= trade_dates >= start
    if end is not None:
        mask &= trade_dates <= end
    selected = work[mask].reset_index(drop=True)
    if selected.empty:
        raise HistoricalComponentBenchmarkError("date filter produced zero prediction rows")
    return selected


def _filter_exclusion_rows(
    exclusions: pd.DataFrame,
    *,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> pd.DataFrame:
    if exclusions.empty:
        return exclusions.copy().reset_index(drop=True)
    trade_dates = pd.to_datetime(exclusions["trade_date"], errors="coerce").dt.normalize()
    if trade_dates.isna().any():
        raise HistoricalComponentBenchmarkError("exclusions contain invalid trade_date values")
    mask = pd.Series(True, index=exclusions.index)
    if start is not None:
        mask &= trade_dates >= start
    if end is not None:
        mask &= trade_dates <= end
    return exclusions[mask].reset_index(drop=True)


def _aware_bars(frame: pd.DataFrame) -> tuple[dict[str, object], ...]:
    if frame.empty:
        raise HistoricalComponentBenchmarkError("cannot replay an empty session")
    rows: list[dict[str, object]] = []
    for raw in frame.itertuples(index=False):
        ts = pd.Timestamp(raw.end)
        if ts.tzinfo is None:
            ts = ts.tz_localize(MOSCOW)
        else:
            ts = ts.tz_convert(MOSCOW)
        rows.append(
            {
                "end": ts.to_pydatetime(),
                "open": float(raw.open),
                "high": float(raw.high),
                "low": float(raw.low),
                "close": float(raw.close),
                "volume": float(raw.volume),
            }
        )
    return tuple(rows)


def _interaction_state(decision_input, level_type: str) -> tuple[str, float]:
    levels = {item.level_id: item for item in decision_input.active_levels}
    interactions = {item.level_id: item for item in decision_input.level_interactions}
    matching = [level for level in levels.values() if level.level_type == level_type]
    if len(matching) != 1:
        raise HistoricalComponentBenchmarkError(
            f"expected exactly one {level_type} level"
        )
    interaction = interactions[matching[0].level_id]
    return interaction.state, float(interaction.structural_quality)


def build_historical_replay_with_exclusions(
    daily: pd.DataFrame,
    intraday: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Replay only days accepted by the unchanged live bridge.

    Historical sessions that make the current live bridge fail closed are recorded
    and excluded. No 5m bars are repaired, synthesized, shifted, or removed to make
    an otherwise ineligible day pass the bridge.
    """

    if len(daily) < 2:
        raise HistoricalComponentBenchmarkError(
            "at least two complete trading days are required"
        )

    daily = daily.copy().reset_index(drop=True)
    daily["trade_date"] = pd.to_datetime(daily["end"]).dt.normalize()
    grouped = {
        pd.Timestamp(key): group.reset_index(drop=True)
        for key, group in intraday.groupby("trade_date", sort=True)
    }

    rows: list[dict[str, object]] = []
    exclusion_rows: list[dict[str, object]] = []
    for index in range(1, len(daily)):
        current_date = pd.Timestamp(daily.loc[index, "trade_date"])
        prior_date = pd.Timestamp(daily.loc[index - 1, "trade_date"])
        current_frame = grouped.get(current_date)
        prior_frame = grouped.get(prior_date)
        if current_frame is None or prior_frame is None:
            raise HistoricalComponentBenchmarkError(
                "complete daily date is missing its intraday session"
            )
        current_bars = _aware_bars(current_frame)
        prior_bars = _aware_bars(prior_frame)
        wall_clock = current_bars[-1]["end"]
        try:
            decision_input = build_live_decision_input(
                current_session_bars=current_bars,
                prior_session_bars=prior_bars,
                wall_clock_as_of=wall_clock,
            )
        except LiveShadowBridgeError as exc:
            exclusion_rows.append(
                {
                    "trade_date": current_date.date().isoformat(),
                    "prior_trade_date": prior_date.date().isoformat(),
                    "error_type": type(exc).__name__,
                    "reason": str(exc),
                    "current_bar_count": len(current_bars),
                    "prior_bar_count": len(prior_bars),
                }
            )
            continue

        high_state, high_quality = _interaction_state(
            decision_input, "PREVIOUS_SESSION_HIGH"
        )
        low_state, low_quality = _interaction_state(
            decision_input, "PREVIOUS_SESSION_LOW"
        )

        row: dict[str, object] = {
            "trade_date": current_date.date().isoformat(),
            "prior_trade_date": prior_date.date().isoformat(),
            "as_of_timestamp": decision_input.as_of_timestamp.isoformat(),
            "price": float(decision_input.price),
            "trend": decision_input.trend,
            "market_regime": decision_input.market_regime,
            "ema_direction": decision_input.ema_3_19_ai.direction,
            "ema_confidence": float(decision_input.ema_3_19_ai.confidence),
            "previous_session_high_state": high_state,
            "previous_session_high_quality": high_quality,
            "previous_session_low_state": low_state,
            "previous_session_low_quality": low_quality,
            "structure_signature": f"HIGH:{high_state}|LOW:{low_state}",
        }
        for horizon in horizons:
            future_index = index + horizon
            row[f"future_price_h{horizon}"] = (
                None
                if future_index >= len(daily)
                else float(daily.loc[future_index, "close"])
            )
        rows.append(row)

    replay = pd.DataFrame(rows)
    exclusions = pd.DataFrame(exclusion_rows, columns=_EXCLUSION_COLUMNS)
    if replay.empty:
        raise HistoricalComponentBenchmarkError(
            "historical replay produced zero live-bridge-eligible rows; "
            f"excluded_days={len(exclusions)}"
        )
    if replay["as_of_timestamp"].duplicated().any():
        raise HistoricalComponentBenchmarkError(
            "historical replay produced duplicate as_of_timestamp values"
        )
    return replay, exclusions


def build_historical_replay_rows(
    daily: pd.DataFrame,
    intraday: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    replay, _ = build_historical_replay_with_exclusions(
        daily,
        intraday,
        horizons=horizons,
    )
    return replay


def _benchmark_observations(
    replay: pd.DataFrame,
    *,
    horizons: Sequence[int],
    always_active: bool,
) -> tuple[BenchmarkObservation, ...]:
    observations: list[BenchmarkObservation] = []
    for raw in replay.itertuples(index=False):
        future_prices: dict[int, float] = {}
        for horizon in horizons:
            value = getattr(raw, f"future_price_h{horizon}")
            if value is not None and not pd.isna(value):
                future_prices[horizon] = float(value)
        trend = str(raw.trend)
        trade_state = (
            "HOLD"
            if always_active and trend in {"BULLISH_USD", "BEARISH_USD"}
            else "WAIT"
        )
        observations.append(
            BenchmarkObservation(
                as_of_timestamp=str(raw.as_of_timestamp),
                price=float(raw.price),
                final_bias=trend,
                trade_state=trade_state,
                confidence=float(raw.ema_confidence),
                future_prices=future_prices,
                trend=trend,
                market_regime=str(raw.market_regime),
            )
        )
    return tuple(observations)


def build_structure_forward_summary(
    replay: pd.DataFrame,
    *,
    horizons: Sequence[int],
    neutral_band_bps: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for grouping_field in ("market_regime", "structure_signature"):
        for group_value, group in replay.groupby(grouping_field, sort=True):
            for horizon in horizons:
                future_column = f"future_price_h{horizon}"
                eligible = group[group[future_column].notna()].copy()
                if eligible.empty:
                    continue
                returns_bps = (
                    eligible[future_column].astype(float)
                    / eligible["price"].astype(float)
                    - 1.0
                ) * 10_000.0
                realized = [
                    realized_bias(
                        start_price=float(start_price),
                        future_price=float(future_price),
                        neutral_band_bps=neutral_band_bps,
                    )
                    for start_price, future_price in zip(
                        eligible["price"], eligible[future_column]
                    )
                ]
                rows.append(
                    {
                        "grouping_field": grouping_field,
                        "group_value": str(group_value),
                        "horizon": int(horizon),
                        "count": int(len(eligible)),
                        "mean_return_bps": float(returns_bps.mean()),
                        "median_return_bps": float(returns_bps.median()),
                        "bullish_rate": realized.count("BULLISH_USD") / len(realized),
                        "neutral_rate": realized.count("NEUTRAL") / len(realized),
                        "bearish_rate": realized.count("BEARISH_USD") / len(realized),
                    }
                )
    return pd.DataFrame(rows)


def run(args: argparse.Namespace) -> Mapping[str, object]:
    source_mode, source, output_dir, run_id, start, end, horizons = _validate_cli(args)
    output_dir.mkdir(parents=True, exist_ok=True)
    root = output_dir.resolve()
    paths = {name: root / name for name in DECLARED_OUTPUTS}
    if any(path.resolve().parent != root for path in paths.values()):
        raise HistoricalComponentBenchmarkError(
            "declared output escaped output_dir"
        )

    if source_mode == "phase3_panel_manifest":
        daily, intraday, provenance = _phase3_manifest_source(source)
    else:
        daily, intraday = _complete_daily_and_intraday(source)
        provenance = {
            "source_mode": "explicit_csv",
            "source_dataset_path": str(source),
            "source_dataset_sha256": _sha256(source),
            "directory_scan_used": False,
        }

    full_replay, full_exclusions = build_historical_replay_with_exclusions(
        daily,
        intraday,
        horizons=horizons,
    )
    replay = _filter_prediction_rows(full_replay, start=start, end=end)
    exclusions = _filter_exclusion_rows(full_exclusions, start=start, end=end)

    bias_only = evaluate_intelligence_quality(
        _benchmark_observations(replay, horizons=horizons, always_active=False),
        horizons=horizons,
        neutral_band_bps=float(args.neutral_band_bps),
        high_confidence_threshold=float(args.high_confidence_threshold),
    )
    always_active = evaluate_intelligence_quality(
        _benchmark_observations(replay, horizons=horizons, always_active=True),
        horizons=horizons,
        neutral_band_bps=float(args.neutral_band_bps),
        high_confidence_threshold=float(args.high_confidence_threshold),
    )
    structure_summary = build_structure_forward_summary(
        replay,
        horizons=horizons,
        neutral_band_bps=float(args.neutral_band_bps),
    )

    replay.to_csv(paths[OUTPUT_REPLAY_ROWS], index=False)
    exclusions.to_csv(paths[OUTPUT_REPLAY_EXCLUSIONS], index=False)
    structure_summary.to_csv(paths[OUTPUT_STRUCTURE_SUMMARY], index=False)
    _write_json(paths[OUTPUT_EMA_BIAS_ONLY], bias_only)
    _write_json(paths[OUTPUT_EMA_ALWAYS_ACTIVE], always_active)

    candidate_days = max(int(len(daily)) - 1, 0)
    live_bridge_coverage = (
        None if candidate_days == 0 else float(len(full_replay) / candidate_days)
    )
    exclusion_reasons = (
        {}
        if full_exclusions.empty
        else {
            str(key): int(value)
            for key, value in full_exclusions["reason"].value_counts().items()
        }
    )

    run_metadata = {
        "project": PROJECT,
        "mode": MODE,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "created_at": _utc_now_iso(),
        "source_provenance": dict(provenance),
        "output_dir": str(root),
        "start_date": None if start is None else start.date().isoformat(),
        "end_date": None if end is None else end.date().isoformat(),
        "horizons": list(horizons),
        "neutral_band_bps": float(args.neutral_band_bps),
        "high_confidence_threshold": float(args.high_confidence_threshold),
        "declared_outputs": list(DECLARED_OUTPUTS),
        "historical_live_bridge_exclusion_policy": "FAIL_CLOSED_EXCLUDE_DAY_NO_REPAIR",
        "full_decision_agent_evaluated": False,
        "full_decision_agent_blocker": (
            "operational scheduler is pinned to SAFE_WAIT; no frozen non-SAFE_WAIT "
            "production decision policy exists"
        ),
    }
    _write_json(paths[OUTPUT_RUN_METADATA], run_metadata)

    quality_report = {
        "project": PROJECT,
        "mode": MODE,
        "run_id": run_id,
        "source_mode": source_mode,
        "complete_daily_rows": int(len(daily)),
        "candidate_prediction_days": candidate_days,
        "live_bridge_eligible_prediction_days": int(len(full_replay)),
        "live_bridge_excluded_prediction_days": int(len(full_exclusions)),
        "live_bridge_coverage": live_bridge_coverage,
        "live_bridge_exclusion_reasons": exclusion_reasons,
        "prediction_window_excluded_days": int(len(exclusions)),
        "full_replay_rows_before_prediction_filter": int(len(full_replay)),
        "prediction_rows_after_filter": int(len(replay)),
        "first_prediction_trade_date": str(replay.iloc[0]["trade_date"]),
        "last_prediction_trade_date": str(replay.iloc[-1]["trade_date"]),
        "prior_context_preserved_before_start_date": True,
        "post_end_rows_used_only_for_forward_labels": True,
        "future_labels_post_hoc_only": True,
        "decision_input_future_data_used": False,
        "historical_session_gaps_repaired": False,
        "historical_bars_synthesized": False,
        "live_bridge_runtime_semantics_relaxed": False,
        "futoi_authority": "BLOCKED/EXCLUDED",
        "news_authority": "EXCLUDED_FROM_HISTORICAL_COMPONENT_REPLAY",
        "macro_authority": "EXCLUDED_FROM_HISTORICAL_COMPONENT_REPLAY",
        "decision_agent": "NOT_EVALUATED",
        "ema_component": "CURRENT_LIVE_BRIDGE_15M_SEMANTICS_REPLAYED",
        "ema_confidence_semantics": "CURRENT_BRIDGE_FIXED_1_0_WHEN_AVAILABLE",
        "structure_component": "CURRENT_PREVIOUS_SESSION_LEVEL_ENGINE_REPLAYED",
        "structure_directional_rule_invented": False,
        "server_runtime_modified": False,
        "broker_order_execution": False,
    }
    _write_json(paths[OUTPUT_QUALITY_REPORT], quality_report)

    return {
        "project": PROJECT,
        "mode": MODE,
        "status": "COMPLETED",
        "run_id": run_id,
        "source_mode": source_mode,
        "replay_rows": int(len(replay)),
        "excluded_rows": int(len(exclusions)),
        "live_bridge_coverage": live_bridge_coverage,
        "structure_summary_rows": int(len(structure_summary)),
        "full_decision_agent_evaluated": False,
        "output_dir": str(root),
    }


def main() -> int:
    args = _parser().parse_args()
    result = run(args)
    for key, value in result.items():
        print(f"{str(key).upper()}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
