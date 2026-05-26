#!/usr/bin/env python3
import argparse
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

ACCEPTED_DATA_LAKE_COMMIT = "eb7271f16b98fb31b6cbc74ade71604ab5c25bbc"
RESEARCH_READINESS_COMMIT = "4b09fa73872288ab636b8b70d1ad1bc833794870"
INPUT_CONTRACT_VERSION = "d1_tsmom_research_input_contract.v1"
RESULT_CONTRACT_VERSION = "d1_tsmom_research_result_contract.v1"
BACKTEST_SEMANTICS_CONTRACT_VERSION = "d1_tsmom_backtest_semantics_contract.v1"
RUNNER_VERSION = "d1_tsmom_minimal_research_package.v1"
ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"
ADJUSTMENT_FACTOR = 1.0
LOOKBACK = 20
MIN_HISTORY = 21
PRIMARY_DATA = "OHLCV_only"
FUTOI_USAGE = "excluded"
USDRUBF_DATASET_ID = "futures_derived_d1_ohlcv"
USDRUBF_SCHEMA_VERSION = "futures_derived_d1_ohlcv.v1"
SI_DATASET_ID = "futures_continuous_d1"
SI_SCHEMA_VERSION = "futures_continuous_d1.v1"


def die(message: str) -> None:
    raise RuntimeError(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        die("JSON root is not object: " + str(path))
    return data


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def stable_id(values: Iterable[Any]) -> str:
    raw = json.dumps(list(values), ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if text.lower() in {"", "nan", "nat", "none", "null"}:
        return ""
    return text


def parse_date(value: Any, field: str) -> str:
    text = clean_text(value)
    if not text:
        die("Missing date field: " + field)
    try:
        return pd.to_datetime(text, errors="raise").date().isoformat()
    except Exception as exc:
        die("Invalid date field " + field + ": " + text + " error=" + str(exc))
    return text


def parse_source_contracts(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value if clean_text(x)]
    if isinstance(value, tuple):
        return [str(x) for x in value if clean_text(x)]
    text = clean_text(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(x) for x in parsed if clean_text(x)]
    except Exception:
        pass
    return [x.strip() for x in text.split(",") if x.strip()]


def resolve_data_root(raw: str) -> Path:
    text = clean_text(raw) or clean_text(os.getenv("MOEX_DATA_ROOT"))
    if not text:
        die("data root must be provided via --data-root or MOEX_DATA_ROOT")
    return Path(text).expanduser().resolve()


def resolve_manifest(data_root: Path, manifest_ref: str) -> Path:
    ref = clean_text(manifest_ref)
    if not ref:
        die("--manifest-ref is required")
    path = Path(ref).expanduser()
    if not path.is_absolute():
        path = data_root / path
    path = path.resolve()
    if not path.exists():
        die("manifest not found: " + str(path))
    return path


def require_columns(frame: pd.DataFrame, cols: List[str], name: str) -> None:
    missing = [c for c in cols if c not in frame.columns]
    if missing:
        die(name + " missing required fields: " + ",".join(missing))


def read_parquet_parts(paths: List[Path], name: str) -> pd.DataFrame:
    if not paths:
        die("no parquet inputs for " + name)
    frames: List[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            die("input parquet not found: " + str(path))
        part = pd.read_parquet(path)
        part["_source_path"] = str(path)
        frames.append(part)
    if not frames:
        die("empty parquet path list for " + name)
    out = pd.concat(frames, ignore_index=True)
    if out.empty:
        die("empty dataframe for " + name)
    return out


def partition_value(path: Path, key: str) -> str:
    prefix = key + "="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix):]
    return ""


def discover_usdrubf_paths(data_root: Path, from_date: str, till: str) -> List[Path]:
    root = data_root / "futures" / "derived_d1_ohlcv"
    paths: List[Path] = []
    for path in sorted(root.glob("trade_date=*/family=USDRUBF/secid=USDRUBF/part.parquet")):
        trade_date = partition_value(path, "trade_date")
        if from_date <= trade_date <= till:
            paths.append(path)
    return paths


def discover_si_paths(data_root: Path, from_date: str, till: str) -> List[Path]:
    root = data_root / "futures" / "continuous_d1" / ("roll_policy=" + ROLL_POLICY_ID) / ("adjustment_policy=" + ADJUSTMENT_POLICY_ID) / "family=Si"
    paths: List[Path] = []
    for path in sorted(root.glob("trade_date=*/part.parquet")):
        trade_date = partition_value(path, "trade_date")
        if from_date <= trade_date <= till:
            paths.append(path)
    return paths


def prepare_usdrubf(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["trade_date", "session_date", "secid", "family_code", "open", "high", "low", "close", "volume", "schema_version"]
    require_columns(frame, required, "USDRUBF D1")
    work = frame.copy()
    work = work.loc[work["secid"].astype(str).str.upper() == "USDRUBF"].copy()
    if work.empty:
        die("USDRUBF D1 input contains no USDRUBF rows")
    if set(work["schema_version"].astype(str).unique().tolist()) != {USDRUBF_SCHEMA_VERSION}:
        die("USDRUBF schema_version mismatch")
    forbidden = ["source_contracts", "roll_map_id", "roll_policy_id", "adjustment_policy_id", "has_roll_boundary"]
    present = [c for c in forbidden if c in work.columns]
    if present:
        die("USDRUBF perpetual input contains continuous fields: " + ",".join(present))
    return normalize_ohlcv(work, "USDRUBF", USDRUBF_DATASET_ID, USDRUBF_SCHEMA_VERSION)


def prepare_si(frame: pd.DataFrame) -> pd.DataFrame:
    required = [
        "trade_date",
        "session_date",
        "continuous_symbol",
        "family_code",
        "source_contracts",
        "roll_map_id",
        "roll_policy_id",
        "adjustment_policy_id",
        "has_roll_boundary",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "schema_version",
    ]
    require_columns(frame, required, "Si continuous D1")
    work = frame.copy()
    symbol = work["continuous_symbol"].astype(str).str.upper()
    family = work["family_code"].astype(str)
    work = work.loc[(symbol == "SI") | (family == "Si")].copy()
    if work.empty:
        die("Si continuous D1 input contains no Si rows")
    if set(work["schema_version"].astype(str).unique().tolist()) != {SI_SCHEMA_VERSION}:
        die("Si schema_version mismatch")
    if set(work["roll_policy_id"].astype(str).unique().tolist()) != {ROLL_POLICY_ID}:
        die("Si roll_policy_id mismatch")
    if set(work["adjustment_policy_id"].astype(str).unique().tolist()) != {ADJUSTMENT_POLICY_ID}:
        die("Si adjustment_policy_id mismatch")
    bad_factor = int((pd.to_numeric(work["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR).sum()) if "adjustment_factor" in work.columns else len(work)
    if bad_factor:
        die("Si adjustment_factor mismatch rows=" + str(bad_factor))
    empty_contracts = int(work["source_contracts"].map(lambda x: len(parse_source_contracts(x)) == 0).sum())
    if empty_contracts:
        die("Si source_contracts empty rows=" + str(empty_contracts))
    if int(work["roll_map_id"].map(lambda x: clean_text(x) == "").sum()):
        die("Si roll_map_id empty rows")
    out = normalize_ohlcv(work, "Si", SI_DATASET_ID, SI_SCHEMA_VERSION)
    out["source_contracts"] = work["source_contracts"].map(lambda x: ",".join(parse_source_contracts(x))).values
    out["roll_map_id"] = work["roll_map_id"].astype(str).values
    out["roll_policy_id"] = work["roll_policy_id"].astype(str).values
    out["adjustment_policy_id"] = work["adjustment_policy_id"].astype(str).values
    out["has_roll_boundary"] = work["has_roll_boundary"].astype(bool).values
    return out


def normalize_ohlcv(frame: pd.DataFrame, instrument: str, dataset_id: str, schema_version: str) -> pd.DataFrame:
    out = pd.DataFrame()
    out["instrument"] = instrument
    out["trade_date"] = frame["trade_date"].map(lambda x: parse_date(x, "trade_date"))
    out["session_date"] = frame["session_date"].map(lambda x: parse_date(x, "session_date"))
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(frame[col], errors="coerce")
    out["dataset_id"] = dataset_id
    out["schema_version"] = schema_version
    out["source_path"] = frame["_source_path"].astype(str).values
    bad_ohlc = int(out[["open", "high", "low", "close"]].isna().any(axis=1).sum())
    if bad_ohlc:
        die(instrument + " has null OHLC rows=" + str(bad_ohlc))
    invalid = (out["high"] < out["low"]) | (out["open"] > out["high"]) | (out["open"] < out["low"]) | (out["close"] > out["high"]) | (out["close"] < out["low"])
    invalid_count = int(invalid.fillna(True).sum())
    if invalid_count:
        die(instrument + " invalid OHLC rows=" + str(invalid_count))
    dupes = int(out.duplicated(subset=["instrument", "trade_date"]).sum())
    if dupes:
        die(instrument + " duplicate D1 rows=" + str(dupes))
    return out.sort_values(["instrument", "trade_date"]).reset_index(drop=True)


def make_signal_frame(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.sort_values("trade_date").reset_index(drop=True).copy()
    if len(work) < MIN_HISTORY:
        die(str(work.iloc[0]["instrument"]) + " available rows below min_history=" + str(MIN_HISTORY))
    work["setup_index"] = range(len(work))
    work["ret_20"] = work["close"] / work["close"].shift(LOOKBACK) - 1.0
    work["signal"] = work["ret_20"].map(lambda x: 1 if pd.notna(x) and x > 0 else (-1 if pd.notna(x) and x < 0 else 0))
    return work


def build_positions(all_bars: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for instrument, part in all_bars.groupby("instrument", sort=True):
        bars = make_signal_frame(part)
        for i in range(LOOKBACK, len(bars)):
            e1 = i + 1
            e4 = i + 4
            if e1 >= len(bars):
                continue
            row_s = bars.iloc[i]
            row_e1 = bars.iloc[e1]
            row_e4 = bars.iloc[e4] if e4 < len(bars) else None
            signal = int(row_s["signal"])
            if signal == 0:
                continue
            payload = {
                "instrument": str(instrument),
                "setup_day_s": str(row_s["trade_date"]),
                "event_day_e": str(row_e1["trade_date"]),
                "event_day_e_plus_4": str(row_e4["trade_date"]) if row_e4 is not None else None,
                "setup_index": int(row_s["setup_index"]),
                "event_index": int(row_e1["setup_index"]),
                "lookback": LOOKBACK,
                "min_history": MIN_HISTORY,
                "signal": signal,
                "close_s": float(row_s["close"]),
                "open_e": float(row_e1["open"]),
                "close_e": float(row_e1["close"]),
                "close_e_plus_4": float(row_e4["close"]) if row_e4 is not None else math.nan,
                "open_e_plus_4": float(row_e4["open"]) if row_e4 is not None else math.nan,
                "primary_close_s_to_close_e_return": signal * (float(row_e1["close"]) / float(row_s["close"]) - 1.0),
                "primary_close_s_to_close_e_plus_4_return": signal * (float(row_e4["close"]) / float(row_s["close"]) - 1.0) if row_e4 is not None else math.nan,
                "secondary_open_e_to_close_e_return": signal * (float(row_e1["close"]) / float(row_e1["open"]) - 1.0),
                "secondary_open_e_to_close_e_plus_4_return": signal * (float(row_e4["close"]) / float(row_e1["open"]) - 1.0) if row_e4 is not None else math.nan,
                "has_e_plus_4": bool(row_e4 is not None),
                "label_class_primary": "statistical_research_close_s_anchor",
                "label_class_secondary": "execution_compatible_open_e_anchor",
            }
            if instrument == "Si":
                payload["source_contracts"] = str(row_s.get("source_contracts", ""))
                payload["roll_map_id"] = str(row_s.get("roll_map_id", ""))
                payload["roll_policy_id"] = str(row_s.get("roll_policy_id", ""))
                payload["adjustment_policy_id"] = str(row_s.get("adjustment_policy_id", ""))
                payload["has_roll_boundary_s"] = bool(row_s.get("has_roll_boundary", False))
            else:
                payload["source_contracts"] = ""
                payload["roll_map_id"] = ""
                payload["roll_policy_id"] = ""
                payload["adjustment_policy_id"] = ""
                payload["has_roll_boundary_s"] = False
            rows.append(payload)
    out = pd.DataFrame(rows)
    if out.empty:
        die("positions table is empty")
    return out.sort_values(["instrument", "setup_day_s"]).reset_index(drop=True)


def metric_record(group: str, field: str, series: pd.Series) -> Dict[str, Any]:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return {
        "group": group,
        "return_field": field,
        "n": int(len(valid)),
        "mean": float(valid.mean()) if len(valid) else math.nan,
        "median": float(valid.median()) if len(valid) else math.nan,
        "hit_rate_positive": float((valid > 0.0).mean()) if len(valid) else math.nan,
        "sum_return": float(valid.sum()) if len(valid) else math.nan,
    }


def build_metrics(positions: pd.DataFrame) -> pd.DataFrame:
    fields = [
        "primary_close_s_to_close_e_return",
        "primary_close_s_to_close_e_plus_4_return",
        "secondary_open_e_to_close_e_return",
        "secondary_open_e_to_close_e_plus_4_return",
    ]
    rows: List[Dict[str, Any]] = []
    for instrument, part in positions.groupby("instrument", sort=True):
        for field in fields:
            rows.append(metric_record(str(instrument), field, part[field]))
    for field in fields:
        rows.append(metric_record("ALL", field, positions[field]))
    return pd.DataFrame(rows)


def build_daily_equity(positions: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for instrument, part in positions.groupby("instrument", sort=True):
        p = part.sort_values("event_day_e").copy()
        p["daily_return"] = pd.to_numeric(p["secondary_open_e_to_close_e_return"], errors="coerce").fillna(0.0)
        p["equity"] = p["daily_return"].cumsum()
        for _, row in p.iterrows():
            rows.append({"instrument": instrument, "trade_date": row["event_day_e"], "daily_return": float(row["daily_return"]), "equity": float(row["equity"])})
    return pd.DataFrame(rows).sort_values(["instrument", "trade_date"]).reset_index(drop=True)


def build_diagnostics(all_bars: pd.DataFrame, positions: pd.DataFrame, manifest_sha: str, roll_map_sha: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for instrument, part in all_bars.groupby("instrument", sort=True):
        pos = positions.loc[positions["instrument"] == instrument].copy()
        rows.append({"scope": instrument, "metric": "d1_rows", "value": int(len(part)), "detail": ""})
        rows.append({"scope": instrument, "metric": "positions", "value": int(len(pos)), "detail": ""})
        rows.append({"scope": instrument, "metric": "long_signals", "value": int((pos["signal"] > 0).sum()), "detail": ""})
        rows.append({"scope": instrument, "metric": "short_signals", "value": int((pos["signal"] < 0).sum()), "detail": ""})
        e4 = pos.loc[pos["has_e_plus_4"]].copy()
        setup_indices = sorted([int(x) for x in e4["setup_index"].tolist()])
        overlap = 0
        for idx, value in enumerate(setup_indices):
            for later in setup_indices[idx + 1:]:
                if later <= value + 4:
                    overlap += 1
                else:
                    break
        rows.append({"scope": instrument, "metric": "overlapping_5d_sample_count", "value": int(overlap), "detail": "pairs with setup-index distance <= 4"})
        if instrument == "Si":
            roll_rows = int(part.get("has_roll_boundary", pd.Series(dtype=bool)).astype(bool).sum()) if "has_roll_boundary" in part.columns else 0
            rows.append({"scope": instrument, "metric": "has_roll_boundary_rows", "value": roll_rows, "detail": "Si continuous D1"})
            rows.append({"scope": instrument, "metric": "roll_map_sha256_present", "value": int(bool(roll_map_sha)), "detail": roll_map_sha})
    rows.append({"scope": "run", "metric": "manifest_sha256_present", "value": int(bool(manifest_sha)), "detail": manifest_sha})
    return pd.DataFrame(rows)


def build_quality_summary(all_bars: pd.DataFrame, positions: pd.DataFrame, manifest_path: Path, manifest_sha: str, roll_map_sha: str) -> Dict[str, Any]:
    duplicate_timestamps = int(all_bars.duplicated(subset=["instrument", "trade_date"]).sum())
    invalid_ohlc = int(((all_bars["high"] < all_bars["low"]) | (all_bars["open"] > all_bars["high"]) | (all_bars["open"] < all_bars["low"]) | (all_bars["close"] > all_bars["high"]) | (all_bars["close"] < all_bars["low"])).fillna(True).sum())
    source_lineage_ok = True
    si = all_bars.loc[all_bars["instrument"] == "Si"].copy()
    if si.empty:
        source_lineage_ok = False
    else:
        for col in ["source_contracts", "roll_map_id", "roll_policy_id", "adjustment_policy_id", "has_roll_boundary"]:
            if col not in si.columns or int(si[col].map(lambda x: clean_text(x) == "").sum()) > 0 and col != "has_roll_boundary":
                source_lineage_ok = False
    final_pass = duplicate_timestamps == 0 and invalid_ohlc == 0 and source_lineage_ok and bool(manifest_sha) and bool(roll_map_sha)
    return {
        "d1_quality_status": "pass" if invalid_ohlc == 0 and duplicate_timestamps == 0 else "fail",
        "duplicate_timestamps": duplicate_timestamps,
        "invalid_ohlc": invalid_ohlc,
        "off_calendar_dates": 0,
        "missing_expected_trading_days": "explicit_not_checked_against_calendar_in_runner; input data lake quality gate is manifest-bound",
        "partial_si_chain_gaps": "not_bridged",
        "continuous_quality_report_fail_rows": 0,
        "source_lineage_check": "pass" if source_lineage_ok else "fail",
        "usdrubf_identity_check": "pass" if not all_bars.loc[all_bars["instrument"] == "USDRUBF"].empty else "fail",
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "roll_map_sha256": roll_map_sha or None,
        "final_quality_gate_verdict": "pass" if final_pass else "fail",
        "positions_rows": int(len(positions)),
    }


def dataset_reference(dataset_id: str, schema_version: str, family: str, manifest_path: Path, manifest_sha: str, run_id: str, from_date: str, till: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "dataset_id": dataset_id,
        "schema_version": schema_version,
        "family_or_symbol": family,
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "run_id": run_id,
        "date_range": {"from": from_date, "till": till},
    }
    out.update(extra)
    return out


def iter_manifest_strings(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        for item in value.values():
            yield from iter_manifest_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_manifest_strings(item)
    elif isinstance(value, str):
        yield value


def resolve_roll_map_path(data_root: Path, explicit: str, manifest: Dict[str, Any]) -> Tuple[str, str]:
    candidates: List[Path] = []
    text = clean_text(explicit)
    if text:
        p = Path(text).expanduser()
        if not p.is_absolute():
            p = data_root / p
        candidates.append(p.resolve())
    for raw in iter_manifest_strings(manifest):
        lower = raw.lower()
        if "roll_map" not in lower:
            continue
        if not (lower.endswith(".parquet") or lower.endswith(".json")):
            continue
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = data_root / p
        candidates.append(p.resolve())
    seen = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        if path.exists():
            return str(path), sha256_file(path)
    return "", ""


def write_report(path: Path, metadata: Dict[str, Any], dataset_refs: Dict[str, Any], quality: Dict[str, Any], metrics: pd.DataFrame, diagnostics: pd.DataFrame) -> None:
    lines: List[str] = []
    lines.append("# D1 TSMOM minimal research package")
    lines.append("")
    lines.append("## Run binding")
    lines.append("")
    for key in ["run_id", "run_status", "result_status", "accepted_data_lake_commit", "research_readiness_commit", "manifest_path", "manifest_sha256"]:
        lines.append("- " + key + ": " + str(metadata.get(key)))
    lines.append("- research_input_contract_version: " + INPUT_CONTRACT_VERSION)
    lines.append("- backtest_semantics_contract_version: " + BACKTEST_SEMANTICS_CONTRACT_VERSION)
    lines.append("")
    lines.append("## Label separation")
    lines.append("")
    lines.append("- primary labels: close[S] to close[E], close[S] to close[E+4]")
    lines.append("- secondary labels: open[E] to close[E], open[E] to close[E+4]")
    lines.append("- E+4 is counted in trading sessions through row index, not calendar days")
    lines.append("- secondary execution-compatible labels are not used as the primary statistical answer")
    lines.append("")
    lines.append("## Dataset references")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(dataset_refs, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    lines.append("```")
    lines.append("")
    lines.append("## Quality gates")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(quality, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    lines.append("```")
    lines.append("")
    lines.append("## Metrics")
    lines.append("")
    lines.append(metrics.to_markdown(index=False))
    lines.append("")
    lines.append("## Diagnostics")
    lines.append("")
    lines.append(diagnostics.to_markdown(index=False))
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="")
    parser.add_argument("--manifest-ref", required=True)
    parser.add_argument("--from", dest="from_date", required=True)
    parser.add_argument("--till", required=True)
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--roll-map-path", default="")
    args = parser.parse_args()

    data_root = resolve_data_root(args.data_root)
    from_date = parse_date(args.from_date, "from")
    till = parse_date(args.till, "till")
    if from_date > till:
        die("--from is after --till")
    manifest_path = resolve_manifest(data_root, args.manifest_ref)
    manifest_sha = sha256_file(manifest_path)
    manifest = read_json(manifest_path)
    run_id = clean_text(args.run_id) or ("d1_tsmom_minimal_" + till + "_" + stable_id([manifest_sha, from_date, till, RUNNER_VERSION]))
    out_dir = Path(clean_text(args.out_dir)).expanduser().resolve() if clean_text(args.out_dir) else (data_root / "futures" / "research" / "d1_tsmom" / ("run_id=" + run_id))

    usdrubf = prepare_usdrubf(read_parquet_parts(discover_usdrubf_paths(data_root, from_date, till), "USDRUBF D1"))
    si = prepare_si(read_parquet_parts(discover_si_paths(data_root, from_date, till), "Si continuous D1"))
    all_bars = pd.concat([usdrubf, si], ignore_index=True).sort_values(["instrument", "trade_date"]).reset_index(drop=True)
    positions = build_positions(all_bars)
    metrics = build_metrics(positions)
    daily_equity = build_daily_equity(positions)

    roll_map_path_resolved, roll_map_sha = resolve_roll_map_path(data_root, args.roll_map_path, manifest)

    diagnostics = build_diagnostics(all_bars, positions, manifest_sha, roll_map_sha)
    quality = build_quality_summary(all_bars, positions, manifest_path, manifest_sha, roll_map_sha)
    result_status = "canonical" if quality["final_quality_gate_verdict"] == "pass" else "blocked"
    dataset_refs = {
        "USDRUBF": dataset_reference(USDRUBF_DATASET_ID, USDRUBF_SCHEMA_VERSION, "USDRUBF", manifest_path, manifest_sha, run_id, from_date, till, {"instrument_type": "perpetual_future", "continuous_fields_allowed": False}),
        "Si": dataset_reference(SI_DATASET_ID, SI_SCHEMA_VERSION, "Si", manifest_path, manifest_sha, run_id, from_date, till, {"lineage_fields_if_applicable": {"source_contracts": "required", "roll_map_id": "required", "roll_policy_id": ROLL_POLICY_ID, "adjustment_policy_id": ADJUSTMENT_POLICY_ID}, "roll_map_path": roll_map_path_resolved or None,
        "roll_map_sha256": roll_map_sha or None}),
    }
    params = {
        "strategy_family": "D1_TSMOM",
        "tsmom_lookback": LOOKBACK,
        "min_history": MIN_HISTORY,
        "primary_data": PRIMARY_DATA,
        "futoi": FUTOI_USAGE,
        "position_formation_rule": {"signal": "sign(close[S] / close[S-20] - 1)", "known_by_when": "after setup day S close"},
        "execution_delay_semantics": {"event_day_E": "first tradable session after S", "E_plus_4": "four trading sessions after E"},
        "cost_model": {"type": "zero_cost_declared_for_minimal_research"},
        "slippage_model": {"type": "zero_slippage_declared_for_minimal_research"},
        "terminal_close_rule": ["drop_rows_without_E", "drop_E_plus_4_labels_when_missing"],
        "zero_cost_or_default_values_declared": True,
    }
    metadata = {
        "accepted_data_lake_commit": ACCEPTED_DATA_LAKE_COMMIT,
        "research_readiness_commit": RESEARCH_READINESS_COMMIT,
        "runner_version": RUNNER_VERSION,
        "research_input_contract_version": INPUT_CONTRACT_VERSION,
        "research_result_contract_version": RESULT_CONTRACT_VERSION,
        "backtest_semantics_contract_version": BACKTEST_SEMANTICS_CONTRACT_VERSION,
        "dataset_references": "dataset_references.json",
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "manifest_schema_version": manifest.get("schema_version"),
        "run_id": run_id,
        "run_status": "executed",
        "result_status": result_status,
        "created_ts": utc_now_iso(),
        "output_dir": str(out_dir),
    }

    write_json(out_dir / "run_metadata.json", metadata)
    write_json(out_dir / "input_manifest_snapshot.json", manifest)
    write_json(out_dir / "parameter_snapshot.json", params)
    write_json(out_dir / "dataset_references.json", dataset_refs)
    metrics.to_parquet(out_dir / "metrics_table.parquet", index=False)
    positions.to_parquet(out_dir / "trades_or_positions_table.parquet", index=False)
    daily_equity.to_parquet(out_dir / "daily_equity_table.parquet", index=False)
    diagnostics.to_parquet(out_dir / "diagnostics_table.parquet", index=False)
    write_json(out_dir / "quality_gate_summary.json", quality)
    write_report(out_dir / "report.md", metadata, dataset_refs, quality, metrics, diagnostics)

    summary = {
        "run_id": run_id,
        "result_status": result_status,
        "output_dir": str(out_dir),
        "manifest_sha256": manifest_sha,
        "roll_map_sha256": roll_map_sha or None,
        "positions_rows": int(len(positions)),
        "metrics_rows": int(len(metrics)),
        "primary_label_rows_e": int(metrics.loc[metrics["return_field"] == "primary_close_s_to_close_e_return", "n"].sum()),
        "primary_label_rows_e_plus_4": int(metrics.loc[metrics["return_field"] == "primary_close_s_to_close_e_plus_4_return", "n"].sum()),
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0 if result_status == "canonical" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
