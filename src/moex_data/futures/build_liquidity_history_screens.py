#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

TZ_MSK = ZoneInfo("Europe/Moscow")
SCHEMA_LIQUIDITY = "futures_liquidity_screen.v1"
SCHEMA_HISTORY_DEPTH = "futures_history_depth_screen.v1"
DEFAULT_THRESHOLD_PROFILE_ID = "slice1_initial_review_v1"

CONTRACT_NORMALIZED_REGISTRY = "contracts/datasets/futures_normalized_instrument_registry_contract.md"
CONTRACT_TRADESTATS = "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md"
CONTRACT_FUTOI = "contracts/datasets/futures_futoi_availability_report_contract.md"
CONTRACT_OBSTATS = "contracts/datasets/futures_obstats_availability_report_contract.md"
CONTRACT_HI2 = "contracts/datasets/futures_hi2_availability_report_contract.md"
CONTRACT_LIQUIDITY_SCREEN = "contracts/datasets/futures_liquidity_screen_contract.md"
CONTRACT_HISTORY_DEPTH_SCREEN = "contracts/datasets/futures_history_depth_screen_contract.md"

CONFIG_SLICE1_UNIVERSE = "configs/datasets/futures_slice1_universe_config.json"
CONFIG_LIQUIDITY_THRESHOLDS = "configs/datasets/futures_liquidity_screen_thresholds_config.json"
CONFIG_HISTORY_DEPTH_THRESHOLDS = "configs/datasets/futures_history_depth_thresholds_config.json"

REQUIRED_REPO_FILES = [
    CONTRACT_NORMALIZED_REGISTRY,
    CONTRACT_TRADESTATS,
    CONTRACT_FUTOI,
    CONTRACT_OBSTATS,
    CONTRACT_HI2,
    CONTRACT_LIQUIDITY_SCREEN,
    CONTRACT_HISTORY_DEPTH_SCREEN,
    CONFIG_SLICE1_UNIVERSE,
    CONFIG_LIQUIDITY_THRESHOLDS,
    CONFIG_HISTORY_DEPTH_THRESHOLDS,
]

REQUIRED_INPUT_CONTRACTS = [
    CONTRACT_NORMALIZED_REGISTRY,
    CONTRACT_TRADESTATS,
    CONTRACT_FUTOI,
    CONTRACT_OBSTATS,
    CONTRACT_HI2,
]

AVAILABILITY_CONTRACTS = {
    "algopack_fo_tradestats": CONTRACT_TRADESTATS,
    "moex_futoi": CONTRACT_FUTOI,
    "algopack_fo_obstats": CONTRACT_OBSTATS,
    "algopack_fo_hi2": CONTRACT_HI2,
}


def repo_root() -> Path:
    return Path.cwd().resolve()


def today_msk() -> str:
    return datetime.now(TZ_MSK).date().isoformat()


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("JSON root is not object: " + str(path))
    return data


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def assert_files_exist(root: Path, rel_paths: Iterable[str]) -> None:
    missing = []
    for rel in rel_paths:
        if not (root / rel).exists():
            missing.append(rel)
    if missing:
        raise FileNotFoundError("Missing required SoT files: " + ", ".join(missing))


def extract_contract_value(text: str, key: str) -> str:
    prefix = key + ":"
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def load_contract(root: Path, rel_path: str) -> Dict[str, str]:
    text = read_text(root / rel_path)
    contract = {
        "path_pattern": extract_contract_value(text, "path_pattern"),
        "schema_version": extract_contract_value(text, "schema_version"),
        "format": extract_contract_value(text, "format"),
        "status": extract_contract_value(text, "status"),
    }
    if not contract["path_pattern"]:
        raise RuntimeError("Contract has no path_pattern: " + rel_path)
    if contract["format"] != "parquet":
        raise RuntimeError("Contract format is not parquet: " + rel_path)
    return contract


def resolve_data_root(args: argparse.Namespace) -> Path:
    raw = str(args.data_root or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT is required for external_pattern inputs and outputs")
    return Path(raw).expanduser().resolve()


def resolve_contract_path(pattern: str, data_root: Path, snapshot_date: str) -> Path:
    value = pattern.replace("${MOEX_DATA_ROOT}", str(data_root))
    value = value.replace("{snapshot_date}", snapshot_date)
    if "{" in value or "}" in value or "$" in value:
        raise RuntimeError("Unresolved path_pattern placeholder: " + pattern)
    return Path(value).expanduser().resolve()


def stable_id(parts: Iterable[Any]) -> str:
    raw = "|".join([str(x) for x in parts])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def read_parquet_required(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError("Missing required input artifact for " + label + ": " + str(path))
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        raise RuntimeError("Cannot read parquet for " + label + " at " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc
    if frame.empty:
        raise RuntimeError("Required input artifact is empty for " + label + ": " + str(path))
    return frame


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except Exception as exc:
        raise RuntimeError("Cannot write parquet " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc


def first_threshold_profile(config: Dict[str, Any], default_id: str) -> Dict[str, Any]:
    profiles = config.get("threshold_profiles") or []
    if not isinstance(profiles, list) or not profiles:
        raise RuntimeError("threshold_profiles is missing or empty")
    for item in profiles:
        if isinstance(item, dict) and str(item.get("threshold_profile_id", "")).strip() == default_id:
            return item
    first = profiles[0]
    if not isinstance(first, dict):
        raise RuntimeError("threshold profile is not object")
    return first


def normalize_key_columns(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    out = frame.copy()
    required = ["snapshot_date", "board", "secid", "family_code"]
    missing = [x for x in required if x not in out.columns]
    if missing:
        raise RuntimeError(label + " missing required columns: " + ", ".join(missing))
    out["board"] = out["board"].astype(str).str.strip().replace({"": "rfud"})
    out["secid"] = out["secid"].astype(str).str.strip()
    out["family_code"] = out["family_code"].astype(str).str.strip()
    return out


def status_for_endpoint(report: pd.DataFrame, endpoint_id: str) -> pd.DataFrame:
    frame = normalize_key_columns(report, endpoint_id)
    if "endpoint_id" in frame.columns:
        frame = frame.loc[frame["endpoint_id"].astype(str) == endpoint_id].copy()
    if frame.empty:
        raise RuntimeError("Availability report has no rows for endpoint_id=" + endpoint_id)
    key_cols = ["snapshot_date", "board", "secid"]
    duplicated = frame.duplicated(key_cols, keep=False)
    if duplicated.any():
        dup = frame.loc[duplicated, key_cols].drop_duplicates().to_dict("records")
        raise RuntimeError("Duplicate availability keys for " + endpoint_id + ": " + json.dumps(dup, ensure_ascii=False, default=str))
    return frame


def select_screen_universe(tradestats: pd.DataFrame, normalized_registry: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    tradestats = normalize_key_columns(tradestats, "tradestats")
    selected = tradestats.loc[tradestats["snapshot_date"].astype(str) == snapshot_date].copy()
    if selected.empty:
        raise RuntimeError("No tradestats availability rows for snapshot_date=" + snapshot_date)
    cols = ["snapshot_date", "board", "secid", "family_code"]
    selected = selected[cols].drop_duplicates().copy()
    registry = normalize_key_columns(normalized_registry, "normalized_registry")
    registry_cols = [x for x in ["snapshot_date", "board", "secid", "asset_class", "instrument_kind", "expiration_date", "last_trade_date"] if x in registry.columns]
    selected = selected.merge(registry[registry_cols].drop_duplicates(), on=["snapshot_date", "board", "secid"], how="left")
    if "asset_class" not in selected.columns:
        selected["asset_class"] = None
    if "instrument_kind" not in selected.columns:
        selected["instrument_kind"] = None
    return selected.sort_values(["family_code", "secid", "board"]).reset_index(drop=True)


def ensure_same_universe(universe: pd.DataFrame, reports: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
    checks: Dict[str, Dict[str, Any]] = {}
    universe_keys = set([(str(r.board), str(r.secid)) for r in universe[["board", "secid"]].itertuples(index=False)])
    for endpoint_id, report in reports.items():
        keys = set([(str(r.board), str(r.secid)) for r in report[["board", "secid"]].itertuples(index=False)])
        missing = sorted([{"board": board, "secid": secid} for board, secid in universe_keys - keys], key=lambda x: (x["board"], x["secid"]))
        extra = sorted([{"board": board, "secid": secid} for board, secid in keys - universe_keys], key=lambda x: (x["board"], x["secid"]))
        checks[endpoint_id] = {"missing": missing, "extra": extra}
        if missing:
            raise RuntimeError("Endpoint " + endpoint_id + " is missing selected instruments: " + json.dumps(missing, ensure_ascii=False))
    return checks


def parse_int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return int(value)
    except Exception:
        return None


def date_part(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    return text[:10]


def availability_lookup(report: pd.DataFrame) -> Dict[Tuple[str, str], Dict[str, Any]]:
    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for _, row in report.iterrows():
        board = str(row.get("board", "rfud") or "rfud").strip()
        secid = str(row.get("secid", "")).strip()
        lookup[(board, secid)] = row.to_dict()
    return lookup


def endpoint_status_bundle(reports: Dict[str, pd.DataFrame], board: str, secid: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for endpoint_id, report in reports.items():
        lookup = availability_lookup(report)
        row = lookup.get((board, secid), {})
        out[endpoint_id] = str(row.get("availability_status", "missing") or "missing")
    return out


def all_required_available(statuses: Dict[str, str]) -> bool:
    return all(str(value) == "available" for value in statuses.values())


def build_liquidity_screen(universe: pd.DataFrame, reports: Dict[str, pd.DataFrame], profile: Dict[str, Any]) -> pd.DataFrame:
    tradestats_lookup = availability_lookup(reports["algopack_fo_tradestats"])
    profile_id = str(profile.get("threshold_profile_id") or DEFAULT_THRESHOLD_PROFILE_ID)
    rows = []
    for _, item in universe.iterrows():
        board = str(item.get("board", "rfud") or "rfud").strip()
        secid = str(item.get("secid", "")).strip()
        snapshot_date = str(item.get("snapshot_date", "")).strip()
        family = str(item.get("family_code", "")).strip()
        statuses = endpoint_status_bundle(reports, board, secid)
        tradestats = tradestats_lookup.get((board, secid), {})
        observed_rows = parse_int_or_none(tradestats.get("observed_rows"))
        screen_from = str(tradestats.get("probe_from", "") or "")
        screen_till = str(tradestats.get("probe_till", "") or "")
        if all_required_available(statuses) and observed_rows and observed_rows > 0:
            liquidity_status = "review_required"
            review_status = "manual_review_required"
            review_notes = "ALGOPACK availability is present, but accepted availability artifacts do not contain daily volume/value/trades distributions. Loader approval remains blocked pending PM review or richer liquidity metrics."
        else:
            liquidity_status = "fail"
            review_status = "blocked"
            review_notes = "One or more required availability endpoints are missing or unavailable: " + json.dumps(statuses, ensure_ascii=False, sort_keys=True)
        rows.append({
            "liquidity_screen_id": stable_id(["liquidity", snapshot_date, board, secid, screen_from, screen_till, profile_id]),
            "snapshot_date": snapshot_date,
            "board": board,
            "secid": secid,
            "family_code": family,
            "asset_class": item.get("asset_class", None),
            "screen_from": screen_from or None,
            "screen_till": screen_till or None,
            "liquidity_status": liquidity_status,
            "schema_version": SCHEMA_LIQUIDITY,
            "median_daily_volume": None,
            "median_daily_value": None,
            "median_daily_trades": None,
            "active_days": None,
            "missing_days": None,
            "threshold_profile_id": profile_id,
            "review_notes": review_notes,
            "validation_status": "completed",
            "review_status": review_status,
        })
    return pd.DataFrame(rows)


def build_history_depth_screen(universe: pd.DataFrame, reports: Dict[str, pd.DataFrame], profile: Dict[str, Any]) -> pd.DataFrame:
    tradestats_lookup = availability_lookup(reports["algopack_fo_tradestats"])
    profile_id = str(profile.get("threshold_profile_id") or DEFAULT_THRESHOLD_PROFILE_ID)
    required_min = parse_int_or_none(profile.get("min_available_trading_days"))
    rows = []
    for _, item in universe.iterrows():
        board = str(item.get("board", "rfud") or "rfud").strip()
        secid = str(item.get("secid", "")).strip()
        snapshot_date = str(item.get("snapshot_date", "")).strip()
        family = str(item.get("family_code", "")).strip()
        statuses = endpoint_status_bundle(reports, board, secid)
        tradestats = tradestats_lookup.get((board, secid), {})
        observed_rows = parse_int_or_none(tradestats.get("observed_rows"))
        screen_from = str(tradestats.get("probe_from", "") or "")
        screen_till = str(tradestats.get("probe_till", "") or "")
        first_date = date_part(tradestats.get("first_available_date")) or date_part(tradestats.get("observed_min_ts"))
        last_date = date_part(tradestats.get("last_available_date")) or date_part(tradestats.get("observed_max_ts"))
        if all_required_available(statuses) and observed_rows and observed_rows > 0:
            history_status = "review_required"
            review_status = "manual_review_required"
            review_notes = "Availability artifacts show recent data presence, but they do not prove full history depth or available trading-day count. Loader approval remains blocked pending PM review or dedicated history-depth backfill probe."
        else:
            history_status = "fail"
            review_status = "blocked"
            review_notes = "One or more required availability endpoints are missing or unavailable: " + json.dumps(statuses, ensure_ascii=False, sort_keys=True)
        rows.append({
            "history_depth_screen_id": stable_id(["history_depth", snapshot_date, board, secid, screen_from, screen_till, profile_id]),
            "snapshot_date": snapshot_date,
            "board": board,
            "secid": secid,
            "family_code": family,
            "screen_from": screen_from or None,
            "screen_till": screen_till or None,
            "history_depth_status": history_status,
            "schema_version": SCHEMA_HISTORY_DEPTH,
            "first_available_date": first_date,
            "last_available_date": last_date,
            "available_trading_days": None,
            "required_min_trading_days": required_min,
            "missing_days": None,
            "threshold_profile_id": profile_id,
            "review_notes": review_notes,
            "validation_status": "completed",
            "review_status": review_status,
        })
    return pd.DataFrame(rows)


def validate_primary_key(df: pd.DataFrame, cols: List[str], label: str) -> None:
    missing = [x for x in cols if x not in df.columns]
    if missing:
        raise RuntimeError(label + " missing primary key columns: " + ", ".join(missing))
    duplicated = df.duplicated(cols, keep=False)
    if duplicated.any():
        dup = df.loc[duplicated, cols].drop_duplicates().to_dict("records")
        raise RuntimeError(label + " duplicate primary key: " + json.dumps(dup, ensure_ascii=False, default=str))


def summarize_screen(df: pd.DataFrame, status_col: str) -> Dict[str, Any]:
    counts = df[status_col].astype(str).value_counts(dropna=False).to_dict() if status_col in df.columns else {}
    by_family: Dict[str, Dict[str, int]] = {}
    if "family_code" in df.columns and status_col in df.columns:
        for family, sub in df.groupby("family_code"):
            by_family[str(family)] = {str(k): int(v) for k, v in sub[status_col].astype(str).value_counts(dropna=False).to_dict().items()}
    return {
        "rows": int(len(df)),
        "status_counts": {str(k): int(v) for k, v in counts.items()},
        "by_family": by_family,
    }


def print_json_line(key: str, value: Any) -> None:
    print(key + ": " + json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    args = parser.parse_args()

    root = repo_root()
    snapshot_date = str(args.snapshot_date).strip()
    data_root = resolve_data_root(args)

    assert_files_exist(root, REQUIRED_REPO_FILES)
    contracts = {rel: load_contract(root, rel) for rel in REQUIRED_REPO_FILES if rel.startswith("contracts/")}

    liquidity_profile = first_threshold_profile(read_json(root / CONFIG_LIQUIDITY_THRESHOLDS), DEFAULT_THRESHOLD_PROFILE_ID)
    history_profile = first_threshold_profile(read_json(root / CONFIG_HISTORY_DEPTH_THRESHOLDS), DEFAULT_THRESHOLD_PROFILE_ID)
    read_json(root / CONFIG_SLICE1_UNIVERSE)

    input_paths = {rel: resolve_contract_path(contracts[rel]["path_pattern"], data_root, snapshot_date) for rel in REQUIRED_INPUT_CONTRACTS}
    output_paths = {
        "futures_liquidity_screen": resolve_contract_path(contracts[CONTRACT_LIQUIDITY_SCREEN]["path_pattern"], data_root, snapshot_date),
        "futures_history_depth_screen": resolve_contract_path(contracts[CONTRACT_HISTORY_DEPTH_SCREEN]["path_pattern"], data_root, snapshot_date),
    }

    normalized_registry = read_parquet_required(input_paths[CONTRACT_NORMALIZED_REGISTRY], "normalized_registry")
    reports: Dict[str, pd.DataFrame] = {}
    for endpoint_id, rel in AVAILABILITY_CONTRACTS.items():
        reports[endpoint_id] = status_for_endpoint(read_parquet_required(input_paths[rel], endpoint_id), endpoint_id)

    tradestats_for_snapshot = reports["algopack_fo_tradestats"].loc[reports["algopack_fo_tradestats"]["snapshot_date"].astype(str) == snapshot_date].copy()
    universe = select_screen_universe(tradestats_for_snapshot, normalized_registry, snapshot_date)
    universe_checks = ensure_same_universe(universe, reports)

    liquidity = build_liquidity_screen(universe, reports, liquidity_profile)
    history = build_history_depth_screen(universe, reports, history_profile)

    validate_primary_key(liquidity, ["liquidity_screen_id", "snapshot_date", "board", "secid"], "liquidity_screen")
    validate_primary_key(history, ["history_depth_screen_id", "snapshot_date", "board", "secid"], "history_depth_screen")

    write_parquet(liquidity, output_paths["futures_liquidity_screen"])
    write_parquet(history, output_paths["futures_history_depth_screen"])

    selected = universe[["family_code", "secid", "board"]].drop_duplicates().sort_values(["family_code", "secid", "board"]).to_dict("records")
    print_json_line("input_artifacts_read", {str(k): str(v) for k, v in input_paths.items()})
    print_json_line("output_artifacts_created", {str(k): str(v) for k, v in output_paths.items()})
    print_json_line("liquidity_screen_summary", summarize_screen(liquidity, "liquidity_status"))
    print_json_line("history_depth_screen_summary", summarize_screen(history, "history_depth_status"))
    print_json_line("selected_instruments_covered", selected)
    print_json_line("availability_universe_checks", universe_checks)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
