#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import DEFAULT_WHITELIST
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

SCHEMA_MANIFEST = "futures_continuous_builder_manifest.v1"
SCHEMA_QUALITY_REPORT = "futures_continuous_quality_report.v1"
SCHEMA_NORMALIZED_REGISTRY = "futures_normalized_instrument_registry.v1"
SCHEMA_EXPIRATION_MAP = "futures_expiration_map.v1"
SCHEMA_ROLL_MAP = "futures_continuous_roll_map.v1"
SCHEMA_CONTINUOUS_5M = "futures_continuous_5m.v1"
SCHEMA_CONTINUOUS_D1 = "futures_continuous_d1.v1"

DATASET_ROLL_MAP = "futures_continuous_roll_map"
DATASET_CONTINUOUS_5M = "futures_continuous_5m"
DATASET_CONTINUOUS_D1 = "futures_continuous_d1"
DATASET_CONTINUOUS_SERIES = "futures_continuous_series_v1"

CONTRACT_NORMALIZED_REGISTRY = "contracts/datasets/futures_normalized_instrument_registry_contract.md"
CONTRACT_EXPIRATION_MAP = "contracts/datasets/futures_expiration_map_contract.md"
CONTRACT_ROLL_MAP = "contracts/datasets/futures_continuous_roll_map_contract.md"
CONTRACT_CONTINUOUS_5M = "contracts/datasets/futures_continuous_5m_contract.md"
CONTRACT_CONTINUOUS_D1 = "contracts/datasets/futures_continuous_d1_contract.md"
CONTRACT_MANIFEST = "contracts/datasets/futures_continuous_builder_manifest_contract.md"
CONTRACT_QUALITY_REPORT = "contracts/datasets/futures_continuous_quality_report_contract.md"
CONTRACT_RAW_5M = "contracts/datasets/futures_raw_5m_contract.md"
CONTRACT_DERIVED_D1 = "contracts/datasets/futures_derived_d1_ohlcv_contract.md"

REQUIRED_CONTRACTS = [
    CONTRACT_NORMALIZED_REGISTRY,
    CONTRACT_EXPIRATION_MAP,
    CONTRACT_ROLL_MAP,
    CONTRACT_CONTINUOUS_5M,
    CONTRACT_CONTINUOUS_D1,
    CONTRACT_MANIFEST,
    CONTRACT_QUALITY_REPORT,
    CONTRACT_RAW_5M,
    CONTRACT_DERIVED_D1,
]

ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"
ADJUSTMENT_FACTOR = 1.0
CALENDAR_STATUS = "canonical_apim_futures_xml"
EXPECTED_CONTINUOUS_SYMBOLS = ["Si", "USDRUBF"]

REQUIRED_QUALITY_CHECKS = [
    "missing_normalized_registry",
    "missing_expiration_anchor",
    "unresolved_decision_source",
    "invalid_calendar_status",
    "duplicate_timestamps",
    "missing_raw_source_partitions",
    "ambiguous_active_source_contract",
    "overlapping_roll_map_windows",
    "missing_next_contract",
    "explicit_partial_chain_gap_for_excluded_SiH7_SiM7",
    "usdrubf_identity_validation",
    "adjustment_factor_not_1",
    "unexpected_included_instruments",
    "excluded_instruments_included",
    "continuous_output_row_source_lineage_completeness",
]

EXTRA_QUALITY_CHECKS = [
    "missing_expiration_map_artifact",
    "missing_roll_map_artifact",
    "missing_continuous_5m_artifact",
    "missing_continuous_d1_artifact",
    "duplicate_d1_rows",
    "missing_source_contracts_in_d1",
    "ohlc_validity",
]


def repo_root() -> Path:
    return Path.cwd().resolve()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def contract_value(root: Path, rel_path: str, key: str) -> str:
    prefix = key + ":"
    for raw in read_text(root / rel_path).splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def contract_path_pattern(root: Path, rel_path: str) -> str:
    pattern = contract_value(root, rel_path, "path_pattern")
    if not pattern:
        raise RuntimeError("Contract path_pattern is missing: " + rel_path)
    prefix = "${MOEX_DATA_ROOT}"
    if not pattern.startswith(prefix):
        raise RuntimeError("Unsupported non-MOEX_DATA_ROOT path_pattern: " + rel_path)
    return pattern


def contract_tail(root: Path, rel_path: str) -> str:
    pattern = contract_path_pattern(root, rel_path)
    return pattern[len("${MOEX_DATA_ROOT}"):].lstrip("/")


def replace_tokens(tail: str, replacements: Dict[str, str]) -> str:
    out = tail
    for key, value in replacements.items():
        out = out.replace("{" + key + "}", str(value))
    out = out.replace("YYYY-MM-DD", str(replacements.get("trade_date", replacements.get("snapshot_date", replacements.get("run_date", "")))))
    return out


def resolve_contract_path(root: Path, data_root: Path, rel_path: str, replacements: Dict[str, str]) -> Path:
    tail = replace_tokens(contract_tail(root, rel_path), replacements)
    unresolved = [part for part in tail.split("/") if "{" in part or "}" in part]
    if unresolved:
        raise RuntimeError("Unresolved path pattern tokens in " + rel_path + ": " + ",".join(unresolved))
    return data_root / tail


def glob_contract_paths(root: Path, data_root: Path, rel_path: str, replacements: Dict[str, str]) -> List[Path]:
    tail = contract_tail(root, rel_path)
    tokenized = replace_tokens(tail, replacements)
    tokenized = tokenized.replace("{family_code}", "*")
    tokenized = tokenized.replace("{trade_date}", "*")
    tokenized = tokenized.replace("{secid}", "*")
    tokenized = tokenized.replace("YYYY-MM-DD", "*")
    unresolved = [part for part in tokenized.split("/") if "{" in part or "}" in part]
    if unresolved:
        raise RuntimeError("Unresolved glob tokens in " + rel_path + ": " + ",".join(unresolved))
    return sorted(data_root.glob(tokenized))


def partition_value(path: Path, key: str) -> str:
    prefix = key + "="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix):]
    return ""


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none", "null"}:
        return None
    return text


def clean_date(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    try:
        return pd.to_datetime(text, errors="raise").date().isoformat()
    except Exception:
        return text[:10] if len(text) >= 10 else None


def bool_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def json_value(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def parse_source_contracts(value: Any) -> List[str]:
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


def read_parquet_if_exists(path: Path) -> Tuple[pd.DataFrame, bool]:
    if not path.exists():
        return pd.DataFrame(), False
    return pd.read_parquet(path), True


def read_partitions(paths: List[Path]) -> pd.DataFrame:
    frames = []
    for path in paths:
        part = pd.read_parquet(path)
        part["_source_partition_path"] = str(path)
        frames.append(part)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def path_root_from_contract(root: Path, data_root: Path, rel_path: str, replacements: Dict[str, str], stop_tokens: Iterable[str]) -> Path:
    tail = contract_tail(root, rel_path)
    for token in stop_tokens:
        marker = "/" + token + "={"
        pos = tail.find(marker)
        if pos >= 0:
            tail = tail[:pos]
            break
    tail = replace_tokens(tail, replacements)
    return data_root / tail


def normalize_dates(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in columns:
        if col in out.columns:
            out[col] = out[col].map(clean_date)
    return out


def check_ohlc(frame: pd.DataFrame) -> int:
    required = ["open", "high", "low", "close"]
    if frame.empty or any(col not in frame.columns for col in required):
        return 0 if frame.empty else len(frame)
    work = frame.copy()
    for col in required:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    invalid = (
        work[required].isna().any(axis=1)
        | (work["high"] < work["low"])
        | (work["open"] > work["high"])
        | (work["open"] < work["low"])
        | (work["close"] > work["high"])
        | (work["close"] < work["low"])
    )
    return int(invalid.sum())


def status_row(
    run_id: str,
    run_date: str,
    snapshot_date: str,
    family_code: str,
    continuous_symbol: str,
    check_id: str,
    dataset_id: str,
    check_status: str,
    affected_source_secid: Optional[str] = None,
    affected_trade_date: Optional[str] = None,
    observed_value: Optional[Any] = None,
    expected_value: Optional[Any] = None,
    review_notes: Optional[str] = None,
) -> Dict[str, Any]:
    quality_report_id = "futures_continuous_quality_report_" + stable_id([
        run_id,
        family_code,
        continuous_symbol,
        check_id,
        affected_source_secid or "",
        affected_trade_date or "",
    ])
    return {
        "quality_report_id": quality_report_id,
        "run_id": run_id,
        "run_date": run_date,
        "snapshot_date": snapshot_date,
        "family_code": family_code,
        "continuous_symbol": continuous_symbol,
        "check_id": check_id,
        "dataset_id": dataset_id,
        "schema_version": SCHEMA_QUALITY_REPORT,
        "roll_policy_id": ROLL_POLICY_ID,
        "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
        "calendar_status": CALENDAR_STATUS if check_status != "fail" else CALENDAR_STATUS,
        "check_status": check_status,
        "affected_source_secid": affected_source_secid,
        "affected_trade_date": affected_trade_date,
        "observed_value": None if observed_value is None else str(observed_value),
        "expected_value": None if expected_value is None else str(expected_value),
        "review_notes": review_notes,
    }


def observed_families(roll_map: pd.DataFrame, continuous_5m: pd.DataFrame, continuous_d1: pd.DataFrame) -> List[str]:
    values: List[str] = []
    for frame in [roll_map, continuous_5m, continuous_d1]:
        if "family_code" in frame.columns:
            values.extend([str(x) for x in frame["family_code"].dropna().astype(str).tolist()])
    out = sorted(set(values))
    return out if out else ["Si", "USDRUBF"]


def family_symbol(family_code: str) -> str:
    return "USDRUBF" if family_code.upper() == "USDRUBF" else family_code


def roll_map_overlap_count(roll_map: pd.DataFrame) -> int:
    if roll_map.empty:
        return 0
    required = ["family_code", "continuous_symbol", "source_secid", "valid_from_session", "valid_through_session", "is_perpetual"]
    if any(col not in roll_map.columns for col in required):
        return 1
    frame = normalize_dates(roll_map, ["valid_from_session", "valid_through_session"]).copy()
    ordinary = frame.loc[~frame["is_perpetual"].map(bool_value)].copy()
    count = 0
    for _, group in ordinary.groupby(["family_code", "continuous_symbol"], sort=True):
        ordered = group.sort_values(["valid_from_session", "source_secid"]).reset_index(drop=True)
        previous_through = None
        for _, row in ordered.iterrows():
            valid_from = clean_date(row.get("valid_from_session"))
            valid_through = clean_date(row.get("valid_through_session"))
            if previous_through and valid_from and valid_from <= previous_through:
                count += 1
            if valid_through:
                previous_through = valid_through
    return count


def raw_partition_missing_count(root: Path, data_root: Path, continuous_5m: pd.DataFrame) -> int:
    if continuous_5m.empty:
        return 0
    required = ["trade_date", "family_code", "source_secid"]
    if any(col not in continuous_5m.columns for col in required):
        return len(continuous_5m)
    missing = 0
    keys = continuous_5m[required].drop_duplicates().copy()
    for _, row in keys.iterrows():
        path = resolve_contract_path(
            root,
            data_root,
            CONTRACT_RAW_5M,
            {
                "trade_date": str(row.get("trade_date")),
                "family_code": str(row.get("family_code")),
                "secid": str(row.get("source_secid")),
            },
        )
        if not path.exists():
            missing += 1
    return missing


def add_quality_rows(
    rows: List[Dict[str, Any]],
    run_id: str,
    run_date: str,
    snapshot_date: str,
    families: List[str],
    check_id: str,
    dataset_id: str,
    status_by_family: Dict[str, Tuple[str, Any, Any, Optional[str]]],
) -> None:
    for family in families:
        status, observed, expected, notes = status_by_family.get(family, ("not_applicable", None, None, None))
        rows.append(status_row(
            run_id,
            run_date,
            snapshot_date,
            family,
            family_symbol(family),
            check_id,
            dataset_id,
            status,
            observed_value=observed,
            expected_value=expected,
            review_notes=notes,
        ))


def build_quality_rows(
    root: Path,
    data_root: Path,
    run_id: str,
    run_date: str,
    snapshot_date: str,
    whitelist: List[str],
    excluded: List[str],
    normalized_path: Path,
    expiration_path: Path,
    roll_map_path: Path,
    continuous_5m_paths: List[Path],
    continuous_d1_paths: List[Path],
    normalized_exists: bool,
    expiration_exists: bool,
    roll_map_exists: bool,
    continuous_5m: pd.DataFrame,
    continuous_d1: pd.DataFrame,
    roll_map: pd.DataFrame,
    expiration_map: pd.DataFrame,
) -> List[Dict[str, Any]]:
    families = observed_families(roll_map, continuous_5m, continuous_d1)
    if "Si" not in families:
        families.append("Si")
    if "USDRUBF" not in families:
        families.append("USDRUBF")
    families = sorted(set(families))

    rows: List[Dict[str, Any]] = []
    excluded_upper = {x.upper() for x in excluded}
    whitelist_upper = {x.upper() for x in whitelist}

    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "missing_normalized_registry", DATASET_CONTINUOUS_SERIES, {
        f: ("pass" if normalized_exists else "fail", str(normalized_path), "existing normalized registry artifact", None) for f in families
    })
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "missing_expiration_map_artifact", DATASET_CONTINUOUS_SERIES, {
        f: ("pass" if expiration_exists else "fail", str(expiration_path), "existing expiration map artifact", None) for f in families
    })
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "missing_roll_map_artifact", DATASET_ROLL_MAP, {
        f: ("pass" if roll_map_exists else "fail", str(roll_map_path), "existing roll map artifact", None) for f in families
    })
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "missing_continuous_5m_artifact", DATASET_CONTINUOUS_5M, {
        f: ("pass" if len(continuous_5m_paths) > 0 else "fail", len(continuous_5m_paths), "at least one continuous 5m partition", None) for f in families
    })
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "missing_continuous_d1_artifact", DATASET_CONTINUOUS_D1, {
        f: ("pass" if len(continuous_d1_paths) > 0 else "fail", len(continuous_d1_paths), "at least one continuous D1 partition", None) for f in families
    })

    if expiration_map.empty:
        missing_anchor = {f: ("fail", "expiration_map_empty_or_missing", "ordinary rows have expiration anchor", None) for f in families if f != "USDRUBF"}
    else:
        missing_anchor_count = 0
        if "is_perpetual" in expiration_map.columns:
            ordinary = expiration_map.loc[~expiration_map["is_perpetual"].map(bool_value)].copy()
        else:
            ordinary = expiration_map.copy()
        scoped = ordinary.loc[ordinary.get("secid", pd.Series(dtype=str)).astype(str).str.upper().isin({x.upper() for x in whitelist if x.upper() != "USDRUBF"})].copy() if "secid" in ordinary.columns else ordinary
        if not scoped.empty:
            for _, row in scoped.iterrows():
                source = clean_text(row.get("decision_source"))
                expiration_date = clean_date(row.get("expiration_date"))
                last_trade_date = clean_date(row.get("last_trade_date"))
                if source == "registry_expiration_date" and expiration_date:
                    continue
                if source == "registry_last_trade_date_fallback" and last_trade_date:
                    continue
                if source == "manual_reviewed_override" and clean_text(row.get("review_notes")):
                    continue
                missing_anchor_count += 1
        missing_anchor = {f: ("pass" if missing_anchor_count == 0 else "fail", missing_anchor_count, 0, None) for f in families if f != "USDRUBF"}
    missing_anchor["USDRUBF"] = ("not_applicable", "perpetual_identity", "ordinary expiration anchor", None)
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "missing_expiration_anchor", DATASET_ROLL_MAP, missing_anchor)

    unresolved = 0
    if "decision_source" in roll_map.columns:
        unresolved = int((roll_map["decision_source"].astype(str) == "unresolved").sum())
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "unresolved_decision_source", DATASET_ROLL_MAP, {
        f: ("pass" if unresolved == 0 else "fail", unresolved, 0, None) for f in families
    })

    invalid_calendar = 0
    if "calendar_status" in roll_map.columns:
        invalid_calendar += int((roll_map["calendar_status"].astype(str) != CALENDAR_STATUS).sum())
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "invalid_calendar_status", DATASET_ROLL_MAP, {
        f: ("pass" if invalid_calendar == 0 else "fail", invalid_calendar, 0, None) for f in families
    })

    duplicate_5m = 0
    if not continuous_5m.empty and all(col in continuous_5m.columns for col in ["continuous_symbol", "trade_date", "end"]):
        duplicate_5m = int(continuous_5m.duplicated(subset=["continuous_symbol", "trade_date", "end"]).sum())
    duplicate_d1 = 0
    if not continuous_d1.empty and all(col in continuous_d1.columns for col in ["continuous_symbol", "trade_date"]):
        duplicate_d1 = int(continuous_d1.duplicated(subset=["continuous_symbol", "trade_date"]).sum())
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "duplicate_timestamps", DATASET_CONTINUOUS_5M, {
        f: ("pass" if duplicate_5m == 0 else "fail", duplicate_5m, 0, None) for f in families
    })
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "duplicate_d1_rows", DATASET_CONTINUOUS_D1, {
        f: ("pass" if duplicate_d1 == 0 else "fail", duplicate_d1, 0, None) for f in families
    })

    missing_raw = raw_partition_missing_count(root, data_root, continuous_5m)
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "missing_raw_source_partitions", DATASET_CONTINUOUS_5M, {
        f: ("pass" if missing_raw == 0 else "fail", missing_raw, 0, "Validated from continuous 5m source_secid/family/trade_date lineage against futures_raw_5m path contract.") for f in families
    })

    ambiguous = 0
    if duplicate_5m > 0:
        ambiguous += duplicate_5m
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "ambiguous_active_source_contract", DATASET_ROLL_MAP, {
        f: ("pass" if ambiguous == 0 else "fail", ambiguous, 0, None) for f in families
    })

    overlap_count = roll_map_overlap_count(roll_map)
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "overlapping_roll_map_windows", DATASET_ROLL_MAP, {
        f: ("pass" if overlap_count == 0 else "fail", overlap_count, 0, None) for f in families
    })

    missing_next_bad = 0
    if "roll_status" in roll_map.columns:
        bad_rows = roll_map.loc[
            (roll_map["roll_status"].astype(str) == "blocked_missing_next_contract")
            & roll_map.get("review_notes", pd.Series([""] * len(roll_map))).map(clean_text).isna()
        ].copy()
        missing_next_bad = int(len(bad_rows))
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "missing_next_contract", DATASET_ROLL_MAP, {
        f: ("pass" if missing_next_bad == 0 else "fail", missing_next_bad, 0, "Missing-next states are acceptable only when explicit in roll_status/review_notes.") for f in families
    })

    partial_gap_present = False
    if "roll_status" in roll_map.columns:
        partial = roll_map.loc[roll_map["roll_status"].astype(str) == "explicit_partial_chain_gap"].copy()
        notes = " ".join([str(x) for x in partial.get("review_notes", pd.Series(dtype=str)).dropna().astype(str).tolist()])
        partial_gap_present = "SiH7" in notes and "SiM7" in notes
    gap_status = "explicit_gap" if partial_gap_present else "fail"
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "explicit_partial_chain_gap_for_excluded_SiH7_SiM7", DATASET_ROLL_MAP, {
        "Si": (gap_status, "SiH7,SiM7" if partial_gap_present else "missing", "explicit gap for excluded SiH7 and SiM7", None),
        "USDRUBF": ("not_applicable", "perpetual_identity", "Si chain gap", None),
    })

    usdrubf_failures = []
    for frame_name, frame in [("roll_map", roll_map), ("continuous_5m", continuous_5m), ("continuous_d1", continuous_d1)]:
        if frame.empty:
            usdrubf_failures.append(frame_name + "_empty")
            continue
        if frame_name == "roll_map":
            part = frame.loc[frame.get("continuous_symbol", pd.Series(dtype=str)).astype(str).str.upper() == "USDRUBF"].copy()
            if part.empty:
                usdrubf_failures.append("roll_map_missing_usdrubf")
            else:
                if sorted(part.get("source_secid", pd.Series(dtype=str)).dropna().astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
                    usdrubf_failures.append("roll_map_source_not_identity")
                if int(part.get("roll_required", pd.Series(dtype=bool)).map(bool_value).sum()) != 0:
                    usdrubf_failures.append("roll_map_roll_required_true")
                if sorted(part.get("roll_status", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()) != ["perpetual_identity"]:
                    usdrubf_failures.append("roll_map_status_not_identity")
        elif frame_name == "continuous_5m":
            part = frame.loc[frame.get("continuous_symbol", pd.Series(dtype=str)).astype(str).str.upper() == "USDRUBF"].copy()
            if part.empty:
                usdrubf_failures.append("continuous_5m_missing_usdrubf")
            else:
                if sorted(part.get("source_secid", pd.Series(dtype=str)).dropna().astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
                    usdrubf_failures.append("continuous_5m_source_not_identity")
                if sorted(part.get("source_contract", pd.Series(dtype=str)).dropna().astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
                    usdrubf_failures.append("continuous_5m_contract_not_identity")
                if int(part.get("is_roll_boundary", pd.Series(dtype=bool)).map(bool_value).sum()) != 0:
                    usdrubf_failures.append("continuous_5m_roll_boundary_true")
        elif frame_name == "continuous_d1":
            part = frame.loc[frame.get("continuous_symbol", pd.Series(dtype=str)).astype(str).str.upper() == "USDRUBF"].copy()
            if part.empty:
                usdrubf_failures.append("continuous_d1_missing_usdrubf")
            else:
                bad_contracts = int(part.get("source_contracts", pd.Series(dtype=object)).map(lambda x: parse_source_contracts(x) != ["USDRUBF"]).sum())
                if bad_contracts > 0:
                    usdrubf_failures.append("continuous_d1_contracts_not_identity")
                if int(part.get("has_roll_boundary", pd.Series(dtype=bool)).map(bool_value).sum()) != 0:
                    usdrubf_failures.append("continuous_d1_roll_boundary_true")
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "usdrubf_identity_validation", DATASET_CONTINUOUS_SERIES, {
        "USDRUBF": ("pass" if not usdrubf_failures else "fail", json_value(usdrubf_failures), "no identity failures", None),
        "Si": ("not_applicable", "ordinary expiring chain", "USDRUBF identity", None),
    })

    bad_adjustment = 0
    for frame in [roll_map, continuous_5m, continuous_d1]:
        if "adjustment_factor" in frame.columns:
            bad_adjustment += int((pd.to_numeric(frame["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR).sum())
        else:
            bad_adjustment += 1
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "adjustment_factor_not_1", DATASET_CONTINUOUS_SERIES, {
        f: ("pass" if bad_adjustment == 0 else "fail", bad_adjustment, 0, None) for f in families
    })

    unexpected_sources = set()
    for frame, col in [(roll_map, "source_secid"), (continuous_5m, "source_secid")]:
        if col in frame.columns:
            unexpected_sources.update({str(x).upper() for x in frame[col].dropna().astype(str).tolist()} - whitelist_upper)
    unexpected_symbols = set()
    for frame in [continuous_5m, continuous_d1]:
        if "continuous_symbol" in frame.columns:
            unexpected_symbols.update({str(x) for x in frame["continuous_symbol"].dropna().astype(str).tolist()} - set(EXPECTED_CONTINUOUS_SYMBOLS))
    unexpected = sorted(unexpected_sources.union({x.upper() for x in unexpected_symbols}))
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "unexpected_included_instruments", DATASET_CONTINUOUS_SERIES, {
        f: ("pass" if not unexpected else "fail", ",".join(unexpected), "Slice 1 whitelist and continuous symbols only", None) for f in families
    })

    excluded_hits = set()
    for frame, columns in [(roll_map, ["source_secid", "next_secid", "source_contract_code"]), (continuous_5m, ["source_secid", "source_contract"] )]:
        for col in columns:
            if col in frame.columns:
                excluded_hits.update({str(x).upper() for x in frame[col].dropna().astype(str).tolist()}.intersection(excluded_upper))
    if "source_contracts" in continuous_d1.columns:
        for item in continuous_d1["source_contracts"].tolist():
            excluded_hits.update({x.upper() for x in parse_source_contracts(item)}.intersection(excluded_upper))
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "excluded_instruments_included", DATASET_CONTINUOUS_SERIES, {
        f: ("pass" if not excluded_hits else "fail", ",".join(sorted(excluded_hits)), "no SiH7 or SiM7 in roll/output source fields", None) for f in families
    })

    lineage_bad = 0
    if not continuous_5m.empty:
        required_lineage = ["source_secid", "source_contract", "roll_map_id", "roll_policy_id", "adjustment_policy_id", "adjustment_factor"]
        missing_cols = [x for x in required_lineage if x not in continuous_5m.columns]
        lineage_bad += len(continuous_5m) if missing_cols else int(continuous_5m[required_lineage].isna().any(axis=1).sum())
        if "roll_map_id" in continuous_5m.columns and "roll_map_id" in roll_map.columns:
            valid_ids = set(roll_map["roll_map_id"].dropna().astype(str).tolist())
            lineage_bad += int((~continuous_5m["roll_map_id"].astype(str).isin(valid_ids)).sum())
    if not continuous_d1.empty:
        required_d1 = ["source_contracts", "roll_map_id", "roll_policy_id", "adjustment_policy_id", "adjustment_factor"]
        missing_cols = [x for x in required_d1 if x not in continuous_d1.columns]
        lineage_bad += len(continuous_d1) if missing_cols else int(continuous_d1[[x for x in required_d1 if x != "source_contracts"]].isna().any(axis=1).sum())
        if "source_contracts" in continuous_d1.columns:
            lineage_bad += int(continuous_d1["source_contracts"].map(lambda x: len(parse_source_contracts(x)) == 0).sum())
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "continuous_output_row_source_lineage_completeness", DATASET_CONTINUOUS_SERIES, {
        f: ("pass" if lineage_bad == 0 else "fail", lineage_bad, 0, None) for f in families
    })

    missing_contracts_d1 = 0
    if "source_contracts" in continuous_d1.columns:
        missing_contracts_d1 = int(continuous_d1["source_contracts"].map(lambda x: len(parse_source_contracts(x)) == 0).sum())
    elif not continuous_d1.empty:
        missing_contracts_d1 = len(continuous_d1)
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "missing_source_contracts_in_d1", DATASET_CONTINUOUS_D1, {
        f: ("pass" if missing_contracts_d1 == 0 else "fail", missing_contracts_d1, 0, None) for f in families
    })

    bad_ohlc = check_ohlc(continuous_5m) + check_ohlc(continuous_d1)
    add_quality_rows(rows, run_id, run_date, snapshot_date, families, "ohlc_validity", DATASET_CONTINUOUS_SERIES, {
        f: ("pass" if bad_ohlc == 0 else "fail", bad_ohlc, 0, None) for f in families
    })

    return rows


def family_summaries(continuous_5m: pd.DataFrame, continuous_d1: pd.DataFrame, roll_map: pd.DataFrame) -> List[Dict[str, Any]]:
    families = observed_families(roll_map, continuous_5m, continuous_d1)
    out = []
    for family in families:
        c5 = continuous_5m.loc[continuous_5m.get("family_code", pd.Series(dtype=str)).astype(str) == family].copy() if not continuous_5m.empty else pd.DataFrame()
        d1 = continuous_d1.loc[continuous_d1.get("family_code", pd.Series(dtype=str)).astype(str) == family].copy() if not continuous_d1.empty else pd.DataFrame()
        rm = roll_map.loc[roll_map.get("family_code", pd.Series(dtype=str)).astype(str) == family].copy() if not roll_map.empty else pd.DataFrame()
        sources = []
        if "source_secid" in c5.columns:
            sources = sorted([str(x) for x in c5["source_secid"].dropna().unique().tolist()])
        out.append({
            "family_code": family,
            "continuous_symbol": family_symbol(family),
            "roll_map_rows": int(len(rm)),
            "continuous_5m_rows": int(len(c5)),
            "continuous_d1_rows": int(len(d1)),
            "continuous_5m_partitions": int(c5["_source_partition_path"].nunique()) if "_source_partition_path" in c5.columns else 0,
            "source_secids": sources,
            "min_trade_date": None if d1.empty or "trade_date" not in d1.columns else str(d1["trade_date"].min()),
            "max_trade_date": None if d1.empty or "trade_date" not in d1.columns else str(d1["trade_date"].max()),
        })
    return out


def roll_boundary_summary(continuous_5m: pd.DataFrame, continuous_d1: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {"continuous_5m": {}, "continuous_d1": {}}
    if not continuous_5m.empty and "is_roll_boundary" in continuous_5m.columns:
        part = continuous_5m.loc[continuous_5m["is_roll_boundary"].map(bool_value)].copy()
        if not part.empty and "continuous_symbol" in part.columns:
            out["continuous_5m"] = {str(k): int(v) for k, v in part["continuous_symbol"].astype(str).value_counts(dropna=False).to_dict().items()}
    if not continuous_d1.empty and "has_roll_boundary" in continuous_d1.columns:
        part = continuous_d1.loc[continuous_d1["has_roll_boundary"].map(bool_value)].copy()
        if not part.empty and "continuous_symbol" in part.columns:
            out["continuous_d1"] = {str(k): int(v) for k, v in part["continuous_symbol"].astype(str).value_counts(dropna=False).to_dict().items()}
    return out


def quality_status_counts(quality: pd.DataFrame) -> Dict[str, int]:
    if quality.empty or "check_status" not in quality.columns:
        return {}
    return {str(k): int(v) for k, v in quality["check_status"].astype(str).value_counts(dropna=False).to_dict().items()}


def validation_blockers(quality: pd.DataFrame) -> List[str]:
    if quality.empty:
        return ["quality_report_empty"]
    fails = quality.loc[quality["check_status"].astype(str) == "fail"].copy()
    blockers = []
    for _, row in fails.iterrows():
        blockers.append(str(row.get("family_code")) + ":" + str(row.get("check_id")) + ":" + str(row.get("observed_value")))
    missing = []
    for check_id in REQUIRED_QUALITY_CHECKS:
        if check_id not in set(quality["check_id"].astype(str).tolist()):
            missing.append(check_id)
    if missing:
        blockers.append("missing_required_quality_checks:" + ",".join(missing))
    return blockers


def write_json(data: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
    except Exception as exc:
        raise RuntimeError("Cannot write parquet " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc


def build_manifest(
    run_id: str,
    run_date: str,
    snapshot_date: str,
    started_ts: str,
    completed_ts: str,
    whitelist: List[str],
    excluded: List[str],
    normalized_path: Path,
    expiration_path: Path,
    roll_map_path: Path,
    continuous_5m_paths: List[Path],
    continuous_d1_paths: List[Path],
    quality_path: Path,
    manifest_path: Path,
    continuous_5m_root: Path,
    continuous_d1_root: Path,
    continuous_5m: pd.DataFrame,
    continuous_d1: pd.DataFrame,
    roll_map: pd.DataFrame,
    quality: pd.DataFrame,
    commit_sha: str,
) -> Dict[str, Any]:
    blockers = validation_blockers(quality)
    status_counts = quality_status_counts(quality)
    usdrubf_rows = quality.loc[(quality["check_id"] == "usdrubf_identity_validation") & (quality["family_code"] == "USDRUBF")] if not quality.empty else pd.DataFrame()
    usdrubf_status = "pass" if not usdrubf_rows.empty and set(usdrubf_rows["check_status"].astype(str).tolist()) == {"pass"} else "fail"
    lineage_rows = quality.loc[quality["check_id"] == "continuous_output_row_source_lineage_completeness"] if not quality.empty else pd.DataFrame()
    lineage_status = "pass" if not lineage_rows.empty and "fail" not in set(lineage_rows["check_status"].astype(str).tolist()) else "fail"
    gap_rows = quality.loc[quality["check_id"] == "explicit_partial_chain_gap_for_excluded_SiH7_SiM7"] if not quality.empty else pd.DataFrame()
    gap_status = "explicit_gap" if not gap_rows.empty and "explicit_gap" in set(gap_rows["check_status"].astype(str).tolist()) else "fail"
    calendar_rows = quality.loc[quality["check_id"] == "invalid_calendar_status"] if not quality.empty else pd.DataFrame()
    calendar_status = CALENDAR_STATUS if not calendar_rows.empty and "fail" not in set(calendar_rows["check_status"].astype(str).tolist()) else "invalid"
    verdict = "pass" if not blockers else "fail"
    output_artifacts = {
        "roll_map": str(roll_map_path),
        "continuous_5m_partition_root": str(continuous_5m_root),
        "continuous_d1_partition_root": str(continuous_d1_root),
        "quality_report": str(quality_path),
        "manifest": str(manifest_path),
    }
    input_artifacts = {
        "normalized_registry": {
            "path": str(normalized_path),
            "schema_version": SCHEMA_NORMALIZED_REGISTRY,
        },
        "expiration_map": {
            "path": str(expiration_path),
            "schema_version": SCHEMA_EXPIRATION_MAP,
        },
        "roll_map": {
            "path": str(roll_map_path),
            "schema_version": SCHEMA_ROLL_MAP,
        },
        "futures_raw_5m": {
            "contract": CONTRACT_RAW_5M,
            "schema_version": "futures_raw_5m.v1",
        },
        "futures_derived_d1_ohlcv": {
            "contract": CONTRACT_DERIVED_D1,
            "schema_version": "futures_derived_d1_ohlcv.v1",
        },
        "continuous_5m": {
            "partition_count": len(continuous_5m_paths),
            "schema_version": SCHEMA_CONTINUOUS_5M,
        },
        "continuous_d1": {
            "partition_count": len(continuous_d1_paths),
            "schema_version": SCHEMA_CONTINUOUS_D1,
        },
    }
    return {
        "schema_version": SCHEMA_MANIFEST,
        "run_id": run_id,
        "run_date": run_date,
        "snapshot_date": snapshot_date,
        "started_ts": started_ts,
        "completed_ts": completed_ts,
        "builder_version": "continuous_quality_report.py",
        "commit_sha": commit_sha,
        "builder_whitelist_applied": whitelist,
        "excluded_instruments_confirmed": excluded,
        "roll_policy_id": ROLL_POLICY_ID,
        "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
        "calendar_status": calendar_status,
        "input_artifacts": input_artifacts,
        "output_artifacts": output_artifacts,
        "roll_map_artifact": {
            "path": str(roll_map_path),
            "schema_version": SCHEMA_ROLL_MAP,
            "rows": int(len(roll_map)),
        },
        "continuous_5m_partitions_created": [str(x) for x in continuous_5m_paths],
        "continuous_d1_partitions_created": [str(x) for x in continuous_d1_paths],
        "row_counts": {
            "roll_map": int(len(roll_map)),
            "continuous_5m": int(len(continuous_5m)),
            "continuous_d1": int(len(continuous_d1)),
            "quality_report": int(len(quality)),
        },
        "partition_counts": {
            "continuous_5m": int(len(continuous_5m_paths)),
            "continuous_d1": int(len(continuous_d1_paths)),
        },
        "schema_versions": {
            "manifest": SCHEMA_MANIFEST,
            "quality_report": SCHEMA_QUALITY_REPORT,
            "roll_map": SCHEMA_ROLL_MAP,
            "continuous_5m": SCHEMA_CONTINUOUS_5M,
            "continuous_d1": SCHEMA_CONTINUOUS_D1,
        },
        "family_summaries": family_summaries(continuous_5m, continuous_d1, roll_map),
        "roll_boundary_summary": roll_boundary_summary(continuous_5m, continuous_d1),
        "partial_chain_gap_summary": {
            "status": gap_status,
            "excluded_gap_contracts": [x for x in excluded if x in {"SiH7", "SiM7"}],
            "notes": "Slice 1 partial Si-chain gap is explicit; excluded contracts are not promoted into outputs.",
        },
        "usdrubf_identity_check": {
            "status": usdrubf_status,
            "expected": "USDRUBF source identity, no roll boundary, adjustment_factor=1.0",
        },
        "source_lineage_check": {
            "status": lineage_status,
            "expected": "continuous 5m rows and D1 rows preserve source contracts and roll_map_id lineage",
        },
        "quality_status_counts": status_counts,
        "builder_result_verdict": verdict,
        "blockers": blockers,
    }


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    parser.add_argument("--roll-policy-id", default=ROLL_POLICY_ID)
    parser.add_argument("--adjustment-policy-id", default=ADJUSTMENT_POLICY_ID)
    parser.add_argument("--whitelist", default=",".join(DEFAULT_WHITELIST))
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    parser.add_argument("--commit-sha", default="")
    args = parser.parse_args()

    root = repo_root()
    data_root = base.resolve_data_root(args)
    snapshot_date = str(args.snapshot_date).strip()
    run_date = str(args.run_date).strip()
    roll_policy_id = str(args.roll_policy_id).strip()
    adjustment_policy_id = str(args.adjustment_policy_id).strip()
    whitelist = parse_list(args.whitelist, DEFAULT_WHITELIST)
    excluded = parse_list(args.excluded, DEFAULT_EXCLUDED)

    if roll_policy_id != ROLL_POLICY_ID:
        raise RuntimeError("Unsupported roll_policy_id: " + roll_policy_id)
    if adjustment_policy_id != ADJUSTMENT_POLICY_ID:
        raise RuntimeError("Unsupported adjustment_policy_id: " + adjustment_policy_id)

    base.assert_files_exist(root, REQUIRED_CONTRACTS)

    started_ts = utc_now_iso()
    run_id = "futures_continuous_builder_manifest_" + run_date + "_" + stable_id([
        snapshot_date,
        started_ts,
        roll_policy_id,
        adjustment_policy_id,
        ",".join(whitelist),
    ])

    normalized_path = resolve_contract_path(root, data_root, CONTRACT_NORMALIZED_REGISTRY, {"snapshot_date": snapshot_date})
    expiration_path = resolve_contract_path(root, data_root, CONTRACT_EXPIRATION_MAP, {"registry_snapshot_date": snapshot_date, "snapshot_date": snapshot_date})
    roll_map_path = resolve_contract_path(root, data_root, CONTRACT_ROLL_MAP, {"snapshot_date": snapshot_date, "roll_policy_id": roll_policy_id})
    quality_path = resolve_contract_path(root, data_root, CONTRACT_QUALITY_REPORT, {"run_date": run_date})
    manifest_path = resolve_contract_path(root, data_root, CONTRACT_MANIFEST, {"run_date": run_date})

    continuous_5m_paths = glob_contract_paths(root, data_root, CONTRACT_CONTINUOUS_5M, {
        "roll_policy_id": roll_policy_id,
        "adjustment_policy_id": adjustment_policy_id,
    })
    continuous_d1_paths = glob_contract_paths(root, data_root, CONTRACT_CONTINUOUS_D1, {
        "roll_policy_id": roll_policy_id,
        "adjustment_policy_id": adjustment_policy_id,
    })

    normalized, normalized_exists = read_parquet_if_exists(normalized_path)
    expiration_map, expiration_exists = read_parquet_if_exists(expiration_path)
    roll_map, roll_map_exists = read_parquet_if_exists(roll_map_path)
    continuous_5m = read_partitions(continuous_5m_paths) if continuous_5m_paths else pd.DataFrame()
    continuous_d1 = read_partitions(continuous_d1_paths) if continuous_d1_paths else pd.DataFrame()

    quality_rows = build_quality_rows(
        root,
        data_root,
        run_id,
        run_date,
        snapshot_date,
        whitelist,
        excluded,
        normalized_path,
        expiration_path,
        roll_map_path,
        continuous_5m_paths,
        continuous_d1_paths,
        normalized_exists,
        expiration_exists,
        roll_map_exists,
        continuous_5m,
        continuous_d1,
        roll_map,
        expiration_map,
    )
    quality = pd.DataFrame(quality_rows)
    required_columns = [
        "quality_report_id",
        "run_id",
        "run_date",
        "snapshot_date",
        "family_code",
        "continuous_symbol",
        "check_id",
        "dataset_id",
        "schema_version",
        "roll_policy_id",
        "adjustment_policy_id",
        "calendar_status",
        "check_status",
        "affected_source_secid",
        "affected_trade_date",
        "observed_value",
        "expected_value",
        "review_notes",
    ]
    quality = quality[required_columns].copy()
    duplicate_quality_pk = int(quality.duplicated(subset=["quality_report_id", "run_id", "family_code", "check_id"]).sum())
    if duplicate_quality_pk > 0:
        raise RuntimeError("Quality report duplicate primary key rows: " + str(duplicate_quality_pk))

    write_parquet(quality, quality_path)
    completed_ts = utc_now_iso()

    continuous_5m_root = path_root_from_contract(root, data_root, CONTRACT_CONTINUOUS_5M, {
        "roll_policy_id": roll_policy_id,
        "adjustment_policy_id": adjustment_policy_id,
    }, ["family"])
    continuous_d1_root = path_root_from_contract(root, data_root, CONTRACT_CONTINUOUS_D1, {
        "roll_policy_id": roll_policy_id,
        "adjustment_policy_id": adjustment_policy_id,
    }, ["family"])

    manifest = build_manifest(
        run_id,
        run_date,
        snapshot_date,
        started_ts,
        completed_ts,
        whitelist,
        excluded,
        normalized_path,
        expiration_path,
        roll_map_path,
        continuous_5m_paths,
        continuous_d1_paths,
        quality_path,
        manifest_path,
        continuous_5m_root,
        continuous_d1_root,
        continuous_5m,
        continuous_d1,
        roll_map,
        quality,
        str(args.commit_sha or ""),
    )
    write_json(manifest, manifest_path)

    print_json_line("run_id", run_id)
    print_json_line("output_artifacts_created", {
        "manifest": str(manifest_path),
        "quality_report": str(quality_path),
    })
    print_json_line("quality_status_counts", manifest["quality_status_counts"])
    print_json_line("builder_result_verdict", manifest["builder_result_verdict"])
    if manifest["blockers"]:
        print_json_line("blockers", manifest["blockers"])
    return 0 if manifest["builder_result_verdict"] == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
