#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

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

SCHEMA_CONTINUOUS_5M = "futures_continuous_5m.v1"
SCHEMA_RAW_5M = "futures_raw_5m.v1"
SCHEMA_ROLL_MAP = "futures_continuous_roll_map.v1"

CONTRACT_CONTINUOUS_5M = "contracts/datasets/futures_continuous_5m_contract.md"
CONTRACT_ROLL_MAP = "contracts/datasets/futures_continuous_roll_map_contract.md"
CONTRACT_RAW_5M = "contracts/datasets/futures_raw_5m_contract.md"
REQUIRED_CONTRACTS = [
    CONTRACT_CONTINUOUS_5M,
    CONTRACT_ROLL_MAP,
    CONTRACT_RAW_5M,
]

ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"
ADJUSTMENT_FACTOR = 1.0
CALENDAR_STATUS = "canonical_apim_futures_xml"
BUILDABLE_ROLL_STATUSES = {
    "active_window",
    "perpetual_identity",
    "explicit_partial_chain_gap",
    "blocked_missing_next_contract",
}
BLOCKED_ROLL_STATUSES = {
    "blocked_unresolved_anchor",
    "blocked_calendar",
}


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


def resolve_contract_path(root: Path, data_root: Path, rel_path: str, replacements: Dict[str, str]) -> Path:
    pattern = contract_path_pattern(root, rel_path)
    tail = pattern[len("${MOEX_DATA_ROOT}"):].lstrip("/")
    for key, value in replacements.items():
        tail = tail.replace("{" + key + "}", str(value))
    tail = tail.replace("YYYY-MM-DD", str(replacements.get("trade_date", replacements.get("snapshot_date", ""))))
    unresolved = [part for part in tail.split("/") if "{" in part or "}" in part]
    if unresolved:
        raise RuntimeError("Unresolved path pattern tokens in " + rel_path + ": " + ",".join(unresolved))
    return data_root / tail


def raw_glob_pattern(root: Path, data_root: Path) -> str:
    pattern = contract_path_pattern(root, CONTRACT_RAW_5M)
    tail = pattern[len("${MOEX_DATA_ROOT}"):].lstrip("/")
    tail = tail.replace("{trade_date}", "*")
    tail = tail.replace("{family_code}", "*")
    tail = tail.replace("{secid}", "*")
    tail = tail.replace("YYYY-MM-DD", "*")
    return str(data_root / tail)


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


def source_scope_from_roll_map(roll_map: pd.DataFrame, excluded: Iterable[str]) -> List[str]:
    excluded_upper = {str(x).upper() for x in excluded}
    sources = []
    for value in roll_map["source_secid"].dropna().astype(str).tolist():
        secid = value.strip()
        if not secid:
            continue
        if secid.upper() in excluded_upper:
            raise RuntimeError("Excluded source instrument present in roll map: " + secid)
        if secid not in sources:
            sources.append(secid)
    if not sources:
        raise RuntimeError("Roll map has zero source_secid values")
    return sources


def load_roll_map(root: Path, data_root: Path, snapshot_date: str, roll_policy_id: str) -> pd.DataFrame:
    path = resolve_contract_path(
        root,
        data_root,
        CONTRACT_ROLL_MAP,
        {"snapshot_date": snapshot_date, "roll_policy_id": roll_policy_id},
    )
    if not path.exists():
        raise FileNotFoundError("Missing roll map artifact: " + str(path))
    frame = pd.read_parquet(path)
    frame["_roll_map_path"] = str(path)
    return frame


def validate_roll_map(frame: pd.DataFrame, roll_policy_id: str, adjustment_policy_id: str, whitelist: List[str], excluded: List[str]) -> List[str]:
    blockers: List[str] = []
    required = [
        "roll_map_id",
        "snapshot_date",
        "family_code",
        "continuous_symbol",
        "source_secid",
        "source_contract_code",
        "next_secid",
        "is_perpetual",
        "roll_required",
        "valid_from_session",
        "valid_through_session",
        "calendar_status",
        "roll_policy_id",
        "adjustment_policy_id",
        "adjustment_factor",
        "roll_status",
        "schema_version",
    ]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        return ["roll_map_missing_required_fields:" + ",".join(missing)]
    if frame.empty:
        return ["roll_map_empty"]
    excluded_upper = {x.upper() for x in excluded}
    whitelist_upper = {x.upper() for x in whitelist}
    observed_sources = {str(x).upper() for x in frame["source_secid"].dropna().astype(str).tolist()}
    if observed_sources != whitelist_upper:
        blockers.append("roll_map_scope_mismatch:observed=" + ",".join(sorted(observed_sources)) + ":expected=" + ",".join(sorted(whitelist_upper)))
    source_hits = sorted(observed_sources.intersection(excluded_upper))
    next_values = {str(x).upper() for x in frame["next_secid"].dropna().astype(str).tolist() if str(x).strip()}
    next_hits = sorted(next_values.intersection(excluded_upper))
    if source_hits:
        blockers.append("excluded_source_instruments_present:" + ",".join(source_hits))
    if next_hits:
        blockers.append("excluded_next_instruments_present:" + ",".join(next_hits))
    schemas = sorted([str(x) for x in frame["schema_version"].dropna().unique().tolist()])
    if schemas != [SCHEMA_ROLL_MAP]:
        blockers.append("invalid_roll_map_schema_versions:" + json.dumps(schemas, ensure_ascii=False))
    bad_roll_policy = frame.loc[frame["roll_policy_id"].astype(str) != roll_policy_id]
    if not bad_roll_policy.empty:
        blockers.append("invalid_roll_policy_rows:" + str(len(bad_roll_policy)))
    bad_adjustment_policy = frame.loc[frame["adjustment_policy_id"].astype(str) != adjustment_policy_id]
    if not bad_adjustment_policy.empty:
        blockers.append("invalid_adjustment_policy_rows:" + str(len(bad_adjustment_policy)))
    bad_adjustment_factor = frame.loc[pd.to_numeric(frame["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR]
    if not bad_adjustment_factor.empty:
        blockers.append("invalid_adjustment_factor_rows:" + str(len(bad_adjustment_factor)))
    bad_calendar = frame.loc[frame["calendar_status"].astype(str) != CALENDAR_STATUS]
    if not bad_calendar.empty:
        blockers.append("invalid_calendar_status_rows:" + str(len(bad_calendar)))
    statuses = {str(x) for x in frame["roll_status"].dropna().astype(str).tolist()}
    blocked_status_hits = sorted(statuses.intersection(BLOCKED_ROLL_STATUSES))
    if blocked_status_hits:
        blockers.append("blocked_roll_map_status_present:" + ",".join(blocked_status_hits))
    invalid_statuses = sorted(statuses - BUILDABLE_ROLL_STATUSES - BLOCKED_ROLL_STATUSES)
    if invalid_statuses:
        blockers.append("invalid_roll_status:" + ",".join(invalid_statuses))
    pk_fields = ["roll_map_id", "snapshot_date", "family_code", "source_secid", "valid_from_session"]
    duplicates = int(frame.duplicated(subset=pk_fields).sum())
    if duplicates > 0:
        blockers.append("duplicate_roll_map_primary_key_rows:" + str(duplicates))
    for _, row in frame.iterrows():
        secid = str(row.get("source_secid"))
        is_perpetual = bool_value(row.get("is_perpetual"))
        valid_from = clean_date(row.get("valid_from_session"))
        valid_through = clean_date(row.get("valid_through_session"))
        status = str(row.get("roll_status"))
        if status not in BUILDABLE_ROLL_STATUSES:
            continue
        if is_perpetual:
            if str(row.get("continuous_symbol")) != secid:
                blockers.append("perpetual_continuous_symbol_mismatch:" + secid)
            if str(row.get("source_contract_code")) != secid:
                blockers.append("perpetual_source_contract_mismatch:" + secid)
            if bool_value(row.get("roll_required")):
                blockers.append("perpetual_roll_required_true:" + secid)
        else:
            if not valid_from:
                blockers.append("ordinary_missing_valid_from_session:" + secid)
            if valid_from and valid_through and valid_from > valid_through:
                blockers.append("ordinary_invalid_valid_window:" + secid)
            if not bool_value(row.get("roll_required")):
                blockers.append("ordinary_roll_required_false:" + secid)
    return blockers


def discover_raw_paths(root: Path, data_root: Path, source_secids: List[str], excluded: List[str], from_date: str, till: str) -> List[Path]:
    source_upper = {x.upper() for x in source_secids}
    excluded_upper = {x.upper() for x in excluded}
    paths: List[Path] = []
    excluded_hits: List[str] = []
    seen_by_source: Dict[str, int] = {x.upper(): 0 for x in source_secids}
    for raw_path in sorted(Path(data_root).glob(str(Path(raw_glob_pattern(root, data_root)).relative_to(data_root)))):
        secid = partition_value(raw_path, "secid")
        trade_date = partition_value(raw_path, "trade_date")
        if secid.upper() in excluded_upper:
            excluded_hits.append(str(raw_path))
            continue
        if secid.upper() not in source_upper:
            continue
        if from_date and trade_date < from_date:
            continue
        if till and trade_date > till:
            continue
        seen_by_source[secid.upper()] = seen_by_source.get(secid.upper(), 0) + 1
        paths.append(raw_path)
    if excluded_hits:
        raise RuntimeError("Excluded raw source partitions found: " + json.dumps(excluded_hits[:20], ensure_ascii=False))
    missing_sources = sorted([k for k, v in seen_by_source.items() if v == 0])
    if missing_sources:
        raise FileNotFoundError("Missing raw source partitions for roll-map sources: " + ",".join(missing_sources))
    if not paths:
        raise RuntimeError("No raw 5m partitions found for continuous 5m build")
    return paths


def read_raw(paths: List[Path]) -> pd.DataFrame:
    frames = []
    for path in paths:
        part = pd.read_parquet(path)
        part["_source_partition_path"] = str(path)
        frames.append(part)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def validate_raw(frame: pd.DataFrame, source_secids: List[str], excluded: List[str]) -> List[str]:
    blockers: List[str] = []
    required = [
        "trade_date",
        "end",
        "session_date",
        "secid",
        "family_code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "schema_version",
    ]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        return ["raw_5m_missing_required_fields:" + ",".join(missing)]
    if frame.empty:
        return ["raw_5m_empty"]
    source_upper = {x.upper() for x in source_secids}
    excluded_upper = {x.upper() for x in excluded}
    observed = {str(x).upper() for x in frame["secid"].dropna().astype(str).tolist()}
    outside = sorted(observed - source_upper)
    if outside:
        blockers.append("raw_sources_outside_roll_map:" + ",".join(outside))
    hits = sorted(observed.intersection(excluded_upper))
    if hits:
        blockers.append("excluded_raw_sources_present:" + ",".join(hits))
    schemas = sorted([str(x) for x in frame["schema_version"].dropna().unique().tolist()])
    if schemas != [SCHEMA_RAW_5M]:
        blockers.append("invalid_raw_schema_versions:" + json.dumps(schemas, ensure_ascii=False))
    if "calendar_denominator_status" in frame.columns:
        calendars = sorted([str(x) for x in frame["calendar_denominator_status"].dropna().unique().tolist()])
        if calendars != [CALENDAR_STATUS]:
            blockers.append("invalid_raw_calendar_status:" + json.dumps(calendars, ensure_ascii=False))
    null_ohlc = int(frame[["open", "high", "low", "close"]].isna().any(axis=1).sum())
    if null_ohlc > 0:
        blockers.append("raw_null_ohlc_rows:" + str(null_ohlc))
    invalid = (frame["high"] < frame["low"]) | (frame["open"] > frame["high"]) | (frame["open"] < frame["low"]) | (frame["close"] > frame["high"]) | (frame["close"] < frame["low"])
    invalid_count = int(invalid.fillna(True).sum())
    if invalid_count > 0:
        blockers.append("raw_invalid_ohlc_rows:" + str(invalid_count))
    duplicates = int(frame.duplicated(subset=["trade_date", "end", "secid"]).sum())
    if duplicates > 0:
        blockers.append("raw_duplicate_primary_key_rows:" + str(duplicates))
    return blockers


def normalize_raw(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["trade_date"] = out["trade_date"].astype(str)
    out["session_date"] = out["session_date"].map(clean_date)
    out["secid"] = out["secid"].astype(str)
    out["family_code"] = out["family_code"].astype(str)
    out["end"] = pd.to_datetime(out["end"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.loc[out["session_date"].notna() & out["end"].notna()].copy()
    return out.sort_values(["secid", "trade_date", "end"]).reset_index(drop=True)


def normalize_roll_map(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["source_secid"] = out["source_secid"].astype(str)
    out["source_contract_code"] = out["source_contract_code"].astype(str)
    out["family_code"] = out["family_code"].astype(str)
    out["continuous_symbol"] = out["continuous_symbol"].astype(str)
    out["valid_from_session"] = out["valid_from_session"].map(clean_date)
    out["valid_through_session"] = out["valid_through_session"].map(clean_date)
    out["_is_perpetual_bool"] = out["is_perpetual"].map(bool_value)
    out["_window_sort"] = out["valid_from_session"].fillna("0001-01-01")
    return out.sort_values(["continuous_symbol", "_window_sort", "source_secid"]).reset_index(drop=True)


def boundary_source_keys(roll_map: pd.DataFrame) -> set:
    keys = set()
    ordinary = roll_map.loc[~roll_map["_is_perpetual_bool"]].copy()
    for continuous_symbol, group in ordinary.groupby("continuous_symbol"):
        ordered = group.sort_values(["valid_from_session", "source_secid"]).reset_index(drop=True)
        for idx, row in ordered.iterrows():
            if idx == 0:
                continue
            valid_from = clean_date(row.get("valid_from_session"))
            if valid_from:
                keys.add((str(row.get("source_secid")), str(continuous_symbol), valid_from))
    return keys


def select_continuous_rows(raw: pd.DataFrame, roll_map: pd.DataFrame, ingest_ts: str, roll_policy_id: str, adjustment_policy_id: str) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []
    ambiguity_hits: List[str] = []
    boundary_keys = boundary_source_keys(roll_map)
    grouped_map = {str(secid): group.copy() for secid, group in roll_map.groupby("source_secid", sort=False)}
    for _, row in raw.iterrows():
        secid = str(row.get("secid"))
        session_date = str(row.get("session_date"))
        candidates = grouped_map.get(secid)
        if candidates is None or candidates.empty:
            continue
        if bool(candidates["_is_perpetual_bool"].any()):
            matched = candidates.loc[candidates["_is_perpetual_bool"]].copy()
        else:
            matched = candidates.loc[
                (candidates["valid_from_session"].notna())
                & (candidates["valid_from_session"] <= session_date)
                & (
                    candidates["valid_through_session"].isna()
                    | (candidates["valid_through_session"] >= session_date)
                )
            ].copy()
        if matched.empty:
            continue
        if len(matched) > 1:
            ambiguity_hits.append(secid + ":" + session_date)
            continue
        mrow = matched.iloc[0]
        is_boundary = (secid, str(mrow.get("continuous_symbol")), session_date) in boundary_keys
        records.append({
            "trade_date": str(row.get("trade_date")),
            "end": row.get("end"),
            "session_date": session_date,
            "continuous_symbol": str(mrow.get("continuous_symbol")),
            "family_code": str(mrow.get("family_code")),
            "source_secid": secid,
            "source_contract": str(mrow.get("source_contract_code")),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "roll_policy_id": roll_policy_id,
            "adjustment_policy_id": adjustment_policy_id,
            "adjustment_factor": ADJUSTMENT_FACTOR,
            "is_roll_boundary": bool(is_boundary),
            "roll_map_id": str(mrow.get("roll_map_id")),
            "schema_version": SCHEMA_CONTINUOUS_5M,
            "ingest_ts": ingest_ts,
            "_source_partition_path": row.get("_source_partition_path"),
        })
    if ambiguity_hits:
        raise RuntimeError("Ambiguous active source contract hits: " + json.dumps(sorted(set(ambiguity_hits))[:50], ensure_ascii=False))
    if not records:
        raise RuntimeError("Continuous 5m builder selected zero rows from raw inputs and roll map")
    out = pd.DataFrame(records)
    return out.sort_values(["continuous_symbol", "trade_date", "end", "source_secid"]).reset_index(drop=True)


def validate_continuous(frame: pd.DataFrame, excluded: List[str], roll_policy_id: str, adjustment_policy_id: str) -> List[str]:
    blockers: List[str] = []
    required = [
        "trade_date",
        "end",
        "session_date",
        "continuous_symbol",
        "family_code",
        "source_secid",
        "source_contract",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "roll_policy_id",
        "adjustment_policy_id",
        "adjustment_factor",
        "is_roll_boundary",
        "roll_map_id",
        "schema_version",
        "ingest_ts",
    ]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        return ["continuous_missing_required_fields:" + ",".join(missing)]
    if frame.empty:
        return ["continuous_empty"]
    null_required = [x for x in required if int(frame[x].isna().sum()) > 0]
    if null_required:
        blockers.append("continuous_null_required_fields:" + ",".join(null_required))
    bad_schema = frame.loc[frame["schema_version"].astype(str) != SCHEMA_CONTINUOUS_5M]
    if not bad_schema.empty:
        blockers.append("invalid_continuous_schema_rows:" + str(len(bad_schema)))
    bad_roll_policy = frame.loc[frame["roll_policy_id"].astype(str) != roll_policy_id]
    if not bad_roll_policy.empty:
        blockers.append("invalid_roll_policy_rows:" + str(len(bad_roll_policy)))
    bad_adjustment_policy = frame.loc[frame["adjustment_policy_id"].astype(str) != adjustment_policy_id]
    if not bad_adjustment_policy.empty:
        blockers.append("invalid_adjustment_policy_rows:" + str(len(bad_adjustment_policy)))
    bad_adjustment_factor = frame.loc[pd.to_numeric(frame["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR]
    if not bad_adjustment_factor.empty:
        blockers.append("invalid_adjustment_factor_rows:" + str(len(bad_adjustment_factor)))
    null_ohlc = int(frame[["open", "high", "low", "close"]].isna().any(axis=1).sum())
    if null_ohlc > 0:
        blockers.append("continuous_null_ohlc_rows:" + str(null_ohlc))
    invalid = (frame["high"] < frame["low"]) | (frame["open"] > frame["high"]) | (frame["open"] < frame["low"]) | (frame["close"] > frame["high"]) | (frame["close"] < frame["low"])
    invalid_count = int(invalid.fillna(True).sum())
    if invalid_count > 0:
        blockers.append("continuous_invalid_ohlc_rows:" + str(invalid_count))
    duplicates = int(frame.duplicated(subset=["continuous_symbol", "trade_date", "end"]).sum())
    if duplicates > 0:
        blockers.append("duplicate_output_timestamps:" + str(duplicates))
    excluded_upper = {x.upper() for x in excluded}
    source_hits = sorted({str(x).upper() for x in frame["source_secid"].astype(str).tolist()}.intersection(excluded_upper))
    contract_hits = sorted({str(x).upper() for x in frame["source_contract"].astype(str).tolist()}.intersection(excluded_upper))
    if source_hits:
        blockers.append("excluded_source_secid_present:" + ",".join(source_hits))
    if contract_hits:
        blockers.append("excluded_source_contract_present:" + ",".join(contract_hits))
    usdrubf = frame.loc[frame["continuous_symbol"].astype(str).str.upper() == "USDRUBF"].copy()
    if not usdrubf.empty:
        if sorted(usdrubf["source_secid"].astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
            blockers.append("usdrubf_invalid_source_secid")
        if sorted(usdrubf["source_contract"].astype(str).str.upper().unique().tolist()) != ["USDRUBF"]:
            blockers.append("usdrubf_invalid_source_contract")
        if int(usdrubf["is_roll_boundary"].astype(bool).sum()) != 0:
            blockers.append("usdrubf_roll_boundary_true_rows")
    lineage_nulls = int(frame[["source_secid", "source_contract", "roll_map_id", "roll_policy_id", "adjustment_policy_id", "adjustment_factor"]].isna().any(axis=1).sum())
    if lineage_nulls > 0:
        blockers.append("missing_source_lineage_rows:" + str(lineage_nulls))
    return blockers


def output_partition_path(root: Path, data_root: Path, roll_policy_id: str, adjustment_policy_id: str, family_code: str, trade_date: str) -> Path:
    return resolve_contract_path(
        root,
        data_root,
        CONTRACT_CONTINUOUS_5M,
        {
            "roll_policy_id": roll_policy_id,
            "adjustment_policy_id": adjustment_policy_id,
            "family_code": family_code,
            "trade_date": trade_date,
        },
    )


def write_partitions(root: Path, data_root: Path, frame: pd.DataFrame, roll_policy_id: str, adjustment_policy_id: str) -> List[str]:
    paths: List[str] = []
    clean = frame.drop(columns=[x for x in ["_source_partition_path"] if x in frame.columns]).copy()
    clean = clean.drop_duplicates(subset=["continuous_symbol", "trade_date", "end"], keep="last").copy()
    for (family_code, trade_date), part in clean.groupby(["family_code", "trade_date"], sort=True):
        path = output_partition_path(root, data_root, roll_policy_id, adjustment_policy_id, str(family_code), str(trade_date))
        path.parent.mkdir(parents=True, exist_ok=True)
        part.sort_values(["continuous_symbol", "end"]).to_parquet(path, index=False)
        paths.append(str(path))
    return paths


def summarize(frame: pd.DataFrame, partition_paths: List[str], raw_paths: List[Path], roll_map: pd.DataFrame) -> Dict[str, Any]:
    rows_by_symbol = {str(k): int(v) for k, v in frame["continuous_symbol"].astype(str).value_counts(dropna=False).to_dict().items()}
    rows_by_source = {str(k): int(v) for k, v in frame["source_secid"].astype(str).value_counts(dropna=False).to_dict().items()}
    boundary_counts = {str(k): int(v) for k, v in frame.loc[frame["is_roll_boundary"].astype(bool), "continuous_symbol"].astype(str).value_counts(dropna=False).to_dict().items()}
    return {
        "rows": int(len(frame)),
        "continuous_symbols": sorted([str(x) for x in frame["continuous_symbol"].dropna().unique().tolist()]),
        "source_secids": sorted([str(x) for x in frame["source_secid"].dropna().unique().tolist()]),
        "rows_by_continuous_symbol": rows_by_symbol,
        "rows_by_source_secid": rows_by_source,
        "roll_boundary_rows_by_symbol": boundary_counts,
        "partition_count": int(len(partition_paths)),
        "raw_partition_count": int(len(raw_paths)),
        "roll_map_rows": int(len(roll_map)),
        "min_end": str(frame["end"].min()),
        "max_end": str(frame["end"].max()),
    }


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--roll-policy-id", default=ROLL_POLICY_ID)
    parser.add_argument("--adjustment-policy-id", default=ADJUSTMENT_POLICY_ID)
    parser.add_argument("--whitelist", default=",".join(DEFAULT_WHITELIST))
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    args = parser.parse_args()

    root = repo_root()
    data_root = base.resolve_data_root(args)
    snapshot_date = str(args.snapshot_date).strip()
    run_date = str(args.run_date).strip()
    from_date = base.parse_iso_date(str(args.from_date or "")) if str(args.from_date or "").strip() else ""
    till = base.parse_iso_date(str(args.till or "")) if str(args.till or "").strip() else ""
    roll_policy_id = str(args.roll_policy_id).strip()
    adjustment_policy_id = str(args.adjustment_policy_id).strip()
    whitelist = parse_list(args.whitelist, DEFAULT_WHITELIST)
    excluded = parse_list(args.excluded, DEFAULT_EXCLUDED)
    ingest_ts = utc_now_iso()
    run_id = "futures_continuous_5m_builder_" + run_date + "_" + stable_id([snapshot_date, ingest_ts, roll_policy_id, adjustment_policy_id, ",".join(whitelist)])

    if roll_policy_id != ROLL_POLICY_ID:
        raise RuntimeError("Unsupported roll_policy_id: " + roll_policy_id)
    if adjustment_policy_id != ADJUSTMENT_POLICY_ID:
        raise RuntimeError("Unsupported adjustment_policy_id: " + adjustment_policy_id)
    for secid in whitelist:
        if secid in excluded:
            raise RuntimeError("Whitelisted instrument is also excluded: " + secid)

    base.assert_files_exist(root, REQUIRED_CONTRACTS)

    roll_map_raw = load_roll_map(root, data_root, snapshot_date, roll_policy_id)
    roll_blockers = validate_roll_map(roll_map_raw, roll_policy_id, adjustment_policy_id, whitelist, excluded)
    if roll_blockers:
        print_json_line("blockers", roll_blockers)
        return 1

    roll_map = normalize_roll_map(roll_map_raw)
    source_secids = source_scope_from_roll_map(roll_map, excluded)
    raw_paths = discover_raw_paths(root, data_root, source_secids, excluded, from_date, till)
    raw = read_raw(raw_paths)
    raw_blockers = validate_raw(raw, source_secids, excluded)
    if raw_blockers:
        print_json_line("blockers", raw_blockers)
        return 1

    raw = normalize_raw(raw)
    continuous = select_continuous_rows(raw, roll_map, ingest_ts, roll_policy_id, adjustment_policy_id)
    continuous_blockers = validate_continuous(continuous, excluded, roll_policy_id, adjustment_policy_id)
    if continuous_blockers:
        print_json_line("blockers", continuous_blockers)
        return 1

    partition_paths = write_partitions(root, data_root, continuous, roll_policy_id, adjustment_policy_id)
    summary = summarize(continuous, partition_paths, raw_paths, roll_map)

    print_json_line("run_id", run_id)
    print_json_line("input_artifacts", {
        "roll_map": str(roll_map_raw["_roll_map_path"].iloc[0]) if "_roll_map_path" in roll_map_raw.columns else "",
        "raw_5m_partitions_read": len(raw_paths),
    })
    print_json_line("output_artifacts_created", {
        "continuous_5m_partitions": partition_paths,
    })
    print_json_line("source_selection_summary", summary)
    print_json_line("usdrubf_identity_handling", {
        "identity_rows": int((continuous["continuous_symbol"].astype(str).str.upper() == "USDRUBF").sum()),
        "roll_boundary_rows": int(continuous.loc[continuous["continuous_symbol"].astype(str).str.upper() == "USDRUBF", "is_roll_boundary"].astype(bool).sum()) if "USDRUBF" in {str(x).upper() for x in continuous["continuous_symbol"].tolist()} else 0,
    })
    print_json_line("partial_chain_gap_handling", {
        "policy": "raw rows are emitted only when their source_secid and session_date match an explicit roll-map window; non-matching rows are omitted, not bridged",
        "roll_status_counts": {str(k): int(v) for k, v in roll_map["roll_status"].astype(str).value_counts(dropna=False).to_dict().items()},
    })
    print_json_line("builder_result_verdict", "pass")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
