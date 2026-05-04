#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures import liquidity_history_metrics_probe_apim_calendar as apim_calendar
from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import DEFAULT_WHITELIST
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

SCHEMA_ROLL_MAP = "futures_continuous_roll_map.v1"
CONTRACT_EXPIRATION_MAP = "contracts/datasets/futures_expiration_map_contract.md"
CONTRACT_ROLL_MAP = "contracts/datasets/futures_continuous_roll_map_contract.md"
REQUIRED_CONTRACTS = [
    CONTRACT_EXPIRATION_MAP,
    CONTRACT_ROLL_MAP,
]
ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"
ADJUSTMENT_FACTOR = 1.0
CALENDAR_STATUS = "canonical_apim_futures_xml"
CALENDAR_SOURCE = "MOEX_APIM_XML:/iss/calendars"
PERPETUAL_IDENTITIES = {"USDRUBF"}
SI_CHAIN_SCOPE = ["SiM6", "SiU6", "SiZ6", "SiU7"]
EXPLICIT_GAP_AFTER_SOURCE = {
    "SiZ6": ["SiH7", "SiM7"],
}
ALLOWED_DECISION_SOURCES = {
    "registry_expiration_date",
    "registry_last_trade_date_fallback",
    "manual_reviewed_override",
    "unresolved",
}
ALLOWED_ROLL_STATUS = {
    "active_window",
    "perpetual_identity",
    "explicit_partial_chain_gap",
    "blocked_unresolved_anchor",
    "blocked_missing_next_contract",
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


def resolve_contract_path(root: Path, data_root: Path, rel_path: str, snapshot_date: str, roll_policy_id: str) -> Path:
    pattern = contract_value(root, rel_path, "path_pattern")
    if not pattern:
        raise RuntimeError("Contract path_pattern is missing: " + rel_path)
    prefix = "${MOEX_DATA_ROOT}"
    if not pattern.startswith(prefix):
        raise RuntimeError("Unsupported non-MOEX_DATA_ROOT path_pattern: " + rel_path)
    tail = pattern[len(prefix):].lstrip("/")
    tail = tail.replace("{snapshot_date}", snapshot_date)
    tail = tail.replace("{registry_snapshot_date}", snapshot_date)
    tail = tail.replace("{roll_policy_id}", roll_policy_id)
    tail = tail.replace("YYYY-MM-DD", snapshot_date)
    return data_root / tail


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


def parse_date_str(value: str) -> datetime.date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def calendar_window(expiration_map: pd.DataFrame, snapshot_date: str) -> Dict[str, str]:
    dates: List[str] = []
    for column in ["first_trade_date", "last_trade_date", "expiration_date", "roll_anchor_date"]:
        if column in expiration_map.columns:
            for value in expiration_map[column].tolist():
                date_value = clean_date(value)
                if date_value:
                    dates.append(date_value)
    dates.append(snapshot_date)
    start = min(dates)
    end = max(dates)
    start_dt = parse_date_str(start) - timedelta(days=14)
    end_dt = parse_date_str(end) + timedelta(days=14)
    return {"from": start_dt.isoformat(), "till": end_dt.isoformat()}


def fetch_canonical_sessions(expiration_map: pd.DataFrame, snapshot_date: str, timeout: float, iss_base_url: str) -> List[str]:
    window = calendar_window(expiration_map, snapshot_date)
    sessions, status = apim_calendar.fetch_futures_calendar(window["from"], window["till"], timeout, iss_base_url)
    if sessions is None or status != CALENDAR_STATUS:
        raise RuntimeError("Canonical futures calendar cannot be resolved: " + str(status))
    ordered = sorted([str(x) for x in sessions])
    if not ordered:
        raise RuntimeError("Canonical futures calendar returned zero sessions")
    return ordered


def previous_session_before(ordered_sessions: List[str], anchor_date: str) -> str:
    candidates = [x for x in ordered_sessions if x < anchor_date]
    if not candidates:
        raise RuntimeError("Previous futures trading session cannot be calculated before " + str(anchor_date))
    return candidates[-1]


def next_session_after(ordered_sessions: List[str], date_value: str) -> str:
    candidates = [x for x in ordered_sessions if x > date_value]
    if not candidates:
        raise RuntimeError("Next futures trading session cannot be calculated after " + str(date_value))
    return candidates[0]


def first_session_on_or_after(ordered_sessions: List[str], date_value: str) -> str:
    candidates = [x for x in ordered_sessions if x >= date_value]
    if not candidates:
        raise RuntimeError("Futures trading session cannot be calculated on or after " + str(date_value))
    return candidates[0]


def ordinary_anchor(row: pd.Series) -> Optional[str]:
    roll_anchor_date = clean_date(row.get("roll_anchor_date"))
    if roll_anchor_date:
        return roll_anchor_date
    decision_source = clean_text(row.get("decision_source")) or ""
    if decision_source == "registry_expiration_date":
        return clean_date(row.get("expiration_date"))
    if decision_source == "registry_last_trade_date_fallback":
        return clean_date(row.get("last_trade_date"))
    return None


def source_rows_for_scope(expiration_map: pd.DataFrame, whitelist: List[str], excluded: List[str]) -> pd.DataFrame:
    if "secid" not in expiration_map.columns:
        raise RuntimeError("expiration map missing secid column")
    excluded_upper = {x.upper() for x in excluded}
    rows = []
    for secid in whitelist:
        if secid.upper() in excluded_upper:
            raise RuntimeError("Whitelisted instrument is also excluded: " + secid)
        matched = expiration_map.loc[expiration_map["secid"].astype(str).str.upper() == secid.upper()].copy()
        if matched.empty:
            raise RuntimeError("Whitelisted instrument is missing from expiration map: " + secid)
        rows.append(matched.tail(1).iloc[0].to_dict())
    out = pd.DataFrame(rows)
    observed_upper = {str(x).upper() for x in out["secid"].astype(str).tolist()}
    excluded_hits = sorted(observed_upper.intersection(excluded_upper))
    if excluded_hits:
        raise RuntimeError("Excluded instruments present in expiration map scope: " + ",".join(excluded_hits))
    return out


def si_scope_rows(scoped: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for secid in SI_CHAIN_SCOPE:
        matched = scoped.loc[scoped["secid"].astype(str).str.upper() == secid.upper()].copy()
        if not matched.empty:
            rows.append(matched.tail(1).iloc[0].to_dict())
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out["_anchor_sort"] = out.apply(lambda row: ordinary_anchor(row) or "9999-12-31", axis=1)
    return out.sort_values(["family_code", "_anchor_sort", "secid"]).reset_index(drop=True)


def build_perpetual_row(row: pd.Series, snapshot_date: str, run_id: str) -> Dict[str, Any]:
    secid = str(clean_text(row.get("secid")) or "USDRUBF")
    board = str(clean_text(row.get("board")) or "rfud")
    family_code = str(clean_text(row.get("family_code")) or secid)
    decision_source = str(clean_text(row.get("decision_source")) or "manual_reviewed_override")
    return {
        "roll_map_id": "futures_continuous_roll_map_" + stable_id([snapshot_date, ROLL_POLICY_ID, board, secid, run_id]),
        "snapshot_date": snapshot_date,
        "family_code": family_code,
        "continuous_symbol": secid,
        "board": board,
        "source_secid": secid,
        "source_contract_code": secid,
        "next_secid": None,
        "next_contract_code": None,
        "is_perpetual": True,
        "roll_required": False,
        "roll_anchor_date": None,
        "roll_date": None,
        "valid_from_session": snapshot_date,
        "valid_through_session": None,
        "calendar_status": CALENDAR_STATUS,
        "calendar_source": CALENDAR_SOURCE,
        "roll_policy_id": ROLL_POLICY_ID,
        "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
        "adjustment_factor": ADJUSTMENT_FACTOR,
        "decision_source": decision_source,
        "roll_status": "perpetual_identity",
        "schema_version": SCHEMA_ROLL_MAP,
        "review_notes": "Perpetual identity row; no fake expiration, no ordinary roll schedule.",
        "source_expiration_status": clean_text(row.get("expiration_status")),
        "source_expiration_map_id": clean_text(row.get("expiration_map_id")),
        "build_run_id": run_id,
        "build_ts": utc_now_iso(),
    }


def build_si_rows(si_rows: pd.DataFrame, snapshot_date: str, run_id: str, ordered_sessions: List[str], excluded: List[str]) -> List[Dict[str, Any]]:
    if si_rows.empty:
        return []
    rows: List[Dict[str, Any]] = []
    previous_roll_date: Optional[str] = None
    excluded_upper = {x.upper() for x in excluded}
    records = [si_rows.iloc[i] for i in range(len(si_rows))]
    for idx, row in enumerate(records):
        secid = str(clean_text(row.get("secid")) or "")
        board = str(clean_text(row.get("board")) or "rfud")
        family_code = str(clean_text(row.get("family_code")) or "Si")
        decision_source = str(clean_text(row.get("decision_source")) or "")
        if decision_source not in ALLOWED_DECISION_SOURCES:
            raise RuntimeError("Invalid decision_source for " + secid + ": " + decision_source)
        is_perpetual = bool_value(row.get("is_perpetual"))
        if is_perpetual:
            raise RuntimeError("Si chain row unexpectedly marked perpetual: " + secid)
        anchor = ordinary_anchor(row)
        roll_date = None
        if anchor:
            roll_date = previous_session_before(ordered_sessions, anchor)
        if idx == 0:
            first_trade = clean_date(row.get("first_trade_date")) or snapshot_date
            valid_from = first_session_on_or_after(ordered_sessions, first_trade)
        else:
            if previous_roll_date is None:
                raise RuntimeError("Previous roll_date missing before " + secid)
            valid_from = next_session_after(ordered_sessions, previous_roll_date)
        valid_through = roll_date
        next_secid = None
        next_contract_code = None
        if idx + 1 < len(records):
            next_secid = str(clean_text(records[idx + 1].get("secid")) or "")
            next_contract_code = next_secid
        missing_explicit = [x for x in EXPLICIT_GAP_AFTER_SOURCE.get(secid, []) if x.upper() in excluded_upper]
        if decision_source == "unresolved" or not anchor or not roll_date:
            roll_status = "blocked_unresolved_anchor"
            review_notes = "Ordinary source contract has unresolved or missing roll anchor."
        elif missing_explicit:
            roll_status = "explicit_partial_chain_gap"
            review_notes = "Partial chain gap is explicit; excluded contracts between source and next are " + ",".join(missing_explicit) + "."
        elif not next_secid:
            roll_status = "blocked_missing_next_contract"
            review_notes = "No included next source contract exists in current Slice 1 scope."
        else:
            roll_status = "active_window"
            review_notes = None
        out = {
            "roll_map_id": "futures_continuous_roll_map_" + stable_id([snapshot_date, ROLL_POLICY_ID, board, secid, run_id]),
            "snapshot_date": snapshot_date,
            "family_code": family_code,
            "continuous_symbol": family_code,
            "board": board,
            "source_secid": secid,
            "source_contract_code": secid,
            "next_secid": next_secid,
            "next_contract_code": next_contract_code,
            "is_perpetual": False,
            "roll_required": True,
            "roll_anchor_date": anchor,
            "roll_date": roll_date,
            "valid_from_session": valid_from,
            "valid_through_session": valid_through,
            "calendar_status": CALENDAR_STATUS,
            "calendar_source": CALENDAR_SOURCE,
            "roll_policy_id": ROLL_POLICY_ID,
            "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
            "adjustment_factor": ADJUSTMENT_FACTOR,
            "decision_source": decision_source,
            "roll_status": roll_status,
            "schema_version": SCHEMA_ROLL_MAP,
            "review_notes": review_notes,
            "source_expiration_status": clean_text(row.get("expiration_status")),
            "source_expiration_map_id": clean_text(row.get("expiration_map_id")),
            "build_run_id": run_id,
            "build_ts": utc_now_iso(),
        }
        rows.append(out)
        previous_roll_date = roll_date
    return rows


def build_roll_map(expiration_map: pd.DataFrame, snapshot_date: str, whitelist: List[str], excluded: List[str], ordered_sessions: List[str], run_id: str) -> pd.DataFrame:
    scoped = source_rows_for_scope(expiration_map, whitelist, excluded)
    rows: List[Dict[str, Any]] = []
    si_rows = si_scope_rows(scoped)
    rows.extend(build_si_rows(si_rows, snapshot_date, run_id, ordered_sessions, excluded))
    for _, row in scoped.sort_values(["family_code", "secid"]).iterrows():
        secid = str(clean_text(row.get("secid")) or "")
        if secid.upper() in PERPETUAL_IDENTITIES:
            rows.append(build_perpetual_row(row, snapshot_date, run_id))
    if not rows:
        raise RuntimeError("Roll map builder produced zero rows")
    return pd.DataFrame(rows)


def validate_roll_map(frame: pd.DataFrame, whitelist: List[str], excluded: List[str], ordered_sessions: List[str]) -> List[str]:
    blockers: List[str] = []
    required = [
        "roll_map_id",
        "snapshot_date",
        "family_code",
        "continuous_symbol",
        "board",
        "source_secid",
        "source_contract_code",
        "next_secid",
        "next_contract_code",
        "is_perpetual",
        "roll_required",
        "roll_anchor_date",
        "roll_date",
        "valid_from_session",
        "valid_through_session",
        "calendar_status",
        "calendar_source",
        "roll_policy_id",
        "adjustment_policy_id",
        "adjustment_factor",
        "decision_source",
        "roll_status",
        "schema_version",
        "review_notes",
    ]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        blockers.append("missing_required_fields:" + ",".join(missing))
        return blockers
    pk_fields = ["roll_map_id", "snapshot_date", "family_code", "source_secid", "valid_from_session"]
    duplicates = int(frame.duplicated(subset=pk_fields).sum())
    if duplicates > 0:
        blockers.append("duplicate_primary_key_rows:" + str(duplicates))
    observed = {str(x).upper() for x in frame["source_secid"].astype(str).tolist()}
    expected = {str(x).upper() for x in whitelist}
    if observed != expected:
        blockers.append("whitelist_scope_mismatch")
    excluded_upper = {str(x).upper() for x in excluded}
    source_hits = sorted(observed.intersection(excluded_upper))
    next_values = {str(x).upper() for x in frame["next_secid"].dropna().astype(str).tolist() if str(x).strip()}
    next_hits = sorted(next_values.intersection(excluded_upper))
    if source_hits:
        blockers.append("excluded_source_instruments_present:" + ",".join(source_hits))
    if next_hits:
        blockers.append("excluded_next_instruments_present:" + ",".join(next_hits))
    bad_schema = frame.loc[frame["schema_version"].astype(str) != SCHEMA_ROLL_MAP]
    if not bad_schema.empty:
        blockers.append("invalid_schema_version_rows:" + str(len(bad_schema)))
    bad_calendar = frame.loc[frame["calendar_status"].astype(str) != CALENDAR_STATUS]
    if not bad_calendar.empty:
        blockers.append("invalid_calendar_status_rows:" + str(len(bad_calendar)))
    bad_roll_policy = frame.loc[frame["roll_policy_id"].astype(str) != ROLL_POLICY_ID]
    if not bad_roll_policy.empty:
        blockers.append("invalid_roll_policy_rows:" + str(len(bad_roll_policy)))
    bad_adjustment_policy = frame.loc[frame["adjustment_policy_id"].astype(str) != ADJUSTMENT_POLICY_ID]
    if not bad_adjustment_policy.empty:
        blockers.append("invalid_adjustment_policy_rows:" + str(len(bad_adjustment_policy)))
    bad_adjustment_factor = frame.loc[pd.to_numeric(frame["adjustment_factor"], errors="coerce") != ADJUSTMENT_FACTOR]
    if not bad_adjustment_factor.empty:
        blockers.append("invalid_adjustment_factor_rows:" + str(len(bad_adjustment_factor)))
    bad_sources = sorted(set(frame["decision_source"].astype(str).tolist()) - ALLOWED_DECISION_SOURCES)
    if bad_sources:
        blockers.append("invalid_decision_source:" + ",".join(bad_sources))
    bad_status = sorted(set(frame["roll_status"].astype(str).tolist()) - ALLOWED_ROLL_STATUS)
    if bad_status:
        blockers.append("invalid_roll_status:" + ",".join(bad_status))
    calendar_set = set(ordered_sessions)
    for _, row in frame.iterrows():
        secid = str(row.get("source_secid"))
        is_perpetual = bool_value(row.get("is_perpetual"))
        status = str(row.get("roll_status"))
        anchor = clean_date(row.get("roll_anchor_date"))
        roll_date = clean_date(row.get("roll_date"))
        valid_from = clean_date(row.get("valid_from_session"))
        valid_through = clean_date(row.get("valid_through_session"))
        if valid_from is None:
            blockers.append("missing_valid_from_session:" + secid)
        elif valid_from not in calendar_set and not is_perpetual:
            blockers.append("valid_from_not_calendar_session:" + secid)
        if is_perpetual:
            if secid.upper() != "USDRUBF":
                blockers.append("unexpected_perpetual_source:" + secid)
            if bool_value(row.get("roll_required")):
                blockers.append("perpetual_roll_required_true:" + secid)
            for key in ["next_secid", "next_contract_code", "roll_anchor_date", "roll_date"]:
                if clean_text(row.get(key)) is not None:
                    blockers.append("perpetual_non_null_" + key + ":" + secid)
            if status != "perpetual_identity":
                blockers.append("perpetual_invalid_roll_status:" + secid)
            if str(row.get("continuous_symbol")) != "USDRUBF":
                blockers.append("perpetual_invalid_continuous_symbol:" + secid)
        else:
            if not bool_value(row.get("roll_required")):
                blockers.append("ordinary_roll_required_false:" + secid)
            if not anchor:
                blockers.append("ordinary_missing_roll_anchor:" + secid)
            if not roll_date:
                blockers.append("ordinary_missing_roll_date:" + secid)
            elif roll_date != previous_session_before(ordered_sessions, anchor):
                blockers.append("ordinary_roll_date_not_previous_session:" + secid)
            if valid_through and valid_from and valid_from > valid_through:
                blockers.append("invalid_window_order:" + secid)
            if status == "active_window" and clean_text(row.get("next_secid")) is None:
                blockers.append("active_window_missing_next_contract:" + secid)
            if status in {"explicit_partial_chain_gap", "blocked_missing_next_contract", "blocked_unresolved_anchor"} and clean_text(row.get("review_notes")) is None:
                blockers.append("explicit_status_missing_review_notes:" + secid)
    ordinary = frame.loc[frame["is_perpetual"].map(lambda x: not bool_value(x))].copy()
    for family_code, group in ordinary.groupby("family_code"):
        ordered = group.sort_values(["valid_from_session", "source_secid"]).reset_index(drop=True)
        previous_through: Optional[str] = None
        for _, row in ordered.iterrows():
            valid_from = clean_date(row.get("valid_from_session"))
            valid_through = clean_date(row.get("valid_through_session"))
            if previous_through and valid_from and valid_from <= previous_through:
                blockers.append("overlapping_roll_map_windows:" + str(family_code))
                break
            previous_through = valid_through or previous_through
    return blockers


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
    except Exception as exc:
        raise RuntimeError("Cannot write parquet " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc


def summarize(frame: pd.DataFrame) -> Dict[str, Any]:
    status_counts = {str(k): int(v) for k, v in frame["roll_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    family_counts = {str(k): int(v) for k, v in frame["family_code"].astype(str).value_counts(dropna=False).to_dict().items()}
    instruments = []
    for _, row in frame.sort_values(["family_code", "source_secid"]).iterrows():
        instruments.append({
            "source_secid": row.get("source_secid"),
            "family_code": row.get("family_code"),
            "continuous_symbol": row.get("continuous_symbol"),
            "next_secid": row.get("next_secid"),
            "roll_required": bool_value(row.get("roll_required")),
            "roll_anchor_date": row.get("roll_anchor_date"),
            "roll_date": row.get("roll_date"),
            "valid_from_session": row.get("valid_from_session"),
            "valid_through_session": row.get("valid_through_session"),
            "roll_status": row.get("roll_status"),
            "decision_source": row.get("decision_source"),
            "adjustment_factor": row.get("adjustment_factor"),
        })
    return {
        "rows": int(len(frame)),
        "roll_status_counts": status_counts,
        "family_counts": family_counts,
        "instruments": instruments,
    }


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--whitelist", default=",".join(DEFAULT_WHITELIST))
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    args = parser.parse_args()

    root = repo_root()
    snapshot_date = str(args.snapshot_date).strip()
    run_date = str(args.run_date).strip()
    data_root = base.resolve_data_root(args)
    whitelist = parse_list(args.whitelist, DEFAULT_WHITELIST)
    excluded = parse_list(args.excluded, DEFAULT_EXCLUDED)

    base.assert_files_exist(root, REQUIRED_CONTRACTS)
    run_id = "futures_continuous_roll_map_builder_" + run_date + "_" + stable_id([snapshot_date, utc_now_iso(), ",".join(whitelist), ROLL_POLICY_ID])
    expiration_map_path = resolve_contract_path(root, data_root, CONTRACT_EXPIRATION_MAP, snapshot_date, ROLL_POLICY_ID)
    output_path = resolve_contract_path(root, data_root, CONTRACT_ROLL_MAP, snapshot_date, ROLL_POLICY_ID)
    if not expiration_map_path.exists():
        raise FileNotFoundError("Missing expiration map artifact: " + str(expiration_map_path))

    expiration_map = pd.read_parquet(expiration_map_path)
    ordered_sessions = fetch_canonical_sessions(expiration_map, snapshot_date, float(args.timeout), str(args.iss_base_url))
    roll_map = build_roll_map(expiration_map, snapshot_date, whitelist, excluded, ordered_sessions, run_id)
    blockers = validate_roll_map(roll_map, whitelist, excluded, ordered_sessions)
    write_parquet(roll_map, output_path)

    summary = summarize(roll_map)
    inputs = {
        "expiration_map": str(expiration_map_path),
        "calendar_source": CALENDAR_SOURCE,
        "calendar_status": CALENDAR_STATUS,
    }
    outputs = {"continuous_roll_map": str(output_path)}
    verdict = "pass" if not blockers else "fail"

    print_json_line("input_artifacts", inputs)
    print_json_line("output_artifacts_created", outputs)
    print_json_line("calendar_binding_summary", {
        "calendar_status": CALENDAR_STATUS,
        "calendar_source": CALENDAR_SOURCE,
        "session_min": ordered_sessions[0],
        "session_max": ordered_sessions[-1],
        "session_count": len(ordered_sessions),
    })
    print_json_line("roll_map_summary", summary)
    print_json_line("si_chain_handling", {
        "scope": SI_CHAIN_SCOPE,
        "excluded_contracts_not_promoted": excluded,
        "partial_gap_sources": EXPLICIT_GAP_AFTER_SOURCE,
    })
    print_json_line("usdrubf_identity_handling", {
        "source_secid": "USDRUBF",
        "roll_required": False,
        "roll_status": "perpetual_identity",
        "adjustment_factor": ADJUSTMENT_FACTOR,
    })
    print_json_line("builder_result_verdict", verdict)
    if blockers:
        print_json_line("blockers", blockers)
    return 0 if verdict == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
