#!/usr/bin/env python3
import argparse
import json
import os
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

SCHEMA_EXPIRATION_MAP = "futures_expiration_map.v1"
CONTRACT_NORMALIZED_REGISTRY = "contracts/datasets/futures_normalized_instrument_registry_contract.md"
CONTRACT_EXPIRATION_MAP = "contracts/datasets/futures_expiration_map_contract.md"
REQUIRED_CONTRACTS = [
    CONTRACT_NORMALIZED_REGISTRY,
    CONTRACT_EXPIRATION_MAP,
]
ALLOWED_DECISION_SOURCES = {
    "registry_expiration_date",
    "registry_last_trade_date_fallback",
    "manual_reviewed_override",
    "unresolved",
}
PERPETUAL_IDENTITIES = {"USDRUBF"}


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


def resolve_contract_path(root: Path, data_root: Path, rel_path: str, snapshot_date: str) -> Path:
    pattern = contract_value(root, rel_path, "path_pattern")
    if not pattern:
        raise RuntimeError("Contract path_pattern is missing: " + rel_path)
    prefix = "${MOEX_DATA_ROOT}"
    if not pattern.startswith(prefix):
        raise RuntimeError("Unsupported non-MOEX_DATA_ROOT path_pattern: " + rel_path)
    tail = pattern[len(prefix):].lstrip("/")
    tail = tail.replace("{snapshot_date}", snapshot_date)
    tail = tail.replace("{registry_snapshot_date}", snapshot_date)
    tail = tail.replace("YYYY-MM-DD", snapshot_date)
    return data_root / tail


def parse_source_date(value: Any) -> Tuple[Optional[str], Optional[str]]:
    if value is None or pd.isna(value):
        return None, None
    raw = str(value).strip()
    if not raw or raw.lower() in {"nan", "nat", "none", "null"}:
        return None, None
    try:
        parsed = pd.to_datetime(raw, errors="raise").date().isoformat()
    except Exception:
        return None, raw
    year = int(parsed[:4])
    if year < 2000 or year > 2200:
        return None, raw
    return parsed, None


def bool_value(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def normalized_column_value(row: pd.Series, column: str) -> Any:
    if column not in row.index:
        return None
    value = row.get(column)
    if value is None or pd.isna(value):
        return None
    return value


def choose_source_row(normalized: pd.DataFrame, secid: str) -> pd.Series:
    if "secid" not in normalized.columns:
        raise RuntimeError("normalized registry missing secid column")
    matches = normalized.loc[normalized["secid"].astype(str).str.upper() == secid.upper()].copy()
    if matches.empty:
        raise RuntimeError("Whitelisted instrument is missing from normalized registry: " + secid)
    if "snapshot_date" in matches.columns:
        matches = matches.sort_values(["snapshot_date"])
    return matches.tail(1).iloc[0]


def build_expiration_row(source_row: pd.Series, requested_secid: str, snapshot_date: str, run_id: str) -> Dict[str, Any]:
    secid = str(normalized_column_value(source_row, "secid") or requested_secid).strip()
    board = str(normalized_column_value(source_row, "board") or "rfud").strip()
    family_code = str(normalized_column_value(source_row, "family_code") or "").strip()
    instrument_kind = str(normalized_column_value(source_row, "instrument_kind") or "").strip()
    source_snapshot_id = normalized_column_value(source_row, "source_snapshot_id")
    snapshot_id = str(normalized_column_value(source_row, "snapshot_id") or source_snapshot_id or "").strip()
    expiration_date, expiration_sentinel = parse_source_date(normalized_column_value(source_row, "expiration_date"))
    last_trade_date, last_trade_sentinel = parse_source_date(normalized_column_value(source_row, "last_trade_date"))
    first_trade_date, first_trade_sentinel = parse_source_date(normalized_column_value(source_row, "first_trade_date"))
    is_perpetual = secid.upper() in PERPETUAL_IDENTITIES or bool_value(normalized_column_value(source_row, "is_perpetual_candidate")) or instrument_kind == "perpetual_future_candidate"
    sentinel_values = [x for x in [expiration_sentinel, last_trade_sentinel, first_trade_sentinel] if x]
    sentinel_date = ";".join(sentinel_values) if sentinel_values else None
    review_notes = None
    roll_anchor_date = None

    if is_perpetual:
        decision_source = "manual_reviewed_override"
        expiration_status = "perpetual_identity"
        validation_status = "pass"
        review_status = "accepted_perpetual_identity"
        review_notes = "PM accepted perpetual identity; no ordinary expiration or roll anchor required."
    elif expiration_date:
        decision_source = "registry_expiration_date"
        expiration_status = "anchored_by_expiration_date"
        validation_status = "pass"
        review_status = "accepted_registry_evidence"
        roll_anchor_date = expiration_date
    elif last_trade_date:
        decision_source = "registry_last_trade_date_fallback"
        expiration_status = "anchored_by_last_trade_date"
        validation_status = "pass"
        review_status = "accepted_registry_fallback"
        roll_anchor_date = last_trade_date
    else:
        decision_source = "unresolved"
        expiration_status = "unresolved_blocker"
        validation_status = "blocker"
        review_status = "blocked"
        review_notes = "Ordinary expiring contract has no validated expiration_date or last_trade_date."

    return {
        "expiration_map_id": "futures_expiration_map_" + stable_id([snapshot_date, board, secid, run_id]),
        "registry_snapshot_date": snapshot_date,
        "snapshot_id": snapshot_id,
        "board": board,
        "secid": secid,
        "family_code": family_code,
        "is_perpetual": bool(is_perpetual),
        "expiration_status": expiration_status,
        "decision_source": decision_source,
        "schema_version": SCHEMA_EXPIRATION_MAP,
        "expiration_date": expiration_date,
        "last_trade_date": last_trade_date,
        "first_trade_date": first_trade_date,
        "roll_anchor_date": roll_anchor_date,
        "sentinel_date": sentinel_date,
        "review_notes": review_notes,
        "validation_status": validation_status,
        "review_status": review_status,
        "source_instrument_kind": instrument_kind or None,
        "source_normalized_schema_version": normalized_column_value(source_row, "schema_version"),
        "build_run_id": run_id,
        "build_ts": utc_now_iso(),
    }


def build_expiration_map(normalized: pd.DataFrame, snapshot_date: str, whitelist: List[str], excluded: List[str], run_id: str) -> pd.DataFrame:
    rows = []
    excluded_upper = {x.upper() for x in excluded}
    for secid in whitelist:
        if secid.upper() in excluded_upper:
            raise RuntimeError("Whitelisted instrument is also excluded: " + secid)
        source_row = choose_source_row(normalized, secid)
        rows.append(build_expiration_row(source_row, secid, snapshot_date, run_id))
    return pd.DataFrame(rows)


def validate_expiration_map(frame: pd.DataFrame, whitelist: List[str], excluded: List[str]) -> List[str]:
    blockers = []
    required = [
        "expiration_map_id",
        "registry_snapshot_date",
        "snapshot_id",
        "board",
        "secid",
        "family_code",
        "is_perpetual",
        "expiration_status",
        "decision_source",
        "schema_version",
    ]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        blockers.append("missing_required_fields:" + ",".join(missing))
        return blockers
    duplicates = int(frame.duplicated(subset=["expiration_map_id", "registry_snapshot_date", "board", "secid"]).sum())
    if duplicates > 0:
        blockers.append("duplicate_primary_key_rows:" + str(duplicates))
    observed = {str(x).upper() for x in frame["secid"].astype(str).tolist()}
    expected = {str(x).upper() for x in whitelist}
    if observed != expected:
        blockers.append("whitelist_scope_mismatch")
    excluded_hits = sorted(observed.intersection({str(x).upper() for x in excluded}))
    if excluded_hits:
        blockers.append("excluded_instruments_present:" + ",".join(excluded_hits))
    invalid_sources = sorted(set(frame["decision_source"].astype(str).tolist()) - ALLOWED_DECISION_SOURCES)
    if invalid_sources:
        blockers.append("invalid_decision_source:" + ",".join(invalid_sources))
    for _, row in frame.iterrows():
        secid = str(row.get("secid", ""))
        decision_source = str(row.get("decision_source", ""))
        is_perpetual = bool(row.get("is_perpetual"))
        expiration_date = row.get("expiration_date")
        last_trade_date = row.get("last_trade_date")
        review_notes = row.get("review_notes")
        if decision_source == "registry_expiration_date" and (expiration_date is None or pd.isna(expiration_date) or str(expiration_date).strip() == ""):
            blockers.append("registry_expiration_date_without_expiration_date:" + secid)
        if decision_source == "registry_last_trade_date_fallback" and (last_trade_date is None or pd.isna(last_trade_date) or str(last_trade_date).strip() == ""):
            blockers.append("registry_last_trade_date_fallback_without_last_trade_date:" + secid)
        if decision_source == "manual_reviewed_override" and (review_notes is None or pd.isna(review_notes) or str(review_notes).strip() == ""):
            blockers.append("manual_reviewed_override_without_review_notes:" + secid)
        if not is_perpetual and decision_source == "unresolved":
            blockers.append("ordinary_unresolved_blocker:" + secid)
        if is_perpetual and row.get("roll_anchor_date") is not None and not pd.isna(row.get("roll_anchor_date")):
            blockers.append("perpetual_has_roll_anchor:" + secid)
    return blockers


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
    except Exception as exc:
        raise RuntimeError("Cannot write parquet " + str(path) + ": " + exc.__class__.__name__ + ": " + str(exc)) from exc


def summarize(frame: pd.DataFrame) -> Dict[str, Any]:
    decision_counts = {str(k): int(v) for k, v in frame["decision_source"].astype(str).value_counts(dropna=False).to_dict().items()} if "decision_source" in frame.columns else {}
    expiration_counts = {str(k): int(v) for k, v in frame["expiration_status"].astype(str).value_counts(dropna=False).to_dict().items()} if "expiration_status" in frame.columns else {}
    instruments = []
    for _, row in frame.sort_values(["family_code", "secid"]).iterrows():
        instruments.append({
            "secid": row.get("secid"),
            "family_code": row.get("family_code"),
            "is_perpetual": bool(row.get("is_perpetual")),
            "decision_source": row.get("decision_source"),
            "expiration_status": row.get("expiration_status"),
            "roll_anchor_date": row.get("roll_anchor_date"),
            "validation_status": row.get("validation_status"),
        })
    return {
        "rows": int(len(frame)),
        "decision_source_counts": decision_counts,
        "expiration_status_counts": expiration_counts,
        "instruments": instruments,
    }


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--data-root", default="")
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

    run_id = "futures_expiration_map_builder_" + run_date + "_" + stable_id([snapshot_date, utc_now_iso(), ",".join(whitelist)])
    normalized_path = resolve_contract_path(root, data_root, CONTRACT_NORMALIZED_REGISTRY, snapshot_date)
    output_path = resolve_contract_path(root, data_root, CONTRACT_EXPIRATION_MAP, snapshot_date)
    if not normalized_path.exists():
        raise FileNotFoundError("Missing normalized registry artifact: " + str(normalized_path))

    normalized = pd.read_parquet(normalized_path)
    expiration_map = build_expiration_map(normalized, snapshot_date, whitelist, excluded, run_id)
    blockers = validate_expiration_map(expiration_map, whitelist, excluded)
    write_parquet(expiration_map, output_path)

    summary = summarize(expiration_map)
    outputs = {"expiration_map": str(output_path)}
    inputs = {"normalized_registry": str(normalized_path)}
    verdict = "pass" if not blockers else "fail"

    print_json_line("input_artifacts", inputs)
    print_json_line("output_artifacts_created", outputs)
    print_json_line("expiration_map_summary", summary)
    print_json_line("excluded_instruments_confirmed", excluded)
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
