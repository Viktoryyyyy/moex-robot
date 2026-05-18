#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

TARGET_FAMILIES = {"W", "MM", "MX"}
TARGET_BOARD = "RFUD"
SCHEMA_VERSION = "futures_wmmmx_contract_validation_recovery.v1"
RAW_ROOT_REL = "futures/raw_5m"
REGISTRY_PATTERN = "futures/registry/normalized/snapshot_date={snapshot_date}/normalized_registry.parquet"
SUMMARY_PATTERN = "futures/registry/normalized/snapshot_date={snapshot_date}/wmmmx_contract_validation_recovery_summary.json"


def path_from_pattern(data_root, pattern, snapshot_date):
    return Path(data_root) / pattern.replace("{snapshot_date}", snapshot_date)


def require_col(frame, candidates, label):
    for col in candidates:
        if col in frame.columns:
            return col
    raise RuntimeError(label + " column missing")


def family_col(frame):
    return require_col(frame, ["family_code", "family", "asset_code", "underlying_family", "underlying_asset"], "family")


def board_col(frame):
    return require_col(frame, ["board", "boardid", "board_id"], "board")


def parse_day(value):
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def raw_5m_evidence(data_root, snapshot_date):
    root = Path(data_root) / RAW_ROOT_REL / ("trade_date=" + snapshot_date)
    rows = []
    if not root.exists():
        return pd.DataFrame(columns=["family_code", "secid", "raw_5m_path"])
    for part in root.glob("family=*/secid=*/part.parquet"):
        family = part.parent.parent.name.replace("family=", "")
        secid = part.parent.name.replace("secid=", "")
        if family.upper() in TARGET_FAMILIES and part.is_file() and part.stat().st_size > 0:
            rows.append({"family_code": family.upper(), "secid": secid, "raw_5m_path": str(part)})
    return pd.DataFrame(rows)


def recover_registry(registry, evidence, snapshot_date):
    fam = family_col(registry)
    board = board_col(registry)
    snapshot_day = parse_day(snapshot_date)
    if snapshot_day is None:
        raise RuntimeError("invalid snapshot_date: " + str(snapshot_date))
    work = registry.copy()
    work["_family"] = work[fam].fillna("").astype(str).str.upper()
    work["_board"] = work[board].fillna("").astype(str).str.upper()
    work["_secid"] = work["secid"].fillna("").astype(str)
    ev = evidence.copy()
    ev["_family"] = ev["family_code"].fillna("").astype(str).str.upper()
    ev["_secid"] = ev["secid"].fillna("").astype(str)
    evidence_keys = set(zip(ev["_family"], ev["_secid"]))
    before = work.loc[work["_family"].isin(TARGET_FAMILIES)].copy()
    reasons = []
    recovered_index = []
    for idx, row in work.iterrows():
        family = str(row.get("_family", ""))
        secid = str(row.get("_secid", ""))
        if family not in TARGET_FAMILIES:
            continue
        if str(row.get("_board", "")) != TARGET_BOARD:
            reasons.append({"secid": secid, "family_code": family, "status": "rejected", "reason": "non_rfud_board"})
            continue
        if str(row.get("instrument_kind", "")) != "expiring_future":
            reasons.append({"secid": secid, "family_code": family, "status": "rejected", "reason": "not_expiring_future"})
            continue
        last_day = parse_day(row.get("last_trade_date", row.get("expiration_date")))
        if last_day is None:
            reasons.append({"secid": secid, "family_code": family, "status": "rejected", "reason": "missing_last_trade_date"})
            continue
        if last_day < snapshot_day:
            reasons.append({"secid": secid, "family_code": family, "status": "rejected", "reason": "expired_before_snapshot"})
            continue
        if (family, secid) not in evidence_keys:
            reasons.append({"secid": secid, "family_code": family, "status": "rejected", "reason": "missing_raw_5m_evidence"})
            continue
        recovered_index.append(idx)
        reasons.append({"secid": secid, "family_code": family, "status": "recovered", "reason": "raw_5m_registry_contract_evidence"})
    if not recovered_index:
        raise RuntimeError("No W/MM/MX contracts recovered from raw_5m evidence")
    work.loc[recovered_index, "mapping_status"] = "mapped"
    work.loc[recovered_index, "validation_status"] = "validated"
    work.loc[recovered_index, "validation_evidence"] = "raw_5m_partition_present_for_snapshot"
    work.loc[recovered_index, "validation_recovery_schema_version"] = SCHEMA_VERSION
    after = work.loc[work["_family"].isin(TARGET_FAMILIES)].copy()
    summary = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_date": snapshot_date,
        "recovered_count": int(len(recovered_index)),
        "recovered_by_family": {str(k): int(v) for k, v in pd.DataFrame(reasons).loc[lambda x: x["status"] == "recovered", "family_code"].value_counts().to_dict().items()},
        "before_status_counts": before.groupby(["_family", "mapping_status", "validation_status"]).size().reset_index(name="n").to_dict(orient="records"),
        "after_status_counts": after.groupby(["_family", "mapping_status", "validation_status"]).size().reset_index(name="n").to_dict(orient="records"),
        "evidence_rows": evidence.sort_values(["family_code", "secid"]).to_dict(orient="records"),
        "decision_rows": reasons,
        "policy": {
            "manual_allowlist_used": False,
            "raw_5m_evidence_required": True,
            "board_required": TARGET_BOARD,
            "instrument_kind_required": "expiring_future",
            "expired_before_snapshot_allowed": False,
            "continuous_builders_invoked": False
        }
    }
    drop_cols = [c for c in ["_family", "_board", "_secid"] if c in work.columns]
    return work.drop(columns=drop_cols), summary


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--data-root", default=os.getenv("MOEX_DATA_ROOT", ""))
    args = parser.parse_args()
    if not str(args.data_root).strip():
        raise RuntimeError("MOEX_DATA_ROOT is required")
    data_root = Path(args.data_root).expanduser().resolve()
    snapshot_date = str(args.snapshot_date).strip()
    registry_path = path_from_pattern(data_root, REGISTRY_PATTERN, snapshot_date)
    summary_path = path_from_pattern(data_root, SUMMARY_PATTERN, snapshot_date)
    if not registry_path.exists():
        raise FileNotFoundError("Missing normalized registry artifact: " + str(registry_path))
    registry = pd.read_parquet(registry_path)
    evidence = raw_5m_evidence(data_root, snapshot_date)
    recovered, summary = recover_registry(registry, evidence, snapshot_date)
    recovered.to_parquet(registry_path, index=False)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
