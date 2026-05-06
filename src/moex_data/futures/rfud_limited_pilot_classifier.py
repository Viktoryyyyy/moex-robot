#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path.cwd() / "src"))

import pandas as pd

from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import DEFAULT_WHITELIST
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk

SCHEMA_CLASSIFICATION = "futures_rfud_limited_pilot_classification.v1"
DEFAULT_CONFIG_REL = "configs/datasets/futures_rfud_limited_pilot_config.json"
EXPECTED_PILOT_FAMILIES = ["CR", "GD", "GL"]
CONTINUOUS_V1 = {
    "roll_policy_id": "expiration_minus_1_trading_session_v1",
    "adjustment_policy_id": "unadjusted_v1",
    "adjustment_factor": 1.0,
}
CONTRACTS = {
    "family_mapping": "contracts/datasets/futures_family_mapping_contract.md",
    "tradestats_availability": "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "futoi_availability": "contracts/datasets/futures_futoi_availability_report_contract.md",
    "liquidity_screen": "contracts/datasets/futures_liquidity_screen_contract.md",
    "history_depth_screen": "contracts/datasets/futures_history_depth_screen_contract.md",
}


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise RuntimeError("JSON root is not object: " + str(path))
    return data


def extract_contract_value(text: str, key: str) -> str:
    prefix = key + ":"
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def contract_path(root: Path, data_root: Path, contract_rel: str, snapshot_date: str) -> Path:
    text = (root / contract_rel).read_text(encoding="utf-8")
    pattern = extract_contract_value(text, "path_pattern")
    prefix = "${MOEX_DATA_ROOT}"
    if not pattern.startswith(prefix):
        raise RuntimeError("Unsupported contract path_pattern: " + contract_rel)
    tail = pattern[len(prefix):].lstrip("/")
    tail = tail.replace("{snapshot_date}", snapshot_date).replace("YYYY-MM-DD", snapshot_date)
    return data_root / tail


def resolve_data_root(raw_value: str) -> Path:
    raw = str(raw_value or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT is required for pilot evidence artifact classification")
    return Path(raw).expanduser().resolve()


def ordered_strings(value: Any) -> List[str]:
    if not isinstance(value, list):
        raise RuntimeError("Expected list value")
    return [str(x).strip() for x in value if str(x).strip()]


def validate_config(config: Dict[str, Any]) -> None:
    if ordered_strings(config.get("pilot_families")) != EXPECTED_PILOT_FAMILIES:
        raise RuntimeError("Pilot config must contain exactly CR, GD, GL in that order")
    if ordered_strings(config.get("current_whitelist_preserved")) != DEFAULT_WHITELIST:
        raise RuntimeError("Current whitelist preservation mismatch")
    if ordered_strings(config.get("excluded_deferred_preserved")) != DEFAULT_EXCLUDED:
        raise RuntimeError("Excluded/deferred preservation mismatch")
    continuous = config.get("continuous_v1") or {}
    if not isinstance(continuous, dict):
        raise RuntimeError("continuous_v1 config is not object")
    for key, expected in CONTINUOUS_V1.items():
        if continuous.get(key) != expected:
            raise RuntimeError("Continuous v1 preservation mismatch: " + key)


def normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def latest_row_by_secid(frame: pd.DataFrame, secid: str):
    if frame.empty or "secid" not in frame.columns:
        return None
    rows = frame.loc[frame["secid"].astype(str).str.upper() == secid.upper()]
    if rows.empty:
        return None
    return rows.tail(1).iloc[0]


def availability_check(frame: pd.DataFrame, secid: str, missing_reason: str, unavailable_reason: str, unresolved_reason: str) -> Tuple[str, str, str]:
    row = latest_row_by_secid(frame, secid)
    if row is None:
        return "missing", missing_reason, "missing evidence row"
    status = normalize_status(row.get("availability_status"))
    if status == "available":
        return "pass", "", "availability_status=available"
    if status == "unavailable":
        return "fail", unavailable_reason, "availability_status=unavailable"
    return "unresolved", unresolved_reason, "availability_status=" + str(row.get("availability_status", ""))


def screen_check(frame: pd.DataFrame, secid: str, status_col: str, missing_reason: str, fail_reason: str, unresolved_reason: str) -> Tuple[str, str, str]:
    row = latest_row_by_secid(frame, secid)
    if row is None:
        return "missing", missing_reason, "missing evidence row"
    if status_col not in row.index:
        return "missing", missing_reason, "missing " + status_col
    status = normalize_status(row.get(status_col))
    if status == "pass":
        return "pass", "", status_col + "=pass"
    if status == "fail":
        return "fail", fail_reason, status_col + "=fail"
    return "unresolved", unresolved_reason, status_col + "=" + str(row.get(status_col, ""))


def classification_from_checks(checks: Dict[str, str], deferral_reasons: List[str], exclusion_reasons: List[str]) -> Tuple[str, str, str, str]:
    if exclusion_reasons:
        return "excluded", "structural_exclusion", "", exclusion_reasons[0]
    required = [
        "identity_check_status",
        "board_check_status",
        "family_mapping_status",
        "raw_5m_check_status",
        "futoi_check_status",
        "liquidity_check_status",
        "history_depth_check_status",
        "expiration_policy_status",
        "calendar_quality_status",
    ]
    failed = [key for key in required if checks.get(key) != "pass"]
    if failed:
        reason = deferral_reasons[0] if deferral_reasons else "mandatory_check_unresolved"
        return "deferred", "mandatory_check_not_pass", reason, ""
    return "included", "all_limited_pilot_core_checks_pass", "", ""


def classify_pilot_instruments(config: Dict[str, Any], family_mapping: pd.DataFrame, tradestats_availability: pd.DataFrame, futoi_availability: pd.DataFrame, liquidity_screen: pd.DataFrame, history_depth_screen: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    validate_config(config)
    if family_mapping.empty:
        raise RuntimeError("family_mapping evidence artifact is empty")
    required_mapping_fields = ["secid", "board", "family_code"]
    missing = [x for x in required_mapping_fields if x not in family_mapping.columns]
    if missing:
        raise RuntimeError("family_mapping missing fields: " + ", ".join(missing))
    pilot_families = ordered_strings(config.get("pilot_families"))
    candidates = family_mapping.loc[family_mapping["family_code"].astype(str).isin(pilot_families)].copy()
    candidates = candidates.loc[candidates["board"].astype(str).str.lower() == "rfud"].copy()
    if candidates.empty:
        raise RuntimeError("No CR/GD/GL RFUD candidates found in family_mapping")
    observed_families = sorted(candidates["family_code"].astype(str).unique().tolist())
    if observed_families != sorted(pilot_families):
        raise RuntimeError("Pilot families cannot be isolated from family_mapping: " + ",".join(observed_families))
    rows: List[Dict[str, Any]] = []
    for _, item in candidates.sort_values(["family_code", "secid"]).iterrows():
        secid = str(item.get("secid", "") or "").strip()
        family = str(item.get("family_code", "") or "").strip()
        board = str(item.get("board", "rfud") or "rfud").strip()
        deferrals: List[str] = []
        exclusions: List[str] = []
        identity_status = "pass" if secid and family and board else "fail"
        if identity_status != "pass":
            exclusions.append("missing_required_identity_fields")
        board_status = "pass" if board.lower() == "rfud" else "deferred"
        if board_status != "pass":
            deferrals.append("unsupported_board_pending_review")
        mapping_status_raw = normalize_status(item.get("mapping_status", "pass"))
        family_mapping_status = "pass" if mapping_status_raw == "pass" and family else "unresolved"
        if family_mapping_status != "pass":
            deferrals.append("family_mapping_ambiguous")
        raw_5m_status, raw_5m_reason, raw_5m_note = availability_check(tradestats_availability, secid, "raw_5m_unavailable", "raw_5m_unavailable", "raw_5m_probe_failed")
        futoi_status, futoi_reason, futoi_note = availability_check(futoi_availability, secid, "futoi_unavailable", "futoi_unavailable", "futoi_unresolved")
        liquidity_status, liquidity_reason, liquidity_note = screen_check(liquidity_screen, secid, "liquidity_status", "liquidity_below_threshold", "liquidity_below_threshold", "liquidity_threshold_pending_pm_decision")
        history_status, history_reason, history_note = screen_check(history_depth_screen, secid, "history_depth_status", "history_depth_below_threshold", "history_depth_below_threshold", "history_depth_threshold_pending_pm_decision")
        for reason in [raw_5m_reason, futoi_reason, liquidity_reason, history_reason]:
            if reason:
                deferrals.append(reason)
        expiration_status = "pass" if secid and family and family != "USDRUBF" else "unresolved"
        if expiration_status != "pass":
            deferrals.append("expiration_anchor_missing")
        calendar_status = "pass" if history_status == "pass" else "unresolved"
        if calendar_status != "pass":
            deferrals.append("calendar_quality_unresolved")
        checks = {
            "identity_check_status": identity_status,
            "board_check_status": board_status,
            "family_mapping_status": family_mapping_status,
            "raw_5m_check_status": raw_5m_status,
            "futoi_check_status": futoi_status,
            "liquidity_check_status": liquidity_status,
            "history_depth_check_status": history_status,
            "expiration_policy_status": expiration_status,
            "perpetual_policy_status": "not_applicable",
            "calendar_quality_status": calendar_status,
            "continuous_eligibility_status": "not_applicable",
        }
        classification_status, classification_reason, deferral_reason, exclusion_reason = classification_from_checks(checks, deferrals, exclusions)
        rows.append({
            "classification_id": stable_id(["rfud_limited_pilot", snapshot_date, board, secid]),
            "eligibility_snapshot_date": snapshot_date,
            "secid": secid,
            "short_code": item.get("contract_code", None),
            "family_code": family,
            "board": board,
            "engine": item.get("engine", "futures"),
            "market": item.get("market", "forts"),
            "instrument_type": item.get("instrument_kind", "ordinary_expiring_future"),
            "classification_status": classification_status,
            "classification_reason": classification_reason,
            "deferral_reason": deferral_reason,
            "exclusion_reason": exclusion_reason,
            "registry_snapshot_date": item.get("snapshot_date", snapshot_date),
            "registry_source": "futures_family_mapping.parquet",
            "identity_check_status": checks["identity_check_status"],
            "board_check_status": checks["board_check_status"],
            "family_mapping_status": checks["family_mapping_status"],
            "raw_5m_check_status": checks["raw_5m_check_status"],
            "futoi_check_status": checks["futoi_check_status"],
            "liquidity_check_status": checks["liquidity_check_status"],
            "history_depth_check_status": checks["history_depth_check_status"],
            "expiration_policy_status": checks["expiration_policy_status"],
            "perpetual_policy_status": checks["perpetual_policy_status"],
            "calendar_quality_status": checks["calendar_quality_status"],
            "continuous_eligibility_status": checks["continuous_eligibility_status"],
            "source_scope": "instrument",
            "roll_policy_id": CONTINUOUS_V1["roll_policy_id"],
            "adjustment_policy_id": CONTINUOUS_V1["adjustment_policy_id"],
            "adjustment_factor": CONTINUOUS_V1["adjustment_factor"],
            "schema_version": SCHEMA_CLASSIFICATION,
            "notes": "; ".join([raw_5m_note, futoi_note, liquidity_note, history_note]),
        })
    frame = pd.DataFrame(rows)
    duplicates = int(frame.duplicated(subset=["eligibility_snapshot_date", "board", "secid"]).sum())
    if duplicates > 0:
        raise RuntimeError("pilot classification duplicate rows: " + str(duplicates))
    return frame


def read_evidence(root: Path, data_root: Path, snapshot_date: str) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    for key, rel in CONTRACTS.items():
        path = contract_path(root, data_root, rel, snapshot_date)
        if not path.exists():
            raise FileNotFoundError("Missing required evidence artifact " + key + ": " + str(path))
        out[key] = pd.read_parquet(path)
    return out


def summarize_classification(frame: pd.DataFrame) -> Dict[str, Any]:
    counts = {str(k): int(v) for k, v in frame["classification_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    by_family: Dict[str, Dict[str, int]] = {}
    for family, sub in frame.groupby("family_code", dropna=False):
        by_family[str(family)] = {str(k): int(v) for k, v in sub["classification_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    rows = []
    cols = ["family_code", "secid", "classification_status", "classification_reason", "deferral_reason", "exclusion_reason"]
    for _, row in frame[cols].iterrows():
        rows.append({str(k): (None if pd.isna(v) else v) for k, v in row.to_dict().items()})
    return {"rows": int(len(frame)), "status_counts": counts, "by_family": by_family, "instruments": rows}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    parser.add_argument("--config", default=DEFAULT_CONFIG_REL)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    root = Path.cwd().resolve()
    data_root = resolve_data_root(args.data_root)
    config = read_json(root / str(args.config))
    evidence = read_evidence(root, data_root, str(args.snapshot_date))
    classification = classify_pilot_instruments(config, evidence["family_mapping"], evidence["tradestats_availability"], evidence["futoi_availability"], evidence["liquidity_screen"], evidence["history_depth_screen"], str(args.snapshot_date))
    if args.output:
        output_path = Path(str(args.output)).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        classification.to_parquet(output_path, index=False)
        print_json_line("pilot_classification_output", str(output_path))
    print_json_line("pilot_classification_summary", summarize_classification(classification))
    non_included = classification.loc[classification["classification_status"].astype(str) != "included"]
    return 0 if non_included.empty else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
