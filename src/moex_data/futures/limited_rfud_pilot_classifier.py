#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

sys.path.insert(0, str(Path.cwd() / "src"))

import pandas as pd

from moex_data.futures.slice1_common import DEFAULT_EXCLUDED, DEFAULT_WHITELIST, print_json_line, today_msk

SCHEMA_VERSION = "futures_limited_rfud_pilot_classification.v1"
CONFIG_PATH = "configs/datasets/futures_limited_rfud_pilot_config.json"
OUTPUT_CONTRACT = "contracts/datasets/futures_limited_rfud_pilot_classification_contract.md"
INPUT_CONTRACTS = {
    "normalized_registry": "contracts/datasets/futures_normalized_instrument_registry_contract.md",
    "family_mapping": "contracts/datasets/futures_family_mapping_contract.md",
    "tradestats_availability": "contracts/datasets/futures_algopack_tradestats_availability_report_contract.md",
    "futoi_availability": "contracts/datasets/futures_futoi_availability_report_contract.md",
    "liquidity_screen": "contracts/datasets/futures_liquidity_screen_contract.md",
    "history_depth_screen": "contracts/datasets/futures_history_depth_screen_contract.md",
}
EXPECTED_PILOT_FAMILIES = ["CR", "GD", "GL"]
REQUIRED_CONTINUOUS_V1 = {
    "roll_policy_id": "expiration_minus_1_trading_session_v1",
    "adjustment_policy_id": "unadjusted_v1",
    "adjustment_factor": 1.0,
}


def read_json(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError("JSON root is not object: " + str(path))
    return data


def contract_value(path: Path, key: str) -> str:
    prefix = key + ":"
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return ""


def contract_path(root: Path, data_root: Path, contract_rel: str, snapshot_date: str) -> Path:
    pattern = contract_value(root / contract_rel, "path_pattern")
    if not pattern.startswith("${MOEX_DATA_ROOT}"):
        raise RuntimeError("Unsupported or missing path_pattern: " + contract_rel)
    tail = pattern[len("${MOEX_DATA_ROOT}"):].lstrip("/")
    tail = tail.replace("{snapshot_date}", snapshot_date).replace("YYYY-MM-DD", snapshot_date)
    return data_root / tail


def assert_files(root: Path, rels: Iterable[str]) -> None:
    missing = [rel for rel in rels if not (root / rel).exists()]
    if missing:
        raise FileNotFoundError("Missing required SoT files: " + ", ".join(missing))


def clean_list(values: Iterable[Any]) -> List[str]:
    return [str(x).strip() for x in values if str(x or "").strip()]


def load_config(root: Path) -> Dict[str, Any]:
    config = read_json(root / CONFIG_PATH)
    if clean_list(config.get("pilot_families") or []) != EXPECTED_PILOT_FAMILIES:
        raise RuntimeError("Pilot families must be exactly CR,GD,GL")
    if clean_list(config.get("preserved_current_whitelist") or []) != list(DEFAULT_WHITELIST):
        raise RuntimeError("Current whitelist preservation config drifted")
    if clean_list(config.get("preserved_excluded_deferred_instruments") or []) != list(DEFAULT_EXCLUDED):
        raise RuntimeError("Excluded/deferred preservation config drifted")
    policy = config.get("continuous_v1_policy") or {}
    for key, expected in REQUIRED_CONTINUOUS_V1.items():
        actual = policy.get(key)
        if key == "adjustment_factor":
            if float(actual) != float(expected):
                raise RuntimeError("Continuous v1 adjustment_factor drifted")
        elif str(actual) != str(expected):
            raise RuntimeError("Continuous v1 " + key + " drifted")
    return config


def colmap(frame: pd.DataFrame) -> Dict[str, str]:
    return {str(col).lower(): col for col in frame.columns}


def get_value(row: pd.Series, names: Iterable[str]) -> Any:
    index = {str(key).lower(): key for key in row.index}
    for name in names:
        key = index.get(str(name).lower())
        if key is not None:
            return row.get(key)
    return None


def text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def latest_by_secid(frame: pd.DataFrame) -> Dict[str, pd.Series]:
    cols = colmap(frame)
    if frame.empty or "secid" not in cols:
        return {}
    out: Dict[str, pd.Series] = {}
    for _, row in frame.iterrows():
        secid = text(row.get(cols["secid"])).upper()
        if secid:
            out[secid] = row
    return out


def status(index: Dict[str, pd.Series], secid: str, names: Iterable[str]) -> str:
    row = index.get(secid.upper())
    if row is None:
        return ""
    return text(get_value(row, names)).lower()


def has_expiration(row: pd.Series) -> bool:
    for name in ["expiration_date", "last_trade_date", "lasttradedate", "matdate", "mat_date"]:
        value = text(get_value(row, [name])).lower()
        if value and value not in ["none", "nan", "nat", "0000-00-00"]:
            return True
    return False


def candidates(normalized: pd.DataFrame, families: List[str]) -> pd.DataFrame:
    cols = colmap(normalized)
    for name in ["secid", "family_code", "board"]:
        if name not in cols:
            raise RuntimeError("Normalized registry missing required field: " + name)
    selected = normalized.loc[
        normalized[cols["family_code"]].astype(str).str.upper().isin([x.upper() for x in families])
        & (normalized[cols["board"]].astype(str).str.lower() == "rfud")
    ].copy()
    return selected.sort_values([cols["family_code"], cols["secid"]]).reset_index(drop=True)


def classify_pilot(normalized: pd.DataFrame, family_mapping: pd.DataFrame, tradestats_availability: pd.DataFrame, futoi_availability: pd.DataFrame, liquidity_screen: pd.DataFrame, history_depth_screen: pd.DataFrame, snapshot_date: str, config: Dict[str, Any]) -> pd.DataFrame:
    families = clean_list(config.get("pilot_families") or [])
    family_index = latest_by_secid(family_mapping)
    trade_index = latest_by_secid(tradestats_availability)
    futoi_index = latest_by_secid(futoi_availability)
    liquidity_index = latest_by_secid(liquidity_screen)
    history_index = latest_by_secid(history_depth_screen)
    rows = []
    for _, row in candidates(normalized, families).iterrows():
        secid = text(get_value(row, ["secid"]))
        family = text(get_value(row, ["family_code"]))
        board = text(get_value(row, ["board"])) or "rfud"
        engine = text(get_value(row, ["engine"])) or "futures"
        market = text(get_value(row, ["market"])) or "forts"
        instrument_type = text(get_value(row, ["instrument_type", "instrument_kind"])) or "ordinary_expiring_future"
        identity = "pass" if secid and family and board else "fail"
        board_check = "pass" if board.lower() == "rfud" and engine.lower() == "futures" and market.lower() == "forts" else "fail"
        family_check = status(family_index, secid, ["mapping_status", "family_mapping_status"]) or "unresolved"
        raw_check = "pass" if status(trade_index, secid, ["availability_status", "raw_5m_check_status"]) == "available" else "fail"
        futoi_raw = status(futoi_index, secid, ["availability_status", "futoi_check_status"])
        futoi_check = "pass" if futoi_raw == "available" else "fail"
        liquidity_raw = status(liquidity_index, secid, ["liquidity_status", "liquidity_check_status"])
        liquidity_check = "pass" if liquidity_raw == "pass" else "fail"
        history_raw = status(history_index, secid, ["history_depth_status", "history_depth_check_status"])
        history_check = "pass" if history_raw == "pass" else "fail"
        is_perpetual = secid.upper() == "USDRUBF" or "perpetual" in instrument_type.lower()
        if secid.upper() == "USDRUBF":
            expiration_check = "not_applicable"
            perpetual_check = "pass"
            continuous_check = "not_applicable"
        elif is_perpetual:
            expiration_check = "not_applicable"
            perpetual_check = "fail"
            continuous_check = "fail"
        else:
            expiration_check = "pass" if has_expiration(row) else "fail"
            perpetual_check = "not_applicable"
            continuous_check = "pass" if expiration_check == "pass" else "fail"
        calendar_check = "pass" if history_check == "pass" else "fail"
        calendar_denominator = status(history_index, secid, ["calendar_denominator_status"])
        calendar_status = status(history_index, secid, ["calendar_status"])
        if calendar_denominator and calendar_denominator not in ["canonical_apim_futures_xml", "pass"]:
            calendar_check = "fail"
        if calendar_status and calendar_status not in ["computed", "pass"]:
            calendar_check = "fail"
        classification = "deferred"
        reason = "mandatory_check_failed"
        defer = ""
        exclude = ""
        if identity != "pass":
            classification = "excluded"
            reason = "structural_invalidity"
            exclude = "missing_required_identity_fields"
        elif board_check != "pass":
            classification = "excluded" if engine.lower() != "futures" or market.lower() != "forts" else "deferred"
            reason = "unsupported_engine_market" if classification == "excluded" else "unsupported_board"
            exclude = "unsupported_engine_market" if classification == "excluded" else ""
            defer = "unsupported_board_pending_review" if classification == "deferred" else ""
        elif family_check != "pass":
            reason = "mandatory_check_unresolved"
            defer = "family_mapping_ambiguous"
        elif raw_check != "pass":
            defer = "raw_5m_unavailable"
        elif futoi_check != "pass":
            defer = "futoi_unavailable" if futoi_raw != "unresolved" else "futoi_unresolved"
        elif liquidity_check != "pass":
            defer = "liquidity_below_threshold" if liquidity_raw == "fail" else "quality_probe_partial_failure"
        elif history_check != "pass":
            defer = "history_depth_below_threshold" if history_raw == "fail" else "quality_probe_partial_failure"
        elif expiration_check == "fail":
            reason = "mandatory_check_unresolved"
            defer = "expiration_anchor_missing"
        elif perpetual_check == "fail":
            reason = "mandatory_check_unresolved"
            defer = "perpetual_candidate_pending_review"
        elif calendar_check != "pass":
            reason = "mandatory_check_unresolved"
            defer = "calendar_quality_unresolved"
        elif continuous_check == "fail":
            reason = "mandatory_check_unresolved"
            defer = "continuous_roll_map_not_buildable"
        else:
            classification = "included"
            reason = "all_mandatory_checks_passed"
        rows.append({
            "eligibility_snapshot_date": snapshot_date,
            "secid": secid,
            "short_code": text(get_value(row, ["short_code", "shortname"])) or secid,
            "family_code": family,
            "board": board,
            "engine": engine,
            "market": market,
            "instrument_type": instrument_type,
            "classification_status": classification,
            "classification_reason": reason,
            "deferral_reason": defer,
            "exclusion_reason": exclude,
            "registry_snapshot_date": snapshot_date,
            "registry_source": "futures_normalized_instrument_registry",
            "identity_check_status": identity,
            "board_check_status": board_check,
            "family_mapping_status": family_check,
            "raw_5m_check_status": raw_check,
            "futoi_check_status": futoi_check,
            "liquidity_check_status": liquidity_check,
            "history_depth_check_status": history_check,
            "expiration_policy_status": expiration_check,
            "perpetual_policy_status": perpetual_check,
            "calendar_quality_status": calendar_check,
            "continuous_eligibility_status": continuous_check,
            "source_scope": "limited_rfud_pilot_CR_GD_GL",
            "roll_policy_id": config["continuous_v1_policy"]["roll_policy_id"],
            "adjustment_policy_id": config["continuous_v1_policy"]["adjustment_policy_id"],
            "adjustment_factor": float(config["continuous_v1_policy"]["adjustment_factor"]),
            "notes": None if classification == "included" else "missing, unresolved, failed, or uncontracted mandatory checks do not include",
            "schema_version": SCHEMA_VERSION,
        })
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No CR/GD/GL RFUD pilot candidates found in normalized registry")
    actual = set(frame["family_code"].astype(str).str.upper().unique().tolist())
    if not actual.issubset(set([x.upper() for x in families])):
        raise RuntimeError("Pilot classifier selected families outside CR/GD/GL")
    if int(frame.duplicated(subset=["eligibility_snapshot_date", "board", "secid"]).sum()) > 0:
        raise RuntimeError("Duplicate pilot classification rows")
    return frame.sort_values(["family_code", "secid"]).reset_index(drop=True)


def summarize(frame: pd.DataFrame) -> Dict[str, Any]:
    items = []
    for _, row in frame[["secid", "family_code", "classification_status", "classification_reason", "deferral_reason", "exclusion_reason"]].iterrows():
        items.append({str(k): (None if pd.isna(v) else v) for k, v in row.to_dict().items()})
    return {
        "rows": int(len(frame)),
        "status_counts": {str(k): int(v) for k, v in frame["classification_status"].astype(str).value_counts(dropna=False).to_dict().items()},
        "by_family": {str(f): {str(k): int(v) for k, v in sub["classification_status"].astype(str).value_counts(dropna=False).to_dict().items()} for f, sub in frame.groupby("family_code", dropna=False)},
        "instruments": items,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    root = Path.cwd().resolve()
    raw_root = str(args.data_root or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw_root:
        raise RuntimeError("MOEX_DATA_ROOT is required")
    data_root = Path(raw_root).expanduser().resolve()
    snapshot_date = str(args.snapshot_date).strip()
    assert_files(root, list(INPUT_CONTRACTS.values()) + [OUTPUT_CONTRACT, CONFIG_PATH])
    config = load_config(root)
    frames = {}
    for key, contract_rel in INPUT_CONTRACTS.items():
        path = contract_path(root, data_root, contract_rel, snapshot_date)
        if not path.exists():
            raise FileNotFoundError("Missing required evidence artifact for " + key + ": " + str(path))
        frames[key] = pd.read_parquet(path)
    output_path = Path(args.output).expanduser().resolve() if args.output else contract_path(root, data_root, OUTPUT_CONTRACT, snapshot_date)
    result = classify_pilot(frames["normalized_registry"], frames["family_mapping"], frames["tradestats_availability"], frames["futoi_availability"], frames["liquidity_screen"], frames["history_depth_screen"], snapshot_date, config)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)
    print_json_line("pilot_classification_artifact", str(output_path))
    print_json_line("pilot_classification_summary", summarize(result))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
