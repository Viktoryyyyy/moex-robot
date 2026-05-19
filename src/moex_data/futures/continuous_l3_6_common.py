from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"
ADJUSTMENT_FACTOR = 1.0
CALENDAR_STATUS = "canonical_apim_futures_xml"
SCHEMA_ELIGIBILITY = "futures_all_universe_eligibility_snapshot.v1"
DATASET_STAGE = "continuous_v1"
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


def require_columns(frame: pd.DataFrame, columns: Iterable[str], name: str) -> None:
    missing = [x for x in columns if x not in frame.columns]
    if missing:
        raise RuntimeError(name + " missing required fields: " + ",".join(missing))


def eligibility_input_path(data_root: Path, snapshot_date: str, explicit: str) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    raw_d1 = data_root / "futures" / "all_universe" / "eligibility_snapshot_raw_d1" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"
    if raw_d1.exists():
        return raw_d1
    return data_root / "futures" / "all_universe" / "eligibility_snapshot" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def eligibility_output_path(data_root: Path, snapshot_date: str, explicit: str) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    return data_root / "futures" / "all_universe" / "eligibility_snapshot_continuous_v1" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def expiration_map_path(data_root: Path, snapshot_date: str) -> Path:
    return data_root / "futures" / "registry" / ("snapshot_date=" + snapshot_date) / "futures_expiration_map.parquet"


def roll_map_path(data_root: Path, snapshot_date: str, roll_policy_id: str) -> Path:
    return data_root / "futures" / "continuous" / "roll_map" / ("snapshot_date=" + snapshot_date) / ("roll_policy=" + roll_policy_id) / "futures_continuous_roll_map.parquet"


def deterministic_family_mask(frame: pd.DataFrame) -> pd.Series:
    if "family_code" not in frame.columns:
        return pd.Series([False] * len(frame), index=frame.index)
    family = frame["family_code"].map(clean_text)
    invalid = {"", "none", "null", "nan", "unknown", "ambiguous", "unresolved"}
    return family.map(lambda x: x is not None and str(x).strip().lower() not in invalid)


def base_gate_mask(frame: pd.DataFrame) -> pd.Series:
    required = ["secid", "board", "classification_status", "raw_5m_eligible", "futoi_eligible", "raw_d1_eligible", "family_code"]
    require_columns(frame, required, "eligibility_snapshot")
    return (
        (frame["board"].astype(str).str.upper() == "RFUD")
        & (frame["classification_status"].astype(str) == "included")
        & frame["raw_5m_eligible"].map(bool_value)
        & frame["futoi_eligible"].map(bool_value)
        & frame["raw_d1_eligible"].map(bool_value)
        & deterministic_family_mask(frame)
    )


def selected_secids_from_eligibility(frame: pd.DataFrame, final_only: bool) -> List[str]:
    mask = base_gate_mask(frame)
    if final_only:
        if "continuous_v1_eligible" not in frame.columns:
            raise RuntimeError("eligibility_snapshot missing continuous_v1_eligible for final continuous selection")
        mask = mask & frame["continuous_v1_eligible"].map(bool_value)
    secids = frame.loc[mask, "secid"].dropna().astype(str).tolist()
    out: List[str] = []
    seen = set()
    for secid in secids:
        key = secid.upper()
        if key not in seen:
            seen.add(key)
            out.append(secid)
    if not out:
        raise RuntimeError("No RFUD continuous v1 eligible instruments after gates")
    return out


def summarize_eligible(frame: pd.DataFrame) -> Dict[str, Any]:
    if frame.empty:
        return {"rows": 0, "families": {}, "secids": []}
    selected = frame.loc[frame.get("continuous_v1_eligible", pd.Series([False] * len(frame))).map(bool_value)].copy()
    families = {str(k): int(v) for k, v in selected.get("family_code", pd.Series(dtype=str)).astype(str).value_counts(dropna=False).to_dict().items()}
    return {
        "rows": int(len(selected)),
        "family_count": int(selected.get("family_code", pd.Series(dtype=str)).nunique()) if not selected.empty else 0,
        "families": families,
        "secids": selected.get("secid", pd.Series(dtype=str)).astype(str).tolist(),
    }


def expiration_buildable_map(expiration: pd.DataFrame) -> Dict[str, Tuple[bool, str]]:
    if expiration.empty:
        return {}
    require_columns(expiration, ["secid", "decision_source", "is_perpetual"], "expiration_map")
    out: Dict[str, Tuple[bool, str]] = {}
    for _, row in expiration.iterrows():
        secid = str(row.get("secid"))
        is_perpetual = bool_value(row.get("is_perpetual"))
        decision_source = str(clean_text(row.get("decision_source")) or "")
        validation_status = str(clean_text(row.get("validation_status")) or "pass")
        expiration_date = clean_text(row.get("expiration_date"))
        last_trade_date = clean_text(row.get("last_trade_date"))
        review_notes = clean_text(row.get("review_notes"))
        ok = False
        reason = "missing_expiration_anchor"
        if is_perpetual:
            ok = True
            reason = "perpetual_identity"
        elif validation_status == "blocker" or decision_source == "unresolved":
            ok = False
            reason = "unresolved_expiration_anchor"
        elif decision_source == "registry_expiration_date" and expiration_date:
            ok = True
            reason = "registry_expiration_date"
        elif decision_source == "registry_last_trade_date_fallback" and last_trade_date:
            ok = True
            reason = "registry_last_trade_date_fallback"
        elif decision_source == "manual_reviewed_override" and review_notes:
            ok = True
            reason = "manual_reviewed_override"
        out[secid.upper()] = (ok, reason)
    return out


def roll_buildable_map(roll_map: pd.DataFrame) -> Dict[str, Tuple[bool, str]]:
    if roll_map.empty:
        return {}
    require_columns(
        roll_map,
        ["source_secid", "roll_status", "roll_policy_id", "adjustment_policy_id", "adjustment_factor", "calendar_status"],
        "roll_map",
    )
    out: Dict[str, Tuple[bool, str]] = {}
    for _, row in roll_map.iterrows():
        secid = str(row.get("source_secid"))
        status = str(clean_text(row.get("roll_status")) or "")
        policy_ok = str(row.get("roll_policy_id")) == ROLL_POLICY_ID
        adjustment_ok = str(row.get("adjustment_policy_id")) == ADJUSTMENT_POLICY_ID
        factor_ok = float(pd.to_numeric(pd.Series([row.get("adjustment_factor")]), errors="coerce").iloc[0]) == ADJUSTMENT_FACTOR
        calendar_ok = str(row.get("calendar_status")) == CALENDAR_STATUS
        ok = status in BUILDABLE_ROLL_STATUSES and policy_ok and adjustment_ok and factor_ok and calendar_ok
        reason = status if ok else "roll_map_not_buildable:" + status
        out[secid.upper()] = (ok, reason)
    return out


def ensure_eligibility_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for col in ["continuous_v1_eligible", "access_api_eligible"]:
        if col not in out.columns:
            out[col] = False
    for col in ["continuous_v1_check_status", "continuous_v1_deferral_reason", "deferral_reason", "backfill_selection_status", "backfill_selection_reason", "dataset_stage", "notes"]:
        if col not in out.columns:
            out[col] = ""
    if "schema_version" not in out.columns:
        out["schema_version"] = SCHEMA_ELIGIBILITY
    return out
