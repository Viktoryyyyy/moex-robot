import json
from pathlib import Path

CONTROLLED_SCOPE = "controlled_batch_w_mm_mx"
DEFAULT_CONTROLLED_CONFIG = "configs/datasets/futures_controlled_batch_w_mm_mx_raw_scope_config.json"


def load_scope_config(root, config_path):
    rel = config_path or DEFAULT_CONTROLLED_CONFIG
    path = Path(rel)
    if not path.is_absolute():
        path = Path(root) / rel
    if not path.exists():
        raise FileNotFoundError("Missing controlled scope config: " + str(path))
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("universe_scope") != CONTROLLED_SCOPE:
        raise RuntimeError("Unsupported controlled universe_scope in config: " + str(data.get("universe_scope")))
    if data.get("raw_only") is not True:
        raise RuntimeError("controlled_batch_w_mm_mx config must be raw_only=true")
    if data.get("continuous_build_allowed") is not False:
        raise RuntimeError("controlled_batch_w_mm_mx config must forbid continuous build")
    if data.get("roll_policy_change_allowed") is not False:
        raise RuntimeError("controlled_batch_w_mm_mx config must forbid roll policy changes")
    if data.get("continuous_artifact_creation_allowed") is not False:
        raise RuntimeError("controlled_batch_w_mm_mx config must forbid continuous artifact creation")
    return data


def _upper_set(values):
    return {str(x).strip().upper() for x in values or [] if str(x).strip()}


def _required(frame, name, columns):
    missing = [x for x in columns if x not in frame.columns]
    if missing:
        raise RuntimeError(name + " missing required controlled-scope columns: " + ",".join(missing))


def _family_column(frame):
    for col in ["family_code", "family", "asset_code", "underlying_asset"]:
        if col in frame.columns:
            return col
    raise RuntimeError("normalized_registry missing family column for controlled scope")


def _family_mask(frame, families):
    col = _family_column(frame)
    wanted = _upper_set(families)
    return frame[col].astype(str).str.upper().isin(wanted)


def _lookup(frame, secid):
    return frame.loc[frame["secid"].astype(str).str.upper() == str(secid).upper()].tail(1)


def select_controlled_instruments(normalized, liquidity, history, whitelist, excluded, config, futoi_availability=None):
    _required(normalized, "normalized_registry", ["secid", "classification_status", "continuous_eligibility_status"])
    _required(liquidity, "liquidity_screen", ["secid", "liquidity_status"])
    _required(history, "history_depth_screen", ["secid", "history_depth_status"])
    if futoi_availability is not None:
        _required(futoi_availability, "futoi_availability_report", ["secid", "availability_status", "probe_status"])
    families = config.get("families") or []
    required_classification = str(config.get("required_classification_status") or "")
    required_continuous = str(config.get("required_continuous_eligibility_status") or "")
    if not families or not required_classification or not required_continuous:
        raise RuntimeError("controlled scope config is incomplete")
    excluded_upper = _upper_set(excluded)
    whitelist_upper = _upper_set(whitelist)
    base = normalized.loc[_family_mask(normalized, families)].copy()
    if whitelist_upper:
        base = base.loc[base["secid"].astype(str).str.upper().isin(whitelist_upper)].copy()
    base = base.loc[~base["secid"].astype(str).str.upper().isin(excluded_upper)].copy()
    base = base.loc[base["classification_status"].astype(str) == required_classification].copy()
    base = base.loc[base["continuous_eligibility_status"].astype(str) == required_continuous].copy()
    if base.empty:
        raise RuntimeError("controlled_batch_w_mm_mx selected zero instruments after classification gates")
    rows = []
    gate_rows = []
    for secid in sorted(base["secid"].astype(str).unique().tolist()):
        nrow = _lookup(base, secid)
        lrow = _lookup(liquidity, secid)
        hrow = _lookup(history, secid)
        if nrow.empty or lrow.empty or hrow.empty:
            raise RuntimeError("Controlled instrument missing accepted prerequisite artifacts: " + secid)
        liquidity_status = str(lrow.iloc[0].get("liquidity_status", "")).strip()
        history_status = str(hrow.iloc[0].get("history_depth_status", "")).strip()
        if liquidity_status != "pass":
            raise RuntimeError("liquidity_status is not pass for " + secid + ": " + liquidity_status)
        if history_status != "pass":
            raise RuntimeError("history_depth_status is not pass for " + secid + ": " + history_status)
        if futoi_availability is not None:
            arow = _lookup(futoi_availability, secid)
            if arow.empty:
                raise RuntimeError("Controlled instrument missing FUTOI availability artifact: " + secid)
            availability_status = str(arow.iloc[0].get("availability_status", "")).strip()
            probe_status = str(arow.iloc[0].get("probe_status", "")).strip()
            if availability_status != "available" or probe_status != "completed":
                raise RuntimeError("FUTOI availability is not completed/available for " + secid + ": " + availability_status + "/" + probe_status)
        else:
            availability_status = "not_checked"
            probe_status = "not_checked"
        row = nrow.iloc[0].to_dict()
        row["secid"] = secid
        row["board"] = str(row.get("board", "rfud") or "rfud")
        row["family_code"] = str(row.get("family_code", row.get("family", "")) or "")
        row["liquidity_status"] = liquidity_status
        row["history_depth_status"] = history_status
        row["short_history_flag"] = False
        row["first_available_date"] = hrow.iloc[0].get("first_available_date")
        row["last_available_date"] = hrow.iloc[0].get("last_available_date")
        row["screen_from"] = hrow.iloc[0].get("screen_from")
        row["screen_till"] = hrow.iloc[0].get("screen_till")
        row["futoi_availability_status"] = availability_status
        row["futoi_probe_status"] = probe_status
        rows.append(row)
        gate_rows.append({
            "secid": secid,
            "family_code": row["family_code"],
            "classification_status": str(nrow.iloc[0].get("classification_status", "")),
            "continuous_eligibility_status": str(nrow.iloc[0].get("continuous_eligibility_status", "")),
            "liquidity_status": liquidity_status,
            "history_depth_status": history_status,
            "futoi_availability_status": availability_status,
            "futoi_probe_status": probe_status,
            "gate_status": "pass"
        })
    import pandas as pd
    selected = pd.DataFrame(rows)
    gate = {
        "universe_scope": CONTROLLED_SCOPE,
        "families": families,
        "required_classification_status": required_classification,
        "required_continuous_eligibility_status": required_continuous,
        "selected_secids": sorted(selected["secid"].astype(str).tolist()),
        "rows": gate_rows,
        "gate_status": "pass"
    }
    return selected, gate


def assert_raw_only_config(config):
    forbidden = config.get("forbidden_components") or []
    return {
        "continuous_build_allowed": bool(config.get("continuous_build_allowed")),
        "continuous_artifact_creation_allowed": bool(config.get("continuous_artifact_creation_allowed")),
        "roll_policy_change_allowed": bool(config.get("roll_policy_change_allowed")),
        "forbidden_components": forbidden,
        "status": "pass" if config.get("continuous_build_allowed") is False and config.get("continuous_artifact_creation_allowed") is False and config.get("roll_policy_change_allowed") is False else "fail"
    }
