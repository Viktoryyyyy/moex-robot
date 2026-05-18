import json
import os
from pathlib import Path

import pandas as pd

from moex_data.futures import futoi_raw_loader
from moex_data.futures import liquidity_history_metrics_probe as base

SCOPE = "controlled_batch_w_mm_mx"
CONFIG = "configs/datasets/futures_controlled_batch_w_mm_mx_raw_scope_config.json"
ELIGIBILITY_REL = "futures/registry/controlled_wmmmx_eligibility/snapshot_date={snapshot_date}/controlled_wmmmx_eligibility.parquet"
NO_OBSERVED_SOURCE_ROWS_REASON = "no_observed_source_rows_as_of_snapshot"
FUTURE_CONTRACT_NOT_YET_LOADABLE_REASON = "future_contract_not_yet_loadable"
SOURCE_SECID_NOT_PROVEN_REASON = "source_secid_not_proven"


def load_config(root, path):
    p = Path(path or CONFIG)
    if not p.is_absolute():
        p = Path(root) / p
    data = json.loads(p.read_text(encoding="utf-8"))
    if data.get("universe_scope") != SCOPE:
        raise RuntimeError("bad universe_scope")
    for key in ["raw_only", "continuous_build_allowed", "roll_policy_change_allowed", "continuous_artifact_creation_allowed"]:
        if key == "raw_only" and data.get(key) is not True:
            raise RuntimeError("raw_only must be true")
        if key != "raw_only" and data.get(key) is not False:
            raise RuntimeError(key + " must be false")
    return data


def _eligibility_path(data_root, snapshot_date):
    return Path(data_root) / ELIGIBILITY_REL.replace("{snapshot_date}", snapshot_date)


def _require(frame, name, cols):
    missing = [x for x in cols if x not in frame.columns]
    if missing:
        raise RuntimeError(name + " missing: " + ",".join(missing))


def _family_col(frame):
    for col in ["family", "family_code", "asset_code", "underlying_asset"]:
        if col in frame.columns:
            return col
    raise RuntimeError("eligibility family column missing")


def _parse_day(value, label, secid):
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise RuntimeError("eligibility " + label + " is not raw-loader-date-range-resolvable for " + str(secid))
    return parsed.date().isoformat()


def _last_by_secid(frame, secid):
    if "secid" not in frame.columns:
        raise RuntimeError("FUTOI gate artifact missing secid column")
    row = frame.loc[frame["secid"].astype(str).str.upper() == str(secid).upper()].tail(1)
    return None if row.empty else row.iloc[0]


def _status(row, field):
    if row is None:
        return "missing"
    return str(row.get(field, "")).strip()


def _source_secid_col(frame):
    return base.canonical_column(frame, ["secid", "SECID"])


def _zero_rows_reason(first_available_date, snapshot_date):
    if str(first_available_date) > str(snapshot_date):
        return FUTURE_CONTRACT_NOT_YET_LOADABLE_REASON
    return NO_OBSERVED_SOURCE_ROWS_REASON


def _probe_source_identity(secid, first_available_date, last_available_date, snapshot_date, timeout, apim_base_url, iss_base_url):
    source_frame, source_url, fetch_status, fetch_error = base.fetch_tradestats(secid, first_available_date, last_available_date, float(timeout), str(apim_base_url), str(iss_base_url))
    out = {
        "source_probe_status": fetch_status,
        "source_probe_error": fetch_error or None,
        "source_endpoint_url": source_url,
        "source_rows_before_filter": int(len(source_frame)),
        "source_rows_after_filter": 0,
        "source_identity_filtered_out_rows": 0,
        "source_observed_secids": [],
        "source_remaining_secids": [],
        "source_identity_gate_status": "blocked",
        "source_identity_exclude_reason": "",
    }
    if fetch_status != "completed" or source_frame.empty:
        out["source_identity_exclude_reason"] = _zero_rows_reason(first_available_date, snapshot_date)
        return out
    col = _source_secid_col(source_frame)
    if not col:
        out["source_identity_exclude_reason"] = SOURCE_SECID_NOT_PROVEN_REASON
        return out
    requested = str(secid).strip().upper()
    source_identity = source_frame[col].astype(str).str.strip().str.upper()
    observed = sorted([str(x) for x in source_identity.dropna().unique().tolist() if str(x)])
    mask = source_identity == requested
    filtered = source_frame.loc[mask].copy()
    remaining = sorted([str(x) for x in filtered[col].astype(str).str.strip().str.upper().dropna().unique().tolist() if str(x)]) if not filtered.empty else []
    out["source_rows_after_filter"] = int(len(filtered))
    out["source_identity_filtered_out_rows"] = int(len(source_frame) - len(filtered))
    out["source_observed_secids"] = observed
    out["source_remaining_secids"] = remaining
    if filtered.empty:
        out["source_identity_exclude_reason"] = _zero_rows_reason(first_available_date, snapshot_date)
        return out
    if remaining != [requested]:
        out["source_identity_exclude_reason"] = SOURCE_SECID_NOT_PROVEN_REASON
        return out
    out["source_identity_gate_status"] = "pass"
    return out


def _load_futoi_gate_frames(root, data_root, snapshot_date):
    contracts = futoi_raw_loader.load_contract_values_extended(root)
    _, _, _, history, futoi_availability = futoi_raw_loader.load_inputs(data_root, contracts, snapshot_date)
    return history, futoi_availability


def load_eligibility(data_root, snapshot_date):
    path = _eligibility_path(data_root, snapshot_date)
    if not path.exists():
        raise FileNotFoundError("Missing controlled WMMMX eligibility artifact: " + str(path))
    frame = pd.read_parquet(path)
    _require(frame, "controlled_wmmmx_eligibility", ["secid", "classification_status", "continuous_eligibility_status", "first_available_date", "last_available_date"])
    return frame, path


def select(root, data_root, snapshot_date, config_path, whitelist, excluded, apim_base_url=None, iss_base_url=None, timeout=60.0):
    cfg = load_config(root, config_path)
    apim_base_url = str(apim_base_url or os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    iss_base_url = str(iss_base_url or os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    eligibility, path = load_eligibility(data_root, snapshot_date)
    history, futoi_availability = _load_futoi_gate_frames(root, data_root, snapshot_date)
    fam_col = _family_col(eligibility)
    families = {str(x).upper() for x in cfg["families"]}
    cls = str(cfg["required_classification_status"])
    cont = str(cfg["required_continuous_eligibility_status"])
    observed_families = {str(x).upper() for x in eligibility[fam_col].dropna().astype(str).unique().tolist()}
    outside = sorted(observed_families - families)
    if outside:
        raise RuntimeError("eligibility contains out-of-scope families: " + ",".join(outside))
    work = eligibility.copy()
    if "board" in work.columns:
        bad_board = work.loc[work["board"].astype(str).str.upper() != "RFUD"]
        if not bad_board.empty:
            raise RuntimeError("eligibility contains non-RFUD board rows")
    bad_cls = work.loc[work["classification_status"].astype(str) != cls]
    if not bad_cls.empty:
        raise RuntimeError("eligibility contains invalid classification_status rows")
    bad_cont = work.loc[work["continuous_eligibility_status"].astype(str) != cont]
    if not bad_cont.empty:
        raise RuntimeError("eligibility contains invalid continuous_eligibility_status rows")
    if whitelist:
        allowed = {str(x).upper() for x in whitelist}
        work = work.loc[work["secid"].astype(str).str.upper().isin(allowed)].copy()
    banned = {str(x).upper() for x in excluded}
    work = work.loc[~work["secid"].astype(str).str.upper().isin(banned)].copy()
    if work.empty:
        raise RuntimeError("controlled scope selected zero instruments before FUTOI gate")
    rows = []
    excluded_rows = []
    for _, row in work.drop_duplicates(subset=["secid"], keep="last").sort_values("secid").iterrows():
        secid = str(row.get("secid"))
        first_available_date = _parse_day(row.get("first_available_date"), "first_available_date", secid)
        last_available_date = _parse_day(row.get("last_available_date"), "last_available_date", secid)
        if first_available_date > last_available_date:
            raise RuntimeError("eligibility raw-loader date range is inverted for " + secid)
        hrow = _last_by_secid(history, secid)
        arow = _last_by_secid(futoi_availability, secid)
        history_status = _status(hrow, "history_depth_status")
        futoi_availability_status = _status(arow, "availability_status")
        futoi_probe_status = _status(arow, "probe_status")
        source_probe = _probe_source_identity(secid, first_available_date, last_available_date, snapshot_date, timeout, apim_base_url, iss_base_url)
        base_row = {
            "secid": secid,
            "family": str(row.get(fam_col)),
            "classification_status": cls,
            "continuous_eligibility_status": cont,
            "first_available_date": first_available_date,
            "last_available_date": last_available_date,
            "raw_loader_date_range_resolvable": True,
            "futoi_history_depth_status": history_status,
            "futoi_availability_status": futoi_availability_status,
            "futoi_probe_status": futoi_probe_status,
            "source_probe_status": source_probe.get("source_probe_status"),
            "source_probe_error": source_probe.get("source_probe_error"),
            "source_endpoint_url": source_probe.get("source_endpoint_url"),
            "source_rows_before_filter": source_probe.get("source_rows_before_filter"),
            "source_rows_after_filter": source_probe.get("source_rows_after_filter"),
            "source_identity_filtered_out_rows": source_probe.get("source_identity_filtered_out_rows"),
            "source_observed_secids": source_probe.get("source_observed_secids"),
            "source_remaining_secids": source_probe.get("source_remaining_secids"),
            "source_identity_gate_status": source_probe.get("source_identity_gate_status"),
            "source_identity_exclude_reason": source_probe.get("source_identity_exclude_reason"),
        }
        blocker_reasons = []
        if source_probe.get("source_identity_gate_status") != "pass":
            excluded_row = dict(base_row)
            excluded_row["gate_status"] = "excluded"
            excluded_row["blocker_reason"] = str(source_probe.get("source_identity_exclude_reason") or SOURCE_SECID_NOT_PROVEN_REASON)
            excluded_rows.append(excluded_row)
            continue
        if history_status != "pass":
            blocker_reasons.append("futoi_history_depth_status_not_pass:" + history_status)
        if futoi_availability_status != "available" or futoi_probe_status != "completed":
            blocker_reasons.append("futoi_availability_not_completed_available:" + futoi_availability_status + "/" + futoi_probe_status)
        if blocker_reasons:
            excluded_row = dict(base_row)
            excluded_row["gate_status"] = "blocked"
            excluded_row["blocker_reason"] = ";".join(blocker_reasons)
            excluded_rows.append(excluded_row)
            continue
        selected_row = dict(base_row)
        selected_row["gate_status"] = "pass"
        rows.append(selected_row)
    secids = sorted([str(x.get("secid")) for x in rows])
    if not secids:
        raise RuntimeError("controlled scope selected zero instruments after FUTOI gate; excluded_rows=" + json.dumps(excluded_rows, ensure_ascii=False, sort_keys=True, default=str))
    return secids, {
        "universe_scope": SCOPE,
        "selected_secids": secids,
        "rows": rows,
        "excluded_rows": excluded_rows,
        "eligibility_artifact": str(path),
        "eligibility_row_count": int(len(eligibility.index)),
        "raw_loader_date_range_resolvable": True,
        "futoi_history_depth_required_status": "pass",
        "futoi_availability_required_status": "available",
        "futoi_probe_required_status": "completed",
        "futoi_gate_blocked_count": int(len([x for x in excluded_rows if "futoi_" in str(x.get("blocker_reason", ""))])),
        "source_identity_excluded_count": int(len([x for x in excluded_rows if str(x.get("source_identity_gate_status", "")) != "pass"])),
        "gate_status": "pass"
    }
