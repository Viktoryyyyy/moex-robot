import json
from pathlib import Path

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures.futoi_raw_loader import FUTOI_AVAILABILITY_CONTRACT
from moex_data.futures.futoi_raw_loader import load_contract_values_extended
from moex_data.futures.futoi_raw_loader import resolve_path_from_contract

SCOPE = "controlled_batch_w_mm_mx"
CONFIG = "configs/datasets/futures_controlled_batch_w_mm_mx_raw_scope_config.json"
PILOT_CLASSIFICATION_REL = "futures/registry/controlled_wmmmx_eligibility/snapshot_date={snapshot_date}/controlled_wmmmx_eligibility.parquet"


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


def _family_col(frame):
    for col in ["family_code", "family", "asset_code", "underlying_asset"]:
        if col in frame.columns:
            return col
    raise RuntimeError("family column missing")


def _row(frame, secid):
    return frame.loc[frame["secid"].astype(str).str.upper() == str(secid).upper()].tail(1)


def _require(frame, name, cols):
    missing = [x for x in cols if x not in frame.columns]
    if missing:
        raise RuntimeError(name + " missing: " + ",".join(missing))


def _pilot_classification_path(data_root, snapshot_date):
    return Path(data_root) / PILOT_CLASSIFICATION_REL.replace("{snapshot_date}", snapshot_date)


def load_frames(root, data_root, snapshot_date):
    contracts = load_contract_values_extended(root)
    normalized = pd.read_parquet(base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["normalized_registry"], snapshot_date))
    liquidity = pd.read_parquet(base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["liquidity_screen"], snapshot_date))
    history = pd.read_parquet(base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["history_depth_screen"], snapshot_date))
    futoi = pd.read_parquet(resolve_path_from_contract(data_root, contracts, FUTOI_AVAILABILITY_CONTRACT, snapshot_date))
    classification_path = _pilot_classification_path(data_root, snapshot_date)
    if not classification_path.exists():
        raise FileNotFoundError("Missing controlled classification artifact: " + str(classification_path))
    classification = pd.read_parquet(classification_path)
    return normalized, liquidity, history, futoi, classification


def select(root, data_root, snapshot_date, config_path, whitelist, excluded):
    cfg = load_config(root, config_path)
    normalized, liquidity, history, futoi, classification = load_frames(root, data_root, snapshot_date)
    _require(normalized, "normalized_registry", ["secid"])
    _require(classification, "pilot_classification", ["secid", "classification_status", "continuous_eligibility_status"])
    _require(liquidity, "liquidity_screen", ["secid", "liquidity_status"])
    _require(history, "history_depth_screen", ["secid", "history_depth_status"])
    _require(futoi, "futoi_availability", ["secid", "availability_status", "probe_status"])
    fam_col = _family_col(normalized)
    families = {str(x).upper() for x in cfg["families"]}
    cls = cfg["required_classification_status"]
    cont = cfg["required_continuous_eligibility_status"]
    status_cols = classification[["secid", "classification_status", "continuous_eligibility_status"]].drop_duplicates(subset=["secid"], keep="last")
    work = normalized.merge(status_cols, on="secid", how="inner")
    work = work.loc[work[fam_col].astype(str).str.upper().isin(families)].copy()
    if whitelist:
        allowed = {str(x).upper() for x in whitelist}
        work = work.loc[work["secid"].astype(str).str.upper().isin(allowed)].copy()
    banned = {str(x).upper() for x in excluded}
    work = work.loc[~work["secid"].astype(str).str.upper().isin(banned)].copy()
    work = work.loc[(work["classification_status"].astype(str) == cls) & (work["continuous_eligibility_status"].astype(str) == cont)].copy()
    if work.empty:
        raise RuntimeError("controlled scope selected zero instruments")
    secids = sorted(work["secid"].astype(str).unique().tolist())
    rows = []
    for secid in secids:
        n = _row(work, secid)
        l = _row(liquidity, secid)
        h = _row(history, secid)
        f = _row(futoi, secid)
        if n.empty or l.empty or h.empty or f.empty:
            raise RuntimeError("missing prerequisite artifact for " + secid)
        if str(l.iloc[0].get("liquidity_status")) != "pass":
            raise RuntimeError("liquidity gate failed for " + secid)
        if str(h.iloc[0].get("history_depth_status")) != "pass":
            raise RuntimeError("history gate failed for " + secid)
        if str(f.iloc[0].get("availability_status")) != "available" or str(f.iloc[0].get("probe_status")) != "completed":
            raise RuntimeError("futoi gate failed for " + secid)
        rows.append({"secid": secid, "family": str(n.iloc[0].get(fam_col)), "classification_status": cls, "continuous_eligibility_status": cont, "gate_status": "pass"})
    return secids, {"universe_scope": SCOPE, "selected_secids": secids, "rows": rows, "classification_artifact": str(_pilot_classification_path(data_root, snapshot_date)), "gate_status": "pass"}
