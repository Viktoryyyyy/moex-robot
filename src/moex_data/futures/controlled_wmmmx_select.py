import json
from pathlib import Path

import pandas as pd

SCOPE = "controlled_batch_w_mm_mx"
CONFIG = "configs/datasets/futures_controlled_batch_w_mm_mx_raw_scope_config.json"
ELIGIBILITY_REL = "futures/registry/controlled_wmmmx_eligibility/snapshot_date={snapshot_date}/controlled_wmmmx_eligibility.parquet"


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


def load_eligibility(data_root, snapshot_date):
    path = _eligibility_path(data_root, snapshot_date)
    if not path.exists():
        raise FileNotFoundError("Missing controlled WMMMX eligibility artifact: " + str(path))
    frame = pd.read_parquet(path)
    _require(frame, "controlled_wmmmx_eligibility", ["secid", "classification_status", "continuous_eligibility_status"])
    return frame, path


def select(root, data_root, snapshot_date, config_path, whitelist, excluded):
    cfg = load_config(root, config_path)
    eligibility, path = load_eligibility(data_root, snapshot_date)
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
        raise RuntimeError("controlled scope selected zero instruments")
    secids = sorted(work["secid"].astype(str).unique().tolist())
    rows = []
    for _, row in work.drop_duplicates(subset=["secid"], keep="last").sort_values("secid").iterrows():
        rows.append({
            "secid": str(row.get("secid")),
            "family": str(row.get(fam_col)),
            "classification_status": cls,
            "continuous_eligibility_status": cont,
            "gate_status": "pass"
        })
    return secids, {
        "universe_scope": SCOPE,
        "selected_secids": secids,
        "rows": rows,
        "eligibility_artifact": str(path),
        "eligibility_row_count": int(len(eligibility.index)),
        "gate_status": "pass"
    }
