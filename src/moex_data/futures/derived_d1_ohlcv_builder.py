#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base

from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import DEFAULT_WHITELIST
from moex_data.futures.slice1_common import SHORT_HISTORY_ALLOWED
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

SCHEMA_D1 = "futures_derived_d1_ohlcv.v1"
SCHEMA_QUALITY = "futures_derived_d1_ohlcv_quality_report.v1"
SCHEMA_MANIFEST = "futures_derived_d1_ohlcv_manifest.v1"
SCHEMA_ELIGIBILITY = "futures_all_universe_eligibility_snapshot.v1"
DATASET_STAGE = "raw_d1"
MODE_L3_5 = "rfud_included_raw_d1"
REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_raw_5m_contract.md",
    "contracts/datasets/futures_derived_d1_ohlcv_contract.md",
    "contracts/datasets/futures_derived_d1_ohlcv_manifest_contract.md",
    "contracts/datasets/futures_derived_d1_ohlcv_quality_report_contract.md",
    "contracts/datasets/futures_all_universe_eligibility_contract.md",
    "configs/datasets/futures_all_universe_eligibility_config.json",
]


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise RuntimeError("JSON root is not object: " + str(path))
    return data


def dump_json(path, data):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_parquet(path, frame):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(p, index=False)


def output_paths(data_root, run_date, chunk_id=""):
    root = data_root / "futures"
    if chunk_id:
        return {
            "derived_d1_partition_root": str(root / "derived_d1_ohlcv"),
            "quality_report": str(root / "quality" / "derived_d1_ohlcv_builder" / ("chunk_id=" + chunk_id) / "quality_report.parquet"),
            "manifest": str(root / "runs" / "derived_d1_ohlcv_builder" / ("chunk_id=" + chunk_id) / "manifest.json"),
        }
    return {
        "derived_d1_partition_root": str(root / "derived_d1_ohlcv"),
        "quality_report": str(root / "quality" / "derived_d1_ohlcv_builder" / ("run_date=" + run_date) / "futures_derived_d1_ohlcv_quality_report.parquet"),
        "manifest": str(root / "runs" / "derived_d1_ohlcv_builder" / ("run_date=" + run_date) / "manifest.json"),
    }


def raw_root(data_root):
    return data_root / "futures" / "raw_5m"


def d1_path(data_root, trade_date, family_code, secid):
    return data_root / "futures" / "derived_d1_ohlcv" / ("trade_date=" + str(trade_date)) / ("family=" + str(family_code)) / ("secid=" + str(secid)) / "part.parquet"


def eligibility_path(data_root, snapshot_date, explicit):
    if explicit:
        return Path(explicit).expanduser().resolve()
    return data_root / "futures" / "all_universe" / "eligibility_snapshot" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def refined_eligibility_path(data_root, snapshot_date, explicit):
    if explicit:
        return Path(explicit).expanduser().resolve()
    return data_root / "futures" / "all_universe" / "eligibility_snapshot_raw_d1" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def quality_report_paths(data_root, explicit):
    if explicit:
        return [Path(x).expanduser().resolve() for x in str(explicit).split(",") if x.strip()]
    base_dir = data_root / "futures" / "all_universe" / "quality" / "raw_5m_backfill"
    return sorted(base_dir.glob("chunk_id=*/quality_report.parquet"))


def partition_value(path, key):
    prefix = key + "="
    for part in Path(path).parts:
        if part.startswith(prefix):
            return part[len(prefix):]
    return ""


def require_columns(frame, cols, name):
    missing = [x for x in cols if x not in frame.columns]
    if missing:
        raise RuntimeError(name + " missing required fields: " + ", ".join(missing))


def accepted_quality(quality_paths, from_date, till):
    frames = []
    for path in quality_paths:
        if path.exists():
            frame = pd.read_parquet(path)
            if len(frame):
                frame["_quality_report_path"] = str(path)
                frames.append(frame)
    if not frames:
        raise RuntimeError("No non-empty raw 5m quality reports found")
    quality = pd.concat(frames, ignore_index=True)
    require_columns(quality, ["dataset_stage", "family_code", "secid", "quality_status", "rows_written", "partition_status", "date_from", "date_till"], "raw_5m_quality_reports")
    q = quality.loc[
        (quality["dataset_stage"].astype(str) == "raw_5m")
        & (quality["quality_status"].astype(str) == "pass")
        & (quality["partition_status"].astype(str) == "written")
        & (pd.to_numeric(quality["rows_written"], errors="coerce") > 0)
    ].copy()
    if from_date:
        q = q.loc[q["date_till"].astype(str) >= str(from_date)].copy()
    if till:
        q = q.loc[q["date_from"].astype(str) <= str(till)].copy()
    if q.empty:
        raise RuntimeError("No accepted raw 5m quality rows after filters")
    return q.sort_values(["family_code", "secid", "date_from", "date_till"]).reset_index(drop=True)


def refine_eligibility_for_raw_d1(eligibility, quality, config):
    require_columns(eligibility, ["secid", "board", "classification_status", "raw_5m_eligible", "raw_d1_eligible", "schema_version"], "eligibility_snapshot")
    stage = config.get("l3_5_raw_d1_included_universe") or {}
    target_board = str(stage.get("board", "RFUD")).upper()
    accepted_secids = set(quality["secid"].astype(str).tolist())
    out = eligibility.copy()
    selected = (
        (out["board"].astype(str).str.upper() == target_board)
        & (out["classification_status"].astype(str) == "included")
        & (out["raw_5m_eligible"] == True)
        & (out["secid"].astype(str).isin(accepted_secids))
    )
    out["raw_d1_eligible"] = False
    out.loc[selected, "raw_d1_eligible"] = True
    out["dataset_stage"] = DATASET_STAGE
    out["schema_version"] = SCHEMA_ELIGIBILITY
    if "raw_d1_check_status" not in out.columns:
        out["raw_d1_check_status"] = ""
    if "deferral_reason" not in out.columns:
        out["deferral_reason"] = ""
    if "backfill_selection_status" not in out.columns:
        out["backfill_selection_status"] = ""
    if "backfill_selection_reason" not in out.columns:
        out["backfill_selection_reason"] = ""
    if "notes" not in out.columns:
        out["notes"] = ""
    included_raw5m = (out["board"].astype(str).str.upper() == target_board) & (out["classification_status"].astype(str) == "included") & (out["raw_5m_eligible"] == True)
    out.loc[included_raw5m & selected, "raw_d1_check_status"] = "pass"
    out.loc[included_raw5m & selected, "backfill_selection_status"] = "selected"
    out.loc[included_raw5m & selected, "backfill_selection_reason"] = "raw_d1_selected_from_accepted_raw_5m_quality"
    out.loc[included_raw5m & ~selected, "raw_d1_check_status"] = "raw_5m_quality_not_accepted"
    out.loc[included_raw5m & ~selected, "deferral_reason"] = "raw_5m_quality_not_accepted"
    out.loc[included_raw5m & ~selected, "backfill_selection_status"] = "deferred"
    out.loc[included_raw5m & ~selected, "backfill_selection_reason"] = "raw_5m_quality_not_accepted"
    out["notes"] = out["notes"].astype(str) + " | PM L3-5 raw D1 eligibility refined from accepted raw 5m quality"
    selected_frame = out.loc[selected].copy().sort_values(["family_code", "secid"]).reset_index(drop=True)
    if selected_frame.empty:
        raise RuntimeError("No RFUD included raw_d1 eligible instruments after raw 5m quality refinement")
    return out, selected_frame


def discover_raw_paths(data_root, selected, from_date, till):
    root = raw_root(data_root)
    if not root.exists():
        raise FileNotFoundError("Missing raw 5m root: " + str(root))
    selected_pairs = {(str(row.get("family_code")), str(row.get("secid"))) for _, row in selected.iterrows()}
    paths = []
    for path in sorted(root.glob("trade_date=*/family=*/secid=*/part.parquet")):
        family_code = partition_value(path, "family")
        secid = partition_value(path, "secid")
        trade_date = partition_value(path, "trade_date")
        if (family_code, secid) not in selected_pairs:
            continue
        if from_date and trade_date < from_date:
            continue
        if till and trade_date > till:
            continue
        paths.append(path)
    if not paths:
        raise RuntimeError("No raw 5m partitions found for raw D1 selected universe")
    return paths


def selected_pairs_from_paths(paths):
    return {(partition_value(path, "family"), partition_value(path, "secid")) for path in paths}


def filter_selected_by_raw_paths(refined, selected, raw_paths):
    pairs = selected_pairs_from_paths(raw_paths)
    if not pairs:
        raise RuntimeError("No raw 5m partitions found for raw D1 selected universe in requested window")
    out = refined.copy()
    sel = selected.copy()
    sel["_raw_d1_pair"] = list(zip(sel["family_code"].astype(str), sel["secid"].astype(str)))
    selected2 = sel.loc[sel["_raw_d1_pair"].isin(pairs)].drop(columns=["_raw_d1_pair"]).copy().sort_values(["family_code", "secid"]).reset_index(drop=True)
    if selected2.empty:
        raise RuntimeError("No raw D1 selected universe rows with raw partitions for requested window")
    missing = sel.loc[~sel["_raw_d1_pair"].isin(pairs), "_raw_d1_pair"].tolist()
    if missing:
        out["_raw_d1_pair"] = list(zip(out["family_code"].astype(str), out["secid"].astype(str)))
        mask = out["_raw_d1_pair"].isin(set(missing))
        out.loc[mask, "raw_d1_eligible"] = False
        out.loc[mask, "raw_d1_check_status"] = "raw_5m_partition_not_found_for_requested_window"
        out.loc[mask, "deferral_reason"] = "raw_5m_partition_not_found_for_requested_window"
        out.loc[mask, "backfill_selection_status"] = "deferred"
        out.loc[mask, "backfill_selection_reason"] = "raw_5m_partition_not_found_for_requested_window"
        out = out.drop(columns=["_raw_d1_pair"])
    return out, selected2


def read_raw(paths):
    frames = []
    for path in paths:
        frame = pd.read_parquet(path)
        frame["_source_partition_path"] = str(path)
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def validate_raw(frame):
    required = ["trade_date", "ts", "board", "secid", "family_code", "open", "high", "low", "close", "volume", "schema_version", "calendar_denominator_status"]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        raise RuntimeError("Raw 5m input missing required fields: " + ", ".join(missing))
    schemas = sorted([str(x) for x in frame["schema_version"].dropna().unique().tolist()])
    if schemas != ["futures_raw_5m.v1"]:
        raise RuntimeError("Raw 5m schema mismatch: " + json.dumps(schemas, ensure_ascii=False))
    calendar = sorted([str(x) for x in frame["calendar_denominator_status"].dropna().unique().tolist()])
    if calendar != ["canonical_apim_futures_xml"]:
        raise RuntimeError("Raw 5m calendar status mismatch: " + json.dumps(calendar, ensure_ascii=False))
    if sorted([str(x) for x in frame["board"].dropna().unique().tolist()]) != ["RFUD"]:
        raise RuntimeError("Raw D1 derivation supports RFUD board only")


def normalize_raw(frame):
    out = frame.copy()
    out["trade_date"] = out["trade_date"].astype(str)
    out["secid"] = out["secid"].astype(str)
    out["family_code"] = out["family_code"].astype(str)
    out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
    out = out.loc[out["trade_date"].notna() & out["ts"].notna()].copy()
    for col in ["open", "high", "low", "close", "volume", "value", "num_trades"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def first_string(series):
    values = series.dropna().astype(str).tolist()
    return values[0] if values else None


def first_bool(series):
    values = series.dropna().tolist()
    return bool(values[0]) if values else False


def aggregate_d1(raw, ingest_ts):
    rows = []
    work = raw.sort_values(["secid", "trade_date", "ts"]).copy()
    for (secid, trade_date), part in work.groupby(["secid", "trade_date"], sort=True):
        value_sum = float(part["value"].sum()) if "value" in part.columns and part["value"].notna().any() else None
        trades_sum = float(part["num_trades"].sum()) if "num_trades" in part.columns and part["num_trades"].notna().any() else None
        rows.append({
            "trade_date": str(trade_date),
            "session_date": first_string(part["session_date"]) if "session_date" in part.columns else str(trade_date),
            "board": first_string(part["board"]),
            "secid": str(secid),
            "family_code": first_string(part["family_code"]),
            "open": part["open"].iloc[0],
            "high": part["high"].max(),
            "low": part["low"].min(),
            "close": part["close"].iloc[-1],
            "volume": part["volume"].sum(),
            "value": value_sum,
            "num_trades": trades_sum,
            "bar_count": int(len(part)),
            "min_ts": part["ts"].min(),
            "max_ts": part["ts"].max(),
            "source_dataset_id": "futures_raw_5m",
            "source_schema_version": "futures_raw_5m.v1",
            "source_partition_count": int(part["_source_partition_path"].nunique()),
            "source_rows": int(len(part)),
            "ingest_ts": ingest_ts,
            "schema_version": SCHEMA_D1,
            "short_history_flag": first_bool(part["short_history_flag"]) if "short_history_flag" in part.columns else False,
            "calendar_denominator_status": "canonical_apim_futures_xml",
            "dataset_stage": DATASET_STAGE,
        })
    return pd.DataFrame(rows).sort_values(["trade_date", "family_code", "secid"]).reset_index(drop=True) if rows else pd.DataFrame()


def quality_counts(raw, d1):
    raw_keys = raw[["secid", "trade_date"]].drop_duplicates().copy()
    d1_keys = d1[["secid", "trade_date"]].drop_duplicates().copy() if not d1.empty else pd.DataFrame(columns=["secid", "trade_date"])
    merged = raw_keys.merge(d1_keys, on=["secid", "trade_date"], how="left", indicator=True)
    missing = merged.loc[merged["_merge"] == "left_only"]
    duplicate_d1 = int(d1.duplicated(subset=["trade_date", "secid"]).sum()) if not d1.empty else 0
    null_ohlc = int(d1[["open", "high", "low", "close"]].isna().any(axis=1).sum()) if not d1.empty else 0
    if d1.empty:
        invalid_ohlc = 0
    else:
        invalid = (d1["high"] < d1["low"]) | (d1["open"] > d1["high"]) | (d1["open"] < d1["low"]) | (d1["close"] > d1["high"]) | (d1["close"] < d1["low"])
        invalid_ohlc = int(invalid.fillna(True).sum())
    return {
        "raw_5m_rows": int(len(raw)),
        "raw_secids": int(raw["secid"].nunique()),
        "raw_trade_dates": int(raw["trade_date"].nunique()),
        "raw_secid_trade_date_pairs": int(len(raw_keys)),
        "d1_rows": int(len(d1)),
        "duplicate_d1_key_count": duplicate_d1,
        "null_ohlc_count": null_ohlc,
        "invalid_ohlc_count": invalid_ohlc,
        "missing_d1_row_count": int(len(missing)),
        "missing_d1_keys_json": json.dumps(missing.head(50).to_dict("records"), ensure_ascii=False, sort_keys=True),
    }


def status_from_counts(counts):
    if int(counts.get("d1_rows") or 0) == 0:
        return "fail", "zero D1 rows"
    checks = [
        ("duplicate_d1_key_count", "duplicate D1 primary-key rows"),
        ("null_ohlc_count", "null D1 OHLC values"),
        ("invalid_ohlc_count", "invalid D1 OHLC ordering"),
        ("missing_d1_row_count", "missing one-row-per-secid-trade_date D1 output"),
    ]
    for key, note in checks:
        if int(counts.get(key) or 0) > 0:
            return "fail", note
    return "pass", "derived raw D1 OHLCV build completed from accepted raw 5m"


def write_partitions(d1, data_root):
    paths = []
    clean = d1.drop_duplicates(subset=["trade_date", "secid"], keep="last").copy()
    for _, row in clean.iterrows():
        path = d1_path(data_root, row["trade_date"], row["family_code"], row["secid"])
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([row.to_dict()]).to_parquet(path, index=False)
        paths.append(str(path))
    return paths


def per_instrument(raw, d1, paths):
    path_counts = {}
    for path in paths:
        secid = partition_value(path, "secid")
        path_counts[secid] = path_counts.get(secid, 0) + 1
    summaries = {}
    for secid, part in raw.groupby("secid", sort=True):
        d1_part = d1.loc[d1["secid"].astype(str) == str(secid)].copy()
        short_history_flag = first_bool(part["short_history_flag"]) if "short_history_flag" in part.columns else False
        status = "pass" if int(len(d1_part)) == int(part["trade_date"].nunique()) else "fail"
        summaries[str(secid)] = {
            "raw_5m_rows": int(len(part)),
            "raw_trade_dates": int(part["trade_date"].nunique()),
            "d1_rows": int(len(d1_part)),
            "partition_count": int(path_counts.get(str(secid), 0)),
            "short_history_flag": short_history_flag,
            "quality_status": status,
        }
    return summaries


def validate_selected_scope(raw, selected):
    observed = sorted(raw["secid"].dropna().astype(str).unique().tolist())
    expected = sorted(selected["secid"].dropna().astype(str).unique().tolist())
    if sorted([x.upper() for x in observed]) != sorted([x.upper() for x in expected]):
        raise RuntimeError("Raw partitions do not cover exact raw D1 selected universe. observed=" + json.dumps(observed, ensure_ascii=False) + " expected=" + json.dumps(expected, ensure_ascii=False))
    selected_boards = sorted(selected["board"].dropna().astype(str).str.upper().unique().tolist())
    if selected_boards != ["RFUD"]:
        raise RuntimeError("Selected raw D1 universe must be RFUD only: " + json.dumps(selected_boards, ensure_ascii=False))
    bad = selected.loc[(selected["classification_status"].astype(str) != "included") | (selected["raw_5m_eligible"] != True) | (selected["raw_d1_eligible"] != True)].copy()
    if len(bad):
        raise RuntimeError("Selected raw D1 universe contains non-eligible rows")


def chunk_groups(selected):
    groups = []
    for fam, frame in selected.groupby("family_code", sort=True):
        groups.append((str(fam), frame.sort_values("secid").reset_index(drop=True)))
    return groups


def build_quality_rows(run_id, run_date, summaries):
    rows = []
    for secid, summary in summaries.items():
        rows.append({
            "quality_report_id": stable_id([run_id, secid]),
            "run_id": run_id,
            "run_date": run_date,
            "secid": secid,
            "dataset_id": "futures_derived_d1_ohlcv",
            "dataset_stage": DATASET_STAGE,
            "schema_version": SCHEMA_QUALITY,
            "quality_status": summary.get("quality_status"),
            "review_notes": "derived D1 rows match raw secid/trade_date pairs" if summary.get("quality_status") == "pass" else "derived D1 row count mismatch",
            "short_history_flag": summary.get("short_history_flag"),
            "raw_5m_rows": summary.get("raw_5m_rows"),
            "raw_trade_dates": summary.get("raw_trade_dates"),
            "d1_rows": summary.get("d1_rows"),
            "partition_count": summary.get("partition_count"),
            "calendar_denominator_status": "canonical_apim_futures_xml",
        })
    return pd.DataFrame(rows).sort_values(["secid"]).reset_index(drop=True)


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--config", default="configs/datasets/futures_all_universe_eligibility_config.json")
    parser.add_argument("--input-eligibility", default="")
    parser.add_argument("--output-eligibility", default="")
    parser.add_argument("--quality-reports", default="")
    parser.add_argument("--selection-mode", default=MODE_L3_5)
    parser.add_argument("--whitelist", default="")
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    args = parser.parse_args()

    root = Path.cwd().resolve()
    data_root = base.resolve_data_root(args)
    run_date = str(args.run_date).strip()
    snapshot_date = str(args.snapshot_date).strip()
    from_date = base.parse_iso_date(str(args.from_date or "")) if str(args.from_date or "").strip() else ""
    till = base.parse_iso_date(str(args.till or "")) if str(args.till or "").strip() else ""
    config = load_json(root / args.config)
    mode = str(args.selection_mode or config.get("active_raw_d1_selection_mode") or MODE_L3_5)
    if mode != MODE_L3_5:
        raise RuntimeError("Unsupported raw D1 selection mode: " + mode)
    if config.get("continuous_build_enabled") is not False or config.get("w1_build_enabled") is not False:
        raise RuntimeError("continuous_build_enabled and w1_build_enabled must be false")
    if args.whitelist:
        raise RuntimeError("Manual whitelist is forbidden for PM L3-5 raw D1 registry expansion")

    base.assert_files_exist(root, REQUIRED_CONTRACTS)
    quality = accepted_quality(quality_report_paths(data_root, str(args.quality_reports or "")), from_date, till)
    ep = eligibility_path(data_root, snapshot_date, str(args.input_eligibility or ""))
    if not ep.exists():
        raise FileNotFoundError("Missing eligibility snapshot: " + str(ep))
    eligibility = pd.read_parquet(ep)
    refined, selected = refine_eligibility_for_raw_d1(eligibility, quality, config)
    refined_path = refined_eligibility_path(data_root, snapshot_date, str(args.output_eligibility or ""))
    raw_paths = discover_raw_paths(data_root, selected, from_date, till)
    refined, selected = filter_selected_by_raw_paths(refined, selected, raw_paths)
    write_parquet(refined_path, refined)

    raw = read_raw(raw_paths)
    validate_raw(raw)
    raw = normalize_raw(raw)
    validate_selected_scope(raw, selected)

    ingest_ts = utc_now_iso()
    run_id = "futures_raw_d1_derivation_" + run_date + "_" + stable_id([ingest_ts, snapshot_date, len(selected), from_date, till])
    d1 = aggregate_d1(raw, ingest_ts)
    counts = quality_counts(raw, d1)
    aggregate_status, aggregate_notes = status_from_counts(counts)
    partition_paths = write_partitions(d1, data_root) if aggregate_status != "fail" else []
    summaries = per_instrument(raw, d1, partition_paths)

    for secid, summary in summaries.items():
        if secid not in SHORT_HISTORY_ALLOWED and summary.get("short_history_flag") is True:
            raise RuntimeError("Unexpected short_history_flag=true for " + str(secid))

    quality_rows = build_quality_rows(run_id, run_date, summaries)
    quality_counts_by_status = {str(k): int(v) for k, v in quality_rows["quality_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    aggregate_outputs = output_paths(data_root, run_date)
    write_parquet(aggregate_outputs["quality_report"], quality_rows)

    chunk_outputs = []
    for fam, frame in chunk_groups(selected):
        chunk_id = "raw_d1_" + stable_id([run_id, fam, from_date, till, ",".join(frame["secid"].astype(str).tolist())])
        paths_for_chunk = [x for x in partition_paths if "/family=" + str(fam) + "/" in str(x)]
        chunk_out = output_paths(data_root, run_date, chunk_id)
        chunk_quality = quality_rows.loc[quality_rows["secid"].astype(str).isin(frame["secid"].astype(str).tolist())].copy()
        write_parquet(chunk_out["quality_report"], chunk_quality)
        chunk_manifest = {
            "schema_version": SCHEMA_MANIFEST,
            "run_id": run_id,
            "chunk_id": chunk_id,
            "dataset_stage": DATASET_STAGE,
            "family_code": fam,
            "secid_list": frame["secid"].astype(str).tolist(),
            "date_from": from_date or str(raw["trade_date"].min()),
            "date_till": till or str(raw["trade_date"].max()),
            "status": "succeeded" if not chunk_quality.empty and chunk_quality["quality_status"].astype(str).eq("pass").all() else "partial_failed",
            "started_at": ingest_ts,
            "finished_at": utc_now_iso(),
            "failed_secid": chunk_quality.loc[chunk_quality["quality_status"].astype(str) != "pass", "secid"].astype(str).tolist(),
            "deferred_secid": [],
            "skipped_secid": [],
            "output_partitions": paths_for_chunk,
            "quality_summary": {str(k): int(v) for k, v in chunk_quality["quality_status"].astype(str).value_counts(dropna=False).to_dict().items()},
            "error_code": "" if chunk_quality["quality_status"].astype(str).eq("pass").all() else "secid_level_failure",
            "error_message": "",
            "retry_allowed": not chunk_quality["quality_status"].astype(str).eq("pass").all(),
            "next_retry_scope": "secid_date_range_dataset_stage" if not chunk_quality["quality_status"].astype(str).eq("pass").all() else "",
        }
        dump_json(chunk_out["manifest"], chunk_manifest)
        chunk_outputs.append(chunk_out)

    manifest = {
        "schema_version": SCHEMA_MANIFEST,
        "run_id": run_id,
        "run_date": run_date,
        "snapshot_date": snapshot_date,
        "dataset_stage": DATASET_STAGE,
        "selection_mode": mode,
        "ingest_ts": ingest_ts,
        "input_artifacts": {
            "eligibility_snapshot": str(ep),
            "refined_raw_d1_eligibility_snapshot": str(refined_path),
            "raw_5m_quality_reports": [str(x) for x in quality_report_paths(data_root, str(args.quality_reports or ""))],
            "raw_5m_partition_root": str(raw_root(data_root)),
            "raw_5m_partitions_read": [str(x) for x in raw_paths],
        },
        "selected_universe": {
            "board": "RFUD",
            "classification_status": "included",
            "raw_5m_eligible": True,
            "raw_d1_eligible": True,
            "family_count": int(selected["family_code"].nunique()),
            "secid_count": int(len(selected)),
            "secids": selected["secid"].astype(str).tolist(),
        },
        "output_artifacts": aggregate_outputs,
        "chunk_outputs": chunk_outputs,
        "partition_paths_created": partition_paths,
        "instrument_summaries": summaries,
        "quality_status_counts": quality_counts_by_status,
        "source_to_output_row_check": counts,
        "calendar_validation_summary": {"calendar_denominator_status": "canonical_apim_futures_xml"},
        "builder_result_verdict": "pass" if quality_counts_by_status.get("fail", 0) == 0 and aggregate_status == "pass" else "fail",
        "aggregate_review_notes": aggregate_notes,
        "forbidden_scope_checks": {
            "futoi_join": "not_performed",
            "continuous_fields": "not_emitted",
            "continuous_build": "not_performed",
            "w1_build": "not_performed",
        },
    }
    dump_json(aggregate_outputs["manifest"], manifest)

    print_json_line("selection_mode", mode)
    print_json_line("selected_universe", manifest["selected_universe"])
    print_json_line("source_raw_5m_status", {"accepted_quality_rows": int(len(quality)), "raw_5m_partitions_read": int(len(raw_paths))})
    print_json_line("output_artifacts_created", aggregate_outputs)
    print_json_line("chunk_outputs", chunk_outputs)
    print_json_line("derived_d1_quality_summary", {"quality_status_counts": quality_counts_by_status, "instruments": summaries})
    print_json_line("source_to_output_row_check", counts)
    print_json_line("builder_result_verdict", manifest["builder_result_verdict"])
    return 0 if manifest["builder_result_verdict"] == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
