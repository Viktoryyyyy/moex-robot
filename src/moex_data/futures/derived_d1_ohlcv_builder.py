#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

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
REQUIRED_CONTRACTS = [
    "contracts/datasets/futures_raw_5m_contract.md",
    "contracts/datasets/futures_derived_d1_ohlcv_contract.md",
    "contracts/datasets/futures_derived_d1_ohlcv_manifest_contract.md",
    "contracts/datasets/futures_derived_d1_ohlcv_quality_report_contract.md",
]



def output_paths(data_root, run_date):
    return {
        "derived_d1_partition_root": str(data_root / "futures" / "derived_d1_ohlcv"),
        "quality_report": str(data_root / "futures" / "quality" / "derived_d1_ohlcv_builder" / ("run_date=" + run_date) / "futures_derived_d1_ohlcv_quality_report.parquet"),
        "manifest": str(data_root / "futures" / "runs" / "derived_d1_ohlcv_builder" / ("run_date=" + run_date) / "manifest.json"),
    }


def raw_root(data_root):
    return data_root / "futures" / "raw_5m"


def d1_path(data_root, trade_date, family_code, secid):
    return data_root / "futures" / "derived_d1_ohlcv" / ("trade_date=" + str(trade_date)) / ("family=" + str(family_code)) / ("secid=" + str(secid)) / "part.parquet"


def partition_value(path, key):
    prefix = key + "="
    for part in Path(path).parts:
        if part.startswith(prefix):
            return part[len(prefix):]
    return ""


def discover_raw_paths(data_root, whitelist, excluded, from_date, till):
    root = raw_root(data_root)
    if not root.exists():
        raise FileNotFoundError("Missing raw 5m root: " + str(root))
    whitelist_upper = {x.upper() for x in whitelist}
    excluded_upper = {x.upper() for x in excluded}
    paths = []
    excluded_hits = []
    for path in sorted(root.glob("trade_date=*/family=*/secid=*/part.parquet")):
        secid = partition_value(path, "secid")
        trade_date = partition_value(path, "trade_date")
        if secid.upper() in excluded_upper:
            excluded_hits.append(str(path))
            continue
        if secid.upper() not in whitelist_upper:
            continue
        if from_date and trade_date < from_date:
            continue
        if till and trade_date > till:
            continue
        paths.append(path)
    if excluded_hits:
        raise RuntimeError("Excluded instruments found in raw 5m input paths: " + json.dumps(excluded_hits[:20], ensure_ascii=False))
    if not paths:
        raise RuntimeError("No raw 5m partitions found for accepted whitelist")
    return paths


def read_raw(paths):
    frames = []
    for path in paths:
        frame = pd.read_parquet(path)
        frame["_source_partition_path"] = str(path)
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def validate_raw(frame):
    required = ["trade_date", "ts", "board", "secid", "family_code", "open", "high", "low", "close", "volume", "schema_version", "short_history_flag", "calendar_denominator_status"]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        raise RuntimeError("Raw 5m input missing required fields: " + ", ".join(missing))
    schemas = sorted([str(x) for x in frame["schema_version"].dropna().unique().tolist()])
    if schemas != ["futures_raw_5m.v1"]:
        raise RuntimeError("Raw 5m schema mismatch: " + json.dumps(schemas, ensure_ascii=False))
    calendar = sorted([str(x) for x in frame["calendar_denominator_status"].dropna().unique().tolist()])
    if calendar != ["canonical_apim_futures_xml"]:
        raise RuntimeError("Raw 5m calendar status mismatch: " + json.dumps(calendar, ensure_ascii=False))


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
            "short_history_flag": first_bool(part["short_history_flag"]),
            "calendar_denominator_status": "canonical_apim_futures_xml",
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
    return "pass", "derived D1 OHLCV build completed from raw 5m"


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
        short_history_flag = first_bool(part["short_history_flag"])
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


def validate_exact_scope(raw, whitelist, excluded):
    observed = sorted(raw["secid"].dropna().astype(str).unique().tolist())
    if sorted([x.upper() for x in observed]) != sorted([x.upper() for x in whitelist]):
        raise RuntimeError("Raw partitions do not cover exact accepted whitelist. observed=" + json.dumps(observed, ensure_ascii=False) + " expected=" + json.dumps(sorted(whitelist), ensure_ascii=False))
    excluded_upper = {x.upper() for x in excluded}
    hits = [x for x in observed if x.upper() in excluded_upper]
    if hits:
        raise RuntimeError("Excluded instruments found in selected raw rows: " + json.dumps(hits, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--whitelist", default=",".join(DEFAULT_WHITELIST))
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    args = parser.parse_args()

    root = Path.cwd().resolve()
    data_root = base.resolve_data_root(args)
    run_date = str(args.run_date).strip()
    from_date = base.parse_iso_date(str(args.from_date or "")) if str(args.from_date or "").strip() else ""
    till = base.parse_iso_date(str(args.till or "")) if str(args.till or "").strip() else ""
    whitelist = parse_list(args.whitelist, DEFAULT_WHITELIST)
    excluded = parse_list(args.excluded, DEFAULT_EXCLUDED)
    ingest_ts = utc_now_iso()
    run_id = "futures_derived_d1_ohlcv_builder_" + run_date + "_" + stable_id([ingest_ts, ",".join(whitelist), from_date, till])

    base.assert_files_exist(root, REQUIRED_CONTRACTS)
    for secid in whitelist:
        if secid in excluded:
            raise RuntimeError("Whitelisted instrument is also excluded: " + secid)

    raw_paths = discover_raw_paths(data_root, whitelist, excluded, from_date, till)
    raw = read_raw(raw_paths)
    validate_raw(raw)
    raw = normalize_raw(raw)
    validate_exact_scope(raw, whitelist, excluded)

    d1 = aggregate_d1(raw, ingest_ts)
    counts = quality_counts(raw, d1)
    aggregate_status, aggregate_notes = status_from_counts(counts)
    partition_paths = write_partitions(d1, data_root) if aggregate_status != "fail" else []
    summaries = per_instrument(raw, d1, partition_paths)

    if "SiU7" in summaries and summaries["SiU7"].get("short_history_flag") is not True:
        raise RuntimeError("SiU7 short_history_flag is not true in derived D1 output")
    for secid, summary in summaries.items():
        if secid not in SHORT_HISTORY_ALLOWED and summary.get("short_history_flag") is True:
            raise RuntimeError("Unexpected short_history_flag=true for " + str(secid))

    outputs = output_paths(data_root, run_date)
    quality_rows = []
    for secid, summary in summaries.items():
        quality_rows.append({
            "quality_report_id": stable_id([run_id, secid]),
            "run_id": run_id,
            "run_date": run_date,
            "secid": secid,
            "dataset_id": "futures_derived_d1_ohlcv",
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
    quality = pd.DataFrame(quality_rows).sort_values(["secid"]).reset_index(drop=True)
    Path(outputs["quality_report"]).parent.mkdir(parents=True, exist_ok=True)
    quality.to_parquet(outputs["quality_report"], index=False)
    quality_counts_by_status = {str(k): int(v) for k, v in quality["quality_status"].astype(str).value_counts(dropna=False).to_dict().items()}

    manifest = {
        "schema_version": SCHEMA_MANIFEST,
        "run_id": run_id,
        "run_date": run_date,
        "ingest_ts": ingest_ts,
        "builder_whitelist_applied": whitelist,
        "excluded_instruments_confirmed": excluded,
        "input_artifacts": {"raw_5m_partition_root": str(raw_root(data_root)), "raw_5m_partitions_read": [str(x) for x in raw_paths]},
        "output_artifacts": outputs,
        "partition_paths_created": partition_paths,
        "instrument_summaries": summaries,
        "quality_status_counts": quality_counts_by_status,
        "source_to_output_row_check": counts,
        "short_history_handling": {"SiU7": summaries.get("SiU7")},
        "calendar_validation_summary": {"calendar_denominator_status": "canonical_apim_futures_xml"},
        "builder_result_verdict": "pass" if quality_counts_by_status.get("fail", 0) == 0 and aggregate_status == "pass" else "fail",
        "aggregate_review_notes": aggregate_notes,
    }
    Path(outputs["manifest"]).parent.mkdir(parents=True, exist_ok=True)
    Path(outputs["manifest"]).write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")

    print_json_line("builder_whitelist_applied", whitelist)
    print_json_line("excluded_instruments_confirmed", excluded)
    print_json_line("output_artifacts_created", outputs)
    print_json_line("derived_d1_quality_summary", {"quality_status_counts": quality_counts_by_status, "instruments": summaries})
    print_json_line("source_to_output_row_check", counts)
    print_json_line("short_history_handling", manifest["short_history_handling"])
    print_json_line("builder_result_verdict", manifest["builder_result_verdict"])
    return 0 if manifest["builder_result_verdict"] == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
