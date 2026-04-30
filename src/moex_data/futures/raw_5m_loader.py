#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path.cwd() / "src"))

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures import liquidity_history_metrics_probe_apim_calendar as apim_calendar

TZ_MSK = ZoneInfo("Europe/Moscow")
SCHEMA_RAW_5M = "futures_raw_5m.v1"
SCHEMA_QUALITY = "futures_raw_5m_quality_report.v1"
SCHEMA_MANIFEST = "futures_raw_5m_loader_manifest.v1"
DEFAULT_WHITELIST = ["SiM6", "SiU6", "SiU7", "SiZ6", "USDRUBF"]
DEFAULT_EXCLUDED = ["SiH7", "SiM7"]
SHORT_HISTORY_ALLOWED = {"SiU7"}


def today_msk():
    return datetime.now(TZ_MSK).date().isoformat()


def utc_now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def stable_id(parts):
    return hashlib.sha256("|".join([str(x) for x in parts]).encode("utf-8")).hexdigest()[:24]


def parse_list(value, default):
    text = str(value or "").strip()
    if not text:
        return list(default)
    return [x.strip() for x in text.split(",") if x.strip()]


def output_paths(data_root, run_date):
    return {
        "raw_5m_partition_root": str(data_root / "futures" / "raw_5m"),
        "quality_report": str(data_root / "futures" / "quality" / "raw_5m_loader" / ("run_date=" + run_date) / "futures_raw_5m_quality_report.parquet"),
        "manifest": str(data_root / "futures" / "runs" / "raw_5m_loader" / ("run_date=" + run_date) / "manifest.json"),
    }


def partition_path(data_root, trade_date, family_code, secid):
    return data_root / "futures" / "raw_5m" / ("trade_date=" + trade_date) / ("family=" + family_code) / ("secid=" + secid) / "part.parquet"


def load_inputs(data_root, contracts, snapshot_date):
    normalized_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["normalized_registry"], snapshot_date)
    liquidity_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["liquidity_screen"], snapshot_date)
    history_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["history_depth_screen"], snapshot_date)
    for path in [normalized_path, liquidity_path, history_path]:
        if not path.exists():
            raise FileNotFoundError("Missing required input artifact: " + str(path))
    return {
        "normalized_registry": str(normalized_path),
        "liquidity_screen": str(liquidity_path),
        "history_depth_screen": str(history_path),
    }, pd.read_parquet(normalized_path), pd.read_parquet(liquidity_path), pd.read_parquet(history_path)


def select_instruments(normalized, liquidity, history, whitelist, excluded):
    for name, frame in [("normalized_registry", normalized), ("liquidity_screen", liquidity), ("history_depth_screen", history)]:
        if "secid" not in frame.columns:
            raise RuntimeError(name + " missing secid column")
    rows = []
    for secid in whitelist:
        if secid in excluded:
            raise RuntimeError("Whitelisted instrument is also excluded: " + secid)
        nrow = normalized.loc[normalized["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        lrow = liquidity.loc[liquidity["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        hrow = history.loc[history["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        if nrow.empty or lrow.empty or hrow.empty:
            raise RuntimeError("Whitelisted instrument is missing from accepted artifacts: " + secid)
        liquidity_status = str(lrow.iloc[0].get("liquidity_status", "")).strip()
        history_status = str(hrow.iloc[0].get("history_depth_status", "")).strip()
        if liquidity_status != "pass":
            raise RuntimeError("liquidity_status is not pass for " + secid + ": " + liquidity_status)
        short_history_flag = secid in SHORT_HISTORY_ALLOWED
        if short_history_flag:
            if history_status not in ["pass", "review_required"]:
                raise RuntimeError("short-history instrument has invalid history_depth_status: " + secid + " " + history_status)
        elif history_status != "pass":
            raise RuntimeError("history_depth_status is not pass for " + secid + ": " + history_status)
        row = nrow.iloc[0].to_dict()
        row["secid"] = secid
        row["board"] = str(row.get("board", "rfud") or "rfud")
        row["family_code"] = str(row.get("family_code", "") or "")
        row["liquidity_status"] = liquidity_status
        row["history_depth_status"] = history_status
        row["short_history_flag"] = short_history_flag
        row["first_available_date"] = hrow.iloc[0].get("first_available_date")
        row["last_available_date"] = hrow.iloc[0].get("last_available_date")
        row["screen_from"] = hrow.iloc[0].get("screen_from")
        row["screen_till"] = hrow.iloc[0].get("screen_till")
        rows.append(row)
    return pd.DataFrame(rows)


def date_bounds(row, from_override, till_override):
    start = base.parse_iso_date(from_override) if from_override else None
    end = base.parse_iso_date(till_override) if till_override else None
    if not start:
        start = base.parse_iso_date(row.get("first_available_date")) or base.parse_iso_date(row.get("screen_from"))
    if not end:
        end = base.parse_iso_date(row.get("last_available_date")) or base.parse_iso_date(row.get("screen_till"))
    if not start or not end:
        raise RuntimeError("Cannot resolve loader date range for " + str(row.get("secid")))
    return start, end


def combine_ts(frame, date_col, time_col):
    dates = frame[date_col].map(base.parse_iso_date)
    if not time_col:
        return pd.to_datetime(dates, errors="coerce")
    values = []
    for d, t in zip(dates.tolist(), frame[time_col].astype(str).str.strip().tolist()):
        if not d or not t or t.lower() == "nan":
            values.append(None)
        elif len(t) >= 10 and "-" in t[:10]:
            values.append(t)
        else:
            values.append(d + " " + t)
    return pd.to_datetime(pd.Series(values), errors="coerce")


def normalize_tradestats(frame, secid, family_code, board, source_url, ingest_ts, short_history_flag, calendar_status):
    if frame.empty:
        return pd.DataFrame(), {"error": "empty_frame", "columns": []}
    date_col = base.canonical_column(frame, ["tradedate", "TRADEDATE", "date", "DATE"])
    time_col = base.canonical_column(frame, ["tradetime", "TRADETIME", "time", "TIME", "moment", "MOMENT"])
    open_col = base.canonical_column(frame, ["pr_open", "PR_OPEN", "open", "OPEN"])
    high_col = base.canonical_column(frame, ["pr_high", "PR_HIGH", "high", "HIGH"])
    low_col = base.canonical_column(frame, ["pr_low", "PR_LOW", "low", "LOW"])
    close_col = base.canonical_column(frame, ["pr_close", "PR_CLOSE", "close", "CLOSE"])
    volume_col = base.canonical_column(frame, ["vol", "VOL", "volume", "VOLUME", "qty", "QTY"])
    value_col = base.canonical_column(frame, ["val", "VAL", "value", "VALUE", "turnover", "TURNOVER"])
    trades_col = base.canonical_column(frame, ["trades", "TRADES", "num_trades", "NUM_TRADES", "numtrades", "NUMTRADES"])
    seq_col = base.canonical_column(frame, ["seqnum", "SEQNUM", "source_seqnum", "SOURCE_SEQNUM"])
    required = {"tradedate": date_col, "open": open_col, "high": high_col, "low": low_col, "close": close_col, "volume": volume_col}
    missing = [k for k, v in required.items() if not v]
    if missing:
        return pd.DataFrame(), {"error": "missing_required_columns:" + ",".join(missing), "columns": [str(x) for x in frame.columns]}
    out = pd.DataFrame()
    out["trade_date"] = frame[date_col].map(base.parse_iso_date)
    out["ts"] = combine_ts(frame, date_col, time_col)
    out["end"] = out["ts"]
    out["session_date"] = out["trade_date"]
    out["board"] = board
    out["secid"] = secid
    out["family_code"] = family_code
    out["open"] = base.coerce_numeric(frame[open_col])
    out["high"] = base.coerce_numeric(frame[high_col])
    out["low"] = base.coerce_numeric(frame[low_col])
    out["close"] = base.coerce_numeric(frame[close_col])
    out["volume"] = base.coerce_numeric(frame[volume_col])
    out["value"] = base.coerce_numeric(frame[value_col]) if value_col else None
    out["num_trades"] = base.coerce_numeric(frame[trades_col]) if trades_col else None
    out["source"] = "MOEX_ALGOPACK_FO_TRADESTATS"
    out["source_endpoint_url"] = source_url
    out["source_seqnum"] = frame[seq_col].astype(str) if seq_col else None
    out["ingest_ts"] = ingest_ts
    out["schema_version"] = SCHEMA_RAW_5M
    out["short_history_flag"] = bool(short_history_flag)
    out["calendar_denominator_status"] = calendar_status
    out = out.loc[out["trade_date"].notna() & out["ts"].notna()].copy()
    out = out.sort_values(["trade_date", "ts", "secid"]).reset_index(drop=True)
    meta = {
        "error": "",
        "columns": [str(x) for x in frame.columns],
        "mapped_columns": {
            "tradedate": str(date_col),
            "tradetime": str(time_col) if time_col else None,
            "open": str(open_col),
            "high": str(high_col),
            "low": str(low_col),
            "close": str(close_col),
            "volume": str(volume_col),
            "value": str(value_col) if value_col else None,
            "num_trades": str(trades_col) if trades_col else None,
            "source_seqnum": str(seq_col) if seq_col else None,
        },
    }
    return out, meta


def quality_counts(frame, expected_calendar):
    if frame.empty:
        return {"rows": 0, "trade_dates": 0, "min_ts": None, "max_ts": None, "duplicate_ts_count": 0, "null_ohlc_count": 0, "invalid_ohlc_count": 0, "off_calendar_date_count": None, "missing_expected_trading_days": None}
    duplicates = int(frame.duplicated(subset=["trade_date", "ts", "secid"]).sum())
    null_ohlc = int(frame[["open", "high", "low", "close"]].isna().any(axis=1).sum())
    invalid = (frame["high"] < frame["low"]) | (frame["open"] > frame["high"]) | (frame["open"] < frame["low"]) | (frame["close"] > frame["high"]) | (frame["close"] < frame["low"])
    dates = set(frame["trade_date"].dropna().astype(str).tolist())
    off_calendar = None
    missing_expected = None
    if expected_calendar is not None and dates:
        off_calendar = len(dates - expected_calendar)
        expected = set([x for x in expected_calendar if min(dates) <= x <= max(dates)])
        missing_expected = len(expected - dates)
    return {"rows": int(len(frame)), "trade_dates": len(dates), "min_ts": str(frame["ts"].min()), "max_ts": str(frame["ts"].max()), "duplicate_ts_count": duplicates, "null_ohlc_count": null_ohlc, "invalid_ohlc_count": int(invalid.fillna(True).sum()), "off_calendar_date_count": off_calendar, "missing_expected_trading_days": missing_expected}


def status_from_counts(counts, fetch_status, calendar_status, history_status, short_history_flag):
    if fetch_status != "completed" or int(counts.get("rows") or 0) == 0:
        return "fail", "source fetch failed or produced zero rows"
    if calendar_status != "canonical_apim_futures_xml":
        return "fail", "calendar denominator is not canonical_apim_futures_xml"
    for key, note in [("duplicate_ts_count", "duplicate primary timestamp rows detected before partition write"), ("null_ohlc_count", "null OHLC values detected"), ("invalid_ohlc_count", "invalid OHLC ordering detected"), ("off_calendar_date_count", "loaded trade dates outside APIM futures calendar")]:
        if counts.get(key) is not None and int(counts.get(key) or 0) > 0:
            return "fail", note
    if short_history_flag:
        return "pass", "loaded with explicit short_history_flag=true"
    if history_status != "pass":
        return "review_required", "history_depth_status is not pass"
    return "pass", "raw 5m partition load completed"


def write_partitions(frame, data_root, family_code, secid):
    paths = []
    clean = frame.drop_duplicates(subset=["trade_date", "ts", "secid"], keep="last").copy()
    for trade_date, part in clean.groupby("trade_date"):
        path = partition_path(data_root, str(trade_date), family_code, secid)
        path.parent.mkdir(parents=True, exist_ok=True)
        part.sort_values(["ts", "secid"]).to_parquet(path, index=False)
        paths.append(str(path))
    return paths


def print_json_line(key, value):
    print(key + ": " + json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--whitelist", default=",".join(DEFAULT_WHITELIST))
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    args = parser.parse_args()

    root = Path.cwd().resolve()
    data_root = base.resolve_data_root(args)
    snapshot_date = str(args.snapshot_date).strip()
    run_date = str(args.run_date).strip()
    whitelist = parse_list(args.whitelist, DEFAULT_WHITELIST)
    excluded = parse_list(args.excluded, DEFAULT_EXCLUDED)
    ingest_ts = utc_now_iso()
    run_id = "futures_raw_5m_loader_" + run_date + "_" + stable_id([snapshot_date, ingest_ts, ",".join(whitelist)])

    base.assert_files_exist(root, base.REQUIRED_CONTRACTS)
    contracts = base.load_contract_values(root)
    input_paths, normalized, liquidity, history = load_inputs(data_root, contracts, snapshot_date)
    instruments = select_instruments(normalized, liquidity, history, whitelist, excluded)

    ranges = {}
    starts = []
    ends = []
    for _, row in instruments.iterrows():
        start, end = date_bounds(row, str(args.from_date or ""), str(args.till or ""))
        secid = str(row.get("secid"))
        ranges[secid] = {"from": start, "till": end}
        starts.append(start)
        ends.append(end)
    calendar_from = min(starts)
    calendar_till = max(ends)
    expected_calendar, calendar_status = apim_calendar.fetch_futures_calendar(calendar_from, calendar_till, float(args.timeout), str(args.iss_base_url))
    if expected_calendar is None or calendar_status != "canonical_apim_futures_xml":
        raise RuntimeError("APIM futures calendar validation failed: " + str(calendar_status))

    outputs = output_paths(data_root, run_date)
    partition_paths = []
    quality_rows = []
    summaries = {}
    for _, row in instruments.iterrows():
        secid = str(row.get("secid"))
        family_code = str(row.get("family_code"))
        board = str(row.get("board", "rfud") or "rfud")
        start = ranges[secid]["from"]
        end = ranges[secid]["till"]
        short_history_flag = bool(row.get("short_history_flag"))
        source_frame, source_url, fetch_status, fetch_error = base.fetch_tradestats(secid, start, end, float(args.timeout), str(args.apim_base_url), str(args.iss_base_url))
        raw, meta = normalize_tradestats(source_frame, secid, family_code, board, source_url, ingest_ts, short_history_flag, calendar_status)
        counts = quality_counts(raw, expected_calendar)
        quality_status, notes = status_from_counts(counts, fetch_status, calendar_status, str(row.get("history_depth_status", "")), short_history_flag)
        paths = write_partitions(raw, data_root, family_code, secid) if quality_status != "fail" else []
        partition_paths.extend(paths)
        quality_rows.append({
            "quality_report_id": stable_id([run_id, secid]),
            "run_id": run_id,
            "run_date": run_date,
            "snapshot_date": snapshot_date,
            "board": board,
            "secid": secid,
            "family_code": family_code,
            "dataset_id": "futures_raw_5m",
            "schema_version": SCHEMA_QUALITY,
            "requested_from": start,
            "requested_till": end,
            "source_endpoint_url": source_url,
            "fetch_status": fetch_status,
            "fetch_error": fetch_error or None,
            "normalization_error": meta.get("error") or None,
            "rows": counts.get("rows"),
            "trade_dates": counts.get("trade_dates"),
            "min_ts": counts.get("min_ts"),
            "max_ts": counts.get("max_ts"),
            "duplicate_ts_count": counts.get("duplicate_ts_count"),
            "null_ohlc_count": counts.get("null_ohlc_count"),
            "invalid_ohlc_count": counts.get("invalid_ohlc_count"),
            "off_calendar_date_count": counts.get("off_calendar_date_count"),
            "missing_expected_trading_days": counts.get("missing_expected_trading_days"),
            "partition_count": len(paths),
            "calendar_denominator_status": calendar_status,
            "history_depth_status": row.get("history_depth_status"),
            "liquidity_status": row.get("liquidity_status"),
            "short_history_flag": short_history_flag,
            "quality_status": quality_status,
            "review_notes": notes,
            "mapped_columns_json": json.dumps(meta.get("mapped_columns") or {}, ensure_ascii=False, sort_keys=True),
            "observed_columns_json": json.dumps(meta.get("columns") or [], ensure_ascii=False, sort_keys=True),
        })
        summaries[secid] = {"requested_from": start, "requested_till": end, "rows": counts.get("rows"), "trade_dates": counts.get("trade_dates"), "partition_count": len(paths), "quality_status": quality_status, "short_history_flag": short_history_flag, "review_notes": notes}

    quality = pd.DataFrame(quality_rows)
    Path(outputs["quality_report"]).parent.mkdir(parents=True, exist_ok=True)
    quality.to_parquet(outputs["quality_report"], index=False)
    quality_counts = {str(k): int(v) for k, v in quality["quality_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    manifest = {
        "schema_version": SCHEMA_MANIFEST,
        "run_id": run_id,
        "run_date": run_date,
        "snapshot_date": snapshot_date,
        "ingest_ts": ingest_ts,
        "loader_whitelist_applied": whitelist,
        "excluded_instruments_confirmed": excluded,
        "input_artifacts": input_paths,
        "output_artifacts": outputs,
        "partition_paths_created": partition_paths,
        "instrument_summaries": summaries,
        "quality_status_counts": quality_counts,
        "calendar_validation_summary": {"calendar_denominator_status": calendar_status, "calendar_from": calendar_from, "calendar_till": calendar_till, "expected_trading_days": len(expected_calendar)},
        "short_history_handling": {"SiU7": summaries.get("SiU7")},
        "loader_result_verdict": "pass" if quality_counts.get("fail", 0) == 0 else "fail",
    }
    Path(outputs["manifest"]).parent.mkdir(parents=True, exist_ok=True)
    Path(outputs["manifest"]).write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")

    print_json_line("loader_whitelist_applied", whitelist)
    print_json_line("excluded_instruments_confirmed", excluded)
    print_json_line("output_artifacts_created", outputs)
    print_json_line("raw_5m_quality_summary", {"quality_status_counts": quality_counts, "instruments": summaries})
    print_json_line("calendar_validation_summary", manifest["calendar_validation_summary"])
    print_json_line("short_history_handling", manifest["short_history_handling"])
    print_json_line("loader_result_verdict", manifest["loader_result_verdict"])
    return 0 if manifest["loader_result_verdict"] == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
