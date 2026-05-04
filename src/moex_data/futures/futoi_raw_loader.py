#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

import pandas as pd

from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures import liquidity_history_metrics_probe_apim_calendar as apim_calendar

from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import DEFAULT_WHITELIST
from moex_data.futures.slice1_common import SHORT_HISTORY_ALLOWED
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import print_json_line
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

SCHEMA_FUTOI_RAW = "futures_futoi_5m_raw.v1"
SCHEMA_QUALITY = "futures_futoi_5m_raw_quality_report.v1"
SCHEMA_MANIFEST = "futures_futoi_5m_raw_loader_manifest.v1"
FUTOI_AVAILABILITY_CONTRACT = "contracts/datasets/futures_futoi_availability_report_contract.md"


def output_paths(data_root, run_date):
    return {
        "futoi_raw_partition_root": str(data_root / "futures" / "futoi_raw"),
        "quality_report": str(data_root / "futures" / "quality" / "futoi_raw_loader" / ("run_date=" + run_date) / "futures_futoi_5m_raw_quality_report.parquet"),
        "manifest": str(data_root / "futures" / "runs" / "futoi_raw_loader" / ("run_date=" + run_date) / "manifest.json"),
    }


def partition_path(data_root, trade_date, family_code, secid):
    return data_root / "futures" / "futoi_raw" / ("trade_date=" + trade_date) / ("family=" + family_code) / ("secid=" + secid) / "part.parquet"


def load_contract_values_extended(root):
    contracts = base.load_contract_values(root)
    text = base.read_text(root / FUTOI_AVAILABILITY_CONTRACT)
    contracts[FUTOI_AVAILABILITY_CONTRACT] = {
        "path_pattern": base.extract_contract_value(text, "path_pattern"),
        "schema_version": base.extract_contract_value(text, "schema_version"),
        "format": base.extract_contract_value(text, "format"),
    }
    return contracts


def resolve_path_from_contract(data_root, contracts, contract_rel, snapshot_date):
    pattern = str(contracts.get(contract_rel, {}).get("path_pattern") or "").strip()
    if not pattern:
        raise RuntimeError("Contract path_pattern is missing: " + contract_rel)
    prefix = "${MOEX_DATA_ROOT}"
    if not pattern.startswith(prefix):
        raise RuntimeError("Unsupported non-MOEX_DATA_ROOT path_pattern in " + contract_rel)
    rel = pattern[len(prefix):].lstrip("/")
    rel = rel.replace("{snapshot_date}", snapshot_date)
    rel = rel.replace("YYYY-MM-DD", snapshot_date)
    return data_root / rel


def load_inputs(data_root, contracts, snapshot_date):
    normalized_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["normalized_registry"], snapshot_date)
    liquidity_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["liquidity_screen"], snapshot_date)
    history_path = base.resolve_contract_path(data_root, contracts, base.CONTRACT_BY_ID["history_depth_screen"], snapshot_date)
    futoi_availability_path = resolve_path_from_contract(data_root, contracts, FUTOI_AVAILABILITY_CONTRACT, snapshot_date)
    for path in [normalized_path, liquidity_path, history_path, futoi_availability_path]:
        if not path.exists():
            raise FileNotFoundError("Missing required input artifact: " + str(path))
    return {
        "normalized_registry": str(normalized_path),
        "liquidity_screen": str(liquidity_path),
        "history_depth_screen": str(history_path),
        "futoi_availability_report": str(futoi_availability_path),
    }, pd.read_parquet(normalized_path), pd.read_parquet(liquidity_path), pd.read_parquet(history_path), pd.read_parquet(futoi_availability_path)


def select_instruments(normalized, liquidity, history, futoi_availability, whitelist, excluded):
    for name, frame in [("normalized_registry", normalized), ("liquidity_screen", liquidity), ("history_depth_screen", history), ("futoi_availability_report", futoi_availability)]:
        if "secid" not in frame.columns:
            raise RuntimeError(name + " missing secid column")
    rows = []
    for secid in whitelist:
        if secid in excluded:
            raise RuntimeError("Whitelisted instrument is also excluded: " + secid)
        nrow = normalized.loc[normalized["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        lrow = liquidity.loc[liquidity["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        hrow = history.loc[history["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        arow = futoi_availability.loc[futoi_availability["secid"].astype(str).str.upper() == secid.upper()].tail(1)
        if nrow.empty or lrow.empty or hrow.empty or arow.empty:
            raise RuntimeError("Whitelisted instrument is missing from accepted artifacts: " + secid)
        liquidity_status = str(lrow.iloc[0].get("liquidity_status", "")).strip()
        history_status = str(hrow.iloc[0].get("history_depth_status", "")).strip()
        futoi_availability_status = str(arow.iloc[0].get("availability_status", "")).strip()
        futoi_probe_status = str(arow.iloc[0].get("probe_status", "")).strip()
        if liquidity_status != "pass":
            raise RuntimeError("liquidity_status is not pass for " + secid + ": " + liquidity_status)
        if futoi_availability_status != "available" or futoi_probe_status != "completed":
            raise RuntimeError("FUTOI availability is not completed/available for " + secid + ": " + futoi_availability_status + "/" + futoi_probe_status)
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
        row["futoi_availability_status"] = futoi_availability_status
        row["futoi_probe_status"] = futoi_probe_status
        row["short_history_flag"] = short_history_flag
        row["first_available_date"] = hrow.iloc[0].get("first_available_date")
        row["last_available_date"] = hrow.iloc[0].get("last_available_date")
        row["screen_from"] = hrow.iloc[0].get("screen_from")
        row["screen_till"] = hrow.iloc[0].get("screen_till")
        row["futoi_source_endpoint_url_probe"] = arow.iloc[0].get("source_endpoint_url")
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
    raw_times = frame[time_col].astype(str).str.strip().tolist()
    for d, t in zip(dates.tolist(), raw_times):
        if not d or not t or t.lower() == "nan":
            values.append(None)
        elif len(t) >= 10 and "-" in t[:10]:
            values.append(t)
        else:
            values.append(d + " " + t)
    return pd.to_datetime(pd.Series(values), errors="coerce")


def ticker_for_instrument(secid, family_code):
    secid_text = str(secid or "").strip()
    family_text = str(family_code or "").strip()
    if secid_text.upper() == "USDRUBF":
        return "usdrubf"
    if family_text:
        return family_text.lower()
    return secid_text.lower()


def endpoint_candidates(secid, family_code):
    primary = ticker_for_instrument(secid, family_code)
    candidates = []
    seen = set()
    for ticker in [primary, str(secid or "").strip().lower(), str(family_code or "").strip().lower()]:
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        candidates.append(ticker)
    return candidates


def fetch_futoi(secid, family_code, screen_from, screen_till, timeout, apim_base_url, iss_base_url):
    tickers = endpoint_candidates(secid, family_code)
    last_url = ""
    last_error = ""
    for ticker in tickers:
        path = "/iss/analyticalproducts/futoi/securities/" + ticker + ".json"
        for base_url, use_apim in [(apim_base_url, True), (iss_base_url, False)]:
            params = {"from": screen_from, "till": screen_till}
            last_url = base.url_join(base_url, path)
            try:
                frame = base.fetch_paged_frame(base_url, path, params, "data", timeout, use_apim)
                if not frame.empty:
                    return frame, last_url, "completed", "", ticker
            except Exception as exc:
                last_error = exc.__class__.__name__ + ": " + str(exc)[:500]
        generic = "/iss/analyticalproducts/futoi/securities.json"
        for base_url, use_apim in [(apim_base_url, True), (iss_base_url, False)]:
            params = {"from": screen_from, "till": screen_till, "ticker": ticker}
            last_url = base.url_join(base_url, generic)
            try:
                frame = base.fetch_paged_frame(base_url, generic, params, "data", timeout, use_apim)
                if not frame.empty:
                    return frame, last_url, "completed", "", ticker
            except Exception as exc:
                last_error = exc.__class__.__name__ + ": " + str(exc)[:500]
    return pd.DataFrame(), last_url, "failed", last_error or "empty_response", tickers[0] if tickers else str(secid).lower()


def normalize_timestamp_column(frame, date_col, timestamp_col):
    if not timestamp_col:
        return pd.to_datetime(pd.Series([None] * len(frame)), errors="coerce")
    values = []
    dates = frame[date_col].map(base.parse_iso_date).tolist()
    raw_values = frame[timestamp_col].astype(str).str.strip().tolist()
    for d, value in zip(dates, raw_values):
        if not value or value.lower() == "nan":
            values.append(None)
        elif len(value) >= 10 and "-" in value[:10]:
            values.append(value)
        elif d:
            values.append(d + " " + value)
        else:
            values.append(None)
    return pd.to_datetime(pd.Series(values), errors="coerce")


def normalize_futoi(frame, secid, family_code, board, source_url, source_ticker, ingest_ts, short_history_flag, calendar_status):
    if frame.empty:
        return pd.DataFrame(), {"error": "empty_frame", "columns": []}
    date_col = base.canonical_column(frame, ["tradedate", "TRADEDATE", "date", "DATE"])
    time_col = base.canonical_column(frame, ["tradetime", "TRADETIME", "time", "TIME"])
    moment_col = base.canonical_column(frame, ["moment", "MOMENT", "ts", "TS", "datetime", "DATETIME"])
    systime_col = base.canonical_column(frame, ["systime", "SYSTIME"])
    ticker_col = base.canonical_column(frame, ["ticker", "TICKER"])
    clgroup_col = base.canonical_column(frame, ["clgroup", "CLGROUP"])
    pos_col = base.canonical_column(frame, ["pos", "POS"])
    pos_long_col = base.canonical_column(frame, ["pos_long", "POS_LONG"])
    pos_short_col = base.canonical_column(frame, ["pos_short", "POS_SHORT"])
    pos_long_num_col = base.canonical_column(frame, ["pos_long_num", "POS_LONG_NUM"])
    pos_short_num_col = base.canonical_column(frame, ["pos_short_num", "POS_SHORT_NUM"])
    sess_col = base.canonical_column(frame, ["sess_id", "SESS_ID"])
    seq_col = base.canonical_column(frame, ["seqnum", "SEQNUM"])
    if not date_col and not moment_col:
        return pd.DataFrame(), {"error": "missing_required_columns:tradedate_or_moment", "columns": [str(x) for x in frame.columns]}
    required = {"clgroup": clgroup_col, "pos": pos_col, "pos_long": pos_long_col, "pos_short": pos_short_col, "pos_long_num": pos_long_num_col, "pos_short_num": pos_short_num_col}
    missing = [k for k, v in required.items() if not v]
    if missing:
        return pd.DataFrame(), {"error": "missing_required_columns:" + ",".join(missing), "columns": [str(x) for x in frame.columns]}
    work = frame.copy()
    if ticker_col:
        ticker_values = work[ticker_col].astype(str).str.lower().str.strip()
        wanted = str(source_ticker or "").lower().strip()
        filtered = work.loc[ticker_values == wanted].copy()
        if not filtered.empty:
            work = filtered
    out = pd.DataFrame()
    if date_col:
        out["trade_date"] = work[date_col].map(base.parse_iso_date)
    else:
        out["trade_date"] = pd.to_datetime(work[moment_col], errors="coerce").dt.date.astype(str)
    if moment_col:
        out["moment"] = normalize_timestamp_column(work, date_col, moment_col) if date_col else pd.to_datetime(work[moment_col], errors="coerce")
    else:
        out["moment"] = combine_ts(work, date_col, time_col)
    out["ts"] = out["moment"]
    if systime_col:
        out["systime"] = normalize_timestamp_column(work, date_col, systime_col) if date_col else pd.to_datetime(work[systime_col], errors="coerce")
    else:
        out["systime"] = None
    out["board"] = board
    out["secid"] = secid
    out["family_code"] = family_code
    out["source_ticker"] = str(source_ticker or "").upper()
    out["source_scope"] = "exact_contract_futoi" if str(source_ticker or "").upper() == str(secid or "").upper() else "family_aggregate_futoi"
    out["clgroup"] = work[clgroup_col].astype(str).str.upper().str.strip()
    out["pos"] = base.coerce_numeric(work[pos_col])
    out["pos_long"] = base.coerce_numeric(work[pos_long_col])
    out["pos_short"] = base.coerce_numeric(work[pos_short_col])
    out["pos_long_num"] = base.coerce_numeric(work[pos_long_num_col])
    out["pos_short_num"] = base.coerce_numeric(work[pos_short_num_col])
    out["sess_id"] = base.coerce_numeric(work[sess_col]) if sess_col else None
    out["seqnum"] = base.coerce_numeric(work[seq_col]) if seq_col else None
    out["source"] = "MOEX_FUTOI"
    out["source_endpoint_url"] = source_url
    out["ingest_ts"] = ingest_ts
    out["schema_version"] = SCHEMA_FUTOI_RAW
    out["short_history_flag"] = bool(short_history_flag)
    out["calendar_denominator_status"] = calendar_status
    out = out.loc[out["trade_date"].notna() & out["ts"].notna() & out["clgroup"].notna()].copy()
    out = out.sort_values(["trade_date", "ts", "secid", "clgroup"]).reset_index(drop=True)
    meta = {"error": "", "columns": [str(x) for x in frame.columns], "mapped_columns": {"tradedate": str(date_col) if date_col else None, "tradetime": str(time_col) if time_col else None, "moment": str(moment_col) if moment_col else None, "systime": str(systime_col) if systime_col else None, "ticker": str(ticker_col) if ticker_col else None, "clgroup": str(clgroup_col), "pos": str(pos_col), "pos_long": str(pos_long_col), "pos_short": str(pos_short_col), "pos_long_num": str(pos_long_num_col), "pos_short_num": str(pos_short_num_col), "sess_id": str(sess_col) if sess_col else None, "seqnum": str(seq_col) if seq_col else None}}
    return out, meta


def filter_calendar_rows(frame, expected_calendar):
    if frame.empty or expected_calendar is None:
        return frame, {"source_off_calendar_date_count": 0, "source_off_calendar_dates": []}
    dates = set(frame["trade_date"].dropna().astype(str).tolist())
    off_dates = sorted([x for x in dates if x not in expected_calendar])
    if not off_dates:
        return frame, {"source_off_calendar_date_count": 0, "source_off_calendar_dates": []}
    filtered = frame.loc[frame["trade_date"].astype(str).isin(expected_calendar)].copy().reset_index(drop=True)
    return filtered, {"source_off_calendar_date_count": len(off_dates), "source_off_calendar_dates": off_dates}


def quality_counts(frame, expected_calendar):
    if frame.empty:
        return {"rows": 0, "trade_dates": 0, "min_ts": None, "max_ts": None, "clgroups": [], "duplicate_key_count": 0, "null_required_count": 0, "invalid_position_count": 0, "off_calendar_date_count": None, "missing_expected_trading_days": None}
    duplicates = int(frame.duplicated(subset=["trade_date", "ts", "secid", "clgroup"]).sum())
    required = ["clgroup", "pos", "pos_long", "pos_short", "pos_long_num", "pos_short_num"]
    null_required = int(frame[required].isna().any(axis=1).sum())
    invalid = (frame["pos_long"] < 0) | (frame["pos_short"] > 0) | (frame["pos_long_num"] < 0) | (frame["pos_short_num"] < 0)
    dates = set(frame["trade_date"].dropna().astype(str).tolist())
    off_calendar = None
    missing_expected = None
    if expected_calendar is not None and dates:
        off_calendar = len(dates - expected_calendar)
        expected = set([x for x in expected_calendar if min(dates) <= x <= max(dates)])
        missing_expected = len(expected - dates)
    clgroups = sorted([str(x) for x in frame["clgroup"].dropna().astype(str).unique().tolist()])
    return {"rows": int(len(frame)), "trade_dates": len(dates), "min_ts": str(frame["ts"].min()), "max_ts": str(frame["ts"].max()), "clgroups": clgroups, "duplicate_key_count": duplicates, "null_required_count": null_required, "invalid_position_count": int(invalid.fillna(True).sum()), "off_calendar_date_count": off_calendar, "missing_expected_trading_days": missing_expected}


def data_gap_status(counts):
    missing = counts.get("missing_expected_trading_days")
    if missing is None:
        return "not_computed"
    if int(missing or 0) == 0:
        return "no_calendar_gaps"
    return "calendar_gaps_detected:" + str(int(missing or 0))


def status_from_counts(counts, fetch_status, calendar_status, futoi_availability_status, futoi_probe_status):
    if fetch_status != "completed" or int(counts.get("rows") or 0) == 0:
        return "fail", "source fetch failed or produced zero rows"
    if futoi_availability_status != "available" or futoi_probe_status != "completed":
        return "fail", "FUTOI availability artifact is not completed/available"
    if calendar_status != "canonical_apim_futures_xml":
        return "fail", "calendar denominator is not canonical_apim_futures_xml"
    checks = [("duplicate_key_count", "duplicate primary-key rows detected before partition write"), ("null_required_count", "null required FUTOI values detected"), ("invalid_position_count", "invalid FUTOI position sign/count values detected"), ("off_calendar_date_count", "loaded trade dates outside APIM futures calendar")]
    for key, note in checks:
        if counts.get(key) is not None and int(counts.get(key) or 0) > 0:
            return "fail", note
    return "pass", "FUTOI raw partition load completed"


def write_partitions(frame, data_root, family_code, secid):
    paths = []
    clean = frame.drop_duplicates(subset=["trade_date", "ts", "secid", "clgroup"], keep="last").copy()
    for trade_date, part in clean.groupby("trade_date"):
        path = partition_path(data_root, str(trade_date), family_code, secid)
        path.parent.mkdir(parents=True, exist_ok=True)
        part.sort_values(["ts", "secid", "clgroup"]).to_parquet(path, index=False)
        paths.append(str(path))
    return paths


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
    run_id = "futures_futoi_5m_raw_loader_" + run_date + "_" + stable_id([snapshot_date, ingest_ts, ",".join(whitelist)])

    base.assert_files_exist(root, list(base.REQUIRED_CONTRACTS) + [FUTOI_AVAILABILITY_CONTRACT])
    contracts = load_contract_values_extended(root)
    input_paths, normalized, liquidity, history, futoi_availability = load_inputs(data_root, contracts, snapshot_date)
    instruments = select_instruments(normalized, liquidity, history, futoi_availability, whitelist, excluded)

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
    source_scope_values = {}
    for _, row in instruments.iterrows():
        secid = str(row.get("secid"))
        family_code = str(row.get("family_code"))
        board = str(row.get("board", "rfud") or "rfud")
        start = ranges[secid]["from"]
        end = ranges[secid]["till"]
        short_history_flag = bool(row.get("short_history_flag"))
        source_frame, source_url, fetch_status, fetch_error, source_ticker = fetch_futoi(secid, family_code, start, end, float(args.timeout), str(args.apim_base_url), str(args.iss_base_url))
        raw, meta = normalize_futoi(source_frame, secid, family_code, board, source_url, source_ticker, ingest_ts, short_history_flag, calendar_status)
        raw, calendar_filter = filter_calendar_rows(raw, expected_calendar)
        counts = quality_counts(raw, expected_calendar)
        gap_status = data_gap_status(counts)
        quality_status, notes = status_from_counts(counts, fetch_status, calendar_status, str(row.get("futoi_availability_status", "")), str(row.get("futoi_probe_status", "")))
        paths = write_partitions(raw, data_root, family_code, secid) if quality_status != "fail" else []
        partition_paths.extend(paths)
        source_scope = str(raw["source_scope"].dropna().iloc[0]) if not raw.empty and "source_scope" in raw.columns else ""
        source_scope_values[secid] = source_scope
        quality_rows.append({"quality_report_id": stable_id([run_id, secid]), "run_id": run_id, "run_date": run_date, "snapshot_date": snapshot_date, "board": board, "secid": secid, "family_code": family_code, "source_ticker": str(source_ticker or "").upper(), "source_scope": source_scope, "dataset_id": "futures_futoi_5m_raw", "schema_version": SCHEMA_QUALITY, "requested_from": start, "requested_till": end, "source_endpoint_url": source_url, "fetch_status": fetch_status, "fetch_error": fetch_error or None, "normalization_error": meta.get("error") or None, "rows": counts.get("rows"), "trade_dates": counts.get("trade_dates"), "min_ts": counts.get("min_ts"), "max_ts": counts.get("max_ts"), "clgroups_json": json.dumps(counts.get("clgroups") or [], ensure_ascii=False, sort_keys=True), "duplicate_key_count": counts.get("duplicate_key_count"), "null_required_count": counts.get("null_required_count"), "invalid_position_count": counts.get("invalid_position_count"), "off_calendar_date_count": counts.get("off_calendar_date_count"), "source_off_calendar_date_count": calendar_filter.get("source_off_calendar_date_count"), "source_off_calendar_dates_json": json.dumps(calendar_filter.get("source_off_calendar_dates") or [], ensure_ascii=False, sort_keys=True), "missing_expected_trading_days": counts.get("missing_expected_trading_days"), "partition_count": len(paths), "calendar_denominator_status": calendar_status, "futoi_availability_status": row.get("futoi_availability_status"), "futoi_probe_status": row.get("futoi_probe_status"), "history_depth_status": row.get("history_depth_status"), "liquidity_status": row.get("liquidity_status"), "short_history_flag": short_history_flag, "data_gap_status": gap_status, "quality_status": quality_status, "review_notes": notes, "mapped_columns_json": json.dumps(meta.get("mapped_columns") or {}, ensure_ascii=False, sort_keys=True), "observed_columns_json": json.dumps(meta.get("columns") or [], ensure_ascii=False, sort_keys=True)})
        summaries[secid] = {"requested_from": start, "requested_till": end, "source_ticker": str(source_ticker or "").upper(), "source_scope": source_scope, "rows": counts.get("rows"), "trade_dates": counts.get("trade_dates"), "partition_count": len(paths), "quality_status": quality_status, "data_gap_status": gap_status, "short_history_flag": short_history_flag, "source_off_calendar_date_count": calendar_filter.get("source_off_calendar_date_count"), "source_off_calendar_dates": calendar_filter.get("source_off_calendar_dates"), "review_notes": notes}

    quality = pd.DataFrame(quality_rows)
    Path(outputs["quality_report"]).parent.mkdir(parents=True, exist_ok=True)
    quality.to_parquet(outputs["quality_report"], index=False)
    quality_status_counts = {str(k): int(v) for k, v in quality["quality_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    manifest = {"schema_version": SCHEMA_MANIFEST, "run_id": run_id, "run_date": run_date, "snapshot_date": snapshot_date, "ingest_ts": ingest_ts, "loader_whitelist_applied": whitelist, "excluded_instruments_confirmed": excluded, "input_artifacts": input_paths, "output_artifacts": outputs, "partition_paths_created": partition_paths, "instrument_summaries": summaries, "quality_status_counts": quality_status_counts, "calendar_validation_summary": {"calendar_denominator_status": calendar_status, "calendar_from": calendar_from, "calendar_till": calendar_till, "expected_trading_days": len(expected_calendar)}, "futoi_source_scope_note": {"by_instrument": source_scope_values, "family_aggregate_futoi": "FUTOI source ticker may be family-level for expiring Si contracts; secid partition preserves accepted whitelist scope without treating FUTOI as OHLCV."}, "short_history_handling": {"SiU7": summaries.get("SiU7")}, "loader_result_verdict": "pass" if quality_status_counts.get("fail", 0) == 0 else "fail"}
    Path(outputs["manifest"]).parent.mkdir(parents=True, exist_ok=True)
    Path(outputs["manifest"]).write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")

    print_json_line("loader_whitelist_applied", whitelist)
    print_json_line("excluded_instruments_confirmed", excluded)
    print_json_line("output_artifacts_created", outputs)
    print_json_line("futoi_raw_quality_summary", {"quality_status_counts": quality_status_counts, "instruments": summaries})
    print_json_line("calendar_validation_summary", manifest["calendar_validation_summary"])
    print_json_line("futoi_source_scope_note", manifest["futoi_source_scope_note"])
    print_json_line("short_history_handling", manifest["short_history_handling"])
    print_json_line("loader_result_verdict", manifest["loader_result_verdict"])
    return 0 if manifest["loader_result_verdict"] == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
