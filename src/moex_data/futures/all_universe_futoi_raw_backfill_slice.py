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

from moex_data.futures import futoi_raw_loader as futoi
from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures import liquidity_history_metrics_probe_apim_calendar as apim_calendar

SCHEMA_MANIFEST = "futures_all_universe_futoi_raw_chunk_manifest.v1"
SCHEMA_QUALITY = "futures_all_universe_futoi_raw_quality_report.v1"
SCHEMA_FUTOI_ELIGIBILITY = "futures_all_universe_futoi_eligibility_snapshot.v1"
DATASET_STAGE = "futoi_raw"
MODE_RFUD_INCLUDED = "rfud_included_universe"
FUTOI_AVAILABILITY_CONTRACT = "contracts/datasets/futures_futoi_availability_report_contract.md"
REQUIRED_SOT_FILES = [
    "contracts/datasets/futures_all_universe_snapshot_contract.md",
    "contracts/datasets/futures_all_universe_eligibility_contract.md",
    "configs/datasets/futures_all_universe_eligibility_config.json",
    FUTOI_AVAILABILITY_CONTRACT,
]


def now_utc():
    return futoi.utc_now_iso()


def today_msk():
    return futoi.today_msk()


def parse_csv(value):
    return [x.strip() for x in str(value or "").split(",") if x.strip()]


def dump_json(path, data):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_parquet(path, frame):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(p, index=False)


def data_root(args):
    raw = str(args.data_root or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT or --data-root is required")
    return Path(raw).expanduser().resolve()


def paths(root, snapshot_date, chunk_id):
    base_dir = root / "futures" / "all_universe"
    return {
        "eligibility_snapshot": str(base_dir / "eligibility_snapshot" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"),
        "futoi_eligibility_snapshot": str(base_dir / "futoi_eligibility_snapshot" / ("snapshot_date=" + snapshot_date) / "futoi_eligibility_snapshot.parquet"),
        "chunk_manifest": str(base_dir / "runs" / "futoi_raw_backfill" / ("chunk_id=" + chunk_id) / "manifest.json"),
        "quality_report": str(base_dir / "quality" / "futoi_raw_backfill" / ("chunk_id=" + chunk_id) / "quality_report.parquet"),
        "aggregate_report": str(base_dir / "quality" / "futoi_raw_backfill" / ("chunk_id=" + chunk_id) / "aggregate_report.json"),
    }


def resolve_availability_path(repo_root, root, snapshot_date):
    contracts = futoi.load_contract_values_extended(repo_root)
    return futoi.resolve_path_from_contract(root, contracts, FUTOI_AVAILABILITY_CONTRACT, snapshot_date)


def load_eligibility(root, snapshot_date):
    p = Path(paths(root, snapshot_date, "probe")["eligibility_snapshot"])
    if not p.exists():
        raise FileNotFoundError("Missing required eligibility snapshot: " + str(p))
    frame = pd.read_parquet(p)
    required = ["secid", "family_code", "classification_status", "futoi_eligible"]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        raise RuntimeError("Eligibility snapshot missing required columns: " + ",".join(missing))
    return str(p), frame


def load_availability(repo_root, root, snapshot_date):
    p = resolve_availability_path(repo_root, root, snapshot_date)
    if not p.exists():
        raise FileNotFoundError("Missing canonical FUTOI availability report: " + str(p))
    frame = pd.read_parquet(p)
    required = ["secid", "availability_status", "probe_status"]
    missing = [x for x in required if x not in frame.columns]
    if missing:
        raise RuntimeError("FUTOI availability report missing required columns: " + ",".join(missing))
    return str(p), frame


def latest_by_secid(frame):
    work = frame.copy()
    work["_secid_upper"] = work["secid"].astype(str).str.upper()
    return {str(row.get("_secid_upper")): row for _, row in work.drop_duplicates("_secid_upper", keep="last").iterrows()}


def derive_futoi_eligibility(eligibility, availability):
    by_secid = latest_by_secid(availability)
    work = eligibility.copy()
    failures = []
    futoi_status = []
    futoi_flags = []
    availability_statuses = []
    probe_statuses = []
    first_dates = []
    last_dates = []
    source_urls = []
    for _, row in work.iterrows():
        secid = str(row.get("secid"))
        status = str(row.get("classification_status", ""))
        if status != "included":
            futoi_status.append("not_applicable_not_included")
            futoi_flags.append(False)
            availability_statuses.append("")
            probe_statuses.append("")
            first_dates.append(None)
            last_dates.append(None)
            source_urls.append(None)
            continue
        arow = by_secid.get(secid.upper())
        if arow is None:
            failures.append(secid + ":missing_futoi_availability_row")
            futoi_status.append("fail_missing_futoi_availability_row")
            futoi_flags.append(False)
            availability_statuses.append("")
            probe_statuses.append("")
            first_dates.append(None)
            last_dates.append(None)
            source_urls.append(None)
            continue
        availability_status = str(arow.get("availability_status", "")).strip()
        probe_status = str(arow.get("probe_status", "")).strip()
        availability_statuses.append(availability_status)
        probe_statuses.append(probe_status)
        first_dates.append(arow.get("first_available_date"))
        last_dates.append(arow.get("last_available_date"))
        source_urls.append(arow.get("source_endpoint_url"))
        if availability_status != "available" or probe_status != "completed":
            failures.append(secid + ":futoi_availability_not_available_completed")
            futoi_status.append("fail_futoi_availability_not_available_completed")
            futoi_flags.append(False)
            continue
        futoi_status.append("pass")
        futoi_flags.append(True)
    work["futoi_check_status"] = futoi_status
    work["futoi_eligible"] = futoi_flags
    work["futoi_availability_status"] = availability_statuses
    work["futoi_probe_status"] = probe_statuses
    work["futoi_first_available_date"] = first_dates
    work["futoi_last_available_date"] = last_dates
    work["futoi_source_endpoint_url_probe"] = source_urls
    work["futoi_eligibility_schema_version"] = SCHEMA_FUTOI_ELIGIBILITY
    if failures:
        raise RuntimeError("Canonical FUTOI availability validation failed: " + ";".join(failures))
    return work


def selected_universe(futoi_eligibility):
    selected = futoi_eligibility.loc[(futoi_eligibility["classification_status"].astype(str) == "included") & (futoi_eligibility["futoi_eligible"] == True)].copy()
    if selected.empty:
        raise RuntimeError("No eligibility_snapshot rows with classification_status=included and futoi_eligible=true")
    if "registry_snapshot_id" not in selected.columns or selected["registry_snapshot_id"].isna().all():
        raise RuntimeError("Selected FUTOI universe lacks registry_snapshot_id")
    return selected.sort_values(["family_code", "secid"]).reset_index(drop=True)


def apply_scope_filters(selected, secid_filter, family_filter):
    out = selected.copy()
    secids = {x.upper() for x in secid_filter}
    families = {x.upper() for x in family_filter}
    if secids:
        out = out.loc[out["secid"].astype(str).str.upper().isin(secids)].copy()
    if families:
        out = out.loc[out["family_code"].astype(str).str.upper().isin(families)].copy()
    if out.empty:
        raise RuntimeError("FUTOI scope filters produced empty selected universe")
    return out.sort_values(["family_code", "secid"]).reset_index(drop=True)


def selected_dates(row):
    raw = str(row.get("selected_trading_dates_json", "") or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return [str(x) for x in data if str(x)]


def date_bounds(row, from_override, till_override):
    if from_override and till_override:
        return str(from_override), str(till_override)
    dates = selected_dates(row)
    if dates:
        return min(dates), max(dates)
    start = str(row.get("futoi_first_available_date", "") or "").strip()
    end = str(row.get("futoi_last_available_date", "") or "").strip()
    if from_override:
        start = str(from_override)
    if till_override:
        end = str(till_override)
    if not start or not end:
        raise RuntimeError("Cannot resolve FUTOI date range for " + str(row.get("secid")))
    return start, end


def fetch_futoi_exact_contract(secid, date_from, date_till, timeout, apim_base_url, iss_base_url):
    ticker = str(secid or "").strip().lower()
    if not ticker:
        raise RuntimeError("Exact FUTOI fetch requires secid")
    path = "/iss/analyticalproducts/futoi/securities/" + ticker + ".json"
    last_url = ""
    last_error = ""
    for base_url, use_apim in [(apim_base_url, True), (iss_base_url, False)]:
        params = {"from": date_from, "till": date_till}
        last_url = base.url_join(base_url, path)
        try:
            frame = base.fetch_paged_frame(base_url, path, params, "data", timeout, use_apim)
            if not frame.empty:
                return frame, last_url, "completed", "", ticker
        except Exception as exc:
            last_error = exc.__class__.__name__ + ": " + str(exc)[:500]
    return pd.DataFrame(), last_url, "failed", last_error or "empty_response", ticker


def quality_row(run_id, chunk_id, erow, date_from, date_till, raw, fetch_status, failure, partitions, calendar_status):
    counts = futoi.quality_counts(raw, None)
    status = "pass" if not failure and int(counts.get("rows") or 0) > 0 else "fail"
    return {
        "run_id": run_id,
        "chunk_id": chunk_id,
        "eligibility_snapshot_id": str(erow.get("eligibility_snapshot_id", "")),
        "registry_snapshot_id": str(erow.get("registry_snapshot_id", "")),
        "dataset_stage": DATASET_STAGE,
        "family_code": str(erow.get("family_code")),
        "secid": str(erow.get("secid")),
        "date_from": date_from,
        "date_till": date_till,
        "rows_written": int(counts.get("rows") or 0) if status == "pass" else 0,
        "trade_dates": counts.get("trade_dates"),
        "min_ts": counts.get("min_ts"),
        "max_ts": counts.get("max_ts"),
        "duplicate_key_count": counts.get("duplicate_key_count"),
        "null_required_count": counts.get("null_required_count"),
        "invalid_position_count": counts.get("invalid_position_count"),
        "calendar_status": calendar_status,
        "source_payload_status": fetch_status,
        "partition_status": "written" if status == "pass" else "not_written",
        "quality_status": status,
        "failure_reason": failure,
        "futoi_availability_status": str(erow.get("futoi_availability_status", "")),
        "futoi_probe_status": str(erow.get("futoi_probe_status", "")),
        "selection_model": "eligibility_snapshot_driven_futoi_eligible_true",
        "output_partitions_json": json.dumps(partitions, sort_keys=True),
        "schema_version": SCHEMA_QUALITY,
    }


def run_instrument(args, root, row, run_id, chunk_id, expected_calendar, calendar_status):
    secid = str(row.get("secid"))
    family_code = str(row.get("family_code"))
    board = str(row.get("board", "RFUD") or "RFUD")
    date_from, date_till = date_bounds(row, str(args.from_date or ""), str(args.till or ""))
    if bool(args.exact_contract_only):
        source_frame, source_url, fetch_status, fetch_error, source_ticker = fetch_futoi_exact_contract(secid, date_from, date_till, float(args.timeout), str(args.apim_base_url), str(args.iss_base_url))
    else:
        source_frame, source_url, fetch_status, fetch_error, source_ticker = futoi.fetch_futoi(secid, family_code, date_from, date_till, float(args.timeout), str(args.apim_base_url), str(args.iss_base_url))
    raw = pd.DataFrame()
    failure = ""
    partitions = []
    try:
        raw, meta = futoi.normalize_futoi(source_frame, secid, family_code, board, source_url, source_ticker, now_utc(), False, calendar_status)
        raw, calendar_filter = futoi.filter_calendar_rows(raw, expected_calendar)
        counts = futoi.quality_counts(raw, expected_calendar)
        qstatus, notes = futoi.status_from_counts(counts, fetch_status, calendar_status, str(row.get("futoi_availability_status", "")), str(row.get("futoi_probe_status", "")))
        if qstatus == "fail":
            failure = notes or fetch_error or "futoi_raw_quality_failed"
        else:
            partitions = futoi.write_partitions(raw, root, family_code, secid)
    except Exception as exc:
        failure = exc.__class__.__name__ + ": " + str(exc)
    return quality_row(run_id, chunk_id, row, date_from, date_till, raw, fetch_status, failure, partitions, calendar_status), partitions, failure


def run_chunk(args, root, selected, run_id, chunk_id):
    starts = []
    ends = []
    for _, row in selected.iterrows():
        start, end = date_bounds(row, str(args.from_date or ""), str(args.till or ""))
        starts.append(start)
        ends.append(end)
    calendar_from = min(starts)
    calendar_till = max(ends)
    expected_calendar, calendar_status = apim_calendar.fetch_futures_calendar(calendar_from, calendar_till, float(args.timeout), str(args.iss_base_url))
    if expected_calendar is None or calendar_status != "canonical_apim_futures_xml":
        raise RuntimeError("APIM futures calendar validation failed: " + str(calendar_status))
    quality_rows = []
    partitions = []
    failed = []
    for _, row in selected.iterrows():
        q, row_partitions, failure = run_instrument(args, root, row, run_id, chunk_id, expected_calendar, calendar_status)
        quality_rows.append(q)
        partitions.extend(row_partitions)
        if failure:
            failed.append(str(row.get("secid")))
    quality = pd.DataFrame(quality_rows)
    status = "succeeded" if not failed else ("partial_failed" if len(failed) < len(selected) else "failed")
    manifest = {
        "schema_version": SCHEMA_MANIFEST,
        "chunk_id": chunk_id,
        "dataset_stage": DATASET_STAGE,
        "selection_model": "eligibility_snapshot_driven_futoi_eligible_true",
        "secid_list": selected["secid"].astype(str).tolist(),
        "family_count": int(selected["family_code"].nunique()),
        "date_from": calendar_from,
        "date_till": calendar_till,
        "status": status,
        "started_at": run_id,
        "finished_at": now_utc(),
        "failed_secid": failed,
        "output_partitions": partitions,
        "quality_summary": {str(k): int(v) for k, v in quality["quality_status"].astype(str).value_counts(dropna=False).to_dict().items()} if not quality.empty else {},
        "calendar_validation_summary": {"calendar_denominator_status": calendar_status, "calendar_from": calendar_from, "calendar_till": calendar_till, "expected_trading_days": len(expected_calendar)},
        "no_futoi_prejoin_into_ohlcv": True,
        "exact_contract_only": bool(args.exact_contract_only),
    }
    return manifest, quality


def aggregate(eligibility, futoi_eligibility, selected, manifest):
    return {
        "candidate_universe_count": int(len(eligibility)),
        "included_count": int((eligibility["classification_status"].astype(str) == "included").sum()),
        "deferred_count": int((eligibility["classification_status"].astype(str) == "deferred").sum()),
        "excluded_count": int((eligibility["classification_status"].astype(str) == "excluded").sum()),
        "futoi_eligible_count": int((futoi_eligibility["futoi_eligible"] == True).sum()),
        "selected_futoi_secid_count": int(len(selected)),
        "failed_secid_count": int(len(manifest.get("failed_secid") or [])),
        "chunk_status": manifest.get("status"),
        "classification_visibility_preserved": True,
    }


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--data-root", default="")
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--selection-mode", choices=[MODE_RFUD_INCLUDED], default=MODE_RFUD_INCLUDED)
    parser.add_argument("--family", default="")
    parser.add_argument("--secid", default="")
    parser.add_argument("--exact-contract-only", action="store_true")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()
    repo_root = Path.cwd().resolve()
    root = data_root(args)
    base.assert_files_exist(repo_root, REQUIRED_SOT_FILES)
    secid_filter = parse_csv(args.secid)
    family_filter = parse_csv(args.family)
    eligibility_path, eligibility = load_eligibility(root, args.snapshot_date)
    availability_path, availability = load_availability(repo_root, root, args.snapshot_date)
    futoi_eligibility = derive_futoi_eligibility(eligibility, availability)
    selected = selected_universe(futoi_eligibility)
    selected = apply_scope_filters(selected, secid_filter, family_filter)
    run_id = "all_universe_futoi_raw_" + args.run_date + "_" + base.stable_id([args.snapshot_date, eligibility_path, availability_path, now_utc(), ",".join(secid_filter), ",".join(family_filter), bool(args.exact_contract_only)])
    chunk_id = "futoi_raw_" + base.stable_id([args.snapshot_date, ",".join(selected["secid"].astype(str).tolist()), args.from_date, args.till, bool(args.exact_contract_only)])
    out = paths(root, args.snapshot_date, chunk_id)
    write_parquet(out["futoi_eligibility_snapshot"], futoi_eligibility)
    manifest, quality = run_chunk(args, root, selected, run_id, chunk_id)
    manifest["input_artifacts"] = {"eligibility_snapshot": eligibility_path, "futoi_availability_report": availability_path}
    manifest["output_artifacts"] = out
    manifest["scope_filters"] = {"secid": secid_filter, "family": family_filter}
    write_parquet(out["quality_report"], quality)
    dump_json(out["chunk_manifest"], manifest)
    aggregate_report = aggregate(eligibility, futoi_eligibility, selected, manifest)
    dump_json(out["aggregate_report"], aggregate_report)
    print(json.dumps({"outputs": out, "selection_mode": args.selection_mode, "scope_filters": {"secid": secid_filter, "family": family_filter}, "exact_contract_only": bool(args.exact_contract_only), "selected_universe": {"secid_count": int(len(selected)), "secids": selected["secid"].astype(str).tolist(), "dataset_stage": DATASET_STAGE}, "chunk_status": aggregate_report.get("chunk_status"), "aggregate_report": aggregate_report}, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if aggregate_report.get("chunk_status") in ["succeeded", "partial_failed"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
