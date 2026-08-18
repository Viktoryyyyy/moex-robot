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

from moex_data.futures import futoi_raw_loader as base_loader
from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures import liquidity_history_metrics_probe_apim_calendar as apim_calendar
from moex_data.futures.slice1_common import DEFAULT_EXCLUDED
from moex_data.futures.slice1_common import DEFAULT_WHITELIST
from moex_data.futures.slice1_common import parse_list
from moex_data.futures.slice1_common import stable_id
from moex_data.futures.slice1_common import today_msk
from moex_data.futures.slice1_common import utc_now_iso

DATASET_STAGE = "futoi_raw"
MODE_WHITELIST = "slice1_whitelist_compat"
MODE_RFUD = "rfud_included_universe"
SCHEMA_MANIFEST = "futures_futoi_5m_raw_loader_manifest.v1"
SCHEMA_QUALITY = "futures_futoi_5m_raw_quality_report.v1"
CONFIG_PATH = "configs/datasets/futures_all_universe_eligibility_config.json"
REQUIRED_SOT_FILES = [
    "contracts/datasets/futures_all_universe_eligibility_contract.md",
    "contracts/datasets/futures_futoi_5m_raw_loader_manifest_contract.md",
    "contracts/datasets/futures_futoi_5m_raw_quality_report_contract.md",
    CONFIG_PATH,
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


def resolve_data_root(args):
    raw = str(args.data_root or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT or --data-root is required")
    return Path(raw).expanduser().resolve()


def eligibility_snapshot_path(data_root, snapshot_date, explicit_path):
    if explicit_path:
        return Path(explicit_path).expanduser().resolve()
    return data_root / "futures" / "all_universe" / "eligibility_snapshot" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"


def output_paths(data_root, run_date, chunk_id):
    return {
        "futoi_raw_partition_root": str(data_root / "futures" / "futoi_raw"),
        "quality_report": str(data_root / "futures" / "quality" / "futoi_raw_backfill" / ("chunk_id=" + chunk_id) / "quality_report.parquet"),
        "manifest": str(data_root / "futures" / "runs" / "futoi_raw_backfill" / ("chunk_id=" + chunk_id) / "manifest.json"),
    }


def selection_mode(config, explicit_mode):
    if explicit_mode:
        mode = str(explicit_mode).strip()
    else:
        policy = config.get("futoi_probe_policy") or {}
        mode = MODE_RFUD if str(policy.get("selected_flag", "")) == "futoi_eligible" else MODE_WHITELIST
    if mode not in [MODE_WHITELIST, MODE_RFUD]:
        raise RuntimeError("Unsupported FUTOI raw selection mode: " + mode)
    return mode


def required_cols(frame, cols, name):
    missing = [x for x in cols if x not in frame.columns]
    if missing:
        raise RuntimeError(name + " missing columns: " + ", ".join(missing))


def load_eligibility(data_root, snapshot_date, explicit_path):
    path = eligibility_snapshot_path(data_root, snapshot_date, explicit_path)
    if not path.exists():
        raise FileNotFoundError("Missing eligibility snapshot artifact: " + str(path))
    frame = pd.read_parquet(path)
    required_cols(frame, ["eligibility_snapshot_id", "registry_snapshot_id", "board", "secid", "family_code", "classification_status", "futoi_eligible"], "eligibility_snapshot")
    return str(path), frame


def selected_from_eligibility(eligibility, target_board):
    board = eligibility["board"].astype(str).str.upper()
    selected = eligibility.loc[(board == str(target_board).upper()) & (eligibility["classification_status"].astype(str) == "included") & (eligibility["futoi_eligible"] == True)].copy()
    return selected.sort_values(["family_code", "secid"]).reset_index(drop=True)


def visibility_rows_from_eligibility(eligibility, target_board):
    board = eligibility["board"].astype(str).str.upper()
    scope = eligibility.loc[board == str(target_board).upper()].copy()
    rows = []
    for _, row in scope.sort_values(["family_code", "secid"]).iterrows():
        selected = str(row.get("classification_status")) == "included" and row.get("futoi_eligible") == True
        reason = str(row.get("classification_reason") or row.get("deferral_reason") or row.get("exclusion_reason") or "")
        if selected:
            status = "selected"
            explicit_status = "available_for_futoi_backfill"
        elif str(row.get("classification_status")) == "included":
            status = "deferred"
            explicit_status = str(row.get("futoi_check_status") or "futoi_unavailable_or_unresolved")
        else:
            status = str(row.get("classification_status"))
            explicit_status = reason or "not_selected"
        rows.append({"secid": str(row.get("secid")), "family_code": str(row.get("family_code")), "board": str(row.get("board")), "classification_status": str(row.get("classification_status")), "futoi_eligible": bool(row.get("futoi_eligible")), "backfill_selection_status": status, "backfill_selection_reason": explicit_status, "eligibility_snapshot_id": str(row.get("eligibility_snapshot_id")), "registry_snapshot_id": str(row.get("registry_snapshot_id"))})
    return rows


def chunk_groups(selected):
    return [(str(family_code), frame.sort_values("secid").reset_index(drop=True)) for family_code, frame in selected.groupby("family_code", sort=True)]


def dates_for_row(row, from_override, till_override):
    if from_override and till_override:
        return base.parse_iso_date(from_override), base.parse_iso_date(till_override)
    raw = str(row.get("selected_trading_dates_json") or "").strip()
    dates = []
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                dates = [str(x) for x in parsed if str(x)]
        except Exception:
            dates = []
    if dates:
        return min(dates), max(dates)
    raise RuntimeError("FUTOI eligibility-selected row requires --from/--till or selected_trading_dates_json: " + str(row.get("secid")))


def quality_row(run_id, chunk_id, row, run_date, snapshot_date, date_from, date_till, source_url, fetch_status, fetch_error, meta, counts, calendar_status, quality_status, notes, paths, source_ticker, source_scope, short_history_flag):
    return {"quality_report_id": stable_id([run_id, chunk_id, row.get("secid")]), "run_id": run_id, "run_date": run_date, "snapshot_date": snapshot_date, "board": str(row.get("board", "RFUD") or "RFUD"), "secid": str(row.get("secid")), "family_code": str(row.get("family_code")), "source_ticker": str(source_ticker or "").upper(), "source_scope": source_scope, "dataset_id": "futures_futoi_5m_raw", "dataset_stage": DATASET_STAGE, "schema_version": SCHEMA_QUALITY, "requested_from": date_from, "requested_till": date_till, "source_endpoint_url": source_url, "fetch_status": fetch_status, "fetch_error": fetch_error or None, "normalization_error": meta.get("error") or None, "rows": counts.get("rows"), "trade_dates": counts.get("trade_dates"), "min_ts": counts.get("min_ts"), "max_ts": counts.get("max_ts"), "clgroups_json": json.dumps(counts.get("clgroups") or [], ensure_ascii=False, sort_keys=True), "duplicate_key_count": counts.get("duplicate_key_count"), "null_required_count": counts.get("null_required_count"), "invalid_position_count": counts.get("invalid_position_count"), "off_calendar_date_count": counts.get("off_calendar_date_count"), "missing_expected_trading_days": counts.get("missing_expected_trading_days"), "partition_count": len(paths), "calendar_denominator_status": calendar_status, "futoi_availability_status": "selected_by_eligibility_snapshot" if bool(row.get("futoi_eligible")) else "not_selected_by_eligibility_snapshot", "futoi_probe_status": str(row.get("futoi_check_status") or "eligibility_snapshot_flag"), "history_depth_status": str(row.get("history_depth_check_status") or "not_required_for_futoi_raw"), "liquidity_status": str(row.get("liquidity_check_status") or "not_required_for_futoi_raw"), "short_history_flag": bool(short_history_flag), "data_gap_status": base_loader.data_gap_status(counts), "quality_status": quality_status, "failure_reason": "" if quality_status == "pass" else notes, "deferred_reason": "", "review_notes": notes, "eligibility_snapshot_id": str(row.get("eligibility_snapshot_id")), "registry_snapshot_id": str(row.get("registry_snapshot_id")), "backfill_selection_status": "selected", "backfill_selection_reason": "futoi_eligible_true", "mapped_columns_json": json.dumps(meta.get("mapped_columns") or {}, ensure_ascii=False, sort_keys=True), "observed_columns_json": json.dumps(meta.get("columns") or [], ensure_ascii=False, sort_keys=True), "output_partitions_json": json.dumps(paths, ensure_ascii=False, sort_keys=True)}


def deferred_quality_row(run_id, chunk_id, run_date, snapshot_date, visibility):
    return {"quality_report_id": stable_id([run_id, chunk_id, visibility.get("secid"), "not_selected"]), "run_id": run_id, "run_date": run_date, "snapshot_date": snapshot_date, "board": visibility.get("board"), "secid": visibility.get("secid"), "family_code": visibility.get("family_code"), "source_ticker": "", "source_scope": "", "dataset_id": "futures_futoi_5m_raw", "dataset_stage": DATASET_STAGE, "schema_version": SCHEMA_QUALITY, "requested_from": None, "requested_till": None, "source_endpoint_url": None, "fetch_status": "not_attempted", "fetch_error": None, "normalization_error": None, "rows": 0, "trade_dates": 0, "min_ts": None, "max_ts": None, "clgroups_json": "[]", "duplicate_key_count": 0, "null_required_count": 0, "invalid_position_count": 0, "off_calendar_date_count": None, "missing_expected_trading_days": None, "partition_count": 0, "calendar_denominator_status": "not_attempted", "futoi_availability_status": "not_selected_by_eligibility_snapshot", "futoi_probe_status": visibility.get("backfill_selection_reason"), "history_depth_status": "not_required_for_futoi_raw", "liquidity_status": "not_required_for_futoi_raw", "short_history_flag": False, "data_gap_status": "not_computed", "quality_status": visibility.get("backfill_selection_status"), "failure_reason": "", "deferred_reason": visibility.get("backfill_selection_reason"), "review_notes": "FUTOI raw not loaded because instrument is not futoi_eligible=true in eligibility snapshot", "eligibility_snapshot_id": visibility.get("eligibility_snapshot_id"), "registry_snapshot_id": visibility.get("registry_snapshot_id"), "backfill_selection_status": visibility.get("backfill_selection_status"), "backfill_selection_reason": visibility.get("backfill_selection_reason"), "mapped_columns_json": "{}", "observed_columns_json": "[]", "output_partitions_json": "[]"}


def run_family_chunk(args, data_root, selected, visibility_rows, expected_calendar, calendar_status, run_id, snapshot_date, run_date, chunk_id):
    outputs = output_paths(data_root, run_date, chunk_id)
    partition_paths = []
    quality_rows = []
    failed = []
    summaries = {}
    source_scope_values = {}
    for _, row in selected.iterrows():
        secid = str(row.get("secid"))
        family_code = str(row.get("family_code"))
        board = str(row.get("board", "RFUD") or "RFUD")
        date_from, date_till = dates_for_row(row, str(args.from_date or ""), str(args.till or ""))
        source_frame, source_url, fetch_status, fetch_error, source_ticker = base_loader.fetch_futoi(secid, family_code, date_from, date_till, float(args.timeout), str(args.apim_base_url))
        raw, meta = base_loader.normalize_futoi(source_frame, secid, family_code, board, source_url, source_ticker, utc_now_iso(), False, calendar_status)
        raw, calendar_filter = base_loader.filter_calendar_rows(raw, expected_calendar)
        counts = base_loader.quality_counts(raw, expected_calendar)
        quality_status, notes = base_loader.status_from_counts(counts, fetch_status, calendar_status, "available", "completed")
        paths = base_loader.write_partitions(raw, data_root, family_code, secid) if quality_status == "pass" else []
        partition_paths.extend(paths)
        if quality_status != "pass":
            failed.append(secid)
        source_scope = str(raw["source_scope"].dropna().iloc[0]) if not raw.empty and "source_scope" in raw.columns else ""
        source_scope_values[secid] = source_scope
        quality_rows.append(quality_row(run_id, chunk_id, row, run_date, snapshot_date, date_from, date_till, source_url, fetch_status, fetch_error, meta, counts, calendar_status, quality_status, notes, paths, source_ticker, source_scope, False))
        summaries[secid] = {"requested_from": date_from, "requested_till": date_till, "source_ticker": str(source_ticker or "").upper(), "source_scope": source_scope, "rows": counts.get("rows"), "trade_dates": counts.get("trade_dates"), "partition_count": len(paths), "quality_status": quality_status, "data_gap_status": base_loader.data_gap_status(counts), "review_notes": notes, "source_off_calendar_date_count": calendar_filter.get("source_off_calendar_date_count"), "source_off_calendar_dates": calendar_filter.get("source_off_calendar_dates")}
    selected_ids = set(selected["secid"].astype(str).tolist())
    for item in visibility_rows:
        if item.get("secid") not in selected_ids:
            quality_rows.append(deferred_quality_row(run_id, chunk_id, run_date, snapshot_date, item))
    quality = pd.DataFrame(quality_rows)
    write_parquet(outputs["quality_report"], quality)
    quality_status_counts = {str(k): int(v) for k, v in quality["quality_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    status = "succeeded" if not failed else ("partial_failed" if len(failed) < len(selected_ids) else "failed")
    manifest = {"schema_version": SCHEMA_MANIFEST, "run_id": run_id, "chunk_id": chunk_id, "run_date": run_date, "snapshot_date": snapshot_date, "ingest_ts": utc_now_iso(), "dataset_stage": DATASET_STAGE, "selection_mode": MODE_RFUD, "eligibility_snapshot_id": str(selected["eligibility_snapshot_id"].iloc[0]) if not selected.empty else "", "registry_snapshot_id": str(selected["registry_snapshot_id"].iloc[0]) if not selected.empty else "", "family_code": str(selected["family_code"].iloc[0]) if not selected.empty else "", "secid_list": sorted(selected_ids), "failed_secid": failed, "deferred_or_excluded_visible": [x for x in visibility_rows if x.get("secid") not in selected_ids], "input_artifacts": {"eligibility_snapshot": str(args.eligibility_snapshot_path or eligibility_snapshot_path(data_root, snapshot_date, ""))}, "output_artifacts": outputs, "partition_paths_created": partition_paths, "instrument_summaries": summaries, "quality_status_counts": quality_status_counts, "calendar_validation_summary": {"calendar_denominator_status": calendar_status, "expected_trading_days": len(expected_calendar) if expected_calendar is not None else 0}, "futoi_source_scope_note": {"by_instrument": source_scope_values, "no_prejoin_with_raw_5m": True}, "short_history_handling": {}, "loader_whitelist_applied": [], "excluded_instruments_confirmed": DEFAULT_EXCLUDED, "backfill_selection_status": "eligibility_snapshot_driven", "backfill_selection_reason": "board=RFUD classification_status=included futoi_eligible=true dataset_stage=futoi_raw", "chunk_dimensions": ["family_code", "date_range", "dataset_stage"], "retry_child_chunk_dimensions": ["secid", "date_range", "dataset_stage"], "status": status, "loader_result_verdict": "pass" if status in ["succeeded", "partial_failed"] else "fail"}
    dump_json(outputs["manifest"], manifest)
    return manifest, quality, outputs


def run_whitelist_compat(args):
    forwarded = ["--snapshot-date", str(args.snapshot_date), "--run-date", str(args.run_date), "--data-root", str(args.data_root or ""), "--apim-base-url", str(args.apim_base_url), "--timeout", str(args.timeout), "--whitelist", str(args.whitelist), "--excluded", str(args.excluded)]
    if args.from_date:
        forwarded.extend(["--from", str(args.from_date)])
    if args.till:
        forwarded.extend(["--till", str(args.till)])
    old_argv = sys.argv
    try:
        sys.argv = [old_argv[0]] + forwarded
        return base_loader.main()
    finally:
        sys.argv = old_argv


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--from", dest="from_date", default="")
    parser.add_argument("--till", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--eligibility-snapshot-path", default="")
    parser.add_argument("--selection-mode", choices=[MODE_WHITELIST, MODE_RFUD], default="")
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--whitelist", default=",".join(DEFAULT_WHITELIST))
    parser.add_argument("--excluded", default=",".join(DEFAULT_EXCLUDED))
    args = parser.parse_args()
    repo_root = Path.cwd().resolve()
    data_root = resolve_data_root(args)
    base.assert_files_exist(repo_root, REQUIRED_SOT_FILES)
    config = load_json(repo_root / args.config)
    mode = selection_mode(config, args.selection_mode)
    if mode == MODE_WHITELIST:
        return run_whitelist_compat(args)
    snapshot_date = str(args.snapshot_date).strip()
    run_date = str(args.run_date).strip()
    eligibility_path, eligibility = load_eligibility(data_root, snapshot_date, str(args.eligibility_snapshot_path or ""))
    target = ((config.get("l3_4_futoi_raw_included_universe") or {}).get("board") or "RFUD")
    selected = selected_from_eligibility(eligibility, target)
    visibility = visibility_rows_from_eligibility(eligibility, target)
    if selected.empty:
        raise RuntimeError("No eligibility-selected FUTOI instruments: board=RFUD classification_status=included futoi_eligible=true")
    starts = []
    ends = []
    for _, row in selected.iterrows():
        date_from, date_till = dates_for_row(row, str(args.from_date or ""), str(args.till or ""))
        starts.append(date_from)
        ends.append(date_till)
    calendar_from = min(starts)
    calendar_till = max(ends)
    expected_calendar, calendar_status = apim_calendar.fetch_futures_calendar(calendar_from, calendar_till, float(args.timeout), "")
    if expected_calendar is None or calendar_status != "canonical_apim_futures_xml":
        raise RuntimeError("APIM futures calendar validation failed: " + str(calendar_status))
    run_id = "futures_futoi_raw_backfill_" + run_date + "_" + stable_id([snapshot_date, eligibility_path, utc_now_iso(), DATASET_STAGE])
    manifests = []
    outputs = []
    for family_code, frame in chunk_groups(selected):
        chunk_visibility = [x for x in visibility if x.get("family_code") == family_code]
        chunk_id = "futoi_raw_" + stable_id([snapshot_date, family_code, calendar_from, calendar_till, ",".join(frame["secid"].astype(str).tolist()), DATASET_STAGE])
        manifest, _quality, out = run_family_chunk(args, data_root, frame, chunk_visibility, expected_calendar, calendar_status, run_id, snapshot_date, run_date, chunk_id)
        manifests.append(manifest)
        outputs.append(out)
    aggregate_status = "partial_failed" if any(x.get("status") in ["partial_failed", "failed"] for x in manifests) else "succeeded"
    print(json.dumps({"selection_mode": MODE_RFUD, "dataset_stage": DATASET_STAGE, "eligibility_snapshot": eligibility_path, "selected_universe": {"family_count": len(chunk_groups(selected)), "secid_count": int(len(selected)), "secids": selected["secid"].astype(str).tolist()}, "chunk_status": aggregate_status, "chunk_manifests": [x.get("output_artifacts", {}).get("manifest") for x in manifests], "quality_reports": [x.get("output_artifacts", {}).get("quality_report") for x in manifests], "forbidden_scope_checks": {"raw_5m_prejoin": False, "raw_d1_derivation": False, "continuous_build": False, "w1_build": False}}, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if aggregate_status in ["succeeded", "partial_failed"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
