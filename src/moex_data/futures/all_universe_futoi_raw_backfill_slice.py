#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import date
from numbers import Real
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd

from moex_data.futures import futoi_raw_loader as futoi
from moex_data.futures import liquidity_history_metrics_probe as base

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
    # Ambiguous evidence must not be resolved by row order or case folding.
    secids = frame["secid"]
    if (secids.isna().any()
            or not secids.map(lambda value: isinstance(value, str) and bool(value) and value == value.strip()).all()
            or secids.str.upper().duplicated().any()):
        raise RuntimeError("Canonical FUTOI availability validation failed: ambiguous_secid")
    return {row["secid"]: row for _, row in frame.iterrows()}


def validate_admission_inputs(eligibility, availability, snapshot_date=None):
    required = {
        "eligibility": ["secid", "board", "family_code", "registry_snapshot_date",
                        "eligibility_snapshot_date", "registry_snapshot_id", "classification_status"],
        "availability": ["secid", "board", "family_code", "snapshot_date",
                         "availability_status", "probe_status", "observed_rows"],
    }
    for name, frame in (("eligibility", eligibility), ("availability", availability)):
        if (not frame.columns.is_unique
                or any(column not in frame.columns for column in required[name])):
            raise RuntimeError("Canonical FUTOI availability validation failed: invalid_" + name + "_columns")
        latest_by_secid(frame)
    if eligibility.empty:
        raise RuntimeError("Canonical FUTOI availability validation failed: empty_eligibility")
    dates = eligibility["registry_snapshot_date"].tolist()
    expected = snapshot_date if snapshot_date is not None else dates[0]
    try:
        valid_date = isinstance(expected, str) and date.fromisoformat(expected).isoformat() == expected
    except ValueError:
        valid_date = False
    same_date = lambda value: isinstance(value, str) and value == expected
    if (not valid_date
            or not eligibility["registry_snapshot_date"].map(same_date).all()
            or not eligibility["eligibility_snapshot_date"].map(same_date).all()
            or not availability["snapshot_date"].map(same_date).all()):
        raise RuntimeError("Canonical FUTOI availability validation failed: snapshot_date_mismatch")
    if not eligibility["classification_status"].isin(["included", "deferred", "excluded"]).all():
        raise RuntimeError("Canonical FUTOI availability validation failed: invalid_classification")
    candidates = eligibility.loc[eligibility["classification_status"] == "included"]
    for field in ("board", "family_code", "registry_snapshot_id"):
        if not candidates[field].map(lambda value: isinstance(value, str) and bool(value.strip())).all():
            raise RuntimeError("Canonical FUTOI availability validation failed: invalid_" + field)
    by_secid = latest_by_secid(availability)
    folded = {key.upper(): key for key in by_secid}
    for _, row in candidates.iterrows():
        evidence = by_secid.get(row["secid"])
        if evidence is None:
            if row["secid"].upper() in folded:
                raise RuntimeError("Canonical FUTOI availability validation failed: secid_case_mismatch")
            continue
        # The generic registry builder intentionally uppercases board identifiers.
        if (str(evidence["board"]).upper() != row["board"].upper()
                or evidence["family_code"] != row["family_code"]):
            raise RuntimeError("Canonical FUTOI availability validation failed: identity_mismatch")
    return by_secid


def observed_row_count(value):
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    try:
        count = int(value)
    except (ValueError, OverflowError):
        return None
    return count if count >= 0 and count == value else None


def has_source_error(row):
    return any(pd.notna(row.get(key)) and bool(str(row.get(key)).strip())
               for key in ("error_code", "error_message"))


def derive_futoi_eligibility(eligibility, availability, snapshot_date=None):
    by_secid = validate_admission_inputs(eligibility, availability, snapshot_date)
    work = eligibility.copy()
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
        arow = by_secid.get(secid)
        if arow is None:
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
        count = observed_row_count(arow.get("observed_rows"))
        # Skipping a source as cleanly unavailable requires both diagnostic fields.
        if (availability_status == "unavailable"
                and not {"error_code", "error_message"}.issubset(arow.index)):
            futoi_status.append("fail_futoi_error_diagnostics_missing")
            futoi_flags.append(False)
            continue
        if (availability_status == "unavailable" and probe_status == "completed"
                and count == 0 and not has_source_error(arow)):
            futoi_status.append("deferred_futoi_unavailable")
            futoi_flags.append(False)
            continue
        if availability_status != "available" or probe_status != "completed":
            futoi_status.append("fail_futoi_availability_not_available_completed")
            futoi_flags.append(False)
            continue
        if count is None or count <= 0 or has_source_error(arow):
            futoi_status.append("fail_futoi_observed_rows_unproven")
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
    work["futoi_deferral_reason"] = [
        "futoi_unavailable" if value == "deferred_futoi_unavailable"
        else value.removeprefix("fail_") if value.startswith("fail_") else ""
        for value in futoi_status
    ]
    work["futoi_retry_required"] = [value.startswith("fail_") for value in futoi_status]
    return work


def selected_universe(futoi_eligibility, *, allow_empty=False):
    selected = futoi_eligibility.loc[(futoi_eligibility["classification_status"].astype(str) == "included") & (futoi_eligibility["futoi_eligible"] == True)].copy()
    if selected.empty:
        if allow_empty:
            return selected.reset_index(drop=True)
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
    return [str(x) for x in data if str(x)] if isinstance(data, list) else []


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


def fetch_futoi_exact_contract(secid, date_from, date_till, timeout, apim_base_url):
    ticker = str(secid or "").strip().lower()
    if not ticker:
        raise RuntimeError("Exact FUTOI fetch requires secid")
    path = "/iss/analyticalproducts/futoi/securities/" + ticker + ".json"
    token = str(os.getenv("MOEX_API_KEY", "")).strip()
    if not token:
        return pd.DataFrame(), "", "failed", "MOEX_API_KEY is required for FUTOI APIM", ticker
    params = {"from": date_from, "till": date_till, "latest": 1}
    source_url = base.url_join(apim_base_url, path)
    try:
        frame = base.fetch_paged_frame(apim_base_url, path, params, "futoi", timeout, True)
    except Exception as exc:
        return pd.DataFrame(), source_url, "failed", exc.__class__.__name__ + ": " + str(exc)[:500], ticker
    if frame.empty:
        return pd.DataFrame(), source_url, "failed", "empty_response", ticker
    columns = {str(c).strip().lower() for c in frame.columns}
    if "error_message" in columns:
        return pd.DataFrame(), source_url, "failed", "ERROR_MESSAGE payload", ticker
    return frame, source_url, "completed", "", ticker


def quality_row(run_id, chunk_id, erow, date_from, date_till, raw, fetch_status, failure, partitions, calendar_status):
    counts = futoi.quality_counts(raw, None)
    status = "pass" if not failure and int(counts.get("rows") or 0) > 0 else "fail"
    return {"run_id": run_id, "chunk_id": chunk_id, "eligibility_snapshot_id": str(erow.get("eligibility_snapshot_id", "")), "registry_snapshot_id": str(erow.get("registry_snapshot_id", "")), "dataset_stage": DATASET_STAGE, "family_code": str(erow.get("family_code")), "secid": str(erow.get("secid")), "date_from": date_from, "date_till": date_till, "rows_written": int(counts.get("rows") or 0) if status == "pass" else 0, "trade_dates": counts.get("trade_dates"), "min_ts": counts.get("min_ts"), "max_ts": counts.get("max_ts"), "duplicate_key_count": counts.get("duplicate_key_count"), "null_required_count": counts.get("null_required_count"), "invalid_position_count": counts.get("invalid_position_count"), "calendar_status": calendar_status, "source_payload_status": fetch_status, "partition_status": "written" if status == "pass" else "not_written", "quality_status": status, "failure_reason": failure, "futoi_availability_status": str(erow.get("futoi_availability_status", "")), "futoi_probe_status": str(erow.get("futoi_probe_status", "")), "selection_model": "eligibility_snapshot_driven_futoi_eligible_true", "output_partitions_json": json.dumps(partitions, sort_keys=True), "schema_version": SCHEMA_QUALITY}


def run_instrument(args, root, row, run_id, chunk_id, expected_calendar, calendar_status):
    secid = str(row.get("secid"))
    family_code = str(row.get("family_code"))
    board = str(row.get("board", "RFUD") or "RFUD")
    date_from, date_till = date_bounds(row, str(args.from_date or ""), str(args.till or ""))
    if bool(args.exact_contract_only):
        source_frame, source_url, fetch_status, fetch_error, source_ticker = fetch_futoi_exact_contract(secid, date_from, date_till, float(args.timeout), str(args.apim_base_url))
    else:
        source_frame, source_url, fetch_status, fetch_error, source_ticker = futoi.fetch_futoi(secid, family_code, date_from, date_till, float(args.timeout), str(args.apim_base_url))
    raw = pd.DataFrame()
    failure = ""
    partitions = []
    try:
        raw, meta = futoi.normalize_futoi(source_frame, secid, family_code, board, source_url, source_ticker, now_utc(), False, calendar_status)
        raw, _calendar_filter = futoi.filter_calendar_rows(raw, expected_calendar)
        counts = futoi.quality_counts(raw, expected_calendar)
        qstatus, notes = futoi.status_from_counts(counts, fetch_status, calendar_status, str(row.get("futoi_availability_status", "")), str(row.get("futoi_probe_status", "")))
        if qstatus == "fail":
            failure = notes or fetch_error or "futoi_raw_quality_failed"
        else:
            partitions = futoi.write_partitions(raw, root, family_code, secid)
    except Exception as exc:
        failure = exc.__class__.__name__ + ": " + str(exc)
    return quality_row(run_id, chunk_id, row, date_from, date_till, raw, fetch_status, failure, partitions, calendar_status), partitions, failure


def quality_report_frame(rows):
    """Keep v1 report columns and Parquet types stable, including empty chunks."""
    template = quality_row("", "", {}, None, None, pd.DataFrame(), "", "", [], "")
    template["deferred_reason"] = ""
    counts = {
        "rows_written", "trade_dates", "duplicate_key_count",
        "null_required_count", "invalid_position_count",
    }
    frame = pd.DataFrame(rows, columns=list(template))
    for column in frame.columns:
        dtype = "Int64" if column in counts else pd.StringDtype(storage="python")
        frame[column] = frame[column].astype(dtype)
    return frame


def run_chunk(args, root, selected, run_id, chunk_id):
    if selected.empty:
        return {
            "schema_version": SCHEMA_MANIFEST, "chunk_id": chunk_id,
            "dataset_stage": DATASET_STAGE,
            "selection_model": "eligibility_snapshot_driven_futoi_eligible_true",
            "secid_list": [], "family_count": 0, "date_from": None, "date_till": None,
            "status": "deferred", "started_at": run_id, "finished_at": now_utc(),
            "failed_secid": [], "output_partitions": [], "quality_summary": {},
            "calendar_validation_summary": {"calendar_denominator_status": "not_requested_no_eligible_instruments"},
            "no_futoi_prejoin_into_ohlcv": True, "exact_contract_only": bool(args.exact_contract_only),
        }, quality_report_frame([])
    starts = []
    ends = []
    for _, row in selected.iterrows():
        start, end = date_bounds(row, str(args.from_date or ""), str(args.till or ""))
        starts.append(start)
        ends.append(end)
    calendar_from = min(starts)
    calendar_till = max(ends)
    reference_secid = base._reference_secid(selected)
    expected_calendar, calendar_status = base.fetch_observed_trading_dates(
        calendar_from, calendar_till, reference_secid,
        float(args.timeout), str(args.apim_base_url),
    )
    if not expected_calendar or calendar_status != base.OBSERVED_DATE_STATUS:
        raise RuntimeError("authoritative observed TradeStats date validation failed: " + str(calendar_status))
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
    manifest = {"schema_version": SCHEMA_MANIFEST, "chunk_id": chunk_id, "dataset_stage": DATASET_STAGE, "selection_model": "eligibility_snapshot_driven_futoi_eligible_true", "secid_list": selected["secid"].astype(str).tolist(), "family_count": int(selected["family_code"].nunique()), "date_from": calendar_from, "date_till": calendar_till, "status": status, "started_at": run_id, "finished_at": now_utc(), "failed_secid": failed, "output_partitions": partitions, "quality_summary": {str(k): int(v) for k, v in quality["quality_status"].astype(str).value_counts(dropna=False).to_dict().items()} if not quality.empty else {}, "calendar_validation_summary": {"calendar_denominator_status": calendar_status, "calendar_from": calendar_from, "calendar_till": calendar_till, "expected_trading_days": len(expected_calendar)}, "no_futoi_prejoin_into_ohlcv": True, "exact_contract_only": bool(args.exact_contract_only)}
    return manifest, quality


def record_availability_outcomes(manifest, quality, scoped, run_id, chunk_id):
    deferred = scoped.loc[~scoped["futoi_eligible"]].copy()
    retry = deferred.loc[deferred["futoi_retry_required"], "secid"].tolist()
    instrument_failures = list(manifest.get("failed_secid") or [])
    manifest["candidate_secid_list"] = scoped["secid"].tolist()
    manifest["deferred_secid"] = deferred["secid"].tolist()
    manifest["deferral_reasons"] = dict(zip(deferred["secid"], deferred["futoi_deferral_reason"]))
    manifest["availability_retry_secid"] = retry
    manifest["failed_secid"] = list(dict.fromkeys(instrument_failures + retry))
    manifest["futoi_coverage_complete"] = not manifest["deferred_secid"] and not manifest["failed_secid"]
    if retry:
        successful = len(manifest["secid_list"]) - len(instrument_failures)
        manifest["status"] = "partial_failed" if successful > 0 else "failed"
    rows = quality.assign(deferred_reason="").to_dict("records")
    for _, row in deferred.iterrows():
        reason = str(row["futoi_deferral_reason"])
        deferred_row = quality_row(
            run_id, chunk_id, row, None, None, pd.DataFrame(),
            row["futoi_probe_status"],
            reason if row["futoi_retry_required"] else "", [], "not_requested",
        )
        deferred_row["quality_status"] = "fail" if row["futoi_retry_required"] else "deferred"
        deferred_row["deferred_reason"] = reason
        rows.append(deferred_row)
    quality = quality_report_frame(rows)
    manifest["quality_summary"] = {
        str(key): int(value) for key, value in quality["quality_status"].value_counts().items()
    }
    return manifest, quality


def aggregate(eligibility, futoi_eligibility, selected, manifest):
    return {"candidate_universe_count": int(len(eligibility)), "included_count": int((eligibility["classification_status"].astype(str) == "included").sum()), "deferred_count": int((eligibility["classification_status"].astype(str) == "deferred").sum()), "excluded_count": int((eligibility["classification_status"].astype(str) == "excluded").sum()), "futoi_eligible_count": int((futoi_eligibility["futoi_eligible"] == True).sum()), "selected_futoi_secid_count": int(len(selected)), "failed_secid_count": int(len(manifest.get("failed_secid") or [])), "chunk_status": manifest.get("status"), "classification_visibility_preserved": True, "futoi_deferred_count": int(futoi_eligibility["futoi_deferral_reason"].ne("").sum()), "scope_deferred_secid_count": len(manifest.get("deferred_secid") or []), "availability_retry_secid_count": len(manifest.get("availability_retry_secid") or []), "futoi_coverage_complete": manifest.get("futoi_coverage_complete", False)}


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
    futoi_eligibility = derive_futoi_eligibility(eligibility, availability, args.snapshot_date)
    selected = futoi_eligibility.loc[futoi_eligibility["classification_status"] == "included"].copy()
    selected = apply_scope_filters(selected, secid_filter, family_filter)
    scoped = selected.copy()
    selected = selected_universe(scoped, allow_empty=True)
    run_id = "all_universe_futoi_raw_" + args.run_date + "_" + base.stable_id([args.snapshot_date, eligibility_path, availability_path, now_utc(), ",".join(secid_filter), ",".join(family_filter), bool(args.exact_contract_only)])
    chunk_id = "futoi_raw_" + base.stable_id([args.snapshot_date, ",".join(scoped["secid"].astype(str).tolist()), args.from_date, args.till, bool(args.exact_contract_only)])
    out = paths(root, args.snapshot_date, chunk_id)
    write_parquet(out["futoi_eligibility_snapshot"], futoi_eligibility)
    manifest, quality = run_chunk(args, root, selected, run_id, chunk_id)
    manifest, quality = record_availability_outcomes(manifest, quality, scoped, run_id, chunk_id)
    manifest["input_artifacts"] = {"eligibility_snapshot": eligibility_path, "futoi_availability_report": availability_path}
    manifest["output_artifacts"] = out
    manifest["scope_filters"] = {"secid": secid_filter, "family": family_filter}
    write_parquet(out["quality_report"], quality)
    dump_json(out["chunk_manifest"], manifest)
    aggregate_report = aggregate(eligibility, futoi_eligibility, selected, manifest)
    dump_json(out["aggregate_report"], aggregate_report)
    print(json.dumps({"outputs": out, "selection_mode": args.selection_mode, "scope_filters": {"secid": secid_filter, "family": family_filter}, "exact_contract_only": bool(args.exact_contract_only), "selected_universe": {"secid_count": int(len(selected)), "secids": selected["secid"].astype(str).tolist(), "dataset_stage": DATASET_STAGE}, "chunk_status": aggregate_report.get("chunk_status"), "aggregate_report": aggregate_report}, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if aggregate_report.get("chunk_status") == "succeeded" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
