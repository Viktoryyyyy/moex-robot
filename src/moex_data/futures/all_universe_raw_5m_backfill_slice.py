#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path.cwd() / "src"))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

import pandas as pd
from moex_data.futures import liquidity_history_metrics_probe as base
from moex_data.futures import liquidity_history_metrics_probe_apim_calendar as apim_calendar
from moex_data.futures import raw_5m_loader

TZ_MSK = ZoneInfo("Europe/Moscow")
SCHEMA_REGISTRY = "futures_all_universe_snapshot.v1"
SCHEMA_ELIGIBILITY = "futures_all_universe_eligibility_snapshot.v1"
SCHEMA_MANIFEST = "futures_all_universe_raw_5m_chunk_manifest.v1"
SCHEMA_QUALITY = "futures_all_universe_raw_5m_quality_report.v1"
DATASET_STAGE = "raw_5m"
REQUIRED_SOT_FILES = [
    "contracts/datasets/futures_all_universe_snapshot_contract.md",
    "contracts/datasets/futures_all_universe_eligibility_contract.md",
    "configs/datasets/futures_all_universe_eligibility_config.json",
]
NOT_APPLICABLE_FIRST_SLICE = "not_applicable_pm_l3_2_raw_5m_slice"
EXPLICIT_PM_EXCLUSION = "explicit_pm_exclusion"
FIRST_SLICE_NOTES = "PM L3-2 bounded raw 5m first executable slice"


def now_utc():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def today_msk():
    return datetime.now(TZ_MSK).date().isoformat()


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


def data_root(args):
    raw = str(args.data_root or os.getenv("MOEX_DATA_ROOT", "")).strip()
    if not raw:
        raise RuntimeError("MOEX_DATA_ROOT or --data-root is required")
    return Path(raw).expanduser().resolve()


def paths(root, snapshot_date, chunk_id):
    base_dir = root / "futures" / "all_universe"
    return {
        "registry_snapshot": str(base_dir / "registry_snapshot" / ("snapshot_date=" + snapshot_date) / "registry_snapshot.parquet"),
        "eligibility_snapshot": str(base_dir / "eligibility_snapshot" / ("snapshot_date=" + snapshot_date) / "eligibility_snapshot.parquet"),
        "chunk_manifest": str(base_dir / "runs" / "raw_5m_backfill" / ("chunk_id=" + chunk_id) / "manifest.json"),
        "quality_report": str(base_dir / "quality" / "raw_5m_backfill" / ("chunk_id=" + chunk_id) / "quality_report.parquet"),
        "aggregate_report": str(base_dir / "quality" / "raw_5m_backfill" / ("chunk_id=" + chunk_id) / "aggregate_report.json"),
    }


def load_registry(repo_root, root, snapshot_date):
    contracts = base.load_contract_values(repo_root)
    p = base.resolve_contract_path(root, contracts, base.CONTRACT_BY_ID["normalized_registry"], snapshot_date)
    if not p.exists():
        raise FileNotFoundError("Missing normalized registry artifact: " + str(p))
    return str(p), pd.read_parquet(p)


def family(row):
    for key in ["family_code", "family", "asset_code", "short_code"]:
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    secid = str(row.get("secid", "") or "").strip()
    return "".join([c for c in secid if not c.isdigit()]) or secid


def build_registry(frame, snapshot_date, source_path, run_id, config):
    if "secid" not in frame.columns:
        raise RuntimeError("normalized registry missing secid")
    rows = []
    for _, row in frame.iterrows():
        secid = str(row.get("secid", "") or "").strip()
        if not secid:
            continue
        board = str(row.get("board", "RFUD") or "RFUD").strip().upper()
        fam = family(row)
        rows.append({
            "registry_snapshot_id": "registry_" + base.stable_id([snapshot_date, secid, board, source_path]),
            "registry_snapshot_date": snapshot_date,
            "engine": str(config.get("engine", "futures")),
            "market": str(config.get("market", "forts")),
            "board": board,
            "secid": secid,
            "short_code": str(row.get("short_code", "") or secid).strip(),
            "family_code": fam,
            "asset_code": str(row.get("asset_code", "") or fam).strip(),
            "instrument_type": str(row.get("instrument_type", "") or row.get("instrument_kind", "") or "future").strip(),
            "expiration_date": str(row.get("expiration_date", "") or row.get("last_trade_date", "") or "").strip(),
            "is_perpetual_candidate": secid.upper() == "USDRUBF",
            "first_seen_date": str(row.get("first_seen_date", "") or snapshot_date).strip(),
            "last_seen_date": str(row.get("last_seen_date", "") or snapshot_date).strip(),
            "registry_source": "normalized_registry",
            "source_scope": source_path,
            "schema_version": SCHEMA_REGISTRY,
            "build_run_id": run_id,
            "build_ts": now_utc(),
            "source_artifact_path": source_path,
            "row_status": "candidate_only",
            "row_status_reason": "candidate_only_registry_discovery",
        })
    return pd.DataFrame(rows)


def recent_dates(snapshot_date, count, timeout, iss_base_url):
    end = datetime.strptime(snapshot_date, "%Y-%m-%d").date()
    start = end - timedelta(days=30)
    dates, status = apim_calendar.fetch_futures_calendar(start.isoformat(), end.isoformat(), timeout, iss_base_url)
    if dates is None or status != "canonical_apim_futures_xml":
        raise RuntimeError("futures calendar unavailable: " + str(status))
    selected = sorted([x for x in dates if x <= snapshot_date])[-int(count):]
    if len(selected) < int(count):
        raise RuntimeError("Not enough recent futures trading dates")
    return selected


def choose(registry, config):
    fs = config.get("first_executable_slice") or {}
    board = str(fs.get("board", "RFUD")).upper()
    fam = str(fs.get("preferred_family_code", "Si"))
    max_n = int(fs.get("max_secid", 2))
    excluded = {str(x).upper() for x in fs.get("excluded_secids", [])}
    preferred = [str(x) for x in fs.get("preferred_secids", [])]
    base_scope = registry.loc[(registry["board"].astype(str).str.upper() == board) & (~registry["secid"].astype(str).str.upper().isin(excluded))].copy()
    if not preferred:
        c = base_scope.loc[base_scope["family_code"].astype(str) == fam].copy()
        if c.empty:
            c = base_scope.copy()
        c = c.sort_values(["family_code", "secid"]).head(max_n)
    else:
        rows = []
        upper_to_row = {str(row.get("secid")).upper(): row for _, row in base_scope.iterrows()}
        for secid in preferred:
            row = upper_to_row.get(secid.upper())
            if row is not None:
                rows.append(row.to_dict())
        c = pd.DataFrame(rows).head(max_n)
    if c.empty:
        raise RuntimeError("No first-slice RFUD candidates after exclusions")
    return c["secid"].astype(str).tolist(), str(c["family_code"].iloc[0])


def classification_for_row(secid, board, family_code, in_slice, explicitly_excluded, supported):
    identity_status = "pass" if str(secid).strip() else "fail"
    board_status = "pass" if board in supported else "deferred"
    family_status = "pass" if str(family_code or "").strip() else "deferred"
    if explicitly_excluded:
        return "excluded", EXPLICIT_PM_EXCLUSION, identity_status, board_status, family_status, "not_applicable_explicit_pm_exclusion"
    if identity_status != "pass":
        return "excluded", "missing_required_identity_fields", identity_status, board_status, family_status, "not_applicable_missing_identity"
    if board_status != "pass":
        return "deferred", "unsupported_board_pending_review", identity_status, board_status, family_status, "not_evaluated_unsupported_board"
    if family_status != "pass":
        return "deferred", "family_mapping_unresolved", identity_status, board_status, family_status, "not_evaluated_family_mapping_unresolved"
    if in_slice:
        return "included", "first_executable_slice_selected", identity_status, board_status, family_status, "pass"
    return "deferred", "not_selected_for_first_executable_slice", identity_status, board_status, family_status, "not_selected_for_first_executable_slice"


def build_eligibility(registry, selected, dates, config, registry_snapshot_id):
    selected_upper = {x.upper() for x in selected}
    supported = {str(x).upper() for x in config.get("supported_boards", ["RFUD"])}
    excluded = {str(x).upper() for x in (config.get("first_executable_slice") or {}).get("excluded_secids", [])}
    rows = []
    for _, row in registry.iterrows():
        secid = str(row.get("secid"))
        board = str(row.get("board", "")).upper()
        family_code = row.get("family_code")
        in_slice = secid.upper() in selected_upper
        explicitly_excluded = secid.upper() in excluded
        status, reason, identity_status, board_status, family_status, raw_5m_status = classification_for_row(secid, board, family_code, in_slice, explicitly_excluded, supported)
        is_included = status == "included"
        is_excluded = status == "excluded"
        if is_included:
            backfill_status = "selected"
        elif is_excluded:
            backfill_status = "excluded"
        else:
            backfill_status = "deferred"
        calendar_status = "pass" if dates else "deferred"
        rows.append({
            "eligibility_snapshot_id": "eligibility_" + base.stable_id([registry_snapshot_id, secid, status, reason]),
            "registry_snapshot_id": registry_snapshot_id,
            "eligibility_snapshot_date": row.get("registry_snapshot_date"),
            "registry_snapshot_date": row.get("registry_snapshot_date"),
            "engine": row.get("engine"),
            "market": row.get("market"),
            "board": board,
            "secid": secid,
            "short_code": row.get("short_code"),
            "family_code": family_code,
            "asset_code": row.get("asset_code"),
            "instrument_type": row.get("instrument_type"),
            "expiration_date": row.get("expiration_date"),
            "classification_status": status,
            "classification_reason": reason,
            "deferral_reason": "" if status in ["included", "excluded"] else reason,
            "exclusion_reason": reason if is_excluded else "",
            "registry_source": row.get("registry_source"),
            "identity_check_status": identity_status,
            "board_check_status": board_status,
            "family_mapping_status": family_status,
            "raw_5m_check_status": raw_5m_status,
            "futoi_check_status": NOT_APPLICABLE_FIRST_SLICE,
            "liquidity_check_status": NOT_APPLICABLE_FIRST_SLICE,
            "history_depth_check_status": NOT_APPLICABLE_FIRST_SLICE,
            "expiration_policy_status": NOT_APPLICABLE_FIRST_SLICE,
            "perpetual_policy_status": NOT_APPLICABLE_FIRST_SLICE,
            "calendar_quality_status": calendar_status,
            "continuous_eligibility_status": "deferred_pm_l3_2_out_of_scope",
            "registry_only_eligible": not is_included,
            "raw_5m_eligible": is_included and raw_5m_status == "pass",
            "futoi_eligible": False,
            "raw_d1_eligible": False,
            "continuous_v1_eligible": False,
            "access_api_eligible": is_included,
            "w1_eligible": False,
            "w1_status": "known_gap",
            "future_no_trade_not_yet_loadable": False,
            "expired_no_current_load_scope": False,
            "backfill_selection_status": backfill_status,
            "backfill_selection_reason": reason,
            "selected_trading_dates_json": json.dumps(dates, sort_keys=True),
            "source_scope": row.get("source_scope"),
            "notes": FIRST_SLICE_NOTES,
            "schema_version": SCHEMA_ELIGIBILITY,
        })
    return pd.DataFrame(rows)


def qrow(run_id, chunk_id, erow, registry_snapshot_id, date_from, date_till, raw, fetch_status, failure, partitions):
    rows = int(len(raw)) if raw is not None else 0
    duplicate_ts = int(raw.duplicated(subset=raw_5m_loader.PRIMARY_KEY).sum()) if raw is not None and not raw.empty else 0
    null_ohlc = int(raw[["open", "high", "low", "close"]].isna().any(axis=1).sum()) if raw is not None and not raw.empty else 0
    invalid = 0
    if raw is not None and not raw.empty:
        bad = (raw["high"] < raw["low"]) | (raw["open"] > raw["high"]) | (raw["open"] < raw["low"]) | (raw["close"] > raw["high"]) | (raw["close"] < raw["low"])
        invalid = int(bad.fillna(True).sum())
    status = "pass" if rows > 0 and duplicate_ts == 0 and null_ohlc == 0 and invalid == 0 and not failure else "fail"
    return {
        "run_id": run_id,
        "chunk_id": chunk_id,
        "eligibility_snapshot_id": str(erow.get("eligibility_snapshot_id")),
        "registry_snapshot_id": registry_snapshot_id,
        "dataset_stage": DATASET_STAGE,
        "family_code": str(erow.get("family_code")),
        "secid": str(erow.get("secid")),
        "date_from": date_from,
        "date_till": date_till,
        "rows_written": rows if status == "pass" else 0,
        "rows_expected_if_known": None,
        "min_ts": str(raw["ts"].min()) if raw is not None and not raw.empty else None,
        "max_ts": str(raw["ts"].max()) if raw is not None and not raw.empty else None,
        "duplicate_ts_count": duplicate_ts,
        "gap_count": None,
        "null_ohlc_count": null_ohlc,
        "invalid_ohlc_count": invalid,
        "futoi_missing_count": None,
        "calendar_status": "canonical_apim_futures_xml",
        "session_calendar_status": "canonical_apim_futures_xml",
        "source_payload_status": fetch_status,
        "partition_status": "written" if status == "pass" else "not_written",
        "quality_status": status,
        "failure_reason": failure,
        "deferred_reason": "",
        "notes": FIRST_SLICE_NOTES,
        "output_partitions_json": json.dumps(partitions, sort_keys=True),
        "schema_version": SCHEMA_QUALITY,
    }


def run_chunk(args, root, eligibility, dates, run_id, registry_snapshot_id, chunk_id):
    selected = eligibility.loc[(eligibility["classification_status"] == "included") & (eligibility["raw_5m_eligible"] == True)].copy()
    date_from = min(dates)
    date_till = max(dates)
    secids = selected["secid"].astype(str).tolist()
    family_code = str(selected["family_code"].iloc[0])
    quality_rows = []
    partitions = []
    failed = []
    for _, erow in selected.iterrows():
        secid = str(erow.get("secid"))
        fam = str(erow.get("family_code"))
        board = str(erow.get("board"))
        frame, source_url, fetch_status, fetch_error = base.fetch_tradestats(secid, date_from, date_till, float(args.timeout), str(args.apim_base_url), str(args.iss_base_url))
        raw = pd.DataFrame()
        failure = ""
        try:
            raw, meta = raw_5m_loader.normalize_tradestats(frame, secid, fam, board, source_url, now_utc(), False, "canonical_apim_futures_xml")
            raw, duplicate_diag = raw_5m_loader.duplicate_timestamp_policy(raw)
            counts = raw_5m_loader.quality_counts(raw, set(dates))
            qstatus, notes = raw_5m_loader.status_from_counts(counts, fetch_status, "canonical_apim_futures_xml", "pass", False)
            if qstatus == "fail":
                failure = notes or fetch_error or "raw_5m_quality_failed"
            else:
                paths_written = raw_5m_loader.write_partitions(raw, root, fam, secid)
                partitions.extend(paths_written)
        except Exception as exc:
            failure = exc.__class__.__name__ + ": " + str(exc)
        if failure:
            failed.append(secid)
        quality_rows.append(qrow(run_id, chunk_id, erow, registry_snapshot_id, date_from, date_till, raw, fetch_status, failure, partitions))
    status = "succeeded" if not failed else ("partial_failed" if len(failed) < len(secids) else "failed")
    manifest = {
        "schema_version": SCHEMA_MANIFEST,
        "chunk_id": chunk_id,
        "input_eligibility_snapshot_id": str(selected["eligibility_snapshot_id"].iloc[0]),
        "dataset_stage": DATASET_STAGE,
        "family_code": family_code,
        "secid_list": secids,
        "date_from": date_from,
        "date_till": date_till,
        "attempt_number": int(args.attempt_number),
        "previous_attempt_id": str(args.previous_attempt_id or ""),
        "status": status,
        "started_at": run_id,
        "finished_at": now_utc(),
        "failed_secid": failed,
        "deferred_secid": [],
        "skipped_secid": [],
        "output_partitions": partitions,
        "quality_summary": {},
        "error_code": "secid_level_failure" if failed else "",
        "error_message": ",".join(failed),
        "retry_allowed": bool(failed),
        "next_retry_scope": "secid_date_range_dataset_stage" if failed else "",
    }
    quality = pd.DataFrame(quality_rows)
    if not quality.empty:
        manifest["quality_summary"] = {str(k): int(v) for k, v in quality["quality_status"].astype(str).value_counts(dropna=False).to_dict().items()}
    return manifest, quality


def aggregate(registry, eligibility, manifest):
    return {
        "candidate_universe_count": int(len(registry)),
        "included_count": int((eligibility["classification_status"] == "included").sum()),
        "deferred_count": int((eligibility["classification_status"] == "deferred").sum()),
        "excluded_count": int((eligibility["classification_status"] == "excluded").sum()),
        "raw_5m_eligible_count": int((eligibility["raw_5m_eligible"] == True).sum()),
        "futoi_eligible_count": int((eligibility["futoi_eligible"] == True).sum()),
        "raw_d1_eligible_count": int((eligibility["raw_d1_eligible"] == True).sum()),
        "continuous_v1_eligible_count": int((eligibility["continuous_v1_eligible"] == True).sum()),
        "access_api_eligible_count": int((eligibility["access_api_eligible"] == True).sum()),
        "w1_gap_count": int((eligibility["w1_status"].astype(str) == "known_gap").sum()),
        "failed_secid_count": int(len(manifest.get("failed_secid") or [])),
        "partial_failed_chunk_count": 1 if manifest.get("status") == "partial_failed" else 0,
        "preserved_partition_count": 0,
        "chunk_status": manifest.get("status"),
    }


def main():
    if load_dotenv is not None:
        load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default=today_msk())
    parser.add_argument("--run-date", default=today_msk())
    parser.add_argument("--config", default="configs/datasets/futures_all_universe_eligibility_config.json")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--iss-base-url", default=os.getenv("MOEX_ISS_BASE_URL", base.DEFAULT_ISS_BASE_URL))
    parser.add_argument("--apim-base-url", default=os.getenv("MOEX_API_URL", base.DEFAULT_APIM_BASE_URL))
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--attempt-number", type=int, default=1)
    parser.add_argument("--previous-attempt-id", default="")
    args = parser.parse_args()
    repo_root = Path.cwd().resolve()
    root = data_root(args)
    base.assert_files_exist(repo_root, REQUIRED_SOT_FILES)
    config = load_json(repo_root / args.config)
    if config.get("continuous_build_enabled") is not False or config.get("w1_build_enabled") is not False:
        raise RuntimeError("continuous_build_enabled and w1_build_enabled must be false")
    source_path, normalized = load_registry(repo_root, root, args.snapshot_date)
    run_id = "all_universe_raw_5m_slice_" + args.run_date + "_" + base.stable_id([args.snapshot_date, source_path, now_utc()])
    registry = build_registry(normalized, args.snapshot_date, source_path, run_id, config)
    selected, fam = choose(registry, config)
    dates = recent_dates(args.snapshot_date, int((config.get("first_executable_slice") or {}).get("recent_trading_dates", 3)), float(args.timeout), str(args.iss_base_url))
    registry_snapshot_id = "registry_snapshot_" + base.stable_id([args.snapshot_date, source_path, len(registry)])
    eligibility = build_eligibility(registry, selected, dates, config, registry_snapshot_id)
    chunk_id = "raw_5m_" + base.stable_id([registry_snapshot_id, fam, min(dates), max(dates), ",".join(selected)])
    out = paths(root, args.snapshot_date, chunk_id)
    write_parquet(out["registry_snapshot"], registry)
    write_parquet(out["eligibility_snapshot"], eligibility)
    manifest, quality = run_chunk(args, root, eligibility, dates, run_id, registry_snapshot_id, chunk_id)
    write_parquet(out["quality_report"], quality)
    dump_json(out["chunk_manifest"], manifest)
    report = aggregate(registry, eligibility, manifest)
    dump_json(out["aggregate_report"], report)
    print(json.dumps({"outputs": out, "first_slice_scope": {"family_code": fam, "secid_list": selected, "trading_dates": dates, "dataset_stage": DATASET_STAGE}, "chunk_status": manifest.get("status"), "aggregate_report": report}, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if manifest.get("status") in ["succeeded", "partial_failed"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("ERROR: " + exc.__class__.__name__ + ": " + str(exc), file=sys.stderr)
        raise SystemExit(1)
