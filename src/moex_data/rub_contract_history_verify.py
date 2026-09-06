"""Verify an isolated contract capture; never publish an accepted pointer."""
from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from moex_data.rub_contract_history_capture import digest, validate_scope


def verify_rows(rows: list[dict], *, instrument: str, secid: str, day: str) -> dict:
    if not rows:
        raise ValueError("captured partition is empty")
    seen = set()
    stamps = []
    for row in rows:
        if row.get("instrument_id") != instrument or row.get("secid") != secid:
            raise ValueError("partition contract identity mismatch")
        if str(row.get("trade_date"))[:10] != day:
            raise ValueError("partition trade date mismatch")
        timestamp = datetime.fromisoformat(str(row["ts"]))
        if timestamp.isoformat() in seen:
            raise ValueError("duplicate 5m timestamp")
        seen.add(timestamp.isoformat())
        stamps.append(timestamp)
        if timestamp.date().isoformat() != day or timestamp.minute % 5 or timestamp.second or timestamp.microsecond:
            raise ValueError("invalid 5m interval timestamp")
        prices = [float(row[name]) for name in ("open", "high", "low", "close")]
        volume = float(row["volume"])
        if not all(math.isfinite(value) and value > 0 for value in prices):
            raise ValueError("non-finite or nonpositive price")
        if not math.isfinite(volume) or volume < 0:
            raise ValueError("invalid volume")
        opening, high, low, close = prices
        if not low <= opening <= high or not low <= close <= high:
            raise ValueError("invalid OHLC ordering")
    stamps.sort()
    return {"row_count":len(rows), "first_timestamp":stamps[0].isoformat(),
        "last_timestamp":stamps[-1].isoformat(),
        "intervals_over_5m":sum((right-left).total_seconds() > 300 for left,right in zip(stamps,stamps[1:])),
        "interval_breaks_are_not_automatically_missing_bars":True}


def verify(run: Path, minimum_observed_dates: int = 30) -> dict:
    import pyarrow.parquet as pq
    import yaml
    from zoneinfo import ZoneInfo

    if run.is_symlink() or not run.is_dir():
        raise ValueError("regular capture directory required")
    run = run.resolve()
    path = run / "capture_manifest.json"
    manifest_sha = digest(path)
    manifest = json.loads(path.read_text())
    if manifest["status"] != "CAPTURED_PENDING_ACCEPTANCE":
        raise ValueError("capture did not finish successfully")
    registry = run / "registry.yaml"
    if digest(registry) != manifest["registry_sha256"]:
        raise ValueError("registry SHA mismatch")
    matches = [entry for entry in yaml.safe_load(registry.read_text())["instruments"]
               if entry.get("instrument_id") == manifest["instrument_id"]]
    if len(matches) != 1:
        raise ValueError("ambiguous registry binding")
    start, end = date.fromisoformat(manifest["date_start"]), date.fromisoformat(manifest["date_end"])
    validate_scope(matches[0],manifest["instrument_id"],manifest["secid"],start,end,datetime.now(ZoneInfo("Europe/Moscow")).date())
    expected = [(start+timedelta(days=offset)).isoformat() for offset in range((end-start).days+1)]
    if [item["trade_date"] for item in manifest["dates"]] != expected:
        raise ValueError("capture omitted or duplicated a requested calendar date")
    results = []
    for item in manifest["dates"]:
        if item["status"] == "SOURCE_EMPTY":
            results.append({"trade_date":item["trade_date"], "status":"SOURCE_EMPTY_NOT_CALENDAR_ATTESTED"})
            continue
        if item["status"] != "CAPTURED":
            raise ValueError("failed capture date")
        artifacts = {}
        for name in ("partition", "quality", "manifest"):
            info = item["artifacts"][name]
            artifact = run / info["path"]
            if artifact.is_symlink() or not artifact.resolve().is_relative_to(run):
                raise ValueError("artifact escaped capture")
            if digest(artifact) != info["sha256"]:
                raise ValueError("artifact SHA mismatch")
            artifacts[name] = artifact
        raw_manifest = json.loads(artifacts["manifest"].read_text())
        quality = json.loads(artifacts["quality"].read_text())["rows"]
        if raw_manifest["refresh_status"] != "succeeded" or len(quality) != 1 or quality[0]["quality_status"] != "pass":
            raise ValueError("producer quality not pass")
        expected_run = manifest["run_id"] + "_" + item["trade_date"].replace("-", "")
        if raw_manifest["run_id"] != expected_run or quality[0]["run_id"] != expected_run:
            raise ValueError("producer run identity mismatch")
        for name, expected in (("instrument_id",manifest["instrument_id"]),
                               ("secid",manifest["secid"]),("trade_date",item["trade_date"]),
                               ("source_id",manifest["source_id"])):
            if quality[0].get(name) != expected or raw_manifest["source_contract"].get(name) != expected:
                raise ValueError("producer lineage identity mismatch")
        rows = pq.ParquetFile(artifacts["partition"]).read().to_pylist()
        summary = verify_rows(rows,instrument=manifest["instrument_id"],secid=manifest["secid"],day=item["trade_date"])
        if summary["row_count"] != item["row_count"] or summary["row_count"] != quality[0]["rows"]:
            raise ValueError("row count mismatch")
        if digest(artifacts["partition"]) != item["artifacts"]["partition"]["sha256"]:
            raise ValueError("partition changed during verification")
        results.append({"trade_date":item["trade_date"], "status":"VERIFIED", **summary})
    count = sum(item["status"] == "VERIFIED" for item in results)
    if count < minimum_observed_dates:
        raise ValueError("fewer than required observed dates")
    if digest(path) != manifest_sha:
        raise ValueError("capture manifest changed during verification")
    return {"schema_version":"rub_contract_history_verification.v1", "run_id":manifest["run_id"],
        "secid":manifest["secid"], "status":"VERIFIED_CAPTURE_NOT_MODEL_ACCEPTANCE",
        "capture_manifest_sha256":manifest_sha,"verified_at_utc":datetime.now(timezone.utc).isoformat(),
        "observed_date_count":count,"row_count":sum(item.get("row_count",0) for item in results),
        "source_empty_dates":[item["trade_date"] for item in results if item["status"] != "VERIFIED"],
        "dates":results,"accepted_pointer_promotion":False,"continuous_roll_readiness":False,
        "intraday_gap_reconciliation_complete":False,"historical_pit_readiness":False}


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run",type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(verify(args.run),ensure_ascii=False,indent=2))
