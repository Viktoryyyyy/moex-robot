"""Bounded explicit-contract capture into a new isolated run, without promotion."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

SOURCE = "moex_algopack_fo_tradestats_5m"
EMPTY_ERRORS = {
    "APIM tradestats response returned no rows",
    "APIM tradestats response contains no rows for requested secid",
    "APIM tradestats response contains no rows for requested secid/date",
}


def validate_scope(entry: dict, instrument: str, secid: str, start: date, end: date, today: date) -> None:
    if instrument not in ("si_futures_family", "cr_futures_family"):
        raise ValueError("only explicit Si/CR contract capture is supported")
    if entry.get("instrument_id") != instrument or entry.get("secid") != secid:
        raise ValueError("explicit contract identity mismatch")
    if entry.get("source_id") != SOURCE or entry.get("evidence_status") != "pilot_passed":
        raise ValueError("registered source pilot evidence required")
    binding = entry.get("contract_binding", {})
    if binding.get("type") != "expiring_current_explicit" or binding.get("observed_secid") != secid:
        raise ValueError("explicit expiring contract binding required")
    expiry = date.fromisoformat(str(binding["observed_last_trade_date"]))
    if start > end or (end - start).days >= 60:
        raise ValueError("capture must cover 1 to 60 calendar dates")
    if end >= today or end > expiry:
        raise ValueError("only completed dates within contract lifetime may be captured")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def reserve(root: Path, run_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
        raise ValueError("run_id must be a bounded safe token")
    if root.is_symlink() or not root.is_dir():
        raise ValueError("existing data root required")
    root = root.resolve()
    target = root / "runs/rub_contract_history" / ("run_id=" + run_id)
    if not target.resolve().is_relative_to(root):
        raise ValueError("capture escaped data root")
    target.mkdir(parents=True, exist_ok=False)
    return target


def write_json(path: Path, payload: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def capture(*, root: Path, registry: Path, instrument: str, secid: str,
            start: date, end: date, run_id: str) -> dict:
    import yaml
    from moex_data.futures import materialize_forts_raw_5m_instrument as producer

    registry_bytes = registry.read_bytes()
    entries = yaml.safe_load(registry_bytes)["instruments"]
    matches = [entry for entry in entries if entry.get("instrument_id") == instrument]
    if len(matches) != 1:
        raise ValueError("registry instrument binding must be unique")
    entry = matches[0]
    validate_scope(entry, instrument, secid, start, end, datetime.now(ZoneInfo("Europe/Moscow")).date())
    run = reserve(root, run_id)
    (run / "registry.yaml").write_bytes(registry_bytes)
    report = {"schema_version": "rub_contract_history_capture.v1", "run_id": run_id,
        "instrument_id": instrument, "secid": secid, "source_id": SOURCE,
        "date_start": start.isoformat(), "date_end": end.isoformat(),
        "registry_sha256": hashlib.sha256(registry_bytes).hexdigest(),
        "binding": entry["contract_binding"], "scope": "explicit_contract_not_continuous_family",
        "started_at_utc": datetime.now(timezone.utc).isoformat(), "dates": [],
        "accepted_pointer_promotion": False, "model_readiness_granted": False,
        "calendar_completeness_claimed": False, "historical_pit_readiness_claimed": False}
    previous_root = os.environ.get("MOEX_DATA_ROOT")
    os.environ["MOEX_DATA_ROOT"] = str(run)
    try:
        for offset in range((end - start).days + 1):
            day = (start + timedelta(days=offset)).isoformat()
            item = {"trade_date": day}
            try:
                result = producer.materialize_instrument_partition(day, instrument, secid,
                    run_id + "_" + day.replace("-", "")).payload
                if result["status"] != "succeeded" or result["quality_status"] != "pass":
                    raise ValueError("canonical materialization did not pass")
                artifacts = {}
                for name, key in (("partition", "storage_partition_path"),
                                  ("manifest", "manifest_reference"), ("quality", "quality_report_reference")):
                    path = Path(result[key])
                    if path.is_symlink() or not path.resolve().is_relative_to(run):
                        raise ValueError("producer artifact escaped isolated run")
                    artifacts[name] = {"path": path.relative_to(run).as_posix(), "sha256": digest(path)}
                item.update(status="CAPTURED", row_count=result["row_count"], artifacts=artifacts)
            except Exception as error:
                item.update(status="SOURCE_EMPTY" if str(error) in EMPTY_ERRORS else "FAILED", error=str(error))
            report["dates"].append(item)
            report["status"] = "RUNNING"
            write_json(run / "capture_manifest.json", report)
            print(json.dumps({"run_id": run_id, **{k:v for k,v in item.items() if k != "artifacts"}}), flush=True)
        report["captured_date_count"] = sum(item["status"] == "CAPTURED" for item in report["dates"])
        report["row_count"] = sum(item.get("row_count", 0) for item in report["dates"])
        report["failed_date_count"] = sum(item["status"] == "FAILED" for item in report["dates"])
        report["status"] = "CAPTURED_PENDING_ACCEPTANCE" if report["captured_date_count"] and not report["failed_date_count"] else "FAILED"
        report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        write_json(run / "capture_manifest.json", report)
    finally:
        if previous_root is None:
            os.environ.pop("MOEX_DATA_ROOT", None)
        else:
            os.environ["MOEX_DATA_ROOT"] = previous_root
    return report


def main() -> int:
    from moex_data.futures.materialize_forts_raw_5m_instrument import load_env_file
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    load_env_file(args.env_file)
    result = capture(root=args.data_root, registry=args.registry, instrument=args.instrument_id,
        secid=args.secid, start=args.start, end=args.end, run_id=args.run_id)
    print(json.dumps({key:value for key,value in result.items() if key != "dates"}), flush=True)
    return 0 if result["status"] == "CAPTURED_PENDING_ACCEPTANCE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
