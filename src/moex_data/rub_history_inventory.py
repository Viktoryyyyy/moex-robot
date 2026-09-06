"""Read-only inventory of accepted RUB datasets and separate continuous history.

File presence and date span are inventory evidence, never completeness or model
acceptance. No network requests, dataset writes or pointer promotion occur here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

PREFIX = "${MOEX_DATA_ROOT}/"
DATE_FIELDS = ("trade_date", "period_start_date", "period_end_date", "week_start", "week_end")


def rooted(root: Path, reference: str) -> Path:
    if not isinstance(reference, str) or not reference.startswith(PREFIX):
        raise ValueError("explicit MOEX_DATA_ROOT reference required")
    relative = Path(reference.removeprefix(PREFIX))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("invalid artifact reference")
    path = root / relative
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("artifact must be a rooted regular file")
    return path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parquet_summary(path: Path) -> dict:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    # schema.names contains nested leaf names (e.g. repeated "element").
    columns = parquet.schema_arrow.names
    fields = [field for field in DATE_FIELDS if field in columns]
    data = parquet.read(columns=fields).to_pydict()
    ranges = {}
    for field, values in data.items():
        non_null = [date.fromisoformat(str(value)[:10]).isoformat() for value in values if value is not None]
        ranges[field] = {"first": min(non_null) if non_null else None,
                         "last": max(non_null) if non_null else None,
                         "distinct_count": len(set(non_null)), "null_count": len(values)-len(non_null)}
    return {"row_count": parquet.metadata.num_rows, "columns": columns, "date_ranges": ranges}


def audit_pointer(root: Path, path: Path, reader=parquet_summary) -> dict:
    output = {"pointer_ref": PREFIX + path.relative_to(root).as_posix()}
    try:
        rooted(root, output["pointer_ref"])
        pointer_bytes = path.read_bytes()
        pointer = json.loads(pointer_bytes)
        output.update({key: pointer.get(key) for key in ("dataset_id", "instrument_id", "timeframe", "run_id")})
        output["pointer_sha256"] = hashlib.sha256(pointer_bytes).hexdigest()
        if "partition_ref" not in pointer:
            output["status"] = "NOT_SINGLE_PARTITION_POINTER"
            return output
        checks = {}
        artifacts = {}
        for name in ("partition", "manifest", "quality_report"):
            artifact = rooted(root, pointer[name + "_ref"])
            checks[name] = sha256(artifact) == pointer[name + "_sha256"]
            artifacts[name] = artifact
        output["hash_checks"] = checks
        if not all(checks.values()):
            raise ValueError("accepted artifact SHA mismatch")
        manifest = json.loads(artifacts["manifest"].read_text())
        quality = json.loads(artifacts["quality_report"].read_text())
        output["declared_quality_status"] = quality.get("quality_status")
        output["declared_refresh_status"] = manifest.get("refresh_status")
        output["partition_ref"] = pointer["partition_ref"]
        output.update(reader(artifacts["partition"]))
        # A scan racing publication must not present mixed pointer generations.
        if path.read_bytes() != pointer_bytes:
            raise ValueError("accepted pointer changed during inventory")
        if sha256(artifacts["partition"]) != pointer["partition_sha256"]:
            raise ValueError("partition changed during inventory")
        output["status"] = "INTEGRITY_VERIFIED_INVENTORIED"
    except Exception as error:
        output["status"] = "ERROR"
        output["error"] = str(error)
    output["source_completeness"] = "NOT_VERIFIED"
    output["model_readiness_granted"] = False
    return output


def continuous_inventory(root: Path, reader=parquet_summary) -> list[dict]:
    groups = []
    for dataset in ("continuous_5m", "continuous_d1", "continuous_w1"):
        for family in ("Si", "CR"):
            paths = sorted((root / "futures" / dataset).glob(
                "roll_policy=*/adjustment_policy=*/family=" + family + "/*/part.parquet"))
            policies = {}
            for path in paths:
                key = path.parents[1].relative_to(root).as_posix()
                policies.setdefault(key, []).append(path)
            if not policies:
                groups.append({"dataset": dataset, "family": family, "status": "NO_FILES",
                               "file_count": 0, "acceptance": "NOT_VERIFIED"})
            for policy, files in sorted(policies.items()):
                entries = []
                for path in files:
                    ref = PREFIX + path.relative_to(root).as_posix()
                    try:
                        rooted(root, ref)
                        before = sha256(path)
                        summary = reader(path)
                        if sha256(path) != before:
                            raise ValueError("file changed during inventory")
                        entries.append({"ref": ref, "sha256": before, **summary})
                    except Exception as error:
                        entries.append({"ref": ref, "error": str(error)})
                dates = [value for entry in entries for field, bounds in entry.get("date_ranges", {}).items()
                         if field in ("trade_date", "week_start") for value in (bounds["first"], bounds["last"]) if value]
                groups.append({"dataset": dataset, "family": family, "policy_path": policy,
                    "status": "ERROR" if any("error" in entry for entry in entries) else "FILES_INVENTORIED",
                    "file_count": len(files), "row_count": sum(entry.get("row_count", 0) for entry in entries),
                    "first_observed_date": min(dates) if dates else None,
                    "last_observed_date": max(dates) if dates else None,
                    "source_completeness": "NOT_VERIFIED", "acceptance": "NOT_VERIFIED", "files": entries})
    return groups


def inventory(root: Path) -> dict:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("data root must be a regular existing directory")
    root = root.resolve()
    pointers = sorted((root / "state" / "datasets").rglob("current_accepted_manifest.json"))
    accepted = [audit_pointer(root, path) for path in pointers]
    continuous = continuous_inventory(root)
    return {"schema_version": "rub_history_inventory.v1", "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "accepted_datasets": accepted, "continuous_history": continuous,
            "inventory_errors": sum(item["status"] == "ERROR" for item in accepted + continuous),
            "completeness_policy": "No exchange-calendar inference from weekdays or file presence",
            "model_readiness_granted": False, "pointer_promotion_performed": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, type=Path)
    args = parser.parse_args()
    report = inventory(args.data_root)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report["inventory_errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
