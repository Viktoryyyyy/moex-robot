from __future__ import annotations

import hashlib
import json
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

ROOT_PREFIX = "${MOEX_DATA_ROOT}/"


def _rooted(root: Path, path: Path) -> str:
    return ROOT_PREFIX + path.resolve().relative_to(root.resolve()).as_posix()


def _sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dates(start: str, end: str, missing: set[str]) -> tuple[str, ...]:
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    values = []
    for offset in range((last - first).days + 1):
        value = (first + timedelta(days=offset)).isoformat()
        if value not in missing:
            values.append(value)
    return tuple(values)


@pytest.fixture(autouse=True)
def _stage5_legacy_fixture_adapter(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    """Adapt the pre-#386 Stage 5 synthetic fixture; never changes production fallback behavior."""
    if Path(str(request.fspath)).name != "test_step5_futoi_positioning_acceptance.py":
        return

    from moex_data.futures import stage2_raw_history_content_reattestation as attestation

    cache: dict[tuple[str, str], dict[str, object]] = {}

    def resolve_content_attested_history(*, dataset_id: str, instrument_id: str, repo_root: str | Path = ".") -> dict[str, object]:
        if dataset_id != "futures_futoi_raw":
            return attestation._stage5_test_real_resolver(
                dataset_id=dataset_id,
                instrument_id=instrument_id,
                repo_root=repo_root,
            )
        root_text = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
        if not root_text:
            raise ValueError("MOEX_DATA_ROOT is required")
        root = Path(root_text).resolve()
        key = (root.as_posix(), instrument_id)
        if key in cache:
            return cache[key]

        pointer = root / "state" / "datasets" / "dataset_id=futures_futoi_raw" / f"instrument_id={instrument_id}" / "current_accepted_manifest.json"
        if not pointer.is_file():
            raise ValueError("accepted raw pointer unavailable; current content-attestation marker missing")
        pointer_values = json.loads(pointer.read_text(encoding="utf-8"))
        manifest_ref = str(pointer_values.get("manifest_ref") or "")
        if not manifest_ref.startswith(ROOT_PREFIX):
            raise ValueError("legacy fixture manifest_ref invalid")
        legacy_manifest_path = root / manifest_ref[len(ROOT_PREFIX):]
        legacy = json.loads(legacy_manifest_path.read_text(encoding="utf-8"))
        start = str(legacy["requested_from"])
        end = str(legacy["requested_till"])
        missing = {str(value) for value in legacy.get("missing_partition_dates", [])}
        accepted_dates = _dates(start, end, missing)

        generation_id = "stage5_test_content_attestation_v1"
        generation_root = root / "state" / "accepted_manifests" / "raw_history_content_attestation" / f"generation_id={generation_id}"
        records: list[dict[str, object]] = []
        total_rows = 0
        for trade_date in accepted_dates:
            canonical = root / "market" / "supplementary" / "dataset_id=futures_futoi_raw" / f"instrument_id={instrument_id}" / f"trade_date={trade_date}" / "source=moex_algopack_futoi" / "part.parquet"
            if not canonical.is_file():
                raise ValueError("synthetic canonical partition missing")
            snapshot = generation_root / "raw" / "dataset_id=futures_futoi_raw" / f"instrument_id={instrument_id}" / f"trade_date={trade_date}" / "part.parquet"
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            if not snapshot.exists():
                os.link(canonical, snapshot)
            sha = _sha_file(snapshot)
            rows = int(len(pd.read_parquet(snapshot).index))
            total_rows += rows
            records.append({
                "trade_date": trade_date,
                "sha256": sha,
                "row_count": rows,
                "canonical_ref": _rooted(root, canonical),
                "snapshot_ref": _rooted(root, snapshot),
                "snapshot_path": snapshot.as_posix(),
            })

        report = generation_root / "reports" / "dataset_id=futures_futoi_raw" / f"instrument_id={instrument_id}" / "content_attestation_report.json"
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps({"status": "accepted", "instrument_id": instrument_id}, sort_keys=True) + "\n", encoding="utf-8")
        content_set_payload = "".join(f"{row['trade_date']}\t{row['sha256']}\n" for row in records).encode("utf-8")
        content_set_sha = hashlib.sha256(content_set_payload).hexdigest()
        manifest = generation_root / "manifests" / "dataset_id=futures_futoi_raw" / f"instrument_id={instrument_id}" / "accepted_manifest.json"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps({
            "content_attestation_report_ref": _rooted(root, report),
            "partition_content_set_sha256": content_set_sha,
        }, sort_keys=True) + "\n", encoding="utf-8")

        marker = root / "state" / "accepted_manifests" / "raw_history_content_attestation" / "current_batch.json"
        marker.parent.mkdir(parents=True, exist_ok=True)
        if not marker.exists():
            marker.write_text('{"status":"accepted","test_fixture":true}\n', encoding="utf-8")

        result: dict[str, object] = {
            "dataset_id": dataset_id,
            "instrument_id": instrument_id,
            "generation_id": generation_id,
            "marker_path": marker.as_posix(),
            "marker_sha256": _sha_file(marker),
            "manifest_path": manifest.as_posix(),
            "manifest_sha256": _sha_file(manifest),
            "partition_content_set_sha256": content_set_sha,
            "requested_from": start,
            "requested_till": end,
            "partition_count": len(records),
            "row_count": total_rows,
            "accepted_dates": accepted_dates,
            "missing_dates": tuple(sorted(missing)),
            "records": tuple(records),
            "canonical_raw_read_required": False,
        }
        cache[key] = result
        return result

    real_resolver = attestation.resolve_content_attested_history
    monkeypatch.setattr(attestation, "_stage5_test_real_resolver", real_resolver, raising=False)
    monkeypatch.setattr(attestation, "resolve_content_attested_history", resolve_content_attested_history)
