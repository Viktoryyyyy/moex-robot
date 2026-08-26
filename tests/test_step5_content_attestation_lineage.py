from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from moex_data import step5_futoi_positioning_acceptance_base as acceptance_base
from moex_data.futures import stage2_raw_history_content_reattestation as attestation
from moex_data.futures.freeze_accepted_futoi_history import freeze_accepted_history

ROOT_PREFIX = "${MOEX_DATA_ROOT}/"


def _rooted(root: Path, path: Path) -> str:
    return ROOT_PREFIX + path.relative_to(root).as_posix()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _date_digest(values: list[str]) -> str:
    payload = (("\n".join(values) + "\n") if values else "").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _current_fixture(root: Path, *, generation_id: str = "generation_current") -> dict[str, object]:
    generation = root / "state" / "accepted_manifests" / "raw_history_content_attestation" / f"generation_id={generation_id}"
    report = generation / "reports" / "dataset_id=futures_futoi_raw" / "instrument_id=si_futures_family" / "content_attestation_report.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text('{"status":"accepted"}\n', encoding="utf-8")
    manifest = generation / "manifests" / "dataset_id=futures_futoi_raw" / "instrument_id=si_futures_family" / "accepted_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({
        "content_attestation_report_ref": _rooted(root, report),
        "partition_content_set_sha256": "c" * 64,
    }, sort_keys=True) + "\n", encoding="utf-8")
    marker = root / "state" / "accepted_manifests" / "raw_history_content_attestation" / "current_batch.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({"generation_id": generation_id, "status": "accepted"}, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "dataset_id": "futures_futoi_raw",
        "instrument_id": "si_futures_family",
        "generation_id": generation_id,
        "marker_path": marker.as_posix(),
        "marker_sha256": _sha(marker),
        "manifest_path": manifest.as_posix(),
        "manifest_sha256": _sha(manifest),
        "partition_content_set_sha256": "c" * 64,
        "requested_from": "2026-08-17",
        "requested_till": "2026-08-17",
        "partition_count": 1,
        "row_count": 1,
        "accepted_dates": ("2026-08-17",),
        "missing_dates": (),
        "records": (),
        "canonical_raw_read_required": False,
    }


def test_stage5_freeze_requires_content_attestation_marker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    with pytest.raises(ValueError, match="current content-attestation marker"):
        freeze_accepted_history(
            data_root=tmp_path,
            output_root=tmp_path / "run",
            repo_root=Path.cwd(),
            instrument_id="si_futures_family",
            start_date="2026-08-17",
            end_date="2026-08-17",
            run_id="missing_marker",
        )


def test_stage5_freeze_rejects_generation_snapshot_sha_mismatch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    resolved = _current_fixture(tmp_path)
    snapshot = tmp_path / "state" / "accepted_manifests" / "raw_history_content_attestation" / "generation_id=generation_current" / "raw" / "dataset_id=futures_futoi_raw" / "instrument_id=si_futures_family" / "trade_date=2026-08-17" / "part.parquet"
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    snapshot.write_bytes(b"fixture bytes intentionally not matching the attested digest")
    canonical = tmp_path / "market" / "supplementary" / "dataset_id=futures_futoi_raw" / "instrument_id=si_futures_family" / "trade_date=2026-08-17" / "source=moex_algopack_futoi" / "part.parquet"
    resolved["records"] = ({
        "trade_date": "2026-08-17",
        "sha256": "a" * 64,
        "row_count": 1,
        "canonical_ref": _rooted(tmp_path, canonical),
        "snapshot_ref": _rooted(tmp_path, snapshot),
        "snapshot_path": snapshot.as_posix(),
    },)
    monkeypatch.setattr(attestation, "resolve_content_attested_history", lambda **_: resolved)
    with pytest.raises(ValueError, match="snapshot SHA-256 differs from generation evidence"):
        freeze_accepted_history(
            data_root=tmp_path,
            output_root=tmp_path / "run",
            repo_root=Path.cwd(),
            instrument_id="si_futures_family",
            start_date="2026-08-17",
            end_date="2026-08-17",
            run_id="digest_mismatch",
        )


def test_stage5_acceptance_rejects_stale_generation_lineage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    resolved = _current_fixture(tmp_path, generation_id="generation_current")
    resolved["records"] = ()
    monkeypatch.setattr(attestation, "resolve_content_attested_history", lambda **_: resolved)
    current_manifest = Path(str(resolved["manifest_path"]))
    report_ref = json.loads(current_manifest.read_text(encoding="utf-8"))["content_attestation_report_ref"]
    manifest_values = {
        "accepted_raw_pointer_ref": _rooted(tmp_path, Path(str(resolved["marker_path"]))),
        "accepted_raw_manifest_ref": _rooted(tmp_path, current_manifest),
        "accepted_raw_acceptance_report_ref": report_ref,
        "accepted_raw_history_run_id": "generation_stale",
        "accepted_raw_partition_dates_sha256": _date_digest(["2026-08-17"]),
    }
    with pytest.raises(ValueError, match="accepted_raw_history_run_id"):
        acceptance_base._validate_eod_raw_lineage(manifest_values, "si_futures_family")


def test_stage5_acceptance_rejects_legacy_pointer_lineage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    resolved = _current_fixture(tmp_path)
    monkeypatch.setattr(attestation, "resolve_content_attested_history", lambda **_: resolved)
    current_manifest = Path(str(resolved["manifest_path"]))
    report_ref = json.loads(current_manifest.read_text(encoding="utf-8"))["content_attestation_report_ref"]
    legacy_pointer = tmp_path / "state" / "datasets" / "dataset_id=futures_futoi_raw" / "instrument_id=si_futures_family" / "current_accepted_manifest.json"
    legacy_pointer.parent.mkdir(parents=True, exist_ok=True)
    legacy_pointer.write_text('{"promotion_basis":"raw_history_acceptance"}\n', encoding="utf-8")
    manifest_values = {
        "accepted_raw_pointer_ref": _rooted(tmp_path, legacy_pointer),
        "accepted_raw_manifest_ref": _rooted(tmp_path, current_manifest),
        "accepted_raw_acceptance_report_ref": report_ref,
        "accepted_raw_history_run_id": resolved["generation_id"],
        "accepted_raw_partition_dates_sha256": _date_digest(["2026-08-17"]),
    }
    with pytest.raises(ValueError, match="accepted_raw_pointer_ref"):
        acceptance_base._validate_eod_raw_lineage(manifest_values, "si_futures_family")
