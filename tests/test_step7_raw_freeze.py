from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import freeze_step7_accepted_raw_5m as freeze


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _attested_fixture(monkeypatch, tmp_path: Path, *, expected_sha: str | None = None) -> tuple[Path, Path, dict[str, object]]:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    instrument = "usdrubf_futures_family"
    trade_date = "2026-08-17"

    snapshot = tmp_path / "state" / "accepted_manifests" / "raw_history_content_attestation" / "generation_id=g1" / "raw" / "dataset_id=futures_raw_5m" / f"instrument_id={instrument}" / f"trade_date={trade_date}" / "part.parquet"
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"marker": [1]}).to_parquet(snapshot, index=False)
    snapshot_sha = expected_sha or _sha(snapshot)

    canonical = tmp_path / "market" / "raw" / "timeframe=5m" / f"instrument_id={instrument}" / f"trade_date={trade_date}" / "source=moex_algopack_fo_tradestats_5m" / "part.parquet"
    canonical.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"marker": [999]}).to_parquet(canonical, index=False)

    report = tmp_path / "state" / "accepted_manifests" / "raw_history_content_attestation" / "generation_id=g1" / "reports" / "dataset_id=futures_raw_5m" / f"instrument_id={instrument}" / "content_attestation_report.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("{}\n", encoding="utf-8")
    manifest = tmp_path / "state" / "accepted_manifests" / "raw_history_content_attestation" / "generation_id=g1" / "manifests" / "dataset_id=futures_raw_5m" / f"instrument_id={instrument}" / "accepted_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    report_ref = freeze.ROOT_PREFIX + report.relative_to(tmp_path).as_posix()
    date_sha = hashlib.sha256((trade_date + "\n").encode()).hexdigest()
    content_sha = hashlib.sha256((trade_date + "\t" + snapshot_sha + "\n").encode()).hexdigest()
    manifest.write_text(json.dumps({
        "partition_dates_sha256": date_sha,
        "content_attestation_report_ref": report_ref,
    }), encoding="utf-8")
    marker = tmp_path / "state" / "accepted_manifests" / "raw_history_content_attestation" / "current_batch.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("{}\n", encoding="utf-8")

    resolved = {
        "dataset_id": "futures_raw_5m",
        "instrument_id": instrument,
        "generation_id": "g1",
        "marker_path": marker.as_posix(),
        "marker_sha256": "a" * 64,
        "manifest_path": manifest.as_posix(),
        "manifest_sha256": "b" * 64,
        "partition_content_set_sha256": content_sha,
        "requested_from": trade_date,
        "requested_till": trade_date,
        "partition_count": 1,
        "row_count": 1,
        "accepted_dates": (trade_date,),
        "missing_dates": (),
        "records": ({
            "trade_date": trade_date,
            "sha256": snapshot_sha,
            "row_count": 1,
            "snapshot_path": snapshot.as_posix(),
            "snapshot_ref": freeze.ROOT_PREFIX + snapshot.relative_to(tmp_path).as_posix(),
            "canonical_ref": freeze.ROOT_PREFIX + canonical.relative_to(tmp_path).as_posix(),
        },),
    }
    monkeypatch.setattr(freeze.content_attestation, "resolve_content_attested_history", lambda **kwargs: resolved)
    monkeypatch.setattr(freeze.stage2, "_validate_quote_partition", lambda *args, **kwargs: (1, ("USDRUBF",)))
    return snapshot, canonical, resolved


def test_freeze_uses_content_attested_snapshot_not_mutable_canonical(monkeypatch, tmp_path: Path) -> None:
    snapshot, canonical, resolved = _attested_fixture(monkeypatch, tmp_path)
    run_root = tmp_path / "runs" / "step7_rub_native_d1_w1" / "run_id=fixture"
    result = freeze.freeze_accepted_quote_history(
        repo_root=tmp_path,
        data_root=tmp_path,
        run_root=run_root,
        instrument_id="usdrubf_futures_family",
        start_date="2026-08-17",
        end_date="2026-08-17",
        run_id="fixture_usdrub",
    )
    frozen = tmp_path / str(result["partitions"][0]["frozen_ref"])[len(freeze.ROOT_PREFIX):]
    assert _sha(frozen) == _sha(snapshot)
    assert _sha(frozen) != _sha(canonical)
    assert result["source_mode"] == "stage2_content_attested_generation_snapshots_only"
    assert result["legacy_pointer_consumption_used"] is False
    assert result["content_attestation_generation_id"] == resolved["generation_id"]
    assert result["frozen_content_sha256"] == resolved["partition_content_set_sha256"]


def test_freeze_rejects_content_attested_snapshot_sha_mismatch(monkeypatch, tmp_path: Path) -> None:
    _attested_fixture(monkeypatch, tmp_path, expected_sha="0" * 64)
    with pytest.raises(ValueError, match="snapshot SHA-256 differs"):
        freeze.freeze_accepted_quote_history(
            repo_root=tmp_path,
            data_root=tmp_path,
            run_root=tmp_path / "runs" / "step7_rub_native_d1_w1" / "run_id=badsha",
            instrument_id="usdrubf_futures_family",
            start_date="2026-08-17",
            end_date="2026-08-17",
            run_id="badsha_usdrub",
        )


def test_stage7_rejects_non_exact_content_attested_history_range(monkeypatch, tmp_path: Path) -> None:
    _attested_fixture(monkeypatch, tmp_path)
    with pytest.raises(ValueError, match="exact full content-attested"):
        freeze.accepted_quote_history(
            tmp_path,
            "usdrubf_futures_family",
            "2026-08-16",
            "2026-08-17",
            repo_root=tmp_path,
        )
