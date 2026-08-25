from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import stage2_raw_history_acceptance as stage2
from moex_data.futures import stage2_raw_history_content_reattestation as content


def _expectation(dataset: str, instrument: str, trade_date: str = "2026-08-17") -> content.RepositoryExpectation:
    history = stage2.HistoryExpectation(
        target_dataset_id=dataset,
        instrument_id=instrument,
        source_id=stage2.QUOTE_SOURCE_ID if dataset == stage2.QUOTE_DATASET_ID else stage2.FUTOI_SOURCE_ID,
        date_start=trade_date,
        date_end=trade_date,
        expected_partitions=1,
        expected_rows=3,
        expected_secid="USDRUBF" if dataset == stage2.QUOTE_DATASET_ID else "SiU6",
        expected_source_ticker=None if dataset == stage2.QUOTE_DATASET_ID else "si",
        expected_missing_dates=0 if dataset == stage2.FUTOI_DATASET_ID else None,
    )
    return content.RepositoryExpectation(
        history=history,
        partition_dates_sha256=content._date_set_sha256((trade_date,)),
        missing_dates_sha256=content._date_set_sha256(()),
        missing_dates_count=0,
    )


def _legacy_state(tmp_path: Path, dataset: str, instrument: str) -> content.LegacyState:
    expected = _expectation(dataset, instrument)
    return content.LegacyState(
        dataset_id=dataset,
        instrument_id=instrument,
        pointer_path=tmp_path / "pointer.json",
        pointer_sha256="1" * 64,
        pointer_values={"run_id": "legacy_run"},
        manifest_path=tmp_path / "legacy_manifest.json",
        manifest_sha256="2" * 64,
        manifest_values={},
        report_path=tmp_path / "legacy_report.json",
        report_sha256="3" * 64,
        report_values={},
        expectation=expected,
        accepted_dates=("2026-08-17",),
        missing_dates=(),
    )


def test_content_set_sha_is_ordered_exact_serialization() -> None:
    records = [
        {"trade_date": "2026-08-14", "sha256": "a" * 64},
        {"trade_date": "2026-08-15", "sha256": "b" * 64},
    ]
    payload = (
        "2026-08-14\t" + "a" * 64 + "\n"
        + "2026-08-15\t" + "b" * 64 + "\n"
    ).encode("utf-8")
    assert content._content_set_sha256(records) == hashlib.sha256(payload).hexdigest()


def test_scope_is_exact_four_canonical_stage2_histories() -> None:
    assert content.EXPECTED_SCOPE == (
        (stage2.QUOTE_DATASET_ID, "usdrubf_futures_family"),
        (stage2.QUOTE_DATASET_ID, "cnyrubf_futures_family"),
        (stage2.FUTOI_DATASET_ID, "si_futures_family"),
        (stage2.FUTOI_DATASET_ID, "cr_futures_family"),
    )


def test_validated_snapshot_hashes_and_hardlinks_same_inode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    dataset = stage2.QUOTE_DATASET_ID
    instrument = "usdrubf_futures_family"
    state = _legacy_state(tmp_path, dataset, instrument)
    canonical = tmp_path / "market" / "part.parquet"
    canonical.parent.mkdir(parents=True)
    pd.DataFrame({"marker": [1, 2, 3]}).to_parquet(canonical, index=False)
    expected_sha = hashlib.sha256(canonical.read_bytes()).hexdigest()
    monkeypatch.setattr(content, "_canonical_partition_path", lambda *args, **kwargs: canonical)
    monkeypatch.setattr(content.stage2, "_validate_quote_partition", lambda *args, **kwargs: (3, ("USDRUBF",)))
    snapshot = tmp_path / "generation" / "raw" / "part.parquet"

    record = content._open_validated_snapshot(
        repo_root=tmp_path,
        state=state,
        trade_date="2026-08-17",
        snapshot_path=snapshot,
        validation_run_id="fixture_validation",
    )

    assert record["sha256"] == expected_sha
    assert record["row_count"] == 3
    assert record["secids"] == ["USDRUBF"]
    assert snapshot.stat().st_ino == canonical.stat().st_ino


def test_recheck_rejects_previously_missing_date_that_appeared(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    state = _legacy_state(tmp_path, stage2.QUOTE_DATASET_ID, "usdrubf_futures_family")
    state = content.LegacyState(**{**state.__dict__, "accepted_dates": (), "missing_dates": ("2026-08-17",)})
    canonical = tmp_path / "market" / "new.parquet"
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(b"new")
    monkeypatch.setattr(content, "_canonical_partition_path", lambda *args, **kwargs: canonical)
    prepared = content.PreparedInstrument(
        dataset_id=state.dataset_id,
        instrument_id=state.instrument_id,
        manifest_path=tmp_path / "m.json",
        manifest_sha256="4" * 64,
        report_path=tmp_path / "r.json",
        report_sha256="5" * 64,
        content_set_sha256=hashlib.sha256(b"").hexdigest(),
        records=(),
        missing_dates=("2026-08-17",),
    )
    with pytest.raises(content.ContentReattestationError, match="previously missing partition appeared"):
        content._recheck_before_publication(tmp_path, (state,), (prepared,))


def test_publish_marker_is_single_atomic_canonical_switch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    generation_root = (
        tmp_path
        / "state"
        / "accepted_manifests"
        / "raw_history_content_attestation"
        / "generation_id=new"
    )
    generation_root.mkdir(parents=True)
    (generation_root / "proof.txt").write_text("ready\n", encoding="utf-8")
    marker_path = content._current_marker_path()
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text('{"generation_id":"old"}\n', encoding="utf-8")
    marker = {"schema_version": content.MARKER_SCHEMA, "generation_id": "new", "status": "accepted"}
    sha = content._publish_marker(marker)
    raw = marker_path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == sha
    assert json.loads(raw)["generation_id"] == "new"
    assert not list(marker_path.parent.glob("*.stage"))


def test_resolver_verifies_marker_manifest_report_and_snapshot_sha(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    dataset = stage2.QUOTE_DATASET_ID
    instrument = "usdrubf_futures_family"
    expected = _expectation(dataset, instrument)
    expected = content.RepositoryExpectation(
        history=stage2.HistoryExpectation(
            target_dataset_id=dataset,
            instrument_id=instrument,
            source_id=stage2.QUOTE_SOURCE_ID,
            date_start="2026-08-17",
            date_end="2026-08-17",
            expected_partitions=1,
            expected_rows=1,
            expected_secid="USDRUBF",
        ),
        partition_dates_sha256=content._date_set_sha256(("2026-08-17",)),
        missing_dates_sha256=content._date_set_sha256(()),
        missing_dates_count=0,
    )
    monkeypatch.setattr(content, "_repo_expectation", lambda *args, **kwargs: expected)

    generation = tmp_path / "state" / "accepted_manifests" / "raw_history_content_attestation" / "generation_id=fixture"
    snapshot = generation / "raw" / "dataset_id=futures_raw_5m" / "instrument_id=usdrubf_futures_family" / "trade_date=2026-08-17" / "part.parquet"
    snapshot.parent.mkdir(parents=True)
    snapshot.write_bytes(b"exact-snapshot-bytes")
    snapshot_sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    record = {
        "trade_date": "2026-08-17",
        "sha256": snapshot_sha,
        "row_count": 1,
        "secids": ["USDRUBF"],
        "canonical_ref": "${MOEX_DATA_ROOT}/market/unused.parquet",
        "snapshot_ref": content._rooted_ref(snapshot),
        "source_device": 1,
        "source_inode": 1,
        "source_size": len(snapshot.read_bytes()),
        "source_mtime_ns": 1,
    }
    content_sha = content._content_set_sha256((record,))
    report_path = generation / "reports" / "dataset_id=futures_raw_5m" / "instrument_id=usdrubf_futures_family" / "content_attestation_report.json"
    report_path.parent.mkdir(parents=True)
    report = {
        "dataset_id": dataset,
        "instrument_id": instrument,
        "source_id": stage2.QUOTE_SOURCE_ID,
        "requested_from": "2026-08-17",
        "requested_till": "2026-08-17",
        "partition_count": 1,
        "row_count": 1,
        "partition_dates_sha256": expected.partition_dates_sha256,
        "missing_partition_dates": [],
        "missing_dates_sha256": expected.missing_dates_sha256,
        "partition_content_records": [record],
        "partition_content_set_sha256": content_sha,
    }
    report_path.write_text(json.dumps(report, sort_keys=True), encoding="utf-8")
    report_sha = hashlib.sha256(report_path.read_bytes()).hexdigest()
    manifest_path = generation / "manifests" / "dataset_id=futures_raw_5m" / "instrument_id=usdrubf_futures_family" / "accepted_manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest = {
        **report,
        "content_attestation_report_ref": content._rooted_ref(report_path),
        "content_attestation_report_sha256": report_sha,
    }
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    selected_entry = {
        "dataset_id": dataset,
        "instrument_id": instrument,
        "manifest_ref": content._rooted_ref(manifest_path),
        "manifest_sha256": manifest_sha,
        "report_ref": content._rooted_ref(report_path),
        "report_sha256": report_sha,
        "partition_content_set_sha256": content_sha,
    }
    entries = [selected_entry]
    for other_dataset, other_instrument in content.EXPECTED_SCOPE[1:]:
        entries.append({
            "dataset_id": other_dataset,
            "instrument_id": other_instrument,
            "manifest_ref": "${MOEX_DATA_ROOT}/unused",
            "manifest_sha256": "0" * 64,
            "report_ref": "${MOEX_DATA_ROOT}/unused",
            "report_sha256": "0" * 64,
            "partition_content_set_sha256": "0" * 64,
        })
    marker_path = content._current_marker_path()
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(json.dumps({
        "schema_version": content.MARKER_SCHEMA,
        "producer": content.PRODUCER,
        "status": "accepted",
        "generation_id": "fixture",
        "entries": entries,
    }), encoding="utf-8")

    resolved = content.resolve_content_attested_history(dataset_id=dataset, instrument_id=instrument, repo_root=tmp_path)
    assert resolved["partition_count"] == 1
    assert resolved["records"][0]["snapshot_path"] == snapshot.as_posix()
    assert resolved["canonical_raw_read_required"] is False

    snapshot.write_bytes(b"tampered")
    with pytest.raises(content.ContentReattestationError, match="snapshot bytes mismatch"):
        content.resolve_content_attested_history(dataset_id=dataset, instrument_id=instrument, repo_root=tmp_path)
