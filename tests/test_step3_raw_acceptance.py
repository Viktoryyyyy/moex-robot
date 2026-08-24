from __future__ import annotations

import json
from pathlib import Path

import pytest

from moex_data import step3_raw_acceptance as acceptance


def _write_json(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values), encoding="utf-8")


def _producer_files(root: Path, name: str, *, refresh_status: bool) -> tuple[Path, Path, Path]:
    base = root / "producer" / name
    partition = base / "part.parquet"
    quality = base / "quality.json"
    manifest = base / "manifest.json"
    partition.parent.mkdir(parents=True, exist_ok=True)
    partition.write_bytes(b"physical-pilot-partition")
    _write_json(quality, {"quality_status": "pass"})
    if refresh_status:
        _write_json(manifest, {"run_id": name, "refresh_status": "succeeded"})
    else:
        _write_json(manifest, {"run_id": name, "status": "succeeded"})
    return partition, quality, manifest


def _pilot_evidence(root: Path, run_id: str, *, zero_quote_rows: bool = False) -> dict[str, object]:
    quote_rows = []
    oi_rows = []
    tom_rows = []
    role_pairs = (
        ("si_front_contract", "SiU6"),
        ("si_next_contract", "SiZ6"),
        ("cr_front_contract", "CRU6"),
        ("cr_next_contract", "CRZ6"),
    )
    for index, (instrument_id, secid) in enumerate(role_pairs):
        partition, quality, manifest = _producer_files(root, instrument_id + "_quote", refresh_status=True)
        quote_rows.append(
            {
                "quality_status": "pass",
                "row_count": 0 if zero_quote_rows and index == 0 else 10,
                "instrument_id_scope": [instrument_id],
                "secid_scope": [secid],
                "source_id": "moex_algopack_fo_tradestats_5m",
                "manifest_reference": manifest.as_posix(),
                "quality_report_reference": quality.as_posix(),
                "storage_partition_path": partition.as_posix(),
            }
        )
        oi_partition, oi_quality, oi_manifest = _producer_files(root, instrument_id + "_oi", refresh_status=False)
        oi_rows.append(
            {
                "dataset_id": "futures_open_interest_raw_5m",
                "quality_status": "pass",
                "row_count": 10,
                "instrument_id": instrument_id,
                "secid": secid,
                "source_id": "moex_algopack_fo_open_interest_5m",
                "manifest_path": oi_manifest.as_posix(),
                "quality_report_path": oi_quality.as_posix(),
                "partition_path": oi_partition.as_posix(),
            }
        )
    for instrument_id, secid in (("usd_tom", "USD000UTSTOM"), ("cny_tom", "CNYRUB_TOM")):
        partition, quality, manifest = _producer_files(root, instrument_id + "_quote", refresh_status=False)
        tom_rows.append(
            {
                "dataset_id": "fx_spot_raw_5m",
                "quality_status": "pass",
                "row_count": 10,
                "instrument_id": instrument_id,
                "secid": secid,
                "source_id": "moex_iss_cets_tom_1m",
                "manifest_path": manifest.as_posix(),
                "quality_report_path": quality.as_posix(),
                "partition_path": partition.as_posix(),
            }
        )
    return {
        "project": "MOEX_Bot",
        "step": 3,
        "status": "pilot_passed",
        "artifact_version": run_id,
        "latest_autodetect_used": False,
        "continuous_series_created": False,
        "counts": {
            "bindings": 4,
            "quote_partitions": 4,
            "open_interest_partitions": 4,
            "tom_partitions": 2,
        },
        "quote_partitions": quote_rows,
        "open_interest_partitions": oi_rows,
        "tom_partitions": tom_rows,
    }


def test_step3_acceptance_promotes_exactly_ten_canonical_pointers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_test_run"
    evidence_path = acceptance.pilot_evidence_path(run_id)
    _write_json(evidence_path, _pilot_evidence(tmp_path, run_id))

    result = acceptance.promote_step3_pilot(run_id=run_id)

    assert result["status"] == "accepted"
    assert result["accepted_pointer_count"] == 10
    assert result["expected_pointer_count"] == 10
    assert acceptance.acceptance_evidence_path(run_id).exists()
    assert len(result["pointers"]) == 10
    for item in result["pointers"]:
        assert Path(str(item["pointer_path"])).exists()
        assert str(item["pointer_ref"]).startswith("${MOEX_DATA_ROOT}/state/datasets/dataset_id=")


def test_step3_acceptance_fails_before_pointer_writes_on_bad_partition(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_bad_run"
    evidence_path = acceptance.pilot_evidence_path(run_id)
    _write_json(evidence_path, _pilot_evidence(tmp_path, run_id, zero_quote_rows=True))

    with pytest.raises(acceptance.Step3AcceptanceError, match="row_count must be positive"):
        acceptance.promote_step3_pilot(run_id=run_id)

    pointer_root = tmp_path / "state" / "datasets"
    assert not pointer_root.exists()
    assert not acceptance.acceptance_evidence_path(run_id).exists()
