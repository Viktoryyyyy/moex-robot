from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data import step4_basis_carry_acceptance as acceptance
from moex_data import step4_basis_carry_pilot_runner as pilot


def _write_json(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values), encoding="utf-8")


def _bindings(front_expiry: str = "2026-09-17") -> list[dict[str, object]]:
    return [
        {"root": "Si", "role": "front", "instrument_id": "si_front_contract", "secid": "SiU6", "last_trade_date": front_expiry, "minimum_days_to_expiry": "1"},
        {"root": "Si", "role": "next", "instrument_id": "si_next_contract", "secid": "SiZ6", "last_trade_date": "2026-12-17", "minimum_days_to_expiry": "1"},
        {"root": "CR", "role": "front", "instrument_id": "cr_front_contract", "secid": "CRU6", "last_trade_date": front_expiry, "minimum_days_to_expiry": "1"},
        {"root": "CR", "role": "next", "instrument_id": "cr_next_contract", "secid": "CRZ6", "last_trade_date": "2026-12-17", "minimum_days_to_expiry": "1"},
    ]


def _write_partition(path: Path, instrument_id: str) -> None:
    base = 80.0 if instrument_id == "usd_rub_basis_carry" else 12.0
    frame = pd.DataFrame(
        {
            "instrument_id": [instrument_id, instrument_id],
            "trade_date": ["2026-08-24", "2026-08-24"],
            "ts": pd.to_datetime(["2026-08-24T07:00:00Z", "2026-08-24T07:05:00Z"]),
            "alignment_policy": ["exact_timestamp_inner_join", "exact_timestamp_inner_join"],
            "spot_rate": [base, base + 0.01],
            "perpetual_rate": [base + 0.10, base + 0.11],
            "front_rate": [base + 0.20, base + 0.21],
            "next_rate": [base + 0.40, base + 0.41],
        }
    )
    frame.to_parquet(path, index=False)


def _build_pilot_fixture(root: Path, run_id: str, *, front_expiry: str = "2026-09-17") -> None:
    run_root = root / "runs" / "step4_rub_basis_carry" / f"run_id={run_id}"
    run_root.mkdir(parents=True)
    outputs: list[dict[str, object]] = []
    for instrument_id in ("usd_rub_basis_carry", "cny_rub_basis_carry"):
        producer_run_id = f"{run_id}_{instrument_id}"
        base = run_root / instrument_id
        partition = base / "part.parquet"
        manifest = base / "manifest.json"
        quality = base / "quality.json"
        partition.parent.mkdir(parents=True, exist_ok=True)
        _write_partition(partition, instrument_id)
        quality_values = {
            "dataset_id": "rub_basis_carry_5m",
            "instrument_id": instrument_id,
            "run_id": producer_run_id,
            "row_count": 2,
            "quality_status": "pass",
            "timestamp_policy": "naive_exchange_localize_europe_moscow_then_utc",
        }
        _write_json(quality, quality_values)
        manifest_values = {
            "dataset_id": "rub_basis_carry_5m",
            "instrument_id": instrument_id,
            "run_id": producer_run_id,
            "row_count": 2,
            "quality_status": "pass",
            "partition_path": partition.as_posix(),
            "quality_report_path": quality.as_posix(),
            "timestamp_policy": "naive_exchange_localize_europe_moscow_then_utc",
        }
        _write_json(manifest, manifest_values)
        outputs.append(
            {
                "dataset_id": "rub_basis_carry_5m",
                "instrument_id": instrument_id,
                "run_id": producer_run_id,
                "row_count": 2,
                "quality_status": "pass",
                "partition_path": partition.as_posix(),
                "manifest_path": manifest.as_posix(),
                "quality_report_path": quality.as_posix(),
                "alignment_policy": "exact_timestamp_inner_join",
                "timestamp_policy": "naive_exchange_localize_europe_moscow_then_utc",
                "forward_fill_used": False,
                "asof_join_used": False,
                "continuous_series_used": False,
            }
        )
    evidence = {
        "project": "MOEX_Bot",
        "step": 4,
        "status": "pilot_passed",
        "trade_date": "2026-08-24",
        "artifact_version": run_id,
        "materialization_root": run_root.as_posix(),
        "run_artifacts_immutable": True,
        "run_id_reuse_allowed": False,
        "alignment_policy": "exact_timestamp_inner_join",
        "timestamp_policy": "naive_exchange_localize_europe_moscow_then_utc",
        "front_next_minimum_days_to_expiry": 1,
        "bindings": _bindings(front_expiry),
        "forward_fill_used": False,
        "asof_join_used": False,
        "latest_autodetect_used": False,
        "continuous_series_used": False,
        "counts": {
            "bindings": 4,
            "perpetual_quote_partitions": 2,
            "front_next_quote_partitions": 4,
            "tom_partitions": 2,
            "derived_partitions": 2,
        },
        "derived_partitions": outputs,
    }
    _write_json(
        root / "state" / "acceptance" / "step4_rub_basis_carry" / f"run_id={run_id}" / "pilot_evidence.json",
        evidence,
    )


def _pointer_path(root: Path, instrument_id: str) -> Path:
    return (
        root
        / "state"
        / "datasets"
        / "dataset_id=rub_basis_carry_5m"
        / f"instrument_id={instrument_id}"
        / "current_accepted_manifest.json"
    )


def test_accepted_pointer_run_id_matches_manifest_and_keeps_acceptance_run(monkeypatch, tmp_path: Path) -> None:
    run_id = "step4_fixture_v1"
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    _build_pilot_fixture(tmp_path, run_id)

    result = acceptance.promote(run_id=run_id)

    assert result["status"] == "accepted"
    assert result["accepted_pointer_count"] == 2
    assert result["physical_partition_readback_required"] is True
    for item in result["pointers"]:
        assert item["physical_readback"]["physical_readback_passed"] is True
    for instrument_id in ("usd_rub_basis_carry", "cny_rub_basis_carry"):
        pointer = json.loads(_pointer_path(tmp_path, instrument_id).read_text(encoding="utf-8"))
        assert pointer["run_id"] == f"{run_id}_{instrument_id}"
        assert pointer["acceptance_run_id"] == run_id


def test_acceptance_rejects_corrupted_parquet_before_any_pointer_write(monkeypatch, tmp_path: Path) -> None:
    run_id = "step4_corrupt_partition_fixture"
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    _build_pilot_fixture(tmp_path, run_id)
    corrupt_partition = (
        tmp_path
        / "runs"
        / "step4_rub_basis_carry"
        / f"run_id={run_id}"
        / "usd_rub_basis_carry"
        / "part.parquet"
    )
    corrupt_partition.write_bytes(b"not-a-parquet-file")

    with pytest.raises(acceptance.Step4AcceptanceError, match="physical derived partition validation failed"):
        acceptance.promote(run_id=run_id)

    assert not _pointer_path(tmp_path, "usd_rub_basis_carry").exists()
    assert not _pointer_path(tmp_path, "cny_rub_basis_carry").exists()


def test_acceptance_rejects_expiry_day_front_binding(monkeypatch, tmp_path: Path) -> None:
    run_id = "step4_expiry_day_fixture"
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    _build_pilot_fixture(tmp_path, run_id, front_expiry="2026-08-24")

    with pytest.raises(acceptance.Step4AcceptanceError, match="strictly after trade_date"):
        acceptance.promote(run_id=run_id)


def test_canonical_root_restoration_accepts_equivalent_trailing_slash(monkeypatch, tmp_path: Path) -> None:
    canonical = tmp_path.resolve()
    monkeypatch.setenv("MOEX_DATA_ROOT", canonical.as_posix() + "/")
    assert pilot._canonical_root_restored(canonical) is True
