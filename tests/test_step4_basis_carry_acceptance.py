from __future__ import annotations

import json
from pathlib import Path

from moex_data import step4_basis_carry_acceptance as acceptance
from moex_data import step4_basis_carry_pilot_runner as pilot


def _write_json(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values), encoding="utf-8")


def _build_pilot_fixture(root: Path, run_id: str) -> None:
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
        partition.write_bytes(b"fixture")
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
        "artifact_version": run_id,
        "materialization_root": run_root.as_posix(),
        "run_artifacts_immutable": True,
        "run_id_reuse_allowed": False,
        "alignment_policy": "exact_timestamp_inner_join",
        "timestamp_policy": "naive_exchange_localize_europe_moscow_then_utc",
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


def test_accepted_pointer_run_id_matches_manifest_and_keeps_acceptance_run(monkeypatch, tmp_path: Path) -> None:
    run_id = "step4_fixture_v1"
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    _build_pilot_fixture(tmp_path, run_id)

    result = acceptance.promote(run_id=run_id)

    assert result["status"] == "accepted"
    assert result["accepted_pointer_count"] == 2
    for instrument_id in ("usd_rub_basis_carry", "cny_rub_basis_carry"):
        pointer_path = (
            tmp_path
            / "state"
            / "datasets"
            / "dataset_id=rub_basis_carry_5m"
            / f"instrument_id={instrument_id}"
            / "current_accepted_manifest.json"
        )
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        assert pointer["run_id"] == f"{run_id}_{instrument_id}"
        assert pointer["acceptance_run_id"] == run_id


def test_canonical_root_restoration_accepts_equivalent_trailing_slash(monkeypatch, tmp_path: Path) -> None:
    canonical = tmp_path.resolve()
    monkeypatch.setenv("MOEX_DATA_ROOT", canonical.as_posix() + "/")
    assert pilot._canonical_root_restored(canonical) is True
