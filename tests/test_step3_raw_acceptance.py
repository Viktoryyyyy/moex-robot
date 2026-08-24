from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data import step3_raw_acceptance as acceptance
from moex_data import step3_raw_pilot_runner as pilot_runner

TRADE_DATE = "2026-08-24"
REFERENCE_TS = "2026-08-24T09:00:00+00:00"
QUOTE_SOURCE = "moex_algopack_fo_tradestats_5m"
OI_SOURCE = "moex_algopack_fo_open_interest_5m"
TOM_SOURCE = "moex_iss_cets_tom_1m"


def _write_json(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values), encoding="utf-8")


def _write_oi_parquet(path: Path, row_count: int = 10, *, include_systime: bool = True) -> tuple[str, str]:
    availability = pd.date_range("2026-08-24T06:00:00Z", periods=row_count, freq="5min")
    values: dict[str, object] = {"availability_ts_utc": availability}
    if include_systime:
        values["systime_source"] = [value.tz_convert("Europe/Moscow").isoformat() for value in availability]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(values).to_parquet(path, index=False)
    return availability.min().isoformat(), availability.max().isoformat()


def _producer_files(
    run_root: Path,
    name: str,
    *,
    dataset_id: str,
    instrument_id: str,
    secid: str,
    source_id: str,
    row_count: int = 10,
) -> tuple[Path, Path, Path]:
    base = run_root / "producer" / name
    partition = base / "part.parquet"
    quality = base / "quality.json"
    manifest = base / "manifest.json"
    partition.parent.mkdir(parents=True, exist_ok=True)

    availability_bounds: tuple[str, str] | None = None
    if dataset_id == "futures_open_interest_raw_5m":
        availability_bounds = _write_oi_parquet(partition, row_count)
    else:
        partition.write_bytes(b"physical-pilot-partition")

    if dataset_id == "futures_raw_5m":
        _write_json(
            quality,
            {
                "run_id": name,
                "rows": [{
                    "run_id": name,
                    "dataset_id": dataset_id,
                    "instrument_id": instrument_id,
                    "source_id": source_id,
                    "secid": secid,
                    "trade_date": TRADE_DATE,
                    "rows": row_count,
                    "quality_status": "pass",
                }],
            },
        )
        _write_json(
            manifest,
            {
                "run_id": name,
                "refresh_status": "succeeded",
                "instrument_scope": [instrument_id],
                "source_scope": [source_id],
                "partitions_written": [partition.as_posix()],
                "quality_report_ref": quality.as_posix(),
                "source_contract": {
                    "instrument_id": instrument_id,
                    "source_id": source_id,
                    "secid": secid,
                    "trade_date": TRADE_DATE,
                },
            },
        )
    else:
        quality_values: dict[str, object] = {
            "dataset_id": dataset_id,
            "run_id": name,
            "instrument_id": instrument_id,
            "source_id": source_id,
            "secid": secid,
            "trade_date": TRADE_DATE,
            "partition_path": partition.as_posix(),
            "rows": row_count,
            "quality_status": "pass",
        }
        manifest_values: dict[str, object] = {
            "dataset_id": dataset_id,
            "run_id": name,
            "status": "succeeded",
            "instrument_id": instrument_id,
            "source_id": source_id,
            "secid": secid,
            "trade_date": TRADE_DATE,
            "row_count": row_count,
            "partition_path": partition.as_posix(),
            "quality_report_path": quality.as_posix(),
        }
        if availability_bounds is not None:
            quality_values["min_availability_ts_utc"] = availability_bounds[0]
            quality_values["max_availability_ts_utc"] = availability_bounds[1]
            manifest_values["min_availability_ts_utc"] = availability_bounds[0]
            manifest_values["max_availability_ts_utc"] = availability_bounds[1]
        _write_json(quality, quality_values)
        _write_json(manifest, manifest_values)
    return partition, quality, manifest


def _pilot_evidence(root: Path, run_id: str, *, zero_quote_rows: bool = False) -> dict[str, object]:
    run_root = root / "runs" / "step3_canonical_raw" / ("run_id=" + run_id)
    run_root.mkdir(parents=True, exist_ok=True)
    quote_rows: list[dict[str, object]] = []
    oi_rows: list[dict[str, object]] = []
    tom_rows: list[dict[str, object]] = []
    bindings: list[dict[str, object]] = []
    role_rows = (
        ("Si", "front", "si_front_contract", "SiU6", "2026-09-17"),
        ("Si", "next", "si_next_contract", "SiZ6", "2026-12-17"),
        ("CR", "front", "cr_front_contract", "CRU6", "2026-09-17"),
        ("CR", "next", "cr_next_contract", "CRZ6", "2026-12-17"),
    )
    for index, (root_name, role, instrument_id, secid, last_trade_date) in enumerate(role_rows):
        bindings.append({
            "root": root_name,
            "role": role,
            "instrument_id": instrument_id,
            "secid": secid,
            "as_of_date": TRADE_DATE,
            "last_trade_date": last_trade_date,
            "source_id": "moex_iss_forts_securities_reference",
            "mapping_fixed_ts_utc": REFERENCE_TS,
            "availability_ts_utc": REFERENCE_TS,
        })
        partition, quality, manifest = _producer_files(
            run_root, instrument_id + "_quote", dataset_id="futures_raw_5m",
            instrument_id=instrument_id, secid=secid, source_id=QUOTE_SOURCE,
        )
        quote_rows.append({
            "status": "succeeded",
            "dataset_id": "futures_raw_5m",
            "quality_status": "pass",
            "row_count": 0 if zero_quote_rows and index == 0 else 10,
            "instrument_id_scope": [instrument_id],
            "secid_scope": [secid],
            "source_id": QUOTE_SOURCE,
            "manifest_reference": manifest.as_posix(),
            "quality_report_reference": quality.as_posix(),
            "storage_partition_path": partition.as_posix(),
        })
        oi_partition, oi_quality, oi_manifest = _producer_files(
            run_root, instrument_id + "_oi", dataset_id="futures_open_interest_raw_5m",
            instrument_id=instrument_id, secid=secid, source_id=OI_SOURCE,
        )
        oi_rows.append({
            "dataset_id": "futures_open_interest_raw_5m",
            "quality_status": "pass",
            "row_count": 10,
            "trade_date": TRADE_DATE,
            "instrument_id": instrument_id,
            "secid": secid,
            "source_id": OI_SOURCE,
            "manifest_path": oi_manifest.as_posix(),
            "quality_report_path": oi_quality.as_posix(),
            "partition_path": oi_partition.as_posix(),
        })

    for instrument_id, secid in (("usd_tom", "USD000UTSTOM"), ("cny_tom", "CNYRUB_TOM")):
        partition, quality, manifest = _producer_files(
            run_root, instrument_id + "_quote", dataset_id="fx_spot_raw_5m",
            instrument_id=instrument_id, secid=secid, source_id=TOM_SOURCE,
        )
        tom_rows.append({
            "dataset_id": "fx_spot_raw_5m",
            "quality_status": "pass",
            "row_count": 10,
            "trade_date": TRADE_DATE,
            "instrument_id": instrument_id,
            "secid": secid,
            "source_id": TOM_SOURCE,
            "manifest_path": manifest.as_posix(),
            "quality_report_path": quality.as_posix(),
            "partition_path": partition.as_posix(),
        })

    return {
        "project": "MOEX_Bot",
        "step": 3,
        "status": "pilot_passed",
        "trade_date": TRADE_DATE,
        "as_of_date": TRADE_DATE,
        "artifact_version": run_id,
        "materialization_root": run_root.as_posix(),
        "materialization_root_ref": "${MOEX_DATA_ROOT}/runs/step3_canonical_raw/run_id=" + run_id,
        "run_artifacts_immutable": True,
        "run_id_reuse_allowed": False,
        "reference_observed_at_utc": REFERENCE_TS,
        "bindings": bindings,
        "latest_autodetect_used": False,
        "historical_backdating_used": False,
        "continuous_series_created": False,
        "counts": {"bindings": 4, "quote_partitions": 4, "open_interest_partitions": 4, "tom_partitions": 2},
        "quote_partitions": quote_rows,
        "open_interest_partitions": oi_rows,
        "tom_partitions": tom_rows,
    }


def _write_pilot(root: Path, run_id: str) -> dict[str, object]:
    evidence = _pilot_evidence(root, run_id)
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)
    return evidence


def test_step3_acceptance_promotes_exactly_ten_canonical_pointers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_good"
    _write_pilot(tmp_path, run_id)
    result = acceptance.promote_step3_pilot(run_id=run_id)
    assert result["status"] == "accepted"
    assert result["accepted_pointer_count"] == 10
    assert result["promotion_semantics"] == "transactional_with_rollback"
    assert result["artifact_semantics"] == "immutable_run_scoped"
    assert acceptance.acceptance_evidence_path(run_id).exists()


def test_step3_acceptance_rejects_zero_rows_before_pointer_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_zero"
    _write_json(acceptance.pilot_evidence_path(run_id), _pilot_evidence(tmp_path, run_id, zero_quote_rows=True))
    with pytest.raises(acceptance.Step3AcceptanceError, match="positive"):
        acceptance.promote_step3_pilot(run_id=run_id)
    assert not (tmp_path / "state" / "datasets").exists()


def test_step3_acceptance_rejects_missing_binding_availability(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_binding")
    evidence["bindings"][0].pop("availability_ts_utc")
    _write_json(acceptance.pilot_evidence_path("bad_binding"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="binding.availability_ts_utc is required"):
        acceptance.promote_step3_pilot(run_id="bad_binding")


def test_step3_acceptance_rejects_wrong_moscow_observation_date(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_observation_date")
    evidence["reference_observed_at_utc"] = "2026-08-23T20:00:00+00:00"
    _write_json(acceptance.pilot_evidence_path("bad_observation_date"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="Europe/Moscow date must equal as_of_date"):
        acceptance.promote_step3_pilot(run_id="bad_observation_date")


def test_step3_acceptance_rejects_bad_binding_root_pattern(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_secid")
    evidence["bindings"][0]["secid"] = "CRU6"
    _write_json(acceptance.pilot_evidence_path("bad_secid"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="root/month pattern"):
        acceptance.promote_step3_pilot(run_id="bad_secid")


def test_step3_acceptance_rejects_reversed_front_next_expiry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_expiry")
    evidence["bindings"][0]["last_trade_date"] = "2027-03-18"
    _write_json(acceptance.pilot_evidence_path("bad_expiry"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="strictly earlier"):
        acceptance.promote_step3_pilot(run_id="bad_expiry")


def test_step3_acceptance_rejects_noncanonical_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_source")
    evidence["quote_partitions"][0]["source_id"] = "legacy_source"
    _write_json(acceptance.pilot_evidence_path("bad_source"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="canonical Step 3 source"):
        acceptance.promote_step3_pilot(run_id="bad_source")


def test_step3_acceptance_rejects_artifact_outside_run_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_root")
    external = tmp_path / "outside" / "part.parquet"
    external.parent.mkdir(parents=True, exist_ok=True)
    external.write_bytes(b"outside")
    evidence["quote_partitions"][0]["storage_partition_path"] = external.as_posix()
    _write_json(acceptance.pilot_evidence_path("bad_root"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="immutable run root"):
        acceptance.promote_step3_pilot(run_id="bad_root")


def test_step3_acceptance_rejects_quote_quality_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_quote_quality")
    quality_path = Path(str(evidence["quote_partitions"][0]["quality_report_reference"]))
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    quality["rows"][0]["quality_status"] = "fail"
    _write_json(quality_path, quality)
    _write_json(acceptance.pilot_evidence_path("bad_quote_quality"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="quality_status"):
        acceptance.promote_step3_pilot(run_id="bad_quote_quality")


def test_step3_acceptance_rejects_missing_supplementary_quality_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_supp_source")
    quality_path = Path(str(evidence["open_interest_partitions"][0]["quality_report_path"]))
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    quality.pop("source_id")
    _write_json(quality_path, quality)
    _write_json(acceptance.pilot_evidence_path("bad_supp_source"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="source_id"):
        acceptance.promote_step3_pilot(run_id="bad_supp_source")


def test_step3_acceptance_rejects_mismatched_supplementary_partition(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_supp_partition")
    row = evidence["tom_partitions"][0]
    quality_path = Path(str(row["quality_report_path"]))
    unrelated = tmp_path / "runs" / "step3_canonical_raw" / "run_id=bad_supp_partition" / "unrelated" / "part.parquet"
    unrelated.parent.mkdir(parents=True, exist_ok=True)
    unrelated.write_bytes(b"unrelated")
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    quality["partition_path"] = unrelated.as_posix()
    _write_json(quality_path, quality)
    _write_json(acceptance.pilot_evidence_path("bad_supp_partition"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="partition_path mismatch"):
        acceptance.promote_step3_pilot(run_id="bad_supp_partition")


def test_step3_acceptance_rejects_missing_oi_availability_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "missing_oi_availability")
    quality_path = Path(str(evidence["open_interest_partitions"][0]["quality_report_path"]))
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    quality.pop("min_availability_ts_utc")
    _write_json(quality_path, quality)
    _write_json(acceptance.pilot_evidence_path("missing_oi_availability"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="min_availability_ts_utc is required"):
        acceptance.promote_step3_pilot(run_id="missing_oi_availability")


def test_step3_acceptance_rejects_missing_oi_parquet_causal_column(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "missing_oi_column")
    partition = Path(str(evidence["open_interest_partitions"][0]["partition_path"]))
    _write_oi_parquet(partition, include_systime=False)
    _write_json(acceptance.pilot_evidence_path("missing_oi_column"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="causal Parquet validation failed"):
        acceptance.promote_step3_pilot(run_id="missing_oi_column")


def test_step3_acceptance_rejects_oi_parquet_bounds_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _pilot_evidence(tmp_path, "bad_oi_bounds")
    row = evidence["open_interest_partitions"][0]
    quality_path = Path(str(row["quality_report_path"]))
    manifest_path = Path(str(row["manifest_path"]))
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    replacement = "2026-08-24T07:00:00+00:00"
    quality["min_availability_ts_utc"] = replacement
    manifest["min_availability_ts_utc"] = replacement
    _write_json(quality_path, quality)
    _write_json(manifest_path, manifest)
    _write_json(acceptance.pilot_evidence_path("bad_oi_bounds"), evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="Parquet availability bounds mismatch"):
        acceptance.promote_step3_pilot(run_id="bad_oi_bounds")


def test_step3_acceptance_rolls_back_partial_pointer_replacement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "rollback"
    _write_pilot(tmp_path, run_id)
    old_pointer = tmp_path / "state" / "datasets" / "dataset_id=futures_raw_5m" / "instrument_id=si_front_contract" / "current_accepted_manifest.json"
    _write_json(old_pointer, {"sentinel": "old"})
    original_replace = acceptance._replace_staged
    calls = {"count": 0}

    def fail_after_two(staged_path: Path, final_path: Path) -> None:
        calls["count"] += 1
        if calls["count"] == 3:
            raise OSError("simulated promotion failure")
        original_replace(staged_path, final_path)

    monkeypatch.setattr(acceptance, "_replace_staged", fail_after_two)
    with pytest.raises(acceptance.Step3AcceptanceError, match="transaction failed"):
        acceptance.promote_step3_pilot(run_id=run_id)
    assert json.loads(old_pointer.read_text(encoding="utf-8")) == {"sentinel": "old"}
    assert not acceptance.acceptance_evidence_path(run_id).exists()


def test_step3_pilot_rejects_trade_date_different_from_as_of_before_source_access() -> None:
    with pytest.raises(pilot_runner.Step3PilotError, match="trade_date must equal as_of_date"):
        pilot_runner.run_pilot(trade_date="2026-08-21", as_of_date=TRADE_DATE, artifact_version="mismatch")


def test_step3_pilot_run_root_is_immutable_and_run_id_cannot_be_reused(tmp_path: Path) -> None:
    first = pilot_runner._reserve_run_root(tmp_path, "immutable_run")
    assert first == tmp_path / "runs" / "step3_canonical_raw" / "run_id=immutable_run"
    with pytest.raises(pilot_runner.Step3PilotError, match="run_id is immutable and cannot be reused"):
        pilot_runner._reserve_run_root(tmp_path, "immutable_run")
