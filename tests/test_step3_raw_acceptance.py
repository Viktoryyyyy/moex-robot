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


def _write_oi_parquet(path: Path, *, include_systime: bool = True) -> tuple[str, str]:
    availability = pd.date_range("2026-08-24T06:00:00Z", periods=10, freq="5min")
    values: dict[str, object] = {"availability_ts_utc": availability}
    if include_systime:
        values["systime_source"] = [value.tz_convert("Europe/Moscow").isoformat() for value in availability]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(values).to_parquet(path, index=False)
    return availability.min().isoformat(), availability.max().isoformat()


def _producer_files(run_root: Path, name: str, dataset_id: str, instrument_id: str, secid: str, source_id: str) -> tuple[Path, Path, Path]:
    base = run_root / "producer" / name
    partition, quality, manifest = base / "part.parquet", base / "quality.json", base / "manifest.json"
    base.mkdir(parents=True, exist_ok=True)
    bounds: tuple[str, str] | None = None
    if dataset_id == "futures_open_interest_raw_5m":
        bounds = _write_oi_parquet(partition)
    else:
        partition.write_bytes(b"physical-pilot-partition")

    if dataset_id == "futures_raw_5m":
        _write_json(quality, {"run_id": name, "rows": [{
            "run_id": name, "dataset_id": dataset_id, "instrument_id": instrument_id,
            "source_id": source_id, "secid": secid, "trade_date": TRADE_DATE,
            "rows": 10, "quality_status": "pass",
        }]})
        _write_json(manifest, {
            "run_id": name, "refresh_status": "succeeded", "instrument_scope": [instrument_id],
            "source_scope": [source_id], "partitions_written": [partition.as_posix()],
            "quality_report_ref": quality.as_posix(),
            "source_contract": {"instrument_id": instrument_id, "source_id": source_id, "secid": secid, "trade_date": TRADE_DATE},
        })
    else:
        q: dict[str, object] = {
            "dataset_id": dataset_id, "run_id": name, "instrument_id": instrument_id,
            "source_id": source_id, "secid": secid, "trade_date": TRADE_DATE,
            "partition_path": partition.as_posix(), "rows": 10, "quality_status": "pass",
        }
        m: dict[str, object] = {
            "dataset_id": dataset_id, "run_id": name, "status": "succeeded", "instrument_id": instrument_id,
            "source_id": source_id, "secid": secid, "trade_date": TRADE_DATE, "row_count": 10,
            "partition_path": partition.as_posix(), "quality_report_path": quality.as_posix(),
        }
        if bounds:
            q.update(min_availability_ts_utc=bounds[0], max_availability_ts_utc=bounds[1])
            m.update(min_availability_ts_utc=bounds[0], max_availability_ts_utc=bounds[1])
        _write_json(quality, q)
        _write_json(manifest, m)
    return partition, quality, manifest


def _evidence(root: Path, run_id: str) -> dict[str, object]:
    run_root = root / "runs" / "step3_canonical_raw" / ("run_id=" + run_id)
    run_root.mkdir(parents=True, exist_ok=True)
    bindings: list[dict[str, object]] = []
    quotes: list[dict[str, object]] = []
    ois: list[dict[str, object]] = []
    roles = (
        ("Si", "front", "si_front_contract", "SiU6", "2026-09-17"),
        ("Si", "next", "si_next_contract", "SiZ6", "2026-12-17"),
        ("CR", "front", "cr_front_contract", "CRU6", "2026-09-17"),
        ("CR", "next", "cr_next_contract", "CRZ6", "2026-12-17"),
    )
    for root_name, role, instrument_id, secid, expiry in roles:
        bindings.append({
            "root": root_name, "role": role, "instrument_id": instrument_id, "secid": secid,
            "as_of_date": TRADE_DATE, "last_trade_date": expiry,
            "source_id": "moex_iss_forts_securities_reference",
            "mapping_fixed_ts_utc": REFERENCE_TS, "availability_ts_utc": REFERENCE_TS,
        })
        part, quality, manifest = _producer_files(run_root, instrument_id + "_quote", "futures_raw_5m", instrument_id, secid, QUOTE_SOURCE)
        quotes.append({
            "status": "succeeded", "dataset_id": "futures_raw_5m", "quality_status": "pass", "row_count": 10,
            "instrument_id_scope": [instrument_id], "secid_scope": [secid], "source_id": QUOTE_SOURCE,
            "manifest_reference": manifest.as_posix(), "quality_report_reference": quality.as_posix(),
            "storage_partition_path": part.as_posix(),
        })
        part, quality, manifest = _producer_files(run_root, instrument_id + "_oi", "futures_open_interest_raw_5m", instrument_id, secid, OI_SOURCE)
        ois.append({
            "dataset_id": "futures_open_interest_raw_5m", "quality_status": "pass", "row_count": 10,
            "trade_date": TRADE_DATE, "instrument_id": instrument_id, "secid": secid, "source_id": OI_SOURCE,
            "manifest_path": manifest.as_posix(), "quality_report_path": quality.as_posix(), "partition_path": part.as_posix(),
        })

    toms: list[dict[str, object]] = []
    for instrument_id, secid in (("usd_tom", "USD000UTSTOM"), ("cny_tom", "CNYRUB_TOM")):
        part, quality, manifest = _producer_files(run_root, instrument_id + "_quote", "fx_spot_raw_5m", instrument_id, secid, TOM_SOURCE)
        toms.append({
            "dataset_id": "fx_spot_raw_5m", "quality_status": "pass", "row_count": 10,
            "trade_date": TRADE_DATE, "instrument_id": instrument_id, "secid": secid, "source_id": TOM_SOURCE,
            "manifest_path": manifest.as_posix(), "quality_report_path": quality.as_posix(), "partition_path": part.as_posix(),
        })

    return {
        "project": "MOEX_Bot", "step": 3, "status": "pilot_passed", "trade_date": TRADE_DATE,
        "as_of_date": TRADE_DATE, "artifact_version": run_id, "materialization_root": run_root.as_posix(),
        "materialization_root_ref": "${MOEX_DATA_ROOT}/runs/step3_canonical_raw/run_id=" + run_id,
        "run_artifacts_immutable": True, "run_id_reuse_allowed": False,
        "reference_observed_at_utc": REFERENCE_TS, "bindings": bindings,
        "latest_autodetect_used": False, "historical_backdating_used": False, "continuous_series_created": False,
        "counts": {"bindings": 4, "quote_partitions": 4, "open_interest_partitions": 4, "tom_partitions": 2},
        "quote_partitions": quotes, "open_interest_partitions": ois, "tom_partitions": toms,
    }


def _store(root: Path, run_id: str, evidence: dict[str, object] | None = None) -> dict[str, object]:
    values = evidence or _evidence(root, run_id)
    _write_json(acceptance.pilot_evidence_path(run_id), values)
    return values


def test_accepts_complete_causal_step3_pilot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    _store(tmp_path, "good")
    result = acceptance.promote_step3_pilot(run_id="good")
    assert result["status"] == "accepted"
    assert result["accepted_pointer_count"] == 10
    assert result["promotion_semantics"] == "transactional_with_rollback"
    assert result["artifact_semantics"] == "immutable_run_scoped"


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda e: e["bindings"][0].pop("availability_ts_utc"), "binding.availability_ts_utc is required"),
        (lambda e: e["bindings"][0].update(secid="CRU6"), "root/month pattern"),
        (lambda e: e["bindings"][0].update(last_trade_date="2027-03-18"), "strictly earlier"),
        (lambda e: e["quote_partitions"][0].update(source_id="legacy_source"), "canonical Step 3 source"),
    ],
)
def test_rejects_invalid_binding_or_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation, match: str) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _evidence(tmp_path, "invalid")
    mutation(evidence)
    _store(tmp_path, "invalid", evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match=match):
        acceptance.promote_step3_pilot(run_id="invalid")


def test_rejects_wrong_moscow_observation_date(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _evidence(tmp_path, "wrong_date")
    evidence["reference_observed_at_utc"] = "2026-08-23T20:00:00+00:00"
    _store(tmp_path, "wrong_date", evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="Europe/Moscow date must equal as_of_date"):
        acceptance.promote_step3_pilot(run_id="wrong_date")


def test_rejects_missing_oi_availability_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _evidence(tmp_path, "missing_avail")
    quality_path = Path(str(evidence["open_interest_partitions"][0]["quality_report_path"]))
    quality = json.loads(quality_path.read_text())
    quality.pop("min_availability_ts_utc")
    _write_json(quality_path, quality)
    _store(tmp_path, "missing_avail", evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="min_availability_ts_utc is required"):
        acceptance.promote_step3_pilot(run_id="missing_avail")


def test_rejects_missing_oi_parquet_causal_column(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _evidence(tmp_path, "missing_column")
    partition = Path(str(evidence["open_interest_partitions"][0]["partition_path"]))
    _write_oi_parquet(partition, include_systime=False)
    _store(tmp_path, "missing_column", evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="causal Parquet validation failed"):
        acceptance.promote_step3_pilot(run_id="missing_column")


def test_rejects_oi_parquet_bounds_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _evidence(tmp_path, "bad_bounds")
    row = evidence["open_interest_partitions"][0]
    quality_path, manifest_path = Path(str(row["quality_report_path"])), Path(str(row["manifest_path"]))
    quality, manifest = json.loads(quality_path.read_text()), json.loads(manifest_path.read_text())
    replacement = "2026-08-24T06:05:00+00:00"
    quality["min_availability_ts_utc"] = replacement
    manifest["min_availability_ts_utc"] = replacement
    _write_json(quality_path, quality)
    _write_json(manifest_path, manifest)
    _store(tmp_path, "bad_bounds", evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="Parquet availability bounds mismatch"):
        acceptance.promote_step3_pilot(run_id="bad_bounds")


def test_rejects_artifact_outside_immutable_run_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence = _evidence(tmp_path, "bad_root")
    outside = tmp_path / "outside" / "part.parquet"
    outside.parent.mkdir(parents=True, exist_ok=True)
    outside.write_bytes(b"outside")
    evidence["quote_partitions"][0]["storage_partition_path"] = outside.as_posix()
    _store(tmp_path, "bad_root", evidence)
    with pytest.raises(acceptance.Step3AcceptanceError, match="immutable run root"):
        acceptance.promote_step3_pilot(run_id="bad_root")


def test_transaction_rolls_back_existing_pointer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    _store(tmp_path, "rollback")
    pointer = tmp_path / "state/datasets/dataset_id=futures_raw_5m/instrument_id=si_front_contract/current_accepted_manifest.json"
    _write_json(pointer, {"sentinel": "old"})
    original = acceptance._replace_staged
    calls = {"n": 0}

    def fail_third(staged: Path, final: Path) -> None:
        calls["n"] += 1
        if calls["n"] == 3:
            raise OSError("simulated")
        original(staged, final)

    monkeypatch.setattr(acceptance, "_replace_staged", fail_third)
    with pytest.raises(acceptance.Step3AcceptanceError, match="transaction failed"):
        acceptance.promote_step3_pilot(run_id="rollback")
    assert json.loads(pointer.read_text()) == {"sentinel": "old"}


def test_pilot_rejects_trade_date_different_from_as_of_before_source_access() -> None:
    with pytest.raises(pilot_runner.Step3PilotError, match="trade_date must equal as_of_date"):
        pilot_runner.run_pilot(trade_date="2026-08-21", as_of_date=TRADE_DATE, artifact_version="mismatch")


def test_pilot_run_id_cannot_be_reused(tmp_path: Path) -> None:
    first = pilot_runner._reserve_run_root(tmp_path, "immutable")
    assert first == tmp_path / "runs/step3_canonical_raw/run_id=immutable"
    with pytest.raises(pilot_runner.Step3PilotError, match="run_id is immutable and cannot be reused"):
        pilot_runner._reserve_run_root(tmp_path, "immutable")
