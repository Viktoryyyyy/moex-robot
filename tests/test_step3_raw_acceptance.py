from __future__ import annotations

import json
from pathlib import Path

import pytest

from moex_data import step3_raw_acceptance as acceptance
from moex_data import step3_raw_pilot_runner as pilot_runner

TRADE_DATE = "2026-08-24"
REFERENCE_TS = "2026-08-24T09:00:00+00:00"


def _write_json(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values), encoding="utf-8")


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
    partition.write_bytes(b"physical-pilot-partition")

    if dataset_id == "futures_raw_5m":
        _write_json(
            quality,
            {
                "run_id": name,
                "rows": [
                    {
                        "run_id": name,
                        "dataset_id": dataset_id,
                        "instrument_id": instrument_id,
                        "source_id": source_id,
                        "secid": secid,
                        "trade_date": TRADE_DATE,
                        "rows": row_count,
                        "quality_status": "pass",
                    }
                ],
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
        _write_json(
            quality,
            {
                "dataset_id": dataset_id,
                "run_id": name,
                "instrument_id": instrument_id,
                "source_id": source_id,
                "secid": secid,
                "trade_date": TRADE_DATE,
                "partition_path": partition.as_posix(),
                "rows": row_count,
                "quality_status": "pass",
            },
        )
        _write_json(
            manifest,
            {
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
            },
        )
    return partition, quality, manifest


def _pilot_evidence(root: Path, run_id: str, *, zero_quote_rows: bool = False) -> dict[str, object]:
    run_root = root / "runs" / "step3_canonical_raw" / ("run_id=" + run_id)
    run_root.mkdir(parents=True, exist_ok=True)
    quote_rows = []
    oi_rows = []
    tom_rows = []
    bindings = []
    role_rows = (
        ("Si", "front", "si_front_contract", "SiU6", "2026-09-17"),
        ("Si", "next", "si_next_contract", "SiZ6", "2026-12-17"),
        ("CR", "front", "cr_front_contract", "CRU6", "2026-09-17"),
        ("CR", "next", "cr_next_contract", "CRZ6", "2026-12-17"),
    )
    for index, (root_name, role, instrument_id, secid, last_trade_date) in enumerate(role_rows):
        bindings.append(
            {
                "root": root_name,
                "role": role,
                "instrument_id": instrument_id,
                "secid": secid,
                "as_of_date": TRADE_DATE,
                "last_trade_date": last_trade_date,
                "source_id": "moex_iss_forts_securities_reference",
                "mapping_fixed_ts_utc": REFERENCE_TS,
                "availability_ts_utc": REFERENCE_TS,
            }
        )
        partition, quality, manifest = _producer_files(
            run_root,
            instrument_id + "_quote",
            dataset_id="futures_raw_5m",
            instrument_id=instrument_id,
            secid=secid,
            source_id="moex_algopack_fo_tradestats_5m",
        )
        quote_rows.append(
            {
                "status": "succeeded",
                "dataset_id": "futures_raw_5m",
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
        oi_partition, oi_quality, oi_manifest = _producer_files(
            run_root,
            instrument_id + "_oi",
            dataset_id="futures_open_interest_raw_5m",
            instrument_id=instrument_id,
            secid=secid,
            source_id="moex_algopack_fo_open_interest_5m",
        )
        oi_rows.append(
            {
                "dataset_id": "futures_open_interest_raw_5m",
                "quality_status": "pass",
                "row_count": 10,
                "trade_date": TRADE_DATE,
                "instrument_id": instrument_id,
                "secid": secid,
                "source_id": "moex_algopack_fo_open_interest_5m",
                "manifest_path": oi_manifest.as_posix(),
                "quality_report_path": oi_quality.as_posix(),
                "partition_path": oi_partition.as_posix(),
            }
        )
    for instrument_id, secid in (("usd_tom", "USD000UTSTOM"), ("cny_tom", "CNYRUB_TOM")):
        partition, quality, manifest = _producer_files(
            run_root,
            instrument_id + "_quote",
            dataset_id="fx_spot_raw_5m",
            instrument_id=instrument_id,
            secid=secid,
            source_id="moex_iss_cets_tom_1m",
        )
        tom_rows.append(
            {
                "dataset_id": "fx_spot_raw_5m",
                "quality_status": "pass",
                "row_count": 10,
                "trade_date": TRADE_DATE,
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
    assert result["promotion_semantics"] == "transactional_with_rollback"
    assert result["artifact_semantics"] == "immutable_run_scoped"
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


def test_step3_acceptance_rejects_noncausal_or_incomplete_binding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_bad_binding"
    evidence = _pilot_evidence(tmp_path, run_id)
    bindings = evidence["bindings"]
    assert isinstance(bindings, list)
    bindings[0].pop("availability_ts_utc")
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)

    with pytest.raises(acceptance.Step3AcceptanceError, match="binding.availability_ts_utc is required"):
        acceptance.promote_step3_pilot(run_id=run_id)


def test_step3_acceptance_rejects_bad_binding_root_pattern(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_bad_secid"
    evidence = _pilot_evidence(tmp_path, run_id)
    bindings = evidence["bindings"]
    assert isinstance(bindings, list)
    bindings[0]["secid"] = "CRU6"
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)

    with pytest.raises(acceptance.Step3AcceptanceError, match="binding SECID does not match canonical root/month pattern"):
        acceptance.promote_step3_pilot(run_id=run_id)


def test_step3_acceptance_rejects_reversed_front_next_expiry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_bad_expiry"
    evidence = _pilot_evidence(tmp_path, run_id)
    bindings = evidence["bindings"]
    assert isinstance(bindings, list)
    bindings[0]["last_trade_date"] = "2027-03-18"
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)

    with pytest.raises(acceptance.Step3AcceptanceError, match="Si front last_trade_date must be strictly earlier than next"):
        acceptance.promote_step3_pilot(run_id=run_id)


def test_step3_acceptance_rejects_noncanonical_source_even_when_evidence_is_self_consistent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_bad_source"
    evidence = _pilot_evidence(tmp_path, run_id)
    quote_rows = evidence["quote_partitions"]
    assert isinstance(quote_rows, list)
    row = quote_rows[0]
    row["source_id"] = "legacy_source"
    manifest_path = Path(str(row["manifest_reference"]))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_scope"] = ["legacy_source"]
    manifest["source_contract"]["source_id"] = "legacy_source"
    _write_json(manifest_path, manifest)
    quality_path = Path(str(row["quality_report_reference"]))
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    quality["rows"][0]["source_id"] = "legacy_source"
    _write_json(quality_path, quality)
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)

    with pytest.raises(acceptance.Step3AcceptanceError, match="source_id does not match canonical Step 3 source"):
        acceptance.promote_step3_pilot(run_id=run_id)


def test_step3_acceptance_rejects_artifact_outside_declared_run_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_bad_run_root"
    evidence = _pilot_evidence(tmp_path, run_id)
    quote_rows = evidence["quote_partitions"]
    assert isinstance(quote_rows, list)
    external = tmp_path / "other_run" / "part.parquet"
    external.parent.mkdir(parents=True, exist_ok=True)
    external.write_bytes(b"other")
    quote_rows[0]["storage_partition_path"] = external.as_posix()
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)

    with pytest.raises(acceptance.Step3AcceptanceError, match="must be inside the declared immutable run root"):
        acceptance.promote_step3_pilot(run_id=run_id)


def test_step3_acceptance_rejects_quality_report_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_bad_quality"
    evidence = _pilot_evidence(tmp_path, run_id)
    quote_rows = evidence["quote_partitions"]
    assert isinstance(quote_rows, list)
    quality_path = Path(str(quote_rows[0]["quality_report_reference"]))
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    quality["rows"][0]["quality_status"] = "fail"
    _write_json(quality_path, quality)
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)

    with pytest.raises(acceptance.Step3AcceptanceError, match="quote quality report mismatch: quality_status"):
        acceptance.promote_step3_pilot(run_id=run_id)


def test_step3_acceptance_rejects_missing_supplementary_quality_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_missing_supp_source"
    evidence = _pilot_evidence(tmp_path, run_id)
    oi_rows = evidence["open_interest_partitions"]
    assert isinstance(oi_rows, list)
    quality_path = Path(str(oi_rows[0]["quality_report_path"]))
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    quality.pop("source_id")
    _write_json(quality_path, quality)
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)

    with pytest.raises(acceptance.Step3AcceptanceError, match="open_interest quality report mismatch: source_id"):
        acceptance.promote_step3_pilot(run_id=run_id)


def test_step3_acceptance_rejects_mismatched_supplementary_quality_partition(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_bad_supp_partition"
    evidence = _pilot_evidence(tmp_path, run_id)
    tom_rows = evidence["tom_partitions"]
    assert isinstance(tom_rows, list)
    quality_path = Path(str(tom_rows[0]["quality_report_path"]))
    unrelated = tmp_path / "runs" / "step3_canonical_raw" / ("run_id=" + run_id) / "unrelated" / "part.parquet"
    unrelated.parent.mkdir(parents=True, exist_ok=True)
    unrelated.write_bytes(b"unrelated")
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    quality["partition_path"] = unrelated.as_posix()
    _write_json(quality_path, quality)
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)

    with pytest.raises(acceptance.Step3AcceptanceError, match="tom.quality_report.partition_path mismatch"):
        acceptance.promote_step3_pilot(run_id=run_id)


def test_step3_acceptance_rolls_back_partial_pointer_replacement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    run_id = "step3_rollback"
    evidence = _pilot_evidence(tmp_path, run_id)
    _write_json(acceptance.pilot_evidence_path(run_id), evidence)

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

    with pytest.raises(acceptance.Step3AcceptanceError, match="pointer promotion transaction failed"):
        acceptance.promote_step3_pilot(run_id=run_id)

    assert json.loads(old_pointer.read_text(encoding="utf-8")) == {"sentinel": "old"}
    pointer_files = list((tmp_path / "state" / "datasets").glob("**/current_accepted_manifest.json"))
    assert pointer_files == [old_pointer]
    assert not acceptance.acceptance_evidence_path(run_id).exists()


def test_step3_pilot_rejects_trade_date_different_from_as_of_before_source_access() -> None:
    with pytest.raises(pilot_runner.Step3PilotError, match="trade_date must equal as_of_date"):
        pilot_runner.run_pilot(
            trade_date="2026-08-21",
            as_of_date=TRADE_DATE,
            artifact_version="step3_mismatch",
        )


def test_step3_pilot_run_root_is_immutable_and_run_id_cannot_be_reused(tmp_path: Path) -> None:
    first = pilot_runner._reserve_run_root(tmp_path, "immutable_run")
    assert first == tmp_path / "runs" / "step3_canonical_raw" / "run_id=immutable_run"
    with pytest.raises(pilot_runner.Step3PilotError, match="run_id is immutable and cannot be reused"):
        pilot_runner._reserve_run_root(tmp_path, "immutable_run")
