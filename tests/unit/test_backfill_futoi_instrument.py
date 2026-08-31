from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from moex_data.futures import backfill_futoi_instrument as backfill


def _registry(path: Path, *, evidence: str = "pilot_passed", enabled: bool = False) -> Path:
    path.write_text(
        "\n".join(
            [
                "registry_id: test",
                "instruments:",
                "  - instrument_id: test_futures_family",
                "    canonical_symbol: TEST",
                "    secid: TEST",
                "    board: RFUD",
                "    market: forts",
                "    engine: futures",
                "    source_artifact_id: external.apim.fo.tradestats.v1",
                "    source_id: moex_algopack_fo_tradestats_5m",
                "    evidence_status: " + evidence,
                "    supplementary_sources:",
                "      futures_futoi_raw:",
                "        source_id: moex_algopack_futoi",
                "        ticker: test",
                "        availability_status: available",
                "        probe_status: completed",
                "        enabled_for_materialization: " + ("true" if enabled else "false"),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _data_lake(path: Path, *, ready: bool = True) -> Path:
    path.write_text(
        "\n".join(
            [
                "stage2_forts_source_bindings:",
                "  status: " + ("all_pilots_passed_backfill_ready" if ready else "pilot_pending"),
                "  readiness_flags:",
                "    backfill_ready: " + ("true" if ready else "false"),
                "    accepted_pointer_ready: false",
                "    scheduler_ready: false",
                "    research_ready: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _materializer(root: Path, *, published: bytes = b"deterministic-test-partition"):
    def fake_materialize(*, trade_date, instrument_id, run_id, registry_path, timeout, apim_base_url, require_enabled):
        assert require_enabled is False
        quality_path = root / "quality" / (run_id + ".json")
        quality_path.parent.mkdir(parents=True, exist_ok=True)
        quality_path.write_text(
            json.dumps(
                {
                    "duplicate_key_count": 0,
                    "null_required_count": 0,
                    "invalid_position_count": 0,
                }
            ),
            encoding="utf-8",
        )
        partition = root / "market" / "supplementary" / (trade_date + ".parquet")
        partition.parent.mkdir(parents=True, exist_ok=True)
        partition.write_bytes(published)
        return {
            "row_count": 2,
            "quality_report_reference": str(quality_path),
            "storage_partition_path": str(partition),
            "publication_run_id": run_id,
            "published_partition_sha256": hashlib.sha256(published).hexdigest(),
        }

    return fake_materialize


def test_backfill_requires_stage2_pilot_evidence_and_keeps_global_flag_false(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    data_lake = _data_lake(tmp_path / "data_lake.yaml")

    missing_evidence = _registry(tmp_path / "missing.yaml", evidence="pilot_required")
    with pytest.raises(backfill.FutoiBackfillError, match="pilot evidence"):
        backfill.backfill_range(
            date_start="2026-08-17",
            date_end="2026-08-17",
            instrument_id="test_futures_family",
            run_id="test_run",
            registry_path=missing_evidence,
            data_lake_path=data_lake,
            observed_trade_dates=["2026-08-17"],
        )

    globally_enabled = _registry(tmp_path / "enabled.yaml", enabled=True)
    with pytest.raises(backfill.FutoiBackfillError, match="global FUTOI materialization flag must remain false"):
        backfill.backfill_range(
            date_start="2026-08-17",
            date_end="2026-08-17",
            instrument_id="test_futures_family",
            run_id="test_run",
            registry_path=globally_enabled,
            data_lake_path=data_lake,
            observed_trade_dates=["2026-08-17"],
        )


def test_backfill_requires_stage2_backfill_readiness(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml", ready=False)

    with pytest.raises(backfill.FutoiBackfillError, match="readiness is not enabled"):
        backfill.backfill_range(
            date_start="2026-08-17",
            date_end="2026-08-17",
            instrument_id="test_futures_family",
            run_id="test_run",
            registry_path=registry,
            data_lake_path=data_lake,
            observed_trade_dates=["2026-08-17"],
        )


def test_backfill_materializes_only_observed_dates_and_writes_immutable_date_evidence(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")
    calls = []
    fake = _materializer(root)

    def materialize(**kwargs):
        calls.append(kwargs["trade_date"])
        return fake(**kwargs)

    monkeypatch.setattr(backfill.materializer, "materialize_futoi_partition", materialize)
    result = backfill.backfill_range(
        date_start="2026-08-16",
        date_end="2026-08-17",
        instrument_id="test_futures_family",
        run_id="test_run",
        registry_path=registry,
        data_lake_path=data_lake,
        observed_trade_dates=["2026-08-17"],
        create_accepted_pointer=True,
    )

    assert calls == ["2026-08-17"]
    assert result["status"] == "succeeded"
    assert result["quality_status"] == "pass"
    assert result["stage2_controlled_backfill"] is True
    assert result["row_count"] == 2
    assert result["partition_count"] == 1
    assert result["observed_trade_dates"] == ["2026-08-17"]
    assert result["date_source_artifact_id"] == backfill.DATE_SOURCE_ARTIFACT_ID
    assert result["date_source_id"] == backfill.DATE_SOURCE_ID
    assert result["date_source_endpoint"] == backfill.DATE_SOURCE_ENDPOINT
    assert result["date_selection_rule"] == "observed_trade_dates_only"
    assert result["reference_secid"] == "TEST"

    manifest = json.loads(Path(str(result["manifest_reference"])).read_text(encoding="utf-8"))
    assert manifest["observed_trade_dates"] == ["2026-08-17"]
    evidence = manifest["partition_evidence"]
    assert len(evidence) == 1
    assert evidence[0]["trade_date"] == "2026-08-17"
    assert evidence[0]["subrun_id"] == "test_run_partition_20260817"
    partition_path = Path(evidence[0]["partition_path"])
    assert evidence[0]["sha256"] == hashlib.sha256(partition_path.read_bytes()).hexdigest()
    assert evidence[0]["row_count"] == 2

    evidence_path = Path(str(result["observed_date_evidence_ref"]))
    evidence_bytes = evidence_path.read_bytes()
    assert hashlib.sha256(evidence_bytes).hexdigest() == result["observed_date_evidence_sha256"]
    date_evidence = json.loads(evidence_bytes)
    assert date_evidence["schema_version"] == backfill.OBSERVED_DATE_EVIDENCE_SCHEMA
    assert date_evidence["producer"] == backfill.PRODUCER_ID
    assert date_evidence["reference_secid"] == "TEST"
    assert date_evidence["requested_from"] == "2026-08-16"
    assert date_evidence["requested_till"] == "2026-08-17"
    assert date_evidence["observed_dates"] == ["2026-08-17"]
    assert date_evidence["observed_date_count"] == 1

    pointer = Path(str(result["accepted_manifest_pointer_reference"]))
    payload = json.loads(pointer.read_text(encoding="utf-8"))
    assert payload["dataset_id"] == "futures_futoi_raw"
    assert payload["instrument_id"] == "test_futures_family"
    assert payload["quality_status"] == "pass"
    assert payload["refresh_status"] == "succeeded"


def test_backfill_failure_on_observed_date_is_not_reclassified_as_non_trading(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")
    monkeypatch.setattr(
        backfill.materializer,
        "materialize_futoi_partition",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("FUTOI APIM exact source returned no rows")),
    )

    result = backfill.backfill_range(
        date_start="2026-08-17",
        date_end="2026-08-17",
        instrument_id="test_futures_family",
        run_id="observed_empty",
        registry_path=registry,
        data_lake_path=data_lake,
        observed_trade_dates=["2026-08-17"],
    )

    assert result["status"] == "failed"
    assert result["quality_status"] == "fail"
    assert result["observed_trade_dates"] == ["2026-08-17"]
    assert result["failed_dates"] == [
        {"trade_date": "2026-08-17", "error": "FUTOI APIM exact source returned no rows"}
    ]
    assert result["accepted_manifest_pointer_reference"] is None


def test_backfill_observed_source_failure_fails_closed_before_materialization(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")
    monkeypatch.setattr(
        backfill.trade_dates,
        "observed_dates",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("TradeStats unavailable")),
    )
    monkeypatch.setattr(
        backfill.materializer,
        "materialize_futoi_partition",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("materializer must not run")),
    )

    with pytest.raises(
        backfill.FutoiBackfillError,
        match="authoritative observed TradeStats date source failed.*reference_secid=TEST.*TradeStats unavailable",
    ):
        backfill.backfill_range(
            date_start="2026-08-17",
            date_end="2026-08-17",
            instrument_id="test_futures_family",
            run_id="source_failure",
            registry_path=registry,
            data_lake_path=data_lake,
        )


def test_backfill_rejects_empty_observed_date_injection(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")

    with pytest.raises(backfill.FutoiBackfillError, match="observed_trade_dates are invalid"):
        backfill.backfill_range(
            date_start="2026-08-17",
            date_end="2026-08-17",
            instrument_id="test_futures_family",
            run_id="empty_dates",
            registry_path=registry,
            data_lake_path=data_lake,
            observed_trade_dates=[],
        )


def test_backfill_evidence_remains_bound_to_materializer_published_bytes_after_competing_replace(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")
    published = b"run-a-published-bytes"
    competing = b"run-b-competing-replacement"

    def fake_materialize(*, trade_date, instrument_id, run_id, registry_path, timeout, apim_base_url, require_enabled):
        quality_path = root / "quality" / (run_id + ".json")
        quality_path.parent.mkdir(parents=True, exist_ok=True)
        quality_path.write_text(
            json.dumps(
                {
                    "duplicate_key_count": 0,
                    "null_required_count": 0,
                    "invalid_position_count": 0,
                }
            ),
            encoding="utf-8",
        )
        partition = root / "market" / "supplementary" / (trade_date + ".parquet")
        partition.parent.mkdir(parents=True, exist_ok=True)
        partition.write_bytes(published)
        publication_sha256 = hashlib.sha256(published).hexdigest()
        partition.write_bytes(competing)
        return {
            "row_count": 2,
            "quality_report_reference": str(quality_path),
            "storage_partition_path": str(partition),
            "publication_run_id": run_id,
            "published_partition_sha256": publication_sha256,
        }

    monkeypatch.setattr(backfill.materializer, "materialize_futoi_partition", fake_materialize)
    result = backfill.backfill_range(
        date_start="2026-08-17",
        date_end="2026-08-17",
        instrument_id="test_futures_family",
        run_id="race_test",
        registry_path=registry,
        data_lake_path=data_lake,
        observed_trade_dates=["2026-08-17"],
    )
    manifest = json.loads(Path(str(result["manifest_reference"])).read_text(encoding="utf-8"))
    evidence = manifest["partition_evidence"][0]
    assert evidence["subrun_id"] == "race_test_partition_20260817"
    assert evidence["sha256"] == hashlib.sha256(published).hexdigest()
    assert evidence["sha256"] != hashlib.sha256(competing).hexdigest()
    assert Path(evidence["partition_path"]).read_bytes() == competing


def test_backfill_failure_blocks_pointer(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")

    def fail_materialize(**kwargs):
        raise RuntimeError("FUTOI APIM schema mismatch")

    monkeypatch.setattr(backfill.materializer, "materialize_futoi_partition", fail_materialize)
    with pytest.raises(backfill.FutoiBackfillError, match="cannot be created"):
        backfill.backfill_range(
            date_start="2026-08-17",
            date_end="2026-08-17",
            instrument_id="test_futures_family",
            run_id="test_failure",
            registry_path=registry,
            data_lake_path=data_lake,
            observed_trade_dates=["2026-08-17"],
            create_accepted_pointer=True,
        )
    pointer = (
        root
        / "state"
        / "datasets"
        / "dataset_id=futures_futoi_raw"
        / "instrument_id=test_futures_family"
        / "current_accepted_manifest.json"
    )
    assert not pointer.exists()
