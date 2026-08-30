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


def _calendar_rows(*rows: tuple[str, int]) -> list[dict[str, object]]:
    return [{"date": trade_date, "futures": is_trading} for trade_date, is_trading in rows]


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
        )


def test_backfill_skips_only_calendar_validated_non_trading_empty_date_and_writes_pointer(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")

    def fake_calendar(date_start, date_end, *, timeout, calendar_base_url):
        assert date_start == "2026-08-16"
        assert date_end == "2026-08-17"
        return _calendar_rows(("2026-08-16", 0), ("2026-08-17", 1))

    def fake_materialize(*, trade_date, instrument_id, run_id, registry_path, timeout, apim_base_url, require_enabled):
        assert require_enabled is False
        if trade_date == "2026-08-16":
            raise ValueError("normalized FUTOI source contains no rows for explicit trade_date")
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
        published = b"deterministic-test-partition"
        partition.write_bytes(published)
        return {
            "row_count": 2,
            "quality_report_reference": str(quality_path),
            "storage_partition_path": str(partition),
            "publication_run_id": run_id,
            "published_partition_sha256": hashlib.sha256(published).hexdigest(),
        }

    monkeypatch.setattr(backfill.futures_calendar, "fetch_futures_calendar_rows", fake_calendar)
    monkeypatch.setattr(backfill.materializer, "materialize_futoi_partition", fake_materialize)
    result = backfill.backfill_range(
        date_start="2026-08-16",
        date_end="2026-08-17",
        instrument_id="test_futures_family",
        run_id="test_run",
        registry_path=registry,
        data_lake_path=data_lake,
        create_accepted_pointer=True,
    )
    assert result["status"] == "succeeded"
    assert result["quality_status"] == "pass"
    assert result["stage2_controlled_backfill"] is True
    assert result["row_count"] == 2
    assert result["partition_count"] == 1
    assert result["skipped_empty_source_dates"] == ["2026-08-16"]
    assert result["skipped_non_trading_dates"] == ["2026-08-16"]
    assert result["skipped_dates_calendar_validated"] is True
    assert result["calendar_source_id"] == "moex_iss_futures_calendar"
    assert result["calendar_endpoint"] == "/iss/calendars.json"
    manifest = json.loads(Path(str(result["manifest_reference"])).read_text(encoding="utf-8"))
    assert manifest["skipped_non_trading_dates"] == ["2026-08-16"]
    assert manifest["skipped_dates_calendar_validated"] is True
    assert manifest["calendar_source_id"] == "moex_iss_futures_calendar"
    assert manifest["calendar_endpoint"] == "/iss/calendars.json"
    evidence = manifest["partition_evidence"]
    assert len(evidence) == 1
    assert evidence[0]["trade_date"] == "2026-08-17"
    assert evidence[0]["subrun_id"] == "test_run_partition_20260817"
    partition_path = Path(evidence[0]["partition_path"])
    assert evidence[0]["sha256"] == hashlib.sha256(partition_path.read_bytes()).hexdigest()
    assert evidence[0]["row_count"] == 2
    pointer = Path(str(result["accepted_manifest_pointer_reference"]))
    assert pointer.exists()
    payload = json.loads(pointer.read_text(encoding="utf-8"))
    assert payload["dataset_id"] == "futures_futoi_raw"
    assert payload["instrument_id"] == "test_futures_family"
    assert payload["quality_status"] == "pass"
    assert payload["refresh_status"] == "succeeded"


def test_backfill_empty_source_on_calendar_trading_day_fails_closed(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")

    monkeypatch.setattr(
        backfill.futures_calendar,
        "fetch_futures_calendar_rows",
        lambda *args, **kwargs: _calendar_rows(("2026-08-17", 1)),
    )
    monkeypatch.setattr(
        backfill.materializer,
        "materialize_futoi_partition",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("FUTOI APIM exact source returned no rows")),
    )

    result = backfill.backfill_range(
        date_start="2026-08-17",
        date_end="2026-08-17",
        instrument_id="test_futures_family",
        run_id="trading_empty",
        registry_path=registry,
        data_lake_path=data_lake,
    )
    assert result["status"] == "failed"
    assert result["quality_status"] == "fail"
    assert result["skipped_empty_source_dates"] == []
    assert result["failed_dates"] == [
        {
            "trade_date": "2026-08-17",
            "error": "empty FUTOI source on canonical futures trading day",
        }
    ]
    assert result["accepted_manifest_pointer_reference"] is None


def test_backfill_empty_source_with_missing_calendar_coverage_fails_closed(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")

    monkeypatch.setattr(
        backfill.futures_calendar,
        "fetch_futures_calendar_rows",
        lambda *args, **kwargs: _calendar_rows(("2026-08-16", 0)),
    )
    monkeypatch.setattr(
        backfill.materializer,
        "materialize_futoi_partition",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("FUTOI APIM exact source returned no rows")),
    )

    result = backfill.backfill_range(
        date_start="2026-08-17",
        date_end="2026-08-17",
        instrument_id="test_futures_family",
        run_id="missing_calendar",
        registry_path=registry,
        data_lake_path=data_lake,
    )
    assert result["status"] == "failed"
    assert result["quality_status"] == "fail"
    assert result["skipped_empty_source_dates"] == []
    assert len(result["failed_dates"]) == 1
    assert result["failed_dates"][0]["trade_date"] == "2026-08-17"
    assert "canonical futures calendar validation failed" in result["failed_dates"][0]["error"]
    assert "futures calendar missing date 2026-08-17" in result["failed_dates"][0]["error"]


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
            create_accepted_pointer=True,
        )
    pointer = root / "state" / "datasets" / "dataset_id=futures_futoi_raw" / "instrument_id=test_futures_family" / "current_accepted_manifest.json"
    assert not pointer.exists()
