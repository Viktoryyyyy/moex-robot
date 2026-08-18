from __future__ import annotations

import json
from pathlib import Path

import pytest

from moex_data.futures import backfill_futoi_instrument as backfill


def _registry(path: Path, *, enabled: bool = True) -> Path:
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


def test_backfill_requires_registry_materialization_enablement(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    registry = _registry(tmp_path / "registry.yaml", enabled=False)
    with pytest.raises(backfill.FutoiBackfillError, match="not enabled"):
        backfill.backfill_range(
            date_start="2026-08-17",
            date_end="2026-08-17",
            instrument_id="test_futures_family",
            run_id="test_run",
            registry_path=registry,
        )


def test_backfill_skips_empty_dates_and_writes_pointer_only_after_pass(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml", enabled=True)

    def fake_materialize(*, trade_date, instrument_id, run_id, registry_path, timeout, apim_base_url, require_enabled):
        assert require_enabled is True
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
        return {
            "row_count": 2,
            "quality_report_reference": str(quality_path),
            "storage_partition_path": str(partition),
        }

    monkeypatch.setattr(backfill.materializer, "materialize_futoi_partition", fake_materialize)
    result = backfill.backfill_range(
        date_start="2026-08-16",
        date_end="2026-08-17",
        instrument_id="test_futures_family",
        run_id="test_run",
        registry_path=registry,
        create_accepted_pointer=True,
    )
    assert result["status"] == "succeeded"
    assert result["quality_status"] == "pass"
    assert result["row_count"] == 2
    assert result["partition_count"] == 1
    assert result["skipped_empty_source_dates"] == ["2026-08-16"]
    pointer = Path(str(result["accepted_manifest_pointer_reference"]))
    assert pointer.exists()
    payload = json.loads(pointer.read_text(encoding="utf-8"))
    assert payload["dataset_id"] == "futures_futoi_raw"
    assert payload["instrument_id"] == "test_futures_family"
    assert payload["quality_status"] == "pass"
    assert payload["refresh_status"] == "succeeded"


def test_backfill_failure_blocks_pointer(tmp_path, monkeypatch) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    registry = _registry(tmp_path / "registry.yaml", enabled=True)

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
            create_accepted_pointer=True,
        )
    pointer = root / "state" / "datasets" / "dataset_id=futures_futoi_raw" / "instrument_id=test_futures_family" / "current_accepted_manifest.json"
    assert not pointer.exists()
