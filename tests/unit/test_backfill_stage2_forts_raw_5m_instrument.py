from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from moex_data.futures import backfill_stage2_forts_raw_5m_instrument as stage2


def _registry(path: Path, *, evidence: str = "pilot_passed", enabled: bool = False) -> Path:
    path.write_text(
        "\n".join(
            [
                "instruments:",
                "  - instrument_id: usdrubf_futures_family",
                "    source_id: moex_algopack_fo_tradestats_5m",
                "    secid: USDRUBF",
                "    enabled_for_raw_5m_materialization: " + ("true" if enabled else "false"),
                "    evidence_status: " + evidence,
                "rules:",
                "  family_partition_key_allowed: false",
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


def test_stage2_quote_authorization_requires_pilot_passed_with_global_flag_false(tmp_path) -> None:
    data_lake = _data_lake(tmp_path / "data_lake.yaml")
    registry = _registry(tmp_path / "registry.yaml")
    stage2._authorize(
        registry_path=registry,
        data_lake_path=data_lake,
        instrument_id="usdrubf_futures_family",
        secid="USDRUBF",
        source_id=stage2.SOURCE_ID,
    )

    enabled = _registry(tmp_path / "enabled.yaml", enabled=True)
    with pytest.raises(stage2.Stage2QuotesBackfillError, match="pilot evidence is not eligible"):
        stage2._authorize(
            registry_path=enabled,
            data_lake_path=data_lake,
            instrument_id="usdrubf_futures_family",
            secid="USDRUBF",
            source_id=stage2.SOURCE_ID,
        )

    pending = _registry(tmp_path / "pending.yaml", evidence="pilot_required")
    with pytest.raises(stage2.Stage2QuotesBackfillError, match="pilot evidence is not eligible"):
        stage2._authorize(
            registry_path=pending,
            data_lake_path=data_lake,
            instrument_id="usdrubf_futures_family",
            secid="USDRUBF",
            source_id=stage2.SOURCE_ID,
        )


def test_stage2_quote_backfill_delegates_only_after_controlled_authorization(tmp_path, monkeypatch) -> None:
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml")
    captured = {}

    def fake_backfill_range(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(payload={"status": "succeeded"})

    original_checker = stage2.base.registry_allows_instrument
    monkeypatch.setattr(stage2.base, "backfill_range", fake_backfill_range)
    result = stage2.backfill_range(
        date_start="2026-08-17",
        date_end="2026-08-17",
        instrument_id="usdrubf_futures_family",
        secid="USDRUBF",
        artifact_version="stage2_test",
        registry_path=registry,
        data_lake_path=data_lake,
    )
    assert result.payload["status"] == "succeeded"
    assert captured["instrument_id"] == "usdrubf_futures_family"
    assert captured["secid"] == "USDRUBF"
    assert stage2.base.registry_allows_instrument is original_checker


def test_stage2_quote_backfill_blocks_when_stage2_readiness_false(tmp_path) -> None:
    registry = _registry(tmp_path / "registry.yaml")
    data_lake = _data_lake(tmp_path / "data_lake.yaml", ready=False)
    with pytest.raises(stage2.Stage2QuotesBackfillError, match="readiness is not enabled"):
        stage2._authorize(
            registry_path=registry,
            data_lake_path=data_lake,
            instrument_id="usdrubf_futures_family",
            secid="USDRUBF",
            source_id=stage2.SOURCE_ID,
        )
