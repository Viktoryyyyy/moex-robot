from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "configs" / "instruments" / "forts_instrument_registry.v1.yaml"
DATA_LAKE = ROOT / "configs" / "datasets" / "futures_data_lake.v1.yaml"
RUNBOOK = ROOT / "docs" / "data" / "futures_data_ingestion_runbook.md"
FUTOI_SOURCE = ROOT / "contracts" / "sources" / "futures" / "moex_algopack_futoi.v1.yaml"
QUOTE_SOURCE = ROOT / "contracts" / "sources" / "futures" / "moex_algopack_fo_tradestats_5m.v1.yaml"
QUOTE_BACKFILL = ROOT / "src" / "moex_data" / "futures" / "backfill_stage2_forts_raw_5m_instrument.py"
FUTOI_BACKFILL = ROOT / "src" / "moex_data" / "futures" / "backfill_futoi_instrument.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_stage2_pilot_promotion_records_completed_backfills_but_keeps_runtime_closed() -> None:
    registry = _text(REGISTRY)
    data_lake = _text(DATA_LAKE)
    runbook = _text(RUNBOOK)
    source = _text(FUTOI_SOURCE)
    quote_source = _text(QUOTE_SOURCE)
    quote_backfill = _text(QUOTE_BACKFILL)
    futoi_backfill = _text(FUTOI_BACKFILL)

    assert registry.count("evidence_status: pilot_passed") == 4
    assert registry.count("enabled_for_raw_5m_materialization: false") == 4
    assert registry.count("enabled_for_materialization: false") == 4
    assert registry.count("enabled_for_loading: false") == 4
    assert registry.count("enabled_for_update: false") == 4
    assert registry.count("enabled_for_retrieval: false") == 4
    assert registry.count("enabled_for_d1_derivation: false") == 4
    assert registry.count("enabled_for_research: false") == 4

    assert "backfill_ready: true" in data_lake
    assert "historical_quotes_backfill_completed: true" in data_lake
    assert "priority_futoi_raw_backfill_completed: true" in data_lake
    assert "raw_physical_audit_completed: true" in data_lake
    assert "reference_full_historical_backfill_required: false" in data_lake
    assert "stage2_historical_quote_instrument_ids:" in data_lake
    assert "stage2_reference_quote_instrument_ids:" in data_lake
    assert "historical_backfill_instrument_ids:" in data_lake
    assert "reference_instrument_ids:" in data_lake
    assert "accepted_pointer_ready: false" in data_lake
    assert "observed_source_refresh_ready: false" in data_lake
    assert "scheduler_ready: false" in data_lake
    assert "research_ready: false" in data_lake

    assert 'entry.get("evidence_status") != "pilot_passed"' in quote_backfill
    assert 'entry.get("enabled_for_raw_5m_materialization") is not False' in quote_backfill
    assert "_stage2_historical_quote_backfill_allows" in quote_backfill
    assert "current-reference only" in quote_backfill
    assert 'str(binding.get("evidence_status")) != "pilot_passed"' in futoi_backfill
    assert 'binding.get("futoi.enabled_for_materialization") is not False' in futoi_backfill

    assert "Canonical historical Stage 2 instruments:" in runbook
    assert "Reference-only current-expiry quote instruments in Stage 2:" in runbook
    assert "Do not perform multi-year historical quote backfill using one fixed current-expiry Si or CR SECID" in runbook
    assert "Stage 2 raw FUTOI historical datasets accepted by physical audit:" in runbook
    assert "accepted pointer remains absent until the architecture gate enables it" in runbook

    assert "historical_coverage_probe:" in quote_source
    assert "identity_filter: SECID+TRADEDATE" in quote_source
    assert "historical_core_secids:" in quote_source
    assert "reference_secids:" in quote_source
    assert "first_available: \"2022-04-26\"" in quote_source
    assert "first_available: \"2024-09-18\"" in quote_source
    assert "first_available: \"2025-03-07\"" in quote_source
    assert "historical_backfill_status: completed_and_physically_validated" in quote_source

    assert "status: active_stage2_proven" in source
    assert "server_revalidation_status: proven" in source
    assert "public_iss_evidence_status: invalidated" in source
    assert "first_available: \"2020-01-03\"" in source
    assert "first_available: \"2022-04-21\"" in source
    assert source.count("first_available: \"2022-12-30\"") == 2
    assert "historical_priority_backfill:" in source
    assert "physical_quality_status: pass" in source
