from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "configs" / "instruments" / "forts_instrument_registry.v1.yaml"
DATA_LAKE = ROOT / "configs" / "datasets" / "futures_data_lake.v1.yaml"
FUTOI_SOURCE = ROOT / "contracts" / "sources" / "futures" / "moex_algopack_futoi.v1.yaml"
ONBOARDING = ROOT / "contracts" / "datasets" / "forts_raw_5m_multi_instrument_onboarding.v1.yaml"
QUOTE_BACKFILL = ROOT / "src" / "moex_data" / "futures" / "backfill_stage2_forts_raw_5m_instrument.py"
FUTOI_BACKFILL = ROOT / "src" / "moex_data" / "futures" / "backfill_futoi_instrument.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_stage2_pilot_promotion_authorizes_controlled_backfill_only() -> None:
    registry = _text(REGISTRY)
    data_lake = _text(DATA_LAKE)
    onboarding = _text(ONBOARDING)
    source = _text(FUTOI_SOURCE)
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
    assert "accepted_pointer_ready: false" in data_lake
    assert "observed_source_refresh_ready: false" in data_lake
    assert "scheduler_ready: false" in data_lake
    assert "research_ready: false" in data_lake

    assert 'entry.get("evidence_status") != "pilot_passed"' in quote_backfill
    assert 'entry.get("enabled_for_raw_5m_materialization") is not False' in quote_backfill
    assert 'str(binding.get("evidence_status")) != "pilot_passed"' in futoi_backfill
    assert 'binding.get("futoi.enabled_for_materialization") is not False' in futoi_backfill

    assert "current_status: pilot_passed" in onboarding
    assert "backfill_producer: moex_data.futures.backfill_futoi_instrument" in onboarding

    assert "status: active_stage2_proven" in source
    assert "server_revalidation_status: proven" in source
    assert "public_iss_evidence_status: invalidated" in source
    assert "first_available: \"2020-01-03\"" in source
    assert "first_available: \"2022-04-21\"" in source
    assert source.count("first_available: \"2022-12-30\"") == 2
