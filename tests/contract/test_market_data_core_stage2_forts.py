from pathlib import Path

import pytest

from moex_data.futures import materialize_forts_raw_5m_instrument as quote_writer


ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "configs" / "instruments" / "forts_instrument_registry.v1.yaml"
DATA_LAKE = ROOT / "configs" / "datasets" / "futures_data_lake.v1.yaml"
QUOTE_SOURCE = ROOT / "contracts" / "sources" / "futures" / "moex_algopack_fo_tradestats_5m.v1.yaml"
FUTOI_SOURCE = ROOT / "contracts" / "sources" / "futures" / "moex_iss_futoi.v1.yaml"
QUOTE_WRITER = ROOT / "src" / "moex_data" / "futures" / "materialize_forts_raw_5m_instrument.py"
QUOTE_BACKFILL = ROOT / "src" / "moex_data" / "futures" / "backfill_forts_raw_5m_instrument.py"
FUTOI_WRITER = ROOT / "src" / "moex_data" / "futures" / "materialize_futoi_instrument.py"
ONBOARDING = ROOT / "contracts" / "datasets" / "forts_raw_5m_multi_instrument_onboarding.v1.yaml"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_stage2_quote_source_identity_is_generic_and_authenticated() -> None:
    source = _text(QUOTE_SOURCE)
    writer = _text(QUOTE_WRITER)

    assert "source_id: moex_algopack_fo_tradestats_5m" in source
    assert "token_env: MOEX_API_KEY" in source
    assert "source_id_is_instrument_specific: false" in source
    assert 'SOURCE_ID: Final[str] = "moex_algopack_fo_tradestats_5m"' in writer
    assert 'headers["Authorization"] = "Bearer " + token' in writer
    assert "MOEX_API_KEY is required" in writer


def test_stage2_bearer_auth_path_does_not_recurse_when_core_helper_is_patched() -> None:
    original = quote_writer.core._auth_headers
    try:
        quote_writer.core._auth_headers = quote_writer._auth_headers_with_bearer
        headers = quote_writer._auth_headers_with_bearer(
            {"MOEX_API_KEY": "test-token", "MOEX_UA": "stage2-regression-test"}
        )
    finally:
        quote_writer.core._auth_headers = original

    assert headers["Authorization"] == "Bearer test-token"
    assert headers["User-Agent"] == "stage2-regression-test"


def test_stage2_bearer_auth_path_fails_closed_without_token() -> None:
    with pytest.raises(ValueError, match="MOEX_API_KEY is required"):
        quote_writer._auth_headers_with_bearer({"MOEX_UA": "stage2-regression-test"})


def test_stage2_registry_has_all_four_explicit_forts_bindings() -> None:
    registry = _text(REGISTRY)
    expected = (
        ("usdrubf_futures_family", "USDRUBF"),
        ("cnyrubf_futures_family", "CNYRUBF"),
        ("si_futures_family", "SiU6"),
        ("cr_futures_family", "CRU6"),
    )
    for instrument_id, secid in expected:
        assert f"instrument_id: {instrument_id}" in registry
        assert f"secid: {secid}" in registry
    assert registry.count("source_id: moex_algopack_fo_tradestats_5m") >= 4
    assert "moex_forts_usdrubf_design_placeholder" not in registry
    assert "moex_forts_si_design_placeholder" not in registry


def test_stage2_quote_writer_and_backfill_do_not_write_legacy_roots() -> None:
    writer = _text(QUOTE_WRITER)
    backfill = _text(QUOTE_BACKFILL)

    assert "${MOEX_DATA_ROOT}/market/raw/timeframe=5m/" in writer
    assert '/ "forts" / "raw_5m"' not in writer
    assert 'root / "manifests" /' not in backfill
    assert 'root / "quality_reports" /' not in backfill
    assert "accepted_manifest.write_accepted_manifest_pointer" in backfill
    assert '"${MOEX_DATA_ROOT}/state/datasets/dataset_id="' in backfill


def test_stage2_futoi_is_registry_bound_and_canonical_supplementary() -> None:
    source = _text(FUTOI_SOURCE)
    writer = _text(FUTOI_WRITER)
    registry = _text(REGISTRY)

    assert "source_id: moex_iss_futoi" in source
    assert "fallback_endpoint_allowed: false" in source
    assert '/ "market"\n        / "supplementary"' in writer
    assert '/ ("dataset_id=" + DATASET_ID)' in writer
    assert 'source_id: moex_iss_futoi' in registry
    for ticker in ("usdrubf", "cnyrubf", "si", "cr"):
        assert f"ticker: {ticker}" in registry
    assert '/ "futures" / "futoi_raw"' not in writer


def test_stage2_scheduler_and_research_remain_fail_closed() -> None:
    registry = _text(REGISTRY)
    data_lake = _text(DATA_LAKE)

    assert registry.count("enabled_for_update: false") == 4
    assert registry.count("enabled_for_d1_derivation: false") == 4
    assert registry.count("enabled_for_research: false") == 4
    assert "observed_source_refresh_ready: false" in data_lake
    assert "scheduler_ready: false" in data_lake
    assert "research_ready: false" in data_lake


def test_stage2_onboarding_uses_canonical_pointer_and_separate_futoi_lane() -> None:
    onboarding = _text(ONBOARDING)

    assert '${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_raw_5m/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json' in onboarding
    assert "secid_pointer_partition_allowed: false" in onboarding
    assert "dataset_id: futures_futoi_raw" in onboarding
    assert "quote_partition_embedding_allowed: false" in onboarding
    assert "independent_quality_manifest_pointer_required: true" in onboarding
