from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CANON = ROOT / "contracts" / "architecture" / "moex_data_access_canon_v1.yaml"
RAW_QUOTES = ROOT / "contracts" / "datasets" / "futures_raw_5m.v1.yaml"
FUTOI = ROOT / "contracts" / "datasets" / "futures_futoi_raw.v1.yaml"
DATA_LAKE = ROOT / "configs" / "datasets" / "futures_data_lake.v1.yaml"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_canonical_quote_and_futoi_paths_are_market_rooted() -> None:
    canon = _text(CANON)
    raw_quotes = _text(RAW_QUOTES)
    futoi = _text(FUTOI)

    raw_pattern = '${MOEX_DATA_ROOT}/market/raw/timeframe=5m/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet'
    futoi_pattern = '${MOEX_DATA_ROOT}/market/supplementary/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet'

    assert raw_pattern in raw_quotes
    assert futoi_pattern in futoi
    assert '${MOEX_DATA_ROOT}/market/raw/timeframe={TIMEFRAME}/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet' in canon
    assert '${MOEX_DATA_ROOT}/market/supplementary/dataset_id={DATASET_ID}/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet' in canon


def test_new_materialization_cannot_extend_legacy_roots() -> None:
    canon = _text(CANON)
    data_lake = _text(DATA_LAKE)

    assert "new_materialization_to_legacy_roots_allowed: false" in canon
    assert "new_dataset_creation_to_legacy_roots_allowed: false" in canon
    assert "new_instrument_onboarding_to_legacy_roots_allowed: false" in canon
    assert "legacy_write_extension_allowed: false" in canon
    assert "new_instrument_onboarding_to_legacy_roots_allowed: false" in data_lake
    assert "new_dataset_creation_to_legacy_roots_allowed: false" in data_lake


def test_futoi_is_an_independent_supplementary_lane() -> None:
    canon = _text(CANON)
    data_lake = _text(DATA_LAKE)

    assert "separate_from_quote_partitions_required: true" in canon
    assert "independent_quality_manifest_pointer_required: true" in canon
    assert "supplementary_data_cannot_be_embedded_in_quote_partition: true" in canon
    assert "quotes_and_futoi_share_partition_file_allowed: false" in data_lake


def test_known_legacy_writers_are_explicitly_inventory_bound() -> None:
    canon = _text(CANON)

    expected_refs = (
        "src/moex_data/futures/materialize_forts_raw_5m_tradestats.py",
        "src/moex_data/futures/materialize_forts_raw_5m_instrument.py",
        "src/moex_data/futures/raw_5m_loader.py",
        "src/moex_data/futures/futoi_raw_loader.py",
        "src/moex_data/futures/refresh_forts_raw_5m_incremental_pointer.py",
    )

    for ref in expected_refs:
        assert ref in canon


def test_canonical_pointer_drops_secid_partition() -> None:
    canon = _text(CANON)

    assert '${MOEX_DATA_ROOT}/state/datasets/dataset_id={DATASET_ID}/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json' in canon
    assert "secid_partition_allowed: false" in canon
    assert "new_instrument_legacy_pointer_fallback_allowed: false" in canon
