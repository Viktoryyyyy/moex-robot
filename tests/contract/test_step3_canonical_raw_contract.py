from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_step3_program_contract_covers_complete_scope() -> None:
    text = _read("configs/datasets/step3_canonical_raw.v1.yaml")
    for token in (
        "si_front_contract",
        "si_next_contract",
        "cr_front_contract",
        "cr_next_contract",
        "usd_tom",
        "cny_tom",
        "futures_open_interest_raw_5m",
        "fx_spot_raw_5m",
        "step3_raw_pilot_runner.py",
        "step3_raw_acceptance.py",
        "step3_canonical_raw_acceptance.v1.yaml",
    ):
        assert token in text
    assert "latest_autodetect_allowed: false" in text
    assert "continuous_series_created_in_step3: false" in text
    assert "expected_accepted_pointer_count: 10" in text
    assert "partial_pointer_set_without_acceptance_marker_is_not_accepted: true" in text


def test_stage3_registries_are_recognized_by_active_data_access_canon_without_mutating_stage2_package_membership() -> None:
    architecture = _read("contracts/architecture/moex_data_access_canon_v1.yaml")
    data_lake = _read("configs/datasets/futures_data_lake.v1.yaml")
    assert "stage3_dynamic_front_next_role_registry_ref: configs/instruments/forts_front_next_roles.v1.yaml" in architecture
    assert "stage3_cets_instrument_registry_ref: configs/instruments/cets_instrument_registry.v1.yaml" in architecture
    assert "materializer_must_receive_explicit_bound_secid: true" in architecture
    assert "static_secid_in_role_registry_allowed: false" in architecture
    assert "contracts/datasets/futures_open_interest_raw_5m.v1.yaml" not in data_lake
    assert "contracts/datasets/fx_spot_raw_5m.v1.yaml" not in data_lake


def test_front_next_contract_is_explicit_as_of_and_not_liquidity_selected() -> None:
    text = _read("contracts/sources/futures/roll_expiry_mapping.v1.yaml")
    assert "explicit_as_of_date_required: true" in text
    assert "LASTTRADEDATE ascending" in text
    assert "post_as_of_volume_selection_allowed: false" in text
    assert "post_as_of_open_interest_selection_allowed: false" in text
    assert "latest_autodetect_allowed: false" in text


def test_front_next_contract_preserves_pit_unknown_availability_rule() -> None:
    text = _read("contracts/sources/futures/roll_expiry_mapping.v1.yaml").lower()
    assert "availability_ts_utc <= forecast_anchor_ts" in text
    assert "unknown" in text and "unresolved" in text
    assert "shift its eligibility by" in text
    assert "at least one trading day" in text
    assert "never infer an earlier availability timestamp" in text


def test_tom_source_ids_are_explicit_and_legacy_csv_is_not_canonical() -> None:
    source = _read("contracts/sources/currency/moex_iss_cets_tom_1m.v1.yaml")
    registry = _read("configs/instruments/cets_instrument_registry.v1.yaml")
    dataset = _read("contracts/datasets/fx_spot_raw_5m.v1.yaml")
    assert "USD000UTSTOM" in source and "USD000UTSTOM" in registry
    assert "CNYRUB_TOM" in source and "CNYRUB_TOM" in registry
    assert "latest_autodetect_allowed: false" in source
    assert "src/api/fx/fx_5m_day.py" not in source
    assert "accepted_pointer_path_contract" in dataset


def test_open_interest_is_separate_supplementary_dataset() -> None:
    source = _read("contracts/sources/futures/moex_algopack_fo_open_interest_5m.v1.yaml")
    dataset = _read("contracts/datasets/futures_open_interest_raw_5m.v1.yaml")
    program = _read("configs/datasets/step3_canonical_raw.v1.yaml")
    architecture = _read("contracts/architecture/moex_data_access_canon_v1.yaml")
    assert "/iss/datashop/algopack/fo/tradestats.json" in source
    for field in ("OI_OPEN", "OI_HIGH", "OI_LOW", "OI_CLOSE"):
        assert field in source
    assert "/market/supplementary/" in dataset
    assert "quote_and_open_interest_share_partition_file_allowed: false" in program
    assert "lane: supplementary" in architecture
    assert "loop_oi_5m.py" not in program
    assert "accepted_pointer_path_contract" in dataset


def test_step3_acceptance_contract_requires_all_ten_pointers() -> None:
    acceptance = _read("contracts/datasets/step3_canonical_raw_acceptance.v1.yaml")
    assert "total: 10" in acceptance
    assert "validate_all_before_pointer_writes: true" in acceptance
    assert "final_acceptance_marker_written_after_all_pointers: true" in acceptance
    assert "partial_pointer_set_without_acceptance_marker_is_not_accepted: true" in acceptance


def test_step3_runtime_uses_only_canonical_parent_env_path() -> None:
    for path in (
        "src/moex_data/futures/materialize_open_interest_instrument.py",
        "src/moex_data/currency/materialize_cets_tom_raw_5m.py",
        "src/moex_data/step3_raw_pilot_runner.py",
        "src/moex_data/step3_raw_acceptance.py",
    ):
        text = _read(path)
        assert "/home/trader/moex_bot/.env" in text
        assert "/home/trader/moex_bot/moex-robot/.env" not in text
