import pandas as pd

from moex_data.futures.registry_evidence_artifacts_producer import build_family_mapping
from moex_data.futures.registry_evidence_artifacts_producer import select_all_rfud_instruments


def test_select_all_rfud_instruments_filters_supported_board_only():
    frame = pd.DataFrame([
        {"snapshot_id": "s1", "snapshot_date": "2026-05-04", "secid": "SiM6", "board": "rfud", "engine": "futures", "market": "forts", "family_code": "Si"},
        {"snapshot_id": "s1", "snapshot_date": "2026-05-04", "secid": "BAD", "board": "other", "engine": "futures", "market": "forts", "family_code": "BAD"},
    ])

    selected = select_all_rfud_instruments(frame)

    assert selected["secid"].tolist() == ["SiM6"]
    assert selected.iloc[0]["selection_status"] == "selected_from_all_rfud_registry"


def test_build_family_mapping_preserves_si_and_usdrubf_identity():
    frame = pd.DataFrame([
        {"snapshot_id": "s1", "snapshot_date": "2026-05-04", "secid": "SiM6", "board": "rfud", "engine": "futures", "market": "forts", "family_code": "Si", "contract_code": "M6"},
        {"snapshot_id": "s1", "snapshot_date": "2026-05-04", "secid": "USDRUBF", "board": "rfud", "engine": "futures", "market": "forts", "family_code": "USDRUBF", "contract_code": ""},
    ])

    mapping = build_family_mapping(frame, "2026-05-04")

    assert mapping.loc[mapping["secid"] == "SiM6", "family_code"].iloc[0] == "Si"
    assert mapping.loc[mapping["secid"] == "USDRUBF", "family_code"].iloc[0] == "USDRUBF"
    assert set(mapping["mapping_status"].tolist()) == {"pass"}
    assert set(mapping["mapping_source"].tolist()) == {"derived_rule"}
