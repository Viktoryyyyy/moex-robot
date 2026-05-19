import json

import pandas as pd

from moex_data.futures import all_universe_raw_5m_backfill_slice as slice_mod


REQUIRED_ELIGIBILITY_FIELDS = [
    "eligibility_snapshot_date",
    "registry_source",
    "identity_check_status",
    "board_check_status",
    "family_mapping_status",
    "raw_5m_check_status",
    "futoi_check_status",
    "liquidity_check_status",
    "history_depth_check_status",
    "expiration_policy_status",
    "perpetual_policy_status",
    "calendar_quality_status",
    "continuous_eligibility_status",
    "notes",
]


def _registry_frame():
    rows = []
    for secid in ["SiM6", "SiU6", "SiH7", "SiM7", "SiZ6", "USDRUBF"]:
        rows.append({
            "registry_snapshot_id": "registry_row_" + secid,
            "registry_snapshot_date": "2026-05-19",
            "engine": "futures",
            "market": "forts",
            "board": "RFUD",
            "secid": secid,
            "short_code": secid,
            "family_code": "Si" if secid != "USDRUBF" else "USDRUBF",
            "asset_code": "Si" if secid != "USDRUBF" else "USDRUBF",
            "instrument_type": "future",
            "expiration_date": "2026-12-17",
            "registry_source": "normalized_registry",
            "source_scope": "/tmp/normalized_registry.parquet",
        })
    return pd.DataFrame(rows)


def _config():
    return {
        "supported_boards": ["RFUD"],
        "first_executable_slice": {
            "board": "RFUD",
            "dataset_stage": "raw_5m",
            "max_secid": 2,
            "preferred_family_code": "Si",
            "preferred_secids": ["SiM6", "SiU6"],
            "excluded_secids": ["SiH7", "SiM7"],
            "raw_5m_eligible_only": True,
        },
    }


def _eligibility():
    registry = _registry_frame()
    selected, family = slice_mod.choose(registry, _config())
    assert family == "Si"
    assert selected == ["SiM6", "SiU6"]
    return slice_mod.build_eligibility(
        registry=registry,
        selected=selected,
        dates=["2026-05-15", "2026-05-18", "2026-05-19"],
        config=_config(),
        registry_snapshot_id="registry_snapshot_test",
    )


def _row(frame, secid):
    matched = frame.loc[frame["secid"] == secid]
    assert len(matched) == 1
    return matched.iloc[0].to_dict()


def test_required_eligibility_contract_fields_exist():
    eligibility = _eligibility()
    missing = [field for field in REQUIRED_ELIGIBILITY_FIELDS if field not in eligibility.columns]
    assert missing == []
    for field in REQUIRED_ELIGIBILITY_FIELDS:
        assert eligibility[field].notna().all(), field


def test_l3_2_selected_instruments_are_only_sim6_siu6_and_raw_5m_coherent():
    eligibility = _eligibility()
    included = sorted(eligibility.loc[eligibility["classification_status"] == "included", "secid"].tolist())
    assert included == ["SiM6", "SiU6"]
    for secid in included:
        row = _row(eligibility, secid)
        assert row["raw_5m_eligible"] is True
        assert row["raw_5m_check_status"] == "pass"
        assert row["backfill_selection_status"] == "selected"
        assert row["backfill_selection_reason"] == "first_executable_slice_selected"
        assert json.loads(row["selected_trading_dates_json"]) == ["2026-05-15", "2026-05-18", "2026-05-19"]


def test_explicit_pm_exclusions_have_excluded_backfill_status():
    eligibility = _eligibility()
    for secid in ["SiH7", "SiM7"]:
        row = _row(eligibility, secid)
        assert row["classification_status"] == "excluded"
        assert row["classification_reason"] == "explicit_pm_exclusion"
        assert row["exclusion_reason"] == "explicit_pm_exclusion"
        assert row["backfill_selection_status"] == "excluded"
        assert row["backfill_selection_reason"] == "explicit_pm_exclusion"
        assert row["raw_5m_eligible"] is False
        assert row["raw_5m_check_status"] == "not_applicable_explicit_pm_exclusion"


def test_non_selected_deferred_instruments_remain_visible_not_silently_included():
    eligibility = _eligibility()
    for secid in ["SiZ6", "USDRUBF"]:
        row = _row(eligibility, secid)
        assert row["classification_status"] == "deferred"
        assert row["deferral_reason"] == "not_selected_for_first_executable_slice"
        assert row["backfill_selection_status"] == "deferred"
        assert row["raw_5m_eligible"] is False
    assert set(eligibility["secid"].tolist()) == {"SiM6", "SiU6", "SiH7", "SiM7", "SiZ6", "USDRUBF"}
