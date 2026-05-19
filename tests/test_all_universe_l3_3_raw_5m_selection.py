import pandas as pd

from moex_data.futures import all_universe_raw_5m_backfill_slice as slice_mod


def _registry_frame():
    rows = []
    for secid, board, family in [
        ("SiM6", "RFUD", "Si"),
        ("SiU6", "RFUD", "Si"),
        ("BRM6", "RFUD", "BR"),
        ("USDRUBF", "RFUD", "USDRUBF"),
        ("SiH7", "RFUD", "Si"),
        ("XXX", "BADB", "XX"),
    ]:
        rows.append({
            "registry_snapshot_id": "registry_row_" + secid,
            "registry_snapshot_date": "2026-05-19",
            "engine": "futures",
            "market": "forts",
            "board": board,
            "secid": secid,
            "short_code": secid,
            "family_code": family,
            "asset_code": family,
            "instrument_type": "future",
            "expiration_date": "2026-12-17",
            "registry_source": "normalized_registry",
            "source_scope": "/tmp/normalized_registry.parquet",
        })
    return pd.DataFrame(rows)


def _config():
    return {
        "active_raw_5m_selection_mode": "rfud_included_universe",
        "supported_boards": ["RFUD"],
        "chunking_policy": {
            "chunk_dimensions": ["family_code", "date_range", "dataset_stage"],
            "retry_child_chunk_dimensions": ["secid", "date_range", "dataset_stage"],
            "secid_level_failure_isolation": True,
        },
        "continuous_build_enabled": False,
        "w1_build_enabled": False,
        "first_executable_slice": {
            "board": "RFUD",
            "dataset_stage": "raw_5m",
            "max_secid": 2,
            "preferred_family_code": "Si",
            "preferred_secids": ["SiM6", "SiU6"],
            "excluded_secids": ["SiH7"],
            "raw_5m_eligible_only": True,
        },
        "l3_3_raw_5m_included_universe": {
            "board": "RFUD",
            "dataset_stage": "raw_5m",
            "classification_status": "included",
            "raw_5m_eligible_only": True,
            "recent_trading_dates": 3,
        },
    }


def _eligibility():
    return slice_mod.build_eligibility(
        registry=_registry_frame(),
        selected=[],
        dates=["2026-05-15", "2026-05-18", "2026-05-19"],
        config=_config(),
        registry_snapshot_id="registry_snapshot_test",
    )


def _row(frame, secid):
    matched = frame.loc[frame["secid"] == secid]
    assert len(matched) == 1
    return matched.iloc[0].to_dict()


def test_l3_3_default_selection_is_registry_eligibility_driven_not_bounded_slice():
    eligibility = _eligibility()
    selected = slice_mod.selected_universe(eligibility)
    assert selected["secid"].tolist() == ["BRM6", "SiM6", "SiU6", "USDRUBF"]
    assert len(selected) > 2
    for secid in selected["secid"].tolist():
        row = _row(eligibility, secid)
        assert row["classification_status"] == "included"
        assert row["classification_reason"] == "rfud_included_universe_selected"
        assert row["raw_5m_eligible"] is True
        assert row["backfill_selection_status"] == "selected"


def test_l3_3_deferred_and_excluded_instruments_remain_visible():
    eligibility = _eligibility()
    excluded = _row(eligibility, "SiH7")
    deferred = _row(eligibility, "XXX")
    assert excluded["classification_status"] == "excluded"
    assert excluded["exclusion_reason"] == "explicit_pm_exclusion"
    assert excluded["raw_5m_eligible"] is False
    assert deferred["classification_status"] == "deferred"
    assert deferred["deferral_reason"] == "unsupported_board_pending_review"
    assert deferred["raw_5m_eligible"] is False
    assert set(eligibility["secid"].tolist()) == {"SiM6", "SiU6", "BRM6", "USDRUBF", "SiH7", "XXX"}


def test_l3_3_chunking_policy_is_family_date_stage_with_secid_retry_isolation():
    config = _config()
    policy = config["chunking_policy"]
    assert policy["chunk_dimensions"] == ["family_code", "date_range", "dataset_stage"]
    assert policy["retry_child_chunk_dimensions"] == ["secid", "date_range", "dataset_stage"]
    assert policy["secid_level_failure_isolation"] is True
    groups = slice_mod.chunk_groups(slice_mod.selected_universe(_eligibility()))
    assert [family for family, _frame in groups] == ["BR", "Si", "USDRUBF"]
    assert groups[1][1]["secid"].tolist() == ["SiM6", "SiU6"]


def test_l3_3_forbidden_scopes_remain_disabled():
    config = _config()
    assert config["continuous_build_enabled"] is False
    assert config["w1_build_enabled"] is False
    eligibility = _eligibility()
    assert eligibility["futoi_eligible"].eq(False).all()
    assert eligibility["raw_d1_eligible"].eq(False).all()
    assert eligibility["continuous_v1_eligible"].eq(False).all()
    assert eligibility["w1_eligible"].eq(False).all()
