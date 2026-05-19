import pandas as pd

from moex_data.futures import futoi_raw_backfill_expansion as mod


def _eligibility_frame():
    return pd.DataFrame([
        {
            "eligibility_snapshot_id": "elig_si",
            "registry_snapshot_id": "reg_si",
            "board": "RFUD",
            "secid": "SiM6",
            "family_code": "Si",
            "classification_status": "included",
            "classification_reason": "rfud_included_universe_selected",
            "deferral_reason": "",
            "exclusion_reason": "",
            "futoi_eligible": True,
            "futoi_check_status": "pass",
            "selected_trading_dates_json": "[\"2026-05-15\", \"2026-05-18\", \"2026-05-19\"]",
        },
        {
            "eligibility_snapshot_id": "elig_br",
            "registry_snapshot_id": "reg_br",
            "board": "RFUD",
            "secid": "BRM6",
            "family_code": "BR",
            "classification_status": "included",
            "classification_reason": "rfud_included_universe_selected",
            "deferral_reason": "",
            "exclusion_reason": "",
            "futoi_eligible": True,
            "futoi_check_status": "pass",
            "selected_trading_dates_json": "[\"2026-05-15\", \"2026-05-18\", \"2026-05-19\"]",
        },
        {
            "eligibility_snapshot_id": "elig_deferred",
            "registry_snapshot_id": "reg_deferred",
            "board": "RFUD",
            "secid": "GZM6",
            "family_code": "GZ",
            "classification_status": "included",
            "classification_reason": "rfud_included_universe_selected",
            "deferral_reason": "",
            "exclusion_reason": "",
            "futoi_eligible": False,
            "futoi_check_status": "futoi_unavailable",
            "selected_trading_dates_json": "[\"2026-05-15\", \"2026-05-18\", \"2026-05-19\"]",
        },
        {
            "eligibility_snapshot_id": "elig_bad_board",
            "registry_snapshot_id": "reg_bad_board",
            "board": "BADB",
            "secid": "BAD1",
            "family_code": "BAD",
            "classification_status": "deferred",
            "classification_reason": "unsupported_board_pending_review",
            "deferral_reason": "unsupported_board_pending_review",
            "exclusion_reason": "",
            "futoi_eligible": False,
            "futoi_check_status": "not_evaluated_unsupported_board",
            "selected_trading_dates_json": "[]",
        },
        {
            "eligibility_snapshot_id": "elig_excluded",
            "registry_snapshot_id": "reg_excluded",
            "board": "RFUD",
            "secid": "SiH7",
            "family_code": "Si",
            "classification_status": "excluded",
            "classification_reason": "explicit_pm_exclusion",
            "deferral_reason": "",
            "exclusion_reason": "explicit_pm_exclusion",
            "futoi_eligible": False,
            "futoi_check_status": "not_applicable_explicit_pm_exclusion",
            "selected_trading_dates_json": "[]",
        },
    ])


def test_l3_4_selects_rfud_included_futoi_eligible_only():
    selected = mod.selected_from_eligibility(_eligibility_frame(), "RFUD")
    assert selected["secid"].tolist() == ["BRM6", "SiM6"]
    assert selected["futoi_eligible"].eq(True).all()
    assert selected["classification_status"].eq("included").all()


def test_l3_4_deferred_and_excluded_remain_visible_without_silent_inclusion():
    visible = mod.visibility_rows_from_eligibility(_eligibility_frame(), "RFUD")
    by_secid = {row["secid"]: row for row in visible}
    assert by_secid["SiM6"]["backfill_selection_status"] == "selected"
    assert by_secid["GZM6"]["backfill_selection_status"] == "deferred"
    assert by_secid["GZM6"]["backfill_selection_reason"] == "futoi_unavailable"
    assert by_secid["SiH7"]["backfill_selection_status"] == "excluded"
    assert by_secid["SiH7"]["backfill_selection_reason"] == "explicit_pm_exclusion"
    assert "BAD1" not in by_secid


def test_l3_4_chunking_is_family_date_stage_and_retry_is_secid_date_stage():
    groups = mod.chunk_groups(mod.selected_from_eligibility(_eligibility_frame(), "RFUD"))
    assert [family for family, _frame in groups] == ["BR", "Si"]
    chunk_dimensions = ["family_code", "date_range", "dataset_stage"]
    retry_dimensions = ["secid", "date_range", "dataset_stage"]
    assert chunk_dimensions == ["family_code", "date_range", "dataset_stage"]
    assert retry_dimensions == ["secid", "date_range", "dataset_stage"]
    assert mod.DATASET_STAGE == "futoi_raw"


def test_l3_4_dates_require_explicit_range_or_eligibility_dates():
    frame = _eligibility_frame()
    assert mod.dates_for_row(frame.iloc[0], "", "") == ("2026-05-15", "2026-05-19")
    assert mod.dates_for_row(frame.iloc[0], "2026-05-18", "2026-05-19") == ("2026-05-18", "2026-05-19")


def test_l3_4_deferred_quality_row_is_explicit_and_writes_no_partitions():
    row = {
        "secid": "GZM6",
        "family_code": "GZ",
        "board": "RFUD",
        "backfill_selection_status": "deferred",
        "backfill_selection_reason": "futoi_unavailable",
        "eligibility_snapshot_id": "elig_deferred",
        "registry_snapshot_id": "reg_deferred",
    }
    q = mod.deferred_quality_row("run", "chunk", "2026-05-19", "2026-05-19", row)
    assert q["fetch_status"] == "not_attempted"
    assert q["rows"] == 0
    assert q["partition_count"] == 0
    assert q["quality_status"] == "deferred"
    assert q["deferred_reason"] == "futoi_unavailable"
    assert q["output_partitions_json"] == "[]"
