import pandas as pd

from moex_data.futures import futoi_stage_eligibility_builder as mod


def test_futoi_stage_builder_sets_flag_only_for_rfud_included_available_completed():
    eligibility = pd.DataFrame([
        {"board": "RFUD", "secid": "A", "family_code": "AA", "classification_status": "included", "futoi_eligible": False, "schema_version": "old", "notes": "n"},
        {"board": "RFUD", "secid": "B", "family_code": "BB", "classification_status": "included", "futoi_eligible": False, "schema_version": "old", "notes": "n"},
        {"board": "RFUD", "secid": "C", "family_code": "CC", "classification_status": "deferred", "futoi_eligible": True, "schema_version": "old", "notes": "n"},
        {"board": "BADB", "secid": "D", "family_code": "DD", "classification_status": "included", "futoi_eligible": True, "schema_version": "old", "notes": "n"},
    ])
    availability = pd.DataFrame([
        {"board": "RFUD", "secid": "A", "availability_status": "available", "probe_status": "completed"},
        {"board": "RFUD", "secid": "B", "availability_status": "unavailable", "probe_status": "completed"},
        {"board": "RFUD", "secid": "C", "availability_status": "available", "probe_status": "completed"},
        {"board": "BADB", "secid": "D", "availability_status": "available", "probe_status": "completed"},
    ])
    out, selected_mask, available = mod.build_stage_eligibility(eligibility, availability, "RFUD")
    by_secid = {row["secid"]: row for row in out.to_dict("records")}
    assert selected_mask.sum() == 1
    assert len(available) == 2
    assert by_secid["A"]["futoi_eligible"] is True
    assert by_secid["A"]["futoi_check_status"] == "pass"
    assert by_secid["B"]["futoi_eligible"] is False
    assert by_secid["B"]["futoi_check_status"] == "futoi_unavailable"
    assert by_secid["C"]["futoi_eligible"] is False
    assert by_secid["D"]["futoi_eligible"] is False
    assert set(out["dataset_stage"].unique()) == {"futoi_raw"}
