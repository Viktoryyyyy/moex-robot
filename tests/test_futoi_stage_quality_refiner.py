import pandas as pd

from moex_data.futures import futoi_stage_quality_refiner as mod


def test_quality_refiner_keeps_only_pass_with_rows_and_partitions():
    eligibility = pd.DataFrame([
        {"secid": "A", "board": "RFUD", "classification_status": "included", "futoi_eligible": True, "futoi_check_status": "pass", "schema_version": "old", "notes": "n"},
        {"secid": "B", "board": "RFUD", "classification_status": "included", "futoi_eligible": True, "futoi_check_status": "pass", "schema_version": "old", "notes": "n"},
        {"secid": "C", "board": "RFUD", "classification_status": "included", "futoi_eligible": True, "futoi_check_status": "pass", "schema_version": "old", "notes": "n"},
        {"secid": "D", "board": "RFUD", "classification_status": "included", "futoi_eligible": False, "futoi_check_status": "futoi_unavailable", "schema_version": "old", "notes": "n"},
    ])
    quality = pd.DataFrame([
        {"secid": "A", "quality_status": "pass", "rows": 10, "partition_count": 2},
        {"secid": "B", "quality_status": "fail", "rows": 0, "partition_count": 0},
        {"secid": "C", "quality_status": "pass", "rows": 0, "partition_count": 0},
        {"secid": "D", "quality_status": "pass", "rows": 10, "partition_count": 2},
    ])
    out, was_selected, still_selected = mod.refine(eligibility, quality)
    by_secid = {row["secid"]: row for row in out.to_dict("records")}
    assert int(was_selected.sum()) == 3
    assert int(still_selected.sum()) == 1
    assert by_secid["A"]["futoi_eligible"] is True
    assert by_secid["A"]["futoi_check_status"] == "pass"
    assert by_secid["B"]["futoi_eligible"] is False
    assert by_secid["B"]["futoi_check_status"] == "futoi_zero_rows_deferred"
    assert by_secid["C"]["futoi_eligible"] is False
    assert by_secid["C"]["futoi_check_status"] == "futoi_zero_rows_deferred"
    assert by_secid["D"]["futoi_eligible"] is False
    assert set(out["dataset_stage"].unique()) == {"futoi_raw"}
