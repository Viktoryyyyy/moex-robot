from moex_data.futures import stage2_raw_history_acceptance as acceptance
from moex_data.futures import stage2_raw_history_acceptance_gate as gate


def test_failed_audit_still_populates_exact_date_set_hash_fields() -> None:
    expectation = acceptance.HistoryExpectation(
        target_dataset_id=acceptance.QUOTE_DATASET_ID,
        instrument_id="usdrubf_futures_family",
        source_id=acceptance.QUOTE_SOURCE_ID,
        date_start="2026-08-16",
        date_end="2026-08-17",
        expected_partitions=1,
        expected_rows=1,
        expected_secid="USDRUBF",
    )
    expected_present = ("2026-08-17",)
    expected_missing = ("2026-08-16",)
    result = {
        "missing_partition_dates": list(expected_missing),
        "hard_check_failures": ["expected_row_count_mismatch"],
        "acceptance_status": "fail",
    }

    gate._apply_exact_date_set_evidence(
        result,
        expectation,
        gate._date_set_sha256(expected_present),
        gate._date_set_sha256(expected_missing),
    )

    assert result["acceptance_status"] == "fail"
    assert result["expected_partition_dates_sha256"] == gate._date_set_sha256(expected_present)
    assert result["actual_partition_dates_sha256"] == gate._date_set_sha256(expected_present)
    assert result["expected_missing_dates_sha256"] == gate._date_set_sha256(expected_missing)
    assert result["actual_missing_dates_sha256"] == gate._date_set_sha256(expected_missing)
    assert result["hard_check_failures"] == ["expected_row_count_mismatch"]
