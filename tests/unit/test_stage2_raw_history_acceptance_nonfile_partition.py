from pathlib import Path

from moex_data.futures import stage2_raw_history_acceptance as acceptance
from moex_data.futures import stage2_raw_history_acceptance_gate as gate


def test_nonfile_partition_path_is_missing_for_exact_date_set(tmp_path, monkeypatch) -> None:
    partition_path = tmp_path / "part.parquet"
    partition_path.mkdir()
    report_path = tmp_path / "acceptance_report.json"
    expectation = acceptance.HistoryExpectation(
        target_dataset_id=acceptance.QUOTE_DATASET_ID,
        instrument_id="usdrubf_futures_family",
        source_id=acceptance.QUOTE_SOURCE_ID,
        date_start="2026-08-17",
        date_end="2026-08-17",
        expected_partitions=0,
        expected_rows=0,
        expected_secid=None,
    )

    monkeypatch.setattr(acceptance, "_expectation", lambda *args, **kwargs: expectation)
    monkeypatch.setattr(acceptance, "_contract_path", lambda *args, **kwargs: "unused")
    monkeypatch.setattr(acceptance, "_partition_path", lambda **kwargs: partition_path)
    monkeypatch.setattr(acceptance, "_acceptance_path", lambda *args, **kwargs: report_path)

    result = acceptance.audit_history(
        repo_root=Path("."),
        target_dataset_id=acceptance.QUOTE_DATASET_ID,
        instrument_id="usdrubf_futures_family",
        run_id="nonfile_partition",
    )

    assert result["actual_partition_count"] == 0
    assert result["missing_partition_dates"] == ["2026-08-17"]
    assert result["failed_partition_dates"][0]["error"] == "canonical partition path is not a file"

    gate._apply_exact_date_set_evidence(
        result,
        expectation,
        gate._date_set_sha256(()),
        gate._date_set_sha256(("2026-08-17",)),
    )

    assert result["actual_partition_dates_sha256"] == gate._date_set_sha256(())
    assert result["actual_missing_dates_sha256"] == gate._date_set_sha256(("2026-08-17",))
