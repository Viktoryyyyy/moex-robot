from moex_data.futures.step5_futoi_source_quality import expected_derived_rows, omission_records


def test_attested_2025_08_11_omission_applies_to_si_and_cr_only_in_range() -> None:
    expected = [{"trade_date": "2025-08-11", "reason": "no_complete_balanced_FIZ_YUR_snapshot"}]
    for instrument_id, raw_count, derived_count in (
        ("si_futures_family", 1757, 1756),
        ("cr_futures_family", 1177, 1176),
    ):
        assert omission_records(
            instrument_id,
            start_date="2025-08-08",
            end_date="2025-08-14",
        ) == expected
        assert expected_derived_rows(
            instrument_id,
            raw_count,
            start_date="2025-08-08",
            end_date="2025-08-14",
        ) == derived_count
        assert omission_records(
            instrument_id,
            start_date="2025-08-12",
            end_date="2025-08-14",
        ) == []
        assert expected_derived_rows(
            instrument_id,
            raw_count,
            start_date="2025-08-12",
            end_date="2025-08-14",
        ) == raw_count
