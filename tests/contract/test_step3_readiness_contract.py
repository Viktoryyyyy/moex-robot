from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_step3_readiness_records_physical_acceptance_without_opening_downstream_gates() -> None:
    text = (ROOT / "configs/datasets/step3_canonical_raw.v1.yaml").read_text(encoding="utf-8")
    architecture = (ROOT / "contracts/architecture/moex_data_access_canon_v1.yaml").read_text(encoding="utf-8")
    assert text.startswith("config_id: step3_canonical_raw.v1\nstatus: raw_canonical_stage3_accepted\n")
    assert "storage_transition_policy:\n  status: raw_canonical_stage3_accepted\n" in architecture

    evidence = text.split("  applied_state_evidence:\n", 1)[1].split("\nreadiness_flags:\n", 1)[0]
    for token in (
        "    status: accepted",
        '    evidence_date: "2026-08-24"',
        "    run_id: step3_pilot_20260824_1705",
        "    binding_count: 4",
        "    quote_partition_count: 4",
        "    open_interest_partition_count: 4",
        "    tom_partition_count: 2",
        "    accepted_pointer_count: 10",
        "    promotion_semantics: transactional_with_rollback",
    ):
        assert token in evidence

    readiness = text.split("\nreadiness_flags:\n", 1)[1]
    for token in (
        "  physical_pilot_passed: true",
        "  accepted_pointer_ready: true",
        "  continuous_series_ready: false",
        "  scheduler_ready: false",
        "  research_ready: false",
    ):
        assert token in readiness
