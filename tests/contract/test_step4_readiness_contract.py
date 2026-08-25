from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "configs" / "datasets" / "step4_rub_basis_carry.v1.yaml"


def _config_text() -> str:
    return CONFIG.read_text(encoding="utf-8")


def _section(text: str, start: str, end: str) -> str:
    start_index = text.index(start)
    end_index = text.index(end, start_index)
    return text[start_index:end_index]


def test_stage4_status_records_physical_acceptance() -> None:
    text = _config_text()
    assert "status: rub_basis_carry_stage4_accepted" in text

    evidence = _section(text, "  applied_state_evidence:\n", "\nreadiness_flags:\n")
    for token in (
        "status: accepted",
        'evidence_date: "2026-08-24"',
        "run_id: step4_pilot_20260824_1908",
        "binding_count: 4",
        "perpetual_quote_partition_count: 2",
        "front_next_quote_partition_count: 4",
        "tom_partition_count: 2",
        "derived_partition_count: 2",
        "accepted_pointer_count: 2",
        "usd_rub_basis_carry_row_count: 27",
        "cny_rub_basis_carry_row_count: 108",
        "physical_partition_readback_required: true",
        "required_schema_complete: true",
        "promotion_semantics: transactional_with_rollback",
    ):
        assert token in evidence


def test_stage4_readiness_flags_match_accepted_state() -> None:
    text = _config_text()
    readiness = text[text.index("readiness_flags:\n") :]
    assert "implementation_ready: true" in readiness
    assert "physical_pilot_passed: true" in readiness
    assert "accepted_pointer_ready: true" in readiness
    assert "scheduler_ready: false" in readiness
    assert "research_ready: false" in readiness
