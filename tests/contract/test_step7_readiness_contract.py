from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "configs" / "datasets" / "step7_rub_native_d1_w1_technical.v1.yaml"


def _config_text() -> str:
    return CONFIG.read_text(encoding="utf-8")


def _section(text: str, start: str, end: str) -> str:
    start_index = text.index(start)
    end_index = text.index(end, start_index)
    return text[start_index:end_index]


def test_stage7_status_records_physical_acceptance() -> None:
    text = _config_text()
    assert "status: rub_native_d1_w1_technical_stage7_accepted" in text

    evidence = _section(text, "  applied_state_evidence:\n", "\nreadiness_flags:\n")
    for token in (
        "status: accepted",
        'evidence_date: "2026-08-27"',
        "run_id: step7_pilot_20260827_v1",
        "acceptance_contract_id: step7_rub_native_d1_w1_technical_acceptance.v1",
        "content_attestation_generation_id: stage2_content_attestation_20260826_v1",
        "stage2_content_attestation_marker_sha256: 03ef2b6d554ce8857af614275ebe6ba699a47cd6e77f9507aa204c6424f789ff",
        "usdrubf_raw_partition_count: 1100",
        "cnyrubf_raw_partition_count: 1100",
        "usdrubf_d1_ohlcv_row_count: 1100",
        "cnyrubf_d1_ohlcv_row_count: 1100",
        "usdrubf_w1_ohlcv_row_count: 224",
        "cnyrubf_w1_ohlcv_row_count: 224",
        "usdrubf_d1_technical_row_count: 1100",
        "cnyrubf_d1_technical_row_count: 1100",
        "usdrubf_w1_technical_row_count: 224",
        "cnyrubf_w1_technical_row_count: 224",
        "accepted_pointer_count: 8",
        "expected_pointer_count: 8",
        "physical_partition_readback_required: true",
        "frozen_raw_physical_revalidation_required: true",
        "current_accepted_raw_scope_match_required: true",
        "independent_d1_w1_oracle_required: true",
        "independent_technical_oracle_required: true",
        "output_single_descriptor_capture_required: true",
        "output_content_sha256_binding_required: true",
        "output_identity_sha256_prewrite_recheck_required: true",
        "promotion_semantics: serialized_transactional_with_rollback",
    ):
        assert token in evidence


def test_stage7_readiness_flags_match_accepted_state_without_expanding_scope() -> None:
    text = _config_text()
    readiness = text[text.index("readiness_flags:\n") :]
    for token in (
        "implementation_ready: true",
        "physical_pilot_passed: true",
        "accepted_pointer_ready: true",
        "scheduler_ready: false",
        "research_ready: false",
        "si_cr_continuous_ready: false",
        "weekly_oi_ready: false",
        "advanced_technical_policy_ready: false",
    ):
        assert token in readiness


def test_stage7_accepted_evidence_does_not_normalize_counts_or_policy_gaps() -> None:
    text = _config_text()
    native_history = _section(text, "native_history:\n", "\ninput_freeze_policy:\n")
    assert native_history.count("expected_d1_rows: 1100") == 2
    assert "production_dependency_enabled: false" in text
    assert "fixed_expiry_substitution_allowed: false" in text
    assert "fabricated_oi_allowed: false" in text
