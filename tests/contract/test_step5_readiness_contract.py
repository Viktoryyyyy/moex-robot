from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "configs" / "datasets" / "step5_futoi_positioning.v1.yaml"


def _config_text() -> str:
    return CONFIG.read_text(encoding="utf-8")


def _section(text: str, start: str, end: str) -> str:
    start_index = text.index(start)
    end_index = text.index(end, start_index)
    return text[start_index:end_index]


def test_stage5_status_records_physical_acceptance() -> None:
    text = _config_text()
    assert "status: futoi_positioning_stage5_accepted" in text

    evidence = _section(text, "  applied_state_evidence:\n", "\nreadiness_flags:\n")
    for token in (
        "status: accepted",
        'evidence_date: "2026-08-26"',
        "run_id: step5_pilot_20260826_v9",
        "content_attestation_generation_id: stage2_content_attestation_20260826_v1",
        "acceptance_validation_mode: manifest_lineage_output_semantics_targeted_omission_oracle",
        "si_raw_partition_count: 1757",
        "cr_raw_partition_count: 1177",
        "si_eod_row_count: 1756",
        "cr_eod_row_count: 1176",
        "si_feature_row_count: 1756",
        "cr_feature_row_count: 1176",
        "source_quality_omission_count: 2",
        "accepted_pointer_count: 4",
        "expected_pointer_count: 4",
        "current_content_attestation_manifest_lineage_revalidated: true",
        "derived_output_semantic_revalidation: true",
        "physical_partition_readback_required: true",
        "source_quality_omissions_independently_revalidated: true",
        "full_frozen_raw_rehash_at_acceptance: false",
        "promotion_semantics: transactional_with_rollback",
    ):
        assert token in evidence


def test_stage5_acceptance_boundary_matches_fast_acceptance() -> None:
    text = _config_text()
    frozen = _section(text, "immutable_raw_input_freeze:\n", "\nscope:\n")
    for token in (
        "content_attestation_generation_re_resolved_at_acceptance: true",
        "marker_manifest_content_set_and_partition_sha_lineage_required: true",
        "frozen_partition_full_sha256_rehash_at_acceptance_required: false",
        "frozen_partition_hardlink_inode_identity_revalidated_at_acceptance: true",
        "derived_eod_semantic_revalidation_at_acceptance: true",
        "derived_feature_semantic_revalidation_at_acceptance: true",
        "source_quality_targeted_raw_partition_reads_only_at_acceptance: true",
    ):
        assert token in frozen
    assert "frozen_partition_hash_and_physical_revalidation_at_acceptance: true" not in frozen


def test_stage5_readiness_flags_match_accepted_state() -> None:
    text = _config_text()
    readiness = text[text.index("readiness_flags:\n") :]
    assert "implementation_ready: true" in readiness
    assert "physical_pilot_passed: true" in readiness
    assert "accepted_pointer_ready: true" in readiness
    assert "scheduler_ready: false" in readiness
    assert "research_ready: false" in readiness
