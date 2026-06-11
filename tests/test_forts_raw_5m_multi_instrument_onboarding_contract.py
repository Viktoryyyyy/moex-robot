from pathlib import Path


CONTRACT_PATH = Path("contracts/datasets/forts_raw_5m_multi_instrument_onboarding.v1.yaml")
REGISTRY_PATH = Path("configs/instruments/forts_instrument_registry.v1.yaml")
DOC_PATH = Path("docs/data/forts_raw_5m_multi_instrument_onboarding.md")


REQUIRED_REGISTRY_FIELDS = [
    "instrument_id",
    "canonical_symbol",
    "display_name",
    "market",
    "board",
    "secid",
    "source_artifact_id",
    "raw_5m_artifact_id",
    "enabled_for_raw_5m_materialization",
    "enabled_for_d1_derivation",
    "enabled_for_research",
    "storage_partition_values.instrument_id",
    "storage_partition_values.secid",
    "evidence_status",
]


REQUIRED_ONBOARDING_STEPS = [
    "add_registry_entry",
    "run_one_date_pilot_materialization",
    "run_full_backfill",
    "validate_manifest_and_quality",
    "create_per_instrument_accepted_pointer",
    "run_observed_source_refresh_check",
    "enable_scheduler_only_after_refresh_check_passed",
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_multi_instrument_onboarding_contract_exists_and_declares_per_instrument_pointer():
    contract_text = _read(CONTRACT_PATH)
    pointer_contract = (
        "${MOEX_DATA_ROOT}/state/datasets/artifact_id=dataset.forts.raw_5m.tradestats.v1/"
        "instrument_id={INSTRUMENT_ID}/secid={SECID}/current_accepted_manifest.json"
    )
    assert "selected_strategy: per_instrument" in contract_text
    assert "intentionally_single_instrument_only: false" in contract_text
    assert pointer_contract in contract_text
    assert "legacy_artifact_level_pointer_status: existing_usdrubf_legacy_only_not_allowed_for_new_instrument_onboarding_or_scheduler" in contract_text
    assert "legacy_artifact_level_pointer_compatibility_rule: fallback_allowed_only_for_existing_usdrubf_when_per_instrument_pointer_is_absent" in contract_text
    assert "legacy_artifact_level_pointer_migration_rule: no_silent_migration_runner_reads_existing_usdrubf_legacy_pointer_until_operator_creates_per_instrument_pointer" in contract_text
    assert "new_instrument_legacy_pointer_fallback_allowed: false" in contract_text
    assert "latest_file_autodetect_allowed: false" in contract_text
    assert "glob_discovery_allowed: false" in contract_text
    assert "implicit_path_selection_allowed: false" in contract_text


def test_registry_required_fields_are_documented_in_contract_and_registry():
    contract_text = _read(CONTRACT_PATH)
    registry_text = _read(REGISTRY_PATH)
    for field in REQUIRED_REGISTRY_FIELDS:
        assert field in contract_text
        assert field in registry_text


def test_onboarding_sequence_is_explicit_in_contract_registry_and_docs():
    contract_text = _read(CONTRACT_PATH)
    registry_text = _read(REGISTRY_PATH)
    docs_text = _read(DOC_PATH)
    for step in REQUIRED_ONBOARDING_STEPS:
        assert step in contract_text
        assert step in registry_text
    assert "Run one-date pilot materialization" in docs_text
    assert "Run full backfill" in docs_text
    assert "Create the per-instrument accepted pointer" in docs_text
    assert "Run observed-source refresh check" in docs_text
    assert "Backward compatibility" in docs_text
    assert "forts.usdrubf" in docs_text
    assert "does not silently migrate pointer state" in docs_text


def test_production_scheduler_policy_requires_observed_source_and_blocks_calendar_cron():
    contract_text = _read(CONTRACT_PATH)
    registry_text = _read(REGISTRY_PATH)
    docs_text = _read(DOC_PATH)
    assert "required_incremental_mode: observed-source" in contract_text
    assert "required_cli_argument: --incremental-mode observed-source" in contract_text
    assert "calendar_mode_allowed_for_server_cron: false" in contract_text
    assert "calendar_mode_blocked_until: moex_calendar_endpoint_contract_resolved" in contract_text
    assert "cron_or_systemd_change_in_this_patch_allowed: false" in contract_text
    assert "server_apply_in_this_patch_allowed: false" in contract_text
    assert "d1_enablement_in_this_patch_allowed: false" in contract_text
    assert "required_incremental_mode: observed-source" in registry_text
    assert "calendar_mode_allowed_for_server_cron: false" in registry_text
    assert "--incremental-mode observed-source" in docs_text
    assert "Calendar mode is not allowed for server cron" in docs_text
