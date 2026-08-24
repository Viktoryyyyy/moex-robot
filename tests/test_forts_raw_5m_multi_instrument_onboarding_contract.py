from pathlib import Path


ARCHITECTURE_PATH = Path("contracts/architecture/moex_data_access_canon_v1.yaml")
DATA_LAKE_PATH = Path("configs/datasets/futures_data_lake.v1.yaml")
REGISTRY_PATH = Path("configs/instruments/forts_instrument_registry.v1.yaml")
RUNBOOK_PATH = Path("docs/data/futures_data_ingestion_runbook.md")
QUOTE_DATASET_PATH = Path("contracts/datasets/futures_raw_5m.v1.yaml")


REQUIRED_REGISTRY_FIELDS = [
    "instrument_id",
    "canonical_symbol",
    "market",
    "board",
    "secid",
    "source_id",
    "enabled_for_loading",
    "enabled_for_update",
    "enabled_for_retrieval",
    "enabled_for_raw_5m_materialization",
    "enabled_for_d1_derivation",
    "enabled_for_research",
    "evidence_status",
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_canonical_onboarding_declares_per_dataset_per_instrument_pointer():
    architecture = _read(ARCHITECTURE_PATH)
    quote_dataset = _read(QUOTE_DATASET_PATH)
    pointer_contract = (
        "${MOEX_DATA_ROOT}/state/datasets/dataset_id={DATASET_ID}/"
        "instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json"
    )
    quote_pointer = (
        "${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_raw_5m/"
        "instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json"
    )
    assert pointer_contract in architecture
    assert quote_pointer in quote_dataset
    assert "secid_partition_allowed: false" in architecture
    assert "new_instrument_legacy_pointer_fallback_allowed: false" in architecture
    assert "latest_autodetect_allowed: false" in architecture
    assert "direct_dynamic_scan_allowed: false" in architecture
    assert "implicit_path_selection_allowed: false" in architecture


def test_registry_required_fields_are_documented_in_architecture_and_registry():
    architecture = _read(ARCHITECTURE_PATH)
    registry = _read(REGISTRY_PATH)
    for field in REQUIRED_REGISTRY_FIELDS:
        assert field in architecture or field in registry
        assert field in registry


def test_onboarding_sequence_is_explicit_in_canonical_runbook():
    runbook = _read(RUNBOOK_PATH)
    assert "Read exact instrument binding" in runbook
    assert "Load one explicit date first" in runbook
    assert "Only then run the approved historical range" in runbook
    assert "Audit physical partitions against the aggregate manifest before acceptance" in runbook
    assert "Run one explicit-date materialization" in runbook
    assert "Reconcile `partitions_written + partitions_skipped`" in runbook
    assert "Do not create or update an accepted pointer unless the active acceptance gate explicitly authorizes it" in runbook
    assert "GitHub/repository is Source of Truth" in runbook
    assert "Server filesystem is Applied State only" in runbook


def test_scheduler_and_runtime_remain_blocked_after_raw_backfill_completion():
    architecture = _read(ARCHITECTURE_PATH)
    data_lake = _read(DATA_LAKE_PATH)
    runbook = _read(RUNBOOK_PATH)
    assert "historical_quotes_backfill_completed: true" in data_lake
    assert "priority_futoi_raw_backfill_completed: true" in data_lake
    assert "observed_source_refresh_ready: false" in data_lake
    assert "scheduler_ready: false" in data_lake
    assert "d1_materialization_ready: false" in data_lake
    assert "research_ready: false" in data_lake
    assert "accepted_pointer_ready: true" in data_lake
    assert "server_apply_requires_merged_github_contract: true" in architecture
    assert "accepted pointer remains absent until the architecture gate enables it" in runbook


def test_legacy_ingestion_contracts_are_not_architecture_proof():
    architecture = _read(ARCHITECTURE_PATH)
    runbook = _read(RUNBOOK_PATH)
    assert "legacy_contract_as_architecture_proof_allowed: false" in architecture
    assert "obsolete_ingestion_markdown_contracts_allowed_in_canonical_contract_tree: false" in architecture
    assert "`${MOEX_DATA_ROOT}/futures/raw_5m/...`" in runbook
    assert "`${MOEX_DATA_ROOT}/futures/futoi_raw/...`" in runbook
    assert "`${MOEX_DATA_ROOT}/forts/raw_5m/...` for new writes" in runbook
