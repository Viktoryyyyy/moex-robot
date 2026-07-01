import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    REPO_ROOT
    / "contracts"
    / "datasets"
    / "usdrubf_phase_research_data_bridge_v0.yaml"
)


REQUIRED_REFERENCE_PATHS = [
    "contracts/datasets/research_usdrubf_5m_full_history.json",
    "contracts/datasets/futures_raw_5m.v1.yaml",
    "contracts/datasets/futures_derived_d1.v1.yaml",
]


FORBIDDEN_LITERAL_PATH_FRAGMENTS = [
    "/home/trader",
    "~/moex_bot/moex_robot",
    "moex_bot/moex_robot",
]


def load_contract_text():
    return CONTRACT_PATH.read_text(encoding="utf-8")


def extract_section(text, header):
    match = re.search(
        rf"^{re.escape(header)}:\n(?P<body>(?:[ ]+.*\n|[ ]*\n)+)",
        text,
        re.MULTILINE,
    )
    assert match is not None, f"Missing YAML section: {header}"
    return match.group("body")


def scalar_value(text, key):
    match = re.search(rf"^\s*{re.escape(key)}:\s*(?P<value>.+?)\s*$", text, re.MULTILINE)
    assert match is not None, f"Missing scalar key: {key}"
    value = match.group("value").strip()
    if value in {"true", "false"}:
        return value == "true"
    if (
        len(value) >= 2
        and value[0] == value[-1]
        and value[0] in {"'", '"'}
    ):
        return value[1:-1]
    return value


def list_items(text, key):
    match = re.search(
        rf"^\s*{re.escape(key)}:\s*\n(?P<body>(?:\s+- .+\n)+)",
        text,
        re.MULTILINE,
    )
    assert match is not None, f"Missing list key: {key}"
    return [
        line.split("- ", 1)[1].strip()
        for line in match.group("body").splitlines()
        if "- " in line
    ]


def test_contract_identity_and_design_bridge_status():
    contract = load_contract_text()
    identity = extract_section(contract, "identity")

    assert scalar_value(identity, "contract_id") == "usdrubf_phase_research_data_bridge_v0"
    assert scalar_value(identity, "status") == "design_bridge_only"
    assert scalar_value(identity, "project") == "MOEX Bot"
    assert scalar_value(identity, "lane") == "ema_3_19_ai"
    assert scalar_value(identity, "instrument") == "USDRUBF"
    assert scalar_value(identity, "family") == "Si"


def test_referenced_dataset_contract_files_exist():
    for repo_relative_path in REQUIRED_REFERENCE_PATHS:
        assert (REPO_ROOT / repo_relative_path).exists(), repo_relative_path


def test_canonical_and_legacy_references_are_explicit():
    contract = load_contract_text()
    canonical_refs = extract_section(contract, "canonical_refs")
    legacy_ref = extract_section(contract, "legacy_compatibility_ref")

    assert (
        scalar_value(canonical_refs, "raw_5m_contract_ref")
        == "contracts/datasets/futures_raw_5m.v1.yaml"
    )
    assert (
        scalar_value(canonical_refs, "derived_d1_contract_ref")
        == "contracts/datasets/futures_derived_d1.v1.yaml"
    )
    assert (
        scalar_value(legacy_ref, "dataset_contract_ref")
        == "contracts/datasets/research_usdrubf_5m_full_history.json"
    )
    assert scalar_value(legacy_ref, "role") == "compatibility_only"
    assert scalar_value(legacy_ref, "canonical_for_new_data") is False


def test_canonical_raw_5m_locator_is_external_parquet_pattern():
    contract = load_contract_text()
    raw_rules = extract_section(contract, "canonical_raw_5m_locator_rules")

    pattern = scalar_value(raw_rules, "pattern")
    assert "MOEX_DATA_ROOT" in pattern
    assert "futures/raw_5m" in pattern
    assert "part.parquet" in pattern
    assert "/home/trader" not in pattern
    assert list_items(raw_rules, "required_bindings") == [
        "MOEX_DATA_ROOT",
        "trade_date",
        "family",
        "secid",
    ]
    assert scalar_value(raw_rules, "hardcoded_server_path_allowed") is False
    assert scalar_value(raw_rules, "implicit_file_selection_allowed") is False


def test_canonical_derived_d1_locator_is_external_parquet_pattern():
    contract = load_contract_text()
    derived_rules = extract_section(contract, "canonical_derived_d1_locator_rules")

    pattern = scalar_value(derived_rules, "pattern")
    assert "MOEX_DATA_ROOT" in pattern
    assert "futures/derived_d1" in pattern
    assert "part.parquet" in pattern
    assert "/home/trader" not in pattern
    assert list_items(derived_rules, "required_bindings") == [
        "MOEX_DATA_ROOT",
        "series_type",
        "family",
    ]
    assert scalar_value(derived_rules, "hardcoded_server_path_allowed") is False
    assert scalar_value(derived_rules, "implicit_file_selection_allowed") is False


def test_no_fallback_policy_is_fail_closed():
    contract = load_contract_text()
    policy = extract_section(contract, "no_fallback_policy")

    assert scalar_value(policy, "implicit_fallback_from_parquet_to_csv") == "forbidden"
    assert (
        scalar_value(policy, "implicit_fallback_from_missing_partition_to_legacy_csv")
        == "forbidden"
    )
    assert scalar_value(policy, "latest_current_autodetect_glob_locators") == "forbidden"
    assert scalar_value(policy, "missing_partition_behavior") == "fail_closed"
    assert scalar_value(policy, "legacy_csv_use") == "explicitly_declared_legacy_consumers_only"


def test_availability_rules_prevent_runtime_label_leakage():
    contract = load_contract_text()
    availability = extract_section(contract, "availability_rules")

    assert (
        scalar_value(availability, "raw_5m_rows_available_after")
        == "actual_source_ingestion_timestamp"
    )
    assert (
        scalar_value(availability, "derived_d1_known_by_when")
        == "D close after finalized D1 bar"
    )
    assert scalar_value(availability, "derived_d1_earliest_executable_point") == "D+1 open"
    assert scalar_value(availability, "manual_phase_labels_runtime_feature") is False
    assert scalar_value(availability, "future_derived_targets_runtime_feature") is False


def test_readiness_flags_do_not_claim_runtime_data_or_ingestion_readiness():
    contract = load_contract_text()
    readiness = extract_section(contract, "readiness_claims")

    assert scalar_value(readiness, "runtime_readiness_claim") is False
    assert scalar_value(readiness, "ingestion_implemented") is False
    assert scalar_value(readiness, "data_artifacts_generated") is False
    assert scalar_value(readiness, "feature_builders_migrated") is False
    assert scalar_value(readiness, "server_materialized") is False
    assert scalar_value(readiness, "backtest_run") is False
    assert scalar_value(readiness, "model_training_run") is False


def test_quality_rules_include_required_checks_and_report_fields():
    contract = load_contract_text()
    quality = extract_section(contract, "quality_rules")

    for expected_raw_check in [
        "non_null_ohlc",
        "high_ge_low",
        "open_high_low_close_inside_range",
        "duplicate_ts_secid_count",
        "monotonic_ts_by_secid",
    ]:
        assert expected_raw_check in quality

    for expected_d1_check in [
        "non_null_ohlc",
        "high_ge_low",
        "no_future_source_rows",
        "duplicate_trade_date_symbol_count",
    ]:
        assert expected_d1_check in quality

    for expected_report_field in [
        "coverage_start",
        "coverage_end",
        "missing_trade_dates",
        "source_contract_refs",
        "build_ts",
        "input_manifest_ref",
        "raw_quality_report_ref",
        "derived_quality_report_ref",
    ]:
        assert expected_report_field in quality


def test_migration_policy_preserves_legacy_contract_and_requires_later_work():
    contract = load_contract_text()
    migration = extract_section(contract, "migration_policy")

    assert scalar_value(migration, "legacy_contract_remains_unchanged_initially") is True
    assert scalar_value(migration, "future_consumers_must_explicitly_reference_bridge") is True
    assert scalar_value(migration, "feature_contract_migration_required_later") is True
    assert scalar_value(migration, "server_materialization_required_later") is True
    assert scalar_value(migration, "validation_required_before_research_enablement") is True


def test_contract_contains_no_hardcoded_server_path_or_glob_autodetect_escape():
    contract = load_contract_text()

    for forbidden_fragment in FORBIDDEN_LITERAL_PATH_FRAGMENTS:
        assert forbidden_fragment not in contract

    assert "latest_current_autodetect_glob_locators: forbidden" in contract
    assert "implicit_fallback_from_parquet_to_csv: forbidden" in contract
    assert "hardcoded_server_path_allowed: false" in contract
