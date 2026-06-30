import json
from pathlib import Path


CONTRACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "contracts"
    / "features"
    / "usdrubf_external_feature_contract_v0.json"
)


REQUIRED_FEATURE_GROUPS = {
    "internal_market",
    "positioning",
    "oil",
    "rates",
    "dollar_index",
    "currency_context",
    "calendar",
    "news_events",
}

MANDATORY_COMMON_COLUMNS = {
    "session_date",
    "feature_name",
    "feature_value",
    "feature_value_type",
    "source_id",
    "source_uri_or_contract_ref",
    "source_timezone",
    "event_timestamp",
    "availability_timestamp",
    "ingestion_timestamp",
    "transformation_version",
    "quality_status",
    "leakage_classification",
}

MANDATORY_SOURCE_KEYS = {
    "source_id",
    "source_group",
    "source_status",
    "source_uri_or_contract_ref",
    "source_system",
    "instrument_or_series_id",
    "native_grain",
    "source_timezone",
    "publication_time_rule",
    "availability_timestamp_rule",
    "latency_policy",
    "allowed_for_runtime",
    "requires_ingestion",
    "requires_static_calendar",
    "requires_pm_l2_source_approval",
    "leakage_risk",
    "notes",
}

REQUIRED_SOURCE_IDS = {
    "internal.usdrubf_d1_ohlc_from_5m",
    "internal.usdrubf_d1_ema_3_19_cross_context",
    "internal.usdrubf_d1_classical_indicators",
    "positioning.moex_algopack_futoi_raw",
    "oil.brent_or_br_or_urals_proxy",
    "rates.cbr_key_rate_calendar",
    "dollar_index.dxy",
    "currency.cnyrub_proxy",
    "currency.usdrub_spot_proxy",
    "calendar.moex_forts_session",
    "calendar.futures_expiry_rollover_placeholder",
    "calendar.ru_tax_period_static",
    "calendar.ru_us_holidays_static",
    "news_events.event_schema_only",
}


def load_contract():
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_and_design_only_status():
    contract = load_contract()

    assert contract["contract_id"] == "usdrubf_external_feature_contract_v0"
    assert contract["status"] == "design_contract_only"
    assert contract["purpose"] == (
        "Define a leakage-safe D1 external feature contract for USDRUBF B/S/OUT "
        "phase research."
    )
    assert contract["runtime_readiness_claim"] is False
    assert contract["ingestion_implemented"] is False
    assert contract["feature_builders_implemented"] is False
    assert contract["data_artifacts_generated"] is False


def test_all_required_feature_groups_exist():
    contract = load_contract()

    assert REQUIRED_FEATURE_GROUPS.issubset(contract["feature_groups"])
    for group_id in REQUIRED_FEATURE_GROUPS:
        assert contract["feature_groups"][group_id]["group_id"] == group_id


def test_dxy_group_is_mandatory_but_requires_new_ingestion():
    contract = load_contract()

    dxy = contract["feature_groups"]["dollar_index"]
    assert dxy["mandatory"] is True
    assert dxy["DXY_in_v0"] is True
    assert dxy["DXY_source_provider"] == "not_approved_yet"
    assert dxy["status"] == "requires_new_ingestion"
    assert dxy["source_ids"] == ["dollar_index.dxy"]


def test_futoi_is_participant_positioning_not_open_interest():
    contract = load_contract()

    positioning = contract["feature_groups"]["positioning"]
    assert positioning["FUTOI_in_v0"] is True
    assert positioning["FUTOI_interpretation"] == "participant_positioning_only"
    assert positioning["FUTOI_is_open_interest"] is False
    assert positioning["explicit_open_interest"] == "separate_future_task"

    futoi_source = {
        row["source_id"]: row for row in contract["source_registry"]
    }["positioning.moex_algopack_futoi_raw"]
    assert "participant positioning" in futoi_source["notes"]
    assert "must not be treated as open interest" in futoi_source["notes"]


def test_news_and_rollover_are_placeholders_only():
    contract = load_contract()

    news = contract["feature_groups"]["news_events"]
    assert news["status"] == "event_schema_only"
    assert news["raw_news_ingestion"] == "forbidden_now"
    assert news["LLM_news_classification"] == "forbidden_now"

    calendar = contract["feature_groups"]["calendar"]
    assert calendar["rollover_expiry_status"] == "schema_placeholder_only"
    assert calendar["rollover_implementation"] == "forbidden_now"

    source_by_id = {row["source_id"]: row for row in contract["source_registry"]}
    assert (
        source_by_id["calendar.futures_expiry_rollover_placeholder"]["source_status"]
        == "schema_placeholder_only"
    )
    assert (
        source_by_id["news_events.event_schema_only"]["source_status"]
        == "event_schema_only"
    )


def test_common_feature_schema_contains_mandatory_columns():
    contract = load_contract()

    declared_columns = set(
        contract["common_feature_schema"]["every_feature_row_must_include"]
    )
    assert MANDATORY_COMMON_COLUMNS.issubset(declared_columns)


def test_source_registry_contains_all_required_sources_and_schema_keys():
    contract = load_contract()

    source_registry = contract["source_registry"]
    source_ids = {row["source_id"] for row in source_registry}
    assert REQUIRED_SOURCE_IDS.issubset(source_ids)

    for source in source_registry:
        assert MANDATORY_SOURCE_KEYS.issubset(source)
        assert source["source_group"] in REQUIRED_FEATURE_GROUPS


def test_alignment_rules_enforce_availability_timestamp_and_no_default_forward_fill():
    contract = load_contract()

    alignment_rules = contract["alignment_rules"]
    assert alignment_rules["primary_grain"] == "D1 session"
    assert alignment_rules["join_key"] == ["session_date"]
    assert (
        "availability_timestamp <= decision_timestamp"
        in alignment_rules["runtime_usability_rule"]
    )
    assert alignment_rules["internal_D1_close_features"] == [
        "usable_only_from_D_plus_1_open"
    ]
    assert alignment_rules["external_daily_close_features"] == [
        "align_by_latest_source_observation_available_before_decision_timestamp"
    ]
    assert alignment_rules["CBR_features"] == [
        "usable_only_after_official_publication_timestamp"
    ]
    assert alignment_rules["news_event_features"] == [
        "usable_only_after_publication_timestamp"
    ]
    assert alignment_rules["FUTOI_positioning_features"] == [
        "usable_only_after_exchange_or_source_publication_timestamp"
    ]
    assert alignment_rules["missing_feature_policy"] == [
        "keep_null_plus_quality_status",
        "no_default_forward_fill",
    ]


def test_leakage_policy_excludes_manual_labels_and_future_targets():
    contract = load_contract()

    leakage_rules = set(contract["leakage_policy"]["mandatory_rules"])
    assert "manual_phase_labels are not runtime features" in leakage_rules
    assert "phase_remaining_sessions is not a runtime feature" in leakage_rules
    assert "next_regime_if_current_ends is not a runtime feature" in leakage_rules
    assert (
        "current_regime_ends_within_1d/3d/5d are targets, not runtime features"
        in leakage_rules
    )
    assert (
        "interval_start_date and interval_end_date from manual labels are not "
        "runtime features"
        in leakage_rules
    )
    assert (
        "DXY/oil/CBR/news/calendar/FUTOI features must use actual availability "
        "timestamp"
        in leakage_rules
    )
    assert "hindsight timestamping is forbidden" in leakage_rules
    assert (
        "any feature using observations after decision_timestamp is forbidden_for_runtime"
        in leakage_rules
    )
    assert contract["leakage_policy"]["manual_phase_labels_runtime_feature"] is False
    assert contract["leakage_policy"]["future_derived_targets_runtime_features"] is False


def test_dependency_classification_preserves_mandatory_classes():
    contract = load_contract()

    dependency = contract["dependency_classification"]
    assert {
        "already_available",
        "available_but_needs_contract",
        "schema_placeholder_only",
        "not_found",
        "requires_new_ingestion",
        "blocked_by_data_coverage",
    }.issubset(dependency)

    assert "DXY source contract" in dependency["not_found"]
    assert "oil source contract" in dependency["not_found"]
    assert "CBR key rate/calendar source contract" in dependency["not_found"]
    assert "CNYRUB feature/source contract" in dependency["not_found"]
    assert "USDRUB spot proxy source contract" in dependency["not_found"]
    assert "explicit open interest contract" in dependency["not_found"]

    assert "DXY" in dependency["requires_new_ingestion"]
    assert "oil proxy" in dependency["requires_new_ingestion"]
    assert "CNYRUB proxy" in dependency["requires_new_ingestion"]
    assert dependency["blocked_by_data_coverage"] == [
        "internal USDRUBF research feature coverage ends 2026-04-06 while labels "
        "extend through 2026-06-26"
    ]


def test_forbidden_outputs_and_actions_are_represented():
    contract = load_contract()

    forbidden = set(contract["forbidden_outputs_and_actions"])
    assert "Do not implement src code." in forbidden
    assert "Do not create data artifacts." in forbidden
    assert "Do not generate CSV/parquet." in forbidden
    assert "Do not run server commands." in forbidden
    assert "Do not run runtime." in forbidden
    assert "Do not run backtest." in forbidden
    assert "Do not train model." in forbidden
    assert "Do not make market recommendations." in forbidden
    assert "Do not use deprecated server path /home/trader/moex_bot/moex_robot." in forbidden
    assert "Do not use old Контекст.md." in forbidden


def test_blockers_are_preserved_without_solving_them():
    contract = load_contract()

    blockers = set(contract["blockers_preserved"])
    assert "DXY source missing" in blockers
    assert (
        "USDRUBF research data coverage currently ends 2026-04-06 while labels "
        "extend through 2026-06-26"
        in blockers
    )
    assert (
        "FUTOI raw exists but phase-research participant positioning feature "
        "contract is missing"
        in blockers
    )
    assert "explicit open interest missing and must not be inferred from FUTOI" in blockers
    assert "news allowed only as event schema in this task" in blockers
    assert "USDRUBF D1 derivation/research enablement remains separate" in blockers
