import json
from pathlib import Path


CONTRACT_PATH = Path("contracts/intelligence/usdrubf_news_macro_source_registry_v1.json")
BASE_CONTRACT_PATH = Path("contracts/intelligence/usdrubf_news_macro_intelligence_v1.json")


def _load_contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _load_base_contract() -> dict:
    return json.loads(BASE_CONTRACT_PATH.read_text(encoding="utf-8"))


def test_registry_identity_and_boundary() -> None:
    contract = _load_contract()
    assert contract["project"] == "MOEX_Bot"
    assert contract["lane"] == "rub_intelligence"
    assert contract["instrument"] == "USDRUBF"
    assert contract["status"] == "source_registry_contract_only"
    boundary = contract["stage12a_boundary"]
    assert boundary["live_fetch"] is False
    assert boundary["x_api_or_scraping"] is False
    assert boundary["flowise_applied_state"] is False
    assert boundary["server_apply"] is False
    assert boundary["broker_action"] is False
    assert boundary["autonomous_trading"] is False


def test_stage_four_social_media_boundary_is_preserved() -> None:
    contract = _load_contract()
    compatibility = contract["compatibility"]
    assert compatibility["scraped_social_media_as_factual_source_remains_false"] is True
    assert compatibility["x_posts_are_discovery_only"] is True
    assert compatibility["x_post_alone_may_create_usable_news_event"] is False
    x_policy = contract["x_policy"]
    assert x_policy["candidate_event_only"] is True
    assert x_policy["usable_news_event_requires_confirmation"] is True
    assert x_policy["official_x_may_trigger_action_directly"] is False
    assert x_policy["squawk_may_trigger_action_directly"] is False
    assert x_policy["osint_may_trigger_action_directly"] is False


def test_primary_registry_has_required_high_value_sources() -> None:
    sources = {item["source_id"]: item for item in _load_contract()["primary_sources"]}
    required = {
        "cbr_press_rss",
        "cbr_events_rss",
        "moex_all_news_rss",
        "moex_fx_news_rss",
        "fed_press_all_rss",
        "fed_monetary_policy_rss",
        "bls_employment_situation_rss",
        "bls_cpi_rss",
        "bls_release_calendar",
        "us_treasury_press_releases",
        "ofac_recent_actions",
        "whitehouse_releases",
        "eu_council_press_releases",
        "eu_commission_news",
        "opec_press_releases",
        "kremlin_events",
        "minfin_ru_press_center",
        "rosstat_official_releases",
        "mfa_ru_news",
        "reuters_major_agency",
    }
    assert required.issubset(sources)
    assert all(item["references"] for item in sources.values())
    assert all(item["available_at_policy"] for item in sources.values())


def test_primary_source_groups_are_frozen_stage_four_groups() -> None:
    contract = _load_contract()
    allowed_groups = set(_load_base_contract()["source_policy"]["source_groups"])
    actual_groups = {item["group"] for item in contract["primary_sources"]}
    assert actual_groups <= allowed_groups


def test_ready_sources_are_official_only_and_blocked_sources_fail_closed() -> None:
    sources = {item["source_id"]: item for item in _load_contract()["primary_sources"]}
    ready = [item for item in sources.values() if item["stage12b_status"] == "READY_CANDIDATE"]
    assert ready
    assert all(item["tier"] in {"OFFICIAL_PRIMARY", "OFFICIAL_SECONDARY"} for item in ready)
    assert sources["reuters_major_agency"]["stage12b_status"] == "BLOCKED_PENDING_APPROVED_ROUTE_AND_RIGHTS"
    assert sources["minfin_ru_press_center"]["stage12b_status"] == "BLOCKED_PENDING_ROUTE_VERIFICATION"
    assert sources["kremlin_events"]["stage12b_status"] == "BLOCKED_PENDING_STABLE_ROUTE_ADAPTER"


def test_existing_macro_registry_is_reused() -> None:
    contract = _load_contract()
    assert contract["compatibility"]["existing_macro_registry_must_be_reused_not_duplicated"] == (
        "src/moex_research/external_data/registry.py"
    )
    assert set(contract["existing_macro_sources_to_reuse"]) == {
        "moex_brent_futures_daily",
        "cme_wti_pre_moex",
        "cbr_ruonia_daily",
        "cbr_key_rate_daily",
        "cbr_banking_liquidity_daily",
    }


def test_x_whitelist_is_exact_bounded_v1_set() -> None:
    x_sources = _load_contract()["x_discovery_whitelist"]
    by_id = {item["source_id"]: item for item in x_sources}
    assert len(x_sources) == 20
    assert set(by_id) == {
        "x_whitehouse",
        "x_us_treasury",
        "x_sec_scott_bessent",
        "x_federal_reserve",
        "x_bls_gov",
        "x_state_dept",
        "x_eu_commission",
        "x_opec_secretariat",
        "x_mfa_russia",
        "x_mid_rf",
        "x_zelenskyyua",
        "x_medvedev_russia",
        "x_reuters",
        "x_deitaone",
        "x_firstsquawk",
        "x_financialjuice",
        "x_newsquawk",
        "x_faytuks",
        "x_sentdefender",
        "x_clashreport",
    }
    assert by_id["x_federal_reserve"]["handle"] == "@federalreserve"
    assert "x_kremlin_russia_e" not in by_id
    assert "x_mario_nawfal" not in by_id
    assert {item["class"] for item in x_sources} == {
        "X_OFFICIAL_DISCOVERY",
        "X_WIRE_DISCOVERY",
        "X_SQUAWK_DISCOVERY",
        "X_OSINT_DISCOVERY",
    }
    assert all(item["url"].startswith("https://x.com/") for item in x_sources)


def test_x_confirmation_allowlist_uses_declared_source_classes() -> None:
    contract = _load_contract()
    x_policy = contract["x_policy"]
    allowed = set(x_policy["confirmation_sources_allowed"])
    assert allowed == {
        "OFFICIAL_PRIMARY",
        "OFFICIAL_SECONDARY",
        "MAJOR_AGENCY_OR_FINANCIAL_MEDIA",
    }
    assert allowed <= set(contract["source_classes"])
    assert x_policy["major_agency_confirmation_requires_approved_route_and_rights"] is True


def test_x_reposts_never_become_independent_confirmation() -> None:
    contract = _load_contract()
    x_policy = contract["x_policy"]
    assert x_policy["multiple_x_reposts_do_not_count_as_independent_confirmation"] is True
    assert x_policy["quoted_or_reposted_primary_source_must_be_resolved_to_original_publisher"] is True
    assert "One discovery candidate" in contract["deduplication_policy"]["same_x_post_repeated_by_accounts"]


def test_rights_are_fail_closed() -> None:
    rights = _load_contract()["rights_and_storage"]
    assert rights["raw_full_text_storage_default"] is False
    assert rights["x_raw_post_storage_default"] is False
    assert rights["reuters_raw_storage_default"] is False
    assert rights["redistribution_rights_assumed"] is False


def test_stage12b_gate_forbids_scraping_fallback() -> None:
    gate = _load_contract()["stage12b_entry_gate"]
    assert "no scraping fallback for blocked or licensed providers" in gate
    assert any("X as discovery-only" in item for item in gate)
