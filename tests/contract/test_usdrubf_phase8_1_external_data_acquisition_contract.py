from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    ROOT / "contracts/experiments/usdrubf_phase8_1_external_data_acquisition_v1.json"
)
APPROVED_FILES = [
    "contracts/experiments/usdrubf_phase8_1_external_data_acquisition_v1.json",
    "src/moex_research/external_data/__init__.py",
    "src/moex_research/external_data/models.py",
    "src/moex_research/external_data/registry.py",
    "src/moex_research/external_data/cbr.py",
    "src/moex_research/external_data/oil_markets.py",
    "tests/unit/test_usdrubf_phase8_1_cbr_external_data.py",
    "tests/unit/test_usdrubf_phase8_1_oil_external_data.py",
    "tests/contract/test_usdrubf_phase8_1_external_data_acquisition_contract.py",
]
SLOTS = [
    "moex_brent_futures_daily",
    "pre_moex_global_oil_market",
    "cbr_ruonia_daily",
    "cbr_key_rate_daily",
    "cbr_banking_liquidity_daily",
]


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_branch_and_exact_repository_scope() -> None:
    contract = _contract()
    assert contract["contract_identity"] == {
        "contract_id": "usdrubf_phase8_1_external_data_acquisition_v1",
        "contract_version": "1.0",
        "project": "MOEX Bot",
        "task_id": "ema_3_19_ai_market_phase_phase_8_1_external_data_acquisition_foundation_v2",
        "lane": "ema_3_19_ai",
        "phase": "8.1",
        "execution_mode": "browser_chatgpt_github_direct",
        "status": "repository_foundation_only",
    }
    assert contract["approved_branch"] == (
        "research/ema-3-19-ai/phase-8-1-external-data-foundation"
    )
    scope = contract["approved_file_scope"]
    assert scope["create_only"] == APPROVED_FILES
    assert scope["existing_files_to_modify"] == []
    assert scope["actual_changed_file_count"] == 9
    assert scope["maximum_changed_file_count"] == 10
    assert all((ROOT / path).is_file() for path in APPROVED_FILES)


def test_exact_five_slots_candidates_and_one_selected_production_source() -> None:
    contract = _contract()
    assert contract["source_slots"] == SLOTS
    assert len(set(contract["source_slots"])) == 5
    oil = contract["sources"]["pre_moex_global_oil_market"]
    assert oil["candidates"] == [
        "ine_shanghai_crude_pre_moex",
        "cme_wti_pre_moex",
    ]
    selection = oil["selection"]
    assert selection["selected_source"] == "cme_wti_pre_moex"
    assert selection["rejected_source"] == "ine_shanghai_crude_pre_moex"
    assert selection["selected_historical_readiness_status"] == "blocked_pending_license"
    assert selection["rejected_historical_readiness_status"] == (
        "blocked_pending_historical_intraday_source"
    )


def test_official_routes_required_fields_and_provenance_are_declared() -> None:
    contract = _contract()
    sources = contract["sources"]
    for source_id in (
        "moex_brent_futures_daily",
        "cbr_ruonia_daily",
        "cbr_key_rate_daily",
        "cbr_banking_liquidity_daily",
    ):
        assert sources[source_id]["required_fields"]
        assert sources[source_id].get("official_route") or sources[source_id].get(
            "official_routes"
        )
    oil_fields = sources["pre_moex_global_oil_market"]["required_fields"]
    for required in (
        "contract_code",
        "observation_timestamp_utc",
        "cutoff_timestamp_moscow",
        "minutes_since_last_trade",
        "expiration_date",
        "raw_payload_sha256",
        "historical_model_use_status",
    ):
        assert required in oil_fields


def test_cutoff_timezone_ordering_and_contract_roll_are_point_in_time() -> None:
    oil = _contract()["sources"]["pre_moex_global_oil_market"]
    cutoff = oil["cutoff_policy"]
    assert cutoff["timezone"] == "Europe/Moscow"
    assert cutoff["modeled_day_cutoff_local_time"] == "08:45:00"
    assert cutoff["target_moex_session_start"] == "D 08:50:00 Europe/Moscow"
    assert cutoff["cme_delay_minutes_minimum"] == 10
    assert cutoff["later_same_day_observations_allowed"] is False
    assert cutoff["full_day_ohlc_or_settlement_allowed"] is False
    assert cutoff["post_cutoff_volume_for_roll_allowed"] is False
    assert cutoff["stale_quote_age_retained"] is True
    selection = oil["contract_selection"]
    assert "seven calendar days" in selection["primary_rule"]
    assert selection["same_day_post_cutoff_volume_allowed"] is False
    assert selection["continuous_ticker_allowed"] is False


def test_availability_and_blocked_status_policies_are_explicit() -> None:
    contract = _contract()
    sources = contract["sources"]
    assert sources["cbr_ruonia_daily"]["historical_model_use_status"] == (
        "candidate_for_phase8_2"
    )
    assert sources["cbr_key_rate_daily"]["historical_model_use_status"] == (
        "candidate_for_phase8_2"
    )
    assert sources["cbr_banking_liquidity_daily"]["source_revision_status"] == (
        "latest_revised"
    )
    assert sources["cbr_banking_liquidity_daily"]["historical_model_use_status"] == (
        "blocked_pending_vintage_policy"
    )
    assert contract["source_status_policy"]["blocked_source_can_enter_phase8_2_matrix"] is False
    assert contract["source_status_policy"]["fabricated_historical_observations_allowed"] is False


def test_deferred_sources_fixed_model_boundaries_and_no_runtime_dataset() -> None:
    contract = _contract()
    assert contract["deferred_sources"] == [
        "EIA Brent API",
        "Ministry of Finance budget-rule operations",
        "OFAC sanctions events",
        "EU sanctions events",
        "Bank of Russia monthly financial-market risk review",
        "tax-payment calendar",
        "exporter foreign-currency sales",
        "news sentiment",
        "LLM sentiment",
        "social-media data",
    ]
    assert not any(contract["fixed_research_components"].values())
    artifact = contract["artifact_policy"]
    assert artifact["runtime_dataset_artifact_allowed"] is False
    assert artifact["real_historical_dataset_allowed"] is False
    assert artifact["minimized_synthetic_fixtures_only"] is True
    assert artifact["secret_or_credential_allowed"] is False


def test_no_secret_or_runtime_artifact_is_committed_in_scope() -> None:
    forbidden_suffixes = {".csv", ".parquet", ".pkl", ".joblib", ".env"}
    assert not any(Path(path).suffix in forbidden_suffixes for path in APPROVED_FILES)
    combined = "\n".join(
        (ROOT / path).read_text(encoding="utf-8")
        for path in APPROVED_FILES
        if not path.startswith("tests/")
    ).lower()
    for forbidden in (
        "api_key=",
        "authorization: bearer",
        "broker_adapter",
        "predict_proba",
        "logisticregression(",
        "joblib.dump",
        "pickle.dump",
    ):
        assert forbidden not in combined
