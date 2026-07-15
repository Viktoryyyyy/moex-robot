from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from .models import BLOCKED_STATUSES, ExternalDataError


@dataclass(frozen=True)
class SourceDefinition:
    source_id: str
    slot_id: str
    selected_for_production_loader: bool
    official_routes: tuple[str, ...]
    source_revision_status: str
    historical_model_use_status: str
    license_status: str
    availability_policy: str
    source_semantics: str | None = None


SOURCE_SLOTS: Final[tuple[str, ...]] = (
    "moex_brent_futures_daily",
    "pre_moex_global_oil_market",
    "cbr_ruonia_daily",
    "cbr_key_rate_daily",
    "cbr_banking_liquidity_daily",
)

SOURCE_REGISTRY: Final[dict[str, SourceDefinition]] = {
    "moex_brent_futures_daily": SourceDefinition(
        source_id="moex_brent_futures_daily",
        slot_id="moex_brent_futures_daily",
        selected_for_production_loader=True,
        official_routes=(
            "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json",
            "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/{contract_code}/candles.json",
        ),
        source_revision_status="official_iss_current_view",
        historical_model_use_status="blocked_pending_source_validation",
        license_status="public_official_route_attribution_required",
        availability_policy="explicit contract only; expired-contract history must be proven before Phase 8.2",
    ),
    "ine_shanghai_crude_pre_moex": SourceDefinition(
        source_id="ine_shanghai_crude_pre_moex",
        slot_id="pre_moex_global_oil_market",
        selected_for_production_loader=False,
        official_routes=("https://www.ine.cn/eng/market/futures/energy/sc/",),
        source_revision_status="official_delayed_web_view",
        historical_model_use_status="blocked_pending_historical_intraday_source",
        license_status="not_confirmed_for_automated_historical_research",
        availability_policy="rejected: no verified reproducible timestamped intraday history route",
    ),
    "cme_wti_pre_moex": SourceDefinition(
        source_id="cme_wti_pre_moex",
        slot_id="pre_moex_global_oil_market",
        selected_for_production_loader=True,
        official_routes=(
            "https://www.cmegroup.com/CmeWS/mvc/quotes/v2/425",
            "https://www.cmegroup.com/markets/energy/crude-oil/light-sweet-crude.quotes.html",
            "https://www.cmegroup.com/CmeWS/mvc/ProductCalendar/Future/425",
            "https://www.cmegroup.com/datamine.html",
        ),
        source_revision_status="official_delayed_current_snapshot",
        historical_model_use_status="blocked_pending_license",
        license_status="DataMine_account_purchase_and_information_license_required",
        availability_policy="08:45 Europe/Moscow cutoff; delayed event timestamp must be at or before cutoff",
    ),
    "cbr_ruonia_daily": SourceDefinition(
        source_id="cbr_ruonia_daily",
        slot_id="cbr_ruonia_daily",
        selected_for_production_loader=True,
        official_routes=("https://www.cbr.ru/eng/hd_base/ruonia/dynamics/",),
        source_revision_status="official_published_history",
        historical_model_use_status="candidate_for_phase8_2",
        license_status="public_reproduction_allowed_with_CBR_attribution",
        availability_policy="usable only from the row-level publication_date",
    ),
    "cbr_key_rate_daily": SourceDefinition(
        source_id="cbr_key_rate_daily",
        slot_id="cbr_key_rate_daily",
        selected_for_production_loader=True,
        official_routes=("https://www.cbr.ru/eng/hd_base/ProcStav/IR_CHG_MPO/",),
        source_revision_status="official_change_date_history",
        historical_model_use_status="candidate_for_phase8_2",
        license_status="public_official_route_attribution_required",
        availability_policy=(
            "official_key_rate_change_date_history; each row is an actual change "
            "point usable no earlier than its effective_date"
        ),
        source_semantics="official_key_rate_change_date_history",
    ),
    "cbr_banking_liquidity_daily": SourceDefinition(
        source_id="cbr_banking_liquidity_daily",
        slot_id="cbr_banking_liquidity_daily",
        selected_for_production_loader=True,
        official_routes=("https://www.cbr.ru/eng/hd_base/bliquidity/",),
        source_revision_status="latest_revised",
        historical_model_use_status="blocked_pending_vintage_policy",
        license_status="public_official_route_attribution_required",
        availability_policy="blocked until row-level historical publication vintages are governed",
    ),
}


def validate_registry() -> None:
    if len(SOURCE_SLOTS) != 5 or len(set(SOURCE_SLOTS)) != 5:
        raise ExternalDataError("exactly five unique logical source slots are required")
    if {item.slot_id for item in SOURCE_REGISTRY.values()} != set(SOURCE_SLOTS):
        raise ExternalDataError("source registry does not cover the exact source slots")
    oil = [
        item
        for item in SOURCE_REGISTRY.values()
        if item.slot_id == "pre_moex_global_oil_market"
    ]
    if {item.source_id for item in oil} != {
        "ine_shanghai_crude_pre_moex",
        "cme_wti_pre_moex",
    }:
        raise ExternalDataError("pre-MOEX candidate set mismatch")
    if sum(item.selected_for_production_loader for item in oil) != 1:
        raise ExternalDataError("exactly one pre-MOEX production source must be selected")


def require_phase8_2_ready(source_id: str) -> SourceDefinition:
    try:
        definition = SOURCE_REGISTRY[source_id]
    except KeyError as exc:
        raise ExternalDataError("unknown external source") from exc
    if definition.historical_model_use_status in BLOCKED_STATUSES:
        raise ExternalDataError(
            f"{source_id} is not Phase 8.2 ready: {definition.historical_model_use_status}"
        )
    if definition.historical_model_use_status != "candidate_for_phase8_2":
        raise ExternalDataError("source does not declare candidate_for_phase8_2")
    return definition


validate_registry()
