from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import math
from datetime import date, datetime, timezone
from typing import Mapping, Sequence

from moex_data.futures import front_next_binding
from src.moex_research.intelligence.usdrubf_level_structure import classify_level_history
from src.moex_research.intelligence.usdrubf_live_shadow_bridge import build_previous_session_zones
from src.moex_research.runners import usdrubf_live_shadow_smoke as live
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as futoi


PROJECT = base.PROJECT
MODE = base.MODE
SOURCE_ID = "moex_algopack_fo_tradestats_5m"
SOURCE_CONTRACT_REF = "contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml"
STRUCTURAL_LEVELS_SCHEMA_VERSION = "usdrubf_structural_levels_snapshot.v1"
LEVEL_STRUCTURE_CONTRACT_REF = "contracts/intelligence/usdrubf_market_state_level_structure_v1.json"
LEVEL_STRUCTURE_ENGINE_REF = "src/moex_research/intelligence/usdrubf_level_structure.py"
USDRUBF_SECID = "USDRUBF"
STRUCTURE_TIMEFRAME = "5m"
MAX_PRIOR_SESSION_LOOKBACK_DAYS = 7


class CurrentChatSnapshotError(base.ChatAnalysisSnapshotError):
    pass


def _finite_number(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise CurrentChatSnapshotError(f"{field} must be finite numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CurrentChatSnapshotError(f"{field} must be finite numeric") from exc
    if not math.isfinite(number):
        raise CurrentChatSnapshotError(f"{field} must be finite numeric")
    return number


def _observed_session_date(
    bars: Sequence[Mapping[str, object]],
    *,
    field: str,
) -> date:
    observed: set[date] = set()
    for index, bar in enumerate(bars):
        end = bar.get("end") if isinstance(bar, Mapping) else None
        if not isinstance(end, datetime) or end.tzinfo is None or end.utcoffset() is None:
            raise CurrentChatSnapshotError(f"{field}[{index}].end must be timezone-aware")
        observed.add(end.astimezone(base.MOSCOW).date())
    if len(observed) != 1:
        raise CurrentChatSnapshotError(f"{field} must contain exactly one observed Moscow trade date")
    return next(iter(observed))


def _source_freshness(
    *,
    now_utc: datetime,
    source_timestamp: datetime,
) -> dict[str, object]:
    source_utc = base._aware(source_timestamp, "live_market_structure.source_timestamp")
    if source_utc > now_utc:
        raise CurrentChatSnapshotError("live market source timestamp is in the future")
    age_seconds = int((now_utc - source_utc).total_seconds())
    return {
        "status": "FRESH" if age_seconds <= base.STALE_AFTER_SECONDS else "STALE",
        "age_seconds": age_seconds,
        "stale_after_seconds": base.STALE_AFTER_SECONDS,
    }


def _session_extrema(
    bars: Sequence[Mapping[str, object]],
    *,
    trade_date: date,
    partial_session: bool,
) -> dict[str, object]:
    if not bars:
        raise CurrentChatSnapshotError("session extrema require closed bars")
    return {
        "trade_date": trade_date.isoformat(),
        "high": max(_finite_number(item.get("high"), "bar.high") for item in bars),
        "low": min(_finite_number(item.get("low"), "bar.low") for item in bars),
        "data_as_of": bars[-1]["end"],
        "source_timeframe": STRUCTURE_TIMEFRAME,
        "closed_bar_count": len(bars),
        "partial_session": partial_session,
    }


def _structural_levels_block(
    *,
    now_utc: datetime,
    current_trade_date: date,
    prior_trade_date: date,
    current_closed: Sequence[Mapping[str, object]],
    prior_closed: Sequence[Mapping[str, object]],
    zones: Sequence[object],
    interactions: Sequence[object],
) -> dict[str, object]:
    market_data_as_of = current_closed[-1]["end"]
    if not isinstance(market_data_as_of, datetime):
        raise CurrentChatSnapshotError("latest closed USDRUBF bar timestamp is malformed")
    freshness = _source_freshness(now_utc=now_utc, source_timestamp=market_data_as_of)

    interaction_by_level_id = {getattr(item, "level_id", None): item for item in interactions}
    level_ids = [getattr(item, "level_id", None) for item in zones]
    if len(level_ids) != len(set(level_ids)) or None in level_ids:
        raise CurrentChatSnapshotError("active structural level ids must be unique and non-empty")
    if set(interaction_by_level_id) != set(level_ids):
        raise CurrentChatSnapshotError("every active level must have exactly one interaction")

    active_levels: list[dict[str, object]] = []
    compact_interactions: list[dict[str, object]] = []
    for zone in zones:
        interaction = interaction_by_level_id[zone.level_id]
        created_utc = base._aware(zone.created_at, f"level {zone.level_id}.created_at")
        if created_utc > now_utc:
            raise CurrentChatSnapshotError("structural level creation timestamp is in the future")
        active_levels.append(
            {
                **asdict(zone),
                "age_seconds": int((now_utc - created_utc).total_seconds()),
                "structural_quality": interaction.structural_quality,
                "provenance": {
                    "component_ref": "components.live_market_structure",
                    "source_id": SOURCE_ID,
                    "source_contract_ref": SOURCE_CONTRACT_REF,
                    "requested_secid": USDRUBF_SECID,
                    "source_trade_date": prior_trade_date.isoformat(),
                    "source_data_as_of": zone.created_at,
                },
            }
        )
        compact_interactions.append(
            {
                **asdict(interaction),
                "provenance": {
                    "component_ref": "components.live_market_structure",
                    "source_id": SOURCE_ID,
                    "source_contract_ref": SOURCE_CONTRACT_REF,
                    "requested_secid": USDRUBF_SECID,
                    "source_trade_date": current_trade_date.isoformat(),
                    "source_data_as_of": market_data_as_of,
                },
            }
        )

    return {
        "schema_version": STRUCTURAL_LEVELS_SCHEMA_VERSION,
        "instrument": USDRUBF_SECID,
        "status": freshness["status"],
        "data_as_of": market_data_as_of,
        "price_context": {
            "price": _finite_number(current_closed[-1].get("close"), "current close"),
            "source_timestamp": market_data_as_of,
            "data_as_of": market_data_as_of,
            "freshness": freshness,
            "source_id": SOURCE_ID,
            "source_contract_ref": SOURCE_CONTRACT_REF,
            "requested_secid": USDRUBF_SECID,
            "source_timeframe": STRUCTURE_TIMEFRAME,
            "closed_bar_only": True,
        },
        "active_levels": active_levels,
        "level_interactions": compact_interactions,
        "observed_extrema": {
            "prior_completed_session": _session_extrema(
                prior_closed,
                trade_date=prior_trade_date,
                partial_session=False,
            ),
            "current_observed_session": _session_extrema(
                current_closed,
                trade_date=current_trade_date,
                partial_session=True,
            ),
        },
        "methodology": {
            "level_structure_contract_ref": LEVEL_STRUCTURE_CONTRACT_REF,
            "level_structure_engine_ref": LEVEL_STRUCTURE_ENGINE_REF,
            "level_generation": "existing_build_previous_session_zones",
            "interaction_classifier": "existing_classify_level_history",
            "source_timeframe": STRUCTURE_TIMEFRAME,
            "closed_bars_only": True,
            "breakout_acceptance_closed_bar_confirmation_required": True,
            "lookahead_forbidden": True,
            "all_active_levels_emitted": True,
            "ranking_applied": False,
            "session_relationship_from_observed_market_data": True,
            "calendar_dependency": False,
            "weekday_weekend_inference": False,
            "bounded_prior_session_observation_lookback_days": MAX_PRIOR_SESSION_LOOKBACK_DAYS,
        },
        "unsupported_facts": {
            "recent_d1_high_low": "NOT_EMITTED_NO_ACCEPTED_LIVE_D1_CONVENTION_IN_CURRENT_STRUCTURE_PATH",
            "swing_hh_hl_lh_ll": "NOT_EMITTED_NO_ACCEPTED_DETERMINISTIC_SWING_CONVENTION_IN_CURRENT_SOT",
        },
        "authority": {
            "directional_authority": False,
            "action_authority": False,
            "standalone_buy_sell_authority": False,
            "stage5_full_mode_ready": False,
            "stage5_pointer_promotion_performed": False,
        },
    }


def _load_usdrubf_bars_for_observed_date(_legacy_key: str, trade_date: date):
    return live._load_bars(USDRUBF_SECID, trade_date)


def _usdrubf_live_market_structure_component(now: datetime) -> base.ProducedComponent:
    now_utc = base._aware(now, "live_market_structure.now")
    now_moscow = now_utc.astimezone(base.MOSCOW)
    current_trade_date = now_moscow.date()

    current_raw = tuple(live._load_bars(USDRUBF_SECID, current_trade_date))
    if not current_raw:
        raise CurrentChatSnapshotError("current observed date has no USDRUBF 5m TradeStats bars")
    current_closed = tuple(base.closed_bars(current_raw, as_of_timestamp=now_moscow))
    if _observed_session_date(current_closed, field="current_closed") != current_trade_date:
        raise CurrentChatSnapshotError("current USDRUBF bars do not match the observed current date")

    prior_trade_date, prior_raw = base.find_prior_session(
        current_trade_date,
        loader=_load_usdrubf_bars_for_observed_date,
        max_lookback_days=MAX_PRIOR_SESSION_LOOKBACK_DAYS,
    )
    prior_closed = tuple(base.closed_bars(prior_raw, as_of_timestamp=now_moscow))
    if _observed_session_date(prior_closed, field="prior_closed") != prior_trade_date:
        raise CurrentChatSnapshotError("prior USDRUBF bars do not match the observed prior date")

    zones = tuple(build_previous_session_zones(prior_closed))
    interactions = tuple(classify_level_history(zone, current_closed) for zone in zones)
    structural_levels = _structural_levels_block(
        now_utc=now_utc,
        current_trade_date=current_trade_date,
        prior_trade_date=prior_trade_date,
        current_closed=current_closed,
        prior_closed=prior_closed,
        zones=zones,
        interactions=interactions,
    )
    market_data_as_of = current_closed[-1]["end"]
    data = {
        "instrument": USDRUBF_SECID,
        "trade_date": current_trade_date.isoformat(),
        "prior_trade_date": prior_trade_date.isoformat(),
        "market_data_as_of": market_data_as_of,
        "price": structural_levels["price_context"]["price"],
        "source_id": SOURCE_ID,
        "source_contract_ref": SOURCE_CONTRACT_REF,
        "requested_secid": USDRUBF_SECID,
        "active_levels": zones,
        "level_interactions": interactions,
        "structural_levels": structural_levels,
        "current_closed_5m_bar_count": len(current_closed),
        "prior_session_5m_bar_count": len(prior_closed),
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }
    return base.ProducedComponent(data=data, data_as_of=market_data_as_of)


def _cnyrubf_live_component(now: datetime) -> base.ProducedComponent:
    now_utc = base._aware(now, "cnyrubf_live.now")
    now_moscow = now_utc.astimezone(base.MOSCOW)
    trade_date = now_moscow.date()
    trade_date_text = trade_date.isoformat()

    bindings = front_next_binding.discover_front_next(
        root="CR",
        as_of_date=trade_date_text,
    )
    front = next((item for item in bindings if item.get("role") == "front"), None)
    if not isinstance(front, Mapping):
        raise CurrentChatSnapshotError("current CR front binding is missing")
    secid = str(front.get("secid") or "").strip()
    if not secid:
        raise CurrentChatSnapshotError("current CR front binding has no secid")
    if str(front.get("as_of_date") or "") != trade_date_text:
        raise CurrentChatSnapshotError("current CR front binding date mismatch")

    raw_bars = tuple(live._load_bars(secid, trade_date))
    if not raw_bars:
        raise CurrentChatSnapshotError("current CR front has no 5m TradeStats bars")
    closed = tuple(base.closed_bars(raw_bars, as_of_timestamp=now_moscow))
    if not closed:
        raise CurrentChatSnapshotError("current CR front has no closed 5m TradeStats bars")

    normalized: list[dict[str, object]] = []
    previous_end: datetime | None = None
    for index, bar in enumerate(closed):
        if not isinstance(bar, Mapping):
            raise CurrentChatSnapshotError("current CR 5m bar must be an object")
        end = bar.get("end")
        if not isinstance(end, datetime) or end.tzinfo is None or end.utcoffset() is None:
            raise CurrentChatSnapshotError("current CR 5m bar end must be timezone-aware")
        end_moscow = end.astimezone(base.MOSCOW)
        if end_moscow.date() != trade_date:
            raise CurrentChatSnapshotError("current CR 5m bar trade date mismatch")
        if previous_end is not None and end <= previous_end:
            raise CurrentChatSnapshotError("current CR 5m bars are not strictly increasing")
        previous_end = end
        normalized.append(
            {
                "end": end,
                "open": _finite_number(bar.get("open"), f"bar[{index}].open"),
                "high": _finite_number(bar.get("high"), f"bar[{index}].high"),
                "low": _finite_number(bar.get("low"), f"bar[{index}].low"),
                "close": _finite_number(bar.get("close"), f"bar[{index}].close"),
                "volume": _finite_number(bar.get("volume"), f"bar[{index}].volume"),
            }
        )

    observation = {
        "source_id": SOURCE_ID,
        "source_contract_ref": SOURCE_CONTRACT_REF,
        "instrument_id": "cnyrubf_front_contract",
        "root": "CR",
        "secid": secid,
        "trade_date": trade_date_text,
        "binding": dict(front),
        "open": normalized[0]["open"],
        "high": max(float(item["high"]) for item in normalized),
        "low": min(float(item["low"]) for item in normalized),
        "close": normalized[-1]["close"],
        "volume": sum(float(item["volume"]) for item in normalized),
        "last_closed_5m_bar_end": normalized[-1]["end"],
        "closed_5m_bar_count": len(normalized),
    }
    data = {
        "mode": "LIVE_ALGOPACK_FO_TRADESTATS_5M_PARTIAL_DAY_CONTEXT",
        "observation": observation,
        "action_authority": False,
        "partial_day": True,
        "implicit_latest_used": False,
    }
    return base.ProducedComponent(data=data, data_as_of=normalized[-1]["end"])


def current_producers() -> Mapping[str, base.ComponentProducer]:
    producers = dict(base.default_producers())
    producers["live_market_structure"] = _usdrubf_live_market_structure_component
    producers["cnyrubf_live"] = _cnyrubf_live_component
    return producers


def refresh_snapshot(
    *,
    now_fn=lambda: datetime.now(timezone.utc),
) -> tuple[dict[str, object], object]:
    return futoi.refresh_snapshot(now_fn=now_fn, producers=current_producers())


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish/read S7.3 current chat snapshot with governed FUTOI and current factual market context"
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--refresh", action="store_true")
    action.add_argument("--read-current", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.refresh:
            snapshot, path = refresh_snapshot()
            readiness = snapshot["readiness"]
            print(f"PROJECT={PROJECT}")
            print(f"MODE={MODE}")
            print("STATUS=COMPLETED")
            print(f"SNAPSHOT_STATUS={readiness['status']}")
            print(f"SNAPSHOT_PATH={path}")
            print(f"GENERATED_AT_UTC={snapshot['identity']['generated_at_utc']}")
            print("COMPONENT_STATUSES=" + json.dumps(readiness["component_statuses"], sort_keys=True))
            return 0
        snapshot, _path = futoi.read_current_snapshot()
        print(json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
        return 0
    except Exception as exc:
        print(f"PROJECT={PROJECT}")
        print(f"MODE={MODE}")
        print("STATUS=FAILED")
        print(f"ERROR_CLASS={exc.__class__.__name__}")
        print(f"ERROR={exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())