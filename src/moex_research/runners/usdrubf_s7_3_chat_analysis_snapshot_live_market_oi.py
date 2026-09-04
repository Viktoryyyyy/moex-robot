from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone

from moex_data import live_basis_carry_context as live_basis_carry
from moex_data import synchronized_live_market_oi_context_partial as live_market
from moex_data.futures import futoi_intraday_previous_session_context_fast as fast_context
from src.moex_research.runners import s7_3_parallel_component_prefetch as parallel_prefetch
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current_context as current_context
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as futoi
from src.moex_research.runners import usdrubf_user_position_context as user_position


current_context.context = fast_context

PROJECT = current_context.PROJECT
MODE = current_context.MODE
COMPONENT = "synchronized_live_market_oi"
BASIS_CARRY_COMPONENT = "live_basis_carry"
LiveLoader = Callable[[], dict[str, object]]


class LiveMarketOIContextSnapshotError(current_context.CurrentContextSnapshotError):
    pass


def _iso_now(now_fn: Callable[[], datetime]) -> str:
    value = now_fn()
    if value.tzinfo is None:
        raise LiveMarketOIContextSnapshotError("clock must be timezone-aware")
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _load_live_or_unavailable(live_loader: LiveLoader) -> dict[str, object]:
    try:
        return live_loader()
    except Exception as exc:
        return {
            "schema_version": live_market.SCHEMA_VERSION,
            "status": "UNAVAILABLE",
            "quality": {
                "status": "FAIL",
                "analysis_usable": False,
                "factual_context_usable": False,
                "fail_closed": True,
            },
            "synchronization": {
                "status": "FAIL",
                "synchronized": False,
                "as_of_utc": None,
            },
            "error_class": exc.__class__.__name__,
            "error": str(exc),
        }


def _load_basis_carry_or_unavailable(live_snapshot: Mapping[str, object]) -> dict[str, object]:
    source_snapshot = dict(live_snapshot)
    usd_tom_supported = "usd_tom" in live_basis_carry.live_core.LOGICAL_ORDER
    instruments = live_snapshot.get("instruments")
    if isinstance(instruments, Mapping):
        sanitized_instruments = dict(instruments)
        if not usd_tom_supported:
            sanitized_instruments.pop("usd_tom", None)
        source_snapshot["instruments"] = sanitized_instruments
    try:
        derived = live_basis_carry.build_context(source_snapshot)
    except Exception as exc:
        return {
            "schema_version": live_basis_carry.SCHEMA_VERSION,
            "status": "UNAVAILABLE",
            "current_live_scope_status": "UNAVAILABLE",
            "data_as_of": None,
            "source_component_ref": live_basis_carry.SOURCE_COMPONENT_REF,
            "ready_metric_count": 0,
            "current_live_schema_usd_tom_supported": usd_tom_supported,
            "structurally_unavailable_metric_ids": [],
            "error_class": exc.__class__.__name__,
            "error": str(exc),
            "directional_authority": False,
            "action_authority": False,
            "standalone_buy_sell_authority": False,
            "stage5_full_mode_ready": False,
            "stage5_pointer_promotion_performed": False,
        }

    all_unavailable: list[str] = []
    structural_unavailable: list[str] = []
    pairs = derived.get("pairs")
    if isinstance(pairs, Mapping):
        for pair in pairs.values():
            if not isinstance(pair, Mapping):
                continue
            metrics = pair.get("metrics")
            if isinstance(metrics, (str, bytes)) or not isinstance(metrics, Sequence):
                continue
            for metric in metrics:
                if not isinstance(metric, Mapping) or metric.get("status") == "READY":
                    continue
                metric_id = str(metric.get("metric_id") or "").strip()
                if not metric_id:
                    continue
                all_unavailable.append(metric_id)
                legs = metric.get("legs")
                reason = str(metric.get("unavailable_reason") or "")
                if (
                    not usd_tom_supported
                    and isinstance(legs, Sequence)
                    and not isinstance(legs, (str, bytes))
                    and "usd_tom" in [str(value) for value in legs]
                    and reason.startswith("leg_unavailable:usd_tom:leg_not_present_in_synchronized_live_component")
                ):
                    structural_unavailable.append(metric_id)

    ready_metric_count = int(derived.get("ready_metric_count") or 0)
    current_live_scope_status = "UNAVAILABLE"
    if ready_metric_count > 0:
        current_live_scope_status = (
            "READY"
            if sorted(all_unavailable) == sorted(structural_unavailable)
            else "PARTIAL"
        )
    derived["requested_metric_availability_status"] = derived.get("status")
    derived["current_live_scope_status"] = current_live_scope_status
    derived["current_live_schema_usd_tom_supported"] = usd_tom_supported
    derived["structurally_unavailable_metric_ids"] = sorted(structural_unavailable)
    return derived


def _recompute_readiness_with_partial(snapshot: dict[str, object]) -> None:
    components = snapshot["components"]
    statuses = {name: str(value["status"]) for name, value in components.items()}
    unavailable = sorted(name for name, status in statuses.items() if status == "UNAVAILABLE")
    retained = sorted(name for name, status in statuses.items() if status == "RETAINED_PREVIOUS")
    partial = sorted(name for name, status in statuses.items() if status == "PARTIAL")
    snapshot["readiness"] = {
        "status": "READY" if not unavailable and not retained and not partial else "PARTIAL",
        "component_statuses": statuses,
        "unavailable_components": unavailable,
        "retained_previous_components": retained,
        "partial_components": partial,
    }


def attach_live_market_oi_context(
    snapshot: dict[str, object],
    live_snapshot: Mapping[str, object],
    *,
    attempted_at_utc: str,
) -> None:
    components = snapshot.get("components")
    authority = snapshot.get("authority")
    analysis_views = snapshot.get("analysis_views")
    analysis_workflow = snapshot.get("analysis_workflow")
    if not all(isinstance(value, dict) for value in (components, authority, analysis_views, analysis_workflow)):
        raise LiveMarketOIContextSnapshotError("S7.3 snapshot structure is missing")

    quality = live_snapshot.get("quality")
    synchronization = live_snapshot.get("synchronization")
    full_usable = bool(
        isinstance(quality, Mapping)
        and quality.get("analysis_usable") is True
        and isinstance(synchronization, Mapping)
        and synchronization.get("synchronized") is True
    )
    factual_usable = bool(
        full_usable
        or (
            isinstance(quality, Mapping)
            and quality.get("factual_context_usable") is True
        )
    )
    component_status = "READY" if full_usable else "PARTIAL" if factual_usable else "UNAVAILABLE"
    data_as_of = synchronization.get("as_of_utc") if isinstance(synchronization, Mapping) else None
    source_error = live_snapshot.get("error")
    components[COMPONENT] = {
        "status": component_status,
        "refresh_attempted_at": attempted_at_utc,
        "last_success_at": attempted_at_utc if factual_usable else None,
        "data_as_of": data_as_of,
        "refresh_error_class": live_snapshot.get("error_class") if not factual_usable else None,
        "refresh_error": (
            str(source_error)
            if not factual_usable and source_error is not None
            else None if factual_usable
            else "live market/OI factual quality gate failed"
        ),
        "data": dict(live_snapshot),
    }
    analysis_views["live_market_oi_component_ref"] = COMPONENT
    analysis_workflow["price_x_oi"] = {
        "consumer": "SEPARATE_ANALYSIS_CHAT",
        "component_ref": COMPONENT,
        "factual_use_requires": (
            f"components.{COMPONENT}.status in {{READY,PARTIAL}} and requested "
            f"components.{COMPONENT}.data.instruments[*].price_oi_usable=true"
        ),
        "full_cross_market_use_requires": (
            f"components.{COMPONENT}.status=READY and "
            f"components.{COMPONENT}.data.quality.analysis_usable=true and "
            f"components.{COMPONENT}.data.synchronization.synchronized=true"
        ),
        "price_oi_same_source_row_required": True,
        "cross_instrument_synchronization_required_for_cross_market_comparison": True,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
    }
    authority["live_market_oi_factual_authority"] = factual_usable
    authority["live_market_oi_directional_authority"] = False
    authority["live_market_oi_action_authority"] = False
    _recompute_readiness_with_partial(snapshot)


def attach_live_basis_carry_context(
    snapshot: dict[str, object],
    live_snapshot: Mapping[str, object],
    *,
    attempted_at_utc: str,
) -> None:
    components = snapshot.get("components")
    authority = snapshot.get("authority")
    analysis_views = snapshot.get("analysis_views")
    analysis_workflow = snapshot.get("analysis_workflow")
    if not all(isinstance(value, dict) for value in (components, authority, analysis_views, analysis_workflow)):
        raise LiveMarketOIContextSnapshotError("S7.3 snapshot structure is missing")

    derived = _load_basis_carry_or_unavailable(live_snapshot)
    status = str(derived.get("current_live_scope_status") or "UNAVAILABLE")
    if status not in {"READY", "PARTIAL", "UNAVAILABLE"}:
        status = "UNAVAILABLE"
    factual_usable = bool(int(derived.get("ready_metric_count") or 0) > 0)
    source_error = derived.get("error")
    components[BASIS_CARRY_COMPONENT] = {
        "status": status,
        "refresh_attempted_at": attempted_at_utc,
        "last_success_at": attempted_at_utc if factual_usable else None,
        "data_as_of": derived.get("data_as_of"),
        "refresh_error_class": derived.get("error_class") if not factual_usable else None,
        "refresh_error": str(source_error) if not factual_usable and source_error is not None else None,
        "data": derived,
    }
    analysis_views["fresh_basis_carry_component_ref"] = BASIS_CARRY_COMPONENT
    analysis_workflow["fresh_basis_carry"] = {
        "consumer": "SEPARATE_ANALYSIS_CHAT",
        "component_ref": BASIS_CARRY_COMPONENT,
        "source_component_ref": COMPONENT,
        "factual_use_requires": (
            f"components.{BASIS_CARRY_COMPONENT}.status in {{READY,PARTIAL}} and requested metric status=READY"
        ),
        "current_live_values_only": True,
        "additional_live_fetch_allowed": False,
        "missing_value_must_not_be_interpreted_as_zero": True,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
    }
    authority["live_basis_carry_factual_authority"] = factual_usable
    authority["live_basis_carry_directional_authority"] = False
    authority["live_basis_carry_action_authority"] = False
    _recompute_readiness_with_partial(snapshot)


def refresh_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    live_loader: LiveLoader = live_market.fetch_live_snapshot,
) -> tuple[dict[str, object], object]:
    base.load_dotenv(base.PROJECT_ENV_PATH, override=False)
    base.install_timestamp_policy()
    root = base._data_root()
    state_dir = base.snapshot_state_dir(root)
    path = state_dir / base.CURRENT_FILENAME
    with base._single_refresh_lock(state_dir):
        previous = base._load_previous(path)
        now = base._aware(now_fn(), "clock")
        through_date = now.astimezone(base.MOSCOW).date().isoformat()
        run_id = "s7_3_futoi_context_live_market_oi_" + now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        refresh_bundle = current_context.context.run_refresh_all(
            through_date=through_date,
            run_id=run_id,
            now_fn=lambda: now,
        )
        delta_bundle = current_context.delta_context.build_all(
            root=root,
            refresh_bundle=refresh_bundle,
            as_of=now,
        )
        producers = parallel_prefetch.prefetch_producers(
            current_context.current.current_producers(),
            now=now,
        )
        snapshot = futoi.build_snapshot(
            now=now,
            previous=previous,
            producers=producers,
            data_root=root,
        )
        current_context._attach_futoi_context(snapshot, refresh_bundle, delta_bundle)
        live_snapshot = _load_live_or_unavailable(live_loader)
        attempted_at = _iso_now(lambda: now)
        attach_live_market_oi_context(
            snapshot,
            live_snapshot,
            attempted_at_utc=attempted_at,
        )
        attach_live_basis_carry_context(
            snapshot,
            live_snapshot,
            attempted_at_utc=attempted_at,
        )
        user_position.attach_user_position_context(snapshot, root=root)
        base._atomic_write(path, snapshot)
    return snapshot, path


def load_live_analysis_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    reader: Callable[..., tuple[dict[str, object], object]] = current_context.read_current_snapshot,
    live_loader: LiveLoader = live_market.fetch_live_snapshot,
) -> dict[str, object]:
    base.load_dotenv(base.PROJECT_ENV_PATH, override=False)
    snapshot, _path = reader(now_fn=now_fn)
    attempted_at = _iso_now(now_fn)
    live_snapshot = _load_live_or_unavailable(live_loader)
    attach_live_market_oi_context(
        snapshot,
        live_snapshot,
        attempted_at_utc=attempted_at,
    )
    attach_live_basis_carry_context(
        snapshot,
        live_snapshot,
        attempted_at_utc=attempted_at,
    )
    return snapshot


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish/read the S7.3 analyst snapshot with synchronized live market/OI context."
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--refresh", action="store_true")
    action.add_argument("--read-live", action="store_true")
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
            print(
                "COMPONENT_STATUSES="
                + json.dumps(readiness["component_statuses"], sort_keys=True)
            )
            return 0
        snapshot = load_live_analysis_snapshot()
        print(
            json.dumps(
                snapshot,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
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
