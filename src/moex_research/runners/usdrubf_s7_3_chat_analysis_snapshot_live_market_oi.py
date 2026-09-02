from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone

from moex_data import synchronized_live_market_oi_context as live_market
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current_context as current_context
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as futoi


PROJECT = current_context.PROJECT
MODE = current_context.MODE
COMPONENT = "synchronized_live_market_oi"
LiveLoader = Callable[[], dict[str, object]]


class LiveMarketOIContextSnapshotError(current_context.CurrentContextSnapshotError):
    pass


def _iso_now(now_fn: Callable[[], datetime]) -> str:
    value = now_fn()
    if value.tzinfo is None:
        raise LiveMarketOIContextSnapshotError("clock must be timezone-aware")
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


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
    usable = bool(
        isinstance(quality, Mapping)
        and quality.get("analysis_usable") is True
        and isinstance(synchronization, Mapping)
        and synchronization.get("synchronized") is True
    )
    data_as_of = synchronization.get("as_of_utc") if isinstance(synchronization, Mapping) else None
    components[COMPONENT] = {
        "status": "READY" if usable else "UNAVAILABLE",
        "refresh_attempted_at": attempted_at_utc,
        "last_success_at": attempted_at_utc if usable else None,
        "data_as_of": data_as_of,
        "refresh_error_class": None,
        "refresh_error": None if usable else "live market/OI synchronization quality gate failed",
        "data": dict(live_snapshot),
    }
    analysis_views["live_market_oi_component_ref"] = COMPONENT
    analysis_workflow["price_x_oi"] = {
        "consumer": "SEPARATE_ANALYSIS_CHAT",
        "component_ref": COMPONENT,
        "factual_use_requires": (
            f"components.{COMPONENT}.status=READY and "
            f"components.{COMPONENT}.data.quality.analysis_usable=true"
        ),
        "price_oi_same_source_row_required": True,
        "cross_instrument_synchronization_required": True,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
    }
    authority["live_market_oi_factual_authority"] = usable
    authority["live_market_oi_directional_authority"] = False
    authority["live_market_oi_action_authority"] = False
    futoi._recompute_readiness(snapshot)


def load_live_analysis_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    reader: Callable[..., tuple[dict[str, object], object]] = current_context.read_current_snapshot,
    live_loader: LiveLoader = live_market.fetch_live_snapshot,
) -> dict[str, object]:
    snapshot, _path = reader(now_fn=now_fn)
    attempted_at = _iso_now(now_fn)
    try:
        live_snapshot = live_loader()
    except Exception as exc:
        live_snapshot = {
            "schema_version": live_market.SCHEMA_VERSION,
            "status": "UNAVAILABLE",
            "quality": {
                "status": "FAIL",
                "analysis_usable": False,
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
    attach_live_market_oi_context(
        snapshot,
        live_snapshot,
        attempted_at_utc=attempted_at,
    )
    return snapshot


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read the S7.3 analyst snapshot with synchronized live market/OI context."
    )
    parser.add_argument("--read-live", action="store_true", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    parse_args(argv)
    try:
        snapshot = load_live_analysis_snapshot()
    except Exception as exc:
        print(f"PROJECT={PROJECT}")
        print(f"MODE={MODE}")
        print("STATUS=FAILED")
        print(f"ERROR_CLASS={exc.__class__.__name__}")
        print(f"ERROR={exc}")
        return 1
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


if __name__ == "__main__":
    raise SystemExit(main())
