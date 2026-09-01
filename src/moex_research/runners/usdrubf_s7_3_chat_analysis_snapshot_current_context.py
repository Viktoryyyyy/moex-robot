from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Callable, Sequence

from moex_data.futures import futoi_intraday_previous_session_context as context
from moex_data.futures import futoi_live_factual_refresh_source_native as futoi_source
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current as current
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as futoi


PROJECT = base.PROJECT
MODE = base.MODE


class CurrentContextSnapshotError(base.ChatAnalysisSnapshotError):
    pass


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _attach_futoi_context(
    snapshot: dict[str, object],
    refresh_bundle: Mapping[str, object],
) -> None:
    instrument_results = refresh_bundle.get("instrument_results")
    if not isinstance(instrument_results, Mapping):
        raise CurrentContextSnapshotError("FUTOI context refresh bundle has no instrument_results")
    components = snapshot.get("components")
    authority = snapshot.get("authority")
    if not isinstance(components, dict) or not isinstance(authority, dict):
        raise CurrentContextSnapshotError("S7.3 snapshot structure is missing")
    governance_values = futoi._load_governance()
    authority_by_instrument = authority.get("futoi_by_instrument")
    if not isinstance(authority_by_instrument, dict):
        authority_by_instrument = {}
        authority["futoi_by_instrument"] = authority_by_instrument

    for instrument_id, component_name in futoi.FUTOI_COMPONENT_BY_INSTRUMENT.items():
        raw_context = instrument_results.get(instrument_id)
        if not isinstance(raw_context, Mapping):
            raise CurrentContextSnapshotError("FUTOI context result is missing for " + instrument_id)
        current_view = raw_context.get(context.CURRENT_ROLE)
        previous_view = raw_context.get(context.PREVIOUS_ROLE)
        if not isinstance(current_view, Mapping) or not isinstance(previous_view, Mapping):
            raise CurrentContextSnapshotError("FUTOI current/previous context is missing for " + instrument_id)
        governance = futoi._governance_state(governance_values, instrument_id)
        allowed = governance.get("factual_use_allowed") is True
        current_factual = current_view.get("factual")
        previous_factual = previous_view.get("factual")
        current_has_factual = isinstance(current_factual, Mapping)
        previous_has_factual = isinstance(previous_factual, Mapping)
        fully_ready = (
            allowed
            and current_view.get("status") == "FRESH"
            and previous_view.get("status") == "FRESH"
            and current_has_factual
            and previous_has_factual
        )
        retained = current_view.get("status") in {
            "RETAINED_STALE",
            "UNAVAILABLE_RETAINED_STALE",
        } and current_has_factual

        existing = components.get(component_name)
        component = _mapping(existing)
        existing_data = _mapping(component.get("data"))
        factual_authority = bool(allowed and (current_has_factual or previous_has_factual))
        existing_data.update(
            {
                "source_id": futoi_source.SOURCE_ID,
                "instrument_id": instrument_id,
                "candidate_status": raw_context.get("status"),
                "quality_status": raw_context.get("quality_status"),
                "acceptance_status": raw_context.get("acceptance_status"),
                "current_intraday": dict(current_view),
                "previous_completed_session": dict(previous_view),
                "factual": dict(current_factual) if current_has_factual else None,
                "freshness": (
                    dict(current_view.get("freshness"))
                    if isinstance(current_view.get("freshness"), Mapping)
                    else None
                ),
                "provenance": (
                    dict(current_view.get("provenance"))
                    if isinstance(current_view.get("provenance"), Mapping)
                    else current_view.get("provenance")
                ),
                "context_refresh": {
                    "schema_version": raw_context.get("schema_version"),
                    "through_date": raw_context.get("through_date"),
                    "refresh_attempted_at": raw_context.get("refresh_attempted_at"),
                    "observed_trade_dates": raw_context.get("observed_trade_dates"),
                    "observed_current_trade_date": raw_context.get("observed_current_trade_date"),
                    "previous_observed_trade_date": raw_context.get("previous_observed_trade_date"),
                    "calendar_dependency": False,
                    "weekday_weekend_inference": False,
                },
                "governance": governance,
                "factual_authority": factual_authority,
                "consumer_factual_use_allowed": factual_authority,
                "directional_authority": False,
                "action_authority": False,
                "standalone_buy_sell_authority": False,
                "stage5_full_mode_required": False,
                "stage5_full_mode_ready": False,
                "stage5_pointer_promotion_performed": False,
                "missing_or_blocked_must_not_be_interpreted_as_neutral": True,
            }
        )
        if fully_ready:
            status = "READY"
        elif retained and allowed:
            status = "RETAINED_PREVIOUS"
        else:
            status = "UNAVAILABLE"
        data_as_of = (
            current_factual.get("snapshot_ts")
            if current_has_factual
            else (
                previous_factual.get("snapshot_ts")
                if previous_has_factual
                else None
            )
        )
        component.update(
            {
                "status": status,
                "refresh_attempted_at": raw_context.get("refresh_attempted_at"),
                "last_success_at": current_view.get("last_success_at"),
                "data_as_of": data_as_of,
                "refresh_error_class": current_view.get("refresh_error_class"),
                "refresh_error": current_view.get("refresh_error"),
                "data": existing_data,
            }
        )
        components[component_name] = component
        authority_by_instrument[instrument_id] = {
            "component_ref": component_name,
            "factual_authority": factual_authority,
            "directional_authority": False,
            "action_authority": False,
            "standalone_buy_sell_authority": False,
        }

    si_authority = authority_by_instrument[futoi_source.SI_INSTRUMENT_ID]
    authority["futoi_factual_authority"] = bool(si_authority["factual_authority"])
    authority["futoi_directional_authority"] = False
    authority["futoi_action_authority"] = False
    analysis_views = snapshot.get("analysis_views")
    if isinstance(analysis_views, dict):
        analysis_views["futoi_context_fields"] = {
            "current": "current_intraday",
            "previous": "previous_completed_session",
        }
    futoi._recompute_readiness(snapshot)


def refresh_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
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
        run_id = "s7_3_futoi_context_" + now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        refresh_bundle = context.run_refresh_all(
            through_date=through_date,
            run_id=run_id,
            now_fn=lambda: now,
        )
        snapshot = futoi.build_snapshot(
            now=now,
            previous=previous,
            producers=current.current_producers(),
            data_root=root,
        )
        _attach_futoi_context(snapshot, refresh_bundle)
        base._atomic_write(path, snapshot)
    return snapshot, path


def read_current_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> tuple[dict[str, object], object]:
    return futoi.read_current_snapshot(now_fn=now_fn)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish/read S7.3 current chat snapshot with intraday and previous-session "
            "Si/CR FUTOI factual context"
        )
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
            print(
                "COMPONENT_STATUSES="
                + json.dumps(readiness["component_statuses"], sort_keys=True)
            )
            return 0
        snapshot, _path = read_current_snapshot()
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
