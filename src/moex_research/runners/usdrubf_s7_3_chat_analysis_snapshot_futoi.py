from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence

from moex_data.futures import futoi_live_factual_refresh_source_native as futoi_source
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base


PROJECT = base.PROJECT
MODE = base.MODE
SCHEMA_VERSION = base.SCHEMA_VERSION
FUTOI_COMPONENT = "futoi_live"
FUTOI_CR_COMPONENT = "futoi_live_cr"
FUTOI_COMPONENT_BY_INSTRUMENT = {
    futoi_source.SI_INSTRUMENT_ID: FUTOI_COMPONENT,
    futoi_source.CR_INSTRUMENT_ID: FUTOI_CR_COMPONENT,
}
FUTOI_GOVERNANCE_RELATIVE_PATH = Path(
    "contracts/intelligence/usdrubf_futoi_live_acceptance_governance_v1.json"
)
FUTOI_CURRENT_BASE_RELATIVE_PATH = Path(
    "state/datasets/dataset_id=futoi_live_factual_context"
)
REPO_ROOT = Path(__file__).resolve().parents[3]


class FutoiSnapshotComponentError(base.ChatAnalysisSnapshotError):
    pass


def _load_json(path: Path, field: str) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise FutoiSnapshotComponentError(f"{field} must be a regular non-symlink JSON file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FutoiSnapshotComponentError(f"{field} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise FutoiSnapshotComponentError(f"{field} must contain a JSON object")
    return value


def _load_governance() -> dict[str, object]:
    values = _load_json(REPO_ROOT / FUTOI_GOVERNANCE_RELATIVE_PATH, "FUTOI governance contract")
    if values.get("project") != PROJECT:
        raise FutoiSnapshotComponentError("FUTOI governance project mismatch")
    if values.get("contract_id") != "usdrubf_futoi_live_acceptance_governance_v1":
        raise FutoiSnapshotComponentError("FUTOI governance contract identity mismatch")
    gates = values.get("gates")
    if isinstance(gates, (str, bytes)) or not isinstance(gates, Sequence) or not gates:
        raise FutoiSnapshotComponentError("FUTOI governance gates must be a non-empty array")
    authority = values.get("authority")
    if not isinstance(authority, Mapping):
        raise FutoiSnapshotComponentError("FUTOI governance authority must be an object")
    instrument_scope = values.get("instrument_scope")
    if isinstance(instrument_scope, (str, bytes)) or not isinstance(instrument_scope, Sequence):
        raise FutoiSnapshotComponentError("FUTOI governance instrument_scope must be an array")
    if set(str(value) for value in instrument_scope) != set(FUTOI_COMPONENT_BY_INSTRUMENT):
        raise FutoiSnapshotComponentError("FUTOI governance instrument_scope mismatch")
    instrument_acceptance = values.get("instrument_acceptance")
    if not isinstance(instrument_acceptance, Mapping):
        raise FutoiSnapshotComponentError("FUTOI governance instrument_acceptance must be an object")
    if set(str(value) for value in instrument_acceptance) != set(FUTOI_COMPONENT_BY_INSTRUMENT):
        raise FutoiSnapshotComponentError("FUTOI governance instrument_acceptance scope mismatch")
    return values


def _governance_state(values: Mapping[str, object], instrument_id: str) -> dict[str, object]:
    if instrument_id not in FUTOI_COMPONENT_BY_INSTRUMENT:
        raise FutoiSnapshotComponentError("unsupported FUTOI governance instrument_id")
    gates = values["gates"]
    blocked: list[str] = []
    required: list[str] = []
    for raw in gates:
        if not isinstance(raw, Mapping):
            raise FutoiSnapshotComponentError("FUTOI governance gate must be an object")
        gate_id = str(raw.get("gate_id") or "").strip()
        if not gate_id:
            raise FutoiSnapshotComponentError("FUTOI governance gate_id is required")
        if raw.get("required") is True:
            required.append(gate_id)
            if raw.get("status") != "PASS":
                blocked.append(gate_id)
    instrument_acceptance = values["instrument_acceptance"]
    raw_instrument = instrument_acceptance.get(instrument_id)
    if not isinstance(raw_instrument, Mapping):
        raise FutoiSnapshotComponentError("FUTOI instrument governance entry is missing")
    factual = raw_instrument.get("factual_live_authority") is True
    directional = raw_instrument.get("directional_authority") is True
    action = raw_instrument.get("action_authority") is True
    standalone = raw_instrument.get("standalone_buy_sell_authority") is True
    all_required_pass = not blocked
    if directional or action or standalone:
        raise FutoiSnapshotComponentError("FUTOI factual governance must not grant directional/action authority")
    local_blockers_raw = raw_instrument.get("local_blockers")
    if isinstance(local_blockers_raw, (str, bytes)) or not isinstance(local_blockers_raw, Sequence):
        raise FutoiSnapshotComponentError("FUTOI instrument local_blockers must be an array")
    local_blockers = [str(value) for value in local_blockers_raw]
    canonical_live_smoke_accepted = raw_instrument.get("canonical_live_smoke_accepted") is True
    instrument_status = str(raw_instrument.get("status") or values.get("status") or "")
    instrument_local_acceptance_pass = (
        canonical_live_smoke_accepted
        and not local_blockers
        and instrument_status == "FUTOI_LIVE_ACCEPTED_FACTUAL_CONTEXT_ONLY"
    )
    return {
        "contract_ref": FUTOI_GOVERNANCE_RELATIVE_PATH.as_posix(),
        "status": instrument_status,
        "instrument_id": instrument_id,
        "required_gate_ids": required,
        "blocked_gate_ids": blocked,
        "local_blockers": local_blockers,
        "canonical_live_smoke_accepted": canonical_live_smoke_accepted,
        "instrument_local_acceptance_pass": instrument_local_acceptance_pass,
        "all_required_gates_pass": all_required_pass,
        "factual_live_authority": factual,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "factual_use_allowed": all_required_pass and instrument_local_acceptance_pass and factual,
    }


def _candidate_path(root: Path, instrument_id: str) -> Path:
    if instrument_id not in FUTOI_COMPONENT_BY_INSTRUMENT:
        raise FutoiSnapshotComponentError("unsupported FUTOI candidate instrument_id")
    return FUTOI_CURRENT_BASE_RELATIVE_PATH / ("instrument_id=" + instrument_id) / "current.json"


def _load_candidate(root: Path, instrument_id: str) -> dict[str, object]:
    identity = futoi_source.source_identity(instrument_id)
    path = root / _candidate_path(root, instrument_id)
    value = _load_json(path, "FUTOI factual current artifact")
    if value.get("project") != PROJECT:
        raise FutoiSnapshotComponentError("FUTOI factual artifact project mismatch")
    if value.get("schema_version") != futoi_source.SCHEMA_VERSION:
        raise FutoiSnapshotComponentError("FUTOI factual artifact schema mismatch")
    if value.get("source_id") != futoi_source.SOURCE_ID:
        raise FutoiSnapshotComponentError("FUTOI factual artifact source mismatch")
    if value.get("instrument_id") != instrument_id:
        raise FutoiSnapshotComponentError("FUTOI factual artifact instrument mismatch")
    if value.get("status") != "PASS" or value.get("quality_status") != "PASS":
        raise FutoiSnapshotComponentError("FUTOI factual artifact quality/status is not PASS")
    if value.get("acceptance_status") != "PASS":
        raise FutoiSnapshotComponentError("FUTOI factual artifact acceptance_status is not PASS")
    if value.get("factual_authority") is not False:
        raise FutoiSnapshotComponentError("FUTOI source artifact must not self-grant factual authority")
    if (
        value.get("directional_authority") is not False
        or value.get("action_authority") is not False
        or value.get("standalone_buy_sell_authority") is not False
    ):
        raise FutoiSnapshotComponentError("FUTOI source artifact must keep directional/action authority false")
    if value.get("stage5_full_mode_ready") is not False:
        raise FutoiSnapshotComponentError("FUTOI source artifact must keep Stage5 full mode disabled")
    if value.get("stage5_pointer_promotion_performed") is not False:
        raise FutoiSnapshotComponentError("FUTOI source artifact must not promote a Stage5 pointer")
    factual = value.get("factual")
    provenance = value.get("provenance")
    freshness = value.get("freshness")
    if not isinstance(factual, Mapping) or not isinstance(provenance, Mapping):
        raise FutoiSnapshotComponentError("FUTOI factual/provenance payload is missing")
    if not isinstance(freshness, Mapping) or freshness.get("status") != "FRESH":
        raise FutoiSnapshotComponentError("FUTOI factual artifact is not source-fresh")
    if str(factual.get("source_ticker") or "").strip().lower() != identity["source_ticker"].lower():
        raise FutoiSnapshotComponentError("FUTOI factual payload source_ticker mismatch")
    if str(factual.get("secid") or "").strip() != identity["secid"]:
        raise FutoiSnapshotComponentError("FUTOI factual payload secid mismatch")
    data_as_of = value.get("data_as_of")
    if not isinstance(data_as_of, str):
        raise FutoiSnapshotComponentError("FUTOI factual data_as_of is missing")
    base._aware(data_as_of, "futoi_live.data_as_of")
    if factual.get("snapshot_ts") != data_as_of:
        raise FutoiSnapshotComponentError("FUTOI factual snapshot_ts/data_as_of mismatch")
    return value


def _previous_ready_futoi(
    previous: Mapping[str, object] | None,
    *,
    component_name: str,
    instrument_id: str,
) -> Mapping[str, object] | None:
    if previous is None:
        return None
    components = previous.get("components")
    if not isinstance(components, Mapping):
        return None
    value = components.get(component_name)
    if not isinstance(value, Mapping):
        return None
    data = value.get("data")
    if not isinstance(data, Mapping) or data.get("factual_authority") is not True:
        return None
    if data.get("instrument_id") != instrument_id:
        return None
    if value.get("status") not in {"READY", "RETAINED_PREVIOUS"}:
        return None
    return value


def _futoi_component(
    *,
    root: Path,
    now: datetime,
    previous: Mapping[str, object] | None,
    governance_values: Mapping[str, object],
    instrument_id: str,
    component_name: str,
) -> dict[str, object]:
    attempted_at = base._iso(now)
    governance = _governance_state(governance_values, instrument_id)
    allowed = governance["factual_use_allowed"] is True

    try:
        candidate = _load_candidate(root, instrument_id)
    except Exception as exc:
        if allowed:
            prior = _previous_ready_futoi(
                previous,
                component_name=component_name,
                instrument_id=instrument_id,
            )
            if prior is not None:
                return {
                    "status": "RETAINED_PREVIOUS",
                    "refresh_attempted_at": attempted_at,
                    "last_success_at": prior.get("last_success_at"),
                    "data_as_of": prior.get("data_as_of"),
                    "refresh_error_class": exc.__class__.__name__,
                    "refresh_error": str(exc),
                    "data": prior.get("data"),
                }
            return {
                "status": "UNAVAILABLE",
                "refresh_attempted_at": attempted_at,
                "last_success_at": None,
                "data_as_of": None,
                "refresh_error_class": exc.__class__.__name__,
                "refresh_error": str(exc),
                "data": None,
            }
        return {
            "status": "GOVERNED_BLOCKED",
            "refresh_attempted_at": attempted_at,
            "last_success_at": None,
            "data_as_of": None,
            "refresh_error_class": exc.__class__.__name__,
            "refresh_error": str(exc),
            "data": {
                "source_id": futoi_source.SOURCE_ID,
                "instrument_id": instrument_id,
                "candidate_status": "UNAVAILABLE",
                "governance": governance,
                "factual_authority": False,
                "consumer_factual_use_allowed": False,
                "directional_authority": False,
                "action_authority": False,
                "standalone_buy_sell_authority": False,
                "stage5_full_mode_ready": False,
                "stage5_pointer_promotion_performed": False,
                "missing_or_blocked_must_not_be_interpreted_as_neutral": True,
            },
        }

    candidate_view = {
        "source_id": candidate["source_id"],
        "instrument_id": candidate["instrument_id"],
        "candidate_status": "PASS",
        "quality_status": candidate["quality_status"],
        "acceptance_status": candidate["acceptance_status"],
        "freshness": candidate["freshness"],
        "factual": candidate["factual"],
        "provenance": candidate["provenance"],
        "governance": governance,
        "factual_authority": bool(allowed),
        "consumer_factual_use_allowed": bool(allowed),
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_required": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
        "missing_or_blocked_must_not_be_interpreted_as_neutral": True,
    }
    return {
        "status": "READY" if allowed else "GOVERNED_BLOCKED",
        "refresh_attempted_at": attempted_at,
        "last_success_at": candidate.get("last_success_at"),
        "data_as_of": candidate.get("data_as_of"),
        "refresh_error_class": None,
        "refresh_error": None,
        "data": candidate_view,
    }


def _recompute_readiness(snapshot: dict[str, object]) -> None:
    components = snapshot["components"]
    statuses = {name: str(value["status"]) for name, value in components.items()}
    unavailable = sorted(name for name, status in statuses.items() if status == "UNAVAILABLE")
    retained = sorted(name for name, status in statuses.items() if status == "RETAINED_PREVIOUS")
    snapshot["readiness"] = {
        "status": "READY" if not unavailable and not retained else "PARTIAL",
        "component_statuses": statuses,
        "unavailable_components": unavailable,
        "retained_previous_components": retained,
    }


def _workflow_entry(instrument_id: str, component_name: str) -> dict[str, object]:
    return {
        "consumer": "SEPARATE_ANALYSIS_CHAT",
        "instrument_id": instrument_id,
        "component_ref": component_name,
        "factual_use_requires": (
            "components." + component_name + ".status=READY and data.factual_authority=true"
        ),
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
    }


def build_snapshot(
    *,
    now: datetime,
    previous: Mapping[str, object] | None = None,
    producers: Mapping[str, base.ComponentProducer] | None = None,
    data_root: Path | None = None,
) -> dict[str, object]:
    now_utc = base._aware(now, "now")
    result = base.build_snapshot(now=now_utc, previous=previous, producers=producers)
    root = data_root if data_root is not None else base._data_root()
    governance_values = _load_governance()
    for instrument_id, component_name in FUTOI_COMPONENT_BY_INSTRUMENT.items():
        result["components"][component_name] = _futoi_component(
            root=root,
            now=now_utc,
            previous=previous,
            governance_values=governance_values,
            instrument_id=instrument_id,
            component_name=component_name,
        )

    # Backward-compatible Si alias remains unchanged; explicit map adds CR without ambiguity.
    result["analysis_views"]["futoi_component_ref"] = FUTOI_COMPONENT
    result["analysis_views"]["futoi_component_refs"] = dict(FUTOI_COMPONENT_BY_INSTRUMENT)
    result["analysis_workflow"]["futoi_positioning"] = _workflow_entry(
        futoi_source.SI_INSTRUMENT_ID,
        FUTOI_COMPONENT,
    )
    result["analysis_workflow"]["futoi_positioning_by_instrument"] = {
        instrument_id: _workflow_entry(instrument_id, component_name)
        for instrument_id, component_name in FUTOI_COMPONENT_BY_INSTRUMENT.items()
    }

    authority_by_instrument: dict[str, object] = {}
    for instrument_id, component_name in FUTOI_COMPONENT_BY_INSTRUMENT.items():
        futoi_data = result["components"][component_name].get("data")
        factual_authority = bool(
            isinstance(futoi_data, Mapping) and futoi_data.get("factual_authority") is True
        )
        authority_by_instrument[instrument_id] = {
            "component_ref": component_name,
            "factual_authority": factual_authority,
            "directional_authority": False,
            "action_authority": False,
            "standalone_buy_sell_authority": False,
        }

    # Preserve legacy futoi_* authority semantics as the Si component only.
    si_authority = authority_by_instrument[futoi_source.SI_INSTRUMENT_ID]
    result["authority"]["futoi_factual_authority"] = si_authority["factual_authority"]
    result["authority"]["futoi_directional_authority"] = False
    result["authority"]["futoi_action_authority"] = False
    result["authority"]["futoi_by_instrument"] = authority_by_instrument
    _recompute_readiness(result)
    return result


def refresh_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    producers: Mapping[str, base.ComponentProducer] | None = None,
) -> tuple[dict[str, object], Path]:
    base.load_dotenv(base.PROJECT_ENV_PATH, override=False)
    base.install_timestamp_policy()
    root = base._data_root()
    state_dir = base.snapshot_state_dir(root)
    path = state_dir / base.CURRENT_FILENAME
    with base._single_refresh_lock(state_dir):
        previous = base._load_previous(path)
        now = base._aware(now_fn(), "clock")
        snapshot = build_snapshot(
            now=now,
            previous=previous,
            producers=producers,
            data_root=root,
        )
        base._atomic_write(path, snapshot)
    return snapshot, path


def read_current_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> tuple[dict[str, object], Path]:
    return base.read_current_snapshot(now_fn=now_fn)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish/read S7.3 chat snapshot with governed factual-only Si/CR FUTOI components"
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
        snapshot, _ = read_current_snapshot()
        print(json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
        return 0
    except Exception as exc:
        print(f"PROJECT={PROJECT}")
        print(f"MODE={MODE}")
        print("STATUS=FAILED")
        print(f"ERROR={exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
