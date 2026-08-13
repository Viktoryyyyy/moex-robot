from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
from hashlib import sha256
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable, Iterator, Mapping, Sequence, TextIO


PROJECT = "MOEX_Bot"
MODE = "shadow_alert_delivery"
STATE_FILENAME = "shadow_alert_delivery_state.json"
STATUS_FILENAME = "shadow_scheduler_status.json"
POINTER_FILENAME = "current_cycle.json"
LOCK_FILENAME = ".shadow_alert_delivery.lock"
_STATE_VERSION = 1
_MAX_DELIVERY_HISTORY = 256
_MAX_RENDERED_EVENTS = 5
_MAX_MESSAGE_CHARS = 3500
_SEVERITY_RANK = {"INFO": 1, "IMPORTANT": 2, "ACTION": 3}
_DELIVERABLE_SEVERITIES = {"IMPORTANT", "ACTION"}
_SUCCESS_STATUSES = {"DRY_RUN_RECORDED", "DELIVERED"}


class ShadowAlertError(ValueError):
    """Raised when persisted alert inputs violate the bounded delivery contract."""


class AlertAlreadyRunning(ShadowAlertError):
    """Raised when another alert consumer owns the same state root."""


class AlertTransportError(RuntimeError):
    """Raised when an eligible alert cannot be handed to its configured transport."""


@dataclass(frozen=True)
class AlertCandidate:
    alert_id: str
    instrument: str
    current_as_of_timestamp: str
    highest_severity: str
    event_count: int
    message: str


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ShadowAlertError(f"{field} must be a mapping")
    return value


def _sequence(value: object, field: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ShadowAlertError(f"{field} must be a sequence")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ShadowAlertError(f"{field} must be non-empty")
    return value.strip()


def _aware(value: object, field: str) -> datetime:
    raw = _text(value, field)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ShadowAlertError(f"{field} must be ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowAlertError(f"{field} must be timezone-aware")
    return parsed


def _plain_json_basename(value: object, field: str) -> str:
    name = _text(value, field)
    path = Path(name)
    if path.name != name or name in {".", ".."} or path.suffix != ".json":
        raise ShadowAlertError(f"{field} must be a plain .json basename")
    return name


def _prepare_state_root(state_root: Path) -> None:
    if not state_root.is_absolute():
        raise ShadowAlertError("state_root must be an explicit absolute path")
    state_root.mkdir(parents=True, exist_ok=True)
    if state_root.is_symlink() or not state_root.is_dir():
        raise ShadowAlertError("state_root must be a regular directory, not a symlink")


def _read_regular_json(path: Path, field: str) -> object:
    if path.is_symlink() or not path.is_file():
        raise ShadowAlertError(f"{field} is not a regular non-symlink file")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ShadowAlertError(f"{field} is unreadable or invalid JSON") from exc


def _write_atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(
        dict(payload),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    )
    temp_path: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(serialized)
            handle.flush()
            temp_path = Path(handle.name)
        temp_path.replace(path)
    except Exception:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise


def _resolve_child_path(state_root: Path, value: object, field: str) -> Path:
    raw = _text(value, field)
    path = Path(raw)
    resolved = path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()
    if resolved.parent != state_root.resolve():
        raise ShadowAlertError(f"{field} escaped the explicit state_root")
    return resolved


@contextmanager
def _single_instance_lock(state_root: Path) -> Iterator[Path]:
    _prepare_state_root(state_root)
    lock_path = state_root / LOCK_FILENAME
    handle: TextIO = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AlertAlreadyRunning(
                f"another alert consumer already holds {lock_path}"
            ) from exc
        yield lock_path
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _validate_event(raw: object, index: int) -> Mapping[str, object]:
    field = f"change_detection.events[{index}]"
    item = _mapping(raw, field)
    required = {
        "event_type",
        "severity",
        "code",
        "reason",
        "previous_value",
        "current_value",
        "level_id",
        "evidence_refs",
    }
    if set(item) != required:
        raise ShadowAlertError(f"{field} field set mismatch")
    severity = _text(item.get("severity"), f"{field}.severity")
    if severity not in _SEVERITY_RANK:
        raise ShadowAlertError(f"invalid {field}.severity")
    _text(item.get("event_type"), f"{field}.event_type")
    _text(item.get("code"), f"{field}.code")
    _text(item.get("reason"), f"{field}.reason")
    level_id = item.get("level_id")
    if level_id is not None:
        _text(level_id, f"{field}.level_id")
    refs = tuple(
        _text(value, f"{field}.evidence_refs")
        for value in _sequence(item.get("evidence_refs"), f"{field}.evidence_refs")
    )
    if len(refs) != len(set(refs)):
        raise ShadowAlertError(f"{field}.evidence_refs must be unique")
    return item


def _validate_change_detection(raw: object) -> Mapping[str, object] | None:
    if raw is None:
        return None
    item = _mapping(raw, "change_detection")
    required = {
        "instrument",
        "previous_as_of_timestamp",
        "current_as_of_timestamp",
        "events",
        "highest_severity",
        "significant_change",
        "action_alert",
    }
    if set(item) != required:
        raise ShadowAlertError("change_detection field set mismatch")
    _text(item.get("instrument"), "change_detection.instrument")
    previous = _aware(
        item.get("previous_as_of_timestamp"),
        "change_detection.previous_as_of_timestamp",
    )
    current = _aware(
        item.get("current_as_of_timestamp"),
        "change_detection.current_as_of_timestamp",
    )
    if current <= previous:
        raise ShadowAlertError("change_detection current timestamp must advance")

    events = tuple(
        _validate_event(value, index)
        for index, value in enumerate(
            _sequence(item.get("events"), "change_detection.events")
        )
    )
    expected_highest = (
        None
        if not events
        else max(
            (_text(event.get("severity"), "event.severity") for event in events),
            key=_SEVERITY_RANK.__getitem__,
        )
    )
    highest = item.get("highest_severity")
    if highest is not None:
        highest = _text(highest, "change_detection.highest_severity")
        if highest not in _SEVERITY_RANK:
            raise ShadowAlertError("invalid change_detection.highest_severity")
    if highest != expected_highest:
        raise ShadowAlertError("change_detection highest_severity is inconsistent")

    significant = item.get("significant_change")
    action = item.get("action_alert")
    if not isinstance(significant, bool) or not isinstance(action, bool):
        raise ShadowAlertError("change_detection flags must be boolean")
    expected_significant = any(
        _text(event.get("severity"), "event.severity") in _DELIVERABLE_SEVERITIES
        for event in events
    )
    expected_action = any(
        _text(event.get("severity"), "event.severity") == "ACTION"
        for event in events
    )
    if significant != expected_significant or action != expected_action:
        raise ShadowAlertError("change_detection flags are inconsistent with events")
    return item


def _load_persisted_change(state_root: Path) -> Mapping[str, object] | None:
    _prepare_state_root(state_root)
    scheduler = _mapping(
        _read_regular_json(state_root / STATUS_FILENAME, "scheduler status"),
        "scheduler status",
    )
    if scheduler.get("project") != PROJECT or scheduler.get("mode") != "controlled_shadow_scheduler":
        raise ShadowAlertError("scheduler status identity mismatch")
    if scheduler.get("scheduler_status") not in {"RUNNING", "COMPLETED"}:
        raise ShadowAlertError("scheduler status is not consumable")
    if scheduler.get("last_cycle_status") != "COMPLETED":
        raise ShadowAlertError("scheduler last cycle is not completed")

    pointer = _mapping(
        _read_regular_json(state_root / POINTER_FILENAME, "current cycle pointer"),
        "current cycle pointer",
    )
    required_pointer = {
        "version",
        "current_as_of_timestamp",
        "market_state_file",
        "change_detection_file",
    }
    if set(pointer) != required_pointer or pointer.get("version") != 1:
        raise ShadowAlertError("current cycle pointer field set or version mismatch")
    pointer_as_of = _aware(
        pointer.get("current_as_of_timestamp"),
        "current cycle pointer timestamp",
    ).isoformat()
    _plain_json_basename(pointer.get("market_state_file"), "current cycle market_state_file")
    change_name = _plain_json_basename(
        pointer.get("change_detection_file"),
        "current cycle change_detection_file",
    )

    scheduler_as_of = _aware(
        scheduler.get("last_cycle_as_of_timestamp"),
        "scheduler last_cycle_as_of_timestamp",
    ).isoformat()
    if scheduler_as_of != pointer_as_of:
        raise ShadowAlertError("scheduler status and current cycle pointer are not aligned")
    scheduler_change_path = _resolve_child_path(
        state_root,
        scheduler.get("last_change_detection_path"),
        "scheduler last_change_detection_path",
    )
    if scheduler_change_path.name != change_name:
        raise ShadowAlertError("scheduler change path does not match current cycle pointer")

    change = _validate_change_detection(
        _read_regular_json(state_root / change_name, "change detection generation")
    )
    scheduler_significant = scheduler.get("last_significant_change")
    scheduler_action = scheduler.get("last_action_candidate")
    if not isinstance(scheduler_significant, bool) or not isinstance(scheduler_action, bool):
        raise ShadowAlertError("scheduler change flags must be boolean")
    if change is None:
        if scheduler_significant or scheduler_action:
            raise ShadowAlertError("scheduler flags conflict with empty change detection")
        return None

    change_as_of = _aware(
        change.get("current_as_of_timestamp"),
        "change_detection.current_as_of_timestamp",
    ).isoformat()
    if change_as_of != pointer_as_of:
        raise ShadowAlertError("change detection timestamp does not match current cycle pointer")
    if scheduler_significant != change.get("significant_change"):
        raise ShadowAlertError("scheduler significant-change flag mismatch")
    if scheduler_action != change.get("action_alert"):
        raise ShadowAlertError("scheduler action-candidate flag mismatch")
    return change


def _canonical_hash(value: object) -> str:
    serialized = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return sha256(serialized.encode("utf-8")).hexdigest()


def _render_message(change: Mapping[str, object]) -> str:
    significant_events = [
        event
        for event in _sequence(change.get("events"), "change_detection.events")
        if _text(_mapping(event, "event").get("severity"), "event.severity")
        in _DELIVERABLE_SEVERITIES
    ]
    lines = [
        "MOEX_Bot RUB Intelligence alert",
        f"instrument={_text(change.get('instrument'), 'change_detection.instrument')}",
        f"severity={_text(change.get('highest_severity'), 'change_detection.highest_severity')}",
        f"as_of={_text(change.get('current_as_of_timestamp'), 'change_detection.current_as_of_timestamp')}",
        f"significant_events={len(significant_events)}",
    ]
    for raw in significant_events[:_MAX_RENDERED_EVENTS]:
        event = _mapping(raw, "event")
        severity = _text(event.get("severity"), "event.severity")
        code = _text(event.get("code"), "event.code")
        reason = _text(event.get("reason"), "event.reason").replace("\n", " ")
        if len(reason) > 240:
            reason = reason[:237] + "..."
        lines.append(f"- [{severity}] {code}: {reason}")
    if len(significant_events) > _MAX_RENDERED_EVENTS:
        lines.append(f"- additional_significant_events={len(significant_events) - _MAX_RENDERED_EVENTS}")
    message = "\n".join(lines)
    if len(message) > _MAX_MESSAGE_CHARS:
        raise ShadowAlertError("rendered alert exceeds bounded message size")
    return message


def _candidate_from_change(change: Mapping[str, object] | None) -> AlertCandidate | None:
    if change is None or change.get("significant_change") is not True:
        return None
    highest = _text(change.get("highest_severity"), "change_detection.highest_severity")
    if highest not in _DELIVERABLE_SEVERITIES:
        raise ShadowAlertError("significant change did not pass deterministic severity gate")
    events = _sequence(change.get("events"), "change_detection.events")
    alert_id = _canonical_hash(change)
    return AlertCandidate(
        alert_id=alert_id,
        instrument=_text(change.get("instrument"), "change_detection.instrument"),
        current_as_of_timestamp=_text(
            change.get("current_as_of_timestamp"),
            "change_detection.current_as_of_timestamp",
        ),
        highest_severity=highest,
        event_count=len(events),
        message=_render_message(change),
    )


def _empty_state(now: datetime) -> dict[str, object]:
    return {
        "version": _STATE_VERSION,
        "project": PROJECT,
        "mode": MODE,
        "delivered": [],
        "last_transport_id": None,
        "last_alert_id": None,
        "last_change_as_of_timestamp": None,
        "last_delivery_status": None,
        "last_external_delivery": None,
        "last_transport_reference": None,
        "last_error_class": None,
        "last_error": None,
        "updated_at": now.isoformat(),
    }


def _load_delivery_state(state_root: Path, now: datetime) -> dict[str, object]:
    path = state_root / STATE_FILENAME
    if not path.exists():
        return _empty_state(now)
    raw = _mapping(_read_regular_json(path, "alert delivery state"), "alert delivery state")
    if raw.get("version") != _STATE_VERSION or raw.get("project") != PROJECT or raw.get("mode") != MODE:
        raise ShadowAlertError("alert delivery state identity/version mismatch")
    delivered = _sequence(raw.get("delivered"), "alert delivery state.delivered")
    if len(delivered) > _MAX_DELIVERY_HISTORY:
        raise ShadowAlertError("alert delivery history exceeded bound")
    for index, record_raw in enumerate(delivered):
        record = _mapping(record_raw, f"delivered[{index}]")
        required = {
            "delivery_key",
            "alert_id",
            "transport_id",
            "status",
            "external_delivery",
            "reference",
            "delivered_at",
        }
        if set(record) != required:
            raise ShadowAlertError(f"delivered[{index}] field set mismatch")
        _text(record.get("delivery_key"), f"delivered[{index}].delivery_key")
        _text(record.get("alert_id"), f"delivered[{index}].alert_id")
        _text(record.get("transport_id"), f"delivered[{index}].transport_id")
        if record.get("status") not in _SUCCESS_STATUSES:
            raise ShadowAlertError(f"delivered[{index}] has invalid status")
        if not isinstance(record.get("external_delivery"), bool):
            raise ShadowAlertError(f"delivered[{index}].external_delivery must be boolean")
        reference = record.get("reference")
        if reference is not None:
            _text(reference, f"delivered[{index}].reference")
        _aware(record.get("delivered_at"), f"delivered[{index}].delivered_at")
    return dict(raw)


def _delivery_key(alert_id: str, transport_id: str) -> str:
    return sha256(f"{transport_id}\0{alert_id}".encode("utf-8")).hexdigest()


def dry_run_transport(candidate: AlertCandidate) -> Mapping[str, object]:
    """Non-delivering fixture transport used for S6.2 boundary proof."""

    return {
        "transport_id": "dry-run",
        "status": "DRY_RUN_RECORDED",
        "external_delivery": False,
        "reference": f"dry-run:{candidate.alert_id[:16]}",
    }


def _validate_receipt(receipt_raw: object, *, expected_transport_id: str) -> Mapping[str, object]:
    receipt = _mapping(receipt_raw, "transport receipt")
    required = {"transport_id", "status", "external_delivery", "reference"}
    if set(receipt) != required:
        raise AlertTransportError("transport receipt field set mismatch")
    transport_id = _text(receipt.get("transport_id"), "transport receipt.transport_id")
    if transport_id != expected_transport_id:
        raise AlertTransportError("transport receipt identity mismatch")
    status = _text(receipt.get("status"), "transport receipt.status")
    if status not in _SUCCESS_STATUSES:
        raise AlertTransportError("transport did not return a successful delivery status")
    external = receipt.get("external_delivery")
    if not isinstance(external, bool):
        raise AlertTransportError("transport receipt external_delivery must be boolean")
    if status == "DRY_RUN_RECORDED" and external:
        raise AlertTransportError("dry-run transport cannot claim external delivery")
    if status == "DELIVERED" and not external:
        raise AlertTransportError("live delivery receipt must confirm external delivery")
    reference = receipt.get("reference")
    if reference is not None:
        _text(reference, "transport receipt.reference")
    return receipt


def process_persisted_alert(
    state_root: Path | str,
    *,
    transport_id: str,
    transport: Callable[[AlertCandidate], Mapping[str, object]],
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> Mapping[str, object]:
    root = Path(state_root).expanduser()
    transport_name = _text(transport_id, "transport_id")
    _prepare_state_root(root)
    with _single_instance_lock(root):
        now = now_fn()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ShadowAlertError("alert clock must be timezone-aware")
        state = _load_delivery_state(root, now)
        candidate = _candidate_from_change(_load_persisted_change(root))
        state["last_transport_id"] = transport_name
        state["last_error_class"] = None
        state["last_error"] = None
        state["updated_at"] = now.isoformat()

        if candidate is None:
            state["last_alert_id"] = None
            state["last_change_as_of_timestamp"] = None
            state["last_delivery_status"] = "NO_ALERT"
            state["last_external_delivery"] = False
            state["last_transport_reference"] = None
            _write_atomic_json(root / STATE_FILENAME, state)
            return dict(state)

        state["last_alert_id"] = candidate.alert_id
        state["last_change_as_of_timestamp"] = candidate.current_as_of_timestamp
        key = _delivery_key(candidate.alert_id, transport_name)
        delivered_records = list(_sequence(state.get("delivered"), "alert delivery state.delivered"))
        delivered_keys = {
            _text(_mapping(record, "delivered record").get("delivery_key"), "delivery_key")
            for record in delivered_records
        }
        if key in delivered_keys:
            state["last_delivery_status"] = "DUPLICATE_SUPPRESSED"
            state["last_external_delivery"] = False
            state["last_transport_reference"] = None
            _write_atomic_json(root / STATE_FILENAME, state)
            return dict(state)

        try:
            receipt = _validate_receipt(
                transport(candidate),
                expected_transport_id=transport_name,
            )
        except Exception as exc:
            failed_at = now_fn()
            if failed_at.tzinfo is None or failed_at.utcoffset() is None:
                raise ShadowAlertError("alert clock must be timezone-aware") from exc
            state["last_delivery_status"] = "TRANSPORT_FAILED"
            state["last_external_delivery"] = False
            state["last_transport_reference"] = None
            state["last_error_class"] = exc.__class__.__name__
            state["last_error"] = str(exc)
            state["updated_at"] = failed_at.isoformat()
            _write_atomic_json(root / STATE_FILENAME, state)
            raise AlertTransportError(str(exc)) from exc

        delivered_at = now_fn()
        if delivered_at.tzinfo is None or delivered_at.utcoffset() is None:
            raise ShadowAlertError("alert clock must be timezone-aware")
        delivered_records.append(
            {
                "delivery_key": key,
                "alert_id": candidate.alert_id,
                "transport_id": transport_name,
                "status": receipt["status"],
                "external_delivery": receipt["external_delivery"],
                "reference": receipt["reference"],
                "delivered_at": delivered_at.isoformat(),
            }
        )
        state["delivered"] = delivered_records[-_MAX_DELIVERY_HISTORY:]
        state["last_delivery_status"] = receipt["status"]
        state["last_external_delivery"] = receipt["external_delivery"]
        state["last_transport_reference"] = receipt["reference"]
        state["updated_at"] = delivered_at.isoformat()
        _write_atomic_json(root / STATE_FILENAME, state)
        return dict(state)
