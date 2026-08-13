from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Callable, Mapping

from dotenv import load_dotenv
import requests

from .usdrubf_shadow_alert_delivery import (
    PROJECT,
    AlertCandidate,
    AlertTransportError,
    ShadowAlertError,
    _candidate_from_change,
    _delivery_key,
    _load_persisted_change,
    _mapping,
    _prepare_state_root,
    _read_regular_json,
    _sequence,
    _single_instance_lock,
    _text,
    _validate_receipt,
    _write_atomic_json,
)


MODE = "shadow_alert_telegram_delivery"
STATE_FILENAME = "shadow_alert_telegram_delivery_state.json"
TRANSPORT_ID = "telegram"
_STATE_VERSION = 1
_MAX_RECORDS = 256
_ALLOWED_RECORD_STATES = {"PENDING", "DELIVERED", "FAILED_UNCERTAIN"}


class TelegramAlertError(ShadowAlertError):
    """Raised when the bounded Telegram delivery contract is invalid."""


class TelegramDeliveryUncertain(AlertTransportError):
    """Raised after a reserved Telegram attempt has an uncertain outcome."""


def _aware_now(now_fn: Callable[[], datetime]) -> datetime:
    value = now_fn()
    if value.tzinfo is None or value.utcoffset() is None:
        raise TelegramAlertError("telegram alert clock must be timezone-aware")
    return value


def _empty_state(now: datetime) -> dict[str, object]:
    return {
        "version": _STATE_VERSION,
        "project": PROJECT,
        "mode": MODE,
        "records": [],
        "last_alert_id": None,
        "last_change_as_of_timestamp": None,
        "last_delivery_status": None,
        "last_external_delivery": None,
        "last_transport_reference": None,
        "last_error_class": None,
        "last_error": None,
        "updated_at": now.isoformat(),
    }


def _load_state(root: Path, now: datetime) -> dict[str, object]:
    path = root / STATE_FILENAME
    if not path.exists():
        return _empty_state(now)
    raw = _mapping(_read_regular_json(path, "telegram alert state"), "telegram alert state")
    if raw.get("version") != _STATE_VERSION:
        raise TelegramAlertError("telegram alert state version mismatch")
    if raw.get("project") != PROJECT or raw.get("mode") != MODE:
        raise TelegramAlertError("telegram alert state identity mismatch")
    records = _sequence(raw.get("records"), "telegram alert state.records")
    if len(records) > _MAX_RECORDS:
        raise TelegramAlertError("telegram alert state record bound exceeded")
    seen_keys: set[str] = set()
    for index, record_raw in enumerate(records):
        record = _mapping(record_raw, f"records[{index}]")
        required = {
            "delivery_key",
            "alert_id",
            "transport_id",
            "state",
            "current_as_of_timestamp",
            "reserved_at",
            "updated_at",
            "external_delivery",
            "reference",
            "error_class",
            "error",
        }
        if set(record) != required:
            raise TelegramAlertError(f"records[{index}] field set mismatch")
        key = _text(record.get("delivery_key"), f"records[{index}].delivery_key")
        if key in seen_keys:
            raise TelegramAlertError("telegram delivery keys must be unique")
        seen_keys.add(key)
        _text(record.get("alert_id"), f"records[{index}].alert_id")
        if record.get("transport_id") != TRANSPORT_ID:
            raise TelegramAlertError("telegram record transport identity mismatch")
        if record.get("state") not in _ALLOWED_RECORD_STATES:
            raise TelegramAlertError("telegram record state is invalid")
        _text(record.get("current_as_of_timestamp"), f"records[{index}].current_as_of_timestamp")
        _text(record.get("reserved_at"), f"records[{index}].reserved_at")
        _text(record.get("updated_at"), f"records[{index}].updated_at")
        external = record.get("external_delivery")
        if external not in {None, True}:
            raise TelegramAlertError("telegram record external_delivery must be null or true")
        reference = record.get("reference")
        if reference is not None:
            _text(reference, f"records[{index}].reference")
        error_class = record.get("error_class")
        error = record.get("error")
        if error_class is not None:
            _text(error_class, f"records[{index}].error_class")
        if error is not None:
            _text(error, f"records[{index}].error")
    return dict(raw)


def _credentials(env: Mapping[str, str] | None = None) -> tuple[str, str]:
    if env is None:
        load_dotenv()
        source: Mapping[str, str] = os.environ
    else:
        source = env
    token = (source.get("TELEGRAM_BOT_TOKEN") or source.get("BOT_TOKEN") or "").strip()
    chat_id = (source.get("TELEGRAM_CHAT_ID") or source.get("CHAT_ID") or "").strip()
    if not token:
        raise TelegramAlertError("telegram bot token is not configured")
    if not chat_id:
        raise TelegramAlertError("telegram chat id is not configured")
    return token, chat_id


def telegram_transport(
    candidate: AlertCandidate,
    *,
    env: Mapping[str, str] | None = None,
    post_fn: Callable[..., object] = requests.post,
    timeout_seconds: float = 10.0,
) -> Mapping[str, object]:
    """Send one bounded alert to Telegram without exposing credentials in outputs."""

    if not 1.0 <= float(timeout_seconds) <= 30.0:
        raise TelegramAlertError("telegram timeout_seconds must be within 1..30")
    token, chat_id = _credentials(env)
    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        response = post_fn(
            endpoint,
            json={
                "chat_id": chat_id,
                "text": candidate.message,
                "disable_web_page_preview": True,
            },
            timeout=float(timeout_seconds),
        )
        raise_for_status = getattr(response, "raise_for_status", None)
        if not callable(raise_for_status):
            raise TelegramAlertError("telegram response object is invalid")
        raise_for_status()
        json_fn = getattr(response, "json", None)
        if not callable(json_fn):
            raise TelegramAlertError("telegram response JSON reader is unavailable")
        payload = _mapping(json_fn(), "telegram response")
    except TelegramAlertError:
        raise
    except Exception as exc:
        # Never persist or re-emit request exception details because they may contain the bot token URL.
        raise TelegramDeliveryUncertain(
            "telegram transport request failed; delivery outcome is uncertain"
        ) from None

    if payload.get("ok") is not True:
        raise TelegramDeliveryUncertain(
            "telegram transport did not confirm delivery; delivery outcome is uncertain"
        )
    result = _mapping(payload.get("result"), "telegram response.result")
    message_id = result.get("message_id")
    if isinstance(message_id, bool) or not isinstance(message_id, int) or message_id <= 0:
        raise TelegramDeliveryUncertain(
            "telegram transport returned an invalid message id; delivery outcome is uncertain"
        )
    return {
        "transport_id": TRANSPORT_ID,
        "status": "DELIVERED",
        "external_delivery": True,
        "reference": f"telegram:message_id:{message_id}",
    }


def _record_index(records: list[object], delivery_key: str) -> int | None:
    for index, raw in enumerate(records):
        record = _mapping(raw, f"records[{index}]")
        if record.get("delivery_key") == delivery_key:
            return index
    return None


def process_persisted_telegram_alert(
    state_root: Path | str,
    *,
    transport: Callable[[AlertCandidate], Mapping[str, object]],
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> Mapping[str, object]:
    """Process one persisted Change Detector alert with fail-closed at-most-once semantics.

    A delivery key is durably reserved before calling the external transport. Any crash or
    transport error after reservation prevents automatic resend of the same alert until an
    explicit recovery procedure is introduced and invoked separately.
    """

    root = Path(state_root).expanduser()
    _prepare_state_root(root)
    with _single_instance_lock(root):
        now = _aware_now(now_fn)
        state = _load_state(root, now)
        candidate = _candidate_from_change(_load_persisted_change(root))
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
        key = _delivery_key(candidate.alert_id, TRANSPORT_ID)
        records = list(_sequence(state.get("records"), "telegram alert state.records"))
        existing_index = _record_index(records, key)
        if existing_index is not None:
            existing = _mapping(records[existing_index], "existing telegram delivery record")
            if existing.get("state") == "DELIVERED":
                status = "DUPLICATE_SUPPRESSED"
            else:
                status = "DELIVERY_BLOCKED_UNCERTAIN"
            state["last_delivery_status"] = status
            state["last_external_delivery"] = False
            state["last_transport_reference"] = existing.get("reference")
            if status == "DELIVERY_BLOCKED_UNCERTAIN":
                state["last_error_class"] = "ExplicitRecoveryRequired"
                state["last_error"] = (
                    "telegram delivery key was already reserved without confirmed delivery; "
                    "automatic resend is disabled"
                )
            _write_atomic_json(root / STATE_FILENAME, state)
            return dict(state)

        reserved_at = _aware_now(now_fn)
        record: dict[str, object] = {
            "delivery_key": key,
            "alert_id": candidate.alert_id,
            "transport_id": TRANSPORT_ID,
            "state": "PENDING",
            "current_as_of_timestamp": candidate.current_as_of_timestamp,
            "reserved_at": reserved_at.isoformat(),
            "updated_at": reserved_at.isoformat(),
            "external_delivery": None,
            "reference": None,
            "error_class": None,
            "error": None,
        }
        records.append(record)
        state["records"] = records[-_MAX_RECORDS:]
        state["last_delivery_status"] = "PENDING"
        state["last_external_delivery"] = False
        state["last_transport_reference"] = None
        state["updated_at"] = reserved_at.isoformat()
        _write_atomic_json(root / STATE_FILENAME, state)

        try:
            receipt = _validate_receipt(
                transport(candidate),
                expected_transport_id=TRANSPORT_ID,
            )
        except Exception:
            failed_at = _aware_now(now_fn)
            current_records = list(_sequence(state.get("records"), "telegram alert state.records"))
            current_index = _record_index(current_records, key)
            if current_index is None:
                raise TelegramAlertError("reserved telegram delivery record disappeared")
            failed = dict(_mapping(current_records[current_index], "reserved telegram record"))
            failed["state"] = "FAILED_UNCERTAIN"
            failed["updated_at"] = failed_at.isoformat()
            failed["error_class"] = "TelegramDeliveryUncertain"
            failed["error"] = "delivery outcome uncertain; explicit recovery required"
            current_records[current_index] = failed
            state["records"] = current_records
            state["last_delivery_status"] = "DELIVERY_BLOCKED_UNCERTAIN"
            state["last_external_delivery"] = False
            state["last_transport_reference"] = None
            state["last_error_class"] = "TelegramDeliveryUncertain"
            state["last_error"] = "delivery outcome uncertain; explicit recovery required"
            state["updated_at"] = failed_at.isoformat()
            _write_atomic_json(root / STATE_FILENAME, state)
            raise TelegramDeliveryUncertain(
                "telegram delivery outcome is uncertain; automatic resend is disabled"
            ) from None

        delivered_at = _aware_now(now_fn)
        current_records = list(_sequence(state.get("records"), "telegram alert state.records"))
        current_index = _record_index(current_records, key)
        if current_index is None:
            raise TelegramAlertError("reserved telegram delivery record disappeared")
        delivered = dict(_mapping(current_records[current_index], "reserved telegram record"))
        delivered["state"] = "DELIVERED"
        delivered["updated_at"] = delivered_at.isoformat()
        delivered["external_delivery"] = True
        delivered["reference"] = receipt["reference"]
        delivered["error_class"] = None
        delivered["error"] = None
        current_records[current_index] = delivered
        state["records"] = current_records
        state["last_delivery_status"] = "DELIVERED"
        state["last_external_delivery"] = True
        state["last_transport_reference"] = receipt["reference"]
        state["last_error_class"] = None
        state["last_error"] = None
        state["updated_at"] = delivered_at.isoformat()
        _write_atomic_json(root / STATE_FILENAME, state)
        return dict(state)
