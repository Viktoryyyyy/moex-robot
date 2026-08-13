from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from src.moex_research.intelligence import usdrubf_shadow_alert_delivery as alerts


def _event(severity: str = "IMPORTANT") -> dict[str, object]:
    return {
        "event_type": "MARKET_STRUCTURE_CHANGED",
        "severity": severity,
        "code": "MARKET_REGIME_CHANGED",
        "reason": "Market regime changed.",
        "previous_value": "RANGE",
        "current_value": "TREND",
        "level_id": None,
        "evidence_refs": [],
    }


def _change(*, severity: str = "IMPORTANT") -> dict[str, object]:
    event = _event(severity)
    return {
        "instrument": "USDRUBF",
        "previous_as_of_timestamp": "2026-08-13T10:00:00+00:00",
        "current_as_of_timestamp": "2026-08-13T10:05:00+00:00",
        "events": [event],
        "highest_severity": severity,
        "significant_change": severity == "IMPORTANT",
        "action_alert": False,
    }


def _write_fixture(
    root: Path,
    *,
    change: object,
    scheduler_as_of: str = "2026-08-13T10:05:00+00:00",
) -> None:
    change_name = "change_detection.fixture.json"
    market_name = "market_state.fixture.json"
    (root / change_name).write_text(json.dumps(change), encoding="utf-8")
    (root / market_name).write_text("{}", encoding="utf-8")
    (root / alerts.POINTER_FILENAME).write_text(
        json.dumps(
            {
                "version": 1,
                "current_as_of_timestamp": "2026-08-13T10:05:00+00:00",
                "market_state_file": market_name,
                "change_detection_file": change_name,
            }
        ),
        encoding="utf-8",
    )
    significant = False if change is None else bool(change["significant_change"])
    action = False if change is None else bool(change["action_alert"])
    (root / alerts.STATUS_FILENAME).write_text(
        json.dumps(
            {
                "project": "MOEX_Bot",
                "mode": "controlled_shadow_scheduler",
                "scheduler_status": "COMPLETED",
                "last_cycle_status": "COMPLETED",
                "last_cycle_as_of_timestamp": scheduler_as_of,
                "last_change_detection_path": str(root / change_name),
                "last_significant_change": significant,
                "last_action_candidate": action,
            }
        ),
        encoding="utf-8",
    )


def _now() -> datetime:
    return datetime(2026, 8, 13, 10, 6, tzinfo=timezone.utc)


def test_important_change_is_recorded_by_non_delivering_dry_transport(tmp_path: Path) -> None:
    _write_fixture(tmp_path, change=_change())
    seen: list[alerts.AlertCandidate] = []

    def transport(candidate: alerts.AlertCandidate):
        seen.append(candidate)
        return alerts.dry_run_transport(candidate)

    result = alerts.process_persisted_alert(
        tmp_path,
        transport_id="dry-run",
        transport=transport,
        now_fn=_now,
    )

    assert result["last_delivery_status"] == "DRY_RUN_RECORDED"
    assert result["last_external_delivery"] is False
    assert len(result["delivered"]) == 1
    assert len(seen) == 1
    assert seen[0].highest_severity == "IMPORTANT"
    assert "MARKET_REGIME_CHANGED" in seen[0].message
    assert seen[0].alert_id == result["last_alert_id"]


def test_restart_reuses_delivery_state_and_suppresses_duplicate(tmp_path: Path) -> None:
    _write_fixture(tmp_path, change=_change())
    calls = 0

    def transport(candidate: alerts.AlertCandidate):
        nonlocal calls
        calls += 1
        return alerts.dry_run_transport(candidate)

    first = alerts.process_persisted_alert(
        tmp_path,
        transport_id="dry-run",
        transport=transport,
        now_fn=_now,
    )
    second = alerts.process_persisted_alert(
        tmp_path,
        transport_id="dry-run",
        transport=transport,
        now_fn=_now,
    )

    assert first["last_delivery_status"] == "DRY_RUN_RECORDED"
    assert second["last_delivery_status"] == "DUPLICATE_SUPPRESSED"
    assert calls == 1
    assert len(second["delivered"]) == 1


def test_info_only_change_is_suppressed_without_transport_call(tmp_path: Path) -> None:
    _write_fixture(tmp_path, change=_change(severity="INFO"))
    calls = 0

    def transport(candidate: alerts.AlertCandidate):
        nonlocal calls
        calls += 1
        return alerts.dry_run_transport(candidate)

    result = alerts.process_persisted_alert(
        tmp_path,
        transport_id="dry-run",
        transport=transport,
        now_fn=_now,
    )

    assert result["last_delivery_status"] == "NO_ALERT"
    assert result["last_external_delivery"] is False
    assert result["delivered"] == []
    assert calls == 0


def test_transport_failure_is_fail_closed_and_does_not_consume_dedupe_key(tmp_path: Path) -> None:
    _write_fixture(tmp_path, change=_change())

    def failing_transport(candidate: alerts.AlertCandidate):
        raise RuntimeError("fixture transport unavailable")

    with pytest.raises(alerts.AlertTransportError, match="fixture transport unavailable"):
        alerts.process_persisted_alert(
            tmp_path,
            transport_id="dry-run",
            transport=failing_transport,
            now_fn=_now,
        )

    failed_state = json.loads((tmp_path / alerts.STATE_FILENAME).read_text(encoding="utf-8"))
    assert failed_state["last_delivery_status"] == "TRANSPORT_FAILED"
    assert failed_state["delivered"] == []

    retried = alerts.process_persisted_alert(
        tmp_path,
        transport_id="dry-run",
        transport=alerts.dry_run_transport,
        now_fn=_now,
    )
    assert retried["last_delivery_status"] == "DRY_RUN_RECORDED"
    assert len(retried["delivered"]) == 1


def test_scheduler_pointer_mismatch_is_blocked_before_transport(tmp_path: Path) -> None:
    _write_fixture(
        tmp_path,
        change=_change(),
        scheduler_as_of="2026-08-13T10:04:00+00:00",
    )

    with pytest.raises(alerts.ShadowAlertError, match="not aligned"):
        alerts.process_persisted_alert(
            tmp_path,
            transport_id="dry-run",
            transport=alerts.dry_run_transport,
            now_fn=_now,
        )

    assert not (tmp_path / alerts.STATE_FILENAME).exists()


def test_inconsistent_change_flags_are_blocked(tmp_path: Path) -> None:
    change = _change()
    change["significant_change"] = False
    _write_fixture(tmp_path, change=change)

    with pytest.raises(alerts.ShadowAlertError, match="flags are inconsistent"):
        alerts.process_persisted_alert(
            tmp_path,
            transport_id="dry-run",
            transport=alerts.dry_run_transport,
            now_fn=_now,
        )
