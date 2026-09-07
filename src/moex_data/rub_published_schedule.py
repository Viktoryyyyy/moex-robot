"""Bounded published plans, never evidence of actual session state or completion."""
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from zoneinfo import ZoneInfo

ARTIFACT = Path(__file__).resolve().parents[2] / "contracts/intelligence/rub_published_schedule_2026-09-08.json"
ARTIFACT_SHA256 = "013891a6ff913dd878d94d8d95ba58a7ba43c63ee9e31838057dd06df3d002bd"


def describe(*, now: datetime, artifact_path: Path = ARTIFACT) -> dict:
    """Describe only explicitly reviewed civil dates; no weekday extrapolation."""
    result = {
        "kind": "PUBLISHED_SCHEDULE_PLAN", "planned_phase": "UNKNOWN",
        "published_plan_covered": False, "actual_session_state": "UNKNOWN",
        "session_completion_proven": False, "authority_granted": False,
        "historical_pit_acceptance": False, "exception_updates_verified": False,
        "reason": "schedule_evidence_unavailable",
    }
    try:
        raw = artifact_path.read_bytes()
        if sha256(raw).hexdigest() != ARTIFACT_SHA256:
            return result
        evidence = json.loads(raw)
        if now.utcoffset() is None:
            return result
        reviewed = datetime.fromisoformat(evidence["reviewed_at_utc"])
        result.update(reviewed_at_utc=reviewed.isoformat(), artifact_sha256=ARTIFACT_SHA256)
        if now < reviewed:
            result["reason"] = "before_evidence_review"
            return result
        local = now.astimezone(ZoneInfo(evidence["timezone"]))
        civil_date = local.date().isoformat()
        result.update(civil_date=civil_date, timezone=evidence["timezone"])
        rule = evidence["civil_dates"].get(civil_date)
        if rule is None:
            result["reason"] = "civil_date_outside_reviewed_plan"
            return result
        intervals = evidence["rules"][rule]
        phase = "OUTSIDE_PUBLISHED_INTERVALS" if intervals else "PUBLISHED_NO_SESSION"
        clock = local.strftime("%H:%M:%S")
        for start, end, label in intervals:
            if start + ":00" <= clock < end + ":00":
                phase = label
                break
        result.update(published_plan_covered=True, planned_phase=phase,
            instruments=evidence["instruments"], source_url=evidence["sources"][rule],
            intervals_local=[{"start": start, "end_exclusive": end, "phase": label}
                             for start, end, label in intervals],
            reason="published_plan_only_actual_state_unverified")
    except (OSError, ValueError, TypeError, KeyError):
        return result
    return result
