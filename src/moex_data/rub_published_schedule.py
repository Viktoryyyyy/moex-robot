"""Bounded published plans, never evidence of actual session state or completion."""
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from zoneinfo import ZoneInfo

ARTIFACT = Path(__file__).resolve().parents[2] / "contracts/intelligence/rub_published_schedule_2026-09-08.json"
ARTIFACT_SHA256 = "013891a6ff913dd878d94d8d95ba58a7ba43c63ee9e31838057dd06df3d002bd"
UPDATED_ARTIFACT = ARTIFACT.with_name('rub_published_schedule_2026-09-08_v2.json')
UPDATED_SHA256 = 'ac8ce487d0170a898ea5370c4046aa304d55001c159f66f5ee38c0b67f0eb369'
UPDATED_REVIEWED_AT = datetime.fromisoformat('2026-09-08T08:24:24.787012+00:00')


def describe(*, now: datetime, artifact_path: Path | None = None) -> dict:
    """Describe only explicitly reviewed civil dates; no weekday extrapolation."""
    result = {
        "kind": "PUBLISHED_SCHEDULE_PLAN", "planned_phase": "UNKNOWN",
        "published_plan_covered": False, "actual_session_state": "UNKNOWN",
        "session_completion_proven": False, "authority_granted": False,
        "historical_pit_acceptance": False, "exception_updates_verified": False,
        "forecast_trading_targets_accepted": False, "source_archive_replayed": False,
        "reason": "schedule_evidence_unavailable",
    }
    try:
        if not isinstance(now, datetime) or now.utcoffset() is None:
            return result
        if artifact_path is None:
            # Keep prior reviewed knowledge for historical consumption; a broken
            # applicable update never falls back to an older apparently good plan.
            artifact_path = UPDATED_ARTIFACT if now >= UPDATED_REVIEWED_AT else ARTIFACT
        if artifact_path.is_symlink():
            return result
        raw = artifact_path.read_bytes()
        digest = sha256(raw).hexdigest()
        if artifact_path == UPDATED_ARTIFACT and digest != UPDATED_SHA256:
            return result
        if digest not in {ARTIFACT_SHA256, UPDATED_SHA256}:
            return result
        evidence = json.loads(raw)
        reviewed = datetime.fromisoformat(evidence["reviewed_at_utc"])
        result.update(reviewed_at_utc=reviewed.isoformat(), artifact_sha256=digest)
        if now < reviewed:
            result["reason"] = "before_evidence_review"
            return result
        if digest == UPDATED_SHA256:
            if evidence['schema'] != 'rub_published_schedule.v2' or reviewed != UPDATED_REVIEWED_AT:
                return result
            source_path = artifact_path.with_name(evidence['source_evidence_manifest'])
            if source_path.is_symlink():
                return result
            source_raw = source_path.read_bytes()
            source_digest = sha256(source_raw).hexdigest()
            if source_digest != evidence['source_evidence_manifest_sha256']:
                return result
            archive = json.loads(source_raw)
            if archive['schema'] != 'rub_schedule_source_archive.v1':
                return result
            sources = archive['sources']
            for source in sources:
                requested = datetime.fromisoformat(source['requested_at_utc'])
                received = datetime.fromisoformat(source['received_at_utc'])
                if (requested.utcoffset() is None or received.utcoffset() is None
                        or not requested <= received <= reviewed
                        or source['tls_verified'] is not True or source['http_status'] != 200):
                    return result
            result.update(source_evidence_manifest_sha256=source_digest,
                source_evidence=[{key: source[key] for key in ('source_url', 'raw_sha256',
                    'receipt_sha256', 'requested_at_utc', 'received_at_utc')} for source in sources],
                coverage_end_inclusive=evidence['coverage_end_inclusive'])
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
        if digest == UPDATED_SHA256:
            result['source_dependencies'] = evidence['source_dependencies'][rule]
    except (OSError, ValueError, TypeError, KeyError):
        return result
    return result
