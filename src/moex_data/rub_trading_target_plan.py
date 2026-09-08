"""Plan candidates only: a published trading day is not a validated forecast target."""
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from moex_data import rub_futures_calendar as calendar
from moex_data import rub_published_schedule as schedule

MOSCOW = ZoneInfo('Europe/Moscow')
POLICY = 'next_not_started_published_trading_day_candidate.v1'


def describe(component, *, now):
    result = {'policy': POLICY, 'kind': 'PUBLISHED_TARGET_CANDIDATES_ONLY',
        'actual_session_state': 'UNKNOWN', 'session_completion_proven': False,
        'forecast_trading_targets_accepted': False, 'historical_pit_acceptance': False,
        'D1': {'status': 'UNAVAILABLE', 'candidate_trading_date': None,
            'first_published_start': None, 'horizon_definition_accepted': False,
            'reason': 'calendar_plan_unavailable'},
        'W1': {'status': 'UNAVAILABLE', 'candidate_trading_date': None,
            'horizon_definition_accepted': False, 'reason': 'weekly_horizon_definition_not_accepted',
            'unselected_definitions': ['calendar_week', 'five_forward_trading_days']}}
    try:
        if not isinstance(now, datetime) or now.utcoffset() is None or not isinstance(component, dict):
            return result
        view = calendar.reconcile(component, now=now)
        data = view.get('data')
        if (view.get('status') != 'READY' or not isinstance(data, dict)
                or data.get('calendar_plan_usable') is not True):
            return result
        known_plan = schedule.describe(now=now)
        pinned = known_plan.get('artifact_sha256')
        if not pinned or 'source_evidence_manifest_sha256' not in known_plan:
            result['D1']['reason'] = 'reviewed_schedule_not_available'
            return result
        result.update(calendar_manifest_sha256=data['manifest_sha256'],
            schedule_artifact_sha256=pinned, calendar_received_at=data['received_at'],
            candidate_scope='next_not_started_day_in_joint_published_coverage')
        today = now.astimezone(MOSCOW).date().isoformat()
        groups = defaultdict(list)
        for row in data['days']:
            if row['is_traded'] == 1 and row['trading_date'] >= today:
                groups[row['trading_date']].append(row['civil_date'])
            elif row['is_traded'] == 0 and row['civil_date'] >= today:
                plan = schedule.describe(now=datetime.fromisoformat(row['civil_date'] + 'T12:00:00').replace(tzinfo=MOSCOW))
                if (plan.get('artifact_sha256') != pinned or not plan['published_plan_covered']
                        or plan['intervals_local']):
                    # Validate a no-session date only when needed below; never skip
                    # over a contradiction to reach a later apparently valid target.
                    groups[row['civil_date']].append(None)
        for target in sorted(groups):
            civil_dates = groups[target]
            if None in civil_dates:
                result['D1']['reason'] = 'calendar_and_schedule_coverage_or_rules_disagree'
                return result
            starts = []
            for civil in sorted(civil_dates):
                if civil <= data['coverage_start']:
                    result['D1']['reason'] = 'first_session_may_precede_calendar_coverage'
                    return result
                plan = schedule.describe(now=datetime.fromisoformat(civil + 'T12:00:00').replace(tzinfo=MOSCOW))
                if (plan.get('artifact_sha256') != pinned or not plan['published_plan_covered']
                        or not plan['intervals_local']):
                    result['D1']['reason'] = 'calendar_and_schedule_coverage_or_rules_disagree'
                    return result
                start = min(interval['start'] for interval in plan['intervals_local'])
                starts.append(datetime.fromisoformat(civil + 'T' + start).replace(tzinfo=MOSCOW))
            first = min(starts)
            if first <= now:
                continue
            result['D1'].update(status='CANDIDATE_ONLY', candidate_trading_date=target,
                first_published_start=first.isoformat(), contributing_civil_dates=sorted(civil_dates),
                reason='published_plan_candidate_not_forecast_admission')
            return result
        result['D1']['reason'] = 'no_not_started_day_in_joint_published_coverage'
    except (ValueError, TypeError, KeyError, OSError, OverflowError):
        result['D1']['reason'] = 'invalid_target_plan_evidence'
    return result


def apply(snapshot, *, now):
    components = snapshot.get('components', {})
    snapshot['trading_target_plan'] = describe(components.get('futures_calendar', {}), now=now)
