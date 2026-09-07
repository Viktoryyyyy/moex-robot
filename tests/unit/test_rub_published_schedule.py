from copy import deepcopy
from datetime import datetime

import pytest

from moex_data.rub_published_schedule import ARTIFACT, describe
from moex_data.rub_snapshot_read_freshness import apply_read_freshness


@pytest.mark.parametrize('stamp,phase', [
    ('2026-09-08T06:49:59+03:00', 'OUTSIDE_PUBLISHED_INTERVALS'),
    ('2026-09-08T06:50:00+03:00', 'OPENING_AUCTION'),
    ('2026-09-08T07:00:00+03:00', 'MORNING'),
    ('2026-09-08T09:30:00+03:00', 'MORNING'),
    ('2026-09-08T10:00:00+03:00', 'MAIN'),
    ('2026-09-08T19:00:00+03:00', 'EVENING'),
    ('2026-09-08T23:49:59+03:00', 'EVENING'),
    ('2026-09-08T23:50:00+03:00', 'OUTSIDE_PUBLISHED_INTERVALS'),
    ('2026-09-12T12:00:00+03:00', 'PUBLISHED_NO_SESSION'),
    ('2026-09-13T12:00:00+03:00', 'PUBLISHED_NO_SESSION'),
    ('2026-09-14T08:59:59+03:00', 'MORNING'),
    ('2026-09-14T09:00:00+03:00', 'MAIN'),
])
def test_published_boundaries_and_dated_change_without_actual_state_claim(stamp, phase):
    view = describe(now=datetime.fromisoformat(stamp))
    assert view['published_plan_covered'] is True
    assert view['planned_phase'] == phase
    assert view['instruments'] == ['si_futures_family', 'cr_futures_family']
    assert view['actual_session_state'] == 'UNKNOWN'
    assert view['session_completion_proven'] is False
    assert view['authority_granted'] is False
    assert view['exception_updates_verified'] is False


@pytest.mark.parametrize('stamp', [
    '2026-09-07T10:00:00+03:00',  # No historical knowledge before the review.
    '2026-09-15T10:00:00+03:00',  # No unreviewed weekday extrapolation.
    '2026-09-19T10:00:00+03:00',  # No automatic weekend closure.
    '2026-09-08T10:00:00',
])
def test_uncovered_or_ambiguous_time_is_unknown(stamp):
    view = describe(now=datetime.fromisoformat(stamp))
    assert view['published_plan_covered'] is False
    assert view['planned_phase'] == 'UNKNOWN'


def test_moscow_civil_date_selected_at_utc_boundary():
    view = describe(now=datetime.fromisoformat('2026-09-11T21:00:00+00:00'))
    assert view['civil_date'] == '2026-09-12'
    assert view['planned_phase'] == 'PUBLISHED_NO_SESSION'


@pytest.mark.parametrize('defect', ['missing', 'corrupted', 'modified'])
def test_untrusted_plan_cannot_supply_coverage(tmp_path, defect):
    path = tmp_path / 'plan.json'
    if defect == 'corrupted':
        path.write_bytes(b'{')
    elif defect == 'modified':
        path.write_bytes(ARTIFACT.read_bytes().replace(b'23:50', b'23:59'))
    view = describe(now=datetime.fromisoformat('2026-09-08T10:00:00+03:00'), artifact_path=path)
    assert view['published_plan_covered'] is False
    assert view['planned_phase'] == 'UNKNOWN'


def test_read_view_does_not_promote_authority_or_change_persisted_snapshot():
    original = {'components': {}, 'authority': {'broker_execution': False,
        'futoi_factual_authority': False}}
    before = deepcopy(original)
    result = apply_read_freshness(original, now=datetime.fromisoformat('2026-09-08T10:00:00+03:00'))
    assert original == before
    view = result['temporal_applicability']
    assert view['published_schedule_plan']['planned_phase'] == 'MAIN'
    assert view['session_state'] == 'UNKNOWN'
    assert view['calendar_coverage_accepted'] is False
    assert result['authority']['broker_execution'] is False
    assert result['authority']['futoi_factual_authority'] is False
