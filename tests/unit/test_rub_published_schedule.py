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
    '2026-09-22T10:00:00+03:00',  # No unreviewed weekday extrapolation.
    '2026-09-26T10:00:00+03:00',  # No automatic weekend closure.
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


@pytest.mark.parametrize('stamp,phase', [
    ('2026-09-15T08:59:59+03:00', 'MORNING'),
    ('2026-09-15T09:00:00+03:00', 'MAIN'),
    ('2026-09-18T19:00:00+03:00', 'EVENING'),
    ('2026-09-19T09:49:59+03:00', 'OUTSIDE_PUBLISHED_INTERVALS'),
    ('2026-09-19T09:50:00+03:00', 'OPENING_AUCTION'),
    ('2026-09-19T10:00:00+03:00', 'WEEKEND_ADDITIONAL'),
    ('2026-09-20T18:59:59+03:00', 'WEEKEND_ADDITIONAL'),
    ('2026-09-20T19:00:00+03:00', 'OUTSIDE_PUBLISHED_INTERVALS'),
    ('2026-09-21T23:49:59+03:00', 'EVENING'),
])
def test_reviewed_extension_has_archive_lineage_and_no_actual_authority(stamp, phase):
    result = describe(now=datetime.fromisoformat(stamp))
    assert result['published_plan_covered'] is True
    assert result['planned_phase'] == phase
    assert result['coverage_end_inclusive'] == '2026-09-21'
    assert len(result['source_evidence']) == 4
    assert result['source_dependencies']
    assert all(len(source['raw_sha256']) == 64 and len(source['receipt_sha256']) == 64
               for source in result['source_evidence'])
    for key in ('authority_granted', 'session_completion_proven', 'historical_pit_acceptance',
                'exception_updates_verified', 'forecast_trading_targets_accepted', 'source_archive_replayed'):
        assert result[key] is False
    assert result['actual_session_state'] == 'UNKNOWN'


def test_updated_review_is_not_backdated_and_coverage_does_not_extrapolate():
    from datetime import timedelta
    from moex_data import rub_published_schedule as module
    prior = describe(now=module.UPDATED_REVIEWED_AT - timedelta(microseconds=1))
    current = describe(now=module.UPDATED_REVIEWED_AT)
    assert prior['artifact_sha256'] == module.ARTIFACT_SHA256
    assert current['artifact_sha256'] == module.UPDATED_SHA256
    assert describe(now=datetime.fromisoformat('2026-09-22T00:00:00+03:00'))['published_plan_covered'] is False


@pytest.mark.parametrize('defect', ['source_missing', 'source_modified', 'updated_missing', 'old_substitution'])
def test_broken_update_does_not_fall_back_to_prior_plan(tmp_path, monkeypatch, defect):
    import json
    from moex_data import rub_published_schedule as module
    raw = module.UPDATED_ARTIFACT.read_bytes()
    evidence = json.loads(raw)
    updated = tmp_path / 'updated.json'
    updated.write_bytes(raw)
    source = tmp_path / evidence['source_evidence_manifest']
    source.write_bytes(module.UPDATED_ARTIFACT.with_name(source.name).read_bytes())
    if defect == 'source_missing': source.unlink()
    elif defect == 'source_modified': source.write_bytes(source.read_bytes() + b' ')
    elif defect == 'updated_missing': updated.unlink()
    else: updated.write_bytes(module.ARTIFACT.read_bytes())
    monkeypatch.setattr(module, 'UPDATED_ARTIFACT', updated)
    result = module.describe(now=datetime.fromisoformat('2026-09-13T12:00:00+03:00'))
    assert result['published_plan_covered'] is False
    assert result['planned_phase'] == 'UNKNOWN'
