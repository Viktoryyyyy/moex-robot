"""Synthetic custody fixtures: no claim of archived production/source replay."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json

import pytest

from moex_data import rub_dated_context as dated
from moex_data.rub_dated_source_admission import validate_envelope

NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)


def hashed(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def rehash(frame):
    frame['raw_source_digest'] = hashed(frame['raw_source_payload'])
    revision = {k: frame[k] for k in ('source_id', 'purpose', 'identity', 'units', 'scope', 'revision_semantics')}
    revision.update(raw_source_digest=frame['raw_source_digest'],
                    source_observation_at_utc=datetime.fromisoformat(frame['source_observation_at_utc']).astimezone(timezone.utc).isoformat())
    frame['revision_id'] = hashed(revision)
    return frame


def evidence():
    return rehash({'schema_version': 'rub_dated_source_evidence.v1',
                  'origin': 'source_observation_acquired_now', 'scope': 'preparation_only',
                  'revision_semantics': 'observed_now_not_historical_pit',
                  'source_id': 'moex_iss_rfud', 'purpose': 'market:usdrubf',
                  'identity': {'secid': 'USDRUBF', 'boardid': 'RFUD'},
                  'units': {'last': 'RUB_per_USD'},
                  'current_usable': False, 'historical_pit_usable': False, 'model_usable': False,
                  'source_observation_at_utc': '2026-09-11T20:50:01+00:00',
                  'request_started_at_utc': '2026-09-13T11:59:58+00:00',
                  'received_at_utc': '2026-09-13T11:59:59+00:00',
                  'accepted_at_utc': NOW.isoformat(),
                  'raw_source_payload': {'columns': ['SECID', 'LAST'], 'data': [['USDRUBF', 80.0]]}})


def test_valid_custody_is_not_semantic_admission_or_live():
    frame = evidence()
    result = validate_envelope(frame, now=NOW)
    assert result['semantic_admission'] == 'unsupported_source_replay'
    assert result['current_usable'] is result['historical_pit_usable'] is result['model_usable'] is False
    with pytest.raises(ValueError, match='unsupported_source_replay'):
        dated.eligible(frame)
    ref = hashed(frame)
    admitted, rejected = dated.validated({'schema_version': dated.SCHEMA,
                                        'frames': {ref: frame}, 'selections': {'market:usdrubf': ref}}, NOW)
    assert admitted == {}
    assert rejected == {'market:usdrubf': 'unsupported_source_replay'}


def test_revision_stable_across_retrieval_but_changes_with_source_evidence():
    frame = evidence()
    repeated = deepcopy(frame)
    for key in ('request_started_at_utc', 'received_at_utc', 'accepted_at_utc'):
        repeated[key] = (datetime.fromisoformat(frame[key]) + timedelta(seconds=30)).isoformat()
    assert validate_envelope(repeated, now=NOW + timedelta(seconds=30))['revision_id'] == frame['revision_id']
    for key, value in [('identity', {'secid': 'SiU6'}), ('units', {'last': 'RUB_per_1000_USD'}),
                       ('raw_source_payload', {'changed': 1}), ('source_observation_at_utc', '2026-09-11T21:00:00+00:00')]:
        changed = deepcopy(frame)
        changed[key] = value
        assert rehash(changed)['revision_id'] != frame['revision_id']


@pytest.mark.parametrize('field,value', [
    ('origin', None), ('schema_version', 'other'), ('scope', 'live'),
    ('revision_semantics', 'historical_pit'), ('current_usable', True),
    ('historical_pit_usable', True), ('model_usable', 0), ('source_id', ''),
    ('purpose', ''), ('identity', {}), ('units', {'last': 1}), ('raw_source_payload', {}),
    ('raw_source_digest', '0' * 64), ('revision_id', '0' * 64),
    ('request_started_at_utc', '2026-09-13T12:00:00+00:00'),
    ('received_at_utc', '2026-09-13T12:00:01+00:00'),
    ('accepted_at_utc', '2026-09-13T12:00:01+00:00'),
    ('accepted_at_utc', '2026-09-13T11:59:58+00:00'),
    ('source_observation_at_utc', '2026-09-13T12:00:00+00:00'),
    ('source_observation_at_utc', '2026-09-09T11:59:59+00:00'),
    ('received_at_utc', '2026-09-13T11:59:59'),
])
def test_envelope_defects_rejected(field, value):
    frame = evidence()
    frame[field] = value
    with pytest.raises(ValueError):
        validate_envelope(frame, now=NOW)


def test_96h_boundary_and_consumer_expiry():
    frame = evidence()
    frame['source_observation_at_utc'] = (NOW - timedelta(hours=96)).isoformat()
    rehash(frame)
    validate_envelope(frame, now=NOW)
    with pytest.raises(ValueError, match='outside_96_hours'):
        validate_envelope(frame, now=NOW + timedelta(microseconds=1))


@pytest.mark.parametrize('field,value', [('identity', {'secid': 'ALIEN'}),
                                        ('units', {'last': 'ALIEN'}), ('purpose', 'futoi'),
                                        ('source_id', 'invented'), ('raw_source_payload', {'accepted': True})])
def test_rehashed_semantic_claims_never_admitted(field, value):
    frame = evidence()
    frame[field] = value
    rehash(frame)
    with pytest.raises(ValueError, match='unsupported_source_replay'):
        dated.eligible(frame)


@pytest.mark.parametrize('origin', [None, '', 'unknown', {}, []])
def test_unknown_origin_never_falls_through_to_live(origin):
    with pytest.raises(ValueError, match='unsupported_dated_evidence_origin'):
        dated.eligible({'origin': origin})
