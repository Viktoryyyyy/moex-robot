"""Acquired-now evidence envelope; semantic admission is deliberately fail-closed."""
from copy import deepcopy

from moex_data.rub_dated_context import MAX_AGE_SECONDS, digest, stamp

ORIGIN = 'source_observation_acquired_now'
SCHEMA = 'rub_dated_source_evidence.v1'


def validate_envelope(frame, *, now):
    """Validate custody, not source truth or normalized market values.

    Return an immutable-by-copy revision description. A valid envelope alone
    cannot be selected for preparation, live, models, or historical PIT.
    """
    try:
        now = stamp(now)
        if frame.get('schema_version') != SCHEMA or frame.get('origin') != ORIGIN:
            raise ValueError('unsupported_source_evidence_schema_or_origin')
        if frame.get('scope') != 'preparation_only' or frame.get('revision_semantics') != 'observed_now_not_historical_pit':
            raise ValueError('invalid_source_evidence_scope')
        for flag in ('current_usable', 'historical_pit_usable', 'model_usable'):
            if frame.get(flag) is not False:
                raise ValueError('source_evidence_usage_not_restricted')
        for field in ('source_id', 'purpose'):
            if not isinstance(frame.get(field), str) or not frame[field].strip():
                raise ValueError('missing_source_evidence_identity')
        for field in ('identity', 'units'):
            value = frame.get(field)
            if not isinstance(value, dict) or not value or any(
                    not isinstance(k, str) or not k.strip() or not isinstance(v, str) or not v.strip()
                    for k, v in value.items()):
                raise ValueError('missing_source_evidence_identity_or_units')
        raw = frame['raw_source_payload']
        if not isinstance(raw, (dict, list)) or not raw:
            raise ValueError('missing_raw_source_evidence')
        raw_digest = digest(raw)
        if frame['raw_source_digest'] != raw_digest:
            raise ValueError('raw_source_digest_mismatch')
        requested = stamp(frame['request_started_at_utc'])
        received = stamp(frame['received_at_utc'])
        accepted = stamp(frame['accepted_at_utc'])
        observed = stamp(frame['source_observation_at_utc'])
        if not requested <= received <= accepted <= now or observed > received:
            raise ValueError('noncausal_source_evidence')
        if not 0 <= (now - observed).total_seconds() <= MAX_AGE_SECONDS:
            raise ValueError('dated_source_age_outside_96_hours')
        revision = {key: deepcopy(frame[key]) for key in
                    ('source_id', 'purpose', 'identity', 'units', 'scope', 'revision_semantics')}
        revision.update(raw_source_digest=raw_digest, source_observation_at_utc=observed.isoformat())
        revision_id = digest(revision)
        if frame['revision_id'] != revision_id:
            raise ValueError('source_revision_digest_mismatch')
        return {'revision_id': revision_id, 'revision': revision,
                'semantic_admission': 'unsupported_source_replay', 'current_usable': False,
                'historical_pit_usable': False, 'model_usable': False}
    except (KeyError, TypeError, AttributeError, OverflowError) as exc:
        raise ValueError('malformed_source_evidence') from exc


def eligible_source_observation(frame):
    """No B purpose is admitted before a source-specific semantic replay exists."""
    validate_envelope(frame, now=frame.get('accepted_at_utc'))
    raise ValueError('unsupported_source_replay')
