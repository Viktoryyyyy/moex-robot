from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import inspect
import json
from pathlib import Path

import pytest
import yaml

from moex_data import rub_macro_requirements as requirements
from moex_research.external_data import cbr_meeting_calendar as calendar
from moex_research.external_data import registry
from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner

ROOT = Path(__file__).resolve().parents[2]


def contract():
    return json.loads((ROOT / calendar.RUNTIME_CONTRACT_REF).read_text(encoding='utf-8'))


def test_research_placeholder_bytes_and_denials_are_unchanged():
    raw = (ROOT / contract()['legacy_design_reference']).read_bytes()
    # Pin both exact checkout representations; Git uses LF, Windows uses CRLF.
    assert sha256(raw).hexdigest() in {
        'fe40d7931bab76f1c44768f97edc6ea40dd729152aca4955f4e140db8133bd4a',
        '26f6a894c41a2d31ce13386750a1272d9ac664850cadf0593503daf874f3d5b0'}
    old = yaml.safe_load(raw)
    assert old['status'] == 'design_only'
    assert old['readiness_flags']['design_only'] is True
    assert all(value is False for key, value in old['readiness_flags'].items() if key != 'design_only')
    assert old['availability_rules']['eligibility_rule'] == 'availability_ts_utc <= forecast_anchor_ts'
    assert contract()['legacy_design_reference_governs_this_runtime'] is False


def test_current_contract_permits_only_received_schedule_runtime():
    value = contract()
    assert value['status'] == 'CURRENT_RECEIVED_SCHEDULE'
    assert value['task_id'] == 'cbr_calendar_runtime_contract_alignment_v1'
    allowed = {'current_schedule_retrieval', 'current_schedule_runtime', 'factual_chat_schedule_display'}
    assert {key for key, flag in value['permissions'].items() if flag is True} == allowed
    assert all(flag is False for key, flag in value['permissions'].items() if key not in allowed)
    assert value['availability_policy']['source_publication_time'] is None
    assert value['availability_policy']['actual_event_time'] is None
    assert value['availability_policy']['maximum_receipt_age_seconds'] == calendar.MAX_RECEIPT_SECONDS
    assert value['receipt_policy'] == calendar.POLICY
    assert value['evidence_scope'] == calendar.SCOPE


def test_runtime_registry_requirements_and_producer_share_identity():
    value = contract(); entry = registry.CURRENT_RECEIVED_SCHEDULE_REGISTRY[calendar.SOURCE_ID]
    assert calendar.SOURCE_ID not in registry.SOURCE_REGISTRY
    assert calendar.SOURCE_ID not in registry.SOURCE_SLOTS
    assert entry['contract_ref'] == calendar.RUNTIME_CONTRACT_REF
    assert entry['calendar_identity'] == value['calendar_identity'] == calendar.CALENDAR_IDENTITY
    assert value['source_id'] == calendar.SOURCE_ID
    assert value['source_url'] == calendar.SOURCE_URL
    assert value['component'] == calendar.COMPONENT
    row = next(r for r in requirements.describe()['requirements'] if r['requirement_id'] == 'cbr_key_rate_meeting_schedule')
    assert row['source_id'] == calendar.SOURCE_ID
    assert row['event_family'] == calendar.CALENDAR_IDENTITY
    assert row['runtime_contract_ref'] == calendar.RUNTIME_CONTRACT_REF
    assert row['source_registry_ref'].endswith('#CURRENT_RECEIVED_SCHEDULE_REGISTRY')
    assert row['admitted'] is False
    assert runner.default_producers()[calendar.COMPONENT] is runner._cbr_meeting_calendar_component
    assert 'RUNTIME_CONTRACT_REF' in inspect.getsource(runner._cbr_meeting_calendar_component)


def test_default_producer_preserves_existing_receipt_payload(monkeypatch, tmp_path):
    payload = {'received_at': '2026-09-08T12:00:00+00:00', 'policy': calendar.POLICY,
        'scope': calendar.SCOPE, 'calendar_schedule_usable': True,
        **dict.fromkeys(calendar.DENIED, False)}
    before = deepcopy(payload)
    monkeypatch.setattr(runner, '_data_root', lambda: tmp_path)
    monkeypatch.setattr(calendar, 'load', lambda *, root: payload if root == tmp_path else None)
    produced = runner._cbr_meeting_calendar_component(datetime(2026,9,8,12,tzinfo=timezone.utc))
    assert produced.data == before == payload
    assert produced.data_as_of == payload['received_at']


def test_missing_runtime_registration_fails_requirements(monkeypatch):
    monkeypatch.delitem(registry.CURRENT_RECEIVED_SCHEDULE_REGISTRY, calendar.SOURCE_ID)
    with pytest.raises(ValueError, match='schedule registry mismatch'):
        requirements.describe()
