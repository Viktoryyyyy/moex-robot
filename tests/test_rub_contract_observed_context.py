from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data import rub_contract_observed_context as context, step3_raw_acceptance as stage3
from test_step3_raw_acceptance import _evidence, _store, _write_json

NOW = datetime(2026, 9, 13, 13, tzinfo=timezone.utc)
RUN = 'contract_test_stage3'


@pytest.fixture
def archive(tmp_path, monkeypatch):
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    evidence = _evidence(tmp_path, RUN)
    for quote in evidence['quote_partitions']:
        frame = pd.DataFrame([{'instrument_id': quote['instrument_id_scope'][0], 'secid': quote['secid_scope'][0],
            'trade_date': '2026-08-24', 'source_id': context.SOURCE,
            'ts': (datetime(2026, 8, 24, 6, tzinfo=timezone.utc)+timedelta(minutes=5*i)).isoformat(),
            'ingest_ts': '2026-08-24T09:05:00+00:00',
            'open': 80.+i, 'high': 82.+i, 'low': 79.+i, 'close': 81.+i, 'volume': 10.} for i in range(10)])
        frame.to_parquet(quote['storage_partition_path'], index=False)
    _store(tmp_path, RUN, evidence)
    stage3.promote_step3_pilot(run_id=RUN)
    parent = tmp_path/'runs/step10_rub_daily_refresh/run_id=contract_test/run_manifest.json'
    _write_json(parent, {'project': 'MOEX_Bot', 'stage': 10, 'run_id': 'contract_test', 'status': 'succeeded',
        'finished_at_utc': '2026-08-24T10:00:00+00:00',
        'source_refresh': {'status': 'refreshed', 'stage3_run_id': RUN, 'trade_date': '2026-08-24'}})
    return tmp_path, evidence, parent


def test_real_acceptance_chain_bounded_contract_dates_units_and_immutable_input(archive):
    root, pilot, _ = archive
    before = {path: path.read_bytes() for path in root.rglob('*') if path.is_file()}
    evidence = context.collect(root, now=NOW)
    assert not evidence['refusals']
    assert len(evidence['rows']) == 4
    result = context.describe(evidence, now=NOW)
    assert result['status'] == 'AVAILABLE'
    assert {row['secid'] for row in result['contracts']} == {'SiU6', 'SiZ6', 'CRU6', 'CRZ6'}
    assert result['historical_original_digest_available'] is False
    assert result['first_accepted_at_utc'] is None
    for contract in result['contracts']:
        assert contract['observations'][0]['close'] == 90
        assert contract['observations'][0]['volume'] == 100
        assert contract['exact_1_5_20_trading_date_comparisons'] is None
        assert contract['observed_weeks'][0]['scope'] == 'observed_archive_dates_only_not_accepted_W1'
    assert all(path.read_bytes() == saved for path, saved in before.items())


def test_old_archive_growth_does_not_blank_recent_context(archive):
    root, _, _ = archive
    for number in range(257):
        run = f'old_{number}'
        _write_json(stage3.acceptance_evidence_path(run), {'status': 'accepted'})
        _write_json(stage3.pilot_evidence_path(run), {'trade_date': '2025-01-01'})
    evidence = context.collect(root, now=NOW)
    assert evidence['inventory_marker_count'] == 258
    assert len(evidence['rows']) == 4 and not evidence['refusals']


@pytest.mark.parametrize('mixed_receipts', [False, True])
def test_binding_must_precede_every_receipt_not_only_parent_finish_or_latest_receipt(archive, mixed_receipts):
    root, pilot, _ = archive
    binding = '2026-08-24T09:06:00+00:00'
    pilot['reference_observed_at_utc'] = binding
    for item in pilot['bindings']:
        item['mapping_fixed_ts_utc'] = item['availability_ts_utc'] = binding
    _store(root, RUN, pilot)
    if mixed_receipts:
        # Latest receipt is after binding, but one earlier receipt still denies admission.
        for quote in pilot['quote_partitions']:
            path = Path(quote['storage_partition_path'])
            frame = pd.read_parquet(path)
            frame.loc[1:, 'ingest_ts'] = '2026-08-24T09:07:00+00:00'
            frame.to_parquet(path, index=False)
    evidence = context.collect(root, now=NOW)
    assert evidence['rows'] == []
    assert evidence['refusals'] == [{'run_id': RUN, 'reason': 'contract_binding_after_source_receipt'}]


def test_binding_equal_to_earliest_receipt_is_admitted_and_preserved(archive):
    root, pilot, _ = archive
    binding = '2026-08-24T09:05:00+00:00'
    pilot['reference_observed_at_utc'] = binding
    for item in pilot['bindings']:
        item['mapping_fixed_ts_utc'] = item['availability_ts_utc'] = binding
    _store(root, RUN, pilot)
    evidence = context.collect(root, now=NOW)
    assert not evidence['refusals'] and len(evidence['rows']) == 4
    assert all(row['source_receipt_lower_bound_utc'] == row['binding_observed_at_utc'] == binding for row in evidence['rows'])
    assert context.describe(evidence, now=NOW)['status'] == 'AVAILABLE'
    for row in evidence['rows']:
        row['binding_observed_at_utc'] = '2026-08-24T09:06:00+00:00'
    assert context.describe(evidence, now=NOW)['status'] == 'UNAVAILABLE'


@pytest.mark.parametrize('defect', ['status', 'rollback', 'run', 'date', 'future_finish', 'future_binding', 'missing_parent',
    'marker_status', 'marker_ref', 'pilot_immutable', 'secid', 'source', 'nan', 'negative_volume', 'duplicate_ts', 'future_receipt'])
def test_invalid_accepted_chain_refused_as_sparse_gap_not_substitute(archive, defect):
    root, pilot, parent_path = archive
    parent = json.loads(parent_path.read_text())
    if defect == 'status': parent['status'] = 'failed'
    elif defect == 'rollback': parent['current_pointer_rollback_status'] = 'restored'
    elif defect == 'run': parent['source_refresh']['stage3_run_id'] = 'another_run'
    elif defect == 'date': parent['source_refresh']['trade_date'] = '2026-08-25'
    elif defect == 'future_finish': parent['finished_at_utc'] = (NOW+timedelta(seconds=1)).isoformat()
    _write_json(parent_path, parent)
    if defect == 'missing_parent': parent_path.unlink()
    if defect in ('future_binding', 'pilot_immutable'):
        if defect == 'future_binding': pilot['reference_observed_at_utc'] = (NOW+timedelta(seconds=1)).isoformat()
        else: pilot['run_artifacts_immutable'] = False
        _store(root, RUN, pilot)
    if defect.startswith('marker'):
        marker_path = stage3.acceptance_evidence_path(RUN)
        marker = json.loads(marker_path.read_text())
        if defect == 'marker_status': marker['status'] = 'failed'
        else: marker['pointers'][0]['manifest_ref'] = marker['pointers'][1]['manifest_ref']
        _write_json(marker_path, marker)
    if defect in ('secid', 'source', 'nan', 'negative_volume', 'duplicate_ts', 'future_receipt'):
        path = Path(pilot['quote_partitions'][0]['storage_partition_path'])
        frame = pd.read_parquet(path)
        if defect == 'secid': frame.loc[0, 'secid'] = 'SiZ6'
        elif defect == 'source': frame.loc[0, 'source_id'] = 'unapproved'
        elif defect == 'nan': frame.loc[0, 'close'] = float('nan')
        elif defect == 'negative_volume': frame.loc[0, 'volume'] = -1
        elif defect == 'duplicate_ts': frame.loc[1, 'ts'] = frame.loc[0, 'ts']
        else: frame.loc[0, 'ingest_ts'] = (NOW+timedelta(seconds=1)).isoformat()
        frame.to_parquet(path, index=False)
    evidence = context.collect(root, now=NOW)
    assert not evidence['rows'] and evidence['refusals']


def test_replay_provenance_identity_future_clock_and_sparse_holes(archive):
    root, _, _ = archive
    source = context.collect(root, now=NOW)
    row = deepcopy(source['rows'][0])
    row.update(source_date='2026-08-26', source_first_bar_at_utc='2026-08-26T06:00:00+00:00',
        source_last_bar_at_utc='2026-08-26T06:45:00+00:00', source_receipt_upper_bound_utc='2026-08-26T09:05:00+00:00',
        source_receipt_lower_bound_utc='2026-08-26T09:05:00+00:00',
        parent_finished_at_utc='2026-08-26T10:00:00+00:00')
    row['binding_observed_at_utc'] = row['source_provenance']['binding_availability_ts_utc'] = '2026-08-26T09:00:00+00:00'
    row['binding_at_source'].update(as_of_date='2026-08-26', mapping_fixed_ts_utc='2026-08-26T09:00:00+00:00',
                                   availability_ts_utc='2026-08-26T09:00:00+00:00')
    source['rows'].append(row)
    view = context.describe(source, now=NOW)
    contract = next(item for item in view['contracts'] if item['secid'] == row['secid'])
    assert contract['calendar_dates_without_accepted_run'] == ['2026-08-25']
    assert contract['comparison_refusal'] == 'complete_observed_date_universe_unproven'
    before = deepcopy(source)
    assert context.describe(source, now=NOW-timedelta(seconds=1))['status'] == 'UNAVAILABLE'
    assert source == before
    source['rows'][0]['source_provenance']['secid'] = 'SiZ6'
    assert context.describe(source, now=NOW)['status'] == 'UNAVAILABLE'


def test_release_compact_export_oracle_same_input(archive, tmp_path):
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    root, _, _ = archive
    source = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {'stage9_daily': {'status': 'READY',
        'data': {'server_core': {'blocks': [], 'contract_price_evidence': context.collect(root, now=NOW)}}}}}
    before = deepcopy(source)
    built = release.build(source, now=NOW, code_revision='a'*40)
    projection_completeness(source, built, now=NOW)
    compact = release.compact(source, now=NOW, code_revision='a'*40)
    assert compact['contract_price_context']['status'] == 'AVAILABLE'
    assert len(compact['contract_price_context']['contracts']) == 4
    directory = release.export(source, now=NOW, code_revision='a'*40, output=tmp_path/'exports')
    replay = release.build(json.loads((directory/'input_snapshot.json').read_text()), now=NOW, code_revision='a'*40)
    assert replay == built and source == before
    for mutation in ('omission', 'price', 'scope'):
        bad = deepcopy(built)
        if mutation == 'omission': bad['contract_price_context']['contracts'].pop()
        elif mutation == 'price': bad['contract_price_context']['contracts'][0]['observations'][0]['close'] += 1
        else: bad['contract_price_context']['historical_pit_usable'] = True
        with pytest.raises(AssertionError, match='contract observed date completeness'):
            projection_completeness(source, bad, now=NOW)


@pytest.mark.parametrize('field,value', [('secid', 'ALIEN'), ('instrument_id', 'cr_front_contract'),
    ('root', 'CR'), ('role', 'next'), ('source_id', 'unapproved'), ('as_of_date', '2026-08-23'),
    ('last_trade_date', '1900-01-01'), ('availability_ts_utc', '2026-08-24T08:00:00+00:00'),
    ('mapping_fixed_ts_utc', '2026-08-24T08:00:00+00:00')])
def test_retained_binding_field_tamper_refused(archive, field, value):
    source = context.collect(archive[0], now=NOW)
    source['rows'][0]['binding_at_source'][field] = value
    assert context.describe(source, now=NOW)['status'] == 'UNAVAILABLE'


@pytest.mark.parametrize('field', ['acceptance_run_id', 'run_id', 'accepted_marker_ref', 'parent_manifest_ref',
    'manifest_ref', 'partition_ref', 'quality_report_ref', 'accepted_marker_sha256_at_revalidation',
    'pilot_evidence_sha256_at_revalidation', 'parent_manifest_sha256_at_revalidation', 'hash_semantics'])
def test_retained_chain_cannot_be_stripped(archive, field):
    source = context.collect(archive[0], now=NOW)
    source['rows'][0]['source_provenance'].pop(field)
    assert context.describe(source, now=NOW)['status'] == 'UNAVAILABLE'


@pytest.mark.parametrize('defect', ['parent_after_capture', 'receipt_before_first_bar', 'zero_count', 'negative_gap', 'completed_session'])
def test_adjacent_capture_causality_and_scope_refused(archive, defect):
    source = context.collect(archive[0], now=NOW)
    row = source['rows'][0]
    if defect == 'parent_after_capture': source['captured_at_utc'] = '2026-08-24T09:30:00+00:00'
    elif defect == 'receipt_before_first_bar': row['source_receipt_lower_bound_utc'] = '2026-08-24T05:30:00+00:00'
    elif defect == 'zero_count': row['source_bar_count'] = 0
    elif defect == 'negative_gap': row['intraday_gap_count'] = -1
    else: row['session_completion_proven'] = True
    assert context.describe(source, now=NOW)['status'] == 'UNAVAILABLE'


@pytest.mark.parametrize('offset,expected', [(45, 'AVAILABLE'), (46, 'UNAVAILABLE')])
def test_retained_window_anchored_to_capture_moscow_date(archive, offset, expected):
    source = context.collect(archive[0], now=NOW)
    # One day alone has zero span, but still must be inside the capture window.
    source['rows'] = source['rows'][:1]
    captured = datetime(2026, 8, 24, 21, tzinfo=timezone.utc) + timedelta(days=offset-1)
    source['captured_at_utc'] = captured.isoformat()
    assert context.describe(source, now=captured+timedelta(days=30))['status'] == expected


@pytest.mark.parametrize('defect', ['missing_ref', 'other_ref', 'traversal_ref', 'other_path', 'relative_path', 'null_path'])
def test_marker_canonical_pointer_identity_required(archive, defect):
    root, _, _ = archive
    path = stage3.acceptance_evidence_path(RUN)
    marker = json.loads(path.read_text())
    item = marker['pointers'][0]
    if defect == 'missing_ref': item.pop('pointer_ref')
    elif defect == 'other_ref': item['pointer_ref'] = marker['pointers'][1]['pointer_ref']
    elif defect == 'traversal_ref': item['pointer_ref'] = '${MOEX_DATA_ROOT}/../current_accepted_manifest.json'
    elif defect == 'other_path': item['pointer_path'] = marker['pointers'][1]['pointer_path']
    elif defect == 'relative_path': item['pointer_path'] = 'current_accepted_manifest.json'
    else: item['pointer_path'] = None
    _write_json(path, marker)
    evidence = context.collect(root, now=NOW)
    assert not evidence['rows'] and evidence['refusals']


def test_portable_pointer_ref_does_not_require_optional_absolute_path(archive):
    root, _, _ = archive
    path = stage3.acceptance_evidence_path(RUN)
    marker = json.loads(path.read_text())
    for item in marker['pointers']: item.pop('pointer_path')
    _write_json(path, marker)
    assert len(context.collect(root, now=NOW)['rows']) == 4


@pytest.mark.parametrize('defect', ['one_contract', 'one_row', 'all_contracts', 'invalid_binding_admitted'])
def test_independent_oracle_detects_shared_descriptor_fault(archive, monkeypatch, defect):
    from moex_data import rub_factual_release as release
    from moex_data.rub_factual_release_acceptance import projection_completeness
    evidence = context.collect(archive[0], now=NOW)
    admitted = context.describe(evidence, now=NOW)
    if defect == 'invalid_binding_admitted': evidence['rows'][0]['binding_at_source']['last_trade_date'] = '1900-01-01'
    source = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {'stage9_daily': {'status': 'READY',
        'data': {'server_core': {'blocks': [], 'contract_price_evidence': evidence}}}}}
    def faulty_descriptor(*args, **kwargs):
        result = deepcopy(admitted)
        if defect == 'one_contract': result['contracts'].pop()
        elif defect == 'one_row':
            result['contracts'][0]['observations'] = []
            result['contracts'][0]['observed_date_count'] = 0
            result['contracts'][0]['observed_weeks'] = []
        elif defect == 'all_contracts': result.update(status='UNAVAILABLE', contracts=[])
        return result
    monkeypatch.setattr(context, 'describe', faulty_descriptor)
    built = release.build(source, now=NOW, code_revision='a'*40)
    with pytest.raises(AssertionError, match='contract independent source-to-output completeness'):
        projection_completeness(source, built, now=NOW)
