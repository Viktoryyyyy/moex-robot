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
