"""Reproducible D1/W1 factual inventory, with no model or session-completion claim."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import subprocess
import os
from moex_data.rub_factual_projection import spot_usable, basis_metrics, market_values, IDENTITY_FIELDS, consumer_context, fresh

SCHEMA = 'rub_factual_release.v1'


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def describe(snapshot):
    from moex_data.rub_production_source_matrix import build as matrix_build
    from moex_data.rub_macro_evidence_inventory import describe as describe_macro, reconcile_components
    snapshot = dict(snapshot)
    if not isinstance(snapshot.get('components'), dict):
        snapshot['components'] = {}
    else:
        snapshot['components'] = dict(snapshot['components'])
    try:
        reference = (snapshot['live_read_freshness']['read_at_utc']
            if 'live_read_freshness' in snapshot else snapshot['identity']['generated_at_utc'])
        target_now = datetime.fromisoformat(reference)
        if target_now.utcoffset() is None:
            target_now = None
    except (ValueError, TypeError, KeyError):
        target_now = None
    if target_now is not None:
        from moex_data.rub_snapshot_read_freshness import apply_read_freshness
        # A failed read-view reconciliation cannot fall back to persisted flags:
        # those flags may admit expired current pairs from the old generation.
        snapshot = apply_read_freshness(snapshot, now=target_now)
    snapshot = reconcile_components(snapshot, now=target_now)
    if 'external_cny' in snapshot['components']:
        from moex_research.external_data.fred_cny_factual import reconcile as reconcile_cny
        # build/API already apply read freshness. Direct describe callers must
        # use the same receipt/evidence decision for both the matrix and facts.
        snapshot['components']['external_cny'] = reconcile_cny(
            snapshot['components']['external_cny'], now=target_now)
    matrix = matrix_build(snapshot)
    components = snapshot['components']
    facts = []
    market = (components.get('synchronized_live_market_oi', {}).get('data') or {}).get('instruments', {})
    for key, item in market.items():
        if (spot_usable(snapshot) if key == 'cnyrub_tom' else item.get('price_oi_usable') is True and fresh(item, target_now)):
            facts.append({'factor': key, 'scope': 'current_source_row',
                'snapshot_path': f'components.synchronized_live_market_oi.data.instruments.{key}',
                'source_identity': {k: item.get(k) for k in IDENTITY_FIELDS if k in item},
                'values': market_values(item, spot=key == 'cnyrub_tom')})
    metrics = basis_metrics(snapshot)
    if metrics:
        facts.append({'factor': 'basis_carry', 'scope': 'individual_READY_metrics_only',
            'snapshot_path': 'components.live_basis_carry.data',
            'values': {'metrics': [{'snapshot_path': path, 'values': deepcopy(metric)} for path, metric in metrics]}})
    for key in ('oil', 'external_cny', 'rosstat_cpi', 'cbr_rates_verified', 'rosstat_monthly_cpi', 'cbr_liquidity_verified', 'futoi_live', 'futoi_live_cr'):
        component = components.get(key, {})
        data = component.get('data') or {}
        macro_block = {'external_cny': 'external_cny', 'rosstat_cpi': 'rosstat_macro',
            'cbr_rates_verified': 'cbr_rates', 'rosstat_monthly_cpi': 'rosstat_macro',
            'cbr_liquidity_verified': 'cbr_rates'}.get(key)
        admission_field = {'rosstat_monthly_cpi': 'monthly_cpi_factual_context_usable',
            'cbr_liquidity_verified': 'liquidity_factual_context_usable'}.get(key, 'factual_context_usable')
        if macro_block is not None and not any(row['block_id'] == macro_block and
                row.get(admission_field) is True for row in matrix['rows']):
            continue
        if component.get('status') != 'READY' or data.get('consumer_factual_use_allowed') is not True:
            continue
        if key.startswith('futoi'):
            fact = data.get('current_intraday', {}).get('factual')
            scope = 'current_pair_only_no_previous_or_history_grant'
        else:
            fact = {k: data[k] for k in ('secid', 'series_id', 'units', 'value', 'price', 'price_unit', 'ohlc',
                'observations', 'document_format', 'next_scheduled_release', 'weekly_release_calendar_accepted', 'observation_start', 'observation_end', 'indices', 'weekly_change_percent', 'listed_publication_date', 'index_manifest_sha256', 'document_manifest_sha256', 'source_trade_date', 'observation_date', 'received_at', 'source_url', 'manifest_sha256', 'raw_sha256', 'provenance', 'observation_month', 'changes_percent', 'quality_status', 'limitations', 'arithmetic_residual', 'source_revision_status') if k in data}
            scope = 'latest_published_dated_reference'
        facts.append({'factor': key, 'scope': scope, 'snapshot_path': 'components.' + key + '.data', 'values': fact})
    horizons = {}
    from moex_data.rub_trading_target_plan import describe as describe_targets
    target_plan = describe_targets(components.get('futures_calendar', {}), now=target_now)
    for horizon, key in (('D1', 'stage9_daily'), ('W1', 'stage9_weekly')):
        component = components.get(key, {})
        data = component.get('data') or {}
        horizons[horizon] = {'kind': 'FACTUAL_CONTEXT_NOT_FORECAST',
            'status': 'INCOMPLETE', 'snapshot_path': 'components.' + key,
            'component_status': component.get('status', 'UNAVAILABLE'),
            'source_as_of': component.get('data_as_of'),
            'source_readiness': deepcopy(data.get('readiness', {})),
            'planning_candidate': target_plan[horizon],
            'target_trading_date': None, 'target_date_proven': False,
            'model_probability': None, 'forecast_generated': False}
    freshness = snapshot.get('live_read_freshness')
    from moex_data.rub_dated_context import describe as dated_context
    return {'schema_version': SCHEMA, 'as_of_utc': freshness.get('read_at_utc') if isinstance(freshness, dict) else None,
        'status': 'INCOMPLETE', 'facts': facts, 'horizons': horizons, **consumer_context(snapshot),
        'dated_context': dated_context(snapshot, now=target_now) if target_now else {'status': 'UNAVAILABLE', 'observations': {}},
        'macro_evidence_inventory': describe_macro(snapshot, now=target_now),
        'blocking_required_factors': matrix['blocking_required_blocks'],
        'matrix': matrix['rows'], 'session_completion_proven': False,
        'historical_dataset_accepted': False, 'model_validated': False,
        'training_authorized': False, 'broker_execution': False,
        'limitations': ['session_and_target_trading_dates_unproven',
            'news_relevance_corpus_not_accepted', 'macro_providers_incomplete',
            'historical_vintages_and_causal_alignment_not_accepted', 'model_evaluation_paused']}


def build(snapshot, *, now, code_revision):
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    if not isinstance(code_revision, str) or not re.fullmatch('[0-9a-f]{40}', code_revision):
        raise ValueError('exact code commit required')
    generated = datetime.fromisoformat(snapshot['identity']['generated_at_utc'])
    if now.utcoffset() is None or generated.utcoffset() is None or generated > now:
        raise ValueError('snapshot must precede aware consumption time')
    result = describe(apply_read_freshness(snapshot, now=now))
    result.update(code_revision=code_revision, input_snapshot_sha256=sha256(_encoded(snapshot)).hexdigest(),
        snapshot_generated_at_utc=generated.isoformat(),
        replay_semantics='frozen_input_and_consumption_time_with_original_evidence_files')
    return result


def export(snapshot, *, now, code_revision, output):
    release = build(snapshot, now=now, code_revision=code_revision)
    raw = _encoded(release)
    directory = Path(output) / sha256(raw).hexdigest()
    directory.mkdir(parents=True, exist_ok=False)
    (directory / 'input_snapshot.json').write_bytes(_encoded(snapshot))
    (directory / 'release.json').write_bytes(raw)
    (directory / 'manifest.json').write_bytes(_encoded({
        'release_sha256': sha256(raw).hexdigest(), 'input_snapshot_sha256': release['input_snapshot_sha256'],
        'code_revision': code_revision, 'as_of_utc': release['as_of_utc']}))
    return directory


def compact(snapshot, *, now, code_revision):
    """Same frozen input/time builder for the current API and manual export."""
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    from moex_data.rub_factual_release_acceptance import projection_completeness
    from moex_data.rub_factual_package import build_package
    if now.utcoffset() is None: raise ValueError('aware consumption time required')
    now = now.astimezone(timezone.utc)
    view = apply_read_freshness(snapshot, now=now)
    value = build(view, now=now, code_revision=code_revision)
    projection_completeness(view, value, now=now)
    return build_package(view, value, now=now)


def executing_revision():
    repo = Path(__file__).resolve().parents[2]
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=repo, text=True).strip():
        raise ValueError('current release requires a clean tracked executing checkout')
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()


def export_current(*, output, now_fn=lambda: datetime.now(timezone.utc), reader=None, code_revision=None):
    from src.moex_research.consumers.usdrubf_chat_snapshot_consumer import load_factual_release
    options = {'now_fn': now_fn, 'code_revision': code_revision}
    if reader is not None: options['reader'] = reader
    package = load_factual_release(**options)
    raw = _encoded(package)
    directory = Path(output); directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    stamp = datetime.fromisoformat(package['as_of_utc']).strftime('%Y-%m-%dT%H-%M-%S.%fZ')
    path = directory / (stamp + '_' + sha256(raw).hexdigest()[:12] + '_rub_factual.json')
    # Exclusive creation also refuses an existing file or symlink.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--snapshot', type=Path)
    source.add_argument('--current', action='store_true')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--code-revision')
    parser.add_argument('--as-of')
    args = parser.parse_args(argv)
    if args.current:
        if args.as_of or args.code_revision:
            parser.error('current export captures the executing revision and current time')
        if 'MOEX_DATA_ROOT' not in os.environ:
            from dotenv import dotenv_values
            from src.moex_research.runners.usdrubf_s7_3_chat_analysis_snapshot import PROJECT_ENV_PATH
            configured_root = dotenv_values(PROJECT_ENV_PATH).get('MOEX_DATA_ROOT')
            if configured_root is not None: os.environ['MOEX_DATA_ROOT'] = configured_root
        directory = export_current(output=args.output or Path('/home/trader/moex_bot/exports/rub_snapshots'))
        print(str(directory)); return 0
    if not all((args.output, args.code_revision, args.as_of)):
        parser.error('frozen audit export requires --output, --code-revision and --as-of')
    directory = export(json.loads(args.snapshot.read_text()), now=datetime.fromisoformat(args.as_of),
        code_revision=args.code_revision, output=args.output)
    print(str(directory)); return 0


if __name__ == '__main__':
    raise SystemExit(main())
