"""Reproducible D1/W1 factual inventory, with no model or session-completion claim."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re

SCHEMA = 'rub_factual_release.v1'


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def describe(snapshot):
    from moex_data.rub_production_source_matrix import build as matrix_build
    snapshot = dict(snapshot)
    if not isinstance(snapshot.get('components'), dict):
        snapshot['components'] = {}
    matrix = matrix_build(snapshot)
    components = snapshot['components']
    facts = []
    market = (components.get('synchronized_live_market_oi', {}).get('data') or {}).get('instruments', {})
    for key, item in market.items():
        if item.get('price_oi_usable') is True or (key == 'cnyrub_tom' and item.get('spot_price_usable') is True):
            facts.append({'factor': key, 'scope': 'current_source_row',
                'snapshot_path': f'components.synchronized_live_market_oi.data.instruments.{key}',
                'source_identity': {k: item.get(k) for k in ('secid', 'timestamp', 'source_trade_date')},
                'values': {k: item[k] for k in ('last', 'oi') if k in item}})
    for key in ('oil', 'external_cny', 'rosstat_cpi', 'cbr_rates_verified', 'futoi_live', 'futoi_live_cr'):
        component = components.get(key, {})
        data = component.get('data') or {}
        if component.get('status') != 'READY' or data.get('consumer_factual_use_allowed') is not True:
            continue
        if key.startswith('futoi'):
            fact = data.get('current_intraday', {}).get('factual')
            scope = 'current_pair_only_no_previous_or_history_grant'
        else:
            fact = {k: data[k] for k in ('secid', 'series_id', 'units', 'value', 'price', 'price_unit', 'ohlc',
                'observations', 'document_format', 'next_scheduled_release', 'weekly_release_calendar_accepted', 'observation_start', 'observation_end', 'indices', 'weekly_change_percent', 'listed_publication_date', 'index_manifest_sha256', 'document_manifest_sha256', 'source_trade_date', 'observation_date', 'received_at', 'source_url', 'manifest_sha256', 'raw_sha256', 'provenance') if k in data}
            scope = 'latest_published_dated_reference'
        facts.append({'factor': key, 'scope': scope, 'snapshot_path': 'components.' + key + '.data', 'values': fact})
    horizons = {}
    from moex_data.rub_trading_target_plan import describe as describe_targets
    try:
        reference = (snapshot['live_read_freshness']['read_at_utc']
            if 'live_read_freshness' in snapshot else snapshot['identity']['generated_at_utc'])
        target_now = datetime.fromisoformat(reference)
    except (ValueError, TypeError, KeyError):
        target_now = None
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
    return {'schema_version': SCHEMA, 'as_of_utc': snapshot.get('live_read_freshness', {}).get('read_at_utc'),
        'status': 'INCOMPLETE', 'facts': facts, 'horizons': horizons,
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--code-revision', required=True)
    parser.add_argument('--as-of', required=True)
    args = parser.parse_args()
    directory = export(json.loads(args.snapshot.read_text()), now=datetime.fromisoformat(args.as_of),
        code_revision=args.code_revision, output=args.output)
    print(str(directory))
