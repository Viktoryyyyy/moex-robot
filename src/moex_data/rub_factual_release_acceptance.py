"""One read-only acceptance gate for a factual release; never a forecast grant."""
import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import tempfile
from urllib.parse import urlsplit

from moex_data import rub_factual_release as release
from moex_data.rub_macro_evidence_inventory import DENIED
from moex_data.rub_snapshot_read_freshness import apply_read_freshness

REQUIRED_FACTORS = ('rosstat_monthly_cpi', 'cbr_liquidity_verified')
AUTHORITY = ('session_completion_proven', 'historical_dataset_accepted',
             'model_validated', 'training_authorized', 'broker_execution')
ADMISSION = {
    'basis_carry': ('basis_carry', 'factual_context_usable'),
    'rosstat_monthly_cpi': ('rosstat_macro', 'monthly_cpi_factual_context_usable'),
    'cbr_liquidity_verified': ('cbr_rates', 'liquidity_factual_context_usable'),
    'rosstat_cpi': ('rosstat_macro', 'factual_context_usable'),
    'cbr_rates_verified': ('cbr_rates', 'factual_context_usable'),
    'external_cny': ('external_cny', 'factual_context_usable'),
    'oil': ('brent', 'factual_context_usable'),
}


def _require(condition, message):
    if not condition:
        raise AssertionError(message)


def _factors(value):
    return {row['factor'] for row in value['facts']}


def projection_completeness(snapshot, value, *, now):
    """Independent reverse oracle over the read-time input, not exported fact counts."""
    from math import isfinite
    view = apply_read_freshness(snapshot, now=now)
    components = view.get('components', {})
    market = components.get('synchronized_live_market_oi', {}).get('data', {})
    spot = market.get('instruments', {}).get('cnyrub_tom', {})
    price = spot.get('last')
    expected_spot = (market.get('quality', {}).get('spot_price_usable') is True
        and spot.get('spot_price_usable') is not False and spot.get('stale') is False
        and isinstance(price, (int, float)) and not isinstance(price, bool) and isfinite(price) and price > 0)
    facts = {fact['factor']: fact for fact in value['facts']}
    rows = {row['block_id']: row for row in value['matrix']}
    _require(('cnyrub_tom' in facts) == expected_spot == rows['cnyrub_tom']['usable_for_full_forecast'], 'spot completeness')
    if expected_spot: _require(facts['cnyrub_tom']['values'] == {'last': price}, 'spot values')
    basis = components.get('live_basis_carry', {})
    grouped = {}
    if basis.get('status') in {'READY', 'PARTIAL'}:
        for key, pair in (basis.get('data') or {}).get('pairs', {}).items():
            for index, metric in enumerate(pair.get('metrics', [])):
                if isinstance(metric, dict) and isinstance(metric.get('metric_id'), str) and metric['metric_id']:
                    grouped.setdefault(metric['metric_id'], []).append((key, index, metric))
    expected = {}
    for identity, copies in grouped.items():
        metric = copies[0][2]; number = metric.get('value')
        if (all(copy[2] == metric for copy in copies) and metric.get('status') == 'READY'
                and isinstance(number, (int, float)) and not isinstance(number, bool) and isfinite(number)):
            expected[identity] = metric
    _require(set(rows['basis_carry']['admitted_metric_ids']) == set(expected), 'basis matrix completeness')
    _require(('basis_carry' in facts) == bool(expected), 'basis fact completeness')
    actual = facts.get('basis_carry', {}).get('values', {}).get('metrics', [])
    _require(len(actual) == len(expected), 'basis exactly once')
    _require({entry['values']['metric_id'] for entry in actual} == set(expected), 'basis IDs')
    for entry in actual:
        metric = entry['values']; node = view
        for part in entry['snapshot_path'].split('.'):
            node = node[int(part)] if isinstance(node, list) else node[part]
        _require(metric == node == expected[metric['metric_id']], 'basis lossless values and source path')


def run(snapshot, *, now, code_revision, output, required_factors=REQUIRED_FACTORS):
    """Archive and verify one input, plus labelled in-memory failure simulations.

    No source retrieval, source-file mutation, training or model evaluation occurs.
    PASS accepts this factual contract only; the release remains INCOMPLETE.
    """
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix='factual-acceptance-', dir=output))
    before = deepcopy(snapshot)
    report = {'schema_version': 'rub_factual_release_acceptance.v1',
              'status': 'FAIL', 'code_revision': code_revision,
              'as_of_utc': now.isoformat(), 'required_factors': list(required_factors),
              'artifact_directory': str(directory.resolve()), 'checks': [],
              'failures': [], 'release_status': None, 'fact_count': 0,
              'forecast_acceptance': False, 'simulations_are_observed_outages': False}

    def check(name, operation):
        try:
            result = operation()
        except Exception as exc:
            # Do not copy arbitrary input or request error text into reports.
            report['checks'].append({'name': name, 'status': 'FAIL', 'detail': type(exc).__name__})
            report['failures'].append(name)
            return None
        report['checks'].append({'name': name, 'status': 'PASS', 'detail': 'verified'})
        return result

    def assemble():
        value = release.build(snapshot, now=now, code_revision=code_revision)
        report.update(release_status=value['status'], fact_count=len(value['facts']))
        return value

    value = check('build_with_pinned_revision_and_aware_time', assemble)
    if value is not None:
        def authority():
            _require(value['schema_version'] == release.SCHEMA and value['status'] == 'INCOMPLETE', 'release contract')
            _require(all(value[key] is False for key in AUTHORITY), 'authority')
            _require(set(value['horizons']) == {'D1', 'W1'}, 'horizons')
            for horizon in value['horizons'].values():
                _require(horizon['kind'] == 'FACTUAL_CONTEXT_NOT_FORECAST'
                         and horizon['status'] == 'INCOMPLETE'
                         and horizon['target_trading_date'] is None
                         and horizon['target_date_proven'] is False
                         and horizon['model_probability'] is None
                         and horizon['forecast_generated'] is False, 'horizon authority')
            inventory = value['macro_evidence_inventory']
            _require(all(inventory[key] is False for key in DENIED), 'macro authority')
            _require(bool(value['blocking_required_factors']), 'unresolved requirements')
        check('factual_only_D1_W1_and_authority', authority)

        def required():
            _require(bool(required_factors) and set(required_factors) <= _factors(value), 'required observed facts')
        check('required_observed_facts', required)

        def matrix():
            rows = {row['block_id']: row for row in value['matrix']}
            _require(len(rows) == len(value['matrix']), 'unique matrix blocks')
            _require(len(_factors(value)) == len(value['facts']), 'unique factors')
            for fact in value['facts']:
                block, gate = ADMISSION.get(fact['factor'], (fact['factor'], 'usable_for_full_forecast'))
                _require(rows[block][gate] is True, 'fact admission agrees with matrix')
                node = snapshot
                for part in fact['snapshot_path'].split('.'):
                    node = node[part]
                _require(isinstance(node, dict), 'source path resolves')
            expected = {row['block_id'] for row in value['matrix']
                        if row['requirement'] == 'required' and not row['usable_for_full_forecast']}
            _require(set(value['blocking_required_factors']) == expected, 'blocking factors agree')
            for event in value['macro_evidence_inventory']['scheduled_events']:
                _require(event['event_status'] == 'SCHEDULED'
                         and event['actual_event_time'] is None, 'planned event is not actual')
        check('all_facts_matrix_paths_and_scheduled_events', matrix)
        check('spot_and_basis_projection_completeness', lambda: projection_completeness(snapshot, value, now=now))

        def uncertainty():
            rows = [row for row in value['macro_evidence_inventory']['facts']
                    if row['component'] in REQUIRED_FACTORS]
            _require(len(rows) == 5, 'five core macro observations')
            for row in rows:
                item = row['uncertainty']
                _require(row['quality_status'] == 'USABLE_WITH_LIMITATIONS'
                         and item['usable_as_dated_context'] is True, 'limited context')
                _require(item['numeric_error_bounds'] is None and item['probabilities'] is None,
                         'no invented uncertainty bounds')
                _require(all(item[key] is False for key in
                             ('forecast_use_allowed', 'historical_pit_acceptance', 'action_authority')), 'uncertainty authority')
                _require(row['source_publication_time'] is None, 'unknown publication time')
            monthly = next(row for row in rows if row['component'] == 'rosstat_monthly_cpi')
            from decimal import Decimal
            for base, index in monthly['indices'].items():
                if index is not None:
                    _require(Decimal(monthly['changes_percent'][base]) == Decimal(index) - 100, 'explicit CPI base')
            for row in rows:
                if row['component'] == 'cbr_liquidity_verified':
                    _require(row['arithmetic_residual']['informational_only'] is True
                             and row['uncertainty']['revision_kind'] == 'latest_revised', 'liquidity limitation')
        check('core_macro_uncertainty_and_comparison_bases', uncertainty)

        def cached():
            if 'factual_release' in snapshot:
                _require(snapshot['factual_release'] == release.describe(snapshot), 'cached projection differs')
            current = release.describe(apply_read_freshness(snapshot, now=now))
            _require(all(value[key] == item for key, item in current.items()), 'current projection differs')
        check('cached_and_current_projection_consistency', cached)

        def archive():
            first = release.export(snapshot, now=now, code_revision=code_revision, output=directory / 'first')
            second = release.export(snapshot, now=now, code_revision=code_revision, output=directory / 'replay')
            for name in ('input_snapshot.json', 'release.json', 'manifest.json'):
                _require((first / name).read_bytes() == (second / name).read_bytes(), 'deterministic artifacts')
            manifest = json.loads((first / 'manifest.json').read_bytes())
            for name, field in (('input_snapshot.json', 'input_snapshot_sha256'), ('release.json', 'release_sha256')):
                _require(sha256((first / name).read_bytes()).hexdigest() == manifest[field], 'artifact digest')
            frozen = json.loads((first / 'input_snapshot.json').read_bytes())
            _require(release.build(frozen, now=now, code_revision=code_revision) == value, 'frozen replay')
            report['release_directory'] = str(first.resolve())
        check('frozen_export_hashes_and_exact_replay', archive)

        def expired():
            result = release.build(snapshot, now=now + timedelta(seconds=1201), code_revision=code_revision)
            _require(not set(REQUIRED_FACTORS).intersection(_factors(result)), 'expired facts excluded')
            _require(not any(row['component'] in REQUIRED_FACTORS
                             for row in result['macro_evidence_inventory']['facts']), 'expired inventory excluded')
        check('simulated_receipt_expiry', expired)

        for factor in REQUIRED_FACTORS:
            for defect in ('changed_value', 'missing_evidence', 'refresh_failed'):
                def failure(factor=factor, defect=defect):
                    changed = deepcopy(snapshot)
                    component = changed['components'][factor]
                    data = component['data']
                    if defect == 'changed_value':
                        if factor == 'rosstat_monthly_cpi':
                            data['indices']['previous_month'] = '999999.99'
                        else:
                            data['observations'][0]['value'] = '999999.99'
                    elif defect == 'missing_evidence':
                        key = 'document_manifest_path' if factor == 'rosstat_monthly_cpi' else 'manifest_path'
                        data[key] = str(directory / 'absent-evidence.json')
                    else:
                        component['refresh_error'] = 'acceptance_simulated_failure'
                    result = release.build(changed, now=now, code_revision=code_revision)
                    _require(factor not in _factors(result), 'failed source excluded')
                    _require([row for row in result['facts'] if row['factor'] != factor]
                             == [row for row in value['facts'] if row['factor'] != factor], 'unrelated facts preserved')
                    _require(result['macro_evidence_inventory']['facts'] ==
                             [row for row in value['macro_evidence_inventory']['facts'] if row['component'] != factor],
                             'only failed macro source excluded')
                check('simulated_' + factor + '_' + defect, failure)

        def missing():
            empty = {'identity': deepcopy(snapshot['identity']), 'components': {}}
            result = release.build(empty, now=now, code_revision=code_revision)
            _require(result['facts'] == [] and result['status'] == 'INCOMPLETE'
                     and bool(result['blocking_required_factors']), 'missing does not become neutral')
        check('simulated_missing_inputs_stay_incomplete', missing)
    check('input_not_mutated', lambda: _require(snapshot == before, 'input mutated'))
    report['status'] = 'FAIL' if report['failures'] else 'PASS'
    (directory / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--snapshot', type=Path)
    source.add_argument('--api-url', help='Local factual API endpoint; redirects are rejected')
    parser.add_argument('--env-file', type=Path, help='Local API token environment file (never copied to artifacts)')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--as-of', help='Aware consumption time; required for frozen input')
    args = parser.parse_args(argv)
    # Bind CLI evidence to the executing checkout, not an arbitrary user label.
    repo = Path(__file__).resolve().parents[2]
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=repo, text=True).strip():
        parser.error('acceptance CLI requires a clean committed checkout')
    if args.snapshot:
        if not args.as_of:
            parser.error('--as-of is required with --snapshot')
        snapshot = json.loads(args.snapshot.read_text(encoding='utf-8'))
        now = datetime.fromisoformat(args.as_of)
    else:
        url = urlsplit(args.api_url)
        if url.scheme != 'http' or url.hostname not in ('127.0.0.1', 'localhost', '::1') or url.username or url.password \
                or url.path != '/v1/rub/factual-snapshot' or url.query or url.fragment:
            parser.error('--api-url must be the local factual-snapshot endpoint')
        if args.as_of:
            parser.error('live acceptance uses current consumption time; --as-of is only for frozen input')
        import requests
        from src.misc.rub_factual_snapshot_http_server import load_api_token
        env = {'MOEX_ENV_FILE': str(args.env_file)} if args.env_file else None
        token = load_api_token(env)
        with requests.Session() as session:
            session.trust_env = False
            response = session.get(args.api_url, headers={'Authorization': 'Bearer ' + token},
                                   timeout=60, allow_redirects=False)
        if response.status_code != 200:
            parser.error('local factual API did not return HTTP 200')
        snapshot = response.json()
        now = datetime.now(timezone.utc)
    report = run(snapshot, now=now, code_revision=revision, output=args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
