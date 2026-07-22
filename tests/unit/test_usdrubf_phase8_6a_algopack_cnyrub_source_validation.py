from __future__ import annotations
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import pytest
from moex_research.external_data.moex_cnyrub_algopack_history import CnyrubAlgoPackDailyCandle, CnyrubAlgoPackError, CnyrubSecurityIdentity
from moex_research.runners import usdrubf_phase8_6a_algopack_cnyrub_source_validation as runner
RETRIEVED = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
DIGEST = 'a' * 64
MOSCOW = timezone(timedelta(hours=3))

def _dates() -> list[pd.Timestamp]:
    all_dates = list(pd.bdate_range('2024-08-05', '2026-06-11'))
    return [*all_dates[:471], all_dates[-1]]

def _eligible() -> pd.DataFrame:
    targets = _dates()
    return pd.DataFrame({'target_trade_date': [item.strftime('%Y-%m-%d') for item in targets], 'target_instrument_id': ['forts.usdrubf'] * 472, 'prior_trade_date': [(item - pd.Timedelta(days=1)).strftime('%Y-%m-%d') for item in targets]})

def _validation(eligible: pd.DataFrame) -> pd.DataFrame:
    return eligible.loc[:319, list(runner.IDENTITY_COLUMNS)].reset_index(drop=True)

def _identity() -> CnyrubSecurityIdentity:
    return CnyrubSecurityIdentity(source_id='moex_cnyrub_tom_daily', security_id='CNYRUB_TOM', board_id='CETS', engine='currency', market='selt', primary_board=True, active_board=True, history_from=date(2010, 9, 27), history_till=None, metadata_route=runner.build_metadata_route(), retrieved_at_utc=RETRIEVED, raw_payload_sha256=DIGEST, source_revision_status='official_iss_current_revision', historical_model_use_status='source_validation_only')

def _candle(trade_date: date, *, source_available_at: datetime | None=None) -> CnyrubAlgoPackDailyCandle:
    begin = datetime.combine(trade_date, datetime.min.time(), tzinfo=MOSCOW).replace(hour=10)
    end = begin.replace(hour=23, minute=55)
    available = source_available_at or end.replace(hour=23, minute=56)
    return CnyrubAlgoPackDailyCandle(source_id=runner.SOURCE_ID, security_id='CNYRUB_TOM', board_id='CETS', engine='currency', market='selt', trade_date=trade_date, open=11.0, high=12.0, low=10.0, close=11.5, volume=100.0, volume_buy=60.0, volume_sell=40.0, volume_imbalance=0.2, value=1000.0, value_buy=600.0, value_sell=400.0, trades=20, trades_buy=12, trades_sell=8, candle_begin=begin, candle_end=end, source_available_at=available, source_route='https://apim.moex.com/iss/datashop/algopack/fx/tradestats/CNYRUB_TOM.json?from=2024-01-01&till=2026-01-01&start=0', retrieved_at_utc=RETRIEVED, raw_payload_sha256=DIGEST, source_revision_status=runner.SOURCE_REVISION_STATUS, historical_model_use_status='source_validation_only')

def _candles(eligible: pd.DataFrame) -> list[CnyrubAlgoPackDailyCandle]:
    return [_candle(date.fromisoformat(item)) for item in eligible['prior_trade_date']]

def _route_validation() -> dict[str, object]:
    return {'official_host': runner.ALGOPACK_HOST, 'tradestats_route': runner.ALGOPACK_TRADESTATS_ROUTE, 'bucket_interval_minutes': runner.ALGOPACK_BUCKET_MINUTES, 'pagination_complete': True, 'schema_stable_within_run': True, 'directional_volume_fields_present': True, 'source_availability_present': True, 'redirects_allowed': False}

def test_exact_directional_acceptance_matrix_schema_includes_availability() -> None:
    eligible = _eligible().iloc[:1].reset_index(drop=True)
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(eligible, _candles(eligible))
    assert tuple(matrix.columns) == runner.ACCEPTANCE_MATRIX_COLUMNS
    assert matrix.loc[0, 'cnyrub_volume_buy'] == 60
    assert matrix.loc[0, 'cnyrub_source_available_at'].endswith('+03:00')
    assert diagnostics.loc[0, 'accepted']
    assert not diagnostics.loc[0, 'arbitrary_date_selection_used']
    assert not set(matrix.columns) & runner._FORBIDDEN_MATRIX_FIELDS

def test_exact_472_and_320_directional_coverage_passes() -> None:
    eligible = _eligible()
    validation = _validation(eligible)
    candle_list = _candles(eligible)
    candles = runner.normalized_candles(candle_list)
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(eligible, candle_list)
    coverage = runner._coverage(matrix, validation)
    gates = runner.evaluate_gates(immutable_inputs_verified=True, phase83_verified=True, eligible=eligible, validation=validation, identity=_identity(), candles=candles, matrix=matrix, coverage=coverage, diagnostics=diagnostics, route_validation=_route_validation())
    assert coverage.iloc[0].eligible_covered_count == 472
    assert coverage.iloc[0].validation_covered_count == 320
    assert gates['G3_algopack_tradestats_route_and_schema']['passed']
    assert gates['G4_point_in_time_session_correctness']['passed']
    assert gates['G6_directional_volume_and_numerical_integrity'] == {'passed': True, 'volume_equals_buy_plus_sell': True, 'value_equals_buy_plus_sell': True, 'trades_equal_buy_plus_sell': True}
    assert gates['G9_final_source_readiness'] == {'passed': True, 'requires': [f'G{index}' for index in range(1, 9)], 'failed_gates': [], 'status': 'moex_algopack_cnyrub_source_candidate_for_phase8_6b', 'historical_model_use_status': 'source_validation_only', 'blocker_classification': None}

@pytest.mark.parametrize('column,value', [('volume_sell', 41.0), ('value_buy', 603.0), ('trades_sell', 9), ('volume_imbalance', 1.1)])
def test_directional_integrity_failure(column: str, value: object) -> None:
    eligible = _eligible()
    validation = _validation(eligible)
    candle_list = _candles(eligible)
    candles = runner.normalized_candles(candle_list)
    candles.loc[0, column] = value
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(eligible, candle_list)
    gates = runner.evaluate_gates(immutable_inputs_verified=True, phase83_verified=True, eligible=eligible, validation=validation, identity=_identity(), candles=candles, matrix=matrix, coverage=runner._coverage(matrix, validation), diagnostics=diagnostics, route_validation=_route_validation())
    assert not gates['G6_directional_volume_and_numerical_integrity']['passed']
    assert gates['G9_final_source_readiness']['blocker_classification'] == 'numerical_or_chronology_integrity_failure'

@pytest.mark.parametrize('blocker', ['token_env_not_configured', 'algopack_authentication_failed', 'algopack_subscription_not_entitled', 'official_route_not_reproducible', 'cnyrub_tom_not_available', 'algopack_rate_limit_blocked', 'algopack_tradestats_not_available', 'algopack_schema_not_stable'])
def test_http_source_failures_keep_exact_blocker(blocker: str) -> None:
    eligible = _eligible()
    validation = _validation(eligible)
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(eligible, [])
    error = CnyrubAlgoPackError('sanitized exact reason', blocker=blocker)
    routes = runner._official_route_validation(_identity(), runner.normalized_candles([]), error)
    gates = runner.evaluate_gates(immutable_inputs_verified=True, phase83_verified=True, eligible=eligible, validation=validation, identity=_identity(), candles=runner.normalized_candles([]), matrix=matrix, coverage=runner._coverage(matrix, validation), diagnostics=diagnostics, route_validation=routes, source_error=error)
    assert not gates['G9_final_source_readiness']['passed']
    assert gates['G9_final_source_readiness']['blocker_classification'] == blocker

def test_late_provider_availability_is_excluded_and_g4_fails() -> None:
    eligible = _eligible().iloc[:1].reset_index(drop=True)
    target = date.fromisoformat(eligible.loc[0, 'target_trade_date'])
    prior = date.fromisoformat(eligible.loc[0, 'prior_trade_date'])
    late = _candle(prior, source_available_at=datetime.combine(target, datetime.min.time(), tzinfo=MOSCOW).replace(hour=6))
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(eligible, [late])
    assert matrix['cnyrub_trade_date'].isna().all()
    assert diagnostics.loc[0, 'blocker_classification'] == 'point_in_time_cutoff_not_provable'

def test_source_systime_error_forces_g4_and_g9_false() -> None:
    eligible = _eligible()
    validation = _validation(eligible)
    matrix, diagnostics = runner.build_cnyrub_pit_acceptance_matrix(eligible, [])
    error = CnyrubAlgoPackError('AlgoPack provider availability timestamp SYSTIME is malformed', blocker='point_in_time_cutoff_not_provable')
    routes = runner._official_route_validation(_identity(), runner.normalized_candles([]), error)
    gates = runner.evaluate_gates(immutable_inputs_verified=True, phase83_verified=True, eligible=eligible, validation=validation, identity=_identity(), candles=runner.normalized_candles([]), matrix=matrix, coverage=runner._coverage(matrix, validation), diagnostics=diagnostics, route_validation=routes, source_error=error)
    assert not gates['G4_point_in_time_session_correctness']['passed']
    assert not gates['G9_final_source_readiness']['passed']
    assert gates['G9_final_source_readiness']['status'] == 'moex_algopack_cnyrub_source_not_ready'
    assert gates['G9_final_source_readiness']['blocker_classification'] == 'point_in_time_cutoff_not_provable'

def test_run_source_validation_writes_exact_nine_artifacts_on_systime_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    eligible = _eligible()
    validation = _validation(eligible)
    captured: dict[str, object] = {}
    monkeypatch.setattr(runner, '_validate_request', lambda _request: None)
    monkeypatch.setattr(runner.base, 'verify_immutable_inputs', lambda _request: dict(runner.EXPECTED_INPUT_SHA256))
    monkeypatch.setattr(runner.base, '_json', lambda _path: {})
    monkeypatch.setattr(runner.base, '_validate_phase83_evidence', lambda _a, _b: None)
    monkeypatch.setattr(runner, '_validate_experiment_contract', lambda _value: None)
    monkeypatch.setattr(runner.pd, 'read_parquet', lambda path: path)
    monkeypatch.setattr(runner.base, '_eligible_identities', lambda _value: eligible)
    monkeypatch.setattr(runner.base, '_validation_identities', lambda _value, _eligible_value: validation)

    def writer(output_dir: Path, payloads: dict[str, object]) -> None:
        captured['output_dir'] = output_dir
        captured['payloads'] = payloads
    monkeypatch.setattr(runner.base, '_write_exact_artifacts', writer)

    def history_loader(*_args: object, **_kwargs: object) -> list[object]:
        raise CnyrubAlgoPackError('AlgoPack provider availability timestamp SYSTIME is malformed', blocker='point_in_time_cutoff_not_provable')
    request = SimpleNamespace(modeling_dataset_path='model', dataset_manifest_path='manifest', feature_schema_path='schema', m0_validation_predictions_path='predictions', phase83_aggregate_metrics_path='aggregate', phase83_gate_results_path='gates', experiment_contract_path='contract', output_dir=tmp_path / 'out', run_id='phase8_6a_algopack_cnyrub_source_validation_20260717_v2', git_commit_sha='a' * 40)
    result = runner.run_source_validation(request, token_loader=lambda: pytest.fail('token loader must not run in mocked history'), identity_loader=lambda **_kwargs: _identity(), history_loader=history_loader)
    payloads = captured['payloads']
    assert tuple(payloads) == runner.DECLARED_OUTPUT_ARTIFACTS
    assert len(payloads) == 9
    final = payloads['gate_results.json']['G9_final_source_readiness']
    assert final['status'] == 'moex_algopack_cnyrub_source_not_ready'
    assert final['blocker_classification'] == 'point_in_time_cutoff_not_provable'
    assert result.final_status == 'moex_algopack_cnyrub_source_not_ready'

def test_contract_matches_runner() -> None:
    contract_path = Path(__file__).parents[2] / 'contracts/experiments/usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2.json'
    contract = json.loads(contract_path.read_text(encoding='utf-8'))
    runner._validate_experiment_contract(contract)
