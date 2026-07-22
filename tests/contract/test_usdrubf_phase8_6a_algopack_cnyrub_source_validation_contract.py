from __future__ import annotations
import json
from pathlib import Path
ROOT = Path(__file__).parents[2]
CONTRACT_PATH = ROOT / 'contracts/experiments/usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2.json'
CONTRACT = json.loads(CONTRACT_PATH.read_text(encoding='utf-8'))
EXPECTED_SCOPE = ['contracts/experiments/usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2.json', 'src/moex_research/external_data/moex_cnyrub_algopack_history.py', 'src/moex_research/runners/usdrubf_phase8_6a_algopack_cnyrub_source_validation.py', 'tests/unit/test_usdrubf_phase8_6a_algopack_cnyrub_history.py', 'tests/unit/test_usdrubf_phase8_6a_algopack_cnyrub_source_validation.py', 'tests/contract/test_usdrubf_phase8_6a_algopack_cnyrub_source_validation_contract.py']
EXPECTED_BLOCKERS = ['security_identity_not_reproducible', 'token_env_not_configured', 'algopack_authentication_failed', 'algopack_subscription_not_entitled', 'official_route_not_reproducible', 'cnyrub_tom_not_available', 'algopack_rate_limit_blocked', 'algopack_tradestats_not_available', 'algopack_schema_not_stable', 'official_schema_not_stable', 'point_in_time_cutoff_not_provable', 'incomplete_identity_coverage', 'numerical_or_chronology_integrity_failure', 'provenance_not_sufficient', 'other_fail_closed_with_exact_reason']

def test_contract_identity_and_exact_create_only_scope() -> None:
    assert CONTRACT['contract_identity'] == {'contract_id': 'usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2', 'contract_version': '2.0', 'project': 'MOEX Bot', 'task_id': 'ema_3_19_ai_phase_8_6a_algopack_cnyrub_source_validation_v2', 'lane': 'ema_3_19_ai', 'phase': '8.6A', 'execution_mode': 'browser_chatgpt_github_direct', 'status': 'source_validation_only'}
    assert CONTRACT['approved_branch'] == 'research/ema-3-19-ai/phase-8-6a-algopack-cnyrub-source-validation-v2'
    scope = CONTRACT['approved_file_scope']
    assert scope['create_only'] == EXPECTED_SCOPE
    assert scope['existing_files_to_modify'] == []
    assert scope['exact_file_count'] == 6
    assert scope['maximum_changed_file_count'] == 6
    assert scope['scope_widening_allowed'] is False

def test_exact_subscribed_algopack_source_and_no_fallback() -> None:
    source = CONTRACT['source_identity']
    assert source['source_id'] == 'moex_algopack_cnyrub_tom_tradestats_5m'
    assert source['official_service'] == 'MOEX AlgoPack subscription'
    assert source['tradestats_route'] == 'https://apim.moex.com/iss/datashop/algopack/fx/tradestats/CNYRUB_TOM.json'
    assert (source['security_id'], source['board_id'], source['engine'], source['market'], source['bucket_interval_minutes']) == ('CNYRUB_TOM', 'CETS', 'currency', 'selt', 5)
    policy = CONTRACT['official_source_policy']
    assert policy['exact_scheme_host_port_path_query_required'] is True
    assert policy['redirects_allowed'] is False
    assert policy['third_party_market_data_allowed'] is False
    assert policy['substitute_security_or_board_allowed'] is False

def test_bearer_redirect_and_token_leak_policy_is_explicit() -> None:
    auth = CONTRACT['authorization_policy']
    assert auth['required_environment_variable'] == 'MOEX_ALGOPACK_TOKEN'
    assert auth['exact_route_validation_before_request'] is True
    assert auth['redirects_allowed'] is False
    assert auth['cross_host_redirect_allowed'] is False
    assert auth['authorization_on_redirect_allowed'] is False
    assert auth['token_in_cli_allowed'] is False
    assert auth['token_in_url_allowed'] is False
    assert auth['token_in_logs_allowed'] is False
    assert auth['token_in_artifacts_allowed'] is False
    assert auth['token_in_repository_allowed'] is False
    assert auth['token_in_exception_text_allowed'] is False

def test_exact_http_classification_and_retry_policy() -> None:
    outcomes = CONTRACT['http_outcome_policy']
    assert outcomes == {'missing_or_empty_token': 'token_env_not_configured', 'HTTP_401': 'algopack_authentication_failed', 'HTTP_403': 'algopack_subscription_not_entitled', 'HTTP_404_route': 'official_route_not_reproducible', 'HTTP_404_ticker': 'cnyrub_tom_not_available', 'HTTP_429': 'algopack_rate_limit_blocked', 'HTTP_5XX': 'algopack_tradestats_not_available', 'transport_timeout': 'algopack_tradestats_not_available', 'malformed_json_or_schema': 'algopack_schema_not_stable', 'response_body_in_blocker_reason_allowed': False}
    retry = CONTRACT['transient_http_retry_policy']
    assert retry['retryable_outcomes'] == ['HTTP_429', 'HTTP_5XX', 'transport_timeout']
    assert retry['non_retryable_outcomes'] == ['HTTP_401', 'HTTP_403', 'HTTP_404', 'schema_failure']
    assert retry['maximum_retry_after_seconds'] == 60.0
    assert retry['redirects_allowed'] is False
    assert retry['same_exact_official_route_only'] is True

def test_systime_pit_and_five_minute_grid_contract() -> None:
    assert 'SYSTIME' in CONTRACT['raw_tradestats_required_fields']
    aggregation = CONTRACT['aggregation_policy']
    assert aggregation['daily_source_available_at'] == 'maximum valid SYSTIME across included rows'
    required = set(CONTRACT['normalized_source_required_fields'])
    assert 'source_available_at' in required
    matrix = set(CONTRACT['acceptance_matrix_fields'])
    assert 'cnyrub_source_available_at' in matrix
    pit = CONTRACT['point_in_time_policy']
    assert pit['provider_availability_field'] == 'SYSTIME'
    assert pit['source_available_at_before_anchor_required'] is True
    assert pit['all_source_rows_completed_required'] is True
    assert pit['five_minute_grid_required'] is True
    assert pit['grid_seconds_required'] == 0
    assert pit['grid_minute_modulus'] == 5
    assert pit['unique_provider_timestamps_required'] is True
    assert pit['chronological_provider_timestamps_required'] is True
    assert pit['forward_fill_allowed'] is False
    assert pit['backward_fill_allowed'] is False
    assert pit['arbitrary_last_available_date_allowed'] is False

def test_pagination_exactness_contract() -> None:
    policy = CONTRACT['pagination_policy']
    assert all((policy[key] is True for key in ('cursor_index_must_equal_requested_start', 'cursor_total_must_be_constant', 'returned_rows_must_not_exceed_remaining_total', 'final_accumulated_count_must_equal_total')))
    assert all((policy[key] is False for key in ('start_greater_than_total_allowed', 'overlapping_pages_allowed', 'skipped_pages_allowed', 'duplicate_provider_row_identity_allowed', 'premature_empty_page_allowed')))

def test_blocker_list_is_exact() -> None:
    assert CONTRACT['blocker_classifications'] == EXPECTED_BLOCKERS

def test_exact_nine_artifacts_and_authority_boundary() -> None:
    assert len(CONTRACT['runtime_artifacts']) == 9
    assert CONTRACT['artifact_policy']['exact_count'] == 9
    assert CONTRACT['artifact_policy']['model_file_allowed'] is False
    authority = CONTRACT['authority_boundary']
    assert authority['direct_main_write_allowed'] is False
    assert authority['merge_allowed'] is False
    assert authority['server_apply_allowed'] is False
    assert authority['controlled_runtime_allowed'] is False
    assert authority['broker_action_allowed'] is False
    assert authority['trading_allowed'] is False
