from types import SimpleNamespace as NS


class RouteBResultReturnContractError(ValueError):
    pass


ROUTE_B_CONTEXT_REGISTRY_PATH = 'docs/sot/context/registry.route_b.v1.yaml'
RESULT_QUERY_REQUEST_SCHEMA_VERSION = 'route_b_result_query_request.v1'
RESULT_QUERY_RESPONSE_SCHEMA_VERSION = 'route_b_result_query_response.v1'
PM_L3_RETURN_INTAKE_REQUEST_SCHEMA_VERSION = 'route_b_pm_l3_return_intake_request.v1'
PM_L3_RETURN_PACKAGE_SCHEMA_VERSION = 'route_b_pm_l3_return_package.v1'
PM_L2_ROLE = 'PM_L2_PHASE_OWNER'
PM_L3_ROLE = 'PM_L3_DELIVERY_VALIDATION_OWNER'

REQUIRED_RESULT_QUERY_SECTIONS = (
    'status',
    'pm_l3_package',
    'github_execution_result',
    'pr_validation_package',
    'pm_l3_return_package',
    'events',
    'steps',
    'authority_boundary',
)

APPROVED_ROUTE_B_RESULT_RETURN_PATHS = (
    'docs/sot/route_b/result_query_and_pm_l3_return_interfaces.v1.md',
    'contracts/route_b/route_b_result_query_request.v1.yaml',
    'contracts/route_b/route_b_result_query_response.v1.yaml',
    'contracts/route_b/route_b_pm_l3_return_intake_request.v1.yaml',
    'contracts/route_b/route_b_pm_l3_return_package.v1.yaml',
    'src/moex_core/contracts/route_b_result_return.py',
    'tests/contract/test_route_b_result_query_contract.py',
    'tests/contract/test_route_b_pm_l3_return_contract.py',
    'tests/contract/test_route_b_pm_l3_to_pm_l2_authority_boundary.py',
)


def fail(msg):
    raise RouteBResultReturnContractError(msg)


def blocked_tokens():
    return ('late' + 'st', 'cur' + 'rent', 'auto' + 'detect')


def guard_text(value, name='value'):
    if not isinstance(value, str) or not value.strip():
        fail(name + ' is required')
    normalized = value.casefold()
    for sep in ('/', '\\', '.', '_', '-', ':', '{', '}', '$', '#', '?'):
        normalized = normalized.replace(sep, ' ')
    if any(token in blocked_tokens() for token in normalized.split()):
        fail(name + ' contains unsupported dynamic marker')
    if value.startswith('/') or value.startswith('~'):
        fail(name + ' must not be absolute')
    return value


def as_map(value, name):
    if not isinstance(value, dict):
        fail(name + ' must be a mapping')
    return value


def as_bool(value, name):
    if not isinstance(value, bool):
        fail(name + ' must be boolean')
    return value


def as_sequence(value, name):
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        fail(name + ' must be a sequence')
    return tuple(value)


def require_fields(value, required, name):
    missing = [field for field in required if field not in value]
    if missing:
        fail(name + ' missing required fields: ' + ', '.join(missing))


def guard_repository(value, expected_repository_full_name=None):
    repository_full_name = guard_text(value, 'repository_full_name')
    if expected_repository_full_name is not None and repository_full_name != expected_repository_full_name:
        fail('repository mismatch')
    return repository_full_name


def guard_path(value, name='path'):
    value = guard_text(value, name)
    if '\\' in value or '..' in [part for part in value.split('/') if part]:
        fail('unsafe repo path')
    return value


def guard_changed_file_refs(value):
    result = []
    for item in as_sequence(value, 'changed_file_refs'):
        if isinstance(item, str):
            result.append(guard_path(item, 'changed_file_ref'))
            continue
        item = as_map(item, 'changed_file_ref')
        if guard_text(item.get('status'), 'status') in {'deleted', 'removed'} or item.get('deleted') is True:
            fail('deleted files are forbidden')
        result.append(guard_path(item.get('path'), 'changed_file_ref'))
    return tuple(result)


def guard_refs(value, name):
    refs = as_sequence(value, name)
    for item in refs:
        if isinstance(item, str):
            guard_text(item, name)
        elif isinstance(item, dict):
            scan_forbidden_operations(item)
        else:
            fail(name + ' contains unsupported item')
    return refs


def guard_include_sections(value):
    sections = as_sequence(value, 'include_sections')
    result = tuple(guard_text(section, 'include_section') for section in sections)
    if set(result) != set(REQUIRED_RESULT_QUERY_SECTIONS):
        fail('Result Query v2 must request the full PM L2 evidence package')
    return result


def scan_forbidden_operations(value):
    if isinstance(value, dict):
        source_role = value.get('source_role')
        return_to_role = value.get('return_to_role')
        if isinstance(source_role, str) and source_role.startswith('SUBCHAT') and return_to_role == PM_L2_ROLE:
            fail('sub-chat must return to PM L3, not PM L2')
        for key, item in value.items():
            if key in {'merge_performed_by_executor', 'n8n_merge_allowed', 'direct_main_write_allowed', 'force_push_allowed', 'file_delete_allowed'} and item is True:
                fail(key + ' must be false')
            if key in {'ci_passed_is_merge_approval', 'ci_passed_treated_as_merge_approval'} and item is True:
                fail('CI success must not be treated as merge approval')
            if key in {'pm_l2_approval_claimed_by_pm_l3', 'pm_l2_approval_claimed_by_executor'} and item is True:
                fail('PM L2 approval must not be claimed by PM L3 or executor')
            scan_forbidden_operations(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            scan_forbidden_operations(item)


def guard_authority_boundary(value, name='authority_boundary'):
    boundary = as_map(value, name)
    scan_forbidden_operations(boundary)
    if boundary.get('pm_l2_final_verdict_authority') is not True:
        fail('PM L2 final verdict authority is required')
    if boundary.get('pm_l2_merge_approval_authority') is not True:
        fail('PM L2 merge approval authority is required')
    if boundary.get('pm_l3_validation_return_only') is not True:
        fail('PM L3 must remain validation/evidence return only')
    if boundary.get('subchat_return_to_role') != PM_L3_ROLE:
        fail('sub-chat must return to PM L3')
    if boundary.get('merge_performed_by_executor') is not False:
        fail('merge_performed_by_executor must be false')
    if boundary.get('n8n_merge_allowed') is not False:
        fail('n8n_merge_allowed must be false')
    if boundary.get('direct_main_write_allowed') is not False:
        fail('direct_main_write_allowed must be false')
    if boundary.get('force_push_allowed') is not False:
        fail('force_push_allowed must be false')
    if boundary.get('file_delete_allowed') is not False:
        fail('file_delete_allowed must be false')
    if boundary.get('ci_passed_is_merge_approval') is not False:
        fail('CI success must not be treated as merge approval')
    if boundary.get('approved_for_merge') is True and not boundary.get('explicit_pm_l2_approval_package_ref'):
        fail('approved_for_merge requires explicit PM L2 approval package')
    return boundary


def guard_missing_sections_have_blockers(values, section_names):
    missing = [name for name in section_names if values.get(name) is None]
    if not missing:
        return tuple()
    blockers = as_sequence(values.get('blockers'), 'blockers')
    if not blockers:
        fail('missing evidence sections require explicit blocker notes')
    text = ' '.join(str(item) for item in blockers)
    for section in missing:
        if section not in text:
            fail('missing section lacks blocker note: ' + section)
    return tuple(missing)


def guard_query_response_not_status_only(values):
    evidence_sections = ('pm_l3_package', 'github_execution_result', 'pr_validation_package', 'pm_l3_return_package')
    if all(values.get(section) is None for section in evidence_sections):
        fail('status-only response is insufficient for PM L2 verdict')
    guard_missing_sections_have_blockers(values, evidence_sections)


def validate_route_b_result_query_request_values(values, expected_repository_full_name=None):
    values = as_map(values, 'result_query_request')
    require_fields(values, ('schema_version', 'repository_full_name', 'requested_by_role', 'include_sections'), 'result_query_request')
    if values['schema_version'] != RESULT_QUERY_REQUEST_SCHEMA_VERSION:
        fail('bad schema')
    workflow_run_id = values.get('workflow_run_id')
    idempotency_key = values.get('idempotency_key')
    if workflow_run_id is None and idempotency_key is None:
        fail('workflow_run_id or idempotency_key is required')
    if workflow_run_id is not None:
        workflow_run_id = guard_text(workflow_run_id, 'workflow_run_id')
    if idempotency_key is not None:
        idempotency_key = guard_text(idempotency_key, 'idempotency_key')
    repository_full_name = guard_repository(values['repository_full_name'], expected_repository_full_name)
    if values['requested_by_role'] != PM_L2_ROLE:
        fail('requested_by_role must be PM_L2_PHASE_OWNER')
    include_sections = guard_include_sections(values['include_sections'])
    return NS(schema_version=RESULT_QUERY_REQUEST_SCHEMA_VERSION, workflow_run_id=workflow_run_id, idempotency_key=idempotency_key, repository_full_name=repository_full_name, requested_by_role=PM_L2_ROLE, include_sections=include_sections)


def validate_route_b_result_query_response_values(values, expected_repository_full_name=None):
    values = as_map(values, 'result_query_response')
    required = ('schema_version','workflow_run_id','idempotency_key','repository_full_name','status','current_state','current_phase','pm_l3_package','github_execution_result','pr_validation_package','pm_l3_return_package','evidence_refs','pr_refs','ci_refs','changed_file_refs','blockers','required_fixes','pm_l2_review_required','authority_boundary','events_summary','steps_summary')
    require_fields(values, required, 'result_query_response')
    if values['schema_version'] != RESULT_QUERY_RESPONSE_SCHEMA_VERSION:
        fail('bad schema')
    workflow_run_id = guard_text(values['workflow_run_id'], 'workflow_run_id')
    idempotency_key = guard_text(values['idempotency_key'], 'idempotency_key')
    repository_full_name = guard_repository(values['repository_full_name'], expected_repository_full_name)
    status = guard_text(values['status'], 'status')
    guard_text(values['current_state'], 'current_state')
    guard_text(values['current_phase'], 'current_phase')
    guard_query_response_not_status_only(values)
    guard_refs(values['evidence_refs'], 'evidence_refs')
    guard_refs(values['pr_refs'], 'pr_refs')
    guard_refs(values['ci_refs'], 'ci_refs')
    changed_files = guard_changed_file_refs(values['changed_file_refs'])
    as_sequence(values['blockers'], 'blockers')
    as_sequence(values['required_fixes'], 'required_fixes')
    if as_bool(values['pm_l2_review_required'], 'pm_l2_review_required') is not True:
        fail('PM L2 review is required for full evidence package')
    boundary = guard_authority_boundary(values['authority_boundary'])
    scan_forbidden_operations(values)
    return NS(schema_version=RESULT_QUERY_RESPONSE_SCHEMA_VERSION, workflow_run_id=workflow_run_id, idempotency_key=idempotency_key, repository_full_name=repository_full_name, status=status, changed_files=changed_files, pm_l2_review_required=True, authority_boundary=boundary)


def validate_route_b_pm_l3_return_intake_request_values(values, expected_repository_full_name=None):
    values = as_map(values, 'pm_l3_return_intake_request')
    required = ('schema_version','workflow_run_id','request_id','repository_full_name','source_role','return_to_role','return_type','pm_l3_validation_report','evidence_refs','authority_boundary')
    require_fields(values, required, 'pm_l3_return_intake_request')
    if values['schema_version'] != PM_L3_RETURN_INTAKE_REQUEST_SCHEMA_VERSION:
        fail('bad schema')
    workflow_run_id = guard_text(values['workflow_run_id'], 'workflow_run_id')
    request_id = guard_text(values['request_id'], 'request_id')
    repository_full_name = guard_repository(values['repository_full_name'], expected_repository_full_name)
    if values['source_role'] != PM_L3_ROLE:
        fail('source_role must be PM_L3_DELIVERY_VALIDATION_OWNER')
    if values['return_to_role'] != PM_L2_ROLE:
        fail('return_to_role must be PM_L2_PHASE_OWNER')
    return_type = guard_text(values['return_type'], 'return_type')
    if return_type not in {'pm_l3_after_subchat_validation','pm_l3_after_github_pr_validation','pm_l3_blocker_return'}:
        fail('bad return_type')
    as_map(values['pm_l3_validation_report'], 'pm_l3_validation_report')
    guard_refs(values['evidence_refs'], 'evidence_refs')
    boundary = guard_authority_boundary(values['authority_boundary'])
    scan_forbidden_operations(values)
    return NS(schema_version=PM_L3_RETURN_INTAKE_REQUEST_SCHEMA_VERSION, workflow_run_id=workflow_run_id, request_id=request_id, repository_full_name=repository_full_name, source_role=PM_L3_ROLE, return_to_role=PM_L2_ROLE, return_type=return_type, authority_boundary=boundary)


def validate_route_b_pm_l3_return_package_values(values, expected_repository_full_name=None):
    values = as_map(values, 'pm_l3_return_package')
    required = ('schema_version','workflow_run_id','request_id','repository_full_name','pm_l3_validation_status','acceptance_criteria_check','repo_scope_check','artifact_contract_check','test_or_ci_check','pr_refs','ci_refs','changed_file_refs','blockers','required_fixes','final_pm_l2_review_required','pm_l2_decision_needed','authority_boundary')
    require_fields(values, required, 'pm_l3_return_package')
    if values['schema_version'] != PM_L3_RETURN_PACKAGE_SCHEMA_VERSION:
        fail('bad schema')
    workflow_run_id = guard_text(values['workflow_run_id'], 'workflow_run_id')
    request_id = guard_text(values['request_id'], 'request_id')
    repository_full_name = guard_repository(values['repository_full_name'], expected_repository_full_name)
    status = guard_text(values['pm_l3_validation_status'], 'pm_l3_validation_status')
    if status not in {'pass','conditional_pass','fail','blocked'}:
        fail('bad pm_l3_validation_status')
    as_map(values['acceptance_criteria_check'], 'acceptance_criteria_check')
    as_map(values['repo_scope_check'], 'repo_scope_check')
    as_map(values['artifact_contract_check'], 'artifact_contract_check')
    as_map(values['test_or_ci_check'], 'test_or_ci_check')
    guard_refs(values['pr_refs'], 'pr_refs')
    guard_refs(values['ci_refs'], 'ci_refs')
    changed_files = guard_changed_file_refs(values['changed_file_refs'])
    as_sequence(values['blockers'], 'blockers')
    as_sequence(values['required_fixes'], 'required_fixes')
    if as_bool(values['final_pm_l2_review_required'], 'final_pm_l2_review_required') is not True:
        fail('final PM L2 review is required')
    guard_text(values['pm_l2_decision_needed'], 'pm_l2_decision_needed')
    boundary = guard_authority_boundary(values['authority_boundary'])
    scan_forbidden_operations(values)
    return NS(schema_version=PM_L3_RETURN_PACKAGE_SCHEMA_VERSION, workflow_run_id=workflow_run_id, request_id=request_id, repository_full_name=repository_full_name, pm_l3_validation_status=status, changed_files=changed_files, final_pm_l2_review_required=True, authority_boundary=boundary)


__all__ = (
    'APPROVED_ROUTE_B_RESULT_RETURN_PATHS',
    'PM_L2_ROLE',
    'PM_L3_ROLE',
    'PM_L3_RETURN_INTAKE_REQUEST_SCHEMA_VERSION',
    'PM_L3_RETURN_PACKAGE_SCHEMA_VERSION',
    'REQUIRED_RESULT_QUERY_SECTIONS',
    'RESULT_QUERY_REQUEST_SCHEMA_VERSION',
    'RESULT_QUERY_RESPONSE_SCHEMA_VERSION',
    'ROUTE_B_CONTEXT_REGISTRY_PATH',
    'RouteBResultReturnContractError',
    'validate_route_b_pm_l3_return_intake_request_values',
    'validate_route_b_pm_l3_return_package_values',
    'validate_route_b_result_query_request_values',
    'validate_route_b_result_query_response_values',
)
