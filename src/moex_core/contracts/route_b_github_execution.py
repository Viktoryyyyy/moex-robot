from types import SimpleNamespace as NS

class RouteBGithubExecutionContractError(ValueError):
    pass

APPROVED_ROUTE_B_EXECUTOR_PATHS = (
    'docs/sot/route_b/github_branch_pr_executor.v1.md',
    'contracts/route_b/route_b_github_execution_request.v1.yaml',
    'contracts/route_b/route_b_github_execution_result.v1.yaml',
    'contracts/route_b/route_b_pr_validation_package.v1.yaml',
    'src/moex_core/contracts/route_b_github_execution.py',
    'tests/contract/test_route_b_github_execution_contracts.py',
    'tests/contract/test_route_b_github_executor_authority_boundaries.py',
    'tests/contract/test_route_b_github_registry_integration.py',
    'tests/contract/test_route_b_pr_validation_package_contract.py',
)
ROUTE_B_CONTEXT_REGISTRY_PATH = 'docs/sot/context/registry.route_b.v1.yaml'
REQUEST_SCHEMA_VERSION = 'route_b_github_execution_request.v1'
RESULT_SCHEMA_VERSION = 'route_b_github_execution_result.v1'
VALIDATION_PACKAGE_SCHEMA_VERSION = 'route_b_pr_validation_package.v1'


def fail(msg):
    raise RouteBGithubExecutionContractError(msg)


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


def walk(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key) == 'target_role_context_ref':
                fail('target_role_context_ref is forbidden')
            guard_text(str(key), 'key')
            walk(item)
    elif isinstance(value, str):
        guard_text(value)
    elif isinstance(value, (list, tuple)):
        for item in value:
            walk(item)
    elif value is None or isinstance(value, (bool, int, float)):
        return
    else:
        fail('unsupported value')


def as_map(value, name):
    if not isinstance(value, dict):
        fail(name + ' must be a mapping')
    walk(value)
    return value


def as_bool(value, name):
    if not isinstance(value, bool):
        fail(name + ' must be boolean')
    return value


def fields(value, required):
    if set(value) != set(required):
        fail('package fields do not match contract')


def path(value):
    value = guard_text(value, 'path')
    if '\\' in value or '..' in [part for part in value.split('/') if part]:
        fail('unsafe repo path')
    return value


def approved_paths(value):
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        fail('paths must be a sequence')
    result = tuple(path(item) for item in value)
    if set(result) != set(APPROVED_ROUTE_B_EXECUTOR_PATHS):
        fail('changed files are outside the approved Route B executor scope')
    return result


def branch(value):
    value = guard_text(value, 'branch')
    if value.startswith('route-b/'):
        fail('route-b branch prefix is forbidden')
    if not value.startswith('n8n/'):
        fail('branch prefix must be n8n/')
    return value


def role_ref(value):
    ref = as_map(value, 'role_ref')
    if 'role_context_ref' not in ref:
        fail('role_context_ref is required')
    role = as_map(ref['role_context_ref'], 'role_context_ref')
    guard_text(role.get('role_id'), 'role_id')
    guard_text(role.get('role_context_version'), 'role_context_version')


def registry_ref(value):
    ref = as_map(value, 'registry_ref')
    ref_path = path(ref.get('path') or ref.get('registry_path'))
    if ref_path != ROUTE_B_CONTEXT_REGISTRY_PATH or ref.get('source_of_truth') != 'github_repo':
        fail('context registry binding is not canonical')
    guard_text(ref.get('repo_ref') or ref.get('registry_ref'), 'registry_ref')


def false_flags(value, names):
    for name in names:
        if as_bool(value.get(name), name):
            fail(name + ' must be false')


def true_flags(value, names):
    for name in names:
        if as_bool(value.get(name), name) is not True:
            fail(name + ' must be true')


def changed_files(value):
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        fail('changed files must be a sequence')
    result = []
    for row in value:
        row = as_map(row, 'changed_file')
        if guard_text(row.get('status'), 'status') in {'deleted', 'removed'} or row.get('deleted') is True:
            fail('deleted files are forbidden')
        result.append(path(row.get('path')))
    return approved_paths(tuple(result))


def pr_refs(value, feature_branch, base_branch, head_sha):
    ref = as_map(value, 'pr_refs')
    if not isinstance(ref.get('pr_number'), int) or ref.get('pr_number') <= 0:
        fail('pr_number is required')
    guard_text(ref.get('pr_url'), 'pr_url')
    if ref.get('head_branch') != feature_branch or ref.get('base_branch') != base_branch or ref.get('head_sha') != head_sha:
        fail('PR refs must bind base branch, feature branch, and head SHA')
    if guard_text(ref.get('status'), 'pr_status') != 'open':
        fail('PR must remain open for PM L2 review')


def ci_refs(value, head_sha):
    ref = as_map(value, 'ci_refs')
    if ref.get('workflow_name') != 'tests':
        fail('ci workflow must be tests')
    if not isinstance(ref.get('run_id'), int) or ref.get('run_id') <= 0:
        fail('ci run_id is required')
    guard_text(ref.get('job_name'), 'job_name')
    if ref.get('head_sha') != head_sha:
        fail('CI must be tied to the PR head SHA')
    if guard_text(ref.get('conclusion'), 'ci_conclusion') not in {'success', 'failure', 'cancelled', 'timed_out', 'action_required'}:
        fail('unsupported CI conclusion')
    return ref


def validate_route_b_github_execution_request_values(values):
    values = as_map(values, 'request')
    required = ('schema_version','workflow_run_id','request_id','repository_full_name','base_branch','base_sha','route_b_context_registry_ref','pm_l2_request_ref','pm_l3_package_ref','execution_scope','branch_plan','pr_plan','validation_requirements','governance_flags','rejection_rules')
    fields(values, required)
    if values['schema_version'] != REQUEST_SCHEMA_VERSION:
        fail('bad schema')
    registry_ref(values['route_b_context_registry_ref'])
    role_ref(values['pm_l2_request_ref'])
    role_ref(values['pm_l3_package_ref'])
    scope = as_map(values['execution_scope'], 'execution_scope')
    if scope.get('scope_type') != 'route_b_github_branch_pr_executor_phase1' or as_bool(scope.get('delete_files_allowed'), 'delete_files_allowed'):
        fail('bad execution scope')
    allowed = approved_paths(scope.get('allowed_paths'))
    plan = as_map(values['branch_plan'], 'branch_plan')
    feature_branch = branch(plan.get('branch_name'))
    if plan.get('source') != 'base_sha':
        fail('branch must come from base_sha')
    pr_plan = as_map(values['pr_plan'], 'pr_plan')
    if pr_plan.get('base_branch') != values['base_branch'] or pr_plan.get('head_branch') != feature_branch or as_bool(pr_plan.get('draft'), 'draft'):
        fail('bad pr plan')
    as_map(values['validation_requirements'], 'validation_requirements')
    false_flags(as_map(values['governance_flags'], 'governance_flags'), ('direct_main_write_allowed','n8n_merge_allowed','force_push_allowed','file_delete_allowed','production_runtime_allowed'))
    true_flags(as_map(values['rejection_rules'], 'rejection_rules'), ('reject_dynamic_marker_tokens','reject_target_role_context_ref','reject_missing_role_context_ref','reject_non_n8n_branch_prefix','reject_direct_main_write','reject_n8n_merge','reject_force_push','reject_file_delete','reject_runtime_or_broker_scope'))
    return NS(schema_version=REQUEST_SCHEMA_VERSION, workflow_run_id=guard_text(values['workflow_run_id']), request_id=guard_text(values['request_id']), repository_full_name=guard_text(values['repository_full_name']), base_branch=guard_text(values['base_branch']), base_sha=guard_text(values['base_sha']), branch_name=feature_branch, allowed_paths=allowed)


def validate_route_b_github_execution_result_values(values):
    values = as_map(values, 'result')
    required = ('schema_version','workflow_run_id','request_id','status','repository_full_name','base_branch','base_sha','feature_branch','branch_ref','branch_created_at','implementation_commit_sha','implementation_tree_sha','changed_files','pr_refs','ci_refs','evidence_refs','authority_boundary','error')
    fields(values, required)
    if values['schema_version'] != RESULT_SCHEMA_VERSION:
        fail('bad schema')
    status = guard_text(values['status'], 'status')
    if status not in {'blocked','failed','pr_opened','ci_failed','ci_passed'}:
        fail('bad status')
    base_branch = guard_text(values['base_branch'], 'base_branch')
    feature_branch = branch(values['feature_branch'])
    head_sha = guard_text(values['implementation_commit_sha'], 'implementation_commit_sha')
    ref = as_map(values['branch_ref'], 'branch_ref')
    if ref.get('branch_name') != feature_branch or ref.get('commit_sha') != head_sha:
        fail('branch_ref must point to implementation commit')
    guard_text(values['branch_created_at'], 'branch_created_at')
    guard_text(values['implementation_tree_sha'], 'tree_sha')
    files = changed_files(values['changed_files'])
    pr_refs(values['pr_refs'], feature_branch, base_branch, head_sha)
    ci = ci_refs(values['ci_refs'], head_sha)
    if status == 'ci_passed' and ci.get('conclusion') != 'success':
        fail('ci_passed requires successful CI')
    as_map(values['evidence_refs'], 'evidence_refs')
    boundary = as_map(values['authority_boundary'], 'authority_boundary')
    if boundary.get('merge_authority') != 'PM_L2_ONLY':
        fail('bad merge authority')
    false_flags(boundary, ('merge_performed_by_executor','approved_for_merge','pm_l2_approval_claimed_by_executor','n8n_merge_allowed','direct_main_write_allowed'))
    if values['error'] is not None and not isinstance(values['error'], dict):
        fail('error must be null or mapping')
    return NS(schema_version=RESULT_SCHEMA_VERSION, workflow_run_id=guard_text(values['workflow_run_id']), request_id=guard_text(values['request_id']), status=status, repository_full_name=guard_text(values['repository_full_name']), feature_branch=feature_branch, implementation_commit_sha=head_sha, changed_files=files)


def validate_route_b_pr_validation_package_values(values):
    values = as_map(values, 'validation_package')
    required = ('schema_version','validation_package_id','workflow_run_id','request_id','repository_full_name','context_registry_binding','scope_validation','git_refs','pr_refs','ci_refs','changed_file_refs','pm_l3_validation','pm_l2_boundary')
    fields(values, required)
    if values['schema_version'] != VALIDATION_PACKAGE_SCHEMA_VERSION:
        fail('bad schema')
    registry_ref(values['context_registry_binding'])
    scope = as_map(values['scope_validation'], 'scope_validation')
    approved_paths(scope.get('approved_scope_paths'))
    true_flags(scope, ('changed_files_within_scope','file_scope_exact','forbidden_operations_absent'))
    refs = as_map(values['git_refs'], 'git_refs')
    base_branch = guard_text(refs.get('base_branch'), 'base_branch')
    feature_branch = branch(refs.get('feature_branch'))
    head_sha = guard_text(refs.get('head_sha'), 'head_sha')
    pr_refs(values['pr_refs'], feature_branch, base_branch, head_sha)
    ci_refs(values['ci_refs'], head_sha)
    files = changed_files(values['changed_file_refs'])
    pm_l3 = as_map(values['pm_l3_validation'], 'pm_l3_validation')
    if guard_text(pm_l3.get('validation_verdict'), 'validation_verdict') not in {'pass','conditional_pass','fail','blocked'}:
        fail('bad verdict')
    as_bool(pm_l3.get('evidence_complete'), 'evidence_complete')
    pm_l2 = as_map(values['pm_l2_boundary'], 'pm_l2_boundary')
    if as_bool(pm_l2.get('pm_l2_review_required'), 'pm_l2_review_required') is not True or pm_l2.get('merge_authority') != 'PM_L2_ONLY' or as_bool(pm_l2.get('n8n_merge_allowed'), 'n8n_merge_allowed'):
        fail('bad PM L2 boundary')
    approval_ref = pm_l2.get('explicit_pm_l2_approval_package_ref')
    if approval_ref is not None:
        guard_text(approval_ref, 'approval_ref')
    if as_bool(pm_l2.get('approved_for_merge'), 'approved_for_merge') and approval_ref is None:
        fail('approved_for_merge requires explicit PM L2 approval package')
    return NS(schema_version=VALIDATION_PACKAGE_SCHEMA_VERSION, validation_package_id=guard_text(values['validation_package_id']), workflow_run_id=guard_text(values['workflow_run_id']), request_id=guard_text(values['request_id']), repository_full_name=guard_text(values['repository_full_name']), feature_branch=feature_branch, head_sha=head_sha, changed_files=files)


__all__ = ('APPROVED_ROUTE_B_EXECUTOR_PATHS','REQUEST_SCHEMA_VERSION','RESULT_SCHEMA_VERSION','ROUTE_B_CONTEXT_REGISTRY_PATH','VALIDATION_PACKAGE_SCHEMA_VERSION','RouteBGithubExecutionContractError','validate_route_b_github_execution_request_values','validate_route_b_github_execution_result_values','validate_route_b_pr_validation_package_values')
