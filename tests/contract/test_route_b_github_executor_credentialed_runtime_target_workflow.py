from __future__ import annotations
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
TARGET=ROOT/'docs/sot/MOEX_ROUTE_B_GITHUB_EXECUTOR_CREDENTIALED_RUNTIME_V0_1_TARGET.json'
VALIDATOR_NODE='Validate Normalized Runtime Request'
RESOLVE_NODE='Resolve Execution Evidence State'
SWITCH_NODE='Route Execution Resolution'
RETURN_NODE='Return Runtime Result'
GITHUB_MUTATION_NODES={'Fetch Base Branch Ref','Fetch Base Commit','Validate Base Ref','Build Git Tree Elements From Exact File Changes','Create Feature Branch','Create Git Tree','Create Implementation Commit','Update Feature Branch Ref','Open Pull Request','Persist Final Execution Evidence'}
def workflow(): return json.loads(TARGET.read_text(encoding='utf-8'))
def node(name):
    matches=[item for item in workflow()['nodes'] if item.get('name')==name]
    assert len(matches)==1
    return matches[0]
def connection_outputs(target,node_name): return [[edge['node'] for edge in output] for output in target['connections'][node_name]['main']]
def reachable_nodes_from_output(target,start_node,output_index):
    stack=[edge['node'] for edge in target['connections'][start_node]['main'][output_index]]; seen=set()
    while stack:
        current=stack.pop()
        if current in seen: continue
        seen.add(current)
        for output in target.get('connections',{}).get(current,{}).get('main',[]): stack.extend(edge['node'] for edge in output)
    return seen
def test_owner_repo_are_derived_from_repository_full_name_and_token_required():
    script=node(VALIDATOR_NODE)['parameters']['jsCode']
    assert 'repository_full_name_parts_required' in script
    assert "repository_full_name.split('/')" in script
    assert 'const owner=repository_full_name_parts[0]' in script
    assert 'const repo=repository_full_name_parts[1]' in script
    assert "repository_full_name!=='Viktoryyyyy/moex-robot'" in script
    assert 'owner,repo,approved_for_merge:false,merge_performed:false' in script
def test_load_execution_evidence_registry_is_scoped_not_event_type_only():
    query=node('Load Execution Evidence Registry')['parameters']['query']
    assert 'event_type IN' in query
    assert "workflow_run_id = '{{ $json.workflow_run_id }}'" in query
    assert "event_payload_json->>'execution_request_id'" in query
    assert "event_payload_json->>'request_fingerprint_sha256'" in query
    assert ' OR ' in query
    assert workflow()['meta']['evidenceRegistryOrdering']=='not_declared'
    assert 'deterministic_ordering_blocked_no_known_timestamp_or_id_column_in_scope' in workflow()['meta']['remainingGaps']
def test_resolve_execution_evidence_state_has_no_stub_logic_and_reads_payload_rows():
    script=node(RESOLVE_NODE)['parameters']['jsCode']
    for forbidden in ('const existing_result=null','const existing_result = null','if(false)','if (false)'): assert forbidden not in script
    for required in ("$items('Load Execution Evidence Registry')",'event_payload_json','same_execution_request_id_different_fingerprint',"resolution:'blocked'","resolution:'return_existing_result'","resolution:'resume_or_return_existing_result'","resolution:'proceed_new_execution'",'implementation_commit_sha','pr_number','pr_url','approved_for_merge:false','merge_performed:false'): assert required in script
def test_persist_accepted_and_blocked_events_are_not_empty_jsonb():
    accepted=node('Persist Accepted Execution Evidence')['parameters']['query']
    blocked=node('Persist Blocked Execution Evidence')['parameters']['query']
    assert "'{}'::jsonb" not in accepted
    assert "'{}'::jsonb" not in blocked
    for required in ('execution_request_id','request_fingerprint_sha256','workflow_run_id','role_task_id',"status:'accepted'",'approved_for_merge:false','merge_performed:false'): assert required in accepted
    for required in ('execution_request_id','request_fingerprint_sha256','workflow_run_id','role_task_id',"status:'blocked'",'blocker_code','error','approved_for_merge:false','merge_performed:false'): assert required in blocked
def test_blocked_existing_and_resume_paths_cannot_reach_github_mutation_nodes():
    target=workflow()
    assert connection_outputs(target,SWITCH_NODE)==[['Persist Accepted Execution Evidence'],['Persist Blocked Execution Evidence'],[RETURN_NODE],[RETURN_NODE]]
    assert GITHUB_MUTATION_NODES.issubset(reachable_nodes_from_output(target,SWITCH_NODE,0))
    for output_index in (1,2,3):
        branch_reachable=reachable_nodes_from_output(target,SWITCH_NODE,output_index)
        assert RETURN_NODE in branch_reachable
        assert not (branch_reachable & GITHUB_MUTATION_NODES), (output_index,branch_reachable & GITHUB_MUTATION_NODES)
