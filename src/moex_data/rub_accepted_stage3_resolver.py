"""Accepted Stage3 archive resolver; preserves the existing parent and marker gate."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
MOSCOW = timezone(timedelta(hours=3))


def _stamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None: raise ValueError('timestamp_timezone_required')
    return parsed


def _resolve(root, marker_path, *, now, earliest, standalone_accepted_at=None, byte_reader=None):
    from moex_data import step3_raw_acceptance as stage3, step9_rub_analysis_bundle as step9
    from datetime import date
    def load(path, label):
        if byte_reader is None: return step9._load_json(path, label)
        import json
        value=json.loads(byte_reader(path).decode('utf-8'))
        if not isinstance(value,dict): raise ValueError(label+' must be a JSON object')
        return value
    run = marker_path.parent.name.removeprefix('run_id=')
    marker_path = step9._resolve_root_ref(step9.ROOT_REF_PREFIX + marker_path.relative_to(root).as_posix(), 'accepted_marker', root)
    marker = load(marker_path, 'accepted_marker')
    if (marker.get('project') != 'MOEX_Bot' or marker.get('step') != 3 or marker.get('status') != 'accepted'
            or marker.get('run_id') != run or marker.get('acceptance_contract_id') != stage3.CONTRACT_ID
            or marker.get('artifact_semantics') != 'immutable_run_scoped'
            or marker.get('accepted_pointer_count') != 10 or marker.get('expected_pointer_count') != 10):
        raise ValueError('accepted_marker_identity_or_status_mismatch')
    pilot_path = step9._resolve_root_ref(marker['pilot_evidence_ref'], 'pilot_evidence', root)
    if pilot_path != stage3.pilot_evidence_path(run).resolve(): raise ValueError('pilot_evidence_run_path_mismatch')
    pilot = load(pilot_path, 'pilot_evidence')
    observed = date.fromisoformat(pilot['trade_date'])
    if observed < earliest or observed > now.astimezone(MOSCOW).date(): return None
    binding = _stamp(pilot["reference_observed_at_utc"])
    if standalone_accepted_at is None:
        if not run.endswith('_stage3'): raise ValueError('successful_parent_proof_unavailable')
        parent_run = run[:-7]
        parent_path = step9._resolve_root_ref(step9.ROOT_REF_PREFIX + 'runs/step10_rub_daily_refresh/run_id=' + parent_run + '/run_manifest.json', 'parent', root)
        parent = load(parent_path, 'parent')
        refresh = parent.get('source_refresh', {})
        if (parent.get('project') != 'MOEX_Bot' or parent.get('stage') != 10 or parent.get('run_id') != parent_run
                or parent.get('status') != 'succeeded' or parent.get('current_pointer_rollback_status') not in (None, 'not_needed')
                or refresh.get('status') != 'refreshed' or refresh.get('stage3_run_id') != run
                or refresh.get('trade_date') != pilot['trade_date']):
            raise ValueError('parent_failed_rolled_back_or_identity_mismatch')
        finished = _stamp(parent['finished_at_utc']); binding = _stamp(pilot['reference_observed_at_utc'])
    else:
        if run != "step3_pilot_20260824_1705" or pilot["trade_date"] != "2026-08-24":
            raise ValueError("standalone_pilot_outside_bounded_admission")
        finished = standalone_accepted_at
        parent = parent_path = None
    if not binding <= finished <= now: raise ValueError('future_binding_or_parent_completion')
    if pilot.get('run_artifacts_immutable') is not True or pilot.get('run_id_reuse_allowed') is not False:
        raise ValueError('immutable_run_proof_missing')
    specs = stage3.validate_pilot_evidence(pilot, run_id=run, byte_reader=byte_reader)
    pointers = marker.get('pointers')
    if not isinstance(pointers, list) or len(pointers) != 10: raise ValueError('accepted_marker_pointer_count')
    for spec in specs:
        matches = [item for item in pointers if item.get('dataset_id') == spec.dataset_id and item.get('instrument_id') == spec.instrument_id]
        if len(matches) != 1: raise ValueError('accepted_marker_pointer_identity')
        item = matches[0]
        expected_pointer = stage3._pointer_path(spec)
        expected_ref = step9.ROOT_REF_PREFIX + expected_pointer.relative_to(root).as_posix()
        if item.get('pointer_ref') != expected_ref:
            raise ValueError('accepted_marker_canonical_pointer_ref_mismatch')
        if 'pointer_path' in item and (not isinstance(item['pointer_path'], str)
                or not Path(item['pointer_path']).is_absolute() or Path(item['pointer_path']) != expected_pointer):
            raise ValueError('accepted_marker_canonical_pointer_path_mismatch')
        if (step9._resolve_root_ref(item['manifest_ref'], 'marker_manifest', root) != spec.manifest_path
                or step9._resolve_root_ref(item['quality_report_ref'], 'marker_quality', root) != spec.quality_path):
            raise ValueError('accepted_marker_support_reference_mismatch')
    return {"run": run, "marker_path": marker_path, "marker": marker, "pilot_path": pilot_path,
        "pilot": pilot, "parent_path": parent_path, "parent": parent,
        "finished": finished, "binding": binding, "specs": specs}


def resolve(root, marker_path, *, now, earliest, byte_reader=None):
    """Legacy gate always requires the successful matching Stage10 parent."""
    return _resolve(root, marker_path, now=now, earliest=earliest, byte_reader=byte_reader)


def resolve_standalone(root, marker_path, *, now, earliest, accepted_at, byte_reader=None):
    """Only the separately admitted Aug24 pilot; never fabricate a parent."""
    return _resolve(root, marker_path, now=now, earliest=earliest, standalone_accepted_at=accepted_at, byte_reader=byte_reader)
