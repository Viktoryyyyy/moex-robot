"""Freeze an unlabeled, source-balanced human review queue from a news audit."""
import argparse
from collections import defaultdict
from hashlib import sha256
import json
from pathlib import Path
import re

from .rub_news_selection import POLICY, _time


def build(raw, *, expected_sha256, count=100):
    if not re.fullmatch('[0-9a-f]{64}', expected_sha256) or sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('news audit hash mismatch')
    if isinstance(count, bool) or not isinstance(count, int) or count < 100:
        raise ValueError('review requires at least 100 candidates')
    audit = json.loads(raw)
    if audit.get('policy') != POLICY: raise ValueError('unsupported news audit')
    as_of = _time(audit['as_of'])
    groups, seen_ids = defaultdict(list), set()
    for item in audit['candidates']:
        if item['event_id'] in seen_ids: raise ValueError('duplicate candidate identity')
        seen_ids.add(item['event_id'])
        if not _time(item['published_at']) <= _time(item['available_at']) <= _time(item['ingested_at']) <= as_of:
            raise ValueError('candidate chronology invalid')
        if not item['source_reference'].startswith('https://'):
            raise ValueError('reviewable source reference required')
        groups[item['source_id']].append(item)
    if not set(audit['selected_ids']) <= seen_ids: raise ValueError('selection references missing candidates')
    for values in groups.values():
        values.sort(key=lambda x: (_time(x['available_at']), x['event_id']), reverse=True)
    chosen, clusters = [], set()
    while groups and len(chosen) < count:
        for source in sorted(list(groups)):
            item = groups[source].pop(0)
            if not groups[source]: del groups[source]
            if item['cluster_id'] in clusters: continue
            clusters.add(item['cluster_id'])
            chosen.append(item)
            if len(chosen) == count: break
    if len(chosen) < count: raise ValueError('insufficient distinct candidate clusters')
    rows = []
    for item in chosen:
        metadata = {k: item[k] for k in ('event_id', 'cluster_id', 'source_id', 'source_reference',
                    'published_at', 'available_at', 'ingested_at', 'content_hash', 'quality_status')}
        metadata['source_provenance'] = item.get('source_provenance', [])
        metadata['included_in_live_view'] = item['event_id'] in audit['selected_ids']
        metadata['review'] = {'rub_relevant': None, 'gold_event_group': None,
            'source_verified': None, 'reviewer': None, 'rationale': None}
        rows.append(metadata)
    return {'schema_version': 'rub_news_review_queue.v1', 'audit_sha256': expected_sha256,
        'source_as_of': audit['as_of'], 'candidate_count': len(rows),
        'source_count': len({x['source_id'] for x in rows}), 'rows': rows,
        'human_review_complete': False, 'distinct_real_events_verified': False,
        'relevance_validation_complete': False, 'model_evaluation_performed': False,
        'action_authority': False,
        'limitations': ['existing_clusters_are_not_gold_labels',
            'review_source_content_at_original_URL', 'metadata_archive_does_not_contain_full_text']}


def export(raw, *, expected_sha256, output, count=100):
    queue = build(raw, expected_sha256=expected_sha256, count=count)
    encoded = json.dumps(queue, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()
    root = Path(output)
    if any(p.is_symlink() for p in (root, *root.parents)): raise ValueError('output symlink refused')
    root.mkdir(parents=True, exist_ok=True)
    path = root / (sha256(encoded).hexdigest() + '.json')
    try:
        with path.open('xb') as stream: stream.write(encoded)
    except FileExistsError:
        if path.read_bytes() != encoded: raise ValueError('review queue collision')
    return path


if __name__ == '__main__':
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--audit', required=True, type=Path)
    cli.add_argument('--audit-sha256', required=True)
    cli.add_argument('--output', required=True, type=Path)
    args = cli.parse_args()
    if args.audit.is_symlink(): raise ValueError('audit symlink refused')
    with args.audit.open('rb') as stream: raw = stream.read(20_000_001)
    if len(raw) > 20_000_000: raise ValueError('audit too large')
    print(export(raw, expected_sha256=args.audit_sha256, output=args.output))
