from hashlib import sha256
import json

import pytest

from moex_research.intelligence.news_review_corpus import build, export
from moex_research.intelligence.rub_news_selection import POLICY


def audit(n=120):
    stamp = '2026-09-08T07:00:00+00:00'
    return {'policy': POLICY, 'as_of': stamp, 'selected_ids': ['event0'],
        'candidates': [{'event_id': f'event{i}', 'cluster_id': f'cluster{i}',
            'source_id': f'source{i%5}', 'source_reference': f'https://example.org/{i}',
            'published_at': stamp, 'available_at': stamp, 'ingested_at': stamp,
            'quality_status': 'OK', 'content_hash': sha256(str(i).encode()).hexdigest()} for i in range(n)]}


def encode(value): return json.dumps(value).encode()


def test_queue_is_source_balanced_unlabeled_and_repeatable(tmp_path):
    raw = encode(audit())
    digest = sha256(raw).hexdigest()
    q = build(raw, expected_sha256=digest)
    assert q['candidate_count'] == 100 and q['source_count'] == 5
    assert all(sum(r['source_id'] == f'source{i}' for r in q['rows']) == 20 for i in range(5))
    assert len({r['cluster_id'] for r in q['rows']}) == 100
    assert all(all(v is None for v in r['review'].values()) for r in q['rows'])
    assert q['human_review_complete'] is q['relevance_validation_complete'] is q['model_evaluation_performed'] is False
    path = export(raw, expected_sha256=digest, output=tmp_path)
    assert path == export(raw, expected_sha256=digest, output=tmp_path)
    assert sha256(path.read_bytes()).hexdigest() == path.stem


@pytest.mark.parametrize('defect', ['hash', 'count', 'duplicate', 'time', 'selection', 'clusters'])
def test_invalid_or_insufficient_audit_fails(defect):
    value = audit(99 if defect == 'count' else 120)
    if defect == 'duplicate': value['candidates'][1]['event_id'] = 'event0'
    if defect == 'time': value['candidates'][1]['ingested_at'] = '2026-09-09T00:00:00+00:00'
    if defect == 'selection': value['selected_ids'] = ['missing']
    if defect == 'clusters':
        for item in value['candidates']: item['cluster_id'] = 'same'
    raw = encode(value)
    digest = '0'*64 if defect == 'hash' else sha256(raw).hexdigest()
    with pytest.raises(ValueError): build(raw, expected_sha256=digest)
