"""Explicit mixed-source research bars and coverage-labelled HTF previews.

Never edits raw captures or promotes accepted pointers. Calendar/session
attestation remains external; daily and weekly outputs are observed-window only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import timedelta, datetime, timezone
from decimal import Decimal
from pathlib import Path

from moex_data.rub_history_gap_reconciliation import FIELDS, reconcile, timestamp


def combine(rows, entries, secid):
    bars = {}
    empty = set()
    evidence = []
    for raw in rows:
        end = timestamp(raw['ts'])
        if end in bars or raw['secid'] != secid:
            raise ValueError('duplicate bar or wrong contract')
        available = timestamp(raw['ingest_ts'], aware=True)
        if available < end:
            raise ValueError('ingest time precedes bar end')
        bars[end] = dict(interval_end=end.isoformat(),
            **{field: str(raw[field]) for field in FIELDS},
            **{field: None if raw.get(field) is None else str(raw[field])
               for field in ('value', 'num_trades')},
            source_id=raw['source_id'], availability_ts=available.isoformat())
    original = dict(bars)
    for entry in entries:
        if entry['secid'] != secid:
            continue
        result = reconcile(entry)
        if result['status'] != 'CLASSIFIED':
            raise ValueError('unresolved gap evidence')
        for neighbor in entry['neighbors']:
            end = timestamp(neighbor['end'], aware=True)
            if end not in original or any(Decimal(original[end][f]) != Decimal(str(neighbor[f])) for f in FIELDS):
                raise ValueError('evidence does not bind to captured neighbors')
        evidence.append(result['evidence_sha256'])
        for item in result['intervals']:
            end = timestamp(item['interval_end'], aware=True)
            if end in bars or end in empty:
                raise ValueError('overlapping or duplicate gap evidence')
            if item['status'] == 'CORROBORATED_EMPTY':
                empty.add(end)
            else:
                bars[end] = dict(interval_end=end.isoformat(), **item['ohlcv'],
                    source_id=item['source_id'], availability_ts=item['availability_ts'],
                    value=None, num_trades=None, evidence_sha256=result['evidence_sha256'])
    return [bars[end] for end in sorted(bars)], empty, evidence


def previews(bars, empty, evidence_available):
    groups = {}
    for bar in bars:
        end = timestamp(bar['interval_end'], aware=True)
        hour = (end - timedelta(microseconds=1)).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        day = end.date()
        monday = day - timedelta(days=day.weekday())
        for frame, key in [('H1', hour.isoformat()), ('D1', day.isoformat()), ('W1', monday.isoformat())]:
            groups.setdefault((frame, key), []).append(bar)
    output = []
    for (frame, key), group in sorted(groups.items()):
        group.sort(key=lambda row: timestamp(row['interval_end']))
        available = max([timestamp(row['availability_ts'], aware=True) for row in group] + [evidence_available])
        complete = False
        if frame == 'H1':
            end = timestamp(key, aware=True)
            observed = {timestamp(row['interval_end']) for row in group} | empty
            complete = all(end - timedelta(minutes=5*i) in observed for i in range(12))
        row = dict(timeframe=frame, period=key,
            open=group[0]['open'], close=group[-1]['close'],
            high=str(max(Decimal(r['high']) for r in group)),
            low=str(min(Decimal(r['low']) for r in group)),
            availability_ts=available.isoformat(), source_row_count=len(group),
            sources=sorted({r['source_id'] for r in group}),
            coverage_status='FULL_CLOCK_HOUR' if complete else 'OBSERVED_WINDOW_ONLY',
            session_calendar_attested=False, historical_pit_ready=False,
            model_acceptance_granted=False)
        for field in ('volume', 'value', 'num_trades'):
            row[field] = None if any(r[field] is None for r in group) else str(sum(Decimal(r[field]) for r in group))
        if frame == 'W1':
            row['week_complete'] = False
        output.append(row)
    return output


def build(run, evidence_path, output):
    import pyarrow.parquet as pq
    from moex_data.rub_contract_history_verify import verify
    from moex_data.rub_contract_history_capture import digest

    verification = verify(run)
    manifest = json.loads((run / 'capture_manifest.json').read_text())
    evidence_bytes = evidence_path.read_bytes()
    entries = json.loads(evidence_bytes)
    rows = []
    for day in manifest['dates']:
        if day['status'] == 'CAPTURED':
            info = day['artifacts']['partition']
            path = run / info['path']
            rows.extend(pq.ParquetFile(path).read().to_pylist())
            if digest(path) != info['sha256']:
                raise ValueError('source changed during build')
    bars, empty, hashes = combine(rows, entries, manifest['secid'])
    checked = max(timestamp(e['checked_at_utc'], aware=True) for e in entries if e['secid'] == manifest['secid'])
    aggregates = previews(bars, empty, checked)
    boundaries = []
    for day in manifest['dates']:
        selected = [b for b in bars if timestamp(b['interval_end']).date().isoformat() == day['trade_date']]
        boundaries.append(dict(trade_date=day['trade_date'],
            first_bar_end=selected[0]['interval_end'] if selected else None,
            last_bar_end=selected[-1]['interval_end'] if selected else None,
            row_count=len(selected), session_calendar_attested=False,
            status='OBSERVED_EDGES_NOT_SESSION_PROOF' if selected else 'SOURCE_EMPTY_NOT_CLOSURE_PROOF'))
    result = dict(schema_version='rub_reconciled_history_preview.v1', secid=manifest['secid'],
        policy='explicit_iss_ohlcv_repair_research_only_v1',
        capture_manifest_sha256=verification['capture_manifest_sha256'],
        evidence_file_sha256=hashlib.sha256(evidence_bytes).hexdigest(),
        evidence_sha256=hashes, source_row_count=len(rows), row_count=len(bars),
        recovered_bar_count=len(bars)-len(rows), empty_interval_count=len(empty),
        empty_intervals=sorted(t.isoformat() for t in empty),
        model_acceptance_granted=False, accepted_pointer_promotion=False,
        built_at_utc=datetime.now(timezone.utc).isoformat(),
        bars=bars, aggregates=aggregates, session_boundaries=boundaries)
    # Exclusive creation prevents replacing an earlier research result.
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
    return {k:v for k,v in result.items() if k not in ('bars','aggregates','session_boundaries','empty_intervals')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True, type=Path)
    parser.add_argument('--evidence', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.run,args.evidence,args.output), indent=2))
