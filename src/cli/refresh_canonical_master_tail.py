from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from src.pipeline.update_master_si_cny_futoi_obstats import resolve_default_end


def _baseline_end(master_path: Path) -> date:
    if not master_path.exists():
        raise SystemExit(f'canonical baseline missing: {master_path}')
    df = pd.read_csv(master_path, usecols=['end'])
    if df.empty:
        raise SystemExit(f'canonical baseline empty: {master_path}')
    df['end'] = pd.to_datetime(df['end'])
    return df['end'].max().date()


def _validate_candidate(candidate: Path, baseline_end: date) -> None:
    if not candidate.exists():
        raise SystemExit(f'updater did not produce candidate file: {candidate}')
    df = pd.read_csv(candidate, usecols=['end'])
    if df.empty:
        raise SystemExit(f'candidate master empty: {candidate}')
    df['end'] = pd.to_datetime(df['end'])
    cand_end = df['end'].max().date()
    if cand_end < baseline_end:
        raise SystemExit(f'candidate regressed end date: candidate={cand_end} baseline={baseline_end}')


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--master-path', required=True, help='Explicit canonical master path')
    p.add_argument('--end', dest='end_date', help='YYYY-MM-DD (default: updater canonical default)')
    p.add_argument('--lookback-days', type=int, default=7)
    args = p.parse_args()

    master_path = Path(args.master_path)
    baseline_end = _baseline_end(master_path)
    end_target = date.fromisoformat(args.end_date) if args.end_date else resolve_default_end()
    from_date = baseline_end - timedelta(days=args.lookback_days)
    candidate_path = master_path.with_name(master_path.name + '.tmp_refresh')
    if candidate_path.exists():
        candidate_path.unlink()

    cmd = [
        sys.executable,
        '-m',
        'src.pipeline.update_master_si_cny_futoi_obstats',
        '--from',
        from_date.isoformat(),
        '--end',
        end_target.isoformat(),
        '--bounded-tail-refresh',
        '--out-master-path',
        str(candidate_path),
    ]
    rc = subprocess.call(cmd)
    if rc != 0:
        if candidate_path.exists():
            candidate_path.unlink()
        raise SystemExit(rc)

    _validate_candidate(candidate_path, baseline_end)
    os.replace(candidate_path, master_path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

