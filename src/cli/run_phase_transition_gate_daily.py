from __future__ import annotations

import os
import subprocess
import sys
from dotenv import load_dotenv

IN_5M = 'data/state/fo_5m_D-1.csv'
IN_DAY = 'data/state/day_metrics_D-1.csv'
IN_HISTORY = 'data/state/rel_range_history.csv'
OUT_JSON = 'data/state/phase_transition_risk.json'
OUT_HISTORY = 'data/state/rel_range_history.csv'
CONFIG = 'config/phase_transition_p10.json'

def run(cmd):
    rc = subprocess.call(cmd)
    if rc != 0:
        raise SystemExit(rc)

def main():
    load_dotenv()
    master_path = os.getenv('MASTER_PATH')
    if not master_path:
        raise SystemExit('MASTER_PATH is required for canonical tail refresh')
    run([sys.executable,'-m','src.cli.refresh_canonical_master_tail','--master-path',master_path,'--lookback-days','7'])
    run([sys.executable,'-m','src.cli.daily_metrics_builder','--key','Si','--date','D-1','--out-5m',IN_5M,'--out-day',IN_DAY])
    run([sys.executable,'-m','src.cli.validate_master_freshness','--in-day',IN_DAY])
    run([sys.executable,'-m','src.cli.phase_transition_gate','--in-day',IN_DAY,'--in-history',IN_HISTORY,'--config',CONFIG,'--out-json',OUT_JSON,'--out-history',OUT_HISTORY])
    return 0

if __name__ == '__main__':
    raise SystemExit(main())

