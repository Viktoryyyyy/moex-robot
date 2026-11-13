"""Verify published aggregates, optionally against a private full rerun."""
import argparse
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parent
GROUPS=['all','long','short','independent','train_2022_2024','validation_2025','test_2026']
METRICS=['signals','filled','fill_pct','mean_R_per_signal','funding_covered_signals','mean_net_R_per_signal']
def public_summary(result):
    return {
        'publication_scope':'Aggregate research statistics only; no individual events or raw bars.',
        **{k:result[k] for k in ['source_sha256','bar_rows','daily_rows','all_daily_ohlcv_reconciled','events_total','events_mature','strategy_names','selection']},
        'summary':{g:{name:{key:record[key] for key in METRICS} for name,record in result['summary'][g].items()} for g in GROUPS},
        'corrections':{g:result['corrections'][g] for g in ['long','short']}}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rerun-dir',type=Path)
    args=parser.parse_args()
    summary=json.loads((ROOT/'summary.json').read_text(encoding='utf-8'))
    robust=json.loads((ROOT/'robustness.json').read_text(encoding='utf-8'))
    assert summary['events_mature']==robust['verified_events']==179
    assert len(summary['strategy_names'])*summary['events_mature']==robust['verified_simulations']==4654
    assert summary['all_daily_ohlcv_reconciled'] and robust['causal_pivot_and_execution_checks_passed']
    if args.rerun_dir:
        actual=json.loads((args.rerun_dir/'breakout_pullback_results.json').read_text(encoding='utf-8'))
        assert summary==public_summary(actual)
        assert len(actual['events'])==179
        assert sum(len(e['trades']) for e in actual['events'])==4654
        rerun_robust=json.loads((args.rerun_dir/'pullback_robustness.json').read_text(encoding='utf-8'))
        assert robust==rerun_robust
    print(json.dumps({'aggregates_verified':True,'events':179,'simulations':4654,
                      'rerun_aggregates_identical':bool(args.rerun_dir)},indent=2))

if __name__=='__main__':main()
