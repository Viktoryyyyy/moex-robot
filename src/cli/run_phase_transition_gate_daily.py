import runpy, sys
def main():
    sys.argv=["daily_metrics_builder","--key","Si","--date","D-1","--out-5m","data/realtime/si_5m_D-1.csv","--out-day","data/state/day_metrics_D-1.csv"]
    runpy.run_module("src.cli.daily_metrics_builder", run_name="__main__")
    sys.argv=["phase_transition_gate","--in-day","data/state/day_metrics_D-1.csv","--in-history","data/state/rel_range_history.csv","--config","config/phase_transition_p10.json","--out-json","data/gate/phase_transition_risk.json","--out-history","data/state/rel_range_history.csv"]
    runpy.run_module("src.cli.phase_transition_gate", run_name="__main__")
if __name__=="__main__": main()
