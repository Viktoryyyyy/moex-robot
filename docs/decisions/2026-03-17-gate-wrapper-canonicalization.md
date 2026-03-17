# Gate Wrapper Canonicalization

- Date: 2026-03-17
- Repo: Viktoryyyyy/moex-robot
- Base branch: origin/main
- Approved source branch: fix/gate_master_preflight_c4
- Merged commit on main: 3b912d8e191764922db9f2feb47859c207627ee8

## Decision

Canonical daily Gate orchestration is fixed via:
- `src/cli/run_phase_transition_gate_daily.py` added as the zero-arg orchestration wrapper
- `src/cli/phase_transition_gate.py` updated with C4/master bootstrap hardening

## Approved diff surface

1. `src/cli/run_phase_transition_gate_daily.py` — added
2. `src/cli/phase_transition_gate.py` — modified

No other files were part of the approved apply surface.

## Architectural conclusion

- canonical Gate branch candidate selected: `fix/gate_master_preflight_c4`
- branch diff vs main confirmed narrow
- merge readiness confirmed
- controlled apply to `main` completed successfully

## Runtime contract

- wrapper uses canonical `data/state/phase_transition_risk.json`
- wrapper uses canonical `data/state/rel_range_history.csv`
- wrapper invokes `src.cli.daily_metrics_builder`
- wrapper invokes `src.cli.phase_transition_gate`

## PM note

`MASTER_PATH` bootstrap dependency exists when rel_range history is missing.
This is an operational caveat, not an architectural blocker.
