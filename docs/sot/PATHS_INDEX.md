# SoT Paths Index (Manifest)

GitHub = Source of Truth
Server = Applied State only

## Policy
- in_git → file must exist in repository
- server_only → runtime data, never committed to Git

If required key is missing → STOP and escalate.

## Registered Keys

- master_5m_si_cny_futoi_obstats → data/master/master_5m_si_cny_futoi_obstats_2020-01-03_2026-02-03.csv (server_only)
- regime_day_r1r4 → data/research/regime_day_r1r4.csv (in_git)
- levels_si_d1_events → data/research/levels_si_d1_events.csv (git_managed)
- lib_moex_api → src/api/utils/lib_moex_api.py (in_git)
- fo_snapshot → src/api/futures/fo_snapshot.py (in_git)
- fo_5m_day → src/api/futures/fo_5m_day.py (in_git)
- fo_5m_period → src/api/futures/fo_5m_period.py (in_git)
- fx_snapshot → src/api/fx/fx_snapshot.py (in_git)
- fx_5m_day → src/api/fx/fx_5m_day.py (in_git)
- fx_5m_period → src/api/fx/fx_5m_period.py (in_git)
- phase_transition_state_dir → data/state/ (server_only)
- phase_transition_logs_dir → logs/ (server_only)

## Enforcement Rule
1. Use manifest key.
2. Resolve path.
3. Verify scope.
4. If mismatch or missing → STOP.
