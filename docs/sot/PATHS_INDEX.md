# SoT Paths Index (Manifest)

GitHub = Source of Truth  
Server = Applied State only

## Policy
- `in_git` → artifact lives in repository
- `server_only` / `external_contract` / `env_provided` → runtime or external contract, not guaranteed in Git
- Contract source is explicit via `contract_mode`: `repo_path`, `external_path`, `external_glob`, `env_var`, `cli_arg`

If required key is missing → STOP and escalate.

## Registered Keys (canonical subset)

- `master_5m_si_cny_futoi_obstats` → `data/master/master_5m_si_cny_futoi_obstats_*.csv` (`external_glob`)
- `master_path_env` → `MASTER_PATH` (`env_var`)
- `phase_transition_state_dir` → `data/state/` (`repo_path`)
- `phase_transition_logs_dir` → `logs/` (`repo_path`)
- `gate_input_fo_5m_d1` → `data/state/fo_5m_D-1.csv` (`repo_path`)
- `gate_input_day_metrics_d1` → `data/state/day_metrics_D-1.csv` (`repo_path`)
- `gate_rel_range_history` → `data/state/rel_range_history.csv` (`repo_path`)
- `gate_phase_transition_risk` → `data/state/phase_transition_risk.json` (`repo_path`)
- `phase_transition_thresholds_config` → `config/phase_transition_p10.json` (`repo_path`)
- `research_ema_pnl_day` → `data/research/ema_pnl_day.csv` (`repo_path`)
- `research_day_metrics_from_master` → `data/research/day_metrics_from_master.csv` (`repo_path`)

## Enforcement Rule
1. Use manifest key.
2. Resolve active contract source (`canonical_path` / `canonical_pattern` / `env_var` / `cli_arg`).
3. Verify ownership/scope.
4. If mismatch or missing → STOP.
