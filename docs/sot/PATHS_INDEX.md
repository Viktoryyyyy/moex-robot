# SoT Paths Index (Manifest)

GitHub = Source of Truth  
Server = Applied State only

## Policy
- `in_git` → artifact lives in repository
- `server_only` / `external_contract` / `env_provided` → runtime or external contract, not guaranteed in Git
- Contract source is explicit via `contract_mode`: `repo_path`, `external_path`, `external_glob`, `env_var`, `cli_arg`

If required key is missing → STOP and escalate.

## Applied Server Repo Root
- Current applied server repo root: `/home/trader/moex_bot/moex-robot`
- Current applied venv root: `/home/trader/moex_bot/venv`
- All `repo_path` server/runtime contracts in this project resolve relative to this path unless a later approved metadata update changes it.

## Registered Keys (canonical subset)

- `applied_server_repo_root` → `/home/trader/moex_bot/moex-robot` (`external_path`, server_only)
- `applied_server_venv_root` → `/home/trader/moex_bot/venv` (`external_path`, server_only)
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

- `research_ema_stage1_baseline_day` → `data/research/ema_stage1_baseline_day.csv` (`repo_path`, server_only)
- `research_ema_gate_joined_day_hist` → `data/research/ema_gate_joined_day_hist.csv` (`repo_path`, server_only)


- `ema_d_day_context_latest_json` → `data/state/ema_d_day_context_latest.json` (`repo_path`, server_only)
- `ema_d_day_context_history_csv` → `data/state/ema_d_day_context_history.csv` (`repo_path`, server_only)
- `ema_3_19_15m_pilot_journal_csv` → `data/state/ema_3_19_15m_pilot_journal.csv` (`repo_path`, server_only)
- `ema_3_19_15m_pilot_day_status_csv` → `data/state/ema_3_19_15m_pilot_day_status.csv` (`repo_path`, server_only)
- `ema_3_19_15m_signals_dir` → `data/signals/` (`repo_path`, server_only)
- `ema_3_19_15m_trade_log_csv` → `data/signals/ema_3_19_15m_realtime_*.csv` (`external_glob`, server_only)
- `ema_3_19_15m_session_state_json` → `data/state/ema_3_19_15m_session_*.json` (`external_glob`, server_only)
- `ema_3_19_15m_runtime_lock` → `data/state/ema_3_19_15m_realtime.lock` (`repo_path`, server_only)

## Enforcement Rule
1. Use manifest key.
2. Resolve active contract source (`canonical_path` / `canonical_pattern` / `env_var` / `cli_arg`).
3. Verify ownership/scope.
4. If mismatch or missing → STOP.
